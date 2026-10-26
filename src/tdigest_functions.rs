use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::Arc;

use base64::Engine;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Float64Array, Int64Array,
    StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, EmitTo, Expr, GroupsAccumulator,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::aggregate_functions::QuantileOutput;
use crate::dynamic;
use crate::dynamic_value::Node;
use crate::tdigest::{Kind, MAX_BYTES, Sketch};

const ENVELOPE: &str = "__datafusion_kql_tdigest_v1";

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn strings(array: &ArrayRef) -> Result<&StringArray> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| error("t-digest sketch input requires encoded JSON"))
}

fn binaries(array: &ArrayRef) -> Result<&BinaryArray> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| error("invalid native t-digest aggregate state"))
}

fn envelope(sketch: &mut Sketch) -> Result<Node> {
    Ok(Node::Bag(BTreeMap::from([(
        ENVELOPE.to_owned(),
        Node::String(base64::engine::general_purpose::STANDARD.encode(sketch.encode()?)),
    )])))
}

fn parse(text: &str) -> Result<Option<Sketch>> {
    if text.len() > 1 << 20 {
        return Err(error("t-digest JSON exceeds its 1 MiB byte budget"));
    }
    let value: serde_json::Value = serde_json::from_str(text)
        .map_err(|cause| error(format!("invalid t-digest JSON: {cause}")))?;
    if value.is_null() {
        return Ok(None);
    }
    let text = value
        .as_object()
        .filter(|object| object.len() == 1)
        .and_then(|object| object.get(ENVELOPE))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            error("expected a versioned datafusion-kql t-digest, not a Kusto-service blob")
        })?;
    if text.len() > MAX_BYTES.div_ceil(3) * 4 {
        return Err(error("encoded t-digest exceeds its byte budget"));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(text)
        .map_err(|cause| error(format!("invalid t-digest base64: {cause}")))?;
    Sketch::decode(&bytes).map(Some)
}

fn merge(target: &mut Option<Sketch>, source: &Sketch) -> Result<()> {
    if let Some(target) = target {
        target.merge(source)
    } else {
        *target = Some(source.clone());
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Operation {
    Build(Kind),
    Merge,
    Quantile {
        kind: Kind,
        ranks: Vec<u64>,
        output: QuantileOutput,
    },
}

impl Operation {
    fn initial(&self) -> Option<Sketch> {
        match self {
            Self::Build(kind) | Self::Quantile { kind, .. } => Some(Sketch::new(*kind)),
            Self::Merge => None,
        }
    }
    fn output_type(&self) -> DataType {
        match self {
            Self::Quantile {
                kind,
                ranks,
                output,
            } => output.data_type(*kind, ranks.len()),
            _ => dynamic::data_type(),
        }
    }
    fn evaluate(&self, sketch: &mut Option<Sketch>) -> Result<ScalarValue> {
        match self {
            Self::Quantile {
                kind,
                ranks,
                output,
            } => {
                let values = ranks
                    .iter()
                    .map(|rank| {
                        sketch
                            .as_mut()
                            .map(|sketch| sketch.percentile(f64::from_bits(*rank)))
                            .transpose()
                            .map(Option::flatten)
                    })
                    .collect::<Result<Vec<_>>>()?;
                output.pack(*kind, values, sketch.as_ref().is_none_or(Sketch::is_empty))
            }
            _ => dynamic::from_node(&match sketch {
                Some(sketch) if !sketch.is_empty() => envelope(sketch)?,
                _ => Node::Null,
            }),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Aggregate {
    operation: Operation,
    signature: Signature,
}

pub(crate) fn build(kind: Kind, value: Expr, weight: Expr) -> Expr {
    AggregateUDF::from(Aggregate {
        operation: Operation::Build(kind),
        signature: Signature::exact(
            vec![kind.sample_type(), DataType::Int64],
            Volatility::Immutable,
        ),
    })
    .call(vec![value, weight])
}

pub(crate) fn merge_aggregate(value: Expr) -> Expr {
    AggregateUDF::from(Aggregate {
        operation: Operation::Merge,
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
    })
    .call(vec![value])
}

pub(crate) fn quantile(
    kind: Kind,
    ranks: &[f64],
    output: QuantileOutput,
    value: Expr,
    weight: Expr,
) -> Expr {
    AggregateUDF::from(Aggregate {
        operation: Operation::Quantile {
            kind,
            ranks: ranks.iter().map(|p| p.to_bits()).collect(),
            output,
        },
        signature: Signature::exact(
            vec![kind.sample_type(), DataType::Int64],
            Volatility::Immutable,
        ),
    })
    .call(vec![value, weight])
}

impl AggregateUDFImpl for Aggregate {
    fn name(&self) -> &str {
        match self.operation {
            Operation::Build(_) => "__kql_tdigest",
            Operation::Merge => "__kql_tdigest_merge",
            Operation::Quantile { .. } => "__kql_bounded_percentile",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.operation.output_type())
    }
    fn default_value(&self, _: &DataType) -> Result<ScalarValue> {
        self.operation.evaluate(&mut self.operation.initial())
    }
    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(State {
            operation: self.operation.clone(),
            sketch: self.operation.initial(),
        }))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}[tdigest]", args.name),
            DataType::Binary,
            true,
        ))])
    }
    fn groups_accumulator_supported(&self, _: AccumulatorArgs) -> bool {
        true
    }
    fn create_groups_accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(Groups {
            operation: self.operation.clone(),
            states: Vec::new(),
            heap_bytes: 0,
        }))
    }
}

fn samples(
    values: &[ArrayRef],
    kind: Kind,
    filter: Option<&BooleanArray>,
    mut visit: impl FnMut(usize, u64, u64) -> Result<()>,
) -> Result<()> {
    let weights = values
        .get(1)
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| error("t-digest weights must be signed long values"))?;
    let input = values
        .first()
        .ok_or_else(|| error("missing t-digest samples"))?;
    if weights.len() != input.len() || filter.is_some_and(|filter| filter.len() != input.len()) {
        return Err(error("invalid t-digest input lengths"));
    }
    let integer = if kind != Kind::Real {
        Some(
            input
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| error("t-digest requires integral samples for this type"))?,
        )
    } else {
        None
    };
    let real = if kind == Kind::Real {
        Some(
            input
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| error("t-digest requires real samples for this type"))?,
        )
    } else {
        None
    };
    for row in 0..input.len() {
        if filter.is_some_and(|filter| filter.is_null(row) || !filter.value(row)) {
            continue;
        }
        if weights.is_null(row) {
            continue;
        }
        let weight = weights.value(row);
        if weight <= 0 || input.is_null(row) {
            continue;
        }
        let value = if let Some(integer) = integer {
            integer.value(row) as u64
        } else {
            real.ok_or_else(|| error("missing real t-digest samples"))?
                .value(row)
                .to_bits()
        };
        visit(row, value, weight as u64)?;
    }
    Ok(())
}

fn update(
    operation: &Operation,
    state: &mut Option<Sketch>,
    values: &[ArrayRef],
    filter: Option<&BooleanArray>,
) -> Result<()> {
    match operation {
        Operation::Merge => {
            let input = strings(&values[0])?;
            if filter.is_some_and(|filter| filter.len() != input.len()) {
                return Err(error("invalid t-digest merge filter length"));
            }
            for (row, value) in input.iter().enumerate() {
                if filter.is_some_and(|filter| filter.is_null(row) || !filter.value(row)) {
                    continue;
                }
                if let Some(sketch) = value.map(parse).transpose()?.flatten() {
                    merge(state, &sketch)?;
                }
            }
            Ok(())
        }
        Operation::Build(kind) | Operation::Quantile { kind, .. } => {
            samples(values, *kind, filter, |_, value, weight| {
                state
                    .as_mut()
                    .ok_or_else(|| error("missing t-digest accumulator"))?
                    .insert(value, weight)
            })
        }
    }
}

#[derive(Debug)]
struct State {
    operation: Operation,
    sketch: Option<Sketch>,
}
impl Accumulator for State {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        update(&self.operation, &mut self.sketch, values, None)
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.operation.evaluate(&mut self.sketch)
    }
    fn size(&self) -> usize {
        size_of::<Self>()
            + self.sketch.as_ref().map_or(0, Sketch::heap_size)
            + operation_size(&self.operation)
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(
            self.sketch.as_mut().map(Sketch::encode).transpose()?,
        )])
    }
    fn merge_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for value in binaries(&values[0])?.iter().flatten() {
            merge(&mut self.sketch, &Sketch::decode(value)?)?;
        }
        Ok(())
    }
}

fn operation_size(operation: &Operation) -> usize {
    if let Operation::Quantile { ranks, .. } = operation {
        ranks.capacity() * size_of::<u64>()
    } else {
        0
    }
}

struct Groups {
    operation: Operation,
    states: Vec<Option<Sketch>>,
    heap_bytes: usize,
}
impl Groups {
    fn resize(&mut self, count: usize) {
        while self.states.len() < count {
            self.states.push(self.operation.initial());
        }
    }
    fn modify(
        &mut self,
        index: usize,
        update: impl FnOnce(&mut Option<Sketch>) -> Result<()>,
    ) -> Result<()> {
        let state = self
            .states
            .get_mut(index)
            .ok_or_else(|| error("invalid t-digest group index"))?;
        let before = state.as_ref().map_or(0, Sketch::heap_size);
        let result = update(state);
        self.heap_bytes = self.heap_bytes - before + state.as_ref().map_or(0, Sketch::heap_size);
        result
    }
    fn take(&mut self, emit: EmitTo) -> Vec<Option<Sketch>> {
        let states = emit.take_needed(&mut self.states);
        self.heap_bytes -= states
            .iter()
            .flatten()
            .map(Sketch::heap_size)
            .sum::<usize>();
        states
    }
}

fn encoded_states(states: &mut [Option<Sketch>]) -> Result<ArrayRef> {
    let mut output = BinaryBuilder::new();
    for state in states {
        if let Some(state) = state {
            output.append_value(state.encode()?);
        } else {
            output.append_null();
        }
    }
    Ok(Arc::new(output.finish()))
}

impl GroupsAccumulator for Groups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        indices: &[usize],
        filter: Option<&BooleanArray>,
        groups: usize,
    ) -> Result<()> {
        if indices.len() != values[0].len()
            || filter.is_some_and(|filter| filter.len() != indices.len())
        {
            return Err(error("invalid grouped t-digest input lengths"));
        }
        self.resize(groups);
        match self.operation {
            Operation::Merge => {
                for (row, value) in strings(&values[0])?.iter().enumerate() {
                    if filter.is_some_and(|filter| filter.is_null(row) || !filter.value(row)) {
                        continue;
                    }
                    if let Some(sketch) = value.map(parse).transpose()?.flatten() {
                        self.modify(indices[row], |state| merge(state, &sketch))?;
                    }
                }
                Ok(())
            }
            Operation::Build(kind) | Operation::Quantile { kind, .. } => {
                samples(values, kind, filter, |row, value, weight| {
                    self.modify(indices[row], |state| {
                        state
                            .as_mut()
                            .ok_or_else(|| error("missing grouped t-digest accumulator"))?
                            .insert(value, weight)
                    })
                })
            }
        }
    }
    fn evaluate(&mut self, emit: EmitTo) -> Result<ArrayRef> {
        let states = self.take(emit);
        let values = states
            .into_iter()
            .map(|mut state| self.operation.evaluate(&mut state))
            .collect::<Result<Vec<_>>>()?;
        if values.is_empty() {
            return Ok(datafusion::arrow::array::new_empty_array(
                &self.operation.output_type(),
            ));
        }
        ScalarValue::iter_to_array(values)
    }
    fn state(&mut self, emit: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![encoded_states(&mut self.take(emit))?])
    }
    fn merge_batch(&mut self, values: &[ArrayRef], indices: &[usize], groups: usize) -> Result<()> {
        if indices.len() != values[0].len() {
            return Err(error("invalid grouped t-digest state lengths"));
        }
        self.resize(groups);
        for (row, value) in binaries(&values[0])?.iter().enumerate() {
            if let Some(value) = value {
                let sketch = Sketch::decode(value)?;
                self.modify(indices[row], |state| merge(state, &sketch))?;
            }
        }
        Ok(())
    }
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let mut states = (0..values[0].len())
            .map(|_| self.operation.initial())
            .collect::<Vec<_>>();
        match self.operation {
            Operation::Merge => {
                if filter.is_some_and(|filter| filter.len() != states.len()) {
                    return Err(error("invalid t-digest state filter length"));
                }
                for (row, value) in strings(&values[0])?.iter().enumerate() {
                    if filter.is_some_and(|filter| filter.is_null(row) || !filter.value(row)) {
                        continue;
                    }
                    states[row] = value.map(parse).transpose()?.flatten();
                }
            }
            Operation::Build(kind) | Operation::Quantile { kind, .. } => {
                samples(values, kind, filter, |row, value, weight| {
                    states[row]
                        .as_mut()
                        .ok_or_else(|| error("missing t-digest row state"))?
                        .insert(value, weight)
                })?
            }
        }
        Ok(vec![encoded_states(&mut states)?])
    }
    fn size(&self) -> usize {
        size_of::<Self>()
            + self.states.capacity() * size_of::<Option<Sketch>>()
            + self.heap_bytes
            + operation_size(&self.operation)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Scalar {
    Merge,
    Percentile(Option<DataType>),
    Array(Option<DataType>),
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Function {
    operation: Scalar,
    signature: Signature,
}

pub(crate) fn scalar(operation: Scalar, values: Vec<Expr>) -> Expr {
    let types = if operation == Scalar::Merge {
        vec![DataType::Utf8; values.len()]
    } else {
        vec![
            DataType::Utf8,
            if matches!(operation, Scalar::Array(_)) {
                dynamic::data_type()
            } else {
                DataType::Float64
            },
        ]
    };
    ScalarUDF::from(Function {
        operation,
        signature: Signature::exact(types, Volatility::Immutable),
    })
    .call(values)
}

impl ScalarUDFImpl for Function {
    fn name(&self) -> &str {
        match self.operation {
            Scalar::Merge => "__kql_tdigest_merge_scalar",
            Scalar::Percentile(_) => "__kql_percentile_tdigest",
            Scalar::Array(_) => "__kql_percentiles_array_tdigest",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(if let Scalar::Percentile(Some(ty)) = &self.operation {
            ty.clone()
        } else {
            dynamic::data_type()
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        let arrays = args
            .args
            .iter()
            .map(|value| value.to_array(1))
            .collect::<Result<Vec<_>>>()?;
        let row_at = |index: usize, row| {
            if matches!(args.args[index], ColumnarValue::Scalar(_)) {
                0
            } else {
                row
            }
        };
        struct Cached<'a> {
            text: Option<&'a str>,
            sketch: Option<Sketch>,
        }
        let count = if self.operation == Scalar::Merge {
            arrays.len()
        } else {
            1
        };
        let mut cached = (0..count)
            .map(|_| None)
            .collect::<Vec<Option<Cached<'_>>>>();
        let mut last_result: Option<ScalarValue> = None;
        let mut last_rank = None;
        let mut last_array = None;
        let mut output = Vec::with_capacity(rows);
        for row in 0..rows {
            let mut changed = false;
            for index in 0..count {
                let array = strings(&arrays[index])?;
                let at = row_at(index, row);
                let text = (!array.is_null(at)).then(|| array.value(at));
                if cached[index]
                    .as_ref()
                    .is_none_or(|cached| cached.text != text)
                {
                    changed = true;
                    cached[index] = Some(Cached {
                        text,
                        sketch: text.map(parse).transpose()?.flatten(),
                    });
                }
            }
            if self.operation == Scalar::Merge {
                if !changed && let Some(result) = &last_result {
                    output.push(result.clone());
                    continue;
                }
                let mut merged = None;
                for entry in &cached {
                    let entry = entry
                        .as_ref()
                        .ok_or_else(|| error("missing t-digest scalar cache"))?;
                    if let Some(sketch) = &entry.sketch {
                        merge(&mut merged, sketch)?;
                    }
                }
                let result = dynamic::from_node(&match merged {
                    Some(mut sketch) => envelope(&mut sketch)?,
                    None => Node::Null,
                })?;
                last_result = Some(result.clone());
                output.push(result);
                continue;
            }
            let sketch = &mut cached[0]
                .as_mut()
                .ok_or_else(|| error("missing t-digest scalar value"))?
                .sketch;
            if let Scalar::Array(ty) = &self.operation {
                let ranks = dynamic::node_at(&arrays[1], row_at(1, row))?;
                if !changed
                    && last_array.as_ref() == Some(&ranks)
                    && let Some(result) = &last_result
                {
                    output.push(result.clone());
                    continue;
                }
                last_array = Some(ranks.clone());
                let Node::Array(ranks) = ranks else {
                    if ranks == Node::Null {
                        let result = dynamic::from_node(&Node::Null)?;
                        last_result = Some(result.clone());
                        output.push(result);
                        continue;
                    }
                    return Err(error(
                        "percentiles_array_tdigest requires an array of percentile ranks",
                    ));
                };
                if ranks.is_empty() || ranks.len() > 1024 {
                    return Err(error("t-digest requires 1-1024 percentile ranks"));
                }
                let mut values = Vec::with_capacity(ranks.len());
                for rank in ranks {
                    let rank = match rank {
                        Node::Long(value) => value as f64,
                        Node::Real(value) => value,
                        _ => return Err(error("t-digest percentile ranks must be numbers")),
                    };
                    validate_rank(rank)?;
                    values.push(match sketch {
                        Some(sketch) => {
                            let value = sketch.percentile(rank)?;
                            if let Some(ty) = ty {
                                match typed(sketch.kind, value, ty)? {
                                    ScalarValue::Int32(value) => value
                                        .map_or(Node::Null, |value| Node::Long(i64::from(value))),
                                    ScalarValue::Int64(value) => {
                                        value.map_or(Node::Null, Node::Long)
                                    }
                                    ScalarValue::Float64(value) => {
                                        value.map_or(Node::Null, Node::Real)
                                    }
                                    ScalarValue::TimestampNanosecond(value, _) => {
                                        value.map_or(Node::Null, Node::Datetime)
                                    }
                                    ScalarValue::DurationNanosecond(value) => {
                                        value.map_or(Node::Null, Node::Timespan)
                                    }
                                    _ => {
                                        return Err(error(
                                            "unsupported typed t-digest array result",
                                        ));
                                    }
                                }
                            } else {
                                sketch.kind.node(value)
                            }
                        }
                        None => Node::Null,
                    });
                }
                let result =
                    dynamic::from_node(&if sketch.as_ref().is_none_or(Sketch::is_empty) {
                        Node::Null
                    } else {
                        Node::Array(values)
                    })?;
                last_result = Some(result.clone());
                output.push(result);
            } else {
                let ranks = arrays[1]
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| error("t-digest percentile requires a real rank"))?;
                let at = row_at(1, row);
                let rank = (!ranks.is_null(at)).then(|| ranks.value(at));
                let key = rank.map(f64::to_bits);
                if !changed
                    && last_rank == Some(key)
                    && let Some(result) = &last_result
                {
                    output.push(result.clone());
                    continue;
                }
                last_rank = Some(key);
                if let Some(rank) = rank {
                    validate_rank(rank)?;
                }
                let value = match (sketch.as_mut(), rank) {
                    (Some(sketch), Some(rank)) => Some((sketch.kind, sketch.percentile(rank)?)),
                    _ => None,
                };
                let result = if let Scalar::Percentile(Some(ty)) = &self.operation {
                    if let Some((kind, value)) = value {
                        typed(kind, value, ty)?
                    } else {
                        ScalarValue::try_from(ty)?
                    }
                } else {
                    dynamic::from_node(&value.map_or(Node::Null, |(kind, value)| kind.node(value)))?
                };
                last_result = Some(result.clone());
                output.push(result);
            }
        }
        let output = if output.is_empty() {
            datafusion::arrow::array::new_empty_array(&self.return_type(&[])?)
        } else {
            ScalarValue::iter_to_array(output)?
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &output, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}

fn typed(kind: Kind, value: Option<u64>, ty: &DataType) -> Result<ScalarValue> {
    let expected = Kind::from_type(ty).ok_or_else(|| error("unsupported typed t-digest result"))?;
    if expected != kind {
        return Err(error(
            "typed percentile_tdigest result must match the sketch's logical type",
        ));
    }
    kind.scalar(value).cast_to(ty)
}

fn validate_rank(rank: f64) -> Result<()> {
    if !rank.is_finite() || !(0.0..=100.0).contains(&rank) {
        return Err(error(
            "t-digest percentile must be finite and between 0 and 100",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn groups(operation: Operation) -> Groups {
        Groups {
            operation,
            states: Vec::new(),
            heap_bytes: 0,
        }
    }

    fn accounted(groups: &Groups) {
        assert_eq!(
            groups.heap_bytes,
            groups
                .states
                .iter()
                .flatten()
                .map(Sketch::heap_size)
                .sum::<usize>()
        );
    }

    #[test]
    fn native_filters_prefix_emission_bypass_and_merge_preserve_group_state() {
        for operation in [
            Operation::Build(Kind::Long),
            Operation::Quantile {
                kind: Kind::Long,
                ranks: vec![50.0_f64.to_bits()],
                output: QuantileOutput::Single,
            },
        ] {
            let values = vec![
                Arc::new(Int64Array::from(vec![1, 100, 2, 200])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, -2, 3, 4])) as _,
            ];
            let filter = BooleanArray::from(vec![Some(true), None, Some(true), Some(false)]);
            let mut direct = groups(operation.clone());
            direct
                .update_batch(&values, &[0, 1, 0, 1], Some(&filter), 2)
                .unwrap();
            accounted(&direct);
            let mut bypass = groups(operation.clone());
            let states = bypass.convert_to_state(&values, Some(&filter)).unwrap();
            bypass.merge_batch(&states, &[0, 1, 0, 1], 2).unwrap();
            accounted(&bypass);
            for groups in [&mut direct, &mut bypass] {
                let first = groups.state(EmitTo::First(1)).unwrap();
                let mut sketch = Sketch::decode(binaries(&first[0]).unwrap().value(0)).unwrap();
                assert_eq!(sketch.percentile(50.0).unwrap(), Some(2));
                accounted(groups);
                groups
                    .update_batch(
                        &[
                            Arc::new(Int64Array::from(vec![300])),
                            Arc::new(Int64Array::from(vec![1])),
                        ],
                        &[0],
                        None,
                        1,
                    )
                    .unwrap();
                let last = groups.state(EmitTo::All).unwrap();
                let mut sketch = Sketch::decode(binaries(&last[0]).unwrap().value(0)).unwrap();
                assert_eq!(sketch.percentile(50.0).unwrap(), Some(300));
                accounted(groups);
                assert_eq!(groups.heap_bytes, 0);
                assert_eq!(groups.evaluate(EmitTo::All).unwrap().len(), 0);
            }
        }
    }

    #[test]
    fn merge_filters_and_bypass_do_not_decode_excluded_values() {
        let mut sketch = Sketch::new(Kind::Long);
        sketch.insert(7, 1).unwrap();
        let text = format!(
            "{{\"{ENVELOPE}\":\"{}\"}}",
            base64::engine::general_purpose::STANDARD.encode(sketch.encode().unwrap())
        );
        let values = vec![Arc::new(StringArray::from(vec![
            Some(text.as_str()),
            Some("invalid"),
            None,
        ])) as ArrayRef];
        let filter = BooleanArray::from(vec![Some(true), Some(false), None]);
        let mut direct = groups(Operation::Merge);
        direct
            .update_batch(&values, &[0, 1, 1], Some(&filter), 2)
            .unwrap();
        let mut bypass = groups(Operation::Merge);
        bypass
            .merge_batch(
                &bypass.convert_to_state(&values, Some(&filter)).unwrap(),
                &[0, 1, 1],
                2,
            )
            .unwrap();
        for groups in [&mut direct, &mut bypass] {
            accounted(groups);
            let values = groups.state(EmitTo::All).unwrap();
            let values = binaries(&values[0]).unwrap();
            let mut sketch = Sketch::decode(values.value(0)).unwrap();
            assert_eq!(sketch.percentile(50.0).unwrap(), Some(7));
            assert!(values.is_null(1));
            accounted(groups);
        }
    }

    #[test]
    fn native_scalar_and_grouped_states_have_consistent_bounded_memory_accounting() {
        let operation = Operation::Quantile {
            kind: Kind::Long,
            ranks: vec![0.0_f64.to_bits(), 50.0_f64.to_bits(), 100.0_f64.to_bits()],
            output: QuantileOutput::Array,
        };
        let mut state = State {
            operation: operation.clone(),
            sketch: operation.initial(),
        };
        let mut groups = groups(operation.clone());
        let values = vec![
            Arc::new(Int64Array::from_iter_values(0..100_000)) as ArrayRef,
            Arc::new(Int64Array::from(vec![1; 100_000])) as _,
        ];
        state.update_batch(&values).unwrap();
        groups
            .update_batch(&values, &vec![0; 100_000], None, 1)
            .unwrap();
        accounted(&groups);
        assert!(state.size() < 101_000);
        assert!(groups.size() < 101_000);
        let mut restored = State {
            operation: operation.clone(),
            sketch: operation.initial(),
        };
        restored
            .merge_batch(&[state.state().unwrap()[0].to_array().unwrap()])
            .unwrap();
        assert_eq!(restored.evaluate().unwrap(), state.evaluate().unwrap());
        let output = groups.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            ScalarValue::try_from_array(&output, 0).unwrap(),
            state.evaluate().unwrap()
        );
        accounted(&groups);
    }
}
