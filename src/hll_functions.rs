use std::collections::BTreeMap;
use std::hash::BuildHasher;
use std::mem::size_of;
use std::sync::Arc;

use base64::Engine;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Int64Builder, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::hash_utils::{HLL_RANDOM_STATE, with_hashes};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, EmitTo, Expr, GroupsAccumulator,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::dynamic;
use crate::dynamic_value::Node;
use crate::hll::{MAX_BYTES, Sketch};

const ENVELOPE: &str = "__datafusion_kql_hll_v1";

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn envelope(sketch: &Sketch) -> Node {
    Node::Bag(BTreeMap::from([(
        ENVELOPE.to_owned(),
        Node::String(base64::engine::general_purpose::STANDARD.encode(sketch.encode())),
    )]))
}

fn parse(text: &str) -> Result<Option<Sketch>> {
    if text.len() > 1 << 20 {
        return Err(error("HLL JSON input exceeds its 1 MiB byte budget"));
    }
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|cause| error(format!("invalid HLL JSON: {cause}")))?;
    if value.is_null() {
        return Ok(None);
    }
    let text = value
        .as_object()
        .filter(|object| object.len() == 1)
        .and_then(|object| object.get(ENVELOPE))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            error("expected a versioned datafusion-kql HLL sketch, not a Kusto-service sketch")
        })?;
    if text.len() > MAX_BYTES.div_ceil(3) * 4 {
        return Err(error("encoded HLL sketch exceeds its byte budget"));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(text)
        .map_err(|cause| error(format!("invalid HLL base64: {cause}")))?;
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

fn strings(array: &ArrayRef) -> Result<&StringArray> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| error("HLL sketch input must be UTF-8 encoded JSON"))
}

fn binaries(array: &ArrayRef) -> Result<&BinaryArray> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| error("invalid native HLL aggregate state"))
}

fn visit_hashes(
    value: &ArrayRef,
    filter: Option<&BooleanArray>,
    domain: u64,
    mut visit: impl FnMut(usize, u64) -> Result<()>,
) -> Result<()> {
    if filter.is_some_and(|filter| filter.len() != value.len()) {
        return Err(error(
            "HLL aggregate filter length does not match its input",
        ));
    }
    let nulls = value.logical_nulls();
    with_hashes([value], &HLL_RANDOM_STATE, |hashes| {
        for (row, hash) in hashes.iter().enumerate() {
            if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row))
                || filter.is_some_and(|filter| filter.is_null(row) || !filter.value(row))
            {
                continue;
            }
            visit(row, hash ^ domain)?;
        }
        Ok(())
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Kind {
    Count(u8),
    Build(u8),
    Merge,
}

impl Kind {
    fn initial(self) -> Result<Option<Sketch>> {
        match self {
            Self::Count(accuracy) | Self::Build(accuracy) => Sketch::new(accuracy).map(Some),
            Self::Merge => Ok(None),
        }
    }

    fn output_type(self) -> DataType {
        if matches!(self, Self::Count(_)) {
            DataType::Int64
        } else {
            dynamic::data_type()
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Aggregate {
    kind: Kind,
    domain: u64,
    signature: Signature,
}

pub(crate) fn aggregate(kind: Kind, value: Expr, domain: &str) -> Expr {
    AggregateUDF::from(Aggregate {
        kind,
        domain: HLL_RANDOM_STATE.hash_one(domain),
        signature: Signature::any(1, Volatility::Immutable),
    })
    .call(vec![value])
}

impl AggregateUDFImpl for Aggregate {
    fn name(&self) -> &str {
        match self.kind {
            Kind::Count(_) => "__kql_dcount",
            Kind::Build(_) => "__kql_hll",
            Kind::Merge => "__kql_hll_merge",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.kind.output_type())
    }

    fn is_nullable(&self) -> bool {
        self.kind == Kind::Merge
    }

    fn default_value(&self, _: &DataType) -> Result<ScalarValue> {
        evaluate(self.kind, self.kind.initial()?.as_ref())
    }

    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(State {
            kind: self.kind,
            domain: self.domain,
            sketch: self.kind.initial()?,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}[hll]", args.name),
            DataType::Binary,
            true,
        ))])
    }

    fn groups_accumulator_supported(&self, _: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(Groups {
            kind: self.kind,
            domain: self.domain,
            states: Vec::new(),
            heap_bytes: 0,
        }))
    }
}

fn evaluate(kind: Kind, sketch: Option<&Sketch>) -> Result<ScalarValue> {
    if matches!(kind, Kind::Count(_)) {
        Ok(ScalarValue::Int64(Some(
            sketch.map_or(Ok(0), Sketch::count)?,
        )))
    } else {
        dynamic::from_node(&sketch.map(envelope).unwrap_or(Node::Null))
    }
}

#[derive(Debug)]
struct State {
    kind: Kind,
    domain: u64,
    sketch: Option<Sketch>,
}

impl Accumulator for State {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if self.kind == Kind::Merge {
            for value in strings(&values[0])?.iter().flatten() {
                if let Some(sketch) = parse(value)? {
                    merge(&mut self.sketch, &sketch)?;
                }
            }
            Ok(())
        } else {
            let sketch = self
                .sketch
                .as_mut()
                .ok_or_else(|| error("missing HLL accumulator"))?;
            visit_hashes(&values[0], None, self.domain, |_, hash| {
                sketch.insert(hash);
                Ok(())
            })
        }
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        evaluate(self.kind, self.sketch.as_ref())
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.sketch.as_ref().map_or(0, Sketch::heap_size)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(
            self.sketch.as_ref().map(Sketch::encode),
        )])
    }

    fn merge_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for value in binaries(&values[0])?.iter().flatten() {
            merge(&mut self.sketch, &Sketch::decode(value)?)?;
        }
        Ok(())
    }
}

struct Groups {
    kind: Kind,
    domain: u64,
    states: Vec<Option<Sketch>>,
    heap_bytes: usize,
}

impl Groups {
    fn resize(&mut self, groups: usize) -> Result<()> {
        while self.states.len() < groups {
            self.states.push(self.kind.initial()?);
        }
        Ok(())
    }

    fn update(
        &mut self,
        index: usize,
        update: impl FnOnce(&mut Option<Sketch>) -> Result<()>,
    ) -> Result<()> {
        let state = self
            .states
            .get_mut(index)
            .ok_or_else(|| error("invalid native HLL group index"))?;
        let before = state.as_ref().map_or(0, Sketch::heap_size);
        let result = update(state);
        self.heap_bytes -= before;
        self.heap_bytes += state.as_ref().map_or(0, Sketch::heap_size);
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

fn encode_states(states: &[Option<Sketch>]) -> ArrayRef {
    let mut output = BinaryBuilder::new();
    for state in states {
        if let Some(state) = state {
            output.append_value(state.encode());
        } else {
            output.append_null();
        }
    }
    Arc::new(output.finish())
}

impl GroupsAccumulator for Groups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        indices: &[usize],
        filter: Option<&BooleanArray>,
        groups: usize,
    ) -> Result<()> {
        if indices.len() != values[0].len() || filter.is_some_and(|f| f.len() != indices.len()) {
            return Err(error("invalid native HLL grouped input lengths"));
        }
        self.resize(groups)?;
        if self.kind == Kind::Merge {
            for (row, value) in strings(&values[0])?.iter().enumerate() {
                if filter.is_some_and(|filter| filter.is_null(row) || !filter.value(row)) {
                    continue;
                }
                if let Some(sketch) = value.map(parse).transpose()?.flatten() {
                    self.update(indices[row], |target| merge(target, &sketch))?;
                }
            }
            Ok(())
        } else {
            visit_hashes(&values[0], filter, self.domain, |row, hash| {
                self.update(indices[row], |state| {
                    state
                        .as_mut()
                        .ok_or_else(|| error("missing grouped HLL accumulator"))?
                        .insert(hash);
                    Ok(())
                })
            })
        }
    }

    fn evaluate(&mut self, emit: EmitTo) -> Result<ArrayRef> {
        let states = self.take(emit);
        if matches!(self.kind, Kind::Count(_)) {
            let mut output = Int64Builder::with_capacity(states.len());
            for state in states {
                output.append_value(state.as_ref().map_or(Ok(0), Sketch::count)?);
            }
            Ok(Arc::new(output.finish()))
        } else {
            dynamic::from_node_rows(
                states
                    .iter()
                    .map(|state| Ok(state.as_ref().map(envelope).unwrap_or(Node::Null))),
                states.len(),
            )
        }
    }

    fn state(&mut self, emit: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![encode_states(&self.take(emit))])
    }

    fn merge_batch(&mut self, values: &[ArrayRef], indices: &[usize], groups: usize) -> Result<()> {
        if indices.len() != values[0].len() {
            return Err(error("invalid native HLL grouped state length"));
        }
        self.resize(groups)?;
        for (row, value) in binaries(&values[0])?.iter().enumerate() {
            if let Some(value) = value {
                let sketch = Sketch::decode(value)?;
                self.update(indices[row], |target| merge(target, &sketch))?;
            }
        }
        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let mut states = Vec::with_capacity(values[0].len());
        for _ in 0..values[0].len() {
            states.push(self.kind.initial()?);
        }
        if self.kind == Kind::Merge {
            if filter.is_some_and(|filter| filter.len() != states.len()) {
                return Err(error("invalid HLL state conversion filter length"));
            }
            for (row, value) in strings(&values[0])?.iter().enumerate() {
                if filter.is_some_and(|filter| filter.is_null(row) || !filter.value(row)) {
                    continue;
                }
                states[row] = value.map(parse).transpose()?.flatten();
            }
        } else {
            visit_hashes(&values[0], filter, self.domain, |row, hash| {
                states[row]
                    .as_mut()
                    .ok_or_else(|| error("missing HLL row state"))?
                    .insert(hash);
                Ok(())
            })?;
        }
        Ok(vec![encode_states(&states)])
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.states.capacity() * size_of::<Option<Sketch>>() + self.heap_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Scalar {
    Count,
    Merge,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Function {
    op: Scalar,
    signature: Signature,
}

pub(crate) fn scalar(op: Scalar, values: Vec<Expr>) -> Expr {
    ScalarUDF::from(Function {
        op,
        signature: Signature::exact(vec![DataType::Utf8; values.len()], Volatility::Immutable),
    })
    .call(values)
}

impl ScalarUDFImpl for Function {
    fn name(&self) -> &str {
        match self.op {
            Scalar::Count => "__kql_dcount_hll",
            Scalar::Merge => "__kql_hll_merge_scalar",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if self.op == Scalar::Count && args.len() != 1
            || self.op == Scalar::Merge && !(2..=64).contains(&args.len())
        {
            return Err(error(
                "dcount_hll requires one sketch; scalar hll_merge requires 2-64 sketches",
            ));
        }
        Ok(if self.op == Scalar::Count {
            DataType::Int64
        } else {
            dynamic::data_type()
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let arrays = args
            .args
            .iter()
            .map(|value| value.to_array(1))
            .collect::<Result<Vec<_>>>()?;
        let arrays = arrays.iter().map(strings).collect::<Result<Vec<_>>>()?;
        struct Cached<'a> {
            text: Option<&'a str>,
            sketch: Option<Sketch>,
            count: Option<i64>,
        }
        let mut cached = (0..arrays.len())
            .map(|_| None)
            .collect::<Vec<Option<Cached<'_>>>>();
        let count = if scalar { 1 } else { args.number_rows };
        let mut counts = Int64Builder::with_capacity(count);
        let mut output = Vec::new();
        for row in 0..count {
            let mut merged = None;
            for (index, array) in arrays.iter().enumerate() {
                let input_row = if matches!(args.args[index], ColumnarValue::Scalar(_)) {
                    0
                } else {
                    row
                };
                let text = (!array.is_null(input_row)).then(|| array.value(input_row));
                if cached[index]
                    .as_ref()
                    .is_none_or(|cached| cached.text != text)
                {
                    let sketch = text.map(parse).transpose()?.flatten();
                    let estimate = if self.op == Scalar::Count {
                        sketch.as_ref().map(Sketch::count).transpose()?
                    } else {
                        None
                    };
                    cached[index] = Some(Cached {
                        text,
                        sketch,
                        count: estimate,
                    });
                }
                let cached = cached[index]
                    .as_ref()
                    .ok_or_else(|| error("missing HLL scalar cache entry"))?;
                if self.op == Scalar::Count {
                    counts.append_option(cached.count);
                } else if let Some(sketch) = &cached.sketch {
                    merge(&mut merged, sketch)?;
                }
            }
            if self.op == Scalar::Merge {
                output.push(merged.as_ref().map(envelope).unwrap_or(Node::Null));
            }
        }
        let output: ArrayRef = if self.op == Scalar::Count {
            Arc::new(counts.finish())
        } else {
            dynamic::from_node_rows(output.into_iter().map(Ok), count)?
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;

    use super::*;

    fn groups(kind: Kind) -> Groups {
        Groups {
            kind,
            domain: 0,
            states: Vec::new(),
            heap_bytes: 0,
        }
    }

    #[test]
    fn grouped_emission_reindexes_remaining_states_and_releases_owned_memory() {
        let mut groups = groups(Kind::Count(1));
        let values: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(3),
            None,
        ]));
        let filter =
            BooleanArray::from(vec![Some(true), Some(false), Some(true), None, Some(true)]);
        groups
            .update_batch(&[values], &[0, 0, 1, 1, 2], Some(&filter), 3)
            .unwrap();
        assert!(groups.heap_bytes > 0);
        let first = groups.evaluate(EmitTo::First(1)).unwrap();
        assert_eq!(
            first
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1]
        );
        let more: ArrayRef = Arc::new(Int64Array::from(vec![3, 4, 5]));
        groups.update_batch(&[more], &[0, 0, 1], None, 2).unwrap();
        let values = groups.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[2, 1]
        );
        assert_eq!(groups.heap_bytes, 0);
        assert!(groups.states.is_empty());
        assert_eq!(groups.size(), size_of::<Groups>());
    }

    #[test]
    fn grouped_state_conversion_and_partial_merge_apply_filters_once() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            None,
        ]));
        let filter =
            BooleanArray::from(vec![Some(true), None, Some(true), Some(false), Some(true)]);
        let indices = [0, 0, 1, 1, 2];
        let mut direct = groups(Kind::Count(2));
        direct
            .update_batch(&[Arc::clone(&values)], &indices, Some(&filter), 3)
            .unwrap();
        let converted = direct.convert_to_state(&[values], Some(&filter)).unwrap();
        let mut merged = groups(Kind::Count(2));
        merged.merge_batch(&converted, &indices, 3).unwrap();
        let direct_states = direct.state(EmitTo::All).unwrap();
        let merged_states = merged.state(EmitTo::All).unwrap();
        assert_eq!(direct_states[0].to_data(), merged_states[0].to_data());
        let mut final_state = groups(Kind::Count(2));
        final_state
            .merge_batch(&merged_states, &[0, 1, 2], 3)
            .unwrap();
        let counts = final_state.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            counts
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 1, 0]
        );
        assert_eq!(direct.heap_bytes, 0);
        assert_eq!(merged.heap_bytes, 0);
    }

    #[test]
    fn foreign_json_versions_and_oversized_payloads_fail_explicitly() {
        for text in [
            r#"[]"#,
            r#"{"__datafusion_kql_hll_v2":"anything"}"#,
            r#"{"__datafusion_kql_hll_v1":"invalid!","extra":1}"#,
            r#"{"__datafusion_kql_hll_v1":12}"#,
        ] {
            assert!(parse(text).is_err(), "{text}");
        }
        let text = serde_json::json!({ENVELOPE:"a".repeat(MAX_BYTES.div_ceil(3)*4+1)}).to_string();
        assert!(
            parse(&text)
                .unwrap_err()
                .to_string()
                .contains("byte budget")
        );
    }
}
