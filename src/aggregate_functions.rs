use std::collections::{BTreeMap, HashSet};
use std::mem::size_of;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Float64Builder, Int64Array, Int64Builder, ListArray,
    ListBuilder, StringArray, StringBuilder, StructArray, UInt64Array, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};
use serde_json::{Map, Value as Json};

use crate::dynamic;
use crate::dynamic_value::Node;

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CollectionKind {
    List,
    ListWithNulls,
    Set,
    Bag,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Collection {
    kind: CollectionKind,
    limit: usize,
    signature: Signature,
}

pub(crate) fn collection(kind: CollectionKind, limit: usize, value: Expr) -> Expr {
    AggregateUDF::from(Collection {
        kind,
        limit,
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
    })
    .call(vec![dynamic::call(
        dynamic::Kernel::EncodeWire,
        vec![value],
    )])
}

impl AggregateUDFImpl for Collection {
    fn name(&self) -> &str {
        match self.kind {
            CollectionKind::List => "__kql_make_list",
            CollectionKind::ListWithNulls => "__kql_make_list_with_nulls",
            CollectionKind::Set => "__kql_make_set",
            CollectionKind::Bag => "__kql_make_bag",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(dynamic::data_type())
    }
    fn is_nullable(&self) -> bool {
        false
    }
    fn default_value(&self, _: &DataType) -> Result<ScalarValue> {
        dynamic::from_json(&if self.kind == CollectionKind::Bag {
            Json::Object(Map::new())
        } else {
            Json::Array(Vec::new())
        })
    }
    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CollectionState {
            kind: self.kind,
            limit: self.limit,
            values: Vec::new(),
            seen: HashSet::new(),
            bag: BTreeMap::new(),
        }))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}[collection]", args.name),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ))])
    }
    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        match self.kind {
            CollectionKind::List | CollectionKind::ListWithNulls => {
                AggregateOrderSensitivity::HardRequirement
            }
            _ => AggregateOrderSensitivity::Insensitive,
        }
    }
}

#[derive(Debug)]
struct CollectionState {
    kind: CollectionKind,
    limit: usize,
    values: Vec<String>,
    seen: HashSet<String>,
    bag: BTreeMap<String, String>,
}

impl CollectionState {
    fn push(&mut self, value: &str) -> Result<()> {
        if value == "jnull" && self.kind != CollectionKind::ListWithNulls {
            return Ok(());
        }
        if self.kind == CollectionKind::Bag {
            if let Node::Bag(bag) = Node::from_wire(value)? {
                for (key, value) in bag {
                    if self.bag.len() == self.limit {
                        break;
                    }
                    if let std::collections::btree_map::Entry::Vacant(entry) = self.bag.entry(key) {
                        entry.insert(value.wire()?);
                    }
                }
            }
        } else if self.values.len() < self.limit
            && (self.kind != CollectionKind::Set || self.seen.insert(value.to_owned()))
        {
            self.values.push(value.to_owned());
        }
        Ok(())
    }
}

impl Accumulator for CollectionState {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| error("collection aggregate expects encoded JSON strings"))?;
        for value in array.iter().flatten() {
            self.push(value)?;
        }
        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = if self.kind == CollectionKind::Bag {
            Node::Bag(
                self.bag
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), Node::from_wire(value)?)))
                    .collect::<Result<BTreeMap<_, _>>>()?,
            )
        } else {
            Node::Array(
                self.values
                    .iter()
                    .map(|value| Node::from_wire(value))
                    .collect::<Result<_>>()?,
            )
        };
        dynamic::from_node(&value)
    }
    fn size(&self) -> usize {
        size_of::<Self>()
            + self.values.capacity() * size_of::<String>()
            + self.values.iter().map(String::capacity).sum::<usize>()
            + self.seen.capacity() * (size_of::<String>() + size_of::<usize>())
            + self.seen.iter().map(String::capacity).sum::<usize>()
            + self
                .bag
                .iter()
                .map(|(key, value)| key.capacity() + value.capacity() + 128)
                .sum::<usize>()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut builder = ListBuilder::new(StringBuilder::new());
        if self.kind == CollectionKind::Bag {
            let mut bag = BTreeMap::new();
            for (key, value) in &self.bag {
                bag.insert(key.clone(), Node::from_wire(value)?);
            }
            builder.values().append_value(Node::Bag(bag).wire()?);
        } else {
            for value in &self.values {
                builder.values().append_value(value);
            }
        }
        builder.append(true);
        Ok(vec![ScalarValue::List(Arc::new(builder.finish()))])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let lists = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| error("collection aggregate expects a list state"))?;
        for list in lists.iter().flatten() {
            self.update_batch(&[list])?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum QuantileOutput {
    Single,
    Columns,
    Array,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Quantile {
    integer: bool,
    percentiles: Vec<u64>,
    output: QuantileOutput,
    signature: Signature,
}

pub(crate) fn quantile(
    integer: bool,
    percentiles: &[f64],
    output: QuantileOutput,
    value: Expr,
    weight: Expr,
) -> Expr {
    AggregateUDF::from(Quantile {
        integer,
        percentiles: percentiles.iter().map(|p| p.to_bits()).collect(),
        output,
        signature: Signature::exact(
            vec![
                if integer {
                    DataType::Int64
                } else {
                    DataType::Float64
                },
                DataType::UInt64,
            ],
            Volatility::Immutable,
        ),
    })
    .call(vec![value, weight])
}

impl Quantile {
    fn sample_type(&self) -> DataType {
        if self.integer {
            DataType::Int64
        } else {
            DataType::Float64
        }
    }
    fn fields(&self) -> Vec<Arc<Field>> {
        (0..self.percentiles.len())
            .map(|i| {
                Arc::new(Field::new(
                    format!("percentile_{i}"),
                    self.sample_type(),
                    true,
                ))
            })
            .collect()
    }
}

impl AggregateUDFImpl for Quantile {
    fn name(&self) -> &str {
        "__kql_nearest_rank"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(match self.output {
            QuantileOutput::Single => self.sample_type(),
            QuantileOutput::Columns => DataType::Struct(self.fields().into()),
            QuantileOutput::Array => dynamic::data_type(),
        })
    }
    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(QuantileState {
            function: self.clone(),
            integers: Vec::new(),
            reals: Vec::new(),
        }))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format!("{}[values]", args.name),
                DataType::List(Arc::new(Field::new("item", self.sample_type(), true))),
                false,
            )),
            Arc::new(Field::new(
                format!("{}[weights]", args.name),
                DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
                false,
            )),
        ])
    }
    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }
}

#[derive(Debug)]
struct QuantileState {
    function: Quantile,
    integers: Vec<(i64, u64)>,
    reals: Vec<(f64, u64)>,
}

impl QuantileState {
    fn selected(&mut self) -> Result<Vec<ScalarValue>> {
        self.integers.sort_unstable_by_key(|(value, _)| *value);
        self.reals.sort_unstable_by(|(a, _), (b, _)| a.total_cmp(b));
        let total = if self.function.integer {
            self.integers
                .iter()
                .map(|(_, weight)| u128::from(*weight))
                .sum::<u128>()
        } else {
            self.reals
                .iter()
                .map(|(_, weight)| u128::from(*weight))
                .sum::<u128>()
        };
        let mut result = Vec::new();
        for percentile in &self.function.percentiles {
            if total == 0 {
                result.push(ScalarValue::try_from(&self.function.sample_type())?);
                continue;
            }
            let rank = ((f64::from_bits(*percentile) / 100.0 * total as f64).ceil() as u128)
                .max(1)
                .min(total);
            let mut accumulated = 0_u128;
            let found = if self.function.integer {
                self.integers.iter().find_map(|(value, weight)| {
                    accumulated += u128::from(*weight);
                    (accumulated >= rank).then_some(ScalarValue::Int64(Some(*value)))
                })
            } else {
                self.reals.iter().find_map(|(value, weight)| {
                    accumulated += u128::from(*weight);
                    (accumulated >= rank).then_some(ScalarValue::Float64(Some(*value)))
                })
            };
            result.push(found.ok_or_else(|| error("invalid quantile rank state"))?);
        }
        Ok(result)
    }
}

impl Accumulator for QuantileState {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let weights = values[1]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| error("percentile weights must be unsigned integers"))?;
        if self.function.integer {
            let samples = values[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| error("integer percentile expects long values"))?;
            self.integers
                .extend(
                    samples
                        .iter()
                        .zip(weights.iter())
                        .filter_map(|(value, weight)| match (value, weight) {
                            (Some(v), Some(w)) if w > 0 => Some((v, w)),
                            _ => None,
                        }),
                );
        } else {
            let samples = values[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| error("real percentile expects real values"))?;
            self.reals
                .extend(
                    samples
                        .iter()
                        .zip(weights.iter())
                        .filter_map(|(value, weight)| match (value, weight) {
                            (Some(v), Some(w)) if w > 0 => Some((v, w)),
                            _ => None,
                        }),
                );
        }
        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let values = self.selected()?;
        match self.function.output {
            QuantileOutput::Single => values
                .into_iter()
                .next()
                .ok_or_else(|| error("percentile requires a rank")),
            QuantileOutput::Columns => {
                let arrays = values
                    .iter()
                    .map(ScalarValue::to_array)
                    .collect::<Result<Vec<_>>>()?;
                Ok(ScalarValue::Struct(Arc::new(StructArray::try_new(
                    self.function.fields().into(),
                    arrays,
                    None,
                )?)))
            }
            QuantileOutput::Array => {
                if self.integers.is_empty() && self.reals.is_empty() {
                    return dynamic::from_json(&Json::Null);
                }
                let json = values
                    .iter()
                    .map(|value| match value {
                        ScalarValue::Int64(Some(value)) => Json::from(*value),
                        ScalarValue::Float64(Some(value)) => Json::from(*value),
                        _ => Json::Null,
                    })
                    .collect();
                dynamic::from_json(&Json::Array(json))
            }
        }
    }
    fn size(&self) -> usize {
        size_of::<Self>()
            + self.integers.capacity() * size_of::<(i64, u64)>()
            + self.reals.capacity() * size_of::<(f64, u64)>()
            + self.function.percentiles.capacity() * size_of::<u64>()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut weights = ListBuilder::new(UInt64Builder::new());
        let values = if self.function.integer {
            let mut values = ListBuilder::new(Int64Builder::new());
            for (value, weight) in &self.integers {
                values.values().append_value(*value);
                weights.values().append_value(*weight);
            }
            values.append(true);
            values.finish()
        } else {
            let mut values = ListBuilder::new(Float64Builder::new());
            for (value, weight) in &self.reals {
                values.values().append_value(*value);
                weights.values().append_value(*weight);
            }
            values.append(true);
            values.finish()
        };
        weights.append(true);
        Ok(vec![
            ScalarValue::List(Arc::new(values)),
            ScalarValue::List(Arc::new(weights.finish())),
        ])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let samples = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| error("percentile expects list states"))?;
        let weights = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| error("percentile expects list states"))?;
        for row in 0..samples.len() {
            self.update_batch(&[samples.value(row), weights.value(row)])?;
        }
        Ok(())
    }
}
