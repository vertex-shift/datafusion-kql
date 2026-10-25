use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, DurationMicrosecondArray,
    DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray,
    FixedSizeListArray, Float64Array, Int64Array, LargeListArray, LargeListViewArray,
    LargeStringArray, ListArray, ListViewArray, MapArray, StringArray, StringViewArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt64Array,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::dynamic_value::{MAX_DEPTH, MAX_NODES, Node, check_size};

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

pub(crate) fn is_collection(ty: &DataType) -> bool {
    if crate::decimal::is_decimal(ty) {
        return false;
    }
    match ty {
        DataType::Dictionary(_, value) => is_collection(value),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Struct(_)
        | DataType::Map(_, _) => true,
        _ => false,
    }
}

fn downcast<T: Array + 'static>(array: &ArrayRef) -> Result<&T> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| error("inconsistent native Arrow dynamic input"))
}

pub(crate) fn validate_nesting(ty: &DataType) -> Result<()> {
    let mut pending = vec![(ty, 0)];
    let mut visited = 0;
    while let Some((ty, depth)) = pending.pop() {
        visited += 1;
        if depth > MAX_DEPTH {
            return Err(error("native Arrow schema exceeds nesting limit"));
        }
        if visited > MAX_NODES {
            return Err(error("native Arrow schema exceeds node budget"));
        }
        match ty {
            DataType::Dictionary(key, value) => {
                pending.push((key, depth + 1));
                pending.push((value, depth + 1));
            }
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::Map(field, _) => pending.push((field.data_type(), depth + 1)),
            DataType::Struct(fields) => {
                check_size(fields.len())?;
                pending.extend(fields.iter().map(|field| (field.data_type(), depth + 1)));
            }
            DataType::Union(fields, _) => pending.extend(
                fields
                    .iter()
                    .map(|(_, field)| (field.data_type(), depth + 1)),
            ),
            _ => {}
        }
        if pending.len() > MAX_NODES - visited {
            return Err(error("native Arrow schema exceeds node budget"));
        }
    }
    Ok(())
}

struct Prepared {
    array: ArrayRef,
    children: Vec<Prepared>,
}

impl Prepared {
    fn new(mut array: ArrayRef, depth: usize) -> Result<Self> {
        if depth > MAX_DEPTH {
            return Err(error("native Arrow collection exceeds nesting limit"));
        }
        while let DataType::Dictionary(_, values) = array.data_type() {
            array = cast(&array, values)?;
        }
        let normalized = match array.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32 => Some(DataType::Int64),
            DataType::Float16 | DataType::Float32 => Some(DataType::Float64),
            _ => None,
        };
        if let Some(ty) = normalized {
            array = cast(&array, &ty)?;
        }
        let children = if crate::dynamic::is_dynamic(array.data_type())
            || crate::decimal::is_decimal(array.data_type())
        {
            vec![]
        } else {
            match array.data_type() {
                DataType::List(_) => vec![Arc::clone(downcast::<ListArray>(&array)?.values())],
                DataType::LargeList(_) => {
                    vec![Arc::clone(downcast::<LargeListArray>(&array)?.values())]
                }
                DataType::ListView(_) => {
                    vec![Arc::clone(downcast::<ListViewArray>(&array)?.values())]
                }
                DataType::LargeListView(_) => {
                    vec![Arc::clone(downcast::<LargeListViewArray>(&array)?.values())]
                }
                DataType::FixedSizeList(_, _) => {
                    vec![Arc::clone(downcast::<FixedSizeListArray>(&array)?.values())]
                }
                DataType::Struct(fields) => {
                    check_size(fields.len())?;
                    let mut names = HashSet::new();
                    for field in fields {
                        if !names.insert(field.name()) {
                            return Err(error("native Arrow struct has duplicate property names"));
                        }
                    }
                    downcast::<StructArray>(&array)?.columns().to_vec()
                }
                DataType::Map(_, _) => {
                    let map = downcast::<MapArray>(&array)?;
                    let mut key_type = map.keys().data_type();
                    while let DataType::Dictionary(_, value) = key_type {
                        key_type = value;
                    }
                    if !matches!(
                        key_type,
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    ) {
                        return Err(error("native Arrow dynamic maps require string keys"));
                    }
                    vec![Arc::clone(map.keys()), Arc::clone(map.values())]
                }
                DataType::Null
                | DataType::Boolean
                | DataType::Int64
                | DataType::UInt64
                | DataType::Float64
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Timestamp(_, _)
                | DataType::Duration(_)
                | DataType::Date32
                | DataType::Date64
                | DataType::FixedSizeBinary(16) => vec![],
                ty => {
                    return Err(error(format!(
                        "cannot preserve Arrow {ty} inside a dynamic collection"
                    )));
                }
            }
        };
        let children = children
            .into_iter()
            .map(|array| Self::new(array, depth + 1))
            .collect::<Result<_>>()?;
        Ok(Self { array, children })
    }

    fn list(&self, start: usize, length: usize, depth: usize, budget: &mut Budget) -> Result<Node> {
        check_size(length)?;
        budget.reserve(length)?;
        let child = &self.children[0];
        Ok(Node::Array(
            (start..start + length)
                .map(|row| child.read(row, depth + 1, budget))
                .collect::<Result<_>>()?,
        ))
    }

    fn read(&self, row: usize, depth: usize, budget: &mut Budget) -> Result<Node> {
        budget.enter(depth)?;
        if self.array.is_null(row) {
            return Ok(Node::Null);
        }
        if crate::dynamic::is_dynamic(self.array.data_type()) {
            let node = crate::dynamic::node_at(&self.array, row)?;
            budget.existing(&node, depth)?;
            return Ok(node);
        }
        if crate::decimal::is_decimal(self.array.data_type()) {
            return Ok(crate::decimal::value(&self.array, row)?.map_or(Node::Null, Node::Decimal));
        }
        Ok(match self.array.data_type() {
            DataType::Null => Node::Null,
            DataType::Boolean => Node::Bool(downcast::<BooleanArray>(&self.array)?.value(row)),
            DataType::Int64 => Node::Long(downcast::<Int64Array>(&self.array)?.value(row)),
            DataType::UInt64 => Node::Long(
                i64::try_from(downcast::<UInt64Array>(&self.array)?.value(row))
                    .map_err(|_| error("unsigned Arrow value exceeds the KQL long range"))?,
            ),
            DataType::Float64 => Node::Real(downcast::<Float64Array>(&self.array)?.value(row)),
            DataType::Utf8 => budget.text(downcast::<StringArray>(&self.array)?.value(row))?,
            DataType::LargeUtf8 => {
                budget.text(downcast::<LargeStringArray>(&self.array)?.value(row))?
            }
            DataType::Utf8View => {
                budget.text(downcast::<StringViewArray>(&self.array)?.value(row))?
            }
            DataType::List(_) => {
                let values = downcast::<ListArray>(&self.array)?;
                let offsets = values.value_offsets();
                return self.list(
                    offsets[row] as usize,
                    (offsets[row + 1] - offsets[row]) as usize,
                    depth,
                    budget,
                );
            }
            DataType::LargeList(_) => {
                let values = downcast::<LargeListArray>(&self.array)?;
                let offsets = values.value_offsets();
                return self.list(
                    offsets[row] as usize,
                    (offsets[row + 1] - offsets[row]) as usize,
                    depth,
                    budget,
                );
            }
            DataType::ListView(_) => {
                let values = downcast::<ListViewArray>(&self.array)?;
                return self.list(
                    values.value_offsets()[row] as usize,
                    values.value_sizes()[row] as usize,
                    depth,
                    budget,
                );
            }
            DataType::LargeListView(_) => {
                let values = downcast::<LargeListViewArray>(&self.array)?;
                return self.list(
                    values.value_offsets()[row] as usize,
                    values.value_sizes()[row] as usize,
                    depth,
                    budget,
                );
            }
            DataType::FixedSizeList(_, _) => {
                let values = downcast::<FixedSizeListArray>(&self.array)?;
                return self.list(
                    values.value_offset(row) as usize,
                    values.value_length() as usize,
                    depth,
                    budget,
                );
            }
            DataType::Struct(fields) => {
                check_size(fields.len())?;
                budget.reserve(fields.len())?;
                let mut bag = BTreeMap::new();
                for (field, child) in fields.iter().zip(&self.children) {
                    budget.bytes(field.name().len())?;
                    bag.insert(field.name().clone(), child.read(row, depth + 1, budget)?);
                }
                Node::Bag(bag)
            }
            DataType::Map(_, _) => {
                let values = downcast::<MapArray>(&self.array)?;
                let offsets = values.value_offsets();
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                check_size(end - start)?;
                budget.reserve((end - start).saturating_mul(2))?;
                let mut bag = BTreeMap::new();
                for index in start..end {
                    let Node::String(key) = self.children[0].read(index, depth + 1, budget)? else {
                        return Err(error("native Arrow map contains a null key"));
                    };
                    if bag.contains_key(&key) {
                        return Err(error("native Arrow map contains duplicate keys"));
                    }
                    bag.insert(key, self.children[1].read(index, depth + 1, budget)?);
                }
                Node::Bag(bag)
            }
            DataType::Timestamp(unit, _) => Node::Datetime(match unit {
                TimeUnit::Second => nanos(
                    downcast::<TimestampSecondArray>(&self.array)?.value(row),
                    1_000_000_000,
                )?,
                TimeUnit::Millisecond => nanos(
                    downcast::<TimestampMillisecondArray>(&self.array)?.value(row),
                    1_000_000,
                )?,
                TimeUnit::Microsecond => nanos(
                    downcast::<TimestampMicrosecondArray>(&self.array)?.value(row),
                    1_000,
                )?,
                TimeUnit::Nanosecond => {
                    downcast::<TimestampNanosecondArray>(&self.array)?.value(row)
                }
            }),
            DataType::Duration(unit) => Node::Timespan(match unit {
                TimeUnit::Second => nanos(
                    downcast::<DurationSecondArray>(&self.array)?.value(row),
                    1_000_000_000,
                )?,
                TimeUnit::Millisecond => nanos(
                    downcast::<DurationMillisecondArray>(&self.array)?.value(row),
                    1_000_000,
                )?,
                TimeUnit::Microsecond => nanos(
                    downcast::<DurationMicrosecondArray>(&self.array)?.value(row),
                    1_000,
                )?,
                TimeUnit::Nanosecond => {
                    downcast::<DurationNanosecondArray>(&self.array)?.value(row)
                }
            }),
            DataType::Date32 => Node::Datetime(nanos(
                i64::from(downcast::<Date32Array>(&self.array)?.value(row)),
                86_400_000_000_000,
            )?),
            DataType::Date64 => Node::Datetime(nanos(
                downcast::<Date64Array>(&self.array)?.value(row),
                1_000_000,
            )?),
            DataType::FixedSizeBinary(16) => Node::Guid(
                downcast::<FixedSizeBinaryArray>(&self.array)?
                    .value(row)
                    .try_into()
                    .map_err(|_| error("invalid native Arrow GUID width"))?,
            ),
            ty => return Err(error(format!("unsupported native Arrow dynamic type {ty}"))),
        })
    }
}

fn nanos(value: i64, scale: i64) -> Result<i64> {
    value
        .checked_mul(scale)
        .ok_or_else(|| error("native Arrow temporal value exceeds the supported nanosecond range"))
}

#[derive(Default)]
struct Budget {
    nodes: usize,
    bytes: usize,
}
impl Budget {
    fn enter(&mut self, depth: usize) -> Result<()> {
        if depth > MAX_DEPTH {
            return Err(error("native Arrow collection exceeds nesting limit"));
        }
        self.reserve(1)?;
        self.nodes += 1;
        Ok(())
    }
    fn reserve(&self, count: usize) -> Result<()> {
        if count > MAX_NODES - self.nodes {
            return Err(error("native Arrow collection exceeds total node budget"));
        }
        Ok(())
    }
    fn bytes(&mut self, count: usize) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(count)
            .ok_or_else(|| error("native Arrow dynamic text length overflow"))?;
        crate::telemetry_text::bounded(self.bytes)
    }
    fn text(&mut self, text: &str) -> Result<Node> {
        self.bytes(text.len())?;
        Ok(Node::String(text.into()))
    }
    fn existing(&mut self, node: &Node, depth: usize) -> Result<()> {
        match node {
            Node::String(text) => self.bytes(text.len())?,
            Node::Array(items) => {
                for child in items {
                    self.enter(depth + 1)?;
                    self.existing(child, depth + 1)?;
                }
            }
            Node::Bag(items) => {
                for (key, child) in items {
                    self.bytes(key.len())?;
                    self.enter(depth + 1)?;
                    self.existing(child, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

pub(crate) fn convert(array: ArrayRef) -> Result<ArrayRef> {
    validate_nesting(array.data_type())?;
    let values = Prepared::new(array, 0)?;
    crate::dynamic::from_node_rows(
        (0..values.array.len()).map(|row| values.read(row, 0, &mut Budget::default())),
        values.array.len(),
    )
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CheckedList {
    signature: Signature,
    output: DataType,
}

pub(crate) fn typed_list(value: Expr, ty: &DataType, target: &DataType) -> Option<Expr> {
    let field = match ty {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            field
        }
        _ => return None,
    };
    let compatible = matches!(
        (field.data_type(), target),
        (
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            DataType::Int64
        ) | (DataType::Int32, DataType::Int32)
            | (
                DataType::Float16 | DataType::Float32 | DataType::Float64,
                DataType::Float64
            )
            | (DataType::Boolean, DataType::Boolean)
            | (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Utf8
            )
    );
    if !compatible {
        return None;
    }
    Some(
        ScalarUDF::from(CheckedList {
            signature: Signature::exact(vec![ty.clone()], Volatility::Immutable),
            output: DataType::List(Arc::new(datafusion::arrow::datatypes::Field::new(
                "item",
                target.clone(),
                true,
            ))),
        })
        .call(vec![value]),
    )
}

impl ScalarUDFImpl for CheckedList {
    fn name(&self) -> &str {
        "__kql_native_expand_list"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.output.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let array = args.args[0].to_array(1)?;
        for row in 0..array.len() {
            if array.is_null(row) {
                continue;
            }
            let length = match array.data_type() {
                DataType::List(_) => downcast::<ListArray>(&array)?.value_length(row) as usize,
                DataType::LargeList(_) => {
                    downcast::<LargeListArray>(&array)?.value_length(row) as usize
                }
                DataType::FixedSizeList(_, _) => {
                    downcast::<FixedSizeListArray>(&array)?.value_length() as usize
                }
                _ => return Err(error("native expansion expected a list")),
            };
            check_size(length)?;
        }
        let output = cast(&array, &self.output)?;
        if scalar {
            Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::try_from_array(&output, 0)?,
            ))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}
