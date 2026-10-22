use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, DurationNanosecondArray,
    FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, ListArray, NullArray, StringArray, StringBuilder, StructArray,
    TimestampNanosecondArray, UnionArray, new_null_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::{can_cast_types, cast_with_options, interleave};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit, UnionFields, UnionMode};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::{Map, Number, Value as Json};

const NULL: i8 = 0;
const BOOL: i8 = 1;
const LONG: i8 = 2;
const REAL: i8 = 3;
const STRING: i8 = 4;
const ARRAY: i8 = 5;
const BAG: i8 = 6;
const DATETIME: i8 = 7;
const TIMESPAN: i8 = 8;
const GUID: i8 = 9;
pub(crate) const MAX_COLLECTION_SIZE: usize = 1_048_576;

static FIELDS: LazyLock<UnionFields> = LazyLock::new(|| {
    let json = || {
        HashMap::from([
            ("ARROW:extension:name".into(), "arrow.json".into()),
            ("ARROW:extension:metadata".into(), "{}".into()),
        ])
    };
    UnionFields::from_iter([
        (NULL, Arc::new(Field::new("null", DataType::Null, true))),
        (BOOL, Arc::new(Field::new("bool", DataType::Boolean, false))),
        (LONG, Arc::new(Field::new("long", DataType::Int64, false))),
        (REAL, Arc::new(Field::new("real", DataType::Float64, false))),
        (
            STRING,
            Arc::new(Field::new("string", DataType::Utf8, false)),
        ),
        (
            ARRAY,
            Arc::new(Field::new("array", DataType::Utf8, false).with_metadata(json())),
        ),
        (
            BAG,
            Arc::new(Field::new("dictionary", DataType::Utf8, false).with_metadata(json())),
        ),
        (
            DATETIME,
            Arc::new(Field::new(
                "datetime",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )),
        ),
        (
            TIMESPAN,
            Arc::new(Field::new(
                "timespan",
                DataType::Duration(TimeUnit::Nanosecond),
                false,
            )),
        ),
        (
            GUID,
            Arc::new(Field::new("guid", crate::guid::data_type(), false)),
        ),
    ])
});

pub(crate) fn data_type() -> DataType {
    DataType::Union(FIELDS.clone(), UnionMode::Dense)
}

pub(crate) fn is_dynamic(ty: &DataType) -> bool {
    matches!(ty,DataType::Union(fields,UnionMode::Dense)
        if fields.len()>TIMESPAN as usize && fields.len()<=FIELDS.len()
            && fields.iter().zip(FIELDS.iter()).all(|(a,b)|a==b))
}

pub(crate) fn from_json(value: &Json) -> Result<ScalarValue> {
    let mut builder = Builder::new(1);
    builder.append_json(value)?;
    ScalarValue::try_from_array(&builder.finish()?, 0)
}

pub(crate) fn from_json_rows(
    values: impl IntoIterator<Item = Result<Json>>,
    count: usize,
) -> Result<ArrayRef> {
    let mut builder = Builder::new(count);
    for value in values {
        builder.append_json(&value?)?;
    }
    builder.finish()
}

#[derive(Clone, Copy, Debug)]
enum Value<'a> {
    Null,
    Bool(bool),
    Long(i64),
    Real(f64),
    String(&'a str),
    Array(&'a str),
    Bag(&'a str),
    Datetime(i64),
    Timespan(i64),
    Guid([u8; 16]),
}

impl Value<'_> {
    fn json(self) -> Result<Json> {
        Ok(match self {
            Self::Null => Json::Null,
            Self::Bool(value) => Json::Bool(value),
            Self::Long(value) => value.into(),
            Self::Real(value) => Number::from_f64(value)
                .map(Json::Number)
                .unwrap_or_else(|| Json::String(value.to_string())),
            Self::String(value) => value.into(),
            Self::Guid(value) => crate::guid::format(&value).into(),
            Self::Array(value) | Self::Bag(value) => serde_json::from_str(value)
                .map_err(|e| failure(format!("corrupt dynamic JSON: {e}")))?,
            Self::Datetime(value) => chrono::DateTime::from_timestamp_nanos(value)
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
                .into(),
            Self::Timespan(value) => {
                let negative = if value < 0 { "-" } else { "" };
                let value = value.unsigned_abs();
                let base = format!(
                    "{negative}{}.{:02}:{:02}:{:02}",
                    value / 86_400_000_000_000,
                    value / 3_600_000_000_000 % 24,
                    value / 60_000_000_000 % 60,
                    value / 1_000_000_000 % 60,
                );
                if value % 100 == 0 {
                    format!("{base}.{:07}", value % 1_000_000_000 / 100).into()
                } else {
                    format!("{base}.{:09}", value % 1_000_000_000).into()
                }
            }
        })
    }

    fn type_name(self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::Long(_) => "long",
            Self::Real(_) => "real",
            Self::String(_) => "string",
            Self::Array(_) => "array",
            Self::Bag(_) => "dictionary",
            Self::Datetime(_) => "datetime",
            Self::Timespan(_) => "timespan",
            Self::Guid(_) => "guid",
        }
    }
}

struct Builder {
    ids: Vec<i8>,
    offsets: Vec<i32>,
    nulls: usize,
    bools: BooleanBuilder,
    longs: Int64Builder,
    reals: Float64Builder,
    strings: StringBuilder,
    arrays: StringBuilder,
    bags: StringBuilder,
    datetimes: datafusion::arrow::array::TimestampNanosecondBuilder,
    timespans: datafusion::arrow::array::DurationNanosecondBuilder,
    guids: FixedSizeBinaryBuilder,
}

impl Builder {
    fn new(capacity: usize) -> Self {
        Self {
            ids: Vec::with_capacity(capacity),
            offsets: Vec::with_capacity(capacity),
            nulls: 0,
            bools: BooleanBuilder::new(),
            longs: Int64Builder::new(),
            reals: Float64Builder::new(),
            strings: StringBuilder::new(),
            arrays: StringBuilder::new(),
            bags: StringBuilder::new(),
            datetimes: datafusion::arrow::array::TimestampNanosecondBuilder::new(),
            timespans: datafusion::arrow::array::DurationNanosecondBuilder::new(),
            guids: FixedSizeBinaryBuilder::new(16),
        }
    }

    fn append(&mut self, value: Value<'_>) -> Result<()> {
        let (id, offset) = match value {
            Value::Null => {
                self.nulls += 1;
                (NULL, self.nulls - 1)
            }
            Value::Bool(value) => {
                self.bools.append_value(value);
                (BOOL, self.bools.len() - 1)
            }
            Value::Long(value) => {
                self.longs.append_value(value);
                (LONG, self.longs.len() - 1)
            }
            Value::Real(value) => {
                self.reals.append_value(value);
                (REAL, self.reals.len() - 1)
            }
            Value::String(value) => {
                self.strings.append_value(value);
                (STRING, self.strings.len() - 1)
            }
            Value::Array(value) => {
                self.arrays.append_value(value);
                (ARRAY, self.arrays.len() - 1)
            }
            Value::Bag(value) => {
                self.bags.append_value(value);
                (BAG, self.bags.len() - 1)
            }
            Value::Datetime(value) => {
                self.datetimes.append_value(value);
                (DATETIME, self.datetimes.len() - 1)
            }
            Value::Timespan(value) => {
                self.timespans.append_value(value);
                (TIMESPAN, self.timespans.len() - 1)
            }
            Value::Guid(value) => {
                self.guids.append_value(value)?;
                (GUID, self.guids.len() - 1)
            }
        };
        self.ids.push(id);
        self.offsets.push(
            i32::try_from(offset)
                .map_err(|_| failure("dynamic batch exceeds Arrow's offset range"))?,
        );
        Ok(())
    }

    fn append_json(&mut self, value: &Json) -> Result<()> {
        match value {
            Json::Null => self.append(Value::Null),
            Json::Bool(value) => self.append(Value::Bool(*value)),
            Json::Number(value) if value.is_i64() => self.append(Value::Long(
                value
                    .as_i64()
                    .ok_or_else(|| failure("invalid dynamic integer"))?,
            )),
            Json::Number(value) => self.append(Value::Real(
                value
                    .as_f64()
                    .ok_or_else(|| failure("dynamic real is outside f64 range"))?,
            )),
            Json::String(value) => self.append(Value::String(value)),
            Json::Array(_) => self.append(Value::Array(&value.to_string())),
            Json::Object(_) => self.append(Value::Bag(&value.to_string())),
        }
    }

    fn finish(mut self) -> Result<ArrayRef> {
        let children: Vec<ArrayRef> = vec![
            Arc::new(NullArray::new(self.nulls)),
            Arc::new(self.bools.finish()),
            Arc::new(self.longs.finish()),
            Arc::new(self.reals.finish()),
            Arc::new(self.strings.finish()),
            Arc::new(self.arrays.finish()),
            Arc::new(self.bags.finish()),
            Arc::new(self.datetimes.finish()),
            Arc::new(self.timespans.finish()),
            Arc::new(self.guids.finish()),
        ];
        Ok(Arc::new(UnionArray::try_new(
            FIELDS.clone(),
            self.ids.into(),
            Some(self.offsets.into()),
            children,
        )?))
    }
}

struct Input {
    array: ArrayRef,
    scalar: bool,
}

impl Input {
    fn new(value: &ColumnarValue) -> Result<Self> {
        let scalar = matches!(value, ColumnarValue::Scalar(_));
        let mut array = value.to_array(1)?;
        let ty = array.data_type();
        let normalized = match ty {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Some(DataType::Int64),
            DataType::Float16 | DataType::Float32 => Some(DataType::Float64),
            DataType::LargeUtf8 | DataType::Utf8View | DataType::Dictionary(_, _) => {
                Some(DataType::Utf8)
            }
            DataType::Timestamp(_, _) => Some(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            DataType::Duration(_) => Some(DataType::Duration(TimeUnit::Nanosecond)),
            _ => None,
        };
        if let Some(ty) = normalized {
            array = cast_with_options(&array, &ty, &Default::default())?;
        }
        Ok(Self { array, scalar })
    }

    fn value(&self, row: usize) -> Result<Value<'_>> {
        let row = if self.scalar { 0 } else { row };
        if is_dynamic(self.array.data_type()) {
            let union = downcast::<UnionArray>(&self.array)?;
            let id = union.type_id(row);
            let offset = union.value_offset(row);
            return child_value(union.child(id), offset, id);
        }
        if self.array.is_null(row) {
            return Ok(Value::Null);
        }
        Ok(match self.array.data_type() {
            DataType::Null => Value::Null,
            DataType::Boolean => Value::Bool(downcast::<BooleanArray>(&self.array)?.value(row)),
            DataType::Int64 => Value::Long(downcast::<Int64Array>(&self.array)?.value(row)),
            DataType::Float64 => Value::Real(downcast::<Float64Array>(&self.array)?.value(row)),
            DataType::Utf8 => Value::String(downcast::<StringArray>(&self.array)?.value(row)),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Value::Datetime(downcast::<TimestampNanosecondArray>(&self.array)?.value(row))
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                Value::Timespan(downcast::<DurationNanosecondArray>(&self.array)?.value(row))
            }
            DataType::FixedSizeBinary(16) => Value::Guid(
                downcast::<FixedSizeBinaryArray>(&self.array)?
                    .value(row)
                    .try_into()
                    .map_err(|_| failure("invalid guid width"))?,
            ),
            ty => {
                return Err(failure(format!(
                    "cannot use Arrow {ty} as a dynamic scalar"
                )));
            }
        })
    }
}

fn downcast<T: Array + 'static>(array: &dyn Array) -> Result<&T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| failure("dynamic kernel received an inconsistent Arrow array"))
}

fn child_value(array: &ArrayRef, row: usize, id: i8) -> Result<Value<'_>> {
    Ok(match id {
        NULL => Value::Null,
        BOOL => Value::Bool(downcast::<BooleanArray>(array)?.value(row)),
        LONG => Value::Long(downcast::<Int64Array>(array)?.value(row)),
        REAL => Value::Real(downcast::<Float64Array>(array)?.value(row)),
        STRING => Value::String(downcast::<StringArray>(array)?.value(row)),
        ARRAY => Value::Array(downcast::<StringArray>(array)?.value(row)),
        BAG => Value::Bag(downcast::<StringArray>(array)?.value(row)),
        DATETIME => Value::Datetime(downcast::<TimestampNanosecondArray>(array)?.value(row)),
        TIMESPAN => Value::Timespan(downcast::<DurationNanosecondArray>(array)?.value(row)),
        GUID => Value::Guid(
            downcast::<FixedSizeBinaryArray>(array)?
                .value(row)
                .try_into()
                .map_err(|_| failure("invalid guid width"))?,
        ),
        _ => return Err(failure("unknown KQL dynamic type id")),
    })
}

fn failure(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

/// Batch kernels live in expressions, never in the caller's global registry.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Kernel {
    IntoDynamic,
    PackAll {
        names: Vec<String>,
        ignore_empty: bool,
    },
    Unpack {
        keys: Vec<String>,
    },
    TopValues {
        real: bool,
        descending: bool,
        nulls_first: bool,
        expanded_limit: usize,
        count: usize,
    },
    TopReduce(TopReduction),
    ParseJson,
    ParseJsonStrict,
    EncodeJson,
    Coalesce,
    DecodeList {
        limit: usize,
    },
    EncodeWindow,
    DecodeWindow,
    Series(crate::series_functions::Op),
    Get,
    GetType,
    Convert(DataType),
    TypeOnly(DataType),
    Expand {
        array_mode: bool,
        arrays_only: bool,
        limit: usize,
        columns: usize,
    },
    Equal(bool),
    Membership {
        negated: bool,
        insensitive: bool,
    },
    ArrayLength,
    PackArray,
    BagPack,
    BagKeys,
    BagHasKey,
    BagMerge,
    BagSetKey,
    BagRemoveKeys,
    ArrayConcat,
    ArraySlice,
    ArrayIndexOf,
    ArrayHas,
    ArrayReverse,
    ArraySum,
    SetUnion,
    SetIntersect,
    SetDifference,
    Zip,
    Repeat,
    Range,
    StrcatArray,
    Split,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum TopReduction {
    Sum,
    Count,
    List { limit: usize, include_nulls: bool },
}

impl Kernel {
    fn output_type(&self) -> DataType {
        match self {
            Self::Unpack { keys } => DataType::Struct(
                keys.iter()
                    .enumerate()
                    .map(|(i, _)| Arc::new(Field::new(format!("field{i}"), data_type(), true)))
                    .collect(),
            ),
            Self::GetType | Self::StrcatArray | Self::EncodeJson | Self::EncodeWindow => {
                DataType::Utf8
            }
            Self::ArrayLength
            | Self::ArrayIndexOf
            | Self::TopReduce(TopReduction::Sum | TopReduction::Count) => DataType::Int64,
            Self::Equal(_) | Self::BagHasKey | Self::ArrayHas | Self::Membership { .. } => {
                DataType::Boolean
            }
            Self::ArraySum => DataType::Float64,
            Self::Convert(ty) | Self::TypeOnly(ty) => ty.clone(),
            Self::Expand { columns, .. } => DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(expansion_fields(*columns).into()),
                false,
            ))),
            _ => data_type(),
        }
    }

    fn name(&self) -> String {
        format!(
            "__kql_{}",
            match self {
                Self::IntoDynamic => "dynamic",
                Self::PackAll { .. } => "pack_all",
                Self::Unpack { .. } => "bag_unpack",
                Self::TopValues { .. } => "mv_apply_top_values",
                Self::TopReduce(_) => "mv_apply_top_reduce",
                Self::ParseJson => "parse_json",
                Self::ParseJsonStrict => "parse_json_strict",
                Self::EncodeJson => "to_json",
                Self::Coalesce => "dynamic_coalesce",
                Self::DecodeList { .. } => "decode_ordered_list",
                Self::EncodeWindow => "window_encode",
                Self::DecodeWindow => "window_decode",
                Self::Series(op) => op.name(),
                Self::Get => "dynamic_get",
                Self::GetType => "gettype",
                Self::Convert(DataType::Utf8) => "dynamic_tostring",
                Self::Convert(DataType::Int32) => "dynamic_toint",
                Self::Convert(DataType::Int64) => "dynamic_tolong",
                Self::Convert(DataType::Float64) => "dynamic_toreal",
                Self::Convert(DataType::Boolean) => "dynamic_tobool",
                Self::Convert(DataType::Timestamp(_, _)) => "dynamic_todatetime",
                Self::Convert(DataType::Duration(_)) => "dynamic_totimespan",
                Self::Convert(DataType::FixedSizeBinary(16)) => "dynamic_toguid",
                Self::Convert(_) => "dynamic_cast",
                Self::TypeOnly(_) => "dynamic_type_only",
                Self::Expand { .. } => "mv_expand_values",
                Self::Equal(false) => "dynamic_eq",
                Self::Equal(true) => "dynamic_ne",
                Self::Membership { .. } => "dynamic_membership",
                Self::ArrayLength => "array_length",
                Self::PackArray => "pack_array",
                Self::BagPack => "bag_pack",
                Self::BagKeys => "bag_keys",
                Self::BagHasKey => "bag_has_key",
                Self::BagMerge => "bag_merge",
                Self::BagSetKey => "bag_set_key",
                Self::BagRemoveKeys => "bag_remove_keys",
                Self::ArrayConcat => "array_concat",
                Self::ArraySlice => "array_slice",
                Self::ArrayIndexOf => "array_index_of",
                Self::ArrayHas => "set_has_element",
                Self::ArrayReverse => "array_reverse",
                Self::ArraySum => "array_sum",
                Self::SetUnion => "set_union",
                Self::SetIntersect => "set_intersect",
                Self::SetDifference => "set_difference",
                Self::Zip => "zip",
                Self::Repeat => "repeat",
                Self::Range => "range",
                Self::StrcatArray => "strcat_array",
                Self::Split => "split",
            }
        )
    }

    fn validate(&self, len: usize) -> Result<()> {
        if let Self::Series(op) = self {
            return op.validate(len);
        }
        let valid = match self {
            Self::PackAll { names, .. } => len == names.len(),
            Self::IntoDynamic
            | Self::Unpack { .. }
            | Self::TopValues { .. }
            | Self::TopReduce(_)
            | Self::ParseJson
            | Self::ParseJsonStrict
            | Self::EncodeJson
            | Self::DecodeList { .. }
            | Self::EncodeWindow
            | Self::DecodeWindow
            | Self::GetType
            | Self::Convert(_)
            | Self::TypeOnly(_)
            | Self::ArrayLength
            | Self::BagKeys
            | Self::ArrayReverse
            | Self::ArraySum => len == 1,
            Self::Get
            | Self::Equal(_)
            | Self::BagHasKey
            | Self::BagRemoveKeys
            | Self::ArrayHas
            | Self::Repeat
            | Self::StrcatArray
            | Self::SetDifference => len == 2,
            Self::ArraySlice | Self::BagSetKey | Self::Range => len == 3,
            Self::ArrayIndexOf => (2..=5).contains(&len),
            Self::Split => (2..=3).contains(&len),
            Self::BagPack => len.is_multiple_of(2),
            Self::PackArray => true,
            Self::Zip
            | Self::ArrayConcat
            | Self::BagMerge
            | Self::SetUnion
            | Self::SetIntersect => len >= 2,
            Self::Expand { columns, .. } => len >= 1 && len == *columns,
            Self::Coalesce | Self::Membership { .. } => len >= 1,
            Self::Series(_) => unreachable!("series arity checked above"),
        };
        if valid {
            Ok(())
        } else {
            Err(failure(format!(
                "invalid argument count for {}",
                self.name()
            )))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Function {
    kernel: Kernel,
    name: String,
    signature: Signature,
}

pub(crate) fn call(kernel: Kernel, args: Vec<Expr>) -> Expr {
    if args.is_empty() && matches!(kernel, Kernel::PackArray | Kernel::BagPack) {
        let (id, text) = if kernel == Kernel::PackArray {
            (ARRAY, "[]")
        } else {
            (BAG, "{}")
        };
        return datafusion::prelude::lit(ScalarValue::Union(
            Some((id, Box::new(ScalarValue::Utf8(Some(text.to_owned()))))),
            FIELDS.clone(),
            UnionMode::Dense,
        ));
    }
    let name = kernel.name();
    let signature = if args.is_empty() {
        Signature::nullary(Volatility::Immutable)
    } else {
        Signature::variadic_any(Volatility::Immutable)
    };
    ScalarUDF::from(Function {
        kernel,
        name,
        signature,
    })
    .call(args)
}

impl ScalarUDFImpl for Function {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.kernel.validate(args.len())?;
        Ok(self.kernel.output_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.kernel.validate(args.args.len())?;
        let scalar = args
            .args
            .iter()
            .all(|v| matches!(v, ColumnarValue::Scalar(_)));
        let count = if scalar { 1 } else { args.number_rows };
        if let Kernel::DecodeList { limit } = self.kernel {
            let array = args.args[0].to_array(1)?;
            let lists = downcast::<ListArray>(&array)?;
            let mut builder = Builder::new(count);
            for row in 0..count {
                let mut items = Vec::new();
                if !lists.is_null(row) {
                    let values = lists.value(row);
                    let values = downcast::<StringArray>(&values)?;
                    for value in values.iter().flatten().take(limit) {
                        items
                            .push(serde_json::from_str(value).map_err(|e| failure(e.to_string()))?);
                    }
                }
                builder.append_json(&Json::Array(items))?;
            }
            let output = builder.finish()?;
            return if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &output, 0,
                )?))
            } else {
                Ok(ColumnarValue::Array(output))
            };
        }
        let inputs = args
            .args
            .iter()
            .map(Input::new)
            .collect::<Result<Vec<_>>>()?;
        let output = if self.kernel == Kernel::ParseJson {
            let mut builder = Builder::new(count);
            for row in 0..count {
                let value = inputs[0].value(row)?;
                if matches!(value, Value::Guid(_)) {
                    builder.append(value)?;
                } else {
                    builder.append_json(&evaluate(&self.kernel, &[value])?)?;
                }
            }
            builder.finish()?
        } else if let Kernel::Unpack { keys } = &self.kernel {
            let mut builders = keys.iter().map(|_| Builder::new(count)).collect::<Vec<_>>();
            for row in 0..count {
                let value = inputs[0].value(row)?.json()?;
                for (key, builder) in keys.iter().zip(&mut builders) {
                    builder.append_json(
                        value
                            .as_object()
                            .and_then(|bag| bag.get(key))
                            .unwrap_or(&Json::Null),
                    )?;
                }
            }
            if keys.is_empty() {
                Arc::new(StructArray::new_empty_fields(count, None)) as ArrayRef
            } else {
                let fields = keys
                    .iter()
                    .enumerate()
                    .map(|(i, _)| Arc::new(Field::new(format!("field{i}"), data_type(), true)))
                    .collect::<Vec<_>>();
                let arrays = builders
                    .into_iter()
                    .map(Builder::finish)
                    .collect::<Result<Vec<_>>>()?;
                Arc::new(StructArray::try_new(fields.into(), arrays, None)?) as ArrayRef
            }
        } else if self.kernel == Kernel::DecodeWindow {
            let mut builder = Builder::new(count);
            for row in 0..count {
                match inputs[0].value(row)? {
                    Value::Null => builder.append(Value::Null)?,
                    Value::String(text) => {
                        let encoded: Vec<Json> =
                            serde_json::from_str(text).map_err(|e| failure(e.to_string()))?;
                        if encoded.len() != 2 {
                            return Err(failure("invalid dynamic window envelope"));
                        }
                        match encoded[0].as_str() {
                            Some("datetime") => builder.append(Value::Datetime(
                                encoded[1]
                                    .as_i64()
                                    .ok_or_else(|| failure("invalid datetime window payload"))?,
                            ))?,
                            Some("timespan") => builder.append(Value::Timespan(
                                encoded[1]
                                    .as_i64()
                                    .ok_or_else(|| failure("invalid timespan window payload"))?,
                            ))?,
                            Some("guid") => {
                                let text = encoded[1]
                                    .as_str()
                                    .ok_or_else(|| failure("invalid guid window payload"))?;
                                let (value, _) = crate::guid::parse_prefix(text)
                                    .filter(|(_, end)| *end == text.len())
                                    .ok_or_else(|| failure("invalid guid window payload"))?;
                                builder.append(Value::Guid(value))?;
                            }
                            Some(_) => builder.append_json(&encoded[1])?,
                            None => return Err(failure("invalid dynamic window type")),
                        }
                    }
                    _ => return Err(failure("dynamic window decoder expects UTF-8")),
                }
            }
            builder.finish()?
        } else if let Kernel::Convert(ty) | Kernel::TypeOnly(ty) = &self.kernel {
            convert(&inputs[0], ty, matches!(self.kernel, Kernel::TypeOnly(_)))?
        } else if matches!(self.kernel, Kernel::Expand { .. }) {
            expand(&self.kernel, &inputs, count)?
        } else if matches!(self.kernel, Kernel::IntoDynamic | Kernel::Coalesce) {
            let mut builder = Builder::new(count);
            for row in 0..count {
                let mut selected = Value::Null;
                for input in &inputs {
                    selected = input.value(row)?;
                    if !matches!(selected, Value::Null) {
                        break;
                    }
                }
                builder.append(selected)?;
            }
            builder.finish()?
        } else {
            self.evaluate(&inputs, count)?
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

impl Function {
    fn evaluate(&self, inputs: &[Input], count: usize) -> Result<ArrayRef> {
        let mut dynamic = Builder::new(count);
        let mut longs = Int64Builder::with_capacity(count);
        let mut reals = Float64Builder::with_capacity(count);
        let mut bools = BooleanBuilder::with_capacity(count);
        let mut strings = StringBuilder::with_capacity(count, 0);
        let ty = self.kernel.output_type();
        for row in 0..count {
            let values = inputs
                .iter()
                .map(|v| v.value(row))
                .collect::<Result<Vec<_>>>()?;
            let value = evaluate(&self.kernel, &values)?;
            match ty {
                DataType::Int64 => longs.append_option(value.as_i64()),
                DataType::Float64 => reals.append_option(value.as_f64()),
                DataType::Boolean => bools.append_option(value.as_bool()),
                DataType::Utf8 => strings.append_option(value.as_str()),
                _ => dynamic.append_json(&value)?,
            }
        }
        Ok(match ty {
            DataType::Int64 => Arc::new(longs.finish()),
            DataType::Float64 => Arc::new(reals.finish()),
            DataType::Boolean => Arc::new(bools.finish()),
            DataType::Utf8 => Arc::new(strings.finish()),
            _ => return dynamic.finish(),
        })
    }
}

fn convert(input: &Input, ty: &DataType, type_only: bool) -> Result<ArrayRef> {
    let options = datafusion::arrow::compute::CastOptions {
        safe: true,
        ..Default::default()
    };
    if !is_dynamic(input.array.data_type()) {
        if *ty == crate::guid::data_type() {
            return crate::guid::parse_array(&input.array);
        }
        if input.array.data_type() == &crate::guid::data_type()
            && *ty == DataType::Utf8
            && !type_only
        {
            return crate::guid::format_array(&input.array);
        }
        return cast_with_options(&input.array, ty, &options).map_err(Into::into);
    }
    let union = downcast::<UnionArray>(&input.array)?;
    let mut casted = Vec::with_capacity(FIELDS.len());
    let DataType::Union(fields, _) = input.array.data_type() else {
        return Err(failure("invalid dynamic union"));
    };
    for (id, _) in fields.iter() {
        let source = union.child(id);
        if *ty == DataType::Utf8 && id == GUID && !type_only {
            casted.push(crate::guid::format_array(source)?);
            continue;
        }
        if *ty == crate::guid::data_type() && id == STRING && !type_only {
            casted.push(crate::guid::parse_array(source)?);
            continue;
        }
        if *ty == DataType::Duration(TimeUnit::Nanosecond)
            && !type_only
            && matches!(id, STRING | LONG | REAL)
        {
            let array: ArrayRef = match id {
                STRING => Arc::new(DurationNanosecondArray::from_iter(
                    downcast::<StringArray>(source)?
                        .iter()
                        .map(|value| value.and_then(crate::temporal::try_timespan)),
                )),
                LONG => Arc::new(DurationNanosecondArray::from_iter(
                    downcast::<Int64Array>(source)?.iter().map(|value| {
                        value.and_then(|v| i64::try_from(i128::from(v) * 86_400_000_000_000).ok())
                    }),
                )),
                _ => Arc::new(DurationNanosecondArray::from_iter(
                    downcast::<Float64Array>(source)?.iter().map(|value| {
                        value.and_then(|v| {
                            let nanos = v * 86_400_000_000_000.0;
                            (nanos.is_finite()
                                && nanos >= i64::MIN as f64
                                && nanos < 9_223_372_036_854_775_808.0)
                                .then_some(nanos as i64)
                        })
                    }),
                )),
            };
            casted.push(array);
            continue;
        }
        let compatible = match ty {
            DataType::Boolean => id == BOOL,
            DataType::Int32 | DataType::Int64 => id == LONG,
            DataType::Float64 => id == REAL,
            DataType::Utf8 => id == STRING,
            DataType::Timestamp(_, _) => id == DATETIME,
            DataType::Duration(_) => id == TIMESPAN,
            DataType::FixedSizeBinary(16) => id == GUID,
            _ => false,
        };
        let array =
            if id == NULL || type_only && !compatible || !can_cast_types(source.data_type(), ty) {
                if *ty == DataType::Utf8 && !type_only {
                    Arc::new(StringArray::from(vec![""; source.len()])) as ArrayRef
                } else {
                    new_null_array(ty, source.len())
                }
            } else {
                cast_with_options(source, ty, &options)?
            };
        casted.push(array);
    }
    let arrays = casted.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
    let indices = (0..union.len())
        .map(|row| (union.type_id(row) as usize, union.value_offset(row)))
        .collect::<Vec<_>>();
    Ok(interleave(&arrays, &indices)?)
}

fn text(value: Value<'_>) -> Option<&str> {
    match value {
        Value::String(text) => Some(text),
        _ => None,
    }
}

fn expanded(
    value: Value<'_>,
    array_mode: bool,
    arrays_only: bool,
    limit: usize,
) -> Result<Vec<Json>> {
    let mut items = match value.json()? {
        Json::Array(items) => items,
        Json::Object(bag) if !arrays_only => bag
            .into_iter()
            .map(|(key, value)| {
                if array_mode {
                    Json::Array(vec![key.into(), value])
                } else {
                    Json::Object(Map::from_iter([(key, value)]))
                }
            })
            .collect(),
        value => vec![value],
    };
    items.truncate(limit);
    size(items.len())?;
    Ok(items)
}

fn expand(kernel: &Kernel, inputs: &[Input], count: usize) -> Result<ArrayRef> {
    let Kernel::Expand {
        array_mode,
        arrays_only,
        limit,
        columns,
    } = kernel
    else {
        return Err(failure("invalid expansion kernel"));
    };
    let mut offsets = Vec::with_capacity(count + 1);
    offsets.push(0_i32);
    let mut builders = (0..*columns)
        .map(|_| Builder::new(count))
        .collect::<Vec<_>>();
    let mut indices = Int64Builder::new();
    for row in 0..count {
        let roots = inputs
            .iter()
            .map(|input| input.value(row))
            .collect::<Result<Vec<_>>>()?;
        let collections = roots
            .iter()
            .map(|value| expanded(*value, *array_mode, *arrays_only, *limit))
            .collect::<Result<Vec<_>>>()?;
        let len = collections.iter().map(Vec::len).max().unwrap_or(0);
        for ((builder, values), root) in builders.iter_mut().zip(collections).zip(roots) {
            // Pad real dynamic nulls before Unnest. Arrow union take kernels
            // cannot represent the null indices used by zipped-list padding.
            for index in 0..len {
                if index == 0 && !values.is_empty() && matches!(root, Value::Guid(_)) {
                    builder.append(root)?;
                } else {
                    builder.append_json(values.get(index).unwrap_or(&Json::Null))?;
                }
            }
        }
        indices.extend((0..len).map(|i| Some(i as i64)));
        offsets.push(
            i32::try_from(indices.len())
                .map_err(|_| failure("mv-expand batch exceeds Arrow offsets"))?,
        );
    }
    let mut arrays = builders
        .into_iter()
        .map(Builder::finish)
        .collect::<Result<Vec<_>>>()?;
    arrays.push(Arc::new(indices.finish()));
    let fields = expansion_fields(*columns);
    let values = StructArray::try_new(fields.clone().into(), arrays, None)?;
    Ok(Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", DataType::Struct(fields.into()), false)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        None,
    )?))
}

fn expansion_fields(columns: usize) -> Vec<Arc<Field>> {
    let mut fields = (0..columns)
        .map(|i| Arc::new(Field::new(format!("column_{i}"), data_type(), true)))
        .collect::<Vec<_>>();
    fields.push(Arc::new(Field::new("index", DataType::Int64, false)));
    fields
}

fn integer(value: Value<'_>) -> Option<i64> {
    match value {
        Value::Long(value) => Some(value),
        Value::Bool(value) => Some(i64::from(value)),
        _ => None,
    }
}

fn json_array(value: Value<'_>) -> Result<Option<Vec<Json>>> {
    match value.json()? {
        Json::Array(items) => Ok(Some(items)),
        _ => Ok(None),
    }
}

fn json_bag(value: Value<'_>) -> Result<Option<Map<String, Json>>> {
    match value.json()? {
        Json::Object(items) => Ok(Some(items)),
        _ => Ok(None),
    }
}

fn size(value: usize) -> Result<()> {
    if value > MAX_COLLECTION_SIZE {
        Err(failure("dynamic collection exceeds 1048576 elements"))
    } else {
        Ok(())
    }
}

fn range(values: &[Value<'_>]) -> Result<Json> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Json::Null);
    }
    let integers = match (values[0], values[1], values[2]) {
        (Value::Long(a), Value::Long(b), Value::Long(step)) => Some((a, b, step, 0)),
        (Value::Datetime(a), Value::Datetime(b), Value::Timespan(step)) => Some((a, b, step, 1)),
        (Value::Timespan(a), Value::Timespan(b), Value::Timespan(step)) => Some((a, b, step, 2)),
        _ => None,
    };
    if let Some((start, end, step, kind)) = integers {
        if step == 0 {
            return Err(failure("range() step cannot be zero"));
        }
        let distance = i128::from(end) - i128::from(start);
        let count = if distance.signum() != i128::from(step).signum() && distance != 0 {
            0
        } else {
            distance / i128::from(step) + 1
        };
        let count = usize::try_from(count).map_err(|_| failure("invalid range size"))?;
        size(count)?;
        let mut output = Vec::with_capacity(count);
        for index in 0..count {
            let value = i64::try_from(i128::from(start) + i128::from(step) * index as i128)
                .map_err(|_| failure("range value overflow"))?;
            output.push(match kind {
                1 => Value::Datetime(value).json()?,
                2 => Value::Timespan(value).json()?,
                _ => Json::from(value),
            });
        }
        return Ok(Json::Array(output));
    }
    let mut numbers = Vec::new();
    for value in values {
        let value = match value {
            Value::Long(v) => *v as f64,
            Value::Real(v) => *v,
            _ => {
                return Err(failure(
                    "range() requires numeric bounds or matching temporal types",
                ));
            }
        };
        if !value.is_finite() {
            return Err(failure("range() requires finite values"));
        }
        numbers.push(value);
    }
    let (start, end, step) = (numbers[0], numbers[1], numbers[2]);
    if step == 0.0 {
        return Err(failure("range() step cannot be zero"));
    }
    let count = ((end - start) / step).floor();
    if !count.is_finite() || count >= MAX_COLLECTION_SIZE as f64 {
        return Err(failure("dynamic collection exceeds 1048576 elements"));
    }
    let count = if count < 0.0 { 0 } else { count as usize + 1 };
    Ok(Json::Array(
        (0..count)
            .map(|i| Json::from(start + i as f64 * step))
            .collect(),
    ))
}

fn evaluate(kernel: &Kernel, values: &[Value<'_>]) -> Result<Json> {
    use Kernel as K;
    Ok(match kernel {
        K::ParseJson => match values[0] {
            // KQL deliberately retains invalid JSON as a dynamic string.
            Value::String(text) => serde_json::from_str(text).unwrap_or_else(|_| text.into()),
            value => value.json()?,
        },
        K::ParseJsonStrict => match values[0] {
            Value::String(text) => serde_json::from_str(text).unwrap_or(Json::Null),
            value => value.json()?,
        },
        K::EncodeJson => values[0].json()?.to_string().into(),
        K::Series(op) => crate::series_functions::evaluate(
            *op,
            &values
                .iter()
                .map(|value| value.json())
                .collect::<Result<Vec<_>>>()?,
        )?,
        K::EncodeWindow => {
            let payload = match values[0] {
                Value::Datetime(value) | Value::Timespan(value) => Json::from(value),
                value => value.json()?,
            };
            Json::Array(vec![values[0].type_name().into(), payload])
                .to_string()
                .into()
        }
        K::Get => {
            let value = values[0].json()?;
            match (value, values[1]) {
                (Json::Object(mut bag), Value::String(key)) => {
                    bag.remove(key).unwrap_or(Json::Null)
                }
                (Json::Array(items), index) => {
                    let index = integer(index)
                        .ok_or_else(|| failure("dynamic array index must be an integer"))?;
                    let index = if index < 0 {
                        items.len() as i128 + i128::from(index)
                    } else {
                        i128::from(index)
                    };
                    usize::try_from(index)
                        .ok()
                        .and_then(|i| items.get(i))
                        .cloned()
                        .unwrap_or(Json::Null)
                }
                _ => Json::Null,
            }
        }
        K::GetType => values[0].type_name().into(),
        K::Equal(negated) => {
            let left_null = matches!(values[0], Value::Null);
            let right_null = matches!(values[1], Value::Null);
            if left_null && right_null {
                Json::Null
            } else {
                let equal = match (values[0], values[1]) {
                    (Value::Bool(a), Value::Bool(b)) => a == b,
                    (Value::Long(a), Value::Long(b)) => a == b,
                    (Value::Real(a), Value::Real(b)) => a == b,
                    (Value::String(a), Value::String(b)) => a == b,
                    (Value::Datetime(a), Value::Datetime(b))
                    | (Value::Timespan(a), Value::Timespan(b)) => a == b,
                    (a, b) if a.type_name() == b.type_name() => a.json()? == b.json()?,
                    _ => false,
                };
                Json::Bool(if *negated { !equal } else { equal })
            }
        }
        K::Membership {
            negated,
            insensitive,
        } => {
            let mut needle = values[0].json()?;
            let mut list = Vec::new();
            fn flatten(value: Json, list: &mut Vec<Json>) -> Result<()> {
                if let Json::Array(items) = value {
                    for item in items {
                        flatten(item, list)?;
                    }
                } else {
                    size(list.len() + 1)?;
                    list.push(value);
                }
                Ok(())
            }
            for value in &values[1..] {
                flatten(value.json()?, &mut list)?;
            }
            if *insensitive {
                if let Json::String(text) = &mut needle {
                    text.make_ascii_lowercase();
                }
                for value in &mut list {
                    if let Json::String(text) = value {
                        text.make_ascii_lowercase();
                    }
                }
            }
            if needle.is_null() && list.iter().any(Json::is_null) {
                Json::Null
            } else {
                let found = !needle.is_null() && list.contains(&needle);
                (if *negated { !found } else { found }).into()
            }
        }
        K::ArrayLength => json_array(values[0])?.map_or(Json::Null, |a| a.len().into()),
        K::PackArray => Json::Array(values.iter().map(|v| v.json()).collect::<Result<_>>()?),
        K::BagPack => {
            let mut result = Map::new();
            for [key, value] in values.as_chunks::<2>().0 {
                let key = text(*key).ok_or_else(|| failure("bag_pack keys must be strings"))?;
                result.insert(key.to_owned(), value.json()?);
            }
            Json::Object(result)
        }
        K::BagKeys => match json_bag(values[0])? {
            Some(bag) => Json::Array(bag.into_iter().map(|(key, _)| Json::String(key)).collect()),
            None => Json::Null,
        },
        K::BagHasKey => {
            let value = values[0].json()?;
            let key = text(values[1]).ok_or_else(|| failure("bag_has_key expects a string key"))?;
            let Some(key) = key.strip_prefix("$.") else {
                return Ok(value
                    .as_object()
                    .is_some_and(|bag| bag.contains_key(key))
                    .into());
            };
            let mut at = &value;
            let mut found = true;
            for key in key.split('.') {
                if let Some(value) = at.as_object().and_then(|v| v.get(key)) {
                    at = value;
                } else {
                    found = false;
                    break;
                }
            }
            found.into()
        }
        K::BagMerge => {
            let mut result = Map::new();
            for value in values.iter().rev() {
                let Some(bag) = json_bag(*value)? else {
                    return Ok(Json::Null);
                };
                result.extend(bag);
            }
            Json::Object(result)
        }
        K::BagSetKey => {
            let Some(mut bag) = json_bag(values[0])? else {
                return Ok(Json::Null);
            };
            let key = text(values[1]).ok_or_else(|| failure("bag_set_key expects a string key"))?;
            if key.starts_with('$') {
                return Err(failure(
                    "bag_set_key JSONPath updates are not implemented; use a root key",
                ));
            }
            bag.insert(key.to_owned(), values[2].json()?);
            Json::Object(bag)
        }
        K::BagRemoveKeys => {
            let Some(mut bag) = json_bag(values[0])? else {
                return Ok(Json::Null);
            };
            let Some(keys) = json_array(values[1])? else {
                return Ok(Json::Null);
            };
            for key in keys {
                let key = key
                    .as_str()
                    .ok_or_else(|| failure("bag_remove_keys expects string keys"))?;
                if key.starts_with('$') {
                    return Err(failure(
                        "bag_remove_keys JSONPath updates are not implemented; use root keys",
                    ));
                }
                bag.remove(key);
            }
            Json::Object(bag)
        }
        K::ArrayConcat | K::SetUnion | K::SetIntersect | K::SetDifference => {
            let mut inputs = Vec::with_capacity(values.len());
            for value in values {
                let Some(items) = json_array(*value)? else {
                    return Ok(Json::Null);
                };
                inputs.push(items);
            }
            let mut result = Vec::new();
            if *kernel == K::ArrayConcat {
                for items in inputs {
                    size(result.len().saturating_add(items.len()))?;
                    result.extend(items);
                }
            } else {
                let others = inputs[1..]
                    .iter()
                    .map(|items| items.iter().map(Json::to_string).collect::<HashSet<_>>())
                    .collect::<Vec<_>>();
                let candidates = if *kernel == K::SetUnion {
                    inputs.into_iter().flatten().collect::<Vec<_>>()
                } else {
                    inputs.remove(0)
                };
                let mut seen = HashSet::new();
                for item in candidates {
                    let key = item.to_string();
                    let include = match kernel {
                        K::SetIntersect => others.iter().all(|set| set.contains(&key)),
                        K::SetDifference => !others[0].contains(&key),
                        _ => true,
                    };
                    if include && seen.insert(key) {
                        result.push(item);
                    }
                }
            }
            Json::Array(result)
        }
        K::ArraySlice => {
            let Some(items) = json_array(values[0])? else {
                return Ok(Json::Null);
            };
            let Some(start) = integer(values[1]) else {
                return Ok(Json::Null);
            };
            let Some(end) = integer(values[2]) else {
                return Ok(Json::Null);
            };
            let len = items.len() as i128;
            let offset = |value: i64| {
                if value < 0 {
                    len + i128::from(value)
                } else {
                    i128::from(value)
                }
            };
            let start = offset(start).clamp(0, len) as usize;
            let end = (offset(end) + 1).clamp(0, len) as usize;
            Json::Array(if end < start {
                Vec::new()
            } else {
                items[start..end].to_vec()
            })
        }
        K::ArrayIndexOf | K::ArrayHas => {
            let Some(items) = json_array(values[0])? else {
                return Ok(Json::Null);
            };
            let needle = values[1].json()?;
            let start = values.get(2).copied().and_then(integer).unwrap_or(0);
            let length = values.get(3).copied().and_then(integer).unwrap_or(-1);
            let occurrence = values.get(4).copied().and_then(integer).unwrap_or(1);
            if length < -1 || occurrence < 1 {
                return Ok(Json::Null);
            }
            let len = items.len() as i128;
            let start = if start < 0 {
                (len + i128::from(start)).max(0)
            } else {
                i128::from(start)
            };
            let end = if length == -1 {
                len
            } else {
                (start + i128::from(length)).min(len)
            };
            let mut remaining = occurrence;
            let mut found = -1_i64;
            for index in start..end {
                if items[index as usize] == needle {
                    remaining -= 1;
                    if remaining == 0 {
                        found = index as i64;
                        break;
                    }
                }
            }
            if *kernel == K::ArrayHas {
                (found >= 0).into()
            } else {
                found.into()
            }
        }
        K::ArrayReverse => {
            let Some(mut items) = json_array(values[0])? else {
                return Ok(Json::Null);
            };
            items.reverse();
            Json::Array(items)
        }
        K::ArraySum => {
            let Some(items) = json_array(values[0])? else {
                return Ok(Json::Null);
            };
            let mut sum = 0.0;
            for item in items {
                let Some(number) = item.as_f64() else {
                    return Ok(Json::Null);
                };
                sum += number;
            }
            Json::from(sum)
        }
        K::Zip => {
            let mut arrays = Vec::new();
            for value in values {
                let Some(items) = json_array(*value)? else {
                    return Ok(Json::Null);
                };
                arrays.push(items);
            }
            let len = arrays.iter().map(Vec::len).max().unwrap_or(0);
            size(len)?;
            Json::Array(
                (0..len)
                    .map(|i| {
                        Json::Array(
                            arrays
                                .iter()
                                .map(|a| a.get(i).cloned().unwrap_or(Json::Null))
                                .collect(),
                        )
                    })
                    .collect(),
            )
        }
        K::Repeat => {
            let Some(count) = integer(values[1]).and_then(|n| usize::try_from(n).ok()) else {
                return Ok(Json::Null);
            };
            size(count)?;
            Json::Array(vec![values[0].json()?; count])
        }
        K::Range => range(values)?,
        K::StrcatArray => {
            let Some(items) = json_array(values[0])? else {
                return Ok(Json::Null);
            };
            let delimiter = text(values[1])
                .ok_or_else(|| failure("strcat_array expects a string delimiter"))?;
            items
                .into_iter()
                .map(|item| match item {
                    Json::String(text) => text,
                    Json::Null => String::new(),
                    value => value.to_string(),
                })
                .collect::<Vec<_>>()
                .join(delimiter)
                .into()
        }
        K::Split => {
            let (Some(value), Some(delimiter)) = (text(values[0]), text(values[1])) else {
                return Ok(Json::Null);
            };
            let parts = if delimiter.is_empty() {
                vec![value]
            } else {
                value.split(delimiter).collect::<Vec<_>>()
            };
            let result = if let Some(index) = values.get(2).copied() {
                integer(index)
                    .and_then(|i| usize::try_from(i).ok())
                    .and_then(|i| parts.get(i))
                    .map(|v| vec![Json::String((*v).to_owned())])
                    .unwrap_or_default()
            } else {
                parts
                    .into_iter()
                    .map(|v| Json::String(v.to_owned()))
                    .collect()
            };
            Json::Array(result)
        }
        K::PackAll {
            names,
            ignore_empty,
        } => {
            let mut bag = Map::new();
            for (name, value) in names.iter().zip(values) {
                let value = value.json()?;
                if !*ignore_empty || !(value.is_null() || value.as_str() == Some("")) {
                    bag.insert(name.clone(), value);
                }
            }
            Json::Object(bag)
        }
        K::TopValues {
            real,
            descending,
            nulls_first,
            expanded_limit,
            count,
        } => {
            let mut items = expanded(values[0], false, true, *expanded_limit)?;
            for item in &mut items {
                let valid = if *real {
                    matches!(item,Json::Number(n) if n.is_f64())
                } else {
                    item.as_i64().is_some()
                };
                if !valid {
                    *item = Json::Null;
                }
            }
            let array: ArrayRef = if *real {
                Arc::new(Float64Array::from_iter(items.iter().map(Json::as_f64)))
            } else {
                Arc::new(Int64Array::from_iter(items.iter().map(Json::as_i64)))
            };
            let indices = datafusion::arrow::compute::sort_to_indices(
                array.as_ref(),
                Some(datafusion::arrow::compute::SortOptions {
                    descending: *descending,
                    nulls_first: *nulls_first,
                }),
                Some(*count),
            )?;
            Json::Array(
                indices
                    .values()
                    .iter()
                    .map(|&i| items[i as usize].clone())
                    .collect(),
            )
        }
        K::TopReduce(reduction) => {
            let Json::Array(items) = values[0].json()? else {
                return Err(failure("invalid optimized mv-apply input"));
            };
            match reduction {
                TopReduction::Sum => {
                    let array = Int64Array::from_iter(items.iter().map(Json::as_i64));
                    datafusion::arrow::compute::sum(&array).unwrap_or(0).into()
                }
                TopReduction::Count => (items.len() as i64).into(),
                TopReduction::List {
                    limit,
                    include_nulls,
                } => Json::Array(
                    items
                        .into_iter()
                        .filter(|value| *include_nulls || !value.is_null())
                        .take(*limit)
                        .collect(),
                ),
            }
        }
        K::IntoDynamic
        | K::Unpack { .. }
        | K::Coalesce
        | K::Convert(_)
        | K::TypeOnly(_)
        | K::Expand { .. }
        | K::DecodeList { .. }
        | K::DecodeWindow => {
            return Err(failure("invalid dynamic kernel dispatch"));
        }
    })
}
