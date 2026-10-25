use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, Decimal256Builder, Int32Array, Int64Array,
    StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, i256};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rust_decimal::Decimal;

use crate::ast::BinaryOp;

pub(crate) const PRECISION: u8 = 57;
pub(crate) const SCALE: i8 = 28;

static POWERS: LazyLock<[i256; 77]> = LazyLock::new(|| {
    let mut powers = [i256::ONE; 77];
    for index in 1..powers.len() {
        powers[index] = powers[index - 1] * i256::from_i128(10);
    }
    powers
});

pub(crate) fn data_type() -> DataType {
    DataType::Decimal256(PRECISION, SCALE)
}

pub(crate) fn is_decimal(ty: &DataType) -> bool {
    matches!(ty, DataType::Decimal128(_, _) | DataType::Decimal256(_, _))
}

pub(crate) fn parse(text: &str) -> Option<Decimal> {
    let text = text.trim();
    if text.contains(['e', 'E']) {
        Decimal::from_scientific(text).ok()
    } else {
        Decimal::from_str_exact(text).ok()
    }
}

pub(crate) fn literal(text: &str) -> Option<Decimal> {
    match text {
        "max" => Some(Decimal::MAX),
        "min" => Some(Decimal::MIN),
        _ => parse(text),
    }
}

pub(crate) fn encode(value: Decimal) -> i256 {
    i256::from_i128(value.mantissa()) * i256::from_i128(10_i128.pow(28 - value.scale()))
}

pub(crate) fn decode(mut value: i256, mut scale: u32) -> Option<Decimal> {
    if value == i256::ZERO {
        return Some(Decimal::ZERO);
    }
    if scale <= 28
        && let Some(value) = value.to_i128()
        && let Ok(decimal) = Decimal::try_from_i128_with_scale(value, scale)
    {
        return Some(decimal);
    }
    let (mut low, mut high) = (0, scale.checked_add(1)?);
    if scale as usize >= POWERS.len() {
        return None;
    }
    while low + 1 < high {
        let middle = (low + high) / 2;
        if value % POWERS[middle as usize] == i256::ZERO {
            low = middle;
        } else {
            high = middle;
        }
    }
    value /= POWERS[low as usize];
    scale -= low;
    if scale <= 28
        && let Some(value) = value.to_i128()
        && let Ok(decimal) = Decimal::try_from_i128_with_scale(value, scale)
    {
        return Some(decimal);
    }
    for drop in scale.saturating_sub(28).max(1)..=scale {
        let divisor = POWERS[drop as usize];
        let mut coefficient = value / divisor;
        let remainder = value % divisor;
        let remainder = if remainder < i256::ZERO {
            -remainder
        } else {
            remainder
        };
        let half = divisor / i256::from_i128(2);
        if remainder > half || remainder == half && coefficient % i256::from_i128(2) != i256::ZERO {
            coefficient += if value < i256::ZERO {
                -i256::ONE
            } else {
                i256::ONE
            };
        }
        if let Some(coefficient) = coefficient.to_i128()
            && let Ok(decimal) = Decimal::try_from_i128_with_scale(coefficient, scale - drop)
        {
            return Some(decimal);
        }
    }
    None
}

pub(crate) fn format(value: Decimal) -> String {
    value.normalize().to_string()
}

fn value(array: &ArrayRef, row: usize) -> Result<Option<Decimal>> {
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(match array.data_type() {
        DataType::Decimal256(_, scale) if *scale >= 0 => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| DataFusionError::Internal("invalid decimal array".into()))?;
            decode(values.value(row), *scale as u32)
        }
        DataType::Decimal128(_, scale) if *scale >= 0 => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| DataFusionError::Internal("invalid decimal array".into()))?;
            decode(i256::from_i128(values.value(row)), *scale as u32)
        }
        DataType::Utf8 => {
            let strings = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("invalid decimal string array".into()))?;
            parse(strings.value(row))
        }
        DataType::Int64 => Some(Decimal::from(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("invalid long decimal input".into()))?
                .value(row),
        )),
        DataType::Int32 => Some(Decimal::from(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| DataFusionError::Internal("invalid int decimal input".into()))?
                .value(row),
        )),
        ty if ty.is_numeric() => parse(&array_value_to_string(array, row)?),
        DataType::Boolean => match ScalarValue::try_from_array(array, row)? {
            ScalarValue::Boolean(Some(true)) => Some(Decimal::ONE),
            ScalarValue::Boolean(Some(false)) => Some(Decimal::ZERO),
            _ => None,
        },
        _ => None,
    })
}

pub(crate) fn convert_array(array: &ArrayRef) -> Result<ArrayRef> {
    let mut output = Decimal256Builder::with_capacity(array.len()).with_data_type(data_type());
    for row in 0..array.len() {
        output.append_option(value(array, row)?.map(encode));
    }
    Ok(Arc::new(output.finish()))
}

pub(crate) fn format_array(array: &ArrayRef) -> Result<ArrayRef> {
    let mut output = StringBuilder::new();
    for row in 0..array.len() {
        output.append_value(value(array, row)?.map(format).unwrap_or_default());
    }
    Ok(Arc::new(output.finish()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Op {
    Convert,
    Format,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Bin,
    BinAt,
    Floor,
    Ceiling,
}

pub(crate) fn arithmetic(op: BinaryOp) -> Op {
    match op {
        BinaryOp::Add => Op::Add,
        BinaryOp::Subtract => Op::Subtract,
        BinaryOp::Multiply => Op::Multiply,
        BinaryOp::Divide => Op::Divide,
        BinaryOp::Modulo => Op::Modulo,
        _ => unreachable!("decimal arithmetic receives only arithmetic operators"),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Function {
    op: Op,
    signature: Signature,
}

pub(crate) fn call(op: Op, args: Vec<Expr>) -> Expr {
    ScalarUDF::from(Function {
        op,
        signature: Signature::any(args.len(), Volatility::Immutable),
    })
    .call(args)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonDecimal {
    path: Vec<crate::json_path::Segment>,
    signature: Signature,
}

pub(crate) fn extract_json(source: Expr, path: Vec<crate::json_path::Segment>) -> Expr {
    ScalarUDF::from(JsonDecimal {
        path,
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
    })
    .call(vec![source])
}

fn json_decimal(text: &str, path: &[crate::json_path::Segment]) -> Option<Decimal> {
    use serde_json::value::RawValue;
    let mut value = serde_json::from_str::<&RawValue>(text).ok()?;
    for segment in path {
        value = match segment {
            crate::json_path::Segment::Key(key) => {
                let values =
                    serde_json::from_str::<BTreeMap<String, &RawValue>>(value.get()).ok()?;
                *values.get(key)?
            }
            crate::json_path::Segment::Index(index) => {
                let values = serde_json::from_str::<Vec<&RawValue>>(value.get()).ok()?;
                let index = if *index < 0 {
                    values.len() as i128 + i128::from(*index)
                } else {
                    i128::from(*index)
                };
                *values.get(usize::try_from(index).ok()?)?
            }
        };
    }
    let text = value.get();
    if text.starts_with('"') {
        parse(&serde_json::from_str::<String>(text).ok()?)
    } else {
        parse(text)
    }
}

impl ScalarUDFImpl for JsonDecimal {
    fn name(&self) -> &str {
        "__kql_extract_json_decimal"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(data_type())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let array = args.args[0].to_array(1)?;
        let strings = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("decimal JSON extraction requires UTF-8 input".into())
            })?;
        let mut output =
            Decimal256Builder::with_capacity(strings.len()).with_data_type(data_type());
        for value in strings {
            output.append_option(
                value
                    .and_then(|text| json_decimal(text, &self.path))
                    .map(encode),
            );
        }
        let output = Arc::new(output.finish());
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                output.as_ref(),
                0,
            )?))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}

impl ScalarUDFImpl for Function {
    fn name(&self) -> &str {
        match self.op {
            Op::Convert => "__kql_todecimal",
            Op::Format => "__kql_decimal_string",
            Op::Add => "__kql_decimal_add",
            Op::Subtract => "__kql_decimal_subtract",
            Op::Multiply => "__kql_decimal_multiply",
            Op::Divide => "__kql_decimal_divide",
            Op::Modulo => "__kql_decimal_modulo",
            Op::Bin => "__kql_decimal_bin",
            Op::BinAt => "__kql_decimal_bin_at",
            Op::Floor => "__kql_decimal_floor",
            Op::Ceiling => "__kql_decimal_ceiling",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(if self.op == Op::Format {
            DataType::Utf8
        } else {
            data_type()
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let output = if matches!(self.op, Op::Convert | Op::Format) {
            let array = args.args[0].to_array(1)?;
            if self.op == Op::Format {
                format_array(&array)?
            } else {
                convert_array(&array)?
            }
        } else {
            let count = if scalar { 1 } else { args.number_rows };
            let arrays = args
                .args
                .iter()
                .map(|arg| arg.to_array(1))
                .collect::<Result<Vec<_>>>()?;
            let scalars = args
                .args
                .iter()
                .zip(&arrays)
                .map(|(arg, array)| {
                    if matches!(arg, ColumnarValue::Scalar(_)) {
                        value(array, 0).map(Some)
                    } else {
                        Ok(None)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let at = |index: usize, row| {
                if let Some(value) = scalars[index] {
                    Ok(value)
                } else {
                    value(&arrays[index], row)
                }
            };
            let mut output = Decimal256Builder::with_capacity(count).with_data_type(data_type());
            for row in 0..count {
                let left = at(0, row)?;
                let result = if matches!(self.op, Op::Floor | Op::Ceiling) {
                    left.map(|value| {
                        if self.op == Op::Floor {
                            value.floor()
                        } else {
                            value.ceil()
                        }
                    })
                } else if matches!(self.op, Op::Bin | Op::BinAt) {
                    let size = at(1, row)?;
                    let origin = if self.op == Op::BinAt {
                        at(2, row)?
                    } else {
                        Some(Decimal::ZERO)
                    };
                    match (left, size, origin) {
                        (Some(left), Some(size), Some(origin)) if size > Decimal::ZERO => {
                            let size = encode(size);
                            let difference = encode(left) - encode(origin);
                            let mut remainder = difference % size;
                            if remainder < i256::ZERO {
                                remainder += size;
                            }
                            decode(encode(left) - remainder, 28)
                        }
                        _ => None,
                    }
                } else {
                    match (left, at(1, row)?) {
                        (Some(left), Some(right)) => match self.op {
                            Op::Add => left.checked_add(right),
                            Op::Subtract => left.checked_sub(right),
                            Op::Multiply => left.checked_mul(right),
                            Op::Divide => left.checked_div(right),
                            Op::Modulo => left.checked_rem(right),
                            _ => unreachable!("conversion is handled before binary arithmetic"),
                        },
                        _ => None,
                    }
                };
                output.append_option(result.map(encode));
            }
            Arc::new(output.finish())
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
