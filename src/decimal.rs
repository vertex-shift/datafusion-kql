use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanBuilder, Decimal128Array, Decimal256Array, Float64Builder, Int32Array,
    Int32Builder, Int64Array, Int64Builder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, i256};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use dec::Rounding;

use crate::ast::BinaryOp;
pub(crate) use crate::decimal_aggregate::aggregate;
pub(crate) use crate::decimal128::{
    Arithmetic, Builder, Decimal, data_type, is_decimal, legacy_type, scalar,
};

static LIMIT: LazyLock<Decimal> =
    LazyLock::new(|| parse("1E+38").expect("finite decimal sentinel"));

pub(crate) fn parse(text: &str) -> Option<Decimal> {
    Decimal::parse(&mut Arithmetic::default(), text)
}

pub(crate) fn parse_wire(text: &str) -> Option<Decimal> {
    let mut context = Arithmetic::default();
    let value = context.parse(text).ok()?;
    Decimal::new(&mut context, value)
}

pub(crate) fn literal(text: &str) -> Option<Decimal> {
    match text {
        "max" => Some(*LIMIT),
        "min" => parse("-1E+38"),
        _ => scalar_string(&mut Arithmetic::default(), text),
    }
}

pub(crate) fn decode(value: i256, scale: i8) -> Option<Decimal> {
    parse(&format!("{value}E{}", -i32::from(scale)))
}

pub(crate) fn format(value: Decimal) -> String {
    value.to_string()
}

fn scalar_string(context: &mut Arithmetic, text: &str) -> Option<Decimal> {
    let mut value = Decimal::parse(context, text)?;
    if !value.0.is_finite() {
        return None;
    }
    if text.contains(['e', 'E']) {
        if value >= *LIMIT || value <= Decimal(-LIMIT.0) {
            return None;
        }
        if value.0.exponent() < -37 {
            let quantum = context.parse("1E-37").ok()?;
            let rounding = context.rounding();
            context.set_rounding(Rounding::Down);
            let rounded = context.quantize(value.0, quantum);
            context.set_rounding(rounding);
            value = Decimal::new(context, rounded)?;
        }
    }
    Some(value)
}

pub(crate) fn value(array: &ArrayRef, row: usize) -> Result<Option<Decimal>> {
    value_with_context(array, row, &mut Arithmetic::default(), false)
}

fn value_with_context(
    array: &ArrayRef,
    row: usize,
    context: &mut Arithmetic,
    scalar: bool,
) -> Result<Option<Decimal>> {
    if array.is_null(row) {
        return Ok(None);
    }
    if crate::decimal128::is_runtime(array.data_type()) {
        return crate::decimal128::read(array, row);
    }
    Ok(match array.data_type() {
        DataType::Decimal256(_, scale) => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| DataFusionError::Internal("invalid decimal array".into()))?;
            Decimal::parse(
                context,
                &format!("{}E{}", values.value(row), -i32::from(*scale)),
            )
        }
        DataType::Decimal128(_, scale) => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| DataFusionError::Internal("invalid decimal array".into()))?;
            Decimal::parse(
                context,
                &format!("{}E{}", values.value(row), -i32::from(*scale)),
            )
        }
        DataType::Utf8 => {
            let strings = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("invalid decimal string array".into()))?;
            if scalar {
                scalar_string(context, strings.value(row))
            } else {
                Decimal::parse(context, strings.value(row))
            }
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
        ty if ty.is_numeric() => Decimal::parse(context, &array_value_to_string(array, row)?),
        DataType::Boolean => match ScalarValue::try_from_array(array, row)? {
            ScalarValue::Boolean(Some(true)) => Some(Decimal::ONE),
            ScalarValue::Boolean(Some(false)) => Some(Decimal::ZERO),
            _ => None,
        },
        _ => None,
    })
}

pub(crate) fn convert_array(array: &ArrayRef) -> Result<ArrayRef> {
    if crate::decimal128::is_runtime(array.data_type()) {
        return Ok(Arc::clone(array));
    }
    let mut context = Arithmetic::default();
    let mut output = Builder::new(array.len());
    for row in 0..array.len() {
        output.append(value_with_context(array, row, &mut context, false)?)?;
    }
    output.finish()
}

pub(crate) fn format_array(array: &ArrayRef) -> Result<ArrayRef> {
    let mut output = StringBuilder::new();
    for row in 0..array.len() {
        output.append_value(value(array, row)?.map(format).unwrap_or_default());
    }
    Ok(Arc::new(output.finish()))
}

pub(crate) fn cast_array(array: &ArrayRef, ty: &DataType) -> Result<ArrayRef> {
    cast_array_mode(array, ty, false)
}

fn cast_array_mode(array: &ArrayRef, ty: &DataType, implicit_scalar: bool) -> Result<ArrayRef> {
    let mut context = Arithmetic::default();
    let mut reals = Float64Builder::with_capacity(array.len());
    let mut longs = Int64Builder::with_capacity(array.len());
    let mut ints = Int32Builder::with_capacity(array.len());
    let mut bools = BooleanBuilder::with_capacity(array.len());
    for row in 0..array.len() {
        let value = value_with_context(array, row, &mut context, false)?;
        match ty {
            DataType::Float64 => reals.append_option(value.and_then(|value| {
                if implicit_scalar {
                    constant_json_real(&value.to_string())
                } else {
                    value.real()
                }
            })),
            DataType::Int64 => longs.append_option(
                value
                    .and_then(|v| v.integer(&mut context))
                    .and_then(|v| i64::try_from(v).ok()),
            ),
            DataType::Int32 => ints.append_option(
                value
                    .and_then(|v| v.integer(&mut context))
                    .and_then(|v| i32::try_from(v).ok()),
            ),
            DataType::Boolean => bools.append_option(
                value
                    .filter(|v| v.0.is_finite())
                    .map(|v| v != Decimal::ZERO),
            ),
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "unsupported decimal cast to {ty}"
                )));
            }
        }
    }
    Ok(match ty {
        DataType::Float64 => Arc::new(reals.finish()),
        DataType::Int64 => Arc::new(longs.finish()),
        DataType::Int32 => Arc::new(ints.finish()),
        DataType::Boolean => Arc::new(bools.finish()),
        _ => {
            return Err(DataFusionError::Execution(format!(
                "unsupported decimal cast to {ty}"
            )));
        }
    })
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
    Negate,
    Abs,
    ToReal,
    ImplicitReal,
    ToLong,
    ToInt,
    ToBool,
    SortKey,
    Compare(BinaryOp),
    Between,
    Contains,
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
    scalar_semantics: bool,
    signature: Signature,
}

fn scalar_semantics(values: &[Expr]) -> bool {
    values.iter().all(|value| {
        let mut scalar = true;
        value.apply(|node| {
            if matches!(node, Expr::Column(_) | Expr::OuterReferenceColumn(..) | Expr::WindowFunction(_) | Expr::AggregateFunction(_))
                || matches!(node, Expr::ScalarFunction(function) if function.func.signature().volatility == Volatility::Volatile)
            {
                scalar = false;
                Ok(TreeNodeRecursion::Stop)
            } else { Ok(TreeNodeRecursion::Continue) }
        }).expect("scalar-semantics analysis has no fallible operations");
        scalar
    })
}

pub(crate) fn call(op: Op, args: Vec<Expr>) -> Expr {
    ScalarUDF::from(Function {
        op,
        scalar_semantics: scalar_semantics(&args),
        signature: Signature::any(args.len(), Volatility::Immutable),
    })
    .call(args)
}

pub(crate) fn cast(value: Expr, ty: DataType) -> Expr {
    call(
        match ty {
            DataType::Float64 => Op::ToReal,
            DataType::Int64 => Op::ToLong,
            DataType::Int32 => Op::ToInt,
            DataType::Boolean => Op::ToBool,
            _ => unreachable!("decimal casts target scalar numeric types"),
        },
        vec![value],
    )
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonDecimal {
    path: Vec<crate::json_path::Segment>,
    scalar_semantics: bool,
    signature: Signature,
}

pub(crate) fn extract_json(source: Expr, path: Vec<crate::json_path::Segment>) -> Expr {
    ScalarUDF::from(JsonDecimal {
        path,
        scalar_semantics: scalar_semantics(std::slice::from_ref(&source)),
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
    })
    .call(vec![source])
}

// The reference scalar path uses a bounded binary64 accumulator, unlike the
// vector path's 19-digit decimal conversion. Preserve that observable difference.
fn constant_json_real(text: &str) -> Option<f64> {
    let (mantissa, exponent) = text
        .split_once(['e', 'E'])
        .map_or(Some((text, 0_i32)), |(number, exponent)| {
            Some((number, exponent.parse().ok()?))
        })?;
    let negative = mantissa.starts_with('-');
    let (integer, fraction) = mantissa
        .trim_start_matches('-')
        .split_once('.')
        .unwrap_or((mantissa.trim_start_matches('-'), ""));
    let mut number = integer.parse::<f64>().ok()?;
    if !number.is_finite() {
        return None;
    }
    let mut significant = integer.trim_start_matches('0').len();
    let mut scale = i64::from(exponent);
    for digit in fraction.bytes() {
        if significant >= 17 {
            break;
        }
        number = number * 10.0 + f64::from(digit - b'0');
        scale -= 1;
        if number != 0.0 {
            significant += 1;
        }
    }
    while scale < -308 && number != 0.0 {
        number /= 1E308;
        scale += 308;
    }
    if scale < -308 {
        number = 0.0;
    } else if scale > 308 {
        return None;
    } else if scale < 0 {
        number /= format!("1E{}", -scale).parse::<f64>().ok()?;
    } else {
        number *= format!("1E{scale}").parse::<f64>().ok()?;
    }
    (number.is_finite()).then_some(if negative { -number } else { number })
}

fn json_decimal(
    text: &str,
    path: &[crate::json_path::Segment],
    scalar: bool,
    context: &mut Arithmetic,
    numbers: &mut dec::Context<dec::Decimal<12>>,
) -> Option<Decimal> {
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
        let text = serde_json::from_str::<String>(text).ok()?;
        if scalar {
            scalar_string(context, &text)
        } else {
            Decimal::parse(context, &text)
        }
    } else if let Ok(integer) = text.parse::<i64>() {
        Some(Decimal::from(integer))
    } else if scalar {
        let number = constant_json_real(text)?;
        Decimal::parse(context, &number.to_string())
    } else {
        numbers.clear_status();
        let number = numbers.parse(text).ok()?;
        let value = context.from_decimal(&number);
        Decimal::new(context, value)
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
        let mut context = Arithmetic::default();
        let mut numbers = dec::Context::<dec::Decimal<12>>::default();
        numbers
            .set_precision(19)
            .map_err(|error| DataFusionError::Internal(error.to_string()))?;
        let mut output = Builder::new(strings.len());
        for value in strings {
            output.append(value.and_then(|text| {
                json_decimal(
                    text,
                    &self.path,
                    self.scalar_semantics,
                    &mut context,
                    &mut numbers,
                )
            }))?;
        }
        let output = output.finish()?;
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
            Op::Negate => "__kql_decimal_negate",
            Op::Abs => "__kql_decimal_abs",
            Op::ToReal => "__kql_decimal_to_real",
            Op::ImplicitReal => "__kql_decimal_implicit_real",
            Op::ToLong => "__kql_decimal_to_long",
            Op::ToInt => "__kql_decimal_to_int",
            Op::ToBool => "__kql_decimal_to_bool",
            Op::SortKey => "__kql_decimal_sort_key",
            Op::Compare(_) => "__kql_decimal_compare",
            Op::Between => "__kql_decimal_between",
            Op::Contains => "__kql_decimal_in",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(match self.op {
            Op::Format => DataType::Utf8,
            Op::ToReal | Op::ImplicitReal => DataType::Float64,
            Op::ToLong => DataType::Int64,
            Op::ToInt => DataType::Int32,
            Op::ToBool | Op::Compare(_) | Op::Between | Op::Contains => DataType::Boolean,
            _ => data_type(),
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let mut context = Arithmetic::default();
        let output = if matches!(self.op, Op::Convert | Op::Format) {
            let array = args.args[0].to_array(1)?;
            if self.op == Op::Format {
                format_array(&array)?
            } else if crate::decimal128::is_runtime(array.data_type()) {
                array
            } else {
                let mut output = Builder::new(array.len());
                for row in 0..array.len() {
                    output.append(value_with_context(
                        &array,
                        row,
                        &mut context,
                        self.scalar_semantics,
                    )?)?;
                }
                output.finish()?
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
                        value_with_context(array, 0, &mut context, true).map(Some)
                    } else {
                        Ok(None)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let at = |index: usize, row, context: &mut Arithmetic| {
                if let Some(value) = scalars[index] {
                    Ok(value)
                } else {
                    value_with_context(&arrays[index], row, context, false)
                }
            };
            if matches!(self.op, Op::Compare(_) | Op::Between | Op::Contains) {
                let mut output = BooleanBuilder::with_capacity(count);
                let mut members = HashSet::new();
                let mut contains_null = false;
                if self.op == Op::Contains {
                    for item in &scalars[1..] {
                        match item {
                            Some(Some(value)) if !value.0.is_nan() => {
                                members.insert(if value.0.is_zero() {
                                    Decimal::ZERO.key()
                                } else {
                                    value.key()
                                });
                            }
                            Some(Some(_)) => {}
                            Some(None) => contains_null = true,
                            None => {
                                return Err(DataFusionError::Execution(
                                    "decimal membership requires constant list values".into(),
                                ));
                            }
                        }
                    }
                }
                for row in 0..count {
                    let left = at(0, row, &mut context)?;
                    let result = if self.op == Op::Contains {
                        match left {
                            None => {
                                if contains_null {
                                    None
                                } else {
                                    Some(false)
                                }
                            }
                            Some(value) => Some(
                                !value.0.is_nan()
                                    && members.contains(&if value.0.is_zero() {
                                        Decimal::ZERO.key()
                                    } else {
                                        value.key()
                                    }),
                            ),
                        }
                    } else {
                        let right = at(1, row, &mut context)?;
                        if self.op == Op::Between {
                            let high = at(2, row, &mut context)?;
                            match (left, right, high) {
                                (Some(value), Some(low), Some(high)) => {
                                    Some(value >= low && value <= high)
                                }
                                _ => None,
                            }
                        } else if let Op::Compare(op) = self.op {
                            match (left, right) {
                                (None, None) => None,
                                (None, Some(_)) | (Some(_), None) => match op {
                                    BinaryOp::Eq => Some(false),
                                    BinaryOp::NotEq => Some(true),
                                    _ => None,
                                },
                                (Some(a), Some(b)) => Some(match op {
                                    BinaryOp::Eq => a == b,
                                    BinaryOp::NotEq => a != b,
                                    BinaryOp::Lt => a < b,
                                    BinaryOp::LtEq => a <= b,
                                    BinaryOp::Gt => a > b,
                                    BinaryOp::GtEq => a >= b,
                                    _ => {
                                        return Err(DataFusionError::Internal(
                                            "invalid decimal comparison".into(),
                                        ));
                                    }
                                }),
                            }
                        } else {
                            unreachable!("only comparison operations enter this branch")
                        }
                    };
                    output.append_option(result);
                }
                let output: ArrayRef = Arc::new(output.finish());
                return if scalar {
                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                        &output, 0,
                    )?))
                } else {
                    Ok(ColumnarValue::Array(output))
                };
            }
            if matches!(
                self.op,
                Op::ToReal | Op::ImplicitReal | Op::ToLong | Op::ToInt | Op::ToBool
            ) {
                let ty = match self.op {
                    Op::ToReal | Op::ImplicitReal => DataType::Float64,
                    Op::ToLong => DataType::Int64,
                    Op::ToInt => DataType::Int32,
                    Op::ToBool => DataType::Boolean,
                    _ => unreachable!("numeric casts are handled here"),
                };
                let output = cast_array_mode(
                    &arrays[0],
                    &ty,
                    self.scalar_semantics && self.op == Op::ImplicitReal,
                )?;
                return if scalar {
                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                        &output, 0,
                    )?))
                } else {
                    Ok(ColumnarValue::Array(output))
                };
            }
            let mut output = Builder::new(count);
            for row in 0..count {
                let left = at(0, row, &mut context)?;
                let result = if self.op == Op::SortKey {
                    left.filter(|value| !value.0.is_nan())
                } else if matches!(self.op, Op::Floor | Op::Ceiling) {
                    left.map(|value| {
                        value.integral(
                            &mut context,
                            if self.op == Op::Floor {
                                Rounding::Floor
                            } else {
                                Rounding::Ceiling
                            },
                        )
                    })
                } else if matches!(self.op, Op::Negate | Op::Abs) {
                    left.and_then(|value| {
                        let value = if self.op == Op::Negate {
                            value.negate().0
                        } else {
                            context.abs(value.0)
                        };
                        Decimal::new(&mut context, value)
                    })
                } else if matches!(self.op, Op::Bin | Op::BinAt) {
                    let size = at(1, row, &mut context)?;
                    let origin = if self.op == Op::BinAt {
                        at(2, row, &mut context)?
                    } else {
                        Some(Decimal::ZERO)
                    };
                    match (left, size, origin) {
                        (Some(left), Some(size), Some(origin)) if size > Decimal::ZERO => {
                            let difference = context.sub(left.0, origin.0);
                            let quotient = context.div(difference, size.0);
                            Decimal::new(&mut context, quotient).and_then(|value| {
                                let floor = value.integral(&mut context, Rounding::Floor);
                                let product = context.mul(floor.0, size.0);
                                let value = context.add(origin.0, product);
                                Decimal::new(&mut context, value)
                            })
                        }
                        _ => None,
                    }
                } else {
                    match (left, at(1, row, &mut context)?) {
                        (Some(left), Some(right))
                            if !(matches!(self.op, Op::Divide | Op::Modulo)
                                && right == Decimal::ZERO) =>
                        {
                            let value = match self.op {
                                Op::Add => context.add(left.0, right.0),
                                Op::Subtract => context.sub(left.0, right.0),
                                Op::Multiply => context.mul(left.0, right.0),
                                Op::Divide => context.div(left.0, right.0),
                                Op::Modulo => context.rem(left.0, right.0),
                                _ => unreachable!("conversion is handled before binary arithmetic"),
                            };
                            Decimal::new(&mut context, value)
                        }
                        _ => None,
                    }
                };
                output.append(result.filter(|value| {
                    !self.scalar_semantics || self.op == Op::SortKey || value.0.is_finite()
                }))?;
            }
            output.finish()?
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
