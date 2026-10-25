use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanBuilder, Int64Array, Int64Builder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::expr_fn::cast;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::lit;
use serde_json::Value as Json;

use crate::ast::Span;
use crate::dynamic::{self, Kernel};
use crate::expressions::Expressions;
use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Op {
    Parse4,
    Parse4Mask,
    Format4,
    Format4Mask,
    Mask4,
    Private4,
    Compare4,
    Match4,
    In4,
    Any4,
    Parse6,
    Compare6,
    Match6,
    In6,
    Any6,
}

impl Op {
    pub(crate) fn lookup(name: &str) -> Option<Self> {
        Some(match name {
            "parse_ipv4" => Self::Parse4,
            "parse_ipv4_mask" => Self::Parse4Mask,
            "format_ipv4" => Self::Format4,
            "format_ipv4_mask" => Self::Format4Mask,
            "ipv4_netmask_suffix" => Self::Mask4,
            "ipv4_is_private" => Self::Private4,
            "ipv4_compare" => Self::Compare4,
            "ipv4_is_match" => Self::Match4,
            "ipv4_is_in_range" => Self::In4,
            "ipv4_is_in_any_range" => Self::Any4,
            "parse_ipv6" => Self::Parse6,
            "ipv6_compare" => Self::Compare6,
            "ipv6_is_match" => Self::Match6,
            "ipv6_is_in_range" => Self::In6,
            "ipv6_is_in_any_range" => Self::Any6,
            _ => return None,
        })
    }
    fn name(self) -> &'static str {
        match self {
            Self::Parse4 => "__kql_parse_ipv4",
            Self::Parse4Mask => "__kql_parse_ipv4_mask",
            Self::Format4 => "__kql_format_ipv4",
            Self::Format4Mask => "__kql_format_ipv4_mask",
            Self::Mask4 => "__kql_ipv4_netmask_suffix",
            Self::Private4 => "__kql_ipv4_is_private",
            Self::Compare4 => "__kql_ipv4_compare",
            Self::Match4 => "__kql_ipv4_is_match",
            Self::In4 => "__kql_ipv4_is_in_range",
            Self::Any4 => "__kql_ipv4_is_in_any_range",
            Self::Parse6 => "__kql_parse_ipv6",
            Self::Compare6 => "__kql_ipv6_compare",
            Self::Match6 => "__kql_ipv6_is_match",
            Self::In6 => "__kql_ipv6_is_in_range",
            Self::Any6 => "__kql_ipv6_is_in_any_range",
        }
    }
    fn output(self) -> DataType {
        match self {
            Self::Format4 | Self::Format4Mask | Self::Parse6 => DataType::Utf8,
            Self::Private4
            | Self::Match4
            | Self::Match6
            | Self::In4
            | Self::In6
            | Self::Any4
            | Self::Any6 => DataType::Boolean,
            _ => DataType::Int64,
        }
    }
    fn v6(self) -> bool {
        matches!(
            self,
            Self::Parse6 | Self::Compare6 | Self::Match6 | Self::In6 | Self::Any6
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct Address {
    value: u128,
    prefix: u32,
}

fn prefix(text: &str, max: u32) -> Option<u32> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse::<u32>().ok().filter(|p| *p <= max)
}

fn split(text: &str) -> Option<(&str, Option<&str>)> {
    if let Some((host, prefix)) = text.split_once('/') {
        if prefix.contains('/') {
            None
        } else {
            Some((host, Some(prefix)))
        }
    } else {
        Some((text, None))
    }
}

fn ipv4(text: &str) -> Option<Address> {
    let (host, mask) = split(text)?;
    let mut value = 0_u32;
    let mut parts = host.split('.');
    for _ in 0..4 {
        let part = parts.next()?;
        if part.is_empty() || part.len() > 3 || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let octet = part.parse::<u8>().ok()?;
        value = (value << 8) | u32::from(octet);
    }
    if parts.next().is_some() {
        return None;
    }
    Some(Address {
        value: u128::from(value),
        prefix: if let Some(mask) = mask {
            prefix(mask, 32)?
        } else {
            32
        },
    })
}

fn ipv6(text: &str) -> Option<Address> {
    if !text.contains(':') {
        let address = ipv4(text)?;
        return Some(Address {
            value: (0xffff_u128 << 32) | address.value,
            prefix: 96 + address.prefix,
        });
    }
    let (host, mask) = split(text)?;
    let address = host.parse::<Ipv6Addr>().ok()?;
    let value = u128::from(address);
    let prefix = if let Some(mask) = mask {
        prefix(mask, 128)?
    } else {
        128
    };
    Some(Address { value, prefix })
}

fn mask(value: u128, prefix: u32, width: u32) -> u128 {
    if prefix == 0 {
        0
    } else {
        value & (u128::MAX << (width - prefix))
    }
}

fn parsed(text: &str, v6: bool) -> Option<Address> {
    if v6 { ipv6(text) } else { ipv4(text) }
}

fn canonical6(value: u128) -> String {
    Ipv6Addr::from(value)
        .segments()
        .map(|part| format!("{part:04x}"))
        .join(":")
}

#[derive(Debug)]
struct Ranges {
    key: String,
    intervals: Option<Vec<(u128, u128)>>,
}

fn ranges(key: &str, v6: bool) -> DataFusionResult<Ranges> {
    if key.len() > 1_048_576 {
        return Err(DataFusionError::Execution(
            "IP range list exceeds its 1 MiB text budget".into(),
        ));
    }
    fn collect(value: &Json, v6: bool, output: &mut Vec<(u128, u128)>) -> DataFusionResult<bool> {
        if let Json::Array(items) = value {
            for item in items {
                if !collect(item, v6, output)? {
                    return Ok(false);
                }
            }
        } else {
            let Some(address) = value.as_str().and_then(|value| parsed(value, v6)) else {
                return Ok(false);
            };
            if output.len() >= dynamic::MAX_COLLECTION_SIZE {
                return Err(DataFusionError::Execution(
                    "IP range list exceeds its collection budget".into(),
                ));
            }
            let width = if v6 { 128 } else { 32 };
            let start = mask(address.value, address.prefix, width);
            let host = if address.prefix == 0 {
                if v6 { u128::MAX } else { u32::MAX.into() }
            } else {
                (1_u128 << (width - address.prefix)) - 1
            };
            output.push((start, start | host));
        }
        Ok(true)
    }
    let value: Json = serde_json::from_str(key)
        .map_err(|error| DataFusionError::Execution(format!("invalid IP range array: {error}")))?;
    let mut intervals = Vec::new();
    let valid = collect(&value, v6, &mut intervals)?;
    intervals.sort_unstable();
    let mut merged = Vec::<(u128, u128)>::new();
    for (start, end) in intervals {
        if let Some(last) = merged.last_mut()
            && start <= last.1.saturating_add(1)
        {
            last.1 = last.1.max(end);
        } else {
            merged.push((start, end));
        }
    }
    Ok(Ranges {
        key: key.into(),
        intervals: valid.then_some(merged),
    })
}

#[derive(Debug)]
struct Function {
    op: Op,
    signature: Signature,
    cache: Mutex<Option<Arc<Ranges>>>,
}
impl PartialEq for Function {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op
    }
}
impl Eq for Function {}
impl Hash for Function {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.op.hash(state);
    }
}

pub(crate) fn plan(
    expressions: &Expressions<'_>,
    op: Op,
    mut args: Vec<Expr>,
    schema: &datafusion::common::DFSchema,
    span: Span,
) -> Result<Expr> {
    let valid = match op {
        Op::Parse4 | Op::Parse6 | Op::Private4 | Op::Mask4 => args.len() == 1,
        Op::Parse4Mask | Op::In4 | Op::In6 => args.len() == 2,
        Op::Compare4 | Op::Compare6 | Op::Match4 | Op::Match6 => (2..=3).contains(&args.len()),
        Op::Format4 | Op::Format4Mask => (1..=2).contains(&args.len()),
        Op::Any4 | Op::Any6 => args.len() >= 2,
    };
    if !valid {
        return Err(Error::syntax(
            "IP function received an invalid argument count",
            span,
        ));
    }
    args[0] = cast(
        expressions.string_value(args[0].clone(), schema)?,
        DataType::Utf8,
    );
    if matches!(op, Op::Any4 | Op::Any6 | Op::In4 | Op::In6) {
        let rhs = dynamic::call(
            Kernel::EncodeJson,
            vec![dynamic::call(Kernel::PackArray, args[1..].to_vec())],
        );
        args = vec![args[0].clone(), rhs];
    } else {
        let mask_at = if matches!(op, Op::Compare4 | Op::Compare6 | Op::Match4 | Op::Match6) {
            args[1] = cast(
                expressions.string_value(args[1].clone(), schema)?,
                DataType::Utf8,
            );
            Some(2)
        } else if matches!(op, Op::Parse4Mask | Op::Format4 | Op::Format4Mask) {
            Some(1)
        } else {
            None
        };
        if let Some(index) = mask_at {
            if args.len() == index {
                args.push(lit(if op.v6() { 128_i64 } else { 32_i64 }));
            }
            let ty = args[index].get_type(schema)?;
            if !ty.is_integer() && ty != DataType::Null {
                return Err(Error::syntax(
                    "IP prefix masks require integer values",
                    span,
                ));
            }
            args[index] = cast(args[index].clone(), DataType::Int64);
        }
    }
    Ok(ScalarUDF::from(Function {
        op,
        signature: Signature::any(args.len(), Volatility::Immutable),
        cache: Mutex::new(None),
    })
    .call(args))
}

impl ScalarUDFImpl for Function {
    fn name(&self) -> &str {
        self.op.name()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> {
        Ok(self.op.output())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        let arrays = args
            .args
            .iter()
            .map(|arg| arg.to_array(1))
            .collect::<DataFusionResult<Vec<_>>>()?;
        let mut longs = Int64Builder::with_capacity(rows);
        let mut bools = BooleanBuilder::with_capacity(rows);
        let mut strings = StringBuilder::new();
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| DataFusionError::Execution("IP range cache is poisoned".into()))?
            .clone();
        for row in 0..rows {
            let index = |arg: usize| {
                if matches!(args.args[arg], ColumnarValue::Scalar(_)) {
                    0
                } else {
                    row
                }
            };
            let text = |arg: usize| -> DataFusionResult<Option<&str>> {
                let array = arrays[arg]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("IP function expects strings".into())
                    })?;
                Ok((!array.is_null(index(arg))).then(|| array.value(index(arg))))
            };
            let integer = |arg: usize| -> DataFusionResult<Option<i64>> {
                let array = arrays[arg]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("IP masks require long values".into())
                    })?;
                Ok((!array.is_null(index(arg))).then(|| array.value(index(arg))))
            };
            let source_text = text(0)?;
            let left = source_text.and_then(|value| parsed(value, self.op.v6()));
            match self.op {
                Op::Parse4 | Op::Parse4Mask | Op::Mask4 => {
                    let value = if let Some(address) = left {
                        if self.op == Op::Mask4 {
                            Some(i64::from(address.prefix))
                        } else {
                            let prefix = if self.op == Op::Parse4Mask {
                                integer(1)?
                                    .and_then(|v| u32::try_from(v).ok())
                                    .filter(|p| *p <= 32)
                            } else {
                                Some(32)
                            };
                            prefix.map(|prefix| {
                                mask(address.value, address.prefix.min(prefix), 32) as i64
                            })
                        }
                    } else {
                        None
                    };
                    longs.append_option(value);
                }
                Op::Format4 | Op::Format4Mask => {
                    let left = left.or_else(|| {
                        source_text
                            .and_then(|value| value.parse::<u32>().ok())
                            .map(|value| Address {
                                value: value.into(),
                                prefix: 32,
                            })
                    });
                    let value = left.zip(integer(1)?).and_then(|(address, prefix)| {
                        let prefix = u32::try_from(prefix)
                            .ok()
                            .filter(|p| *p <= 32)?
                            .min(address.prefix);
                        let value =
                            Ipv4Addr::from(mask(address.value, prefix, 32) as u32).to_string();
                        Some(if self.op == Op::Format4Mask {
                            format!("{value}/{prefix}")
                        } else {
                            value
                        })
                    });
                    strings.append_value(value.unwrap_or_default());
                }
                Op::Parse6 => strings.append_value(
                    left.map(|address| canonical6(mask(address.value, address.prefix, 128)))
                        .unwrap_or_default(),
                ),
                Op::Private4 => bools.append_option(left.map(|address| {
                    Ipv4Addr::from(mask(address.value, address.prefix, 32) as u32).is_private()
                })),
                Op::Compare4 | Op::Match4 | Op::Compare6 | Op::Match6 => {
                    let width = if self.op.v6() { 128 } else { 32 };
                    let right = text(1)?.and_then(|value| parsed(value, self.op.v6()));
                    let compare =
                        left.zip(right)
                            .zip(integer(2)?)
                            .and_then(|((left, right), prefix)| {
                                let prefix = u32::try_from(prefix)
                                    .ok()
                                    .filter(|p| *p <= width)?
                                    .min(left.prefix)
                                    .min(right.prefix);
                                Some(
                                    match mask(left.value, prefix, width).cmp(&mask(
                                        right.value,
                                        prefix,
                                        width,
                                    )) {
                                        Ordering::Less => -1,
                                        Ordering::Equal => 0,
                                        Ordering::Greater => 1,
                                    },
                                )
                            });
                    if matches!(self.op, Op::Match4 | Op::Match6) {
                        bools.append_option(compare.map(|value| value == 0));
                    } else {
                        longs.append_option(compare);
                    }
                }
                Op::In4 | Op::Any4 | Op::In6 | Op::Any6 => {
                    let key = text(1)?.ok_or_else(|| {
                        DataFusionError::Execution("IP range source has no array value".into())
                    })?;
                    if cache.as_ref().is_none_or(|ranges| ranges.key != key) {
                        let ranges = Arc::new(ranges(key, self.op.v6())?);
                        *self.cache.lock().map_err(|_| {
                            DataFusionError::Execution("IP range cache is poisoned".into())
                        })? = Some(Arc::clone(&ranges));
                        cache = Some(ranges);
                    }
                    let ranges = cache.as_ref().ok_or_else(|| {
                        DataFusionError::Internal("IP range cache was not initialized".into())
                    })?;
                    let found = left
                        .zip(ranges.intervals.as_ref())
                        .map(|(address, ranges)| {
                            let address = mask(
                                address.value,
                                address.prefix,
                                if self.op.v6() { 128 } else { 32 },
                            );
                            let index = ranges.partition_point(|(start, _)| *start <= address);
                            index > 0 && address <= ranges[index - 1].1
                        });
                    bools.append_option(found);
                }
            }
        }
        let output: ArrayRef = match self.op.output() {
            DataType::Utf8 => Arc::new(strings.finish()),
            DataType::Boolean => Arc::new(bools.finish()),
            _ => Arc::new(longs.finish()),
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
