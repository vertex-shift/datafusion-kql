use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, DurationNanosecondArray, Int64Array, Int64Builder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use regex::{Regex, RegexBuilder};

use crate::ast::Span;
use crate::telemetry_text::bounded;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Rewrite {
    Text(String),
    Capture(usize),
}

#[derive(Debug, Clone)]
enum Mode {
    IndexOf(Option<i64>),
    Repeat,
    TimespanText,
    Replace {
        pattern: String,
        compiled: Arc<Regex>,
        rewrite: Vec<Rewrite>,
    },
}

#[derive(Debug, Clone)]
struct Function {
    mode: Mode,
    signature: Signature,
}

impl PartialEq for Function {
    fn eq(&self, other: &Self) -> bool {
        match (&self.mode, &other.mode) {
            (Mode::IndexOf(a), Mode::IndexOf(b)) => a == b,
            (Mode::Repeat, Mode::Repeat) => true,
            (Mode::TimespanText, Mode::TimespanText) => true,
            (
                Mode::Replace {
                    pattern: a,
                    rewrite: ar,
                    ..
                },
                Mode::Replace {
                    pattern: b,
                    rewrite: br,
                    ..
                },
            ) => a == b && ar == br,
            _ => false,
        }
    }
}
impl Eq for Function {}
impl Hash for Function {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(&self.mode).hash(state);
        match &self.mode {
            Mode::IndexOf(value) => value.hash(state),
            Mode::Replace {
                pattern, rewrite, ..
            } => {
                pattern.hash(state);
                rewrite.hash(state);
            }
            Mode::Repeat | Mode::TimespanText => {}
        }
    }
}

pub(crate) fn indexof(values: Vec<Expr>, occurrence: Option<i64>) -> Expr {
    ScalarUDF::from(Function {
        mode: Mode::IndexOf(occurrence),
        signature: Signature::exact(
            vec![
                DataType::Utf8,
                DataType::Utf8,
                DataType::Int64,
                DataType::Int64,
            ],
            Volatility::Immutable,
        ),
    })
    .call(values)
}

pub(crate) fn strrep(values: Vec<Expr>) -> Expr {
    ScalarUDF::from(Function {
        mode: Mode::Repeat,
        signature: Signature::exact(
            vec![DataType::Utf8, DataType::Int64, DataType::Utf8],
            Volatility::Immutable,
        ),
    })
    .call(values)
}

pub(crate) fn timespan(value: Expr) -> Expr {
    ScalarUDF::from(Function {
        mode: Mode::TimespanText,
        signature: Signature::exact(
            vec![DataType::Duration(TimeUnit::Nanosecond)],
            Volatility::Immutable,
        ),
    })
    .call(vec![value])
}

pub(crate) fn replace_regex(
    source: Expr,
    pattern: String,
    replacement: String,
    span: Span,
) -> crate::Result<Expr> {
    let compiled = RegexBuilder::new(&pattern)
        .size_limit(16 << 20)
        .build()
        .map_err(|error| {
            crate::Error::syntax(format!("invalid replacement regex: {error}"), span)
        })?;
    let mut rewrite = Vec::new();
    let mut text = String::new();
    let mut chars = replacement.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            text.push(ch);
            continue;
        }
        match chars.next() {
            Some('\\') => text.push('\\'),
            Some(ch @ '0'..='9') => {
                let index = (ch as u8 - b'0') as usize;
                if index >= compiled.captures_len() {
                    return Err(crate::Error::syntax(
                        "replacement references an unknown capture group",
                        span,
                    ));
                }
                if !text.is_empty() {
                    rewrite.push(Rewrite::Text(std::mem::take(&mut text)));
                }
                rewrite.push(Rewrite::Capture(index));
            }
            _ => {
                return Err(crate::Error::syntax(
                    "replacement escapes must be \\\\ or capture references \\0 through \\9",
                    span,
                ));
            }
        }
    }
    if !text.is_empty() {
        rewrite.push(Rewrite::Text(text));
    }
    Ok(ScalarUDF::from(Function {
        mode: Mode::Replace {
            pattern,
            compiled: Arc::new(compiled),
            rewrite,
        },
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
    })
    .call(vec![source]))
}

fn append(output: &mut String, text: &str) -> Result<()> {
    bounded(
        output
            .len()
            .checked_add(text.len())
            .ok_or_else(|| DataFusionError::Execution("text output length overflow".into()))?,
    )?;
    output.push_str(text);
    Ok(())
}

fn find(source: &str, needle: &str, start: i64, length: i64, occurrence: i64) -> Option<i64> {
    if length < -1 || occurrence <= 0 {
        return None;
    }
    let count = if source.is_ascii() {
        source.len()
    } else {
        source.chars().count()
    } as i64;
    let start = if start < 0 {
        count.checked_add(start)?
    } else {
        start
    };
    if start < 0 {
        return None;
    }
    if start > count {
        return Some(-1);
    }
    let byte = |index: i64| {
        if source.is_ascii() {
            index as usize
        } else {
            source
                .char_indices()
                .nth(index as usize)
                .map_or(source.len(), |(byte, _)| byte)
        }
    };
    let mut byte_start = byte(start);
    let mut position = start;
    let needle_length = needle.chars().count() as i64;
    for current in 1..=occurrence {
        let window = &source[byte_start..];
        let Some(offset) = window.find(needle) else {
            return Some(-1);
        };
        let distance = if source.is_ascii() {
            offset
        } else {
            window[..offset].chars().count()
        } as i64;
        if length != -1 && distance.saturating_add(needle_length) > length {
            return Some(-1);
        };
        position += distance;
        if current == occurrence {
            return Some(position);
        };
        byte_start += offset;
        if needle.is_empty() {
            let Some(ch) = source[byte_start..].chars().next() else {
                return Some(-1);
            };
            byte_start += ch.len_utf8();
            position += 1;
        } else {
            byte_start += needle.len();
            position += needle_length;
        }
    }
    Some(-1)
}

impl ScalarUDFImpl for Function {
    fn name(&self) -> &str {
        match self.mode {
            Mode::IndexOf(_) => "__kql_indexof",
            Mode::Repeat => "__kql_strrep",
            Mode::TimespanText => "__kql_timespan_text",
            Mode::Replace { .. } => "__kql_replace_regex",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(if matches!(self.mode, Mode::IndexOf(_)) {
            DataType::Int64
        } else {
            DataType::Utf8
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let count = if scalar { 1 } else { args.number_rows };
        let arrays = args
            .args
            .iter()
            .map(|value| value.to_array(1))
            .collect::<Result<Vec<_>>>()?;
        if matches!(self.mode, Mode::TimespanText) {
            let values = arrays[0]
                .as_any()
                .downcast_ref::<DurationNanosecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("timespan text requires nanosecond duration".into())
                })?;
            let mut output = StringBuilder::new();
            for value in values {
                let Some(value) = value else {
                    output.append_value("");
                    continue;
                };
                let absolute = value.unsigned_abs();
                let days = absolute / 86_400_000_000_000;
                let mut text = if value < 0 {
                    "-".to_owned()
                } else {
                    String::new()
                };
                if days != 0 {
                    text.push_str(&format!("{days}."));
                }
                text.push_str(&format!(
                    "{:02}:{:02}:{:02}",
                    absolute / 3_600_000_000_000 % 24,
                    absolute / 60_000_000_000 % 60,
                    absolute / 1_000_000_000 % 60
                ));
                let fraction = absolute % 1_000_000_000;
                if fraction != 0 {
                    if fraction.is_multiple_of(100) {
                        text.push_str(&format!(".{:07}", fraction / 100));
                    } else {
                        text.push_str(&format!(".{fraction:09}"));
                    }
                }
                output.append_value(text);
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
        let row_at = |index: usize, row: usize| {
            if matches!(args.args[index], ColumnarValue::Scalar(_)) {
                0
            } else {
                row
            }
        };
        let string = |index: usize, row| -> Result<&str> {
            let array = arrays[index]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("text kernel requires UTF-8 input".into())
                })?;
            let row = row_at(index, row);
            Ok(if array.is_null(row) {
                ""
            } else {
                array.value(row)
            })
        };
        let integer = |index: usize, row| -> Result<Option<i64>> {
            let array = arrays[index]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("text kernel requires integer input".into())
                })?;
            let row = row_at(index, row);
            Ok((!array.is_null(row)).then(|| array.value(row)))
        };
        let mut strings = StringBuilder::new();
        let mut longs = Int64Builder::with_capacity(count);
        for row in 0..count {
            let source = string(0, row)?;
            bounded(source.len())?;
            match &self.mode {
                Mode::IndexOf(occurrence) => {
                    let needle = string(1, row)?;
                    bounded(needle.len())?;
                    let result = match (integer(2, row)?, integer(3, row)?, occurrence) {
                        (Some(start), Some(length), Some(occurrence)) => {
                            find(source, needle, start, length, *occurrence)
                        }
                        _ => None,
                    };
                    longs.append_option(result);
                }
                Mode::Repeat => {
                    let multiplier = integer(1, row)?.unwrap_or(0).max(0) as usize;
                    if multiplier > 67_108_864 {
                        return Err(DataFusionError::Execution(
                            "strrep multiplier exceeds 67108864".into(),
                        ));
                    }
                    let delimiter = string(2, row)?;
                    bounded(delimiter.len())?;
                    let size = source
                        .len()
                        .checked_mul(multiplier)
                        .and_then(|size| {
                            size.checked_add(
                                delimiter.len().checked_mul(multiplier.saturating_sub(1))?,
                            )
                        })
                        .ok_or_else(|| {
                            DataFusionError::Execution("strrep output length overflow".into())
                        })?;
                    bounded(size)?;
                    let mut result = String::with_capacity(size);
                    if size != 0 {
                        for index in 0..multiplier {
                            if index != 0 {
                                result.push_str(delimiter);
                            }
                            result.push_str(source);
                        }
                    }
                    strings.append_value(result);
                }
                Mode::Replace {
                    compiled, rewrite, ..
                } => {
                    let mut result = String::with_capacity(source.len());
                    let mut previous = 0;
                    for (index, captures) in compiled.captures_iter(source).enumerate() {
                        if index >= crate::dynamic::MAX_COLLECTION_SIZE {
                            return Err(DataFusionError::Execution(
                                "replace_regex exceeds its match budget".into(),
                            ));
                        }
                        let matched = captures.get(0).ok_or_else(|| {
                            DataFusionError::Internal("replacement regex has no whole match".into())
                        })?;
                        append(&mut result, &source[previous..matched.start()])?;
                        for item in rewrite {
                            let text = match item {
                                Rewrite::Text(text) => text,
                                Rewrite::Capture(index) => {
                                    captures.get(*index).map_or("", |capture| capture.as_str())
                                }
                            };
                            append(&mut result, text)?;
                        }
                        previous = matched.end();
                    }
                    append(&mut result, &source[previous..])?;
                    strings.append_value(result);
                }
                Mode::TimespanText => unreachable!("timespan formatting uses a typed batch path"),
            }
        }
        let output: ArrayRef = if matches!(self.mode, Mode::IndexOf(_)) {
            Arc::new(longs.finish())
        } else {
            Arc::new(strings.finish())
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
