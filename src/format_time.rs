use std::sync::Arc;

use chrono::Timelike;
use datafusion::arrow::array::{
    Array, ArrayRef, DurationNanosecondArray, StringBuilder, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::ast::Span;
use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Part {
    Text(String),
    Date(&'static str),
    Fraction { digits: u8, optional: bool },
    AmPm(bool),
    Days(usize),
    Hours(usize),
    Minutes(usize),
    Seconds(usize),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Function {
    parts: Vec<Part>,
    duration: bool,
    signature: Signature,
}

pub(crate) fn call(value: Expr, pattern: &str, duration: bool, span: Span) -> Result<Expr> {
    if pattern.is_empty() {
        return Err(Error::syntax("time format must not be empty", span));
    }
    let chars = pattern.chars().collect::<Vec<_>>();
    let mut parts = Vec::new();
    let mut at = 0;
    while at < chars.len() {
        let ch = chars[at];
        if ch == '\\' {
            at += 1;
            let ch = chars
                .get(at)
                .ok_or_else(|| Error::syntax("unterminated format escape", span))?;
            parts.push(Part::Text(ch.to_string()));
            at += 1;
            continue;
        }
        if ch == '\'' || ch == '"' {
            let quote = ch;
            at += 1;
            let mut text = String::new();
            while at < chars.len() && chars[at] != quote {
                if chars[at] == '\\' {
                    at += 1;
                }
                let ch = chars
                    .get(at)
                    .ok_or_else(|| Error::syntax("unterminated quoted format", span))?;
                text.push(*ch);
                at += 1;
            }
            if at == chars.len() {
                return Err(Error::syntax("unterminated quoted format", span));
            }
            at += 1;
            parts.push(Part::Text(text));
            continue;
        }
        if !ch.is_ascii_alphabetic() || matches!(ch, 'T' | 'Z') {
            parts.push(Part::Text(ch.to_string()));
            at += 1;
            continue;
        }
        let start = at;
        while at < chars.len() && chars[at] == ch {
            at += 1;
        }
        let count = at - start;
        let part = if matches!(ch, 'f' | 'F') && count <= 7 {
            Part::Fraction {
                digits: count as u8,
                optional: ch == 'F',
            }
        } else if duration {
            match (ch, count) {
                ('d', 1..=8) => Part::Days(count),
                ('h', 1..=2) => Part::Hours(count),
                ('m', 1..=2) => Part::Minutes(count),
                ('s', 1..=2) => Part::Seconds(count),
                _ => {
                    return Err(Error::unsupported(
                        format!(
                            "timespan format token '{}'",
                            chars[start..at].iter().collect::<String>()
                        ),
                        span,
                    ));
                }
            }
        } else {
            match (ch, count) {
                ('d', 1) => Part::Date("%-d"),
                ('d', 2) => Part::Date("%d"),
                ('d', 3) => Part::Date("%a"),
                ('d', 4) => Part::Date("%A"),
                ('M', 1) => Part::Date("%-m"),
                ('M', 2) => Part::Date("%m"),
                ('M', 3) => Part::Date("%b"),
                ('M', 4) => Part::Date("%B"),
                ('y', 1) => Part::Date("%-y"),
                ('y', 2) => Part::Date("%y"),
                ('y', 3..=4) => Part::Date("%Y"),
                ('H', 1) => Part::Date("%-H"),
                ('H', 2) => Part::Date("%H"),
                ('h', 1) => Part::Date("%-I"),
                ('h', 2) => Part::Date("%I"),
                ('m', 1) => Part::Date("%-M"),
                ('m', 2) => Part::Date("%M"),
                ('s', 1) => Part::Date("%-S"),
                ('s', 2) => Part::Date("%S"),
                ('t', 1) => Part::AmPm(true),
                ('t', 2) => Part::AmPm(false),
                ('K', 1) => Part::Text("Z".into()),
                ('z', 1) => Part::Text("+0".into()),
                ('z', 2) => Part::Text("+00".into()),
                ('z', 3) => Part::Text("+00:00".into()),
                _ => {
                    return Err(Error::unsupported(
                        format!(
                            "datetime format token '{}'",
                            chars[start..at].iter().collect::<String>()
                        ),
                        span,
                    ));
                }
            }
        };
        parts.push(part);
    }
    Ok(ScalarUDF::from(Function {
        parts,
        duration,
        signature: Signature::exact(
            vec![if duration {
                DataType::Duration(TimeUnit::Nanosecond)
            } else {
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            }],
            Volatility::Immutable,
        ),
    })
    .call(vec![value]))
}

impl Function {
    fn render(&self, value: i64) -> String {
        let date = chrono::DateTime::from_timestamp_nanos(value);
        let absolute = value.unsigned_abs();
        let mut output = if self.duration && value < 0 {
            "-".to_owned()
        } else {
            String::new()
        };
        for part in &self.parts {
            match part {
                Part::Text(text) => output.push_str(text),
                Part::Date(format) => output.push_str(&date.format(format).to_string()),
                Part::Fraction { digits, optional } => {
                    let nanos = if self.duration {
                        (absolute % 1_000_000_000) as u32
                    } else {
                        date.nanosecond()
                    };
                    let fraction = nanos / 10_u32.pow(9 - u32::from(*digits));
                    let text = format!("{fraction:0width$}", width = usize::from(*digits));
                    if *optional && text.trim_end_matches('0').is_empty() && output.ends_with('.') {
                        output.pop();
                    } else {
                        output.push_str(if *optional {
                            text.trim_end_matches('0')
                        } else {
                            &text
                        });
                    }
                }
                Part::AmPm(short) => {
                    output.push_str(match (date.hour() < 12, *short) {
                        (true, true) => "A",
                        (true, false) => "AM",
                        (false, true) => "P",
                        (false, false) => "PM",
                    });
                }
                Part::Days(width) => output.push_str(&format!(
                    "{:0width$}",
                    absolute / 86_400_000_000_000,
                    width = *width
                )),
                Part::Hours(width) => output.push_str(&format!(
                    "{:0width$}",
                    absolute / 3_600_000_000_000 % 24,
                    width = *width
                )),
                Part::Minutes(width) => output.push_str(&format!(
                    "{:0width$}",
                    absolute / 60_000_000_000 % 60,
                    width = *width
                )),
                Part::Seconds(width) => output.push_str(&format!(
                    "{:0width$}",
                    absolute / 1_000_000_000 % 60,
                    width = *width
                )),
            }
        }
        output
    }
}

impl ScalarUDFImpl for Function {
    fn name(&self) -> &str {
        if self.duration {
            "__kql_format_timespan"
        } else {
            "__kql_format_datetime"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let source = args.args[0].to_array(1)?;
        let mut output = StringBuilder::new();
        if self.duration {
            let source = source
                .as_any()
                .downcast_ref::<DurationNanosecondArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "format_timespan requires timespan".into(),
                    )
                })?;
            for value in source.iter() {
                output.append_value(value.map(|value| self.render(value)).unwrap_or_default());
            }
        } else {
            let source = source
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "format_datetime requires datetime".into(),
                    )
                })?;
            for value in source.iter() {
                output.append_value(value.map(|value| self.render(value)).unwrap_or_default());
            }
        }
        let array = Arc::new(output.finish()) as ArrayRef;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &array, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(array))
        }
    }
}
