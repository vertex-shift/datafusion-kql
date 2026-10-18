use chrono::{DateTime, NaiveDate, NaiveDateTime};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::ScalarValue;

use crate::ast::{Literal, ScalarType, Span};
use crate::error::{Error, Result};

pub(crate) fn data_type(ty: ScalarType) -> DataType {
    match ty {
        ScalarType::Bool => DataType::Boolean,
        ScalarType::Int => DataType::Int32,
        ScalarType::Long => DataType::Int64,
        ScalarType::Real => DataType::Float64,
        ScalarType::String => DataType::Utf8,
        ScalarType::Datetime => DataType::Timestamp(TimeUnit::Nanosecond, None),
        ScalarType::Timespan => DataType::Duration(TimeUnit::Nanosecond),
    }
}

pub(crate) fn literal_value(literal: &Literal, span: Span) -> Result<ScalarValue> {
    Ok(match literal {
        Literal::Null => ScalarValue::Null,
        Literal::Bool(value) => ScalarValue::Boolean(Some(*value)),
        Literal::Integer(value) => ScalarValue::Int64(Some(*value)),
        Literal::Real(value) => ScalarValue::Float64(Some(*value)),
        Literal::String(value) => ScalarValue::Utf8(Some(value.clone())),
        Literal::Timespan(text) => ScalarValue::DurationNanosecond(Some(timespan(text, span)?)),
        Literal::Typed { ty, value } => {
            let value = unquote(value);
            if value == "null" {
                ScalarValue::try_from(&data_type(*ty))?
            } else {
                match ty {
                    ScalarType::Datetime => {
                        ScalarValue::TimestampNanosecond(Some(datetime(value, span)?), None)
                    }
                    ScalarType::Timespan => {
                        ScalarValue::DurationNanosecond(Some(timespan(value, span)?))
                    }
                    _ => ScalarValue::try_from_string(value.to_owned(), &data_type(*ty))?,
                }
            }
        }
    })
}

fn unquote(text: &str) -> &str {
    if text.len() >= 2
        && ((text.starts_with('"') && text.ends_with('"'))
            || (text.starts_with('\'') && text.ends_with('\'')))
    {
        &text[1..text.len() - 1]
    } else {
        text
    }
}

pub(crate) fn datetime(text: &str, span: Span) -> Result<i64> {
    let text = unquote(text.trim());
    let parsed = if let Ok(value) = DateTime::parse_from_rfc3339(text) {
        value.naive_utc()
    } else if let Ok(value) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        value
            .and_hms_opt(0, 0, 0)
            .expect("midnight is a valid time")
    } else {
        let text = text.strip_suffix('Z').unwrap_or(text);
        ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"]
            .iter()
            .find_map(|format| NaiveDateTime::parse_from_str(text, format).ok())
            .ok_or_else(|| Error::syntax("invalid ISO-8601 datetime literal", span))?
    };
    parsed.and_utc().timestamp_nanos_opt().ok_or_else(|| {
        Error::unsupported(
            "datetime is outside Arrow's signed 64-bit nanosecond timestamp range",
            span,
        )
    })
}

/// Parse in integer arithmetic, avoiding loss of ticks through floating point.
pub(crate) fn timespan(text: &str, span: Span) -> Result<i64> {
    let invalid = || Error::syntax("invalid or out-of-range timespan literal", span);
    let text = unquote(text.trim());
    let (sign, text) = if let Some(text) = text.strip_prefix('-') {
        (-1_i128, text)
    } else {
        (1_i128, text.strip_prefix('+').unwrap_or(text))
    };
    let nanos = if text.contains(':') {
        let mut parts = text.split(':');
        let first = parts.next().ok_or_else(invalid)?;
        let minutes: i128 = parts
            .next()
            .ok_or_else(invalid)?
            .parse()
            .map_err(|_| invalid())?;
        let seconds = parts.next().ok_or_else(invalid)?;
        if parts.next().is_some() || !(0..60).contains(&minutes) {
            return Err(invalid());
        }
        let (days, hours) = if let Some((days, hours)) = first.split_once('.') {
            (
                days.parse::<i128>().map_err(|_| invalid())?,
                hours.parse::<i128>().map_err(|_| invalid())?,
            )
        } else {
            (0, first.parse::<i128>().map_err(|_| invalid())?)
        };
        if days < 0 || !(0..24).contains(&hours) {
            return Err(invalid());
        }
        let seconds = decimal_scaled(seconds, 1_000_000_000).ok_or_else(invalid)?;
        if !(0..60_000_000_000).contains(&seconds) {
            return Err(invalid());
        }
        days.checked_mul(86_400_000_000_000)
            .and_then(|n| n.checked_add(hours * 3_600_000_000_000))
            .and_then(|n| n.checked_add(minutes * 60_000_000_000))
            .and_then(|n| n.checked_add(seconds))
            .ok_or_else(invalid)?
    } else {
        let units = [
            ("microseconds", 1_000_i128),
            ("microsecond", 1_000),
            ("ticks", 100),
            ("tick", 100),
            ("ms", 1_000_000),
            ("us", 1_000),
            ("ns", 1),
            ("d", 86_400_000_000_000),
            ("h", 3_600_000_000_000),
            ("m", 60_000_000_000),
            ("s", 1_000_000_000),
        ];
        let (number, multiplier) = units
            .iter()
            .find_map(|(suffix, multiplier)| text.strip_suffix(suffix).map(|n| (n, *multiplier)))
            .unwrap_or((text, 86_400_000_000_000));
        decimal_scaled(number, multiplier).ok_or_else(invalid)?
    };
    let nanos = nanos.checked_mul(sign).ok_or_else(invalid)?;
    i64::try_from(nanos).map_err(|_| invalid())
}

fn decimal_scaled(text: &str, multiplier: i128) -> Option<i128> {
    let (mantissa, exponent) = match text.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().ok()?),
        None => (text, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if (whole.is_empty() && fraction.is_empty())
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{whole}{fraction}").parse::<i128>().ok()?;
    let scale = i32::try_from(fraction.len()).ok()?.checked_sub(exponent)?;
    if scale.unsigned_abs() > 38 {
        return None;
    }
    let numerator = digits.checked_mul(multiplier)?;
    if scale < 0 {
        numerator.checked_mul(10_i128.checked_pow(scale.unsigned_abs())?)
    } else {
        let denominator = 10_i128.checked_pow(scale as u32)?;
        (numerator % denominator == 0).then_some(numerator / denominator)
    }
}
