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
    IndexOf(i64),
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

pub(crate) fn indexof(values: Vec<Expr>, occurrence: i64) -> Expr {
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

pub(crate) fn format_timespan(array: &ArrayRef) -> Result<ArrayRef> {
    let values = array
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
    Ok(Arc::new(output.finish()))
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
    if source.is_empty() {
        return Some(-1);
    }
    if needle.is_empty() {
        return Some(0);
    }
    let ascii = source.is_ascii();
    let count = if ascii {
        source.len()
    } else {
        source.chars().count()
    } as i64;
    let start = if start < 0 {
        count.checked_add(start)?.max(0)
    } else {
        start
    };
    if start > count {
        return Some(-1);
    }
    let byte = |index: i64| {
        if ascii {
            index as usize
        } else {
            source
                .char_indices()
                .nth(index as usize)
                .map_or(source.len(), |(byte, _)| byte)
        }
    };
    let byte_start = byte(start);
    let limit = byte(if length == -1 {
        count
    } else {
        start.saturating_add(length).min(count)
    });
    if byte_start >= limit {
        return Some(-1);
    }
    let mut end = limit.saturating_add(needle.len() - 1).min(source.len());
    let found = if occurrence == 1 {
        while !source.is_char_boundary(end) {
            end += 1;
        }
        source[byte_start..end].find(needle)
    } else {
        overlapping(
            &source.as_bytes()[byte_start..end],
            needle.as_bytes(),
            occurrence,
        )
    };
    let Some(offset) = found else {
        return Some(-1);
    };
    if byte_start + offset >= limit {
        return Some(-1);
    }
    Some(
        start
            + if ascii {
                offset
            } else {
                source[byte_start..byte_start + offset].chars().count()
            } as i64,
    )
}

fn overlapping(source: &[u8], needle: &[u8], occurrence: i64) -> Option<usize> {
    if needle.len() > source.len() {
        return None;
    }
    // Prefix reuse keeps overlapping searches linear. Input byte budgets keep
    // the failure-table offsets within u32.
    let mut prefix = vec![0_u32; needle.len()];
    let mut matched = 0;
    for index in 1..needle.len() {
        while matched > 0 && needle[index] != needle[matched] {
            matched = prefix[matched - 1] as usize;
        }
        if needle[index] == needle[matched] {
            matched += 1;
        }
        prefix[index] = matched as u32;
    }
    matched = 0;
    let mut found = 0;
    for (index, byte) in source.iter().enumerate() {
        while matched > 0 && *byte != needle[matched] {
            matched = prefix[matched - 1] as usize;
        }
        if *byte == needle[matched] {
            matched += 1;
        }
        if matched == needle.len() {
            found += 1;
            if found == occurrence {
                return Some(index + 1 - needle.len());
            }
            matched = prefix[matched - 1] as usize;
        }
    }
    None
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
            let output = format_timespan(&arrays[0])?;
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
                    let result = match (integer(2, row)?, integer(3, row)?) {
                        (Some(start), Some(length)) => {
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

#[cfg(test)]
mod tests {
    use super::find;

    fn reference(
        source: &str,
        needle: &str,
        start: i64,
        length: i64,
        occurrence: i64,
    ) -> Option<i64> {
        if length < -1 || occurrence <= 0 {
            return None;
        }
        if source.is_empty() {
            return Some(-1);
        }
        if needle.is_empty() {
            return Some(0);
        }
        let count = source.chars().count() as i64;
        let start = if start < 0 {
            (count + start).max(0)
        } else {
            start
        };
        let end = if length == -1 {
            count
        } else {
            start.saturating_add(length)
        };
        Some(
            source
                .char_indices()
                .enumerate()
                .filter(|(position, (byte, _))| {
                    (*position as i64) >= start
                        && (*position as i64) < end
                        && source[*byte..].starts_with(needle)
                })
                .nth((occurrence - 1) as usize)
                .map_or(-1, |(position, _)| position as i64),
        )
    }

    #[test]
    fn overlapping_search_matches_a_character_position_oracle() {
        for source in [
            "",
            "abc",
            "aaaa",
            "ababab",
            "\u{e9}\u{e9}\u{e9}",
            "\u{e9}\u{4e2d}\u{1f642}abc",
            "\u{1f642}a\u{1f642}a",
        ] {
            for needle in [
                "",
                "a",
                "ab",
                "aa",
                "b",
                "\u{e9}\u{e9}",
                "\u{4e2d}\u{1f642}",
                "\u{1f642}a",
                "missing",
            ] {
                for start in [-20, -6, -1, 0, 1, 2, 5, 20] {
                    for length in [-2, -1, 0, 1, 2, 8] {
                        for occurrence in [0, 1, 2, 4, 20] {
                            assert_eq!(
                                find(source, needle, start, length, occurrence),
                                reference(source, needle, start, length, occurrence),
                                "source={source:?},needle={needle:?},start={start},length={length},occurrence={occurrence}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn repeated_prefixes_do_not_require_repeated_full_pattern_scans() {
        let source = "a".repeat(100_000);
        let needle = "a".repeat(50_000);
        assert_eq!(find(&source, &needle, 0, -1, 50_001), Some(50_000));
        assert_eq!(find(&source, &needle, 0, -1, i64::MAX), Some(-1));
        assert_eq!(find(&source, &needle, 0, 1, 2), Some(-1));
    }
}
