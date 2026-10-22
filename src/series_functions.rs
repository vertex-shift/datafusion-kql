use datafusion::common::{DataFusionError, Result};
use serde_json::{Map, Number, Value as Json};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Op {
    Add,
    Subtract,
    Multiply,
    Divide,
    Pow,
    Equals,
    NotEquals,
    Less,
    LessEquals,
    Greater,
    GreaterEquals,
    Abs,
    Acos,
    Asin,
    Atan,
    Cos,
    Sin,
    Tan,
    Exp,
    Log,
    Log10,
    Sqrt,
    Floor,
    Ceiling,
    Sign,
    FillForward,
    FillBackward,
    FillConst,
    FillLinear,
    Stats,
    FitLine,
    Fir,
    Iir,
}

impl Op {
    pub fn name(self) -> &'static str {
        match self {
            Self::Add => "series_add",
            Self::Subtract => "series_subtract",
            Self::Multiply => "series_multiply",
            Self::Divide => "series_divide",
            Self::Pow => "series_pow",
            Self::Equals => "series_equals",
            Self::NotEquals => "series_not_equals",
            Self::Less => "series_less",
            Self::LessEquals => "series_less_equals",
            Self::Greater => "series_greater",
            Self::GreaterEquals => "series_greater_equals",
            Self::Abs => "series_abs",
            Self::Acos => "series_acos",
            Self::Asin => "series_asin",
            Self::Atan => "series_atan",
            Self::Cos => "series_cos",
            Self::Sin => "series_sin",
            Self::Tan => "series_tan",
            Self::Exp => "series_exp",
            Self::Log => "series_log",
            Self::Log10 => "series_log10",
            Self::Sqrt => "series_sqrt",
            Self::Floor => "series_floor",
            Self::Ceiling => "series_ceiling",
            Self::Sign => "series_sign",
            Self::FillForward => "series_fill_forward",
            Self::FillBackward => "series_fill_backward",
            Self::FillConst => "series_fill_const",
            Self::FillLinear => "series_fill_linear",
            Self::Stats => "series_stats_dynamic",
            Self::FitLine => "series_fit_line_dynamic",
            Self::Fir => "series_fir",
            Self::Iir => "series_iir",
        }
    }

    pub fn lookup(name: &str) -> Option<Self> {
        Some(match name {
            "series_add" => Self::Add,
            "series_subtract" => Self::Subtract,
            "series_multiply" => Self::Multiply,
            "series_divide" => Self::Divide,
            "series_pow" => Self::Pow,
            "series_equals" => Self::Equals,
            "series_not_equals" => Self::NotEquals,
            "series_less" => Self::Less,
            "series_less_equals" => Self::LessEquals,
            "series_greater" => Self::Greater,
            "series_greater_equals" => Self::GreaterEquals,
            "series_abs" => Self::Abs,
            "series_acos" => Self::Acos,
            "series_asin" => Self::Asin,
            "series_atan" => Self::Atan,
            "series_cos" => Self::Cos,
            "series_sin" => Self::Sin,
            "series_tan" => Self::Tan,
            "series_exp" => Self::Exp,
            "series_log" => Self::Log,
            "series_log10" => Self::Log10,
            "series_sqrt" => Self::Sqrt,
            "series_floor" => Self::Floor,
            "series_ceiling" => Self::Ceiling,
            "series_sign" => Self::Sign,
            "series_fill_forward" => Self::FillForward,
            "series_fill_backward" => Self::FillBackward,
            "series_fill_const" => Self::FillConst,
            "series_fill_linear" => Self::FillLinear,
            "series_stats_dynamic" => Self::Stats,
            "series_fit_line_dynamic" => Self::FitLine,
            "series_fir" => Self::Fir,
            "series_iir" => Self::Iir,
            _ => return None,
        })
    }

    pub fn validate(self, len: usize) -> Result<()> {
        let valid = match self {
            Self::Add
            | Self::Subtract
            | Self::Multiply
            | Self::Divide
            | Self::Pow
            | Self::Equals
            | Self::NotEquals
            | Self::Less
            | Self::LessEquals
            | Self::Greater
            | Self::GreaterEquals => len == 2,
            Self::FillForward | Self::FillBackward | Self::Stats => (1..=2).contains(&len),
            Self::FillConst => (2..=3).contains(&len),
            Self::FillLinear => (1..=4).contains(&len),
            Self::Fir => (2..=4).contains(&len),
            Self::Iir => len == 3,
            _ => len == 1,
        };
        if valid {
            Ok(())
        } else {
            Err(error(format!("invalid argument count for {}", self.name())))
        }
    }
}

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn real(value: f64) -> Json {
    Number::from_f64(value)
        .map(Json::Number)
        .unwrap_or_else(|| Json::String(value.to_string()))
}

fn boolean(args: &[Json], index: usize, default: bool) -> Result<bool> {
    match args.get(index) {
        None => Ok(default),
        Some(Json::Bool(value)) => Ok(*value),
        _ => Err(error("series flag argument must be boolean")),
    }
}

pub(crate) fn evaluate(op: Op, args: &[Json]) -> Result<Json> {
    op.validate(args.len())?;
    let Some(series) = args[0].as_array() else {
        return Ok(Json::Null);
    };
    if series.len() > crate::dynamic::MAX_COLLECTION_SIZE {
        return Err(error("series exceeds 1048576 points"));
    }
    if matches!(
        op,
        Op::FillForward | Op::FillBackward | Op::FillConst | Op::FillLinear
    ) {
        return fill(op, series, args);
    }
    if op == Op::Stats {
        return statistics(series, boolean(args, 1, false)?);
    }
    if op == Op::FitLine {
        return fit(series);
    }
    if matches!(op, Op::Fir | Op::Iir) {
        return filter(op, series, args);
    }
    if matches!(
        op,
        Op::Add
            | Op::Subtract
            | Op::Multiply
            | Op::Divide
            | Op::Pow
            | Op::Equals
            | Op::NotEquals
            | Op::Less
            | Op::LessEquals
            | Op::Greater
            | Op::GreaterEquals
    ) {
        let Some(other) = args[1].as_array() else {
            return Ok(Json::Null);
        };
        let len = series.len().max(other.len());
        if len > crate::dynamic::MAX_COLLECTION_SIZE {
            return Err(error("series exceeds 1048576 points"));
        }
        let mut output = Vec::with_capacity(len);
        for index in 0..len {
            let (Some(a), Some(b)) = (series.get(index), other.get(index)) else {
                output.push(Json::Null);
                continue;
            };
            let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) else {
                output.push(Json::Null);
                continue;
            };
            let compared = if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
                Some(x.cmp(&y))
            } else {
                x.partial_cmp(&y)
            };
            let value = match op {
                Op::Add | Op::Subtract | Op::Multiply => {
                    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
                        let value = match op {
                            Op::Add => x.checked_add(y),
                            Op::Subtract => x.checked_sub(y),
                            _ => x.checked_mul(y),
                        }
                        .ok_or_else(|| error("series integer arithmetic overflow"))?;
                        Json::from(value)
                    } else {
                        real(match op {
                            Op::Add => x + y,
                            Op::Subtract => x - y,
                            _ => x * y,
                        })
                    }
                }
                Op::Divide => real(x / y),
                Op::Pow => real(x.powf(y)),
                Op::Equals => Json::Bool(compared == Some(std::cmp::Ordering::Equal)),
                Op::NotEquals => Json::Bool(compared != Some(std::cmp::Ordering::Equal)),
                Op::Less => Json::Bool(compared == Some(std::cmp::Ordering::Less)),
                Op::LessEquals => Json::Bool(matches!(
                    compared,
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )),
                Op::Greater => Json::Bool(compared == Some(std::cmp::Ordering::Greater)),
                Op::GreaterEquals => Json::Bool(matches!(
                    compared,
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                )),
                _ => unreachable!("binary operation selected above"),
            };
            output.push(value);
        }
        return Ok(Json::Array(output));
    }
    let mut output = Vec::with_capacity(series.len());
    for point in series {
        let Some(value) = point.as_f64() else {
            output.push(Json::Null);
            continue;
        };
        let value = match op {
            Op::Abs if point.is_i64() => Json::from(
                point
                    .as_i64()
                    .and_then(i64::checked_abs)
                    .ok_or_else(|| error("series abs overflow"))?,
            ),
            Op::Abs => real(value.abs()),
            Op::Acos => real(value.acos()),
            Op::Asin => real(value.asin()),
            Op::Atan => real(value.atan()),
            Op::Cos => real(value.cos()),
            Op::Sin => real(value.sin()),
            Op::Tan => real(value.tan()),
            Op::Exp => real(value.exp()),
            Op::Log => real(value.ln()),
            Op::Log10 => real(value.log10()),
            Op::Sqrt => real(value.sqrt()),
            Op::Floor => real(value.floor()),
            Op::Ceiling => real(value.ceil()),
            Op::Sign => Json::from(if value > 0.0 {
                1
            } else if value < 0.0 {
                -1
            } else {
                0
            }),
            _ => unreachable!("other series operations handled above"),
        };
        output.push(value);
    }
    Ok(Json::Array(output))
}

fn fill(op: Op, series: &[Json], args: &[Json]) -> Result<Json> {
    let missing_index = if op == Op::FillConst { 2 } else { 1 };
    let missing = args.get(missing_index).cloned().unwrap_or(Json::Null);
    let missing = if let Json::String(text) = &missing {
        match serde_json::from_str::<Json>(text) {
            Ok(value) if value.is_number() => value,
            _ => missing,
        }
    } else {
        missing
    };
    let is_missing = |value: &Json| {
        if let (Some(a), Some(b)) = (value.as_i64(), missing.as_i64()) {
            a == b
        } else if let (Some(a), Some(b)) = (value.as_f64(), missing.as_f64()) {
            a == b
        } else {
            value == &missing
        }
    };
    if series
        .iter()
        .any(|value| !is_missing(value) && !value.is_number())
    {
        return Ok(Json::Null);
    }
    let integer = series
        .iter()
        .filter(|value| value.is_number())
        .any(Json::is_i64)
        && series
            .iter()
            .filter(|value| value.is_number())
            .all(Json::is_i64);
    let floating = series
        .iter()
        .any(|value| value.is_number() && !value.is_i64());
    let mut output = series.to_vec();
    match op {
        Op::FillForward | Op::FillBackward => {
            let mut previous: Option<Json> = None;
            for index in 0..series.len() {
                let index = if op == Op::FillBackward {
                    series.len() - 1 - index
                } else {
                    index
                };
                if is_missing(&output[index]) {
                    if let Some(value) = &previous {
                        output[index] = value.clone();
                    }
                } else {
                    previous = Some(output[index].clone());
                }
            }
        }
        Op::FillConst => {
            let value = &args[1];
            if !value.is_number() && !value.is_null() {
                return Err(error("series fill constant must be numeric or null"));
            }
            let value = if integer && !value.is_null() {
                integer_fill(value)?
            } else if floating && value.is_number() {
                real(
                    value
                        .as_f64()
                        .ok_or_else(|| error("invalid real fill value"))?,
                )
            } else {
                value.clone()
            };
            for point in &mut output {
                if is_missing(point) {
                    *point = value.clone();
                }
            }
        }
        Op::FillLinear => {
            let edges = boolean(args, 2, true)?;
            let nonmissing = series
                .iter()
                .enumerate()
                .filter(|(_, value)| !is_missing(value))
                .map(|(i, _)| i)
                .collect::<Vec<_>>();
            if nonmissing.is_empty() {
                let constant = args.get(3).cloned().unwrap_or_else(|| real(0.0));
                if !constant.is_number() && !constant.is_null() {
                    return Err(error("linear fill constant must be numeric or null"));
                }
                let constant = if integer {
                    integer_fill(&constant)?
                } else if floating && constant.is_number() {
                    real(
                        constant
                            .as_f64()
                            .ok_or_else(|| error("invalid real fill value"))?,
                    )
                } else {
                    constant
                };
                return Ok(Json::Array(vec![constant; series.len()]));
            }
            if edges {
                let first = nonmissing[0];
                let last = nonmissing[nonmissing.len() - 1];
                for point in &mut output[..first] {
                    *point = series[first].clone();
                }
                for point in &mut output[last + 1..] {
                    *point = series[last].clone();
                }
            }
            for pair in nonmissing.windows(2) {
                let (left, right) = (pair[0], pair[1]);
                for (index, point) in output.iter_mut().enumerate().take(right).skip(left + 1) {
                    *point = if integer {
                        let (Some(a), Some(b)) = (series[left].as_i64(), series[right].as_i64())
                        else {
                            return Err(error("invalid integer interpolation"));
                        };
                        let denominator = (right - left) as i128;
                        let numerator = i128::from(a) * denominator
                            + (i128::from(b) - i128::from(a)) * (index - left) as i128;
                        Json::from(
                            i64::try_from(numerator / denominator)
                                .map_err(|_| error("integer interpolation overflow"))?,
                        )
                    } else {
                        let a = series[left]
                            .as_f64()
                            .ok_or_else(|| error("invalid interpolation endpoint"))?;
                        let b = series[right]
                            .as_f64()
                            .ok_or_else(|| error("invalid interpolation endpoint"))?;
                        real(a + (b - a) * (index - left) as f64 / (right - left) as f64)
                    };
                }
            }
        }
        _ => unreachable!("fill operation selected above"),
    }
    Ok(Json::Array(output))
}

fn integer_fill(value: &Json) -> Result<Json> {
    if value.is_null() || value.is_i64() {
        return Ok(value.clone());
    }
    let number = value
        .as_f64()
        .ok_or_else(|| error("invalid integer fill value"))?;
    if number < i64::MIN as f64 || number >= 9_223_372_036_854_775_808.0 {
        Ok(Json::Null)
    } else {
        Ok(Json::from(number as i64))
    }
}

fn statistics(series: &[Json], ignore: bool) -> Result<Json> {
    let mut count = 0_usize;
    let mut mean = 0.0;
    let mut m2 = 0.0;
    let mut sum = 0.0;
    let mut correction = 0.0;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut min_idx = 0;
    let mut max_idx = 0;
    for (index, point) in series.iter().enumerate() {
        let value = point.as_f64().filter(|value| value.is_finite());
        let Some(value) = value else {
            if ignore {
                continue;
            } else {
                return Ok(Json::Null);
            }
        };
        if value < min {
            min = value;
            min_idx = index;
        }
        if value > max {
            max = value;
            max_idx = index;
        }
        count += 1;
        let delta = value - mean;
        mean += delta / count as f64;
        m2 += delta * (value - mean);
        let adjusted = value - correction;
        let updated = sum + adjusted;
        correction = (updated - sum) - adjusted;
        sum = updated;
    }
    if count == 0 {
        return Ok(Json::Null);
    }
    let variance = if count > 1 {
        m2 / (count - 1) as f64
    } else {
        0.0
    };
    Ok(Json::Object(Map::from_iter([
        ("min".into(), real(min)),
        ("min_idx".into(), min_idx.into()),
        ("max".into(), real(max)),
        ("max_idx".into(), max_idx.into()),
        ("avg".into(), real(mean)),
        ("variance".into(), real(variance)),
        ("stdev".into(), real(variance.sqrt())),
        ("sum".into(), real(sum)),
        ("len".into(), series.len().into()),
    ])))
}

fn fit(series: &[Json]) -> Result<Json> {
    let Some(values) = series
        .iter()
        .map(|point| point.as_f64().filter(|v| v.is_finite()))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(Json::Null);
    };
    if values.len() < 2 {
        return Ok(Json::Null);
    }
    let n = values.len() as f64;
    let xmean = (n - 1.0) / 2.0;
    let ymean = values.iter().sum::<f64>() / n;
    let mut xx = 0.0;
    let mut xy = 0.0;
    let mut yy = 0.0;
    for (index, value) in values.iter().enumerate() {
        let dx = index as f64 - xmean;
        let dy = value - ymean;
        xx += dx * dx;
        xy += dx * dy;
        yy += dy * dy;
    }
    let slope = xy / xx;
    let intercept = ymean - slope * xmean;
    let fitted = (0..values.len())
        .map(|i| intercept + slope * i as f64)
        .collect::<Vec<_>>();
    let residual = values
        .iter()
        .zip(&fitted)
        .map(|(value, fit)| (value - fit).powi(2))
        .sum::<f64>();
    Ok(Json::Object(Map::from_iter([
        (
            "rsquare".into(),
            real(if yy == 0.0 {
                1.0
            } else {
                (1.0 - residual / yy).clamp(0.0, 1.0)
            }),
        ),
        ("slope".into(), real(slope)),
        ("variance".into(), real(yy / (n - 1.0))),
        ("rvariance".into(), real(residual / (n - 1.0))),
        ("interception".into(), real(intercept)),
        (
            "line_fit".into(),
            Json::Array(fitted.into_iter().map(real).collect()),
        ),
    ])))
}

fn filter(op: Op, series: &[Json], args: &[Json]) -> Result<Json> {
    let Some(values) = series
        .iter()
        .map(|v| v.as_f64())
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(Json::Null);
    };
    let Some(coefficients) = args[1]
        .as_array()
        .and_then(|a| a.iter().map(Json::as_f64).collect::<Option<Vec<_>>>())
    else {
        return Ok(Json::Null);
    };
    if coefficients.is_empty() {
        return Ok(Json::Null);
    }
    let denominators = if op == Op::Iir {
        let Some(values) = args[2]
            .as_array()
            .and_then(|a| a.iter().map(Json::as_f64).collect::<Option<Vec<_>>>())
        else {
            return Ok(Json::Null);
        };
        if values.is_empty() || values[0] == 0.0 {
            return Err(error("series_iir requires a nonzero first denominator"));
        }
        values
    } else {
        vec![1.0]
    };
    if series
        .len()
        .saturating_mul(coefficients.len() + denominators.len())
        > 67_108_864
    {
        return Err(error(
            "series filter exceeds the 67108864 multiply-add work budget",
        ));
    }
    let negative = coefficients.iter().any(|v| *v < 0.0);
    let normalize = if op == Op::Fir {
        boolean(args, 2, !negative)?
    } else {
        false
    };
    let center = if op == Op::Fir {
        boolean(args, 3, false)?
    } else {
        false
    };
    let scale = if normalize {
        if negative {
            return Ok(Json::Null);
        }
        let sum = coefficients.iter().sum::<f64>();
        if sum == 0.0 {
            return Ok(Json::Null);
        }
        sum
    } else {
        denominators[0]
    };
    let delay = if center {
        (coefficients.len() - 1) / 2
    } else {
        0
    };
    let mut output = vec![0.0; series.len()];
    for index in 0..series.len() {
        let mut value = 0.0;
        for (lag, coefficient) in coefficients.iter().enumerate() {
            let at = index as i128 + delay as i128 - lag as i128;
            if at >= 0 && (at as usize) < values.len() {
                value += coefficient * values[at as usize];
            }
        }
        for (lag, coefficient) in denominators.iter().enumerate().skip(1) {
            if index >= lag {
                value -= coefficient * output[index - lag];
            }
        }
        output[index] = value / scale;
    }
    Ok(Json::Array(output.into_iter().map(real).collect()))
}
