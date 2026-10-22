use chrono::{Datelike, Duration, Months, NaiveDate};
use datafusion::arrow::array::{ArrayRef, new_empty_array};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Unit {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl Unit {
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "year" => Self::Year,
            "quarter" => Self::Quarter,
            "month" => Self::Month,
            "week" => Self::Week,
            "day" => Self::Day,
            "hour" => Self::Hour,
            "minute" => Self::Minute,
            "second" => Self::Second,
            "millisecond" => Self::Millisecond,
            "microsecond" => Self::Microsecond,
            "nanosecond" => Self::Nanosecond,
            _ => return None,
        })
    }
    fn nanos(self) -> Option<i64> {
        Some(match self {
            Self::Week => 604_800_000_000_000,
            Self::Day => 86_400_000_000_000,
            Self::Hour => 3_600_000_000_000,
            Self::Minute => 60_000_000_000,
            Self::Second => 1_000_000_000,
            Self::Millisecond => 1_000_000,
            Self::Microsecond => 1_000,
            Self::Nanosecond => 1,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Op {
    Add(Unit),
    Diff(Unit),
    Start(Unit),
    End(Unit),
    FromUnix(i64),
    ToUnix,
    MakeDatetime,
    MakeTimespan,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Function {
    op: Op,
    signature: Signature,
}

pub(crate) fn call(op: Op, args: Vec<Expr>) -> Expr {
    ScalarUDF::from(Function {
        op,
        signature: Signature::variadic_any(Volatility::Immutable),
    })
    .call(args)
}

fn time(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::TimestampNanosecond(value, _) => *value,
        _ => None,
    }
}
fn integer(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int64(value) => *value,
        _ => None,
    }
}
fn numeric(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float64(value) => *value,
        ScalarValue::Int64(value) => value.map(|v| v as f64),
        _ => None,
    }
}

fn add(value: i64, amount: i64, unit: Unit) -> Option<i64> {
    if let Some(scale) = unit.nanos() {
        return i64::try_from(i128::from(value) + i128::from(amount) * i128::from(scale)).ok();
    }
    let months = i128::from(amount)
        * match unit {
            Unit::Year => 12,
            Unit::Quarter => 3,
            _ => 1,
        };
    let count = u32::try_from(months.unsigned_abs()).ok()?;
    let date = chrono::DateTime::from_timestamp_nanos(value);
    let date = if months < 0 {
        date.checked_sub_months(Months::new(count))?
    } else {
        date.checked_add_months(Months::new(count))?
    };
    date.timestamp_nanos_opt()
}

fn start(value: i64, offset: i64, unit: Unit) -> Option<i64> {
    let date = chrono::DateTime::from_timestamp_nanos(value);
    let base = match unit {
        Unit::Year => NaiveDate::from_ymd_opt(date.year(), 1, 1)?,
        Unit::Month => NaiveDate::from_ymd_opt(date.year(), date.month(), 1)?,
        Unit::Week => date
            .date_naive()
            .checked_sub_signed(Duration::days(i64::from(
                date.weekday().num_days_from_sunday(),
            )))?,
        Unit::Day => date.date_naive(),
        _ => return None,
    }
    .and_hms_opt(0, 0, 0)?
    .and_utc()
    .timestamp_nanos_opt()?;
    add(base, offset, unit)
}

fn difference(left: i64, right: i64, unit: Unit) -> Option<i64> {
    let a = chrono::DateTime::from_timestamp_nanos(left);
    let b = chrono::DateTime::from_timestamp_nanos(right);
    let calendar = match unit {
        Unit::Year => Some(i64::from(a.year()) - i64::from(b.year())),
        Unit::Month => Some(
            (i64::from(a.year()) - i64::from(b.year())) * 12 + i64::from(a.month0())
                - i64::from(b.month0()),
        ),
        Unit::Quarter => Some(
            (i64::from(a.year()) - i64::from(b.year())) * 4 + i64::from(a.month0() / 3)
                - i64::from(b.month0() / 3),
        ),
        _ => None,
    };
    if calendar.is_some() {
        return calendar;
    }
    let unit_ns = i128::from(unit.nanos()?);
    let offset = if unit == Unit::Week {
        3 * 86_400_000_000_000_i128
    } else {
        0
    };
    i64::try_from(
        (i128::from(left) - offset).div_euclid(unit_ns)
            - (i128::from(right) - offset).div_euclid(unit_ns),
    )
    .ok()
}

impl Function {
    fn data_type(&self) -> DataType {
        match self.op {
            Op::Diff(_) => DataType::Int64,
            Op::ToUnix => DataType::Float64,
            Op::MakeTimespan => DataType::Duration(TimeUnit::Nanosecond),
            _ => DataType::Timestamp(TimeUnit::Nanosecond, None),
        }
    }
    fn evaluate(&self, args: &[ScalarValue]) -> Result<ScalarValue> {
        let result = match self.op {
            Op::Add(unit) => time(&args[0])
                .zip(integer(&args[1]))
                .and_then(|(date, amount)| add(date, amount, unit)),
            Op::Diff(unit) => {
                let value = time(&args[0])
                    .zip(time(&args[1]))
                    .and_then(|(a, b)| difference(a, b, unit));
                return Ok(ScalarValue::Int64(value));
            }
            Op::Start(unit) => time(&args[0])
                .zip(integer(&args[1]))
                .and_then(|(date, offset)| start(date, offset, unit)),
            Op::End(unit) => time(&args[0])
                .zip(integer(&args[1]))
                .and_then(|(date, offset)| {
                    start(date, offset.checked_add(1)?, unit)?.checked_sub(100)
                }),
            Op::ToUnix => {
                return Ok(ScalarValue::Float64(
                    time(&args[0]).map(|value| value as f64 / 1_000_000_000.0),
                ));
            }
            Op::FromUnix(scale) => {
                if let Some(value) = integer(&args[0]) {
                    i64::try_from(i128::from(value) * i128::from(scale)).ok()
                } else {
                    numeric(&args[0]).and_then(|value| {
                        let value = value * scale as f64;
                        (value.is_finite()
                            && value >= i64::MIN as f64
                            && value < 9_223_372_036_854_775_808.0)
                            .then_some(value as i64)
                    })
                }
            }
            Op::MakeDatetime => {
                let make = || -> Option<i64> {
                    let year = i32::try_from(integer(&args[0])?).ok()?;
                    let month = u32::try_from(integer(&args[1])?).ok()?;
                    let day = u32::try_from(integer(&args[2])?).ok()?;
                    let hour = args.get(3).map(integer).unwrap_or(Some(0))?;
                    let minute = args.get(4).map(integer).unwrap_or(Some(0))?;
                    let second = args.get(5).map(numeric).unwrap_or(Some(0.0))?;
                    if !(0..24).contains(&hour)
                        || !(0..60).contains(&minute)
                        || !(0.0..60.0).contains(&second)
                    {
                        return None;
                    }
                    let nanos = (second.fract() * 1_000_000_000.0).trunc() as u32;
                    NaiveDate::from_ymd_opt(year, month, day)?
                        .and_hms_nano_opt(hour as u32, minute as u32, second as u32, nanos)?
                        .and_utc()
                        .timestamp_nanos_opt()
                };
                make()
            }
            Op::MakeTimespan => {
                let make = || -> Option<i64> {
                    let (days, hours, minutes, seconds) = if args.len() == 4 {
                        (
                            integer(&args[0])?,
                            integer(&args[1])?,
                            integer(&args[2])?,
                            numeric(&args[3])?,
                        )
                    } else {
                        (
                            0,
                            integer(&args[0])?,
                            integer(&args[1])?,
                            args.get(2).map(numeric).unwrap_or(Some(0.0))?,
                        )
                    };
                    let seconds = seconds * 1_000_000_000.0;
                    if !seconds.is_finite()
                        || seconds < i64::MIN as f64
                        || seconds >= 9_223_372_036_854_775_808.0
                    {
                        return None;
                    }
                    let nanos = i128::from(days) * 86_400_000_000_000
                        + i128::from(hours) * 3_600_000_000_000
                        + i128::from(minutes) * 60_000_000_000
                        + i128::from(seconds as i64);
                    i64::try_from(nanos).ok()
                };
                return Ok(ScalarValue::DurationNanosecond(make()));
            }
        };
        Ok(ScalarValue::TimestampNanosecond(result, None))
    }
}

impl ScalarUDFImpl for Function {
    fn name(&self) -> &str {
        match self.op {
            Op::Add(_) => "__kql_datetime_add",
            Op::Diff(_) => "__kql_datetime_diff",
            Op::Start(_) => "__kql_period_start",
            Op::End(_) => "__kql_period_end",
            Op::FromUnix(_) => "__kql_from_unixtime",
            Op::ToUnix => "__kql_datetime_to_unixtime",
            Op::MakeDatetime => "__kql_make_datetime",
            Op::MakeTimespan => "__kql_make_timespan",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let valid = match self.op {
            Op::MakeDatetime => (3..=6).contains(&args.len()),
            Op::MakeTimespan => (2..=4).contains(&args.len()),
            Op::ToUnix | Op::FromUnix(_) => args.len() == 1,
            _ => args.len() == 2,
        };
        if !valid {
            return Err(DataFusionError::Plan(
                "invalid calendar function argument count".into(),
            ));
        }
        Ok(self.data_type())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let count = if scalar { 1 } else { args.number_rows };
        let arrays = args
            .args
            .iter()
            .map(|arg| arg.to_array(1))
            .collect::<Result<Vec<ArrayRef>>>()?;
        let mut output = Vec::with_capacity(count);
        for row in 0..count {
            let values = args
                .args
                .iter()
                .zip(&arrays)
                .map(|(arg, array)| match arg {
                    ColumnarValue::Scalar(value) => Ok(value.clone()),
                    ColumnarValue::Array(_) => ScalarValue::try_from_array(array, row),
                })
                .collect::<Result<Vec<_>>>()?;
            output.push(self.evaluate(&values)?);
        }
        let array = if output.is_empty() {
            new_empty_array(&self.data_type())
        } else {
            ScalarValue::iter_to_array(output)?
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &array, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(array))
        }
    }
}
