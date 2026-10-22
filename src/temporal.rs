use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime};
use datafusion::arrow::array::{
    Array, ArrayRef, DurationNanosecondArray, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::ast::Span;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Target {
    Datetime,
    Timespan,
}

pub(crate) fn try_datetime(text: &str) -> Option<i64> {
    if let Ok(value) = crate::literals::datetime(text, Span::default()) {
        return Some(value);
    }
    [
        "%m/%d/%Y %H:%M:%S%.f",
        "%Y/%m/%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y",
    ]
    .iter()
    .find_map(|format| {
        NaiveDateTime::parse_from_str(text.trim(), format)
            .ok()?
            .and_utc()
            .timestamp_nanos_opt()
    })
    .or_else(|| {
        NaiveDate::parse_from_str(text.trim(), "%m/%d/%Y")
            .ok()?
            .and_hms_opt(0, 0, 0)?
            .and_utc()
            .timestamp_nanos_opt()
    })
}

pub(crate) fn try_timespan(text: &str) -> Option<i64> {
    crate::literals::timespan(text, Span::default()).ok()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Parse {
    target: Target,
    signature: Signature,
}

pub(crate) fn parse(target: Target, value: Expr) -> Expr {
    ScalarUDF::from(Parse {
        target,
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
    })
    .call(vec![value])
}

impl ScalarUDFImpl for Parse {
    fn name(&self) -> &str {
        match self.target {
            Target::Datetime => "__kql_todatetime",
            Target::Timespan => "__kql_totimespan",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(match self.target {
            Target::Datetime => DataType::Timestamp(TimeUnit::Nanosecond, None),
            Target::Timespan => DataType::Duration(TimeUnit::Nanosecond),
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let source = args.args[0].to_array(1)?;
        let strings = source
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("temporal conversion expects UTF-8".into())
            })?;
        let array: ArrayRef = match self.target {
            Target::Datetime => Arc::new(TimestampNanosecondArray::from_iter(
                strings.iter().map(|value| value.and_then(try_datetime)),
            )),
            Target::Timespan => Arc::new(DurationNanosecondArray::from_iter(
                strings.iter().map(|value| value.and_then(try_timespan)),
            )),
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
