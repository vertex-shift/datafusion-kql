use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, DurationNanosecondArray, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::logical_expr::{PartitionEvaluator, WindowUDF, WindowUDFImpl};

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Session {
    signature: Signature,
}

pub(crate) fn session(args: Vec<Expr>) -> Expr {
    WindowUDF::from(Session {
        signature: Signature::exact(
            vec![
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Duration(TimeUnit::Nanosecond),
                DataType::Duration(TimeUnit::Nanosecond),
                DataType::Boolean,
            ],
            Volatility::Immutable,
        ),
    })
    .call(args)
}

impl WindowUDFImpl for Session {
    fn name(&self) -> &str {
        "__kql_row_window_session"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn field(&self, args: WindowUDFFieldArgs) -> Result<Arc<datafusion::arrow::datatypes::Field>> {
        Ok(Arc::new(datafusion::arrow::datatypes::Field::new(
            args.name(),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )))
    }
    fn partition_evaluator(
        &self,
        _: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(SessionState {
            first: None,
            previous: None,
        }))
    }
}

#[derive(Debug)]
struct SessionState {
    first: Option<i64>,
    previous: Option<i64>,
}

impl SessionState {
    fn advance(
        &mut self,
        value: Option<i64>,
        first: Option<i64>,
        neighbor: Option<i64>,
        restart: bool,
    ) -> Option<i64> {
        let Some(value) = value else {
            self.first = None;
            self.previous = None;
            return None;
        };
        let from_first = self.first.zip(first).is_some_and(|(start, limit)| {
            i128::from(value) - i128::from(start) >= i128::from(limit)
        });
        let from_neighbor = self
            .previous
            .zip(neighbor)
            .is_some_and(|(previous, limit)| {
                i128::from(value) - i128::from(previous) >= i128::from(limit)
            });
        if self.first.is_none() || from_first || from_neighbor || restart {
            self.first = Some(value);
        }
        self.previous = Some(value);
        self.first
    }

    fn row(&mut self, values: &[ArrayRef], row: usize) -> Result<Option<i64>> {
        let source = values[0]
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| DataFusionError::Execution("session input must be datetime".into()))?;
        let first = values[1]
            .as_any()
            .downcast_ref::<DurationNanosecondArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("session distance must be timespan".into())
            })?;
        let neighbor = values[2]
            .as_any()
            .downcast_ref::<DurationNanosecondArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("session distance must be timespan".into())
            })?;
        let restart = values[3]
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| DataFusionError::Execution("session restart must be bool".into()))?;
        Ok(self.advance(
            (!source.is_null(row)).then(|| source.value(row)),
            (!first.is_null(row)).then(|| first.value(row)),
            (!neighbor.is_null(row)).then(|| neighbor.value(row)),
            !restart.is_null(row) && restart.value(row),
        ))
    }
}

impl PartitionEvaluator for SessionState {
    fn supports_bounded_execution(&self) -> bool {
        true
    }
    fn is_causal(&self) -> bool {
        true
    }
    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &std::ops::Range<usize>,
    ) -> Result<ScalarValue> {
        Ok(ScalarValue::TimestampNanosecond(
            self.row(values, range.start)?,
            None,
        ))
    }
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let mut output =
            datafusion::arrow::array::TimestampNanosecondBuilder::with_capacity(num_rows);
        for row in 0..num_rows {
            output.append_option(self.row(values, row)?);
        }
        Ok(Arc::new(output.finish()))
    }
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
