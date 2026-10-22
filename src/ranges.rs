use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNano, TimeUnit};
use datafusion::catalog::TableFunctionArgs;
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::expr_fn::cast;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::lit;

use crate::ast;
use crate::expressions::Expressions;
use crate::{Error, Result};

pub(crate) fn plan(
    expressions: &Expressions<'_>,
    name: &str,
    start: &ast::Expr,
    end: &ast::Expr,
    step: &ast::Expr,
) -> Result<LogicalPlan> {
    let mut values = Vec::new();
    for expr in [start, end, step] {
        let value = expressions.scalar(expr, &DFSchema::empty())?;
        let value = expressions.constant(value, expr.span)?;
        if value.is_null() {
            return Err(Error::syntax(
                "range bounds and step must be non-null",
                expr.span,
            ));
        }
        values.push(value);
    }
    let mut projection = None;
    let arguments = if matches!(values[0].data_type(), DataType::Timestamp(_, _)) {
        if !matches!(values[1].data_type(), DataType::Timestamp(_, _))
            || !matches!(values[2].data_type(), DataType::Duration(_))
        {
            return Err(Error::syntax(
                "datetime range requires datetime bounds and a timespan step",
                step.span,
            ));
        }
        let ty = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let stride = match values[2].cast_to(&DataType::Duration(TimeUnit::Nanosecond))? {
            ScalarValue::DurationNanosecond(Some(value)) if value != 0 => value,
            _ => return Err(Error::syntax("range step cannot be zero", step.span)),
        };
        vec![
            lit(values[0].cast_to(&ty)?),
            lit(values[1].cast_to(&ty)?),
            lit(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(0, 0, stride),
            ))),
        ]
    } else if values
        .iter()
        .all(|value| matches!(value.data_type(), DataType::Duration(_)))
    {
        let mut args = Vec::new();
        for value in &values {
            args.push(lit(value
                .cast_to(&DataType::Duration(TimeUnit::Nanosecond))?
                .cast_to(&DataType::Int64)?));
        }
        if matches!(&args[2], Expr::Literal(ScalarValue::Int64(Some(0)), _)) {
            return Err(Error::syntax("range step cannot be zero", step.span));
        }
        projection = Some((None, DataType::Duration(TimeUnit::Nanosecond)));
        args
    } else if values.iter().all(|value| value.data_type().is_integer()) {
        let args = values
            .iter()
            .map(|value| Ok(lit(value.cast_to(&DataType::Int64)?)))
            .collect::<Result<Vec<_>>>()?;
        if matches!(&args[2], Expr::Literal(ScalarValue::Int64(Some(0)), _)) {
            return Err(Error::syntax("range step cannot be zero", step.span));
        }
        args
    } else if values.iter().all(|value| value.data_type().is_numeric()) {
        let mut real = Vec::new();
        for value in &values {
            match value.cast_to(&DataType::Float64)? {
                ScalarValue::Float64(Some(value)) if value.is_finite() => real.push(value),
                _ => {
                    return Err(Error::syntax(
                        "real range requires finite bounds and step",
                        step.span,
                    ));
                }
            }
        }
        let [from, to, stride]: [f64; 3] = real
            .try_into()
            .map_err(|_| Error::Plan("invalid range argument count".into()))?;
        if stride == 0.0 {
            return Err(Error::syntax("range step cannot be zero", step.span));
        }
        let count = ((to - from) / stride).floor();
        if !count.is_finite() || count >= 9_223_372_036_854_775_808.0 {
            return Err(Error::syntax(
                "real range requires too many representable indices",
                step.span,
            ));
        }
        let end = if count < 0.0 { -1 } else { count as i64 };
        projection = Some((Some((from, stride)), DataType::Float64));
        vec![lit(0_i64), lit(end), lit(1_i64)]
    } else {
        return Err(Error::syntax(
            "range supports numeric, datetime, or timespan sequences",
            start.span,
        ));
    };
    let function = expressions
        .state
        .table_functions()
        .get("generate_series")
        .ok_or_else(|| Error::Plan("session has no generate_series table function".into()))?;
    let provider = function
        .create_table_provider_with_args(TableFunctionArgs::new(&arguments, expressions.state))?;
    let source =
        LogicalPlanBuilder::scan("__kql_range", provider_as_source(provider), None)?.build()?;
    let column = Expr::Column(Column::from(source.schema().qualified_field(0)));
    let value = if let Some((real, ty)) = projection {
        let value = cast(column, ty);
        if let Some((from, step)) = real {
            lit(from) + value * lit(step)
        } else {
            value
        }
    } else {
        column
    };
    Ok(LogicalPlanBuilder::from(source)
        .project(vec![value.alias(name)])?
        .build()?)
}
