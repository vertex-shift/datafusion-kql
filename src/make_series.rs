use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, ListArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::{Column, DataFusionError, ScalarValue};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::expr_fn::{ExprFunctionExt, cast};
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility, WindowFrame,
};
use datafusion::prelude::lit;
use serde_json::Value as Json;

use crate::ast::{self, ExprKind, NamedExpr, Span};
use crate::dynamic;
use crate::expressions::Expressions;
use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Axis {
    Integer,
    Real,
    Datetime,
    Timespan,
}

impl Axis {
    fn data_type(self) -> DataType {
        match self {
            Self::Integer => DataType::Int64,
            Self::Real => DataType::Float64,
            Self::Datetime => DataType::Timestamp(TimeUnit::Nanosecond, None),
            Self::Timespan => DataType::Duration(TimeUnit::Nanosecond),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Number {
    Integer(i64),
    Real(u64),
}

fn number(value: ScalarValue, axis: Axis, span: Span) -> Result<Number> {
    let value = value.cast_to(&axis.data_type())?;
    match value {
        ScalarValue::Int64(Some(value))
        | ScalarValue::TimestampNanosecond(Some(value), _)
        | ScalarValue::DurationNanosecond(Some(value)) => Ok(Number::Integer(value)),
        ScalarValue::Float64(Some(value)) if value.is_finite() => Ok(Number::Real(value.to_bits())),
        _ => Err(Error::syntax(
            "series bounds and step must be non-null finite constants",
            span,
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Fill {
    axis: Axis,
    start: Option<Number>,
    end: Option<Number>,
    step: Number,
    inclusive: bool,
    defaults: Vec<String>,
    signature: Signature,
}

fn fields(count: usize) -> Vec<Arc<Field>> {
    let mut fields = (0..count)
        .map(|i| {
            Arc::new(Field::new(
                format!("series_{i}"),
                dynamic::data_type(),
                false,
            ))
        })
        .collect::<Vec<_>>();
    fields.push(Arc::new(Field::new("axis", dynamic::data_type(), false)));
    fields
}

fn unique(
    input: &LogicalPlan,
    scalars: &HashMap<String, Expr>,
    reserved: &mut HashSet<String>,
    prefix: &str,
) -> String {
    let mut index = 0;
    loop {
        let name = format!("__kql_series_{prefix}_{index}");
        if input
            .schema()
            .index_of_column_by_name(None, &name)
            .is_none()
            && !scalars.contains_key(&name)
            && reserved.insert(name.clone())
        {
            return name;
        }
        index += 1;
    }
}

pub(crate) fn plan(
    state: &SessionState,
    scalars: &HashMap<String, Expr>,
    now: i64,
    mut input: LogicalPlan,
    series: &ast::MakeSeries,
    span: Span,
) -> Result<LogicalPlan> {
    let expressions = Expressions {
        state,
        scalars,
        now,
    };
    let mut axis_value = expressions.scalar(&series.axis, input.schema())?;
    let axis_type = axis_value.get_type(input.schema().as_ref())?;
    let step = expressions.scalar(&series.step, &datafusion::common::DFSchema::empty())?;
    let step_value = expressions.constant(step.clone(), series.step.span)?;
    let axis = match axis_type {
        DataType::Timestamp(_, _) => Axis::Datetime,
        DataType::Duration(_) => Axis::Timespan,
        ty if ty.is_integer() => Axis::Integer,
        ty if ty.is_floating() => Axis::Real,
        _ => {
            return Err(Error::syntax(
                "make-series axis must be numeric, datetime, or timespan",
                series.axis.span,
            ));
        }
    };
    let axis = if axis == Axis::Integer && step_value.data_type().is_floating() {
        Axis::Real
    } else {
        axis
    };
    let step = if matches!(axis, Axis::Datetime | Axis::Timespan) {
        if !matches!(step_value.data_type(), DataType::Duration(_)) {
            return Err(Error::syntax(
                "temporal series require a timespan step",
                series.step.span,
            ));
        }
        number(step_value, Axis::Timespan, series.step.span)?
    } else {
        number(step_value, axis, series.step.span)?
    };
    if match step {
        Number::Integer(step) => step <= 0,
        Number::Real(step) => f64::from_bits(step) <= 0.0,
    } {
        return Err(Error::syntax(
            "make-series step must be positive",
            series.step.span,
        ));
    }
    let bound = |value: &ast::Expr| -> Result<Number> {
        let value_expr = expressions.scalar(value, &datafusion::common::DFSchema::empty())?;
        number(
            expressions.constant(value_expr, value.span)?,
            axis,
            value.span,
        )
    };
    let mut start = series.from.as_ref().map(bound).transpose()?;
    let end = series.to.as_ref().map(bound).transpose()?;
    axis_value = cast(axis_value, axis.data_type());
    let mut reserved = series
        .by
        .iter()
        .chain(series.aggregates.iter().map(|a| &a.value))
        .filter_map(|v| v.name.clone())
        .collect::<HashSet<_>>();
    let bucket = unique(&input, scalars, &mut reserved, "bucket");
    let origin = if series.align_to_start {
        if let Some(start) = start {
            axis_literal(start, axis)
        } else {
            let name = unique(&input, scalars, &mut reserved, "origin");
            let partitions = series
                .by
                .iter()
                .map(|group| expressions.scalar(&group.expr, input.schema()))
                .collect::<Result<Vec<_>>>()?;
            let minimum = Expr::from(WindowFunction::new(
                state.udaf("min")?,
                vec![axis_value.clone()],
            ))
            .partition_by(partitions)
            .window_frame(WindowFrame::new(None))
            .build()?
            .alias(&name);
            input = LogicalPlanBuilder::from(input)
                .window(vec![minimum])?
                .build()?;
            Expr::Column(Column::from_name(name))
        }
    } else {
        let zero = match axis {
            Axis::Real => Number::Real(0.0_f64.to_bits()),
            _ => Number::Integer(0),
        };
        axis_literal(zero, axis)
    };
    let width = match step {
        Number::Integer(value) if matches!(axis, Axis::Datetime | Axis::Timespan) => {
            lit(ScalarValue::DurationNanosecond(Some(value)))
        }
        value => axis_literal(value, axis),
    };
    let binned = expressions.bin_at(
        axis_value,
        width.clone(),
        origin.clone(),
        input.schema(),
        span,
    )?;
    if !series.align_to_start
        && let Some(from) = start
    {
        let value = expressions.bin_at(
            axis_literal(from, axis),
            width,
            origin,
            input.schema(),
            span,
        )?;
        start = Some(number(expressions.constant(value, span)?, axis, span)?);
    }
    let mut projected = (0..input.schema().fields().len())
        .map(|i| Expr::Column(Column::from(input.schema().qualified_field(i))))
        .collect::<Vec<_>>();
    projected.push(binned.alias(&bucket));
    input = LogicalPlanBuilder::from(input)
        .project(projected)?
        .build()?;
    let mut by = series.by.clone();
    by.push(NamedExpr {
        name: Some(bucket.clone()),
        expr: ast::Expr {
            kind: ExprKind::Identifier(vec![bucket]),
            span,
            depth: 1,
        },
    });
    let aggregates = series
        .aggregates
        .iter()
        .map(|a| a.value.clone())
        .collect::<Vec<_>>();
    let bins = crate::aggregation::plan(state, scalars, now, input, &aggregates, &by)?;
    if bins.schema().fields().len() != by.len() + aggregates.len() {
        return Err(Error::syntax(
            "make-series aggregates must return exactly one numeric column each",
            span,
        ));
    }
    let mut defaults = Vec::new();
    let mut packed = vec![
        lit("axis"),
        cast(
            Expr::Column(Column::from(bins.schema().qualified_field(series.by.len()))),
            if axis == Axis::Real {
                DataType::Float64
            } else {
                DataType::Int64
            },
        ),
    ];
    let mut names = Vec::new();
    for (i, aggregate) in series.aggregates.iter().enumerate() {
        let index = by.len() + i;
        let ty = bins.schema().field(index).data_type();
        if !ty.is_integer() && !ty.is_floating() {
            return Err(Error::syntax(
                "make-series supports numeric aggregate results",
                aggregate.value.expr.span,
            ));
        }
        let ty = if ty.is_floating() {
            DataType::Float64
        } else {
            DataType::Int64
        };
        let default = if let Some(default) = &aggregate.default {
            let value = expressions.scalar(default, &datafusion::common::DFSchema::empty())?;
            let value = expressions.constant(value, default.span)?;
            if !value.is_null() && !value.data_type().is_numeric() {
                return Err(Error::syntax(
                    "series defaults must be numeric or null",
                    default.span,
                ));
            }
            value.cast_to(&ty)?
        } else {
            ScalarValue::Int64(Some(0)).cast_to(&ty)?
        };
        defaults.push(
            match default {
                ScalarValue::Int64(Some(value)) => Json::from(value),
                ScalarValue::Float64(Some(value)) => Json::from(value),
                _ => Json::Null,
            }
            .to_string(),
        );
        packed.push(lit(format!("value_{i}")));
        packed.push(cast(
            Expr::Column(Column::from(bins.schema().qualified_field(index))),
            ty,
        ));
        names.push(bins.schema().field(index).name().clone());
    }
    let records = expressions.function("named_struct", packed)?;
    let raw = unique(&bins, scalars, &mut reserved, "records");
    let count = unique(&bins, scalars, &mut reserved, "count");
    let groups = (0..series.by.len())
        .map(|i| Expr::Column(Column::from(bins.schema().qualified_field(i))))
        .collect::<Vec<_>>();
    let collected = LogicalPlanBuilder::from(bins)
        .aggregate(
            groups,
            vec![
                state.udaf("array_agg")?.call(vec![records]).alias(&raw),
                state.udaf("count")?.call(vec![lit(1_i64)]).alias(&count),
            ],
        )?
        .build()?;
    let collected = if !series.nonempty {
        LogicalPlanBuilder::from(collected)
            .filter(Expr::Column(Column::from_name(count)).gt(lit(0_i64)))?
            .build()?
    } else {
        collected
    };
    let fill = ScalarUDF::from(Fill {
        axis,
        start,
        end,
        step,
        inclusive: series.inclusive,
        defaults,
        signature: Signature::any(1, Volatility::Immutable),
    });
    let result_name = unique(&collected, scalars, &mut reserved, "result");
    let mut projection = (0..series.by.len())
        .map(|i| Expr::Column(Column::from(collected.schema().qualified_field(i))))
        .collect::<Vec<_>>();
    projection.push(
        fill.call(vec![Expr::Column(Column::from_name(raw))])
            .alias(&result_name),
    );
    let filled = LogicalPlanBuilder::from(collected)
        .project(projection)?
        .build()?;
    let mut projection = (0..series.by.len())
        .map(|i| Expr::Column(Column::from(filled.schema().qualified_field(i))))
        .collect::<Vec<_>>();
    for (i, name) in names.into_iter().enumerate() {
        projection.push(
            expressions
                .function(
                    "get_field",
                    vec![
                        Expr::Column(Column::from_name(&result_name)),
                        lit(format!("series_{i}")),
                    ],
                )?
                .alias(name),
        );
    }
    let axis_name = if let ExprKind::Identifier(parts) = &series.axis.kind {
        parts.last().cloned().unwrap_or_else(|| "Axis".into())
    } else {
        "Axis".into()
    };
    projection.push(
        expressions
            .function(
                "get_field",
                vec![Expr::Column(Column::from_name(result_name)), lit("axis")],
            )?
            .alias(axis_name),
    );
    Ok(LogicalPlanBuilder::from(filled)
        .project(projection)?
        .build()?)
}

fn axis_literal(value: Number, axis: Axis) -> Expr {
    match value {
        Number::Real(value) => lit(f64::from_bits(value)),
        Number::Integer(value) => lit(match axis {
            Axis::Datetime => ScalarValue::TimestampNanosecond(Some(value), None),
            Axis::Timespan => ScalarValue::DurationNanosecond(Some(value)),
            _ => ScalarValue::Int64(Some(value)),
        }),
    }
}

impl ScalarUDFImpl for Fill {
    fn name(&self) -> &str {
        "__kql_make_series"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(fields(self.defaults.len()).into()))
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let source = args.args[0].to_array(1)?;
        let lists = source.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            DataFusionError::Execution("make-series expects collected bins".into())
        })?;
        let defaults = self
            .defaults
            .iter()
            .map(|value| {
                serde_json::from_str(value).map_err(|e| DataFusionError::Execution(e.to_string()))
            })
            .collect::<datafusion::error::Result<Vec<Json>>>()?;
        let mut outputs = (0..=defaults.len())
            .map(|_| Vec::new())
            .collect::<Vec<Vec<Json>>>();
        for row in 0..lists.len() {
            let records = if lists.is_null(row) {
                None
            } else {
                Some(lists.value(row))
            };
            let values = self.fill(records.as_deref(), &defaults)?;
            for (output, value) in outputs.iter_mut().zip(values) {
                output.push(Json::Array(value));
            }
        }
        let arrays = outputs
            .into_iter()
            .map(|rows| dynamic::from_json_rows(rows.into_iter().map(Ok), lists.len()))
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        let array = Arc::new(StructArray::try_new(
            fields(self.defaults.len()).into(),
            arrays,
            None,
        )?) as ArrayRef;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &array, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(array))
        }
    }
}

impl Fill {
    fn fill(
        &self,
        records: Option<&dyn Array>,
        defaults: &[Json],
    ) -> datafusion::error::Result<Vec<Vec<Json>>> {
        let records = records
            .map(|records| {
                records
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("invalid make-series record type".into())
                    })
            })
            .transpose()?;
        let mut bins = Vec::new();
        if let Some(records) = records {
            for row in 0..records.len() {
                let axis = records.column(0);
                if axis.is_null(row) {
                    continue;
                }
                let key = if self.axis == Axis::Real {
                    let array = axis
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("invalid real series axis".into())
                        })?;
                    let value = array.value(row);
                    if !value.is_finite() {
                        continue;
                    }
                    Number::Real(value.to_bits())
                } else {
                    let array = axis.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        DataFusionError::Execution("invalid integer series axis".into())
                    })?;
                    Number::Integer(array.value(row))
                };
                bins.push((key, row));
            }
        }
        let mut result = vec![Vec::new(); defaults.len() + 1];
        if self.axis == Axis::Real {
            let step = if let Number::Real(step) = self.step {
                f64::from_bits(step)
            } else {
                return Err(series_error("invalid real step"));
            };
            let min = bins
                .iter()
                .filter_map(|(value, _)| {
                    if let Number::Real(value) = value {
                        Some(f64::from_bits(*value))
                    } else {
                        None
                    }
                })
                .min_by(f64::total_cmp);
            let max = bins
                .iter()
                .filter_map(|(value, _)| {
                    if let Number::Real(value) = value {
                        Some(f64::from_bits(*value))
                    } else {
                        None
                    }
                })
                .max_by(f64::total_cmp);
            let start = match self.start {
                Some(Number::Real(value)) => Some(f64::from_bits(value)),
                _ => min,
            };
            let end = match self.end {
                Some(Number::Real(value)) => Some(f64::from_bits(value)),
                _ => max.map(|v| v + step),
            };
            let (Some(start), Some(end)) = (start, end) else {
                return Ok(result);
            };
            let distance = (end - start) / step;
            let count = if distance < 0.0 {
                0.0
            } else if self.inclusive {
                distance.floor() + 1.0
            } else {
                distance.ceil()
            };
            if !count.is_finite() || count > dynamic::MAX_COLLECTION_SIZE as f64 + 2.0 {
                return Err(series_error("make-series exceeds 1048576 points"));
            }
            let valid = |i: usize| {
                let value = start + i as f64 * step;
                if self.inclusive {
                    value <= end
                } else {
                    value < end
                }
            };
            let mut count = count as usize;
            while count > 0 && !valid(count - 1) {
                count -= 1;
            }
            while valid(count) {
                count += 1;
                if count > dynamic::MAX_COLLECTION_SIZE {
                    return Err(series_error("make-series exceeds 1048576 points"));
                }
            }
            for (output, default) in result.iter_mut().zip(defaults) {
                *output = vec![default.clone(); count];
            }
            result[defaults.len()] = (0..count)
                .map(|i| Json::from(start + i as f64 * step))
                .collect();
            for (key, row) in bins {
                let Number::Real(key) = key else {
                    continue;
                };
                let index = ((f64::from_bits(key) - start) / step).round();
                if index >= 0.0 && (index as usize) < count {
                    self.assign(&mut result, records, row, index as usize)?;
                }
            }
            if count > dynamic::MAX_COLLECTION_SIZE {
                return Err(series_error("make-series exceeds 1048576 points"));
            }
        } else {
            let step = if let Number::Integer(step) = self.step {
                i128::from(step)
            } else {
                return Err(series_error("invalid integer step"));
            };
            let min = bins
                .iter()
                .filter_map(|(value, _)| {
                    if let Number::Integer(value) = value {
                        Some(i128::from(*value))
                    } else {
                        None
                    }
                })
                .min();
            let max = bins
                .iter()
                .filter_map(|(value, _)| {
                    if let Number::Integer(value) = value {
                        Some(i128::from(*value))
                    } else {
                        None
                    }
                })
                .max();
            let start = match self.start {
                Some(Number::Integer(value)) => Some(i128::from(value)),
                _ => min,
            };
            let end = match self.end {
                Some(Number::Integer(value)) => Some(i128::from(value)),
                _ => max.map(|v| v + step),
            };
            let (Some(start), Some(end)) = (start, end) else {
                return Ok(result);
            };
            let distance = end - start;
            let count = if distance < 0 {
                0
            } else if self.inclusive {
                distance / step + 1
            } else {
                (distance + step - 1) / step
            };
            let count =
                usize::try_from(count).map_err(|_| series_error("invalid series bounds"))?;
            if count > dynamic::MAX_COLLECTION_SIZE {
                return Err(series_error("make-series exceeds 1048576 points"));
            }
            for (output, default) in result.iter_mut().zip(defaults) {
                *output = vec![default.clone(); count];
            }
            for i in 0..count {
                let value = i64::try_from(start + i as i128 * step)
                    .map_err(|_| series_error("series axis is outside its supported range"))?;
                result[defaults.len()].push(self.axis_json(value));
            }
            for (key, row) in bins {
                let Number::Integer(key) = key else {
                    continue;
                };
                let delta = i128::from(key) - start;
                if delta >= 0 && delta % step == 0 && delta / step < count as i128 {
                    self.assign(&mut result, records, row, (delta / step) as usize)?;
                }
            }
        }
        Ok(result)
    }

    fn axis_json(&self, value: i64) -> Json {
        match self.axis {
            Axis::Datetime => {
                let date = chrono::DateTime::from_timestamp_nanos(value);
                let base = date.format("%Y-%m-%dT%H:%M:%S");
                let fraction = date.timestamp_subsec_nanos();
                if value % 100 == 0 {
                    format!("{base}.{:07}Z", fraction / 100).into()
                } else {
                    format!("{base}.{fraction:09}Z").into()
                }
            }
            Axis::Timespan => {
                let sign = if value < 0 { "-" } else { "" };
                let value = value.unsigned_abs();
                format!(
                    "{sign}{}.{:02}:{:02}:{:02}.{:09}",
                    value / 86_400_000_000_000,
                    value / 3_600_000_000_000 % 24,
                    value / 60_000_000_000 % 60,
                    value / 1_000_000_000 % 60,
                    value % 1_000_000_000
                )
                .into()
            }
            _ => Json::from(value),
        }
    }

    fn assign(
        &self,
        result: &mut [Vec<Json>],
        records: Option<&StructArray>,
        row: usize,
        index: usize,
    ) -> datafusion::error::Result<()> {
        let records = records.ok_or_else(|| series_error("missing series records"))?;
        for (series, output) in result[..self.defaults.len()].iter_mut().enumerate() {
            let value = ScalarValue::try_from_array(records.column(series + 1), row)?;
            output[index] = match value {
                ScalarValue::Int64(Some(value)) => Json::from(value),
                ScalarValue::Float64(Some(value)) => Json::from(value),
                _ if value.is_null() => Json::Null,
                _ => return Err(series_error("unsupported series aggregate value")),
            };
        }
        Ok(())
    }
}

fn series_error(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}
