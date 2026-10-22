use std::collections::{BTreeMap, HashMap};

use datafusion::arrow::datatypes::Field;
use datafusion::common::{Column, DFSchema, metadata::FieldMetadata};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::expr_fn::ExprFunctionExt;
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::logical_expr::{
    Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, WindowFrame, WindowFrameBound,
    WindowFrameUnits,
};

use crate::Result;
use crate::ast::{self, BindingValue, ExprKind, Operator, Source};

const HIDDEN: &str = "datafusion_kql:hidden";
const ORDINAL: &str = "datafusion_kql:ordinal";

pub(crate) fn hidden(field: &Field) -> bool {
    field
        .metadata()
        .get(HIDDEN)
        .is_some_and(|value| value == "true")
}

pub(crate) fn ordinal(schema: &DFSchema) -> Option<Expr> {
    schema.fields().iter().enumerate().find_map(|(i, field)| {
        field
            .metadata()
            .get(ORDINAL)
            .is_some_and(|value| value == "true")
            .then(|| Expr::Column(Column::from(schema.qualified_field(i))))
    })
}

pub(crate) fn helper(value: Expr, name: String, ordinal: bool) -> Expr {
    let mut metadata = BTreeMap::from([(HIDDEN.to_owned(), "true".to_owned())]);
    metadata.insert(
        ORDINAL.to_owned(),
        if ordinal { "true" } else { "false" }.to_owned(),
    );
    value.alias_with_metadata(name, Some(FieldMetadata::from(metadata)))
}

pub(crate) fn user_alias(value: Expr, name: String, schema: &DFSchema) -> Result<Expr> {
    let metadata = value.metadata(schema)?;
    if metadata.inner().get(HIDDEN).is_some_and(|v| v == "true") {
        let mut metadata = metadata.inner().clone();
        metadata.insert(HIDDEN.to_owned(), "false".to_owned());
        metadata.insert(ORDINAL.to_owned(), "false".to_owned());
        Ok(value.alias_with_metadata(name, Some(FieldMetadata::from(metadata))))
    } else {
        Ok(value.alias(name))
    }
}

fn select(input: LogicalPlan, keep_order: bool) -> Result<LogicalPlan> {
    let kept = input
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            !hidden(field)
                || keep_order && field.metadata().get(ORDINAL).is_some_and(|v| v == "true")
        })
        .map(|(i, _)| Expr::Column(Column::from(input.schema().qualified_field(i))))
        .collect::<Vec<_>>();
    if kept.len() == input.schema().fields().len() {
        return Ok(input);
    }
    Ok(LogicalPlanBuilder::from(input).project(kept)?.build()?)
}

pub(crate) fn visible(input: LogicalPlan) -> Result<LogicalPlan> {
    select(input, false)
}
pub(crate) fn finalize(input: LogicalPlan) -> Result<LogicalPlan> {
    // The final ordering requirement prevents repartitioning a serialized
    // result after windows have finished evaluating their row-local values.
    let input = if let Some(order) = ordinal(input.schema()) {
        LogicalPlanBuilder::from(input)
            .sort(vec![order.sort(true, false)])?
            .build()?
    } else {
        input
    };
    visible(input)
}
pub(crate) fn cleanup(input: LogicalPlan) -> Result<LogicalPlan> {
    select(input, true)
}

pub(crate) fn sequence(
    state: &SessionState,
    scalars: &HashMap<String, Expr>,
    input: LogicalPlan,
    keys: Vec<Sort>,
) -> Result<LogicalPlan> {
    let mut index = 0;
    let name = loop {
        let name = format!("__kql_row_order_{index}");
        // Keep the native window field distinct from its metadata-bearing alias.
        let raw = format!("{name}_value");
        if input
            .schema()
            .index_of_column_by_name(None, &name)
            .is_none()
            && !scalars.contains_key(&name)
            && input.schema().index_of_column_by_name(None, &raw).is_none()
            && !scalars.contains_key(&raw)
        {
            break name;
        }
        index += 1;
    };
    let row = state
        .udwf("row_number")?
        .call(Vec::new())
        .order_by(keys)
        .window_frame(WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(datafusion::common::ScalarValue::UInt64(None)),
            WindowFrameBound::CurrentRow,
        ))
        .build()?;
    let raw = format!("{name}_value");
    let row = row.alias(raw.clone());
    let window = LogicalPlanBuilder::from(input.clone())
        .window(vec![row])?
        .build()?;
    let mut projection = input
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !hidden(f))
        .map(|(i, _)| Expr::Column(Column::from(input.schema().qualified_field(i))))
        .collect::<Vec<_>>();
    projection.push(helper(Expr::Column(Column::from_name(raw)), name, true));
    Ok(LogicalPlanBuilder::from(window)
        .project(projection)?
        .build()?)
}

pub(crate) fn is_window(name: &str) -> bool {
    matches!(
        name,
        "prev"
            | "next"
            | "row_number"
            | "row_cumsum"
            | "row_rank_dense"
            | "row_rank_min"
            | "row_window_session"
    )
}

fn any_expression(expression: &ast::Expr, predicate: fn(&str) -> bool) -> bool {
    match &expression.kind {
        ExprKind::Call { name, args } => {
            predicate(name) || args.iter().any(|e| any_expression(e, predicate))
        }
        ExprKind::Unary { expr, .. } | ExprKind::Dynamic(expr) | ExprKind::Cast { expr, .. } => {
            any_expression(expr, predicate)
        }
        ExprKind::NamedArgument { value, .. } => any_expression(value, predicate),
        ExprKind::ToScalar(value) => match value.as_ref() {
            ast::ArgumentValue::Scalar(expr) => any_expression(expr, predicate),
            ast::ArgumentValue::Tabular(query) => needed(query),
        },
        ExprKind::Binary { left, right, .. } => {
            any_expression(left, predicate) || any_expression(right, predicate)
        }
        ExprKind::Access { expr, index } => {
            any_expression(expr, predicate) || any_expression(index, predicate)
        }
        ExprKind::Array(items) => items.iter().any(|e| any_expression(e, predicate)),
        ExprKind::Object(items) => items.iter().any(|(_, e)| any_expression(e, predicate)),
        ExprKind::InList { expr, values, .. } => {
            any_expression(expr, predicate) || values.iter().any(|e| any_expression(e, predicate))
        }
        ExprKind::Between {
            expr, low, high, ..
        } => [expr, low, high]
            .iter()
            .any(|e| any_expression(e, predicate)),
        _ => false,
    }
}

pub(crate) fn expressions(operator: &Operator) -> Vec<&ast::Expr> {
    match operator {
        Operator::Where(expr) | Operator::Parse { input: expr, .. } => vec![expr],
        Operator::Project(items) | Operator::Extend(items) | Operator::Serialize(items) => {
            items.iter().map(|i| &i.expr).collect()
        }
        Operator::MvExpand { columns, .. } | Operator::MvApply { columns, .. } => {
            columns.iter().map(|c| &c.value.expr).collect()
        }
        Operator::Scan(scan) => scan
            .steps
            .iter()
            .flat_map(|step| {
                std::iter::once(&step.condition)
                    .chain(step.assignments.iter().map(|(_, value)| value))
            })
            .collect(),
        Operator::Summarize { aggregates, by } => {
            aggregates.iter().chain(by).map(|i| &i.expr).collect()
        }
        Operator::MakeSeries(series) => series
            .aggregates
            .iter()
            .map(|a| &a.value.expr)
            .chain(series.by.iter().map(|b| &b.expr))
            .chain(std::iter::once(&series.axis))
            .collect(),
        Operator::Sort(keys) => keys.iter().map(|k| &k.expr).collect(),
        Operator::Top { key, .. } => vec![&key.expr],
        Operator::Sample {
            distinct: Some(value),
            ..
        } => vec![value],
        Operator::Distinct(items) => items.iter().collect(),
        _ => Vec::new(),
    }
}

pub(crate) fn uses_windows(operator: &Operator) -> bool {
    expressions(operator)
        .iter()
        .any(|e| any_expression(e, is_window))
}

pub(crate) fn needed(query: &ast::Query) -> bool {
    fn interesting(name: &str) -> bool {
        is_window(name)
            || matches!(
                name,
                "make_list" | "make_list_if" | "make_list_with_nulls" | "makelist"
            )
    }
    fn source(value: &Source) -> bool {
        match value {
            Source::Subquery(query) => needed(query),
            Source::Union { inputs, .. } => inputs.iter().any(source),
            Source::Print(items) => items.iter().any(|i| any_expression(&i.expr, interesting)),
            Source::Call { args, .. } => args.iter().any(|arg| match &arg.value {
                ast::ArgumentValue::Scalar(expr) => any_expression(expr, interesting),
                ast::ArgumentValue::Tabular(query) => needed(query),
            }),
            _ => false,
        }
    }
    fn pipeline(pipeline: &ast::Pipeline) -> bool {
        source(&pipeline.source)
            || pipeline.operators.iter().any(|operator| {
                matches!(operator.kind, Operator::Serialize(_) | Operator::Scan(_))
                    || expressions(&operator.kind)
                        .iter()
                        .any(|e| any_expression(e, interesting))
                    || match &operator.kind {
                        Operator::Join { input, .. } | Operator::Lookup { input, .. } => {
                            needed(input)
                        }
                        Operator::Union { inputs, .. } => inputs.iter().any(source),
                        Operator::MvApply { operators, .. }
                        | Operator::Partition { operators, .. } => {
                            operators.iter().any(|operator| {
                                matches!(operator.kind, Operator::Serialize(_) | Operator::Scan(_))
                                    || expressions(&operator.kind)
                                        .iter()
                                        .any(|expr| any_expression(expr, interesting))
                                    || matches!(
                                        &operator.kind,
                                        Operator::MvApply { .. } | Operator::Partition { .. }
                                    )
                            })
                        }
                        _ => false,
                    }
            })
    }
    fn binding_needed(binding: &ast::Binding) -> bool {
        match &binding.value {
            BindingValue::Scalar(expr) => any_expression(expr, interesting),
            BindingValue::Tabular(value) => pipeline(value),
            BindingValue::Parameter { default, .. } => default
                .as_ref()
                .is_some_and(|value| any_expression(value, interesting)),
            BindingValue::Function(function) => {
                function.bindings.iter().any(binding_needed)
                    || match &function.body {
                        ast::FunctionBody::Scalar(expr) => any_expression(expr, interesting),
                        ast::FunctionBody::Tabular(value) => pipeline(value),
                    }
            }
        }
    }
    query.bindings.iter().any(binding_needed) || pipeline(&query.pipeline)
}
