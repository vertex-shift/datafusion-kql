use std::collections::{HashMap, HashSet};

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::expr_fn::{cast, try_cast};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::lit;

use crate::ast::{self, BinaryOp, ExprKind, Find, Span};
use crate::dynamic::{self, Kernel};
use crate::expressions::{Expressions, is_string};
use crate::{Error, Result, ordering};

fn field(plan: &LogicalPlan, index: usize) -> Expr {
    Expr::Column(Column::from(plan.schema().qualified_field(index)))
}

fn columns(expr: &ast::Expr, names: &mut HashSet<String>) {
    if let ExprKind::Identifier(parts) = &expr.kind {
        names.insert(parts[0].clone());
    }
    if let ExprKind::SearchTerm {
        column: Some(name), ..
    } = &expr.kind
    {
        names.insert(name.clone());
    }
    expr.map_children(|child| {
        columns(child, names);
        Ok(child.clone())
    })
    .expect("search column inspection does not fail");
}

fn any_column(expr: &ast::Expr) -> bool {
    match &expr.kind {
        ExprKind::Literal(ast::Literal::String(_))
        | ExprKind::Wildcard
        | ExprKind::SearchTerm { column: None, .. } => true,
        ExprKind::Binary {
            left,
            op: BinaryOp::And | BinaryOp::Or,
            right,
        } => any_column(left) || any_column(right),
        ExprKind::Binary { left, .. } if matches!(left.kind, ExprKind::Wildcard) => true,
        ExprKind::Unary { expr, .. } => any_column(expr),
        _ => false,
    }
}

fn bag(input: &LogicalPlan, indices: &[usize]) -> Expr {
    dynamic::call(
        Kernel::PackAll {
            names: indices
                .iter()
                .map(|&i| input.schema().field(i).name().clone())
                .collect(),
            ignore_empty: false,
        },
        indices.iter().map(|&i| field(input, i)).collect(),
    )
}

pub(crate) fn plan(
    expressions: &Expressions<'_>,
    legs: Vec<(String, LogicalPlan, ast::Expr)>,
    predicate: &ast::Expr,
    case_sensitive: bool,
    find: Option<&Find>,
) -> Result<LogicalPlan> {
    let source = find.map_or("$table", |find| find.source.as_str());
    let mut referenced = HashSet::new();
    columns(predicate, &mut referenced);
    referenced.remove(source);
    let mut types = HashMap::<String, Vec<DataType>>::new();
    for (_, input, _) in &legs {
        for field in input
            .schema()
            .fields()
            .iter()
            .filter(|field| !ordering::hidden(field))
        {
            let ty = crate::tabular::canonical(field.data_type());
            let candidates = types.entry(field.name().clone()).or_default();
            if !candidates.contains(&ty) {
                candidates.push(ty);
            }
        }
        if input
            .schema()
            .index_of_column_by_name(None, source)
            .is_some()
        {
            return Err(Error::Plan(format!(
                "search source column '{source}' conflicts with input"
            )));
        }
    }
    for name in &referenced {
        if !types.contains_key(name) && !expressions.scalars.contains_key(name) {
            return Err(Error::syntax(
                format!("unknown search column '{name}'"),
                predicate.span,
            ));
        }
    }
    let legs = legs
        .into_iter()
        .filter(|(_, plan, _)| {
            any_column(predicate)
                || referenced.is_empty()
                || plan
                    .schema()
                    .fields()
                    .iter()
                    .any(|field| referenced.contains(field.name()))
        })
        .collect::<Vec<_>>();
    let mut selected = referenced.clone();
    if let Some((_, first, _)) = legs.first() {
        for field in first
            .schema()
            .fields()
            .iter()
            .filter(|field| !ordering::hidden(field))
        {
            if legs.iter().all(|(_, plan, _)| {
                plan.schema()
                    .index_of_column_by_name(None, field.name())
                    .is_some_and(|i| {
                        crate::tabular::canonical(plan.schema().field(i).data_type())
                            == crate::tabular::canonical(field.data_type())
                    })
            }) {
                selected.insert(field.name().clone());
            }
        }
    }
    let packed = legs.iter().any(|(_, input, _)| {
        input
            .schema()
            .fields()
            .iter()
            .any(|field| !ordering::hidden(field) && !selected.contains(field.name()))
    });
    let mut labels = Vec::new();
    let mut plans = Vec::new();
    for (label, input, prepared) in legs {
        let input = ordering::visible(input)?;
        let mut locals = expressions.scalars.clone();
        locals.insert(source.to_owned(), lit(label.clone()));
        let local = Expressions {
            state: expressions.state,
            scalars: &locals,
            now: expressions.now,
        };
        let predicate = filter(&local, &input, &prepared, case_sensitive, &types)?;
        crate::expressions::check_expression(&predicate)?;
        let filtered = LogicalPlanBuilder::from(input.clone())
            .filter(predicate)?
            .build()?;
        let mut projected = Vec::new();
        if let Some(project) = find.and_then(|find| find.project.as_ref()) {
            let mut used = HashSet::new();
            for (name, requested) in project {
                if !used.insert(name) || name == source {
                    return Err(Error::Plan(format!(
                        "duplicate find output column '{name}'"
                    )));
                }
                let ty = if let Some(ty) = requested {
                    crate::literals::data_type(*ty)
                } else {
                    let candidates = types
                        .get(name)
                        .ok_or_else(|| Error::Plan(format!("unknown find projection '{name}'")))?;
                    if candidates.len() != 1 {
                        return Err(Error::Plan(format!(
                            "find projection '{name}' requires an explicit type"
                        )));
                    }
                    candidates[0].clone()
                };
                let value =
                    if let Some(index) = filtered.schema().index_of_column_by_name(None, name) {
                        let column = field(&filtered, index);
                        if dynamic::is_dynamic(filtered.schema().field(index).data_type())
                            && ty == DataType::Utf8
                        {
                            dynamic::call(Kernel::Convert(ty.clone()), vec![column])
                        } else {
                            try_cast(column, ty.clone())
                        }
                    } else {
                        lit(ScalarValue::try_from(&ty)?)
                    };
                let value = if is_string(&ty) {
                    local.function("coalesce", vec![value, lit("")])?
                } else {
                    value
                };
                projected.push(value.alias(name));
            }
            if find.is_some_and(|find| find.pack_all) {
                if used.contains(&"column1".to_owned()) {
                    return Err(Error::Plan(
                        "find pack_all output column 'column1' conflicts with a projection".into(),
                    ));
                }
                projected.push(
                    bag(
                        &filtered,
                        &(0..filtered.schema().fields().len()).collect::<Vec<_>>(),
                    )
                    .alias("column1"),
                );
            }
        } else {
            let mut rest = Vec::new();
            for (index, field) in filtered.schema().fields().iter().enumerate() {
                if selected.contains(field.name()) {
                    projected.push(crate::search::field(&filtered, index));
                } else {
                    rest.push(index);
                }
            }
            if packed {
                if selected.contains("pack_") {
                    return Err(Error::Plan(
                        "search packed output 'pack_' conflicts with a selected column".into(),
                    ));
                }
                projected.push(bag(&filtered, &rest).alias("pack_"));
            }
        }
        labels.push(label);
        plans.push(
            LogicalPlanBuilder::from(filtered)
                .project(projected)?
                .build()?,
        );
    }
    crate::tabular::union(plans, ast::UnionKind::Outer, Some(source), &labels)
}

pub(crate) fn filter(
    expressions: &Expressions<'_>,
    input: &LogicalPlan,
    predicate: &ast::Expr,
    sensitive: bool,
    types: &HashMap<String, Vec<DataType>>,
) -> Result<Expr> {
    match &predicate.kind {
        ExprKind::Literal(ast::Literal::String(text)) => {
            term(expressions, input, None, text, sensitive, predicate.span)
        }
        ExprKind::SearchTerm { column, term: text } => term(
            expressions,
            input,
            column.as_deref(),
            text,
            sensitive,
            predicate.span,
        ),
        ExprKind::Wildcard => Ok(lit(true)),
        ExprKind::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => Ok(
            filter(expressions, input, left, sensitive, types)?.and(filter(
                expressions,
                input,
                right,
                sensitive,
                types,
            )?),
        ),
        ExprKind::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => Ok(
            filter(expressions, input, left, sensitive, types)?.or(filter(
                expressions,
                input,
                right,
                sensitive,
                types,
            )?),
        ),
        ExprKind::Unary {
            op: ast::UnaryOp::Not,
            expr,
        } => Ok(!filter(expressions, input, expr, sensitive, types)?),
        ExprKind::Call { name, args } if name == "not" && args.len() == 1 => {
            Ok(!filter(expressions, input, &args[0], sensitive, types)?)
        }
        ExprKind::Binary { left, op, right } if matches!(left.kind, ExprKind::Wildcard) => {
            let mut predicates = Vec::new();
            for (_, field) in input
                .schema()
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, field)| !ordering::hidden(field))
            {
                let value = ast::Expr {
                    kind: ExprKind::Identifier(vec![field.name().clone()]),
                    span: left.span,
                    depth: 1,
                };
                let left = ast::Expr {
                    kind: ExprKind::Call {
                        name: "tostring".into(),
                        args: vec![value],
                    },
                    span: left.span,
                    depth: 1,
                };
                let expr = ast::Expr {
                    kind: ExprKind::Binary {
                        left: Box::new(left),
                        op: *op,
                        right: right.clone(),
                    },
                    span: predicate.span,
                    depth: predicate.depth,
                };
                predicates.push(filter(expressions, input, &expr, sensitive, types)?);
            }
            Ok(disjunction(predicates))
        }
        _ => {
            let mut locals = expressions.scalars.clone();
            let rewritten = fill(predicate, input, types, &mut locals, sensitive)?;
            Expressions {
                state: expressions.state,
                scalars: &locals,
                now: expressions.now,
            }
            .predicate(&rewritten, input.schema())
        }
    }
}

fn fill(
    expr: &ast::Expr,
    input: &LogicalPlan,
    types: &HashMap<String, Vec<DataType>>,
    locals: &mut HashMap<String, Expr>,
    sensitive: bool,
) -> Result<ast::Expr> {
    if let ExprKind::Identifier(parts) = &expr.kind
        && input
            .schema()
            .index_of_column_by_name(None, &parts[0])
            .is_none()
        && !locals.contains_key(&parts[0])
        && let Some(types) = types.get(&parts[0])
    {
        let ty = &types[0];
        locals.insert(
            parts[0].clone(),
            if is_string(ty) {
                lit("")
            } else {
                lit(ScalarValue::try_from(ty)?)
            },
        );
    }
    let mut expr = expr.map_children(|child| fill(child, input, types, locals, sensitive))?;
    if sensitive && let ExprKind::Binary { op, .. } = &mut expr.kind {
        *op = match *op {
            BinaryOp::Has => BinaryOp::HasCs,
            BinaryOp::NotHas => BinaryOp::NotHasCs,
            BinaryOp::Contains => BinaryOp::ContainsCs,
            BinaryOp::NotContains => BinaryOp::NotContainsCs,
            BinaryOp::StartsWith => BinaryOp::StartsWithCs,
            BinaryOp::NotStartsWith => BinaryOp::NotStartsWithCs,
            BinaryOp::EndsWith => BinaryOp::EndsWithCs,
            BinaryOp::NotEndsWith => BinaryOp::NotEndsWithCs,
            op => op,
        };
    }
    Ok(expr)
}

fn term(
    expressions: &Expressions<'_>,
    input: &LogicalPlan,
    column: Option<&str>,
    text: &str,
    sensitive: bool,
    span: Span,
) -> Result<Expr> {
    if text.is_empty() {
        return Ok(lit(false));
    }
    let mut pattern = text
        .split('*')
        .map(regex::escape)
        .collect::<Vec<_>>()
        .join(".*");
    if !text.starts_with('*') {
        pattern = format!(r"(?:^|[^\p{{L}}\p{{N}}]){pattern}");
    }
    if !text.ends_with('*') {
        pattern.push_str(r"(?:$|[^\p{L}\p{N}])");
    }
    let mut predicates = Vec::new();
    for (index, field) in input
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !ordering::hidden(field))
    {
        if column.is_none() && field.name() == "$table" {
            continue;
        }
        if column.is_some_and(|name| name != field.name()) {
            continue;
        }
        let value = if dynamic::is_dynamic(field.data_type()) {
            dynamic::call(
                Kernel::Convert(DataType::Utf8),
                vec![crate::search::field(input, index)],
            )
        } else {
            cast(crate::search::field(input, index), DataType::Utf8)
        };
        let mut args = vec![value, lit(pattern.clone())];
        if !sensitive {
            args.push(lit("i"));
        }
        let matched = expressions.function("regexp_like", args)?;
        predicates.push(expressions.function("coalesce", vec![matched, lit(false)])?);
    }
    if predicates.is_empty() && column.is_some() && input.schema().fields().is_empty() {
        return Err(Error::syntax("search has no searchable columns", span));
    }
    Ok(disjunction(predicates))
}

fn disjunction(mut predicates: Vec<Expr>) -> Expr {
    while predicates.len() > 1 {
        let mut next = Vec::with_capacity(predicates.len().div_ceil(2));
        let mut values = predicates.into_iter();
        while let Some(left) = values.next() {
            next.push(if let Some(right) = values.next() {
                left.or(right)
            } else {
                left
            });
        }
        predicates = next;
    }
    predicates.pop().unwrap_or(lit(false))
}
