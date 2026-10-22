use std::collections::{HashMap, HashSet};

use datafusion::common::{Column, DFSchema, NullEquality, ScalarValue};
use datafusion::logical_expr::expr_fn::{ExprFunctionExt, when};
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::logical_expr::{Expr, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::lit;

use crate::ast::{self, ExprKind, NamedExpr, NestedTop, Span};
use crate::dynamic::{self, Kernel};
use crate::expressions::Expressions;
use crate::{Error, Result, ordering};

struct Names {
    prefix: String,
    next: usize,
    used: HashSet<String>,
}
impl Names {
    fn fresh(&mut self, kind: &str) -> String {
        loop {
            let name = format!("{}_{}_{}", self.prefix, kind, self.next);
            self.next += 1;
            if self.used.insert(name.clone()) {
                return name;
            }
        }
    }
}
fn field(plan: &LogicalPlan, index: usize) -> Expr {
    Expr::Column(Column::from(plan.schema().qualified_field(index)))
}
fn named(plan: &LogicalPlan, name: &str) -> Result<Expr> {
    Ok(field(
        plan,
        plan.schema()
            .index_of_column_by_name(None, name)
            .ok_or_else(|| Error::Plan(format!("missing internal top column '{name}'")))?,
    ))
}
fn ast_column(name: &str) -> ast::Expr {
    ast::Expr {
        kind: ExprKind::Identifier(vec![name.into()]),
        span: Span::default(),
        depth: 1,
    }
}

pub(crate) fn count(expressions: &Expressions<'_>, expr: &ast::Expr, real: bool) -> Result<usize> {
    let value = expressions.constant(expressions.scalar(expr, &DFSchema::empty())?, expr.span)?;
    if !(value.data_type().is_integer() || real && value.data_type().is_floating()) {
        return Err(Error::syntax(
            "top count must be a nonnegative integer",
            expr.span,
        ));
    }
    if real
        && let ScalarValue::Float64(Some(value)) = value
        && value.is_finite()
        && value >= 0.0
        && value < (usize::MAX as f64)
    {
        return Ok(value as usize);
    }
    match value.cast_to(&datafusion::arrow::datatypes::DataType::Int64)? {
        ScalarValue::Int64(Some(value)) if value >= 0 => {
            usize::try_from(value).map_err(|_| Error::syntax("top count is too large", expr.span))
        }
        _ => Err(Error::syntax(
            "top count must be a nonnegative integer",
            expr.span,
        )),
    }
}

pub(crate) fn hitters(
    expressions: &Expressions<'_>,
    input: LogicalPlan,
    count_expr: &ast::Expr,
    value: &ast::Expr,
    weight: Option<&ast::Expr>,
) -> Result<LogicalPlan> {
    let count = count(expressions, count_expr, true)?;
    let value_name = crate::planner::default_name(value, 0);
    let mut input = ordering::visible(input)?;
    let lowered = expressions.scalar(value, input.schema())?;
    let encoded = dynamic::is_dynamic(&lowered.get_type(input.schema())?);
    let grouping = if encoded {
        let mut name = "__kql_hitters_key".to_owned();
        while input
            .schema()
            .index_of_column_by_name(None, &name)
            .is_some()
            || expressions.scalars.contains_key(&name)
        {
            name.push('_');
        }
        let mut columns = (0..input.schema().fields().len())
            .map(|i| field(&input, i))
            .collect::<Vec<_>>();
        columns.push(dynamic::call(Kernel::EncodeWindow, vec![lowered]).alias(&name));
        input = LogicalPlanBuilder::from(input).project(columns)?.build()?;
        ast_column(&name)
    } else {
        value.clone()
    };
    let (function, args, measure) = if let Some(weight) = weight {
        let ty = expressions
            .scalar(weight, input.schema())?
            .get_type(input.schema())?;
        if !ty.is_numeric() {
            return Err(Error::syntax(
                "top-hitters weights must be numeric",
                weight.span,
            ));
        }
        (
            "sum",
            vec![weight.clone()],
            format!(
                "approximate_sum_{}",
                crate::planner::default_name(weight, 0)
            ),
        )
    } else {
        (
            "count",
            Vec::new(),
            format!("approximate_count_{value_name}"),
        )
    };
    let aggregate = NamedExpr {
        name: Some(measure.clone()),
        expr: ast::Expr {
            kind: ExprKind::Call {
                name: function.into(),
                args,
            },
            span: value.span,
            depth: value.depth + 1,
        },
    };
    let grouped = crate::aggregation::plan(
        expressions.state,
        expressions.scalars,
        expressions.now,
        input,
        &[aggregate],
        &[NamedExpr {
            name: Some(value_name.clone()),
            expr: grouping,
        }],
    )?;
    let order = named(&grouped, &measure)?.sort(false, false);
    let result = LogicalPlanBuilder::from(grouped)
        .sort(vec![order])?
        .limit(0, Some(count))?
        .build()?;
    if encoded {
        let key = dynamic::call(Kernel::DecodeWindow, vec![named(&result, &value_name)?])
            .alias(&value_name);
        let score = named(&result, &measure)?;
        Ok(LogicalPlanBuilder::from(result)
            .project(vec![key, score])?
            .build()?)
    } else {
        Ok(result)
    }
}

fn aggregate(
    expressions: &Expressions<'_>,
    input: LogicalPlan,
    keys: &[String],
    value: &NamedExpr,
    score: &str,
) -> Result<LogicalPlan> {
    let groups = keys
        .iter()
        .map(|key| NamedExpr {
            name: Some(key.clone()),
            expr: ast_column(key),
        })
        .collect::<Vec<_>>();
    crate::aggregation::plan(
        expressions.state,
        expressions.scalars,
        expressions.now,
        input,
        &[NamedExpr {
            name: Some(score.into()),
            expr: value.expr.clone(),
        }],
        &groups,
    )
}

fn select(
    expressions: &Expressions<'_>,
    input: LogicalPlan,
    prefix: &[String],
    score: &str,
    count: Option<usize>,
    ascending: bool,
    names: &mut Names,
) -> Result<LogicalPlan> {
    let Some(count) = count else {
        return Ok(input);
    };
    let sort = named(&input, score)?.sort(ascending, ascending);
    if prefix.is_empty() {
        return Ok(LogicalPlanBuilder::from(input)
            .sort(vec![sort])?
            .limit(0, Some(count))?
            .build()?);
    }
    let rank = names.fresh("rank");
    let value = expressions
        .state
        .udwf("row_number")?
        .call(Vec::new())
        .partition_by(
            prefix
                .iter()
                .map(|key| named(&input, key))
                .collect::<Result<Vec<_>>>()?,
        )
        .order_by(vec![sort])
        .build()?
        .alias(&rank);
    let columns = (0..input.schema().fields().len())
        .map(|i| field(&input, i))
        .collect::<Vec<_>>();
    Ok(LogicalPlanBuilder::from(input)
        .window(vec![value])?
        .filter(Expr::Column(Column::from_name(rank)).lt_eq(lit(count as u64)))?
        .project(columns)?
        .build()?)
}

fn attach(
    left: LogicalPlan,
    right: LogicalPlan,
    keys: &[String],
    kind: JoinType,
    extra: &str,
    names: &mut Names,
) -> Result<LogicalPlan> {
    let alias = names.fresh("groups");
    let mut renamed = HashMap::new();
    let mut projection = Vec::new();
    for (index, field_) in right.schema().fields().iter().enumerate() {
        let name = names.fresh("join");
        projection.push(field(&right, index).alias(&name));
        renamed.insert(field_.name().clone(), name);
    }
    let right = LogicalPlanBuilder::from(right)
        .project(projection)?
        .alias(alias)?
        .build()?;
    let left_keys = keys
        .iter()
        .map(|key| match named(&left, key)? {
            Expr::Column(column) => Ok(column),
            _ => Err(Error::Plan("top join key is not a column".into())),
        })
        .collect::<Result<Vec<_>>>()?;
    let right_keys = keys
        .iter()
        .map(|key| {
            match named(
                &right,
                renamed
                    .get(key)
                    .ok_or_else(|| Error::Plan("missing top join key".into()))?,
            )? {
                Expr::Column(column) => Ok(column),
                _ => Err(Error::Plan("top join key is not a column".into())),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let count = left.schema().fields().len();
    let appended = named(
        &right,
        renamed
            .get(extra)
            .ok_or_else(|| Error::Plan("missing top join output".into()))?,
    )?;
    let columns = (0..count)
        .map(|i| field(&left, i))
        .chain(std::iter::once(appended.alias(extra)))
        .collect::<Vec<_>>();
    Ok(LogicalPlanBuilder::from(left)
        .join_detailed(
            right,
            kind,
            (left_keys, right_keys),
            None,
            NullEquality::NullEqualsNull,
        )?
        .project(columns)?
        .build()?)
}

pub(crate) fn nested(
    expressions: &Expressions<'_>,
    input: LogicalPlan,
    levels: &[NestedTop],
    temporary: String,
) -> Result<LogicalPlan> {
    if levels.is_empty() || levels.len() > 32 {
        return Err(Error::Plan(
            "top-nested requires 1-32 hierarchy levels".into(),
        ));
    }
    let input = ordering::visible(input)?;
    let mut names = Names {
        prefix: temporary,
        next: 0,
        used: input
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .chain(expressions.scalars.keys().cloned())
            .collect(),
    };
    let mut output_names = HashSet::new();
    let mut working = if levels.len() > 1 || levels.iter().any(|level| level.others.is_some()) {
        crate::materialize::plan(input, names.fresh("source"))?
    } else {
        input
    };
    let mut keys = Vec::new();
    let mut outputs = Vec::new();
    let mut score_columns = Vec::new();
    for (index, level) in levels.iter().enumerate() {
        let value_name = crate::planner::output_name(&level.value, index, &mut output_names)?;
        let score_name = level
            .aggregate
            .name
            .clone()
            .unwrap_or_else(|| format!("aggregated_{value_name}"));
        if !output_names.insert(score_name.clone()) {
            return Err(Error::syntax(
                "duplicate top-nested output name",
                level.aggregate.expr.span,
            ));
        }
        let value = expressions.scalar(&level.value.expr, working.schema())?;
        let ty = value.get_type(working.schema())?;
        let dynamic = dynamic::is_dynamic(&ty);
        let value = if dynamic {
            dynamic::call(Kernel::EncodeWindow, vec![value])
        } else {
            value
        };
        let key = names.fresh("key");
        let score = names.fresh("score");
        score_columns.push(score.clone());
        let mut projection = (0..working.schema().fields().len())
            .map(|i| field(&working, i))
            .collect::<Vec<_>>();
        projection.push(value.alias(&key));
        working = LogicalPlanBuilder::from(working)
            .project(projection)?
            .build()?;
        let mut groups = keys.clone();
        groups.push(key.clone());
        let grouped = aggregate(
            expressions,
            working.clone(),
            &groups,
            &level.aggregate,
            &score,
        )?;
        let limit = level
            .count
            .as_ref()
            .map(|expr| count(expressions, expr, false))
            .transpose()?;
        let selected = select(
            expressions,
            grouped,
            &keys,
            &score,
            limit,
            level.ascending,
            &mut names,
        )?;
        if levels.len() == 1 && level.others.is_none() {
            let key_expr = named(&selected, &key)?;
            let key_expr = if dynamic {
                dynamic::call(Kernel::DecodeWindow, vec![key_expr])
            } else {
                key_expr
            };
            let score_expr = named(&selected, &score)?;
            return Ok(LogicalPlanBuilder::from(selected)
                .project(vec![
                    key_expr.alias(value_name),
                    score_expr.alias(score_name),
                ])?
                .build()?);
        }
        let mut others_output = None;
        if let Some(others) = &level.others {
            let value = expressions
                .constant(expressions.scalar(others, &DFSchema::empty())?, others.span)?;
            let other = if dynamic {
                let expression = dynamic::call(
                    Kernel::EncodeWindow,
                    vec![dynamic::call(Kernel::IntoDynamic, vec![lit(value)])],
                );
                lit(expressions.constant(expression, others.span)?)
            } else {
                lit(value.cast_to(&ty)?)
            };
            let marker = names.fresh("selected");
            let mut columns = groups
                .iter()
                .map(|key| named(&selected, key))
                .collect::<Result<Vec<_>>>()?;
            columns.push(lit(true).alias(&marker));
            let selected = LogicalPlanBuilder::from(selected)
                .project(columns)?
                .build()?;
            let selected = crate::materialize::plan(selected, names.fresh("selected_cache"))?;
            let joined = attach(
                working,
                selected,
                &groups,
                JoinType::Left,
                &marker,
                &mut names,
            )?;
            let is_other = named(&joined, &marker)?.is_null();
            let flag = names.fresh("other");
            let mut columns = Vec::new();
            for (i, field_) in joined.schema().fields().iter().enumerate() {
                if field_.name() == &marker {
                    continue;
                }
                let value = field(&joined, i);
                columns.push(if field_.name() == &key {
                    when(
                        is_other.clone(),
                        lit(ScalarValue::try_from(field_.data_type())?),
                    )
                    .otherwise(value)?
                    .alias(&key)
                } else {
                    value
                });
            }
            columns.push(is_other.alias(&flag));
            working = LogicalPlanBuilder::from(joined).project(columns)?.build()?;
            groups.push(flag.clone());
            let grouped = aggregate(
                expressions,
                working.clone(),
                &groups,
                &level.aggregate,
                &score,
            )?;
            let grouped = crate::materialize::plan(grouped, names.fresh("other_cache"))?;
            working = attach(
                working,
                grouped,
                &groups,
                JoinType::Inner,
                &score,
                &mut names,
            )?;
            others_output = Some((flag, other));
        } else {
            let selected = crate::materialize::plan(selected, names.fresh("selected_cache"))?;
            working = attach(
                working,
                selected,
                &groups,
                JoinType::Inner,
                &score,
                &mut names,
            )?;
        }
        keys = groups;
        let value = if let Some((flag, other)) = others_output {
            when(named(&working, &flag)?, other).otherwise(named(&working, &key)?)?
        } else {
            named(&working, &key)?
        };
        let value = if dynamic {
            dynamic::call(Kernel::DecodeWindow, vec![value])
        } else {
            value
        };
        outputs.push(value.alias(value_name));
        outputs.push(named(&working, &score)?.alias(score_name));
    }
    let internal = keys
        .iter()
        .chain(&score_columns)
        .map(|name| named(&working, name))
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlanBuilder::from(working)
        .project(internal)?
        .distinct()?
        .project(outputs)?
        .build()?)
}
