use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::array::new_empty_array;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Column, DFSchema, ScalarValue, TableReference};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::expr_fn::cast;
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::logical_expr::{Expr, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionContext, lit};

use crate::QueryParameters;
use crate::ast::{
    self, BindingValue, ExprKind, JoinKind, NamedExpr, Operator, Source, Span, UnionKind,
};
use crate::dynamic::{self, Kernel};
use crate::error::{Error, Result};
use crate::expressions::{Expressions, is_string};
use crate::functions::{BoundFunction, Expander, Functions};
use crate::literals::data_type;
use crate::ordering;

type PlanFuture<'a> = Pin<Box<dyn Future<Output = Result<LogicalPlan>> + Send + 'a>>;
type ExpressionFuture<'a> = Pin<Box<dyn Future<Output = Result<ast::Expr>> + Send + 'a>>;
type MembershipFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(LogicalPlan, ast::Expr)>> + Send + 'a>>;
type LegsFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<(String, LogicalPlan)>>> + Send + 'a>>;

/// Plan a previously parsed query against this session's current catalog.
/// No input batches are read or collected.
pub async fn logical_plan(ctx: &SessionContext, query: &ast::Query) -> Result<LogicalPlan> {
    logical_plan_with_parameters(ctx, query, &QueryParameters::new()).await
}

/// Plan a reusable AST with typed request parameters and no source interpolation.
pub async fn logical_plan_with_parameters(
    ctx: &SessionContext,
    query: &ast::Query,
    parameters: &QueryParameters,
) -> Result<LogicalPlan> {
    let state = ctx.state();
    ordering::finalize(
        Planner::new(&state, ordering::needed(query), parameters)?
            .query(query)
            .await?,
    )
}

pub(crate) async fn dataframe(
    ctx: &SessionContext,
    query: &ast::Query,
    parameters: &QueryParameters,
) -> Result<DataFrame> {
    let state = ctx.state();
    let plan = ordering::finalize(
        Planner::new(&state, ordering::needed(query), parameters)?
            .query(query)
            .await?,
    )?;
    Ok(DataFrame::new(state, plan))
}

struct Planner<'a> {
    state: &'a SessionState,
    scalars: HashMap<String, Expr>,
    tables: HashMap<String, LogicalPlan>,
    functions: Functions,
    deferred: Functions,
    parameters: QueryParameters,
    now: i64,
    next_alias: usize,
    track_order: bool,
}

impl<'s> Planner<'s> {
    fn new(
        state: &'s SessionState,
        track_order: bool,
        parameters: &QueryParameters,
    ) -> Result<Self> {
        let now = state
            .execution_props()
            .query_execution_start_time
            .ok_or_else(|| Error::Plan("session has no query start time".into()))?
            .timestamp_nanos_opt()
            .ok_or_else(|| Error::Plan("query start time is out of range".into()))?;
        Ok(Self {
            state,
            scalars: HashMap::new(),
            tables: HashMap::new(),
            functions: HashMap::new(),
            deferred: HashMap::new(),
            parameters: parameters.clone(),
            now,
            next_alias: 0,
            track_order,
        })
    }

    fn expressions(&self) -> Expressions<'_> {
        Expressions {
            state: self.state,
            scalars: &self.scalars,
            now: self.now,
        }
    }

    fn expander<'a>(&'a mut self, schema: &'a DFSchema) -> Expander<'a> {
        Expander::new(
            &self.functions,
            &mut self.scalars,
            &self.parameters,
            &mut self.next_alias,
            schema,
            &mut self.deferred,
        )
    }

    fn expand_expression(&mut self, expr: &ast::Expr, schema: &DFSchema) -> Result<ast::Expr> {
        if self.functions.is_empty() {
            Ok(expr.clone())
        } else {
            self.expander(schema).expression(expr)
        }
    }

    fn query<'a>(&'a mut self, query: &'a ast::Query) -> PlanFuture<'a> {
        Box::pin(async move {
            let saved_scalars = self.scalars.clone();
            let saved_tables = self.tables.clone();
            let saved_functions = self.functions.clone();
            let result = self.query_inner(query).await;
            self.scalars = saved_scalars;
            self.tables = saved_tables;
            self.functions = saved_functions;
            result
        })
    }

    async fn query_inner(&mut self, query: &ast::Query) -> Result<LogicalPlan> {
        let mut names = HashSet::new();
        for binding in &query.bindings {
            if !names.insert(&binding.name) {
                return Err(Error::syntax(
                    "duplicate let binding in this scope",
                    binding.span,
                ));
            }
            if let BindingValue::Function(definition) = &binding.value {
                let function = BoundFunction::new(
                    *definition.clone(),
                    self.scalars.clone(),
                    self.tables.clone(),
                    self.functions.clone(),
                    HashMap::new(),
                );
                if definition.view && !function.tabular() {
                    return Err(Error::syntax("a view must return a table", binding.span));
                }
                self.scalars.remove(&binding.name);
                self.tables.remove(&binding.name);
                self.functions
                    .insert(binding.name.clone(), Arc::new(function));
                continue;
            }
            if let BindingValue::Parameter { ty, default } = &binding.value {
                let value = if let Some(value) = self.parameters.get(&binding.name) {
                    lit(value.clone())
                } else {
                    let default = default.as_ref().ok_or_else(|| {
                        Error::syntax(
                            format!("missing query parameter '{}'", binding.name),
                            binding.span,
                        )
                    })?;
                    self.expressions().scalar(default, &DFSchema::empty())?
                };
                let value = self.expressions().constant(value, binding.span)?;
                let value = crate::expressions::parameter_cast(
                    lit(value),
                    *ty,
                    &DFSchema::empty(),
                    binding.span,
                )?;
                let value = lit(self.expressions().constant(value, binding.span)?);
                self.tables.remove(&binding.name);
                self.functions.remove(&binding.name);
                self.scalars.insert(binding.name.clone(), value);
                continue;
            }
            if let BindingValue::Scalar(ast::Expr {
                kind: ExprKind::Call { name, args },
                span,
                ..
            }) = &binding.value
                && (self
                    .functions
                    .get(name)
                    .is_some_and(|function| function.tabular())
                    || name == "materialize" && !self.functions.contains_key(name))
            {
                let plan = self
                    .source(&Source::Call {
                        name: name.clone(),
                        args: scalar_arguments(args),
                        span: *span,
                    })
                    .await?;
                check_plan(&plan)?;
                self.scalars.remove(&binding.name);
                self.functions.remove(&binding.name);
                self.tables.insert(binding.name.clone(), plan);
                continue;
            }
            let table_name = match &binding.value {
                BindingValue::Scalar(ast::Expr {
                    kind: ExprKind::Identifier(parts),
                    ..
                }) if !self.scalars.contains_key(&parts[0])
                    && self
                        .functions
                        .get(&parts[0])
                        .is_none_or(|function| function.tabular()) =>
                {
                    Some(parts)
                }
                _ => None,
            };
            if let Some(parts) = table_name {
                let plan = self
                    .source(&Source::Table {
                        name: parts.clone(),
                        span: binding.span,
                    })
                    .await?;
                check_plan(&plan)?;
                self.scalars.remove(&binding.name);
                self.functions.remove(&binding.name);
                self.tables.insert(binding.name.clone(), plan);
            } else {
                match &binding.value {
                    BindingValue::Scalar(expr) => {
                        let expr = self.expand_expression(expr, &DFSchema::empty())?;
                        let expr = self.resolve_expression(&expr).await?;
                        let empty = LogicalPlanBuilder::empty(true).build()?;
                        let (prepared, expr) = self.membership(empty, &expr).await?;
                        let value = self.expressions().scalar(&expr, prepared.schema())?;
                        let value = if prepared.inputs().is_empty() {
                            value
                        } else {
                            Expr::ScalarSubquery(datafusion::logical_expr::Subquery {
                                subquery: Arc::new(
                                    LogicalPlanBuilder::from(prepared)
                                        .project(vec![value.alias("value")])?
                                        .build()?,
                                ),
                                outer_ref_columns: Vec::new(),
                                spans: datafusion::common::Spans::new(),
                            })
                        };
                        self.tables.remove(&binding.name);
                        self.functions.remove(&binding.name);
                        self.scalars.insert(binding.name.clone(), value);
                    }
                    BindingValue::Tabular(pipeline) => {
                        let plan = self.pipeline(pipeline).await?;
                        self.scalars.remove(&binding.name);
                        self.functions.remove(&binding.name);
                        self.tables.insert(binding.name.clone(), plan);
                    }
                    BindingValue::Parameter { .. } | BindingValue::Function(_) => {
                        unreachable!("declarations are handled before ordinary bindings")
                    }
                }
            }
        }
        self.pipeline(&query.pipeline).await
    }

    async fn table(&self, parts: &[String], span: Span) -> Result<LogicalPlan> {
        if parts.len() == 1 {
            if let Some(plan) = self.tables.get(&parts[0]) {
                return Ok(plan.clone());
            }
            if self.scalars.contains_key(&parts[0]) {
                return Err(Error::syntax(
                    "a scalar binding cannot be used as a table",
                    span,
                ));
            }
        }
        let reference = table_reference(parts, span)?;
        let provider = self
            .state
            .schema_for_ref(reference.clone())?
            .table(reference.table())
            .await?
            .ok_or_else(|| Error::Plan(format!("unknown table '{reference}'")))?;
        let plan =
            LogicalPlanBuilder::scan(reference, provider_as_source(provider), None)?.build()?;
        self.normalize_strings(plan)
    }

    fn legs<'a>(&'a mut self, source: &'a Source) -> LegsFuture<'a> {
        Box::pin(async move {
            if let Source::Pattern {
                prefix,
                pattern,
                span,
            } = source
            {
                let mut path = prefix.clone();
                path.push("__kql_pattern".into());
                let schema = self.state.schema_for_ref(table_reference(&path, *span)?)?;
                let mut names = schema.table_names();
                if prefix.is_empty() {
                    names.extend(
                        self.functions
                            .iter()
                            .filter(|(_, f)| f.definition.view)
                            .map(|(name, _)| name.clone()),
                    );
                }
                names.sort();
                names.dedup();
                let mut legs = Vec::new();
                for name in names
                    .into_iter()
                    .filter(|name| pattern_matches(pattern, name))
                {
                    if legs.len() >= 1024 {
                        return Err(Error::Plan(
                            "table wildcard matched more than 1024 sources".into(),
                        ));
                    }
                    if prefix.is_empty()
                        && self.functions.get(&name).is_some_and(|f| f.definition.view)
                    {
                        let plan = self.invoke_function(&name, &[], None, *span).await?;
                        legs.push((name, plan));
                        continue;
                    }
                    let provider = schema.table(&name).await?.ok_or_else(|| {
                        Error::Plan(format!("table '{name}' was removed during planning"))
                    })?;
                    let mut path = prefix.clone();
                    path.push(name);
                    let reference = table_reference(&path, *span)?;
                    let label = reference.to_string();
                    let plan =
                        LogicalPlanBuilder::scan(reference, provider_as_source(provider), None)?
                            .build()?;
                    legs.push((label, self.normalize_strings(plan)?));
                }
                Ok(legs)
            } else {
                Ok(vec![(
                    crate::tabular::source_name(source),
                    self.source(source).await?,
                )])
            }
        })
    }

    fn source<'a>(&'a mut self, source: &'a Source) -> PlanFuture<'a> {
        Box::pin(async move {
            match source {
                Source::Pattern { .. } => {
                    let legs = self.legs(source).await?;
                    let (labels, plans): (Vec<_>, Vec<_>) = legs.into_iter().unzip();
                    self.union(plans, UnionKind::Outer, None, &labels)
                }
                Source::Table { name, span } => {
                    if name.len() == 1 && self.functions.contains_key(&name[0]) {
                        self.invoke_function(&name[0], &[], None, *span).await
                    } else {
                        self.table(name, *span).await
                    }
                }
                Source::Call { name, args, span } => {
                    if name == "materialize" && !self.functions.contains_key(name) {
                        self.materialize(args, *span).await
                    } else {
                        self.invoke_function(name, args, None, *span).await
                    }
                }
                Source::Subquery(query) => self.query(query).await,
                Source::Print(items) => {
                    let mut plan = LogicalPlanBuilder::empty(true).build()?;
                    let items = if self.functions.is_empty() {
                        items.clone()
                    } else {
                        self.expander(plan.schema()).named(items)?
                    };
                    let mut items = self.resolve_named(&items).await?;
                    for item in &mut items {
                        let (input, expr) = self.membership(plan, &item.expr).await?;
                        plan = input;
                        item.expr = expr;
                    }
                    let expressions = self.named_expressions(&items, plan.schema())?;
                    Ok(LogicalPlanBuilder::from(plan)
                        .project(expressions)?
                        .build()?)
                }
                Source::Datatable { columns, values } => {
                    let mut prepared = Vec::new();
                    for value in values {
                        let value = self.expand_expression(value, &DFSchema::empty())?;
                        prepared.push(self.resolve_expression(&value).await?);
                    }
                    self.datatable(columns, &prepared)
                }
                Source::Range {
                    name,
                    start,
                    end,
                    step,
                } => {
                    let start = self.expand_expression(start, &DFSchema::empty())?;
                    let start = self.resolve_expression(&start).await?;
                    let end = self.expand_expression(end, &DFSchema::empty())?;
                    let end = self.resolve_expression(&end).await?;
                    let step = self.expand_expression(step, &DFSchema::empty())?;
                    let step = self.resolve_expression(&step).await?;
                    crate::ranges::plan(&self.expressions(), name, &start, &end, &step)
                }
                Source::Union {
                    kind,
                    withsource,
                    inputs,
                } => {
                    let mut legs = Vec::new();
                    for input in inputs {
                        legs.extend(self.legs(input).await?);
                    }
                    let (labels, plans): (Vec<_>, Vec<_>) = legs.into_iter().unzip();
                    self.union(plans, *kind, withsource.as_deref(), &labels)
                }
                Source::Search {
                    inputs,
                    predicate,
                    case_sensitive,
                    find,
                } => {
                    let mut legs = Vec::new();
                    for input in inputs {
                        legs.extend(self.legs(input).await?);
                    }
                    let mut prepared = Vec::new();
                    for (label, plan) in legs {
                        let expr = self.expand_expression(predicate, plan.schema())?;
                        let expr = self.resolve_expression(&expr).await?;
                        let (plan, expr) = self.membership(plan, &expr).await?;
                        prepared.push((label, plan, expr));
                    }
                    crate::search::plan(
                        &self.expressions(),
                        prepared,
                        predicate,
                        *case_sensitive,
                        find.as_ref(),
                    )
                }
            }
        })
    }

    fn resolve_expression<'a>(&'a mut self, expr: &'a ast::Expr) -> ExpressionFuture<'a> {
        Box::pin(async move {
            if !expr.any(|kind| {
                matches!(
                    kind,
                    ExprKind::ToScalar(_)
                        | ExprKind::DeferredCall { .. }
                        | ExprKind::InTabular { .. }
                        | ExprKind::InList { .. }
                )
            }) {
                return Ok(expr.clone());
            }
            let membership = match &expr.kind {
                ExprKind::InTabular {
                    expr,
                    query,
                    negated,
                    case_insensitive,
                } => Some((
                    expr.as_ref().clone(),
                    query.as_ref().clone(),
                    *negated,
                    *case_insensitive,
                )),
                ExprKind::InList {
                    expr,
                    values,
                    negated,
                    case_insensitive,
                } if values.len() == 1 => {
                    let source = match &values[0].kind {
                        ExprKind::Identifier(name) if !self.scalars.contains_key(&name[0]) => {
                            let reference = table_reference(name, values[0].span)?;
                            let exists = self.tables.contains_key(&name[0])
                                || self.functions.get(&name[0]).is_some_and(|f| f.tabular())
                                || self
                                    .state
                                    .schema_for_ref(reference.clone())?
                                    .table_exist(reference.table());
                            exists.then(|| Source::Table {
                                name: name.clone(),
                                span: values[0].span,
                            })
                        }
                        ExprKind::Call { name, args }
                            if self.functions.get(name).is_some_and(|f| f.tabular()) =>
                        {
                            Some(Source::Call {
                                name: name.clone(),
                                args: scalar_arguments(args),
                                span: values[0].span,
                            })
                        }
                        _ => None,
                    };
                    source.map(|source| {
                        (
                            expr.as_ref().clone(),
                            ast::Query {
                                bindings: Vec::new(),
                                pipeline: ast::Pipeline {
                                    source,
                                    operators: Vec::new(),
                                },
                            },
                            *negated,
                            *case_insensitive,
                        )
                    })
                }
                _ => None,
            };
            if let Some((value, query, negated, case_insensitive)) = membership {
                let value = self.resolve_expression(&value).await?;
                let input = ordering::visible(self.query(&query).await?)?;
                if input.schema().fields().is_empty() {
                    return Err(Error::syntax(
                        "tabular membership requires a result column",
                        expr.span,
                    ));
                }
                let column = field_expr(&input, 0);
                let column = if case_insensitive {
                    self.expressions().ascii_lower(column)?
                } else {
                    column
                };
                let input = LogicalPlanBuilder::from(input)
                    .project(vec![column.alias("value")])?
                    .build()?;
                let name = format!("__kql_membership_query_{}", self.next_alias);
                self.next_alias += 1;
                self.scalars.insert(
                    name.clone(),
                    Expr::ScalarSubquery(datafusion::logical_expr::Subquery {
                        subquery: Arc::new(input),
                        outer_ref_columns: Vec::new(),
                        spans: datafusion::common::Spans::new(),
                    }),
                );
                return Ok(ast::Expr {
                    kind: ExprKind::InPrepared {
                        expr: Box::new(value),
                        binding: name,
                        negated,
                        case_insensitive,
                    },
                    span: expr.span,
                    depth: expr.depth,
                });
            }
            if let ExprKind::DeferredCall { name, args } = &expr.kind {
                let function = self.deferred.get(name).cloned().ok_or_else(|| {
                    Error::syntax("scalar function closure is no longer in scope", expr.span)
                })?;
                let mut prepared = Vec::new();
                for arg in args {
                    prepared.push(self.resolve_expression(arg).await?);
                }
                let names = prepared
                    .iter()
                    .map(|arg| match &arg.kind {
                        ExprKind::NamedArgument { name, .. } => Some(name.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                let slots = crate::functions::arguments(
                    &function.definition.parameters,
                    &names,
                    expr.span,
                )?;
                let mut child = Self::new(self.state, self.track_order, &self.parameters)?;
                child.now = self.now;
                child.scalars = function.scalars.clone();
                child.tables = function.tables.clone();
                child.functions = function.functions.clone();
                for (parameter, slot) in function.definition.parameters.iter().zip(slots) {
                    let ast::ParameterType::Scalar(ty) = parameter.ty else {
                        return Err(Error::syntax(
                            "a scalar closure requires scalar arguments",
                            expr.span,
                        ));
                    };
                    let value = if let Some(index) = slot {
                        let argument = match &prepared[index].kind {
                            ExprKind::NamedArgument { value, .. } => value.as_ref(),
                            _ => &prepared[index],
                        };
                        self.expressions().scalar(argument, &DFSchema::empty())?
                    } else {
                        child.expressions().scalar(
                            parameter.default.as_ref().ok_or_else(|| {
                                Error::syntax("missing function argument", expr.span)
                            })?,
                            &DFSchema::empty(),
                        )?
                    };
                    let value = crate::expressions::parameter_cast(
                        value,
                        ty,
                        &DFSchema::empty(),
                        expr.span,
                    )?;
                    child.tables.remove(&parameter.name);
                    child.functions.remove(&parameter.name);
                    child.scalars.insert(parameter.name.clone(), value);
                }
                let ast::FunctionBody::Scalar(value) = &function.definition.body else {
                    return Err(Error::syntax("expected a scalar function body", expr.span));
                };
                let query = ast::Query {
                    bindings: function.definition.bindings.clone(),
                    pipeline: ast::Pipeline {
                        source: Source::Print(vec![NamedExpr {
                            name: Some("value".into()),
                            expr: value.clone(),
                        }]),
                        operators: Vec::new(),
                    },
                };
                let input = child.query(&query).await?;
                let name = self.temporary_name(&input);
                let input = crate::materialize::plan(input, name)?;
                let value = Expr::ScalarSubquery(datafusion::logical_expr::Subquery {
                    subquery: Arc::new(input),
                    outer_ref_columns: Vec::new(),
                    spans: datafusion::common::Spans::new(),
                });
                let name = format!("__kql_scalar_function_{}", self.next_alias);
                self.next_alias += 1;
                self.scalars.insert(name.clone(), value);
                return Ok(ast::Expr {
                    kind: ExprKind::BindingReference(name),
                    span: expr.span,
                    depth: 1,
                });
            }
            if let ExprKind::ToScalar(argument) = &expr.kind {
                let input = match argument.as_ref() {
                    ast::ArgumentValue::Tabular(query) => self.query(query).await?,
                    ast::ArgumentValue::Scalar(value) => {
                        let table = match &value.kind {
                            ExprKind::Identifier(name) if !self.scalars.contains_key(&name[0]) => {
                                self.tables.contains_key(&name[0])
                                    || self.functions.get(&name[0]).is_some_and(|f| f.tabular())
                                    || self
                                        .state
                                        .schema_for_ref(table_reference(name, value.span)?)?
                                        .table_exist(name.last().expect("nonempty identifier"))
                            }
                            ExprKind::Call { name, .. } => {
                                self.functions.get(name).is_some_and(|f| f.tabular())
                            }
                            _ => false,
                        };
                        if table {
                            let source = match &value.kind {
                                ExprKind::Identifier(name) => Source::Table {
                                    name: name.clone(),
                                    span: value.span,
                                },
                                ExprKind::Call { name, args } => Source::Call {
                                    name: name.clone(),
                                    args: scalar_arguments(args),
                                    span: value.span,
                                },
                                _ => unreachable!(),
                            };
                            self.source(&source).await?
                        } else {
                            let expanded = self.expand_expression(value, &DFSchema::empty())?;
                            let value = self.resolve_expression(&expanded).await?;
                            let value = self.expressions().scalar(&value, &DFSchema::empty())?;
                            LogicalPlanBuilder::empty(true)
                                .project(vec![value.alias("value")])?
                                .build()?
                        }
                    }
                };
                let input = ordering::finalize(input)?;
                if input.schema().fields().is_empty() {
                    return Err(Error::syntax(
                        "toscalar requires at least one result column",
                        expr.span,
                    ));
                }
                let column = field_expr(&input, 0);
                let input = LogicalPlanBuilder::from(input)
                    .limit(0, Some(1))?
                    .project(vec![column.alias("value")])?
                    .build()?;
                let first = self
                    .state
                    .udaf("first_value")?
                    .call(vec![field_expr(&input, 0)])
                    .alias("value");
                let input = LogicalPlanBuilder::from(input)
                    .aggregate(Vec::<Expr>::new(), vec![first])?
                    .build()?;
                let name = self.temporary_name(&input);
                let input = crate::materialize::plan(input, name)?;
                let mut value = Expr::ScalarSubquery(datafusion::logical_expr::Subquery {
                    subquery: Arc::new(input),
                    outer_ref_columns: Vec::new(),
                    spans: datafusion::common::Spans::new(),
                });
                if is_string(&value.get_type(&DFSchema::empty())?) {
                    value = self
                        .expressions()
                        .function("coalesce", vec![value, lit("")])?;
                }
                let name = format!("__kql_scalar_subquery_{}", self.next_alias);
                self.next_alias += 1;
                self.scalars.insert(name.clone(), value);
                return Ok(ast::Expr {
                    kind: ExprKind::BindingReference(name),
                    span: expr.span,
                    depth: 1,
                });
            }
            let mut children = Vec::new();
            expr.map_children(|child| {
                children.push(child.clone());
                Ok(child.clone())
            })?;
            let mut prepared = Vec::new();
            for child in &children {
                prepared.push(self.resolve_expression(child).await?);
            }
            let mut prepared = prepared.into_iter();
            expr.map_children(|_| Ok(prepared.next().expect("one prepared expression per child")))
        })
    }

    fn membership<'a>(
        &'a mut self,
        mut plan: LogicalPlan,
        expr: &'a ast::Expr,
    ) -> MembershipFuture<'a> {
        Box::pin(async move {
            if !expr.any(|kind| matches!(kind, ExprKind::InPrepared { .. })) {
                return Ok((plan, expr.clone()));
            }
            let mut children = Vec::new();
            expr.map_children(|child| {
                children.push(child.clone());
                Ok(child.clone())
            })?;
            let mut prepared = Vec::new();
            for child in &children {
                let (input, value) = self.membership(plan, child).await?;
                plan = input;
                prepared.push(value);
            }
            let mut prepared = prepared.into_iter();
            let expr = expr.map_children(|_| {
                Ok(prepared
                    .next()
                    .expect("one membership expression per child"))
            })?;
            let ExprKind::InPrepared {
                expr: value,
                binding,
                negated,
                case_insensitive,
            } = &expr.kind
            else {
                return Ok((plan, expr));
            };
            let Some(Expr::ScalarSubquery(subquery)) = self.scalars.get(binding) else {
                return Err(Error::Plan(
                    "tabular membership query is no longer in scope".into(),
                ));
            };
            let mut right = subquery.subquery.as_ref().clone();
            let value = self.expressions().scalar(value, plan.schema())?;
            if dynamic::is_dynamic(&value.get_type(plan.schema())?)
                || dynamic::is_dynamic(right.schema().field(0).data_type())
            {
                return Err(Error::unsupported(
                    "cast dynamic tabular membership operands to scalar types",
                    expr.span,
                ));
            }
            let value = if *case_insensitive {
                self.expressions().ascii_lower(value)?
            } else {
                value
            };
            let nullable = value.nullable(plan.schema())?;
            let has_rows = if nullable {
                let name = self.temporary_name(&right);
                right = crate::materialize::plan(right, name)?;
                let limited = LogicalPlanBuilder::from(right.clone())
                    .limit(0, Some(1))?
                    .build()?;
                let count = self
                    .state
                    .udaf("count")?
                    .call(vec![lit(1_i64)])
                    .alias("count");
                let count = LogicalPlanBuilder::from(limited)
                    .aggregate(Vec::<Expr>::new(), vec![count])?
                    .build()?;
                Some(
                    Expr::ScalarSubquery(datafusion::logical_expr::Subquery {
                        subquery: Arc::new(count),
                        outer_ref_columns: Vec::new(),
                        spans: datafusion::common::Spans::new(),
                    })
                    .gt(lit(0_i64)),
                )
            } else {
                None
            };
            let alias = self.temporary_name(&right);
            let right = LogicalPlanBuilder::from(right).alias(alias)?.build()?;
            let key = field_expr(&right, 0);
            let joined = LogicalPlanBuilder::from(plan)
                .join_on(right, JoinType::LeftMark, vec![value.clone().eq(key)])?
                .build()?;
            let mark = field_expr(&joined, joined.schema().fields().len() - 1);
            let result = if let Some(has_rows) = has_rows {
                datafusion::logical_expr::expr_fn::when(
                    value.is_null().and(has_rows),
                    lit(ScalarValue::Boolean(None)),
                )
                .otherwise(mark)?
            } else {
                mark
            };
            let result = if *negated { !result } else { result };
            let name = self.temporary_name(&joined);
            let mut projection = (0..joined.schema().fields().len() - 1)
                .map(|i| field_expr(&joined, i))
                .collect::<Vec<_>>();
            projection.push(ordering::helper(result, name.clone(), false));
            plan = LogicalPlanBuilder::from(joined)
                .project(projection)?
                .build()?;
            let binding = format!("{name}_binding");
            self.scalars
                .insert(binding.clone(), Expr::Column(Column::from_name(name)));
            Ok((
                plan,
                ast::Expr {
                    kind: ExprKind::BindingReference(binding),
                    span: expr.span,
                    depth: 1,
                },
            ))
        })
    }

    async fn membership_operator(
        &mut self,
        mut plan: LogicalPlan,
        operator: &Operator,
    ) -> Result<(LogicalPlan, Operator)> {
        if !operator
            .expression_refs()
            .iter()
            .any(|expr| expr.any(|kind| matches!(kind, ExprKind::InPrepared { .. })))
        {
            return Ok((plan, operator.clone()));
        }
        let mut values = Vec::new();
        operator.map_expressions(|value| {
            values.push(value.clone());
            Ok(value.clone())
        })?;
        let mut prepared = Vec::new();
        for value in &values {
            let (input, value) = self.membership(plan, value).await?;
            plan = input;
            prepared.push(value);
        }
        let mut prepared = prepared.into_iter();
        let operator = operator.map_expressions(|_| {
            Ok(prepared
                .next()
                .expect("one membership expression per operator expression"))
        })?;
        Ok((plan, operator))
    }

    async fn resolve_named(&mut self, items: &[NamedExpr]) -> Result<Vec<NamedExpr>> {
        let mut result = Vec::new();
        for item in items {
            result.push(NamedExpr {
                name: item.name.clone(),
                expr: self.resolve_expression(&item.expr).await?,
            });
        }
        Ok(result)
    }

    async fn resolve_operator(&mut self, operator: &Operator) -> Result<Operator> {
        if !operator.expression_refs().iter().any(|expr| {
            expr.any(|kind| {
                matches!(
                    kind,
                    ExprKind::ToScalar(_)
                        | ExprKind::DeferredCall { .. }
                        | ExprKind::InTabular { .. }
                        | ExprKind::InList { .. }
                )
            })
        }) {
            return Ok(operator.clone());
        }
        let mut values = Vec::new();
        operator.map_expressions(|expr| {
            values.push(expr.clone());
            Ok(expr.clone())
        })?;
        let mut prepared = Vec::new();
        for value in &values {
            prepared.push(self.resolve_expression(value).await?);
        }
        let mut prepared = prepared.into_iter();
        operator.map_expressions(|_| {
            Ok(prepared
                .next()
                .expect("one prepared expression per operator expression"))
        })
    }

    async fn materialize(&mut self, args: &[ast::CallArgument], span: Span) -> Result<LogicalPlan> {
        let [argument] = args else {
            return Err(Error::syntax(
                "materialize expects one tabular argument",
                span,
            ));
        };
        if argument.name.is_some() {
            return Err(Error::syntax(
                "materialize does not accept named arguments",
                span,
            ));
        }
        let input = match &argument.value {
            ast::ArgumentValue::Tabular(query) => self.query(query).await?,
            ast::ArgumentValue::Scalar(ast::Expr {
                kind: ExprKind::Identifier(name),
                span,
                ..
            }) => {
                self.source(&Source::Table {
                    name: name.clone(),
                    span: *span,
                })
                .await?
            }
            ast::ArgumentValue::Scalar(ast::Expr {
                kind: ExprKind::Call { name, args },
                span,
                ..
            }) => {
                self.source(&Source::Call {
                    name: name.clone(),
                    args: scalar_arguments(args),
                    span: *span,
                })
                .await?
            }
            _ => {
                return Err(Error::syntax(
                    "materialize requires a tabular expression",
                    span,
                ));
            }
        };
        let name = self.temporary_name(&input);
        crate::materialize::plan(input, name)
    }

    async fn invoke_function(
        &mut self,
        name: &str,
        args: &[ast::CallArgument],
        input: Option<LogicalPlan>,
        span: Span,
    ) -> Result<LogicalPlan> {
        let function =
            self.functions.get(name).cloned().ok_or_else(|| {
                Error::syntax(format!("unknown KQL tabular function '{name}'"), span)
            })?;
        if !function.tabular() {
            return Err(Error::syntax(
                "a scalar function cannot be used as a table",
                span,
            ));
        }
        let mut names = Vec::new();
        if input.is_some() {
            names.push(None);
        }
        names.extend(args.iter().map(|arg| arg.name.as_deref()));
        let slots = crate::functions::arguments(&function.definition.parameters, &names, span)?;
        let offset = usize::from(input.is_some());
        let mut child = Self::new(self.state, self.track_order, &self.parameters)?;
        child.now = self.now;
        child.scalars = function.scalars.clone();
        child.tables = function.tables.clone();
        child.functions = function.functions.clone();
        for (parameter, slot) in function.definition.parameters.iter().zip(slots) {
            let argument = slot.and_then(|index| index.checked_sub(offset).map(|i| &args[i].value));
            match &parameter.ty {
                ast::ParameterType::Scalar(ty) => {
                    if slot == Some(0) && input.is_some() {
                        return Err(Error::syntax(
                            "invoke requires a tabular first parameter",
                            span,
                        ));
                    }
                    let value = match argument {
                        Some(ast::ArgumentValue::Scalar(expr)) => {
                            let expr = self.expand_expression(expr, &DFSchema::empty())?;
                            self.expressions().scalar(&expr, &DFSchema::empty())?
                        }
                        Some(ast::ArgumentValue::Tabular(_)) => {
                            return Err(Error::syntax("expected a scalar argument", span));
                        }
                        None => {
                            let default = parameter
                                .default
                                .as_ref()
                                .ok_or_else(|| Error::syntax("missing scalar argument", span))?;
                            child.expressions().scalar(default, &DFSchema::empty())?
                        }
                    };
                    let value =
                        crate::expressions::parameter_cast(value, *ty, &DFSchema::empty(), span)?;
                    let value = lit(self.expressions().constant(value, span)?);
                    child.tables.remove(&parameter.name);
                    child.functions.remove(&parameter.name);
                    child.scalars.insert(parameter.name.clone(), value);
                }
                ast::ParameterType::Tabular(columns) => {
                    let plan = if slot == Some(0) && input.is_some() {
                        input
                            .clone()
                            .ok_or_else(|| Error::syntax("missing invoke input", span))?
                    } else {
                        match argument {
                            Some(ast::ArgumentValue::Tabular(query)) => self.query(query).await?,
                            Some(ast::ArgumentValue::Scalar(ast::Expr {
                                kind: ExprKind::Identifier(parts),
                                span,
                                ..
                            })) => {
                                self.source(&Source::Table {
                                    name: parts.clone(),
                                    span: *span,
                                })
                                .await?
                            }
                            _ => return Err(Error::syntax("expected a tabular argument", span)),
                        }
                    };
                    let plan = if let Some(columns) = columns {
                        let mut projected = Vec::new();
                        for (name, ty) in columns {
                            let index = plan
                                .schema()
                                .index_of_column_by_name(None, name)
                                .ok_or_else(|| {
                                    Error::syntax(
                                        format!("tabular argument is missing column '{name}'"),
                                        span,
                                    )
                                })?;
                            projected.push(
                                crate::expressions::parameter_cast(
                                    field_expr(&plan, index),
                                    *ty,
                                    plan.schema(),
                                    span,
                                )?
                                .alias(name),
                            );
                        }
                        if let Some(ordinal) = ordering::ordinal(plan.schema()) {
                            projected.push(ordinal);
                        }
                        LogicalPlanBuilder::from(plan).project(projected)?.build()?
                    } else {
                        plan
                    };
                    child.scalars.remove(&parameter.name);
                    child.functions.remove(&parameter.name);
                    child.tables.insert(parameter.name.clone(), plan);
                }
            }
        }
        let pipeline = match &function.definition.body {
            ast::FunctionBody::Tabular(pipeline) => pipeline.clone(),
            ast::FunctionBody::Scalar(ast::Expr {
                kind: ExprKind::Identifier(parts),
                span,
                ..
            }) => ast::Pipeline {
                source: Source::Table {
                    name: parts.clone(),
                    span: *span,
                },
                operators: Vec::new(),
            },
            ast::FunctionBody::Scalar(ast::Expr {
                kind: ExprKind::Call { name, args },
                span,
                ..
            }) => ast::Pipeline {
                source: Source::Call {
                    name: name.clone(),
                    args: scalar_arguments(args),
                    span: *span,
                },
                operators: Vec::new(),
            },
            _ => return Err(Error::syntax("function does not return a table", span)),
        };
        child
            .query(&ast::Query {
                bindings: function.definition.bindings.clone(),
                pipeline,
            })
            .await
    }

    async fn pipeline(&mut self, pipeline: &ast::Pipeline) -> Result<LogicalPlan> {
        let mut plan = self.source(&pipeline.source).await?;
        check_plan(&plan)?;
        for original in &pipeline.operators {
            let expanded = if self.functions.is_empty() {
                None
            } else {
                Some(ast::SpannedOperator {
                    kind: self.expander(plan.schema()).operator(&original.kind)?,
                    span: original.span,
                })
            };
            let original = expanded.as_ref().unwrap_or(original);
            let prepared = ast::SpannedOperator {
                kind: self.resolve_operator(&original.kind).await?,
                span: original.span,
            };
            let original = &prepared;
            let (input, kind) = self.membership_operator(plan, &original.kind).await?;
            plan = input;
            let membership_prepared = ast::SpannedOperator {
                kind,
                span: original.span,
            };
            let original = &membership_prepared;
            if matches!(original.kind, Operator::Serialize(_))
                && ordering::ordinal(plan.schema()).is_none()
            {
                plan = ordering::sequence(self.state, &self.scalars, plan, Vec::new())?;
            }
            let (rewritten, saved) = if self.track_order && ordering::uses_windows(&original.kind) {
                let (updated, kind, bindings) = crate::windows::prepare(
                    self.state,
                    &self.scalars,
                    self.now,
                    plan,
                    &original.kind,
                )?;
                plan = updated;
                let saved = std::mem::replace(&mut self.scalars, bindings);
                (
                    Some(ast::SpannedOperator {
                        kind,
                        span: original.span,
                    }),
                    Some(saved),
                )
            } else {
                (None, None)
            };
            let operator = rewritten.as_ref().unwrap_or(original);
            plan = match &operator.kind {
                Operator::Where(predicate) => {
                    let expression = self.expressions().predicate(predicate, plan.schema())?;
                    LogicalPlanBuilder::from(plan).filter(expression)?.build()?
                }
                Operator::Search {
                    predicate,
                    case_sensitive,
                } => {
                    if plan
                        .schema()
                        .index_of_column_by_name(None, "$table")
                        .is_some()
                    {
                        let expression = crate::search::filter(
                            &self.expressions(),
                            &plan,
                            predicate,
                            *case_sensitive,
                            &HashMap::new(),
                        )?;
                        crate::expressions::check_expression(&expression)?;
                        LogicalPlanBuilder::from(ordering::visible(plan)?)
                            .filter(expression)?
                            .build()?
                    } else {
                        let label = crate::tabular::source_name(&pipeline.source);
                        crate::search::plan(
                            &self.expressions(),
                            vec![(label, plan, predicate.clone())],
                            predicate,
                            *case_sensitive,
                            None,
                        )?
                    }
                }
                Operator::TopHitters {
                    count,
                    value,
                    weight,
                } => crate::top::hitters(&self.expressions(), plan, count, value, weight.as_ref())?,
                Operator::Evaluate { name, args, schema } => {
                    let temporary = self.temporary_name(&plan);
                    crate::evaluate::plan(
                        &self.expressions(),
                        plan,
                        name,
                        args,
                        schema.as_ref(),
                        temporary,
                        operator.span,
                    )?
                }
                Operator::TopNested(levels) => {
                    let name = self.temporary_name(&plan);
                    crate::top::nested(&self.expressions(), plan, levels, name)?
                }
                Operator::Invoke { name, args } => {
                    self.invoke_function(name, args, Some(plan), operator.span)
                        .await?
                }
                Operator::MvApply {
                    index,
                    columns,
                    limit,
                    operators,
                } => {
                    if index.is_none()
                        && let Some(native) =
                            self.fast_apply(plan.clone(), columns, *limit, operators)?
                    {
                        native
                    } else {
                        self.apply_grouped(
                            plan,
                            None,
                            Some((index.as_deref(), columns, *limit)),
                            operators,
                        )
                        .await?
                    }
                }
                Operator::Scan(scan) => {
                    let name = self.temporary_name(&plan);
                    crate::scan::plan(self.state, &self.scalars, self.now, plan, scan, name)?
                }
                Operator::Partition {
                    key,
                    strategy,
                    operators,
                } => {
                    let index = plan
                        .schema()
                        .index_of_column_by_name(None, key)
                        .ok_or_else(|| {
                            Error::syntax(format!("unknown partition key '{key}'"), operator.span)
                        })?;
                    let ty = plan.schema().field(index).data_type();
                    if dynamic::is_dynamic(ty) || ty.is_nested() {
                        return Err(Error::unsupported(
                            "partition keys must be scalar, not dynamic or nested",
                            operator.span,
                        ));
                    }
                    if *strategy != ast::PartitionStrategy::Legacy
                        && operators.iter().any(|operator| {
                            matches!(
                                operator.kind,
                                Operator::Join { .. }
                                    | Operator::Lookup { .. }
                                    | Operator::Union { .. }
                                    | Operator::As(_)
                                    | Operator::AsMaterialized(_)
                                    | Operator::Invoke { .. }
                                    | Operator::GetSchema
                            )
                        })
                    {
                        return Err(Error::unsupported(
                            "native/shuffle partition subqueries require implicit-source transformations",
                            operator.span,
                        ));
                    }
                    if *strategy != ast::PartitionStrategy::Legacy
                        && let Some(native) =
                            self.native_partition(plan.clone(), key, operators).await?
                    {
                        native
                    } else {
                        self.apply_grouped(plan, Some((key, *strategy)), None, operators)
                            .await?
                    }
                }
                Operator::Take(count) => {
                    let count = self.row_limit(count)?;
                    LogicalPlanBuilder::from(plan)
                        .limit(0, Some(count))?
                        .build()?
                }

                Operator::Project(items) => {
                    let mut expressions = self.named_expressions(items, plan.schema())?;
                    if let Some(order) = ordering::ordinal(plan.schema()) {
                        expressions.push(order);
                    }
                    LogicalPlanBuilder::from(plan)
                        .project(expressions)?
                        .build()?
                }
                Operator::Extend(items) | Operator::Serialize(items) => {
                    let added = self.named_expressions(items, plan.schema())?;
                    let names = added
                        .iter()
                        .map(Expr::name_for_alias)
                        .collect::<datafusion::error::Result<HashSet<_>>>()?;
                    let mut expressions = plan
                        .schema()
                        .fields()
                        .iter()
                        .enumerate()
                        .filter(|(_, f)| !names.contains(f.name()))
                        .map(|(i, _)| field_expr(&plan, i))
                        .collect::<Vec<_>>();
                    expressions.extend(added);
                    LogicalPlanBuilder::from(plan)
                        .project(expressions)?
                        .build()?
                }
                Operator::ProjectAway(patterns) | Operator::ProjectKeep(patterns) => {
                    let keep = matches!(operator.kind, Operator::ProjectKeep(_));
                    let expressions = plan
                        .schema()
                        .fields()
                        .iter()
                        .enumerate()
                        .filter(|(_, f)| {
                            if ordering::hidden(f) {
                                return true;
                            }
                            let matches = patterns.iter().any(|p| pattern_matches(p, f.name()));
                            matches == keep
                        })
                        .map(|(i, _)| field_expr(&plan, i))
                        .collect::<Vec<_>>();
                    LogicalPlanBuilder::from(plan)
                        .project(expressions)?
                        .build()?
                }
                Operator::ProjectRename(renames) => {
                    let mut old_names = HashSet::new();
                    for (_, old) in renames {
                        if !old_names.insert(old) {
                            return Err(Error::syntax(
                                "column renamed more than once",
                                operator.span,
                            ));
                        }
                        plan.schema()
                            .index_of_column_by_name(None, old)
                            .ok_or_else(|| Error::Plan(format!("unknown column '{old}'")))?;
                    }
                    let expressions = plan
                        .schema()
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(i, field)| {
                            let column = field_expr(&plan, i);
                            if let Some((new, _)) =
                                renames.iter().find(|(_, old)| old == field.name())
                            {
                                column.alias(new)
                            } else {
                                column
                            }
                        })
                        .collect::<Vec<_>>();
                    LogicalPlanBuilder::from(plan)
                        .project(expressions)?
                        .build()?
                }
                Operator::ProjectReorder(columns) => crate::tabular::reorder(plan, columns)?,
                Operator::GetSchema => {
                    let result = crate::tabular::schema(&plan)?;
                    if self.track_order {
                        ordering::sequence(
                            self.state,
                            &self.scalars,
                            result,
                            vec![
                                Expr::Column(Column::from_name("ColumnOrdinal")).sort(true, false),
                            ],
                        )?
                    } else {
                        result
                    }
                }
                Operator::As(name) | Operator::AsMaterialized(name) => {
                    if self.tables.contains_key(name)
                        || self.scalars.contains_key(name)
                        || self.functions.contains_key(name)
                    {
                        return Err(Error::Plan(format!(
                            "query binding '{name}' already exists"
                        )));
                    }
                    let plan = if matches!(operator.kind, Operator::AsMaterialized(_)) {
                        let alias = self.temporary_name(&plan);
                        crate::materialize::plan(plan, alias)?
                    } else {
                        plan
                    };
                    self.tables.insert(name.clone(), plan.clone());
                    plan
                }
                Operator::Sample { count, distinct } => {
                    let count = self.row_limit(count)?;
                    let mut input = ordering::visible(plan)?;
                    if let Some(value) = distinct {
                        let projected = self.named_expressions(
                            &[NamedExpr {
                                name: None,
                                expr: value.clone(),
                            }],
                            input.schema(),
                        )?;
                        input = LogicalPlanBuilder::from(input)
                            .project(projected)?
                            .distinct()?
                            .build()?;
                    }
                    let visible = (0..input.schema().fields().len())
                        .map(|i| field_expr(&input, i))
                        .collect::<Vec<_>>();
                    let random = self.temporary_name(&input);
                    let mut projected = visible.clone();
                    projected.push(
                        self.expressions()
                            .function("random", Vec::new())?
                            .alias(&random),
                    );
                    LogicalPlanBuilder::from(input)
                        .project(projected)?
                        .sort(vec![
                            Expr::Column(Column::from_name(random)).sort(true, false),
                        ])?
                        .limit(0, Some(count))?
                        .project(visible)?
                        .build()?
                }
                Operator::MvExpand {
                    array_mode,
                    arrays_only,
                    index,
                    columns,
                    limit,
                } => self.mv_expand(
                    plan,
                    (*array_mode, *arrays_only),
                    index.as_deref(),
                    columns,
                    *limit,
                )?,
                Operator::Parse { .. } => {
                    let temporary = self.temporary_name(&plan);
                    crate::parsing::plan(&self.expressions(), plan, operator, temporary)?
                }
                Operator::Summarize { aggregates, by } => self.summarize(plan, aggregates, by)?,
                Operator::MakeSeries(series) => crate::make_series::plan(
                    self.state,
                    &self.scalars,
                    self.now,
                    plan,
                    series,
                    operator.span,
                )?,
                Operator::Sort(keys) => {
                    let sorts = keys
                        .iter()
                        .map(|key| {
                            Ok(self
                                .expressions()
                                .scalar(&key.expr, plan.schema())?
                                .sort(key.ascending, key.nulls_first))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let sorted = LogicalPlanBuilder::from(plan)
                        .sort(sorts.clone())?
                        .build()?;
                    if self.track_order {
                        ordering::sequence(self.state, &self.scalars, sorted, sorts)?
                    } else {
                        sorted
                    }
                }
                Operator::Top { count, key } => {
                    let count = self.row_limit(count)?;
                    let sort = self
                        .expressions()
                        .scalar(&key.expr, plan.schema())?
                        .sort(key.ascending, key.nulls_first);
                    let sorted = LogicalPlanBuilder::from(plan)
                        .sort(vec![sort.clone()])?
                        .limit(0, Some(count))?
                        .build()?;
                    if self.track_order {
                        ordering::sequence(self.state, &self.scalars, sorted, vec![sort])?
                    } else {
                        sorted
                    }
                }
                Operator::Distinct(items) => {
                    let named = items
                        .iter()
                        .cloned()
                        .map(|expr| NamedExpr { name: None, expr })
                        .collect::<Vec<_>>();
                    let expressions = self.named_expressions(&named, plan.schema())?;
                    LogicalPlanBuilder::from(plan)
                        .project(expressions)?
                        .distinct()?
                        .build()?
                }
                Operator::Count => {
                    let count = self
                        .state
                        .udaf("count")?
                        .call(vec![lit(1_i64)])
                        .alias("Count");
                    LogicalPlanBuilder::from(plan)
                        .aggregate(Vec::<Expr>::new(), vec![count])?
                        .build()?
                }
                Operator::Join { kind, input, keys } => {
                    let right = self.query(input).await?;
                    self.join(plan, right, *kind, keys)?
                }
                Operator::Lookup { kind, input, keys } => {
                    let left = ordering::visible(plan)?;
                    let right = ordering::visible(self.query(input).await?)?;
                    let left_len = left.schema().fields().len();
                    let excluded = right
                        .schema()
                        .fields()
                        .iter()
                        .enumerate()
                        .filter(|(_, field)| keys.iter().any(|key| key.right == *field.name()))
                        .map(|(i, _)| left_len + i)
                        .collect::<HashSet<_>>();
                    let result = self.join(left, right, *kind, keys)?;
                    let projected = (0..result.schema().fields().len())
                        .filter(|i| !excluded.contains(i))
                        .map(|i| field_expr(&result, i))
                        .collect::<Vec<_>>();
                    LogicalPlanBuilder::from(result)
                        .project(projected)?
                        .build()?
                }
                Operator::Union {
                    kind,
                    withsource,
                    inputs,
                } => {
                    let mut plans = vec![plan];
                    let mut labels = vec![crate::tabular::source_name(&pipeline.source)];
                    for input in inputs {
                        for (label, plan) in self.legs(input).await? {
                            labels.push(label);
                            plans.push(plan);
                        }
                    }
                    self.union(plans, *kind, withsource.as_deref(), &labels)?
                }
            };
            if let Some(saved) = saved {
                self.scalars = saved;
            }
            plan = ordering::cleanup(plan)?;
            check_plan(&plan)?;
        }
        Ok(plan)
    }

    fn fast_apply(
        &mut self,
        input: LogicalPlan,
        columns: &[ast::ExpandColumn],
        expansion_limit: usize,
        operators: &[ast::SpannedOperator],
    ) -> Result<Option<LogicalPlan>> {
        use crate::dynamic::TopReduction;
        let [column] = columns else {
            return Ok(None);
        };
        let Some(ty @ (ast::ScalarType::Long | ast::ScalarType::Real)) = column.ty else {
            return Ok(None);
        };
        let [first, rest @ ..] = operators else {
            return Ok(None);
        };
        let Operator::Top { count, key } = &first.kind else {
            return Ok(None);
        };
        let target = output_name(&column.value, 0, &mut HashSet::new())?;
        if !matches!(&key.expr.kind,ExprKind::Identifier(parts) if parts.as_slice()==std::slice::from_ref(&target))
        {
            return Ok(None);
        }
        let aggregates = match rest {
            [] => None,
            [
                ast::SpannedOperator {
                    kind: Operator::Summarize { aggregates, by },
                    ..
                },
            ] if by.is_empty() => Some(aggregates),
            _ => return Ok(None),
        };
        let count = self.row_limit(count)?;
        let source = self
            .expressions()
            .scalar(&column.value.expr, input.schema())?;
        if !dynamic::is_dynamic(&source.get_type(input.schema())?) {
            return Ok(None);
        }
        let mut reductions = Vec::new();
        let mut names = HashSet::new();
        if let Some(aggregates) = aggregates {
            for (index, aggregate) in aggregates.iter().enumerate() {
                let ExprKind::Call { name, args } = &aggregate.expr.kind else {
                    return Ok(None);
                };
                let column_matches=args.first().is_some_and(|arg| matches!(&arg.kind,ExprKind::Identifier(parts) if parts.as_slice()==std::slice::from_ref(&target)));
                let reduction = match name.as_str() {
                    "sum" if args.len() == 1 && column_matches && ty == ast::ScalarType::Long => {
                        TopReduction::Sum
                    }
                    "count" if args.is_empty() || args.len() == 1 && column_matches => {
                        TopReduction::Count
                    }
                    "make_list" | "make_list_with_nulls"
                        if (1..=2).contains(&args.len()) && column_matches =>
                    {
                        let limit = if let Some(limit) = args.get(1) {
                            self.row_limit(limit)?
                        } else {
                            1_048_576
                        };
                        if limit == 0 || limit > 1_048_576 {
                            return Ok(None);
                        }
                        TopReduction::List {
                            limit,
                            include_nulls: name == "make_list_with_nulls",
                        }
                    }
                    _ => return Ok(None),
                };
                reductions.push((output_name(aggregate, index, &mut names)?, reduction));
            }
        }
        let temporary = self.temporary_name(&input);
        let mut projection = (0..input.schema().fields().len())
            .map(|i| field_expr(&input, i))
            .collect::<Vec<_>>();
        projection.push(ordering::helper(
            dynamic::call(
                Kernel::TopValues {
                    real: ty == ast::ScalarType::Real,
                    descending: !key.ascending,
                    nulls_first: key.nulls_first,
                    expanded_limit: expansion_limit,
                    count,
                },
                vec![source],
            ),
            temporary.clone(),
            false,
        ));
        let selected = LogicalPlanBuilder::from(input)
            .project(projection)?
            .build()?;
        if aggregates.is_none() {
            let value = NamedExpr {
                name: Some(target),
                expr: ast::Expr {
                    kind: ExprKind::BindingReference(temporary.clone()),
                    span: column.value.expr.span,
                    depth: 1,
                },
            };
            self.scalars.insert(
                temporary.clone(),
                Expr::Column(Column::from_name(&temporary)),
            );
            return self
                .mv_expand(
                    selected,
                    (false, true),
                    None,
                    &[ast::ExpandColumn {
                        value,
                        ty: Some(ty),
                    }],
                    2_147_483_647,
                )
                .map(Some);
        }
        let mut projection = selected
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                field.name() != &target
                    && field.name() != &temporary
                    && !names.contains(field.name())
            })
            .map(|(i, _)| field_expr(&selected, i))
            .collect::<Vec<_>>();
        for (name, reduction) in reductions {
            projection.push(
                dynamic::call(
                    Kernel::TopReduce(reduction),
                    vec![Expr::Column(Column::from_name(&temporary))],
                )
                .alias(name),
            );
        }
        Ok(Some(
            LogicalPlanBuilder::from(selected)
                .project(projection)?
                .build()?,
        ))
    }

    async fn native_partition(
        &mut self,
        input: LogicalPlan,
        key: &str,
        operators: &[ast::SpannedOperator],
    ) -> Result<Option<LogicalPlan>> {
        let [operator] = operators else {
            return Ok(None);
        };
        let expanded = if self.functions.is_empty() {
            operator.kind.clone()
        } else {
            self.expander(input.schema()).operator(&operator.kind)?
        };
        let operator = self.resolve_operator(&expanded).await?;
        let key = field_expr(
            &input,
            input
                .schema()
                .index_of_column_by_name(None, key)
                .ok_or_else(|| Error::Plan("unknown partition key".into()))?,
        );
        let temporary = self.temporary_name(&input);
        let result = match &operator {
            Operator::Top { count, key: sort } => {
                use datafusion::logical_expr::expr_fn::ExprFunctionExt;
                let count = self.row_limit(count)?;
                let sort = self
                    .expressions()
                    .scalar(&sort.expr, input.schema())?
                    .sort(sort.ascending, sort.nulls_first);
                let rank = self
                    .state
                    .udwf("row_number")?
                    .call(Vec::new())
                    .partition_by(vec![key])
                    .order_by(vec![sort])
                    .build()?
                    .alias(&temporary);
                let columns = (0..input.schema().fields().len())
                    .map(|i| field_expr(&input, i))
                    .collect::<Vec<_>>();
                LogicalPlanBuilder::from(input)
                    .window(vec![rank])?
                    .filter(Expr::Column(Column::from_name(&temporary)).lt_eq(lit(count as u64)))?
                    .project(columns)?
                    .build()?
            }
            Operator::Summarize { aggregates, by } => {
                let mut columns = (0..input.schema().fields().len())
                    .map(|i| field_expr(&input, i))
                    .collect::<Vec<_>>();
                columns.push(key.alias(&temporary));
                let input = LogicalPlanBuilder::from(input).project(columns)?.build()?;
                let mut by = by.clone();
                by.push(NamedExpr {
                    name: Some(temporary.clone()),
                    expr: ast::Expr {
                        kind: ExprKind::Identifier(vec![temporary.clone()]),
                        span: ast::Span::default(),
                        depth: 1,
                    },
                });
                let result = self.summarize(input, aggregates, &by)?;
                let columns = result
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, field)| field.name() != &temporary)
                    .map(|(i, _)| field_expr(&result, i))
                    .collect::<Vec<_>>();
                LogicalPlanBuilder::from(result).project(columns)?.build()?
            }
            Operator::Count => {
                let aggregate = self
                    .state
                    .udaf("count")?
                    .call(vec![lit(1_i64)])
                    .alias("Count");
                let result = LogicalPlanBuilder::from(input)
                    .aggregate(vec![key.alias(&temporary)], vec![aggregate])?
                    .build()?;
                let count = field_expr(&result, 1);
                LogicalPlanBuilder::from(result)
                    .project(vec![count])?
                    .build()?
            }
            _ => return Ok(None),
        };
        Ok(Some(ordering::visible(result)?))
    }

    async fn apply_grouped(
        &mut self,
        input: LogicalPlan,
        partition: Option<(&str, ast::PartitionStrategy)>,
        expansion: Option<(Option<&str>, &[ast::ExpandColumn], usize)>,
        operators: &[ast::SpannedOperator],
    ) -> Result<LogicalPlan> {
        let input = ordering::finalize(input)?;
        let mode = if let Some((key, strategy)) = partition {
            crate::grouped::Mode::Key {
                index: input
                    .schema()
                    .index_of_column_by_name(None, key)
                    .ok_or_else(|| Error::Plan(format!("unknown partition key '{key}'")))?,
                strategy,
            }
        } else {
            crate::grouped::Mode::Row
        };
        let (slot, name, placeholder) = crate::grouped::input(Arc::clone(input.schema().inner()))?;
        let mut child = Self::new(self.state, self.track_order, &self.parameters)?;
        child.now = self.now;
        child.scalars = self.scalars.clone();
        child.tables = self.tables.clone();
        child.functions = self.functions.clone();
        child.tables.insert(name.clone(), placeholder);
        let mut body = Vec::new();
        let mut expanded_names = HashSet::new();
        if let Some((index, columns, limit)) = expansion {
            child.track_order = true;
            body.push(ast::SpannedOperator {
                kind: Operator::Serialize(Vec::new()),
                span: Span::default(),
            });
            for (i, column) in columns.iter().enumerate() {
                expanded_names.insert(output_name(&column.value, i, &mut HashSet::new())?);
            }
            if let Some(index) = index {
                expanded_names.insert(index.to_owned());
            }
            body.push(ast::SpannedOperator {
                kind: Operator::MvExpand {
                    array_mode: false,
                    arrays_only: true,
                    index: index.map(str::to_owned),
                    columns: columns.to_vec(),
                    limit,
                },
                span: Span::default(),
            });
        }
        body.extend_from_slice(operators);
        let query = ast::Query {
            bindings: Vec::new(),
            pipeline: ast::Pipeline {
                source: Source::Table {
                    name: vec![name],
                    span: Span::default(),
                },
                operators: body,
            },
        };
        let template = ordering::finalize(child.query(&query).await?)?;
        let restore = if expansion.is_some() {
            input
                .schema()
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, field)| {
                    !expanded_names.contains(field.name())
                        && template
                            .schema()
                            .index_of_column_by_name(None, field.name())
                            .is_none()
                })
                .map(|(i, _)| i)
                .collect()
        } else {
            Vec::new()
        };
        let name = self.temporary_name(&input);
        let result = crate::grouped::plan(input, template, slot, mode, restore, name)?;
        if expansion.is_some() && self.track_order {
            ordering::sequence(self.state, &self.scalars, result, Vec::new())
        } else {
            Ok(result)
        }
    }

    fn mv_expand(
        &mut self,
        plan: LogicalPlan,
        modes: (bool, bool),
        index: Option<&str>,
        columns: &[ast::ExpandColumn],
        limit: usize,
    ) -> Result<LogicalPlan> {
        let mut used = HashSet::new();
        let mut targets = Vec::new();
        let mut sources = Vec::new();
        let mut projection = (0..plan.schema().fields().len())
            .map(|i| field_expr(&plan, i))
            .collect::<Vec<_>>();
        for (i, column) in columns.iter().enumerate() {
            let name = output_name(&column.value, i, &mut used)?;
            let expression = self
                .expressions()
                .scalar(&column.value.expr, plan.schema())?;
            if !dynamic::is_dynamic(&expression.get_type(plan.schema().as_ref())?) {
                return Err(Error::syntax(
                    "mv-expand requires a dynamic array or property bag",
                    column.value.expr.span,
                ));
            }
            sources.push(expression);
            targets.push((name, i, column.ty));
        }
        if let Some(index) = index
            && (plan.schema().index_of_column_by_name(None, index).is_some()
                || used.contains(index))
        {
            return Err(Error::Plan(format!(
                "mv-expand index column '{index}' already exists"
            )));
        }
        let temporary = self.temporary_name(&plan);
        projection.push(
            dynamic::call(
                Kernel::Expand {
                    array_mode: modes.0,
                    arrays_only: modes.1,
                    limit,
                    columns: columns.len(),
                },
                sources,
            )
            .alias(&temporary),
        );
        let expanded = LogicalPlanBuilder::from(plan.clone())
            .project(projection)?
            .unnest_column(Column::from_name(&temporary))?
            .build()?;
        let expanded = if let Some(order) = ordering::ordinal(plan.schema()) {
            let index = self.expressions().function(
                "get_field",
                vec![Expr::Column(Column::from_name(&temporary)), lit("index")],
            )?;
            ordering::sequence(
                self.state,
                &self.scalars,
                expanded,
                vec![order.sort(true, false), index.sort(true, false)],
            )?
        } else {
            expanded
        };
        let expanded_column = |index: usize, ty: Option<ast::ScalarType>| -> Result<Expr> {
            let column = self.expressions().function(
                "get_field",
                vec![
                    Expr::Column(Column::from_name(&temporary)),
                    lit(format!("column_{index}")),
                ],
            )?;
            Ok(
                if let Some(ty) = ty.filter(|ty| *ty != ast::ScalarType::Dynamic) {
                    dynamic::call(Kernel::TypeOnly(data_type(ty)), vec![column])
                } else {
                    column
                },
            )
        };
        let mut projection = plan
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| !ordering::hidden(field))
            .map(|(i, field)| {
                if let Some((name, index, ty)) =
                    targets.iter().find(|(name, _, _)| name == field.name())
                {
                    Ok(expanded_column(*index, *ty)?.alias(name))
                } else {
                    Ok(field_expr(&plan, i))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        if let Some(order) = ordering::ordinal(expanded.schema()) {
            projection.push(order);
        }
        for (name, index, ty) in &targets {
            if plan.schema().index_of_column_by_name(None, name).is_none() {
                projection.push(expanded_column(*index, *ty)?.alias(name));
            }
        }
        if let Some(index) = index {
            projection.push(
                self.expressions()
                    .function(
                        "get_field",
                        vec![Expr::Column(Column::from_name(temporary)), lit("index")],
                    )?
                    .alias(index),
            );
        }
        Ok(LogicalPlanBuilder::from(expanded)
            .project(projection)?
            .build()?)
    }

    fn temporary_name(&mut self, plan: &LogicalPlan) -> String {
        loop {
            let name = format!("__kql_temporary_{}", self.next_alias);
            self.next_alias += 1;
            if plan.schema().index_of_column_by_name(None, &name).is_none() {
                return name;
            }
        }
    }

    fn datatable(
        &self,
        columns: &[(String, ast::ScalarType)],
        values: &[ast::Expr],
    ) -> Result<LogicalPlan> {
        if columns.is_empty() || !values.len().is_multiple_of(columns.len()) {
            return Err(Error::Plan(
                "datatable values must fill a nonempty column schema".into(),
            ));
        }
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, ty)| {
                    datafusion::arrow::datatypes::Field::new(
                        name,
                        data_type(*ty),
                        *ty != ast::ScalarType::String,
                    )
                })
                .collect::<Vec<_>>(),
        ));
        let expressions = self.expressions();
        let mut arrays = Vec::with_capacity(columns.len());
        for (index, (_, ty)) in columns.iter().enumerate() {
            let ty = data_type(*ty);
            let scalars = values
                .iter()
                .skip(index)
                .step_by(columns.len())
                .map(|expr| {
                    let lowered = expressions.scalar(expr, &DFSchema::empty())?;
                    let scalar = expressions.constant(lowered, expr.span)?.cast_to(&ty)?;
                    Ok(if is_string(&ty) && scalar.is_null() {
                        ScalarValue::Utf8(Some(String::new()))
                    } else {
                        scalar
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            arrays.push(if scalars.is_empty() {
                new_empty_array(&ty)
            } else {
                ScalarValue::iter_to_array(scalars)?
            });
        }
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
            .map_err(datafusion::error::DataFusionError::from)?;
        let provider = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(LogicalPlanBuilder::scan(
            "__kql_datatable",
            provider_as_source(Arc::new(provider)),
            None,
        )?
        .build()?)
    }

    fn row_limit(&self, value: &ast::Expr) -> Result<usize> {
        let number = if let ExprKind::Literal(ast::Literal::Integer(value)) = &value.kind {
            *value
        } else {
            let expression = self.expressions().scalar(value, &DFSchema::empty())?;
            match self.expressions().constant(expression, value.span)? {
                ScalarValue::Int64(Some(number)) => number,
                ScalarValue::Int32(Some(number)) => i64::from(number),
                _ => {
                    return Err(Error::syntax(
                        "row count must be a nonnegative constant integer",
                        value.span,
                    ));
                }
            }
        };
        usize::try_from(number)
            .map_err(|_| Error::syntax("row count must be nonnegative and fit usize", value.span))
    }

    fn named_expressions(&self, items: &[NamedExpr], schema: &DFSchema) -> Result<Vec<Expr>> {
        let mut used = HashSet::new();
        let mut result = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            if matches!(item.expr.kind, ExprKind::Wildcard) && item.name.is_none() {
                for (i, field) in schema.fields().iter().enumerate() {
                    if ordering::hidden(field) {
                        continue;
                    }
                    if !used.insert(field.name().clone()) {
                        return Err(Error::Plan(format!(
                            "duplicate output column '{}'",
                            field.name()
                        )));
                    }
                    result.push(Expr::Column(Column::from(schema.qualified_field(i))));
                }
            } else {
                let name = output_name(item, index, &mut used)?;
                result.push(ordering::user_alias(
                    self.expressions().scalar(&item.expr, schema)?,
                    name,
                    schema,
                )?);
            }
        }
        Ok(result)
    }

    fn summarize(
        &self,
        plan: LogicalPlan,
        aggregates: &[NamedExpr],
        by: &[NamedExpr],
    ) -> Result<LogicalPlan> {
        self.normalize_strings(crate::aggregation::plan(
            self.state,
            &self.scalars,
            self.now,
            plan,
            aggregates,
            by,
        )?)
    }

    fn normalize_strings(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        if !plan.schema().fields().iter().any(|f| {
            is_string(f.data_type())
                && (f.is_nullable() || matches!(f.data_type(), DataType::Dictionary(_, _)))
        }) {
            return Ok(plan);
        }
        let expressions = self.expressions();
        let projected = plan
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let value = field_expr(&plan, i);
                if is_string(field.data_type()) {
                    let ty = if matches!(field.data_type(), DataType::Dictionary(_, _)) {
                        DataType::Utf8
                    } else {
                        field.data_type().clone()
                    };
                    let value = if field.data_type() != &ty {
                        cast(value, ty.clone())
                    } else {
                        value
                    };
                    let value = if field.is_nullable() {
                        expressions.function("coalesce", vec![value, cast(lit(""), ty)])?
                    } else {
                        value
                    };
                    Ok(value.alias(field.name()))
                } else {
                    Ok(value)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(LogicalPlanBuilder::from(plan).project(projected)?.build()?)
    }

    fn join(
        &mut self,
        left: LogicalPlan,
        right: LogicalPlan,
        kind: JoinKind,
        keys: &[ast::JoinKey],
    ) -> Result<LogicalPlan> {
        let mut left = ordering::visible(left)?;
        let right = ordering::visible(right)?;
        if kind == JoinKind::InnerUnique {
            let on = keys
                .iter()
                .map(|k| {
                    let index = left
                        .schema()
                        .index_of_column_by_name(None, &k.left)
                        .ok_or_else(|| {
                            Error::Plan(format!("unknown left join key '{}'", k.left))
                        })?;
                    Ok(field_expr(&left, index))
                })
                .collect::<Result<Vec<_>>>()?;
            let selected = (0..left.schema().fields().len())
                .map(|i| field_expr(&left, i))
                .collect();
            left = LogicalPlanBuilder::from(left)
                .distinct_on(on, selected, None)?
                .build()?;
        }
        let alias = self.next_alias;
        self.next_alias += 1;
        let left_alias = format!("__kql_left_{alias}");
        let right_alias = format!("__kql_right_{alias}");
        let left = LogicalPlanBuilder::from(left)
            .alias(left_alias.clone())?
            .build()?;
        let right = LogicalPlanBuilder::from(right)
            .alias(right_alias.clone())?
            .build()?;
        let left_keys: Vec<Column> = keys
            .iter()
            .map(|k| Column::new(Some(left_alias.clone()), k.left.clone()))
            .collect();
        let right_keys: Vec<Column> = keys
            .iter()
            .map(|k| Column::new(Some(right_alias.clone()), k.right.clone()))
            .collect();
        let join_type = match kind {
            JoinKind::InnerUnique | JoinKind::Inner => JoinType::Inner,
            JoinKind::LeftOuter => JoinType::Left,
            JoinKind::RightOuter => JoinType::Right,
            JoinKind::FullOuter => JoinType::Full,
            JoinKind::LeftSemi => JoinType::LeftSemi,
            JoinKind::RightSemi => JoinType::RightSemi,
            JoinKind::LeftAnti => JoinType::LeftAnti,
            JoinKind::RightAnti => JoinType::RightAnti,
        };
        let plan = LogicalPlanBuilder::from(left)
            .join(right, join_type, (left_keys, right_keys), None)?
            .build()?;
        let reserved = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<HashSet<_>>();
        let mut used = HashSet::new();
        let projected = plan
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let base = field.name();
                let mut name = base.clone();
                if used.contains(&name) {
                    let mut suffix = 1;
                    loop {
                        name = format!("{base}{suffix}");
                        if !used.contains(&name) && !reserved.contains(&name) {
                            break;
                        }
                        suffix += 1;
                    }
                }
                used.insert(name.clone());
                field_expr(&plan, i).alias(name)
            })
            .collect::<Vec<_>>();
        self.normalize_strings(LogicalPlanBuilder::from(plan).project(projected)?.build()?)
    }

    fn union(
        &self,
        plans: Vec<LogicalPlan>,
        kind: UnionKind,
        withsource: Option<&str>,
        labels: &[String],
    ) -> Result<LogicalPlan> {
        let plan = crate::tabular::union(plans, kind, withsource, labels)?;
        check_plan(&plan)?;
        Ok(plan)
    }
}

fn table_reference(parts: &[String], span: Span) -> Result<TableReference> {
    Ok(match parts {
        [table] => TableReference::bare(table.clone()),
        [schema, table] => TableReference::partial(schema.clone(), table.clone()),
        [catalog, schema, table] => {
            TableReference::full(catalog.clone(), schema.clone(), table.clone())
        }
        _ => {
            return Err(Error::syntax(
                "table paths have at most three components",
                span,
            ));
        }
    })
}

fn field_expr(plan: &LogicalPlan, index: usize) -> Expr {
    Expr::Column(Column::from(plan.schema().qualified_field(index)))
}

pub(crate) fn output_name(
    item: &NamedExpr,
    index: usize,
    used: &mut HashSet<String>,
) -> Result<String> {
    if let Some(name) = &item.name {
        if !used.insert(name.clone()) {
            return Err(Error::syntax(
                format!("duplicate output column '{name}'"),
                item.expr.span,
            ));
        }
        return Ok(name.clone());
    }
    let base = default_name(&item.expr, index);
    let mut name = base.clone();
    let mut suffix = 1;
    while !used.insert(name.clone()) {
        name = format!("{base}{suffix}");
        suffix += 1;
    }
    Ok(name)
}

pub(crate) fn default_name(expr: &ast::Expr, index: usize) -> String {
    match &expr.kind {
        ExprKind::Identifier(parts) => parts.last().expect("identifiers are nonempty").clone(),
        ExprKind::Call { name, args } => {
            let column = args.first().and_then(|arg| {
                if let ExprKind::Identifier(parts) = &arg.kind {
                    parts.last()
                } else {
                    None
                }
            });
            if matches!(name.as_str(), "bin" | "bin_at" | "floor")
                && let Some(column) = column
            {
                return column.clone();
            }
            let name = match name.as_str() {
                "make_list" | "make_list_if" | "make_list_with_nulls" | "makelist" => "list",
                "make_set" | "make_set_if" | "makeset" => "set",
                "make_bag" | "make_bag_if" | "make_dictionary" => "bag",
                "take_any" | "any" | "take_anyif" => "any",
                "percentiles_array" | "percentilesw_array" => "percentiles",
                name => name,
            };
            if matches!(name, "percentile" | "percentilew") && args.len() >= 2 {
                let rank = match &args[args.len() - 1].kind {
                    ExprKind::Literal(ast::Literal::Integer(value)) => value.to_string(),
                    ExprKind::Literal(ast::Literal::Real(value)) => {
                        value.to_string().replace('.', "_")
                    }
                    _ => format!("{}", index + 1),
                };
                return format!(
                    "percentile_{}_{rank}",
                    column.map(String::as_str).unwrap_or("")
                );
            }
            format!("{name}_{}", column.map(String::as_str).unwrap_or(""))
        }
        _ => format!("Column{}", index + 1),
    }
}

fn scalar_arguments(args: &[ast::Expr]) -> Vec<ast::CallArgument> {
    args.iter()
        .map(|arg| {
            let (name, value) = match &arg.kind {
                ExprKind::NamedArgument { name, value } => (Some(name.clone()), *value.clone()),
                _ => (None, arg.clone()),
            };
            ast::CallArgument {
                name,
                value: ast::ArgumentValue::Scalar(value),
            }
        })
        .collect()
}

fn check_plan(plan: &LogicalPlan) -> Result<()> {
    fn visit(plan: &LogicalPlan, depth: usize, nodes: &mut usize) -> Result<()> {
        *nodes += 1;
        if depth > 256 || *nodes > 16_384 {
            return Err(Error::Plan(
                "expanded tabular query exceeds planning limits".into(),
            ));
        }
        for input in plan.inputs() {
            visit(input, depth + 1, nodes)?;
        }
        Ok(())
    }
    visit(plan, 0, &mut 0)
}

pub(crate) fn pattern_matches(pattern: &str, name: &str) -> bool {
    let (mut p, mut n, mut star, mut matched) = (0, 0, None, 0);
    let pattern = pattern.as_bytes();
    let name = name.as_bytes();
    while n < name.len() {
        if p < pattern.len() && pattern[p] == name[n] && pattern[p] != b'*' {
            p += 1;
            n += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star = Some(p);
            matched = n;
            p += 1;
        } else if let Some(at) = star {
            matched += 1;
            n = matched;
            p = at + 1;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}
