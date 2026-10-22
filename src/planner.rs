use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::array::new_empty_array;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionArgs;
use datafusion::common::{Column, DFSchema, ScalarValue, TableReference};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::expr_fn::cast;
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::logical_expr::{Expr, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionContext, lit};

use crate::ast::{
    self, BindingValue, ExprKind, JoinKind, NamedExpr, Operator, Source, Span, UnionKind,
};
use crate::dynamic::{self, Kernel};
use crate::error::{Error, Result};
use crate::expressions::{Expressions, is_string};
use crate::literals::data_type;

type PlanFuture<'a> = Pin<Box<dyn Future<Output = Result<LogicalPlan>> + Send + 'a>>;

/// Plan a previously parsed query against this session's current catalog.
/// No input batches are read or collected.
pub async fn logical_plan(ctx: &SessionContext, query: &ast::Query) -> Result<LogicalPlan> {
    let state = ctx.state();
    Planner::new(&state)?.query(query).await
}

pub(crate) async fn dataframe(ctx: &SessionContext, query: &ast::Query) -> Result<DataFrame> {
    let state = ctx.state();
    let plan = Planner::new(&state)?.query(query).await?;
    Ok(DataFrame::new(state, plan))
}

struct Planner<'a> {
    state: &'a SessionState,
    scalars: HashMap<String, Expr>,
    tables: HashMap<String, LogicalPlan>,
    now: i64,
    next_alias: usize,
}

impl<'s> Planner<'s> {
    fn new(state: &'s SessionState) -> Result<Self> {
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
            now,
            next_alias: 0,
        })
    }

    fn expressions(&self) -> Expressions<'_> {
        Expressions {
            state: self.state,
            scalars: &self.scalars,
            now: self.now,
        }
    }

    fn query<'a>(&'a mut self, query: &'a ast::Query) -> PlanFuture<'a> {
        Box::pin(async move {
            let saved_scalars = self.scalars.clone();
            let saved_tables = self.tables.clone();
            let result = self.query_inner(query).await;
            self.scalars = saved_scalars;
            self.tables = saved_tables;
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
            let table_name = match &binding.value {
                BindingValue::Scalar(ast::Expr {
                    kind: ExprKind::Identifier(parts),
                    ..
                }) if !self.scalars.contains_key(&parts[0]) => Some(parts),
                _ => None,
            };
            if let Some(parts) = table_name {
                let plan = self.table(parts, binding.span).await?;
                check_plan(&plan)?;
                self.scalars.remove(&binding.name);
                self.tables.insert(binding.name.clone(), plan);
            } else {
                match &binding.value {
                    BindingValue::Scalar(expr) => {
                        let value = self.expressions().scalar(expr, &DFSchema::empty())?;
                        self.tables.remove(&binding.name);
                        self.scalars.insert(binding.name.clone(), value);
                    }
                    BindingValue::Tabular(pipeline) => {
                        let plan = self.pipeline(pipeline).await?;
                        self.scalars.remove(&binding.name);
                        self.tables.insert(binding.name.clone(), plan);
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
        let reference = match parts {
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
        };
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

    fn source<'a>(&'a mut self, source: &'a Source) -> PlanFuture<'a> {
        Box::pin(async move {
            match source {
                Source::Table { name, span } => self.table(name, *span).await,
                Source::Subquery(query) => self.query(query).await,
                Source::Print(items) => {
                    let plan = LogicalPlanBuilder::empty(true).build()?;
                    let expressions = self.named_expressions(items, plan.schema())?;
                    Ok(LogicalPlanBuilder::from(plan)
                        .project(expressions)?
                        .build()?)
                }
                Source::Datatable { columns, values } => self.datatable(columns, values),
                Source::Range {
                    name,
                    start,
                    end,
                    step,
                } => {
                    let expressions = self.expressions();
                    let mut args = Vec::with_capacity(3);
                    for expr in [start, end, step] {
                        let lowered = expressions.scalar(expr, &DFSchema::empty())?;
                        let value = expressions.constant(lowered, expr.span)?;
                        if !matches!(value, ScalarValue::Int64(Some(_))) {
                            return Err(Error::unsupported(
                                "range currently supports non-null long bounds and step",
                                expr.span,
                            ));
                        }
                        args.push(lit(value));
                    }
                    let function = self
                        .state
                        .table_functions()
                        .get("generate_series")
                        .ok_or_else(|| {
                            Error::Plan("session has no generate_series table function".into())
                        })?;
                    let provider = function.create_table_provider_with_args(
                        TableFunctionArgs::new(&args, self.state),
                    )?;
                    let plan = LogicalPlanBuilder::scan(
                        "__kql_range",
                        provider_as_source(provider),
                        None,
                    )?
                    .build()?;
                    let column = field_expr(&plan, 0).alias(name);
                    Ok(LogicalPlanBuilder::from(plan)
                        .project(vec![column])?
                        .build()?)
                }
                Source::Union { kind, inputs } => {
                    let mut plans = Vec::with_capacity(inputs.len());
                    for input in inputs {
                        plans.push(self.source(input).await?);
                    }
                    self.union(plans, *kind, Span::default())
                }
            }
        })
    }

    async fn pipeline(&mut self, pipeline: &ast::Pipeline) -> Result<LogicalPlan> {
        let mut plan = self.source(&pipeline.source).await?;
        check_plan(&plan)?;
        for operator in &pipeline.operators {
            plan = match &operator.kind {
                Operator::Where(predicate) => {
                    let expression = self.expressions().predicate(predicate, plan.schema())?;
                    LogicalPlanBuilder::from(plan).filter(expression)?.build()?
                }
                Operator::Take(count) => LogicalPlanBuilder::from(plan)
                    .limit(0, Some(*count))?
                    .build()?,
                Operator::Project(items) => {
                    let expressions = self.named_expressions(items, plan.schema())?;
                    LogicalPlanBuilder::from(plan)
                        .project(expressions)?
                        .build()?
                }
                Operator::Extend(items) => {
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
                Operator::MvExpand {
                    array_mode,
                    index,
                    columns,
                    limit,
                } => self.mv_expand(plan, *array_mode, index.as_deref(), columns, *limit)?,
                Operator::Parse { .. } => {
                    let temporary = self.temporary_name(&plan);
                    crate::parsing::plan(&self.expressions(), plan, operator, temporary)?
                }
                Operator::Summarize { aggregates, by } => self.summarize(plan, aggregates, by)?,
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
                    LogicalPlanBuilder::from(plan).sort(sorts)?.build()?
                }
                Operator::Top { count, key } => {
                    let sort = self
                        .expressions()
                        .scalar(&key.expr, plan.schema())?
                        .sort(key.ascending, key.nulls_first);
                    LogicalPlanBuilder::from(plan)
                        .sort(vec![sort])?
                        .limit(0, Some(*count))?
                        .build()?
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
                Operator::Union { kind, inputs } => {
                    let mut plans = vec![plan];
                    for input in inputs {
                        plans.push(self.source(input).await?);
                    }
                    self.union(plans, *kind, operator.span)?
                }
            };
            check_plan(&plan)?;
        }
        Ok(plan)
    }

    fn mv_expand(
        &mut self,
        plan: LogicalPlan,
        array_mode: bool,
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
                    array_mode,
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

    fn named_expressions(&self, items: &[NamedExpr], schema: &DFSchema) -> Result<Vec<Expr>> {
        let mut used = HashSet::new();
        let mut result = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            if matches!(item.expr.kind, ExprKind::Wildcard) && item.name.is_none() {
                for (i, field) in schema.fields().iter().enumerate() {
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
                result.push(self.expressions().scalar(&item.expr, schema)?.alias(name));
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
        mut left: LogicalPlan,
        right: LogicalPlan,
        kind: JoinKind,
        keys: &[ast::JoinKey],
    ) -> Result<LogicalPlan> {
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

    fn union(&self, plans: Vec<LogicalPlan>, kind: UnionKind, span: Span) -> Result<LogicalPlan> {
        let mut fields: Vec<(String, DataType)> = Vec::new();
        for plan in &plans {
            for field in plan.schema().fields() {
                let ty = if is_string(field.data_type()) {
                    DataType::Utf8
                } else {
                    field.data_type().clone()
                };
                if let Some((_, previous)) = fields.iter().find(|(name, _)| name == field.name()) {
                    if previous != &ty {
                        return Err(Error::unsupported(
                            format!(
                                "union type splitting for column '{}'; use project/extend to cast it explicitly",
                                field.name()
                            ),
                            span,
                        ));
                    }
                } else {
                    fields.push((field.name().clone(), ty));
                }
            }
        }
        if kind == UnionKind::Inner {
            fields.retain(|(name, _)| {
                plans
                    .iter()
                    .all(|p| p.schema().fields().iter().any(|f| f.name() == name))
            });
        }
        let mut aligned = Vec::with_capacity(plans.len());
        for plan in plans {
            let expressions = fields
                .iter()
                .map(|(name, ty)| {
                    let index = plan.schema().index_of_column_by_name(None, name);
                    let expr = if let Some(index) = index {
                        cast(field_expr(&plan, index), ty.clone())
                    } else if is_string(ty) {
                        cast(lit(""), ty.clone())
                    } else {
                        lit(ScalarValue::try_from(ty)?)
                    };
                    Ok(expr.alias(name))
                })
                .collect::<Result<Vec<_>>>()?;
            aligned.push(
                LogicalPlanBuilder::from(plan)
                    .project(expressions)?
                    .build()?,
            );
        }
        let mut aligned = aligned.into_iter();
        let first = aligned
            .next()
            .ok_or_else(|| Error::Plan("union requires input tables".into()))?;
        let mut result = LogicalPlanBuilder::from(first);
        for input in aligned {
            result = result.union(input)?;
        }
        let plan = result.build()?;
        check_plan(&plan)?;
        Ok(plan)
    }
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

fn default_name(expr: &ast::Expr, index: usize) -> String {
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
            if matches!(name.as_str(), "bin" | "floor")
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

fn pattern_matches(pattern: &str, name: &str) -> bool {
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
