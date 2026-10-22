use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::datasource::provider_as_source;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, TableType};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, reset_plan_states};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::lit;
use futures::TryStreamExt;

use crate::ast::{self, ExprKind, ScanOutput};
use crate::expressions::{Expressions, parameter_cast};
use crate::{Error, ordering};

#[derive(Debug)]
struct LogicalStep {
    condition: Expr,
    assignments: Vec<(usize, Expr)>,
    output: ScanOutput,
}

pub(crate) fn plan(
    state: &SessionState,
    scalars: &HashMap<String, Expr>,
    now: i64,
    input: LogicalPlan,
    scan: &ast::Scan,
    temporary: String,
) -> crate::Result<LogicalPlan> {
    if let Some(plan) = cumulative(state, scalars, now, input.clone(), scan, &temporary)? {
        return Ok(plan);
    }
    let input = ordering::finalize(input)?;
    let expressions = Expressions {
        state,
        scalars,
        now,
    };
    let mut fields = input
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone().with_nullable(true))
        .collect::<Vec<_>>();
    let mut defaults = fields
        .iter()
        .map(|field| default_value(field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    let mut names = fields
        .iter()
        .map(|field| field.name().clone())
        .collect::<HashSet<_>>();
    for declaration in &scan.declarations {
        if !names.insert(declaration.name.clone()) {
            return Err(Error::Plan(format!(
                "scan declaration '{}' already exists",
                declaration.name
            )));
        }
        let ty = crate::literals::data_type(declaration.ty);
        let value = if let Some(default) = &declaration.default {
            let value = expressions.scalar(default, &DFSchema::empty())?;
            let value = expressions.constant(value, default.span)?;
            let value =
                parameter_cast(lit(value), declaration.ty, &DFSchema::empty(), default.span)?;
            expressions.constant(value, default.span)?
        } else {
            default_value(&ty)?
        };
        fields.push(Field::new(&declaration.name, ty, true));
        defaults.push(value);
    }
    if let Some(name) = &scan.match_id
        && !names.insert(name.clone())
    {
        return Err(Error::Plan(format!(
            "scan match-id column '{name}' already exists"
        )));
    }
    let mut steps = HashMap::new();
    for (index, step) in scan.steps.iter().enumerate() {
        if steps.insert(step.name.clone(), index).is_some() {
            return Err(Error::Plan(format!("duplicate scan step '{}'", step.name)));
        }
    }
    let mut references = BTreeMap::new();
    let mut rewritten = Vec::new();
    for (index, step) in scan.steps.iter().enumerate() {
        let condition = rewrite(&step.condition, index, &steps, &fields, &mut references)?;
        let mut assignments = Vec::new();
        let mut assigned = HashSet::new();
        for (name, value) in &step.assignments {
            if !assigned.insert(name) {
                return Err(Error::syntax("duplicate scan assignment", value.span));
            }
            let column = fields
                .iter()
                .position(|field| field.name() == name)
                .ok_or_else(|| {
                    Error::syntax(
                        format!("unknown scan assignment column '{name}'"),
                        value.span,
                    )
                })?;
            assignments.push((
                column,
                rewrite(value, index, &steps, &fields, &mut references)?,
            ));
        }
        rewritten.push((condition, assignments, step.output));
    }
    let mut context_fields = fields.clone();
    let mut refs = Vec::new();
    for (&(step, column), name) in &references {
        context_fields.push(Field::new(name, fields[column].data_type().clone(), true));
        refs.push((step, column));
    }
    let context = DFSchema::try_from(Schema::new(context_fields))?;
    let mut logical_steps = Vec::new();
    for (condition, assignments, output) in rewritten {
        let predicate = expressions.predicate(&condition, &context)?;
        if predicate.get_type(&context)? != DataType::Boolean {
            return Err(Error::syntax(
                "scan conditions must be boolean",
                condition.span,
            ));
        }
        let assignments = assignments
            .into_iter()
            .map(|(index, value)| {
                let expression = expressions.scalar(&value, &context)?;
                let ty = fields[index].data_type();
                let actual = expression.get_type(&context)?;
                if actual != *ty
                    && actual != DataType::Null
                    && !(actual.is_numeric() && ty.is_numeric())
                {
                    return Err(Error::syntax(
                        format!("scan assignment requires {ty}, received {actual}"),
                        value.span,
                    ));
                }
                let expression = datafusion::logical_expr::expr_fn::cast(expression, ty.clone());
                let expression = if *ty == DataType::Utf8 {
                    expressions.function("coalesce", vec![expression, lit("")])?
                } else {
                    expression
                };
                Ok((index, expression))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        logical_steps.push(LogicalStep {
            condition: predicate,
            assignments,
            output,
        });
    }
    let raw = format!("{temporary}_value");
    let mut output_fields = fields.clone();
    if let Some(name) = &scan.match_id {
        output_fields.push(Field::new(name, DataType::Int64, false));
    }
    output_fields.push(Field::new(&raw, DataType::Int64, false));
    let table = Arc::new(ScanTable {
        input,
        steps: logical_steps,
        context: Arc::new(context),
        schema: Arc::new(Schema::new(output_fields)),
        defaults,
        references: refs,
        match_id: scan.match_id.is_some(),
    });
    let input =
        LogicalPlanBuilder::scan(temporary.clone(), provider_as_source(table), None)?.build()?;
    let mut projection = (0..input.schema().fields().len() - 1)
        .map(|i| Expr::Column(Column::from(input.schema().qualified_field(i))))
        .collect::<Vec<_>>();
    projection.push(ordering::helper(
        Expr::Column(Column::from_name(raw)),
        temporary,
        true,
    ));
    Ok(LogicalPlanBuilder::from(input)
        .project(projection)?
        .build()?)
}

fn cumulative(
    state: &SessionState,
    scalars: &HashMap<String, Expr>,
    now: i64,
    mut input: LogicalPlan,
    scan: &ast::Scan,
    temporary: &str,
) -> crate::Result<Option<LogicalPlan>> {
    use datafusion::logical_expr::expr_fn::ExprFunctionExt;
    use datafusion::logical_expr::registry::FunctionRegistry;
    use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
    let [step] = scan.steps.as_slice() else {
        return Ok(None);
    };
    if step.output != ScanOutput::All
        || !matches!(
            step.condition.kind,
            ExprKind::Literal(ast::Literal::Bool(true))
        )
        || step.assignments.is_empty()
    {
        return Ok(None);
    }
    let expressions = Expressions {
        state,
        scalars,
        now,
    };
    let mut terms = HashMap::new();
    for (name, value) in &step.assignments {
        let ExprKind::Binary {
            left,
            op: ast::BinaryOp::Add,
            right,
        } = &value.kind
        else {
            return Ok(None);
        };
        let term = if matches!(&left.kind,ExprKind::Identifier(parts) if parts == &[step.name.clone(),name.clone()])
        {
            right.as_ref()
        } else if matches!(&right.kind,ExprKind::Identifier(parts) if parts == &[step.name.clone(),name.clone()])
        {
            left.as_ref()
        } else {
            return Ok(None);
        };
        if !matches!(&term.kind,ExprKind::Identifier(parts) if parts.len() == 1 && input.schema().index_of_column_by_name(None,&parts[0]).is_some())
        {
            return Ok(None);
        }
        let Some(declaration) = scan.declarations.iter().find(|d| &d.name == name) else {
            return Ok(None);
        };
        if declaration.ty != ast::ScalarType::Long {
            return Ok(None);
        }
        let Some(default) = &declaration.default else {
            return Ok(None);
        };
        let initial = expressions.constant(
            expressions.scalar(default, &DFSchema::empty())?,
            default.span,
        )?;
        if initial != ScalarValue::Int64(Some(0)) {
            return Ok(None);
        }
        let term = expressions.scalar(term, input.schema())?;
        if term.get_type(input.schema())? != DataType::Int64 || term.nullable(input.schema())? {
            return Ok(None);
        }
        if terms.insert(name.clone(), term).is_some() {
            return Err(Error::syntax("duplicate scan assignment", value.span));
        }
    }
    let mut names = input
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<HashSet<_>>();
    for declaration in &scan.declarations {
        if !names.insert(declaration.name.clone()) {
            return Err(Error::Plan(format!(
                "scan declaration '{}' already exists",
                declaration.name
            )));
        }
    }
    if let Some(name) = &scan.match_id
        && !names.insert(name.clone())
    {
        return Err(Error::Plan(format!(
            "scan match-id column '{name}' already exists"
        )));
    }
    if ordering::ordinal(input.schema()).is_none() {
        input = ordering::sequence(state, scalars, input, Vec::new())?;
    }
    let order = ordering::ordinal(input.schema())
        .ok_or_else(|| Error::Plan("cumulative scan is not serialized".into()))?;
    let mut windows = Vec::new();
    let mut projection = (0..input.schema().fields().len())
        .map(|i| Expr::Column(Column::from(input.schema().qualified_field(i))))
        .collect::<Vec<_>>();
    for (index, declaration) in scan.declarations.iter().enumerate() {
        if let Some(term) = terms.get(&declaration.name) {
            let raw = format!("{temporary}_sum_{index}_value");
            let sum = Expr::from(datafusion::logical_expr::expr::WindowFunction::new(
                state.udaf("sum")?,
                vec![term.clone()],
            ))
            .order_by(vec![order.clone().sort(true, false)])
            .window_frame(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            ))
            .build()?
            .alias(&raw);
            windows.push(sum);
            projection.push(Expr::Column(Column::from_name(raw)).alias(&declaration.name));
        } else {
            let ty = crate::literals::data_type(declaration.ty);
            let value = if let Some(default) = &declaration.default {
                let value = expressions.constant(
                    expressions.scalar(default, &DFSchema::empty())?,
                    default.span,
                )?;
                expressions.constant(
                    parameter_cast(lit(value), declaration.ty, &DFSchema::empty(), default.span)?,
                    default.span,
                )?
            } else {
                default_value(&ty)?
            };
            projection.push(lit(value).alias(&declaration.name));
        }
    }
    if let Some(name) = &scan.match_id {
        projection.push(lit(0_i64).alias(name));
    }
    Ok(Some(
        LogicalPlanBuilder::from(input)
            .window(windows)?
            .project(projection)?
            .build()?,
    ))
}

fn default_value(ty: &DataType) -> Result<ScalarValue> {
    if *ty == DataType::Utf8 {
        Ok(ScalarValue::Utf8(Some(String::new())))
    } else {
        ScalarValue::try_from(ty)
    }
}

fn rewrite(
    expr: &ast::Expr,
    current: usize,
    steps: &HashMap<String, usize>,
    fields: &[Field],
    references: &mut BTreeMap<(usize, usize), String>,
) -> crate::Result<ast::Expr> {
    if let ExprKind::Identifier(parts) = &expr.kind
        && parts.len() >= 2
        && let Some(&step) = steps.get(&parts[0])
    {
        if step > current {
            return Err(Error::syntax(
                "scan cannot reference a later step",
                expr.span,
            ));
        }
        let column = fields
            .iter()
            .position(|field| field.name() == &parts[1])
            .ok_or_else(|| {
                Error::syntax(
                    format!("unknown scan state column '{}'", parts[1]),
                    expr.span,
                )
            })?;
        let name = references
            .entry((step, column))
            .or_insert_with(|| {
                let mut name = format!("__kql_scan_state_{step}_{column}");
                while fields.iter().any(|field| field.name() == &name) {
                    name.push('_');
                }
                name
            })
            .clone();
        let mut path = vec![name];
        path.extend_from_slice(&parts[2..]);
        return Ok(ast::Expr {
            kind: ExprKind::Identifier(path),
            span: expr.span,
            depth: expr.depth,
        });
    }
    expr.map_children(|child| rewrite(child, current, steps, fields, references))
}

#[derive(Debug)]
struct ScanTable {
    input: LogicalPlan,
    steps: Vec<LogicalStep>,
    context: Arc<DFSchema>,
    schema: SchemaRef,
    defaults: Vec<ScalarValue>,
    references: Vec<(usize, usize)>,
    match_id: bool,
}

#[derive(Debug)]
struct PhysicalStep {
    condition: Arc<dyn PhysicalExpr>,
    assignments: Vec<(usize, Arc<dyn PhysicalExpr>)>,
    output: ScanOutput,
}

#[async_trait]
impl TableProvider for ScanTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = state.create_physical_plan(&self.input).await?;
        if !matches!(input.boundedness(), Boundedness::Bounded) {
            return Err(DataFusionError::Plan("scan requires bounded input".into()));
        }
        let input: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(input));
        let simplifier = ExprSimplifier::new(
            SimplifyContext::builder()
                .with_schema(Arc::clone(&self.context))
                .with_config_options(Arc::new(state.config_options().clone()))
                .build(),
        );
        let physical = |expr: &Expr| {
            let expr = simplifier.coerce(expr.clone(), &self.context)?;
            state.create_physical_expr(simplifier.simplify(expr)?, &self.context)
        };
        let steps = self
            .steps
            .iter()
            .map(|step| {
                Ok(PhysicalStep {
                    condition: physical(&step.condition)?,
                    assignments: step
                        .assignments
                        .iter()
                        .map(|(index, expr)| Ok((*index, physical(expr)?)))
                        .collect::<Result<Vec<_>>>()?,
                    output: step.output,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let plan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            input,
            steps: Arc::new(steps),
            defaults: Arc::new(self.defaults.clone()),
            references: Arc::new(self.references.clone()),
            context: Arc::clone(self.context.inner()),
            match_id: self.match_id,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        });
        if let Some(projection) = projection {
            let expressions = projection
                .iter()
                .map(|&index| {
                    let name = self.schema.field(index).name().clone();
                    (
                        Arc::new(datafusion::physical_expr::expressions::Column::new(
                            &name, index,
                        )) as _,
                        name,
                    )
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(ProjectionExec::try_new(expressions, plan)?))
        } else {
            Ok(plan)
        }
    }
}

#[derive(Debug)]
struct ScanExec {
    input: Arc<dyn ExecutionPlan>,
    steps: Arc<Vec<PhysicalStep>>,
    defaults: Arc<Vec<ScalarValue>>,
    references: Arc<Vec<(usize, usize)>>,
    context: SchemaRef,
    match_id: bool,
    properties: Arc<PlanProperties>,
}

impl ScanExec {
    fn with_input(&self, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            input,
            steps: Arc::clone(&self.steps),
            defaults: Arc::clone(&self.defaults),
            references: Arc::clone(&self.references),
            context: Arc::clone(&self.context),
            match_id: self.match_id,
            properties: Arc::clone(&self.properties),
        }
    }
}

pub(crate) fn bind_input(
    plan: &Arc<dyn ExecutionPlan>,
    bind: impl FnOnce(Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>>,
) -> Result<Option<Transformed<Arc<dyn ExecutionPlan>>>> {
    let Some(scan) = plan.downcast_ref::<ScanExec>() else {
        return Ok(None);
    };
    let input = bind(Arc::clone(&scan.input))?;
    Ok(Some(if input.transformed {
        Transformed::yes(Arc::new(scan.with_input(input.data)) as Arc<dyn ExecutionPlan>)
    } else {
        Transformed::no(Arc::clone(plan))
    }))
}

impl DisplayAs for ScanExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KqlScanExec: steps={}", self.steps.len())
    }
}

impl ExecutionPlan for ScanExec {
    fn name(&self) -> &str {
        "KqlScanExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal("scan source is opaque".into()));
        }
        Ok(self)
    }
    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            self.with_input(reset_plan_states(Arc::clone(&self.input))?),
        ))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "scan has one output partition".into(),
            ));
        }
        let input = self.input.execute(0, Arc::clone(&context))?;
        let reservation = MemoryConsumer::new("KQL scan state").register(context.memory_pool());
        let state = ScanStream {
            input,
            pending: None,
            position: 0,
            ordinal: 0,
            next_match: 0,
            states: vec![None; self.steps.len()],
            last: vec![None; self.steps.len()],
            steps: Arc::clone(&self.steps),
            defaults: Arc::clone(&self.defaults),
            references: Arc::clone(&self.references),
            context: Arc::clone(&self.context),
            schema: self.schema(),
            match_id: self.match_id,
            output: Vec::new(),
            reservation,
            batch_size: context.session_config().batch_size(),
            finished: false,
        };
        let stream = futures::stream::try_unfold(state, |mut state| async move {
            loop {
                if state.output.len() >= state.batch_size || state.finished {
                    if state.output.is_empty() {
                        return Ok(None);
                    }
                    let batch = state.batch()?;
                    return Ok(Some((batch, state)));
                }
                if state
                    .pending
                    .as_ref()
                    .is_none_or(|batch| state.position >= batch.num_rows())
                {
                    state.pending = state.input.try_next().await?;
                    state.position = 0;
                    if state.pending.is_none() {
                        for pending in &mut state.last {
                            if let Some(row) = pending.take() {
                                state.output.push(row);
                            }
                        }
                        state.finished = true;
                        continue;
                    }
                }
                if state.pending.as_ref().is_some_and(|b| b.num_rows() == 0) {
                    continue;
                }
                state.row()?;
                state.position += 1;
                state.ordinal = state
                    .ordinal
                    .checked_add(1)
                    .ok_or_else(|| DataFusionError::Execution("scan row order overflow".into()))?;
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[derive(Clone)]
struct Sequence {
    records: Vec<Option<Arc<Vec<ScalarValue>>>>,
    match_id: i64,
}

struct ScanStream {
    input: SendableRecordBatchStream,
    pending: Option<RecordBatch>,
    position: usize,
    ordinal: i64,
    next_match: i64,
    states: Vec<Option<Sequence>>,
    last: Vec<Option<Vec<ScalarValue>>>,
    steps: Arc<Vec<PhysicalStep>>,
    defaults: Arc<Vec<ScalarValue>>,
    references: Arc<Vec<(usize, usize)>>,
    context: SchemaRef,
    schema: SchemaRef,
    match_id: bool,
    output: Vec<Vec<ScalarValue>>,
    reservation: MemoryReservation,
    batch_size: usize,
    finished: bool,
}

impl ScanStream {
    fn context(&self, current: &[ScalarValue], sequence: Option<&Sequence>) -> Result<RecordBatch> {
        let mut arrays = current
            .iter()
            .map(|value| value.to_array())
            .collect::<Result<Vec<_>>>()?;
        for &(step, column) in self.references.iter() {
            let value = sequence
                .and_then(|s| s.records.get(step))
                .and_then(Option::as_ref)
                .map(|row| &row[column])
                .unwrap_or(&self.defaults[column]);
            arrays.push(value.to_array()?);
        }
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.context),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?)
    }

    fn scalar(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Result<ScalarValue> {
        match expr.evaluate(batch)? {
            datafusion::logical_expr::ColumnarValue::Scalar(value) => Ok(value),
            datafusion::logical_expr::ColumnarValue::Array(array) => {
                ScalarValue::try_from_array(&array, 0)
            }
        }
    }

    fn row(&mut self) -> Result<()> {
        let batch = self
            .pending
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("scan has no input row".into()))?;
        let mut current = self.defaults.as_ref().clone();
        for (index, column) in batch.columns().iter().enumerate() {
            current[index] = ScalarValue::try_from_array(column, self.position)?;
        }
        for index in (0..self.steps.len()).rev() {
            let promoted = if index > 0 && self.states[index - 1].is_some() {
                let context = self.context(&current, self.states[index - 1].as_ref())?;
                if matches!(
                    Self::scalar(&self.steps[index].condition, &context)?,
                    ScalarValue::Boolean(Some(true))
                ) {
                    Some(context)
                } else {
                    None
                }
            } else {
                None
            };
            let context = if let Some(context) = promoted.as_ref() {
                context.clone()
            } else if index == 0 || self.states[index].is_some() {
                let context = self.context(&current, self.states[index].as_ref())?;
                if !matches!(
                    Self::scalar(&self.steps[index].condition, &context)?,
                    ScalarValue::Boolean(Some(true))
                ) {
                    continue;
                }
                context
            } else {
                continue;
            };
            if promoted.is_some() {
                if let Some(row) = self.last[index].take() {
                    self.output.push(row);
                }
                if let Some(row) = self.last[index - 1].take() {
                    self.output.push(row);
                }
                self.states[index] = self.states[index - 1].take();
            }
            if self.states[index].is_none() {
                let match_id = self.next_match;
                self.next_match = self
                    .next_match
                    .checked_add(1)
                    .ok_or_else(|| DataFusionError::Execution("scan match-id overflow".into()))?;
                self.states[index] = Some(Sequence {
                    records: vec![None; self.steps.len()],
                    match_id,
                });
            }
            let mut row = current.clone();
            for (column, expr) in &self.steps[index].assignments {
                row[*column] = Self::scalar(expr, &context)?;
            }
            let sequence = self.states[index]
                .as_mut()
                .ok_or_else(|| DataFusionError::Internal("scan has no active sequence".into()))?;
            sequence.records[index] = Some(Arc::new(row.clone()));
            if self.match_id {
                row.push(ScalarValue::Int64(Some(sequence.match_id)));
            }
            let ordinal = self
                .ordinal
                .checked_mul(self.steps.len() as i64)
                .and_then(|value| value.checked_add((self.steps.len() - 1 - index) as i64))
                .ok_or_else(|| DataFusionError::Execution("scan output order overflow".into()))?;
            row.push(ScalarValue::Int64(Some(ordinal)));
            match self.steps[index].output {
                ScanOutput::All => self.output.push(row),
                ScanOutput::Last => self.last[index] = Some(row),
                ScanOutput::None => {}
            }
        }
        self.account()
    }

    fn account(&self) -> Result<()> {
        let mut bytes = self
            .states
            .len()
            .saturating_mul(size_of::<Option<Sequence>>());
        for sequence in self.states.iter().flatten() {
            bytes = bytes.saturating_add(
                sequence
                    .records
                    .capacity()
                    .saturating_mul(size_of::<Option<Arc<Vec<ScalarValue>>>>()),
            );
            for row in sequence.records.iter().flatten() {
                bytes = bytes.saturating_add(row.iter().map(ScalarValue::size).sum::<usize>());
            }
        }
        for row in self.last.iter().flatten().chain(&self.output) {
            bytes = bytes.saturating_add(row.iter().map(ScalarValue::size).sum::<usize>());
        }
        self.reservation.try_resize(bytes)
    }

    fn batch(&mut self) -> Result<RecordBatch> {
        let count = self.output.len();
        let arrays = (0..self.schema.fields().len())
            .map(|index| {
                ScalarValue::iter_to_array(self.output.iter().map(|row| row[index].clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(count)),
        )?;
        self.output.clear();
        self.account()?;
        Ok(batch)
    }
}
