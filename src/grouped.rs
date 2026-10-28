use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, UInt32Array};
use datafusion::arrow::compute::{SortOptions, partition as partition_ranges, take};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::datasource::provider_as_source;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::expr_fn::{cast, when};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, reset_plan_states};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet, SpillMetrics};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream, SpillManager, execute_stream,
};
use datafusion::prelude::lit;
use futures::{StreamExt, TryStreamExt};

use crate::ast::PartitionStrategy;
use crate::materialize::CachedData;

static NEXT_SLOT: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub(crate) enum Mode {
    Row,
    Key {
        index: usize,
        strategy: PartitionStrategy,
    },
}

pub(crate) fn input(schema: SchemaRef) -> crate::Result<(u64, String, LogicalPlan)> {
    let slot = NEXT_SLOT.fetch_add(1, Ordering::Relaxed);
    let name = format!("__kql_group_input_{slot}");
    let table = Arc::new(GroupTable { slot, schema });
    let plan = LogicalPlanBuilder::scan(name.clone(), provider_as_source(table), None)?.build()?;
    Ok((slot, name, plan))
}

pub(crate) fn plan(
    input: LogicalPlan,
    template: LogicalPlan,
    slot: u64,
    mode: Mode,
    restore: Vec<usize>,
    name: String,
) -> crate::Result<LogicalPlan> {
    let fields = restore
        .iter()
        .map(|&i| Arc::clone(input.schema().field(i)))
        .chain(template.schema().fields().iter().cloned())
        .collect::<Vec<_>>();
    let table = Arc::new(ApplyTable {
        schema: Arc::new(Schema::new(fields)),
        input,
        template,
        slot,
        mode,
        restore,
    });
    Ok(LogicalPlanBuilder::scan(name, provider_as_source(table), None)?.build()?)
}

#[derive(Debug)]
struct GroupTable {
    slot: u64,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for GroupTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = match projection {
            Some(projection) => Arc::new(self.schema.project(projection)?),
            None => Arc::clone(&self.schema),
        };
        Ok(Arc::new(GroupInputExec {
            slot: self.slot,
            input_schema: Arc::clone(&self.schema),
            projection: projection.cloned(),
            properties: properties(schema, 1),
            bound: None,
        }))
    }
}

fn properties(schema: SchemaRef, partitions: usize) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(partitions),
        EmissionType::Incremental,
        Boundedness::Bounded,
    ))
}

#[derive(Debug)]
struct GroupInputExec {
    slot: u64,
    input_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
    bound: Option<Arc<CachedData>>,
}

impl DisplayAs for GroupInputExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KqlGroupInputExec: slot={}", self.slot)
    }
}

impl ExecutionPlan for GroupInputExec {
    fn name(&self) -> &str {
        "KqlGroupInputExec"
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
            return Err(DataFusionError::Internal("group input is a leaf".into()));
        }
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let data = self
            .bound
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("unbound KQL group input".into()))?;
        let stream = data.replay(Arc::clone(&self.input_schema), 0, 1);
        if let Some(projection) = self.projection.clone() {
            let stream = stream.and_then(move |batch| {
                futures::future::ready(batch.project(&projection).map_err(DataFusionError::from))
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                stream,
            )))
        } else {
            Ok(stream)
        }
    }
}

#[derive(Debug)]
struct ApplyTable {
    input: LogicalPlan,
    template: LogicalPlan,
    slot: u64,
    mode: Mode,
    restore: Vec<usize>,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for ApplyTable {
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
        let mut input = state.create_physical_plan(&self.input).await?;
        if !matches!(input.boundedness(), Boundedness::Bounded) {
            return Err(DataFusionError::Plan(
                "grouped subqueries require bounded input".into(),
            ));
        }
        if let Mode::Key { index, strategy } = self.mode {
            let column: Arc<dyn PhysicalExpr> = if matches!(
                input.schema().field(index).data_type(),
                DataType::Float16 | DataType::Float32 | DataType::Float64
            ) {
                let value = cast(
                    Expr::Column(datafusion::common::Column::from(
                        self.input.schema().qualified_field(index),
                    )),
                    DataType::Float64,
                );
                let key = when(value.clone().eq(lit(0.0_f64)), lit(0.0_f64)).otherwise(value)?;
                state.create_physical_expr(key, self.input.schema())?
            } else {
                Arc::new(Column::new(input.schema().field(index).name(), index))
            };
            input = if strategy == PartitionStrategy::Legacy {
                Arc::new(CoalescePartitionsExec::new(input))
            } else {
                Arc::new(RepartitionExec::try_new(
                    input,
                    Partitioning::Hash(
                        vec![Arc::clone(&column)],
                        state.config().target_partitions(),
                    ),
                )?)
            };
            let keys =
                LexOrdering::new(vec![PhysicalSortExpr::new(column, SortOptions::default())])
                    .ok_or_else(|| {
                        DataFusionError::Internal("missing partition sort key".into())
                    })?;
            input = Arc::new(SortExec::new(keys, input).with_preserve_partitioning(true));
        }
        let template = state.create_physical_plan(&self.template).await?;
        let apply: Arc<dyn ExecutionPlan> = Arc::new(ApplyExec {
            properties: properties(self.schema(), input.output_partitioning().partition_count()),
            input,
            template,
            slot: self.slot,
            mode: self.mode.clone(),
            restore: self.restore.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        });
        project(apply, projection)
    }
}

fn project(
    input: Arc<dyn ExecutionPlan>,
    projection: Option<&Vec<usize>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(indices) = projection {
        let expressions = indices
            .iter()
            .map(|&index| {
                let name = input.schema().field(index).name().clone();
                (Arc::new(Column::new(&name, index)) as _, name)
            })
            .collect::<Vec<_>>();
        Ok(Arc::new(ProjectionExec::try_new(expressions, input)?))
    } else {
        Ok(input)
    }
}

#[derive(Debug)]
struct ApplyExec {
    input: Arc<dyn ExecutionPlan>,
    template: Arc<dyn ExecutionPlan>,
    slot: u64,
    mode: Mode,
    restore: Vec<usize>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for ApplyExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KqlApplyExec: mode={:?}", self.mode)
    }
}

impl ExecutionPlan for ApplyExec {
    fn name(&self) -> &str {
        "KqlApplyExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
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
            return Err(DataFusionError::Internal("grouped source is opaque".into()));
        }
        Ok(self)
    }
    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: reset_plan_states(Arc::clone(&self.input))?,
            template: Arc::clone(&self.template),
            slot: self.slot,
            mode: self.mode.clone(),
            restore: self.restore.clone(),
            properties: Arc::clone(&self.properties),
            metrics: self.metrics.clone(),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let spill_enabled = context.runtime_env().disk_manager.tmp_files_enabled();
        let reservation = MemoryConsumer::new("KQL grouped subquery")
            .with_can_spill(spill_enabled)
            .register(context.memory_pool());
        let ranges_reservation =
            MemoryConsumer::new("KQL grouped boundaries").register(context.memory_pool());
        let manager = SpillManager::new(
            context.runtime_env(),
            SpillMetrics::new(&self.metrics, partition),
            input.schema(),
        )
        .with_compression_type(
            context
                .session_config()
                .options()
                .execution
                .spill_compression,
        );
        let state = ApplyStream {
            input,
            pending: None,
            position: 0,
            ranges: Vec::new(),
            range_index: 0,
            mode: self.mode.clone(),
            template: Arc::clone(&self.template),
            slot: self.slot,
            restore: self.restore.clone(),
            context,
            schema: self.schema(),
            output: None,
            original: None,
            groups: 0,
            reservation,
            ranges_reservation,
            manager,
            spill_enabled,
            data: None,
        };
        let stream = futures::stream::try_unfold(state, |mut state| async move {
            loop {
                if let Some(output) = &mut state.output
                    && let Some(batch) = output.try_next().await?
                {
                    return Ok(Some((state.restore_columns(batch)?, state)));
                }
                state.output = None;
                state.original = None;
                state.data = None;
                let Some(data) = state.next_group().await? else {
                    return Ok(None);
                };
                state.groups += 1;
                if matches!(
                    state.mode,
                    Mode::Key {
                        strategy: PartitionStrategy::Legacy,
                        ..
                    }
                ) && state.groups > 64
                {
                    return Err(DataFusionError::Execution("legacy partition is limited to 64 keys; use hint.strategy=native or shuffle".into()));
                }
                let plan = bind(Arc::clone(&state.template), state.slot, &data)?;
                let plan = reset_plan_states(plan)?;
                state.data = Some(data);
                state.output = Some(execute_stream(plan, Arc::clone(&state.context))?);
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

fn bind(
    plan: Arc<dyn ExecutionPlan>,
    slot: u64,
    data: &Arc<CachedData>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(bind_changes(plan, slot, data, &mut HashMap::new())?.data)
}

fn bind_changes(
    plan: Arc<dyn ExecutionPlan>,
    slot: u64,
    data: &Arc<CachedData>,
    caches: &mut HashMap<usize, Arc<dyn ExecutionPlan>>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    plan.transform_up(|plan| {
        if let Some(input) = plan.downcast_ref::<GroupInputExec>()
            && input.slot == slot
        {
            return Ok(Transformed::yes(Arc::new(GroupInputExec {
                slot: input.slot,
                input_schema: Arc::clone(&input.input_schema),
                projection: input.projection.clone(),
                properties: Arc::clone(&input.properties),
                bound: Some(Arc::clone(data)),
            }) as Arc<dyn ExecutionPlan>));
        }
        if let Some(apply) = plan.downcast_ref::<ApplyExec>() {
            let input = bind_changes(Arc::clone(&apply.input), slot, data, caches)?;
            let template = bind_changes(Arc::clone(&apply.template), slot, data, caches)?;
            if !input.transformed && !template.transformed {
                return Ok(Transformed::no(plan));
            }
            return Ok(Transformed::yes(Arc::new(ApplyExec {
                input: input.data,
                template: template.data,
                slot: apply.slot,
                mode: apply.mode.clone(),
                restore: apply.restore.clone(),
                properties: Arc::clone(&apply.properties),
                metrics: apply.metrics.clone(),
            }) as Arc<dyn ExecutionPlan>));
        }
        if let Some(id) = crate::materialize::cache_id(&plan) {
            if let Some(cached) = caches.get(&id) {
                return Ok(Transformed::yes(Arc::clone(cached)));
            }
            if let Some(bound) = crate::materialize::bind_input(&plan, |input| {
                bind_changes(input, slot, data, caches)
            })? {
                if bound.transformed {
                    caches.insert(id, Arc::clone(&bound.data));
                }
                return Ok(bound);
            }
        }
        if let Some(bound) =
            crate::scan::bind_input(&plan, |input| bind_changes(input, slot, data, caches))?
        {
            return Ok(bound);
        }
        Ok(Transformed::no(plan))
    })
}

struct ApplyStream {
    input: SendableRecordBatchStream,
    pending: Option<RecordBatch>,
    position: usize,
    ranges: Vec<Range<usize>>,
    range_index: usize,
    mode: Mode,
    template: Arc<dyn ExecutionPlan>,
    slot: u64,
    restore: Vec<usize>,
    context: Arc<TaskContext>,
    schema: SchemaRef,
    output: Option<SendableRecordBatchStream>,
    original: Option<RecordBatch>,
    groups: usize,
    reservation: MemoryReservation,
    ranges_reservation: MemoryReservation,
    manager: SpillManager,
    spill_enabled: bool,
    data: Option<Arc<CachedData>>,
}

impl ApplyStream {
    async fn next_group(&mut self) -> Result<Option<Arc<CachedData>>> {
        let mut key = None;
        let Some(first) = self.next_segment(&mut key).await? else {
            return Ok(None);
        };
        self.original = if self.restore.is_empty() {
            None
        } else {
            Some(first.project(&self.restore)?.slice(0, 1))
        };
        let reservation = self.reservation.new_empty();
        let manager = self.manager.clone();
        let batch_size = self.context.session_config().batch_size();
        let spill_enabled = self.spill_enabled;
        let row = matches!(self.mode, Mode::Row);
        let rest = futures::stream::try_unfold((self, key), move |(state, mut key)| async move {
            if row {
                return Ok(None);
            }
            Ok(state
                .next_segment(&mut key)
                .await?
                .map(|batch| (batch, (state, key))))
        });
        let stream = futures::stream::once(futures::future::ready(Ok(first))).chain(rest);
        crate::materialize::buffer_input(stream, reservation, manager, batch_size, spill_enabled)
            .await
            .map(Some)
    }

    async fn next_segment(&mut self, key: &mut Option<ScalarValue>) -> Result<Option<RecordBatch>> {
        loop {
            if self
                .pending
                .as_ref()
                .is_none_or(|b| self.position >= b.num_rows())
            {
                self.pending = self.input.try_next().await?;
                self.position = 0;
                self.range_index = 0;
                self.ranges.clear();
                if let Some(batch) = &self.pending
                    && let Mode::Key { index, .. } = self.mode
                {
                    self.ranges = partition_ranges(&[Arc::clone(batch.column(index))])?.ranges();
                }
                self.ranges_reservation.try_resize(
                    self.ranges
                        .capacity()
                        .checked_mul(size_of::<Range<usize>>())
                        .ok_or_else(|| {
                            DataFusionError::ResourcesExhausted(
                                "grouped boundary size overflow".into(),
                            )
                        })?,
                )?;
            }
            let Some(batch) = &self.pending else {
                return Ok(None);
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let start = self.position;
            if let Mode::Key { index, .. } = self.mode {
                let current =
                    canonical_key(ScalarValue::try_from_array(batch.column(index), start)?);
                if key.as_ref().is_some_and(|key| key != &current) {
                    return Ok(None);
                }

                fn canonical_key(value: ScalarValue) -> ScalarValue {
                    let normalize = |value: f64| {
                        if value == 0.0 { 0.0 } else { value }
                    };
                    match value {
                        ScalarValue::Float16(value) => {
                            ScalarValue::Float64(value.map(|value| normalize(value.to_f64())))
                        }
                        ScalarValue::Float32(value) => {
                            ScalarValue::Float64(value.map(|value| normalize(f64::from(value))))
                        }
                        ScalarValue::Float64(value) => ScalarValue::Float64(value.map(normalize)),
                        value => value,
                    }
                }
                *key = Some(current);
                let range = self.ranges.get(self.range_index).ok_or_else(|| {
                    DataFusionError::Internal("missing grouped key boundary".into())
                })?;
                self.position = range.end;
                self.range_index += 1;
            } else {
                self.position += 1;
            }
            let segment = batch.slice(start, self.position - start);
            return Ok(Some(segment));
        }
    }

    fn restore_columns(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if self.restore.is_empty() {
            return Ok(batch);
        }
        let original = self
            .original
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing original mv-apply row".into()))?;
        let indices = UInt32Array::from_value(0, batch.num_rows());
        let mut arrays = original
            .columns()
            .iter()
            .map(|array| take(array.as_ref(), &indices, None).map_err(DataFusionError::from))
            .collect::<Result<Vec<ArrayRef>>>()?;
        arrays.extend(batch.columns().iter().cloned());
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?)
    }
}
