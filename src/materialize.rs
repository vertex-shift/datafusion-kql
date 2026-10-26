use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::provider_as_source;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::spill_file::SpillFile;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, reset_plan_states};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet, SpillMetrics};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream, SpillManager,
    execute_stream_partitioned,
};
use futures::{Stream, TryStreamExt};
use tokio::sync::OnceCell;
use tokio::task::JoinSet;

type Cache = Arc<Snapshot>;

#[derive(Debug)]
struct Snapshot {
    data: OnceCell<Result<Arc<CachedData>, Arc<DataFusionError>>>,
    metrics: ExecutionPlanMetricsSet,
}

impl Snapshot {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            data: OnceCell::new(),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

#[derive(Debug)]
struct MemoryPartition {
    batches: Vec<RecordBatch>,
    reservation: MemoryReservation,
}

enum CachedPartition {
    Memory(MemoryPartition),
    Spill {
        file: Arc<dyn SpillFile>,
        manager: SpillManager,
        rows: usize,
    },
}

impl fmt::Debug for CachedPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory(memory) => memory.fmt(f),
            Self::Spill { file, rows, .. } => f
                .debug_struct("SpilledPartition")
                .field("bytes", &file.size())
                .field("rows", rows)
                .finish(),
        }
    }
}

#[derive(Debug)]
struct CachedData {
    partitions: Vec<CachedPartition>,
}

#[derive(Debug)]
struct MaterializedTable {
    input: LogicalPlan,
    cache: Cache,
}

pub(crate) fn plan(input: LogicalPlan, name: String) -> crate::Result<LogicalPlan> {
    let table = Arc::new(MaterializedTable {
        input,
        cache: Snapshot::new(),
    });
    Ok(LogicalPlanBuilder::scan(name, provider_as_source(table), None)?.build()?)
}

#[async_trait]
impl TableProvider for MaterializedTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(self.input.schema().inner())
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
            return Err(DataFusionError::Plan(
                "materialize requires a bounded input".into(),
            ));
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(self.schema()),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        let cached: Arc<dyn ExecutionPlan> = Arc::new(MaterializeExec {
            input,
            cache: Arc::clone(&self.cache),
            properties,
        });
        if let Some(indices) = projection {
            let expressions = indices
                .iter()
                .map(|&index| {
                    let name = self.input.schema().field(index).name().clone();
                    (Arc::new(Column::new(&name, index)) as _, name)
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(ProjectionExec::try_new(expressions, cached)?))
        } else {
            Ok(cached)
        }
    }
}

#[derive(Debug)]
struct MaterializeExec {
    input: Arc<dyn ExecutionPlan>,
    cache: Cache,
    properties: Arc<PlanProperties>,
}

pub(crate) fn bind_input(
    plan: &Arc<dyn ExecutionPlan>,
    bind: impl FnOnce(Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>>,
) -> Result<Option<Transformed<Arc<dyn ExecutionPlan>>>> {
    let Some(materialized) = plan.downcast_ref::<MaterializeExec>() else {
        return Ok(None);
    };
    let input = bind(Arc::clone(&materialized.input))?;
    Ok(Some(if input.transformed {
        Transformed::yes(Arc::new(MaterializeExec {
            input: input.data,
            cache: Snapshot::new(),
            properties: Arc::clone(&materialized.properties),
        }) as Arc<dyn ExecutionPlan>)
    } else {
        Transformed::no(Arc::clone(plan))
    }))
}

pub(crate) fn cache_id(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    plan.downcast_ref::<MaterializeExec>()
        .map(|plan| Arc::as_ptr(&plan.cache) as usize)
}

impl DisplayAs for MaterializeExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KqlMaterializeExec")
    }
}

impl ExecutionPlan for MaterializeExec {
    fn name(&self) -> &str {
        "KqlMaterializeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.cache.metrics.clone_inner())
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // This is a cache boundary; per-consumer rewrites must not alter its input.
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
            return Err(DataFusionError::Internal(
                "materialization is an opaque source".into(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.properties.output_partitioning().partition_count() {
            return Err(DataFusionError::Execution(
                "invalid materialization partition".into(),
            ));
        }
        let input = Arc::clone(&self.input);
        let cache = Arc::clone(&self.cache);
        let schema = self.schema();
        let output_schema = Arc::clone(&schema);
        let stride = self.properties.output_partitioning().partition_count();
        let future = futures::stream::once(async move {
            let data = cache
                .data
                .get_or_init(|| async {
                    materialize(input, context, cache.metrics.clone())
                        .await
                        .map(Arc::new)
                        .map_err(Arc::new)
                })
                .await
                .as_ref()
                .map_err(|error| DataFusionError::Shared(Arc::clone(error)))?;
            Ok::<_, DataFusionError>(CachedStream {
                data: Arc::clone(data),
                schema,
                partition,
                index: 0,
                stride,
                active: None,
                read_rows: 0,
                finished: false,
            })
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            future,
        )))
    }
}

async fn materialize(
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    metrics: ExecutionPlanMetricsSet,
) -> Result<CachedData> {
    let schema = input.schema();
    let input = reset_plan_states(input)?;
    let streams = execute_stream_partitioned(input, Arc::clone(&context))?;
    let spill_enabled = context.runtime_env().disk_manager.tmp_files_enabled();
    let batch_size = context.session_config().batch_size();
    let spilling = Arc::new(AtomicBool::new(false));
    let managers = (0..streams.len())
        .map(|partition| {
            SpillManager::new(
                context.runtime_env(),
                SpillMetrics::new(&metrics, partition),
                Arc::clone(&schema),
            )
            .with_compression_type(
                context
                    .session_config()
                    .options()
                    .execution
                    .spill_compression,
            )
        })
        .collect::<Vec<_>>();
    let mut tasks = JoinSet::new();
    let mut partitions = (0..streams.len()).map(|_| None).collect::<Vec<_>>();
    for (index, mut stream) in streams.into_iter().enumerate() {
        let reservation = MemoryConsumer::new(format!("KQL materialization[{index}]"))
            .with_can_spill(spill_enabled)
            .register(context.memory_pool());
        let spilling = Arc::clone(&spilling);
        let manager = managers[index].clone();
        tasks.spawn(async move {
            let mut batches = Vec::new();
            while let Some(batch) = stream.try_next().await? {
                if batch.num_rows() == 0 {
                    continue;
                }
                let previous_capacity = batches.capacity();
                batches.try_reserve(1).map_err(|error| {
                    DataFusionError::ResourcesExhausted(format!(
                        "materialization batch bookkeeping allocation failed: {error}"
                    ))
                })?;
                let spill = if spilling.load(Ordering::Relaxed) {
                    true
                } else {
                    match reservation.try_grow(batch_size_bytes(
                        &batch,
                        batches.capacity() - previous_capacity,
                    )?) {
                        Ok(()) => false,
                        Err(DataFusionError::ResourcesExhausted(_)) if spill_enabled => {
                            spilling.store(true, Ordering::Relaxed);
                            true
                        }
                        Err(error) => return Err(error),
                    }
                };
                batches.push(batch);
                if spill {
                    let partition = spill_partition(
                        MemoryPartition {
                            batches,
                            reservation,
                        },
                        Some(stream),
                        manager,
                        batch_size,
                    )
                    .await?;
                    return Ok::<_, DataFusionError>((index, partition));
                }
            }
            Ok((
                index,
                CachedPartition::Memory(MemoryPartition {
                    batches,
                    reservation,
                }),
            ))
        });
    }
    while let Some(result) = tasks.join_next().await {
        let result = result
            .map_err(|error| {
                DataFusionError::Execution(format!("materialize task failed: {error}"))
            })
            .and_then(|result| result);
        match result {
            Ok((index, partition)) => partitions[index] = Some(partition),
            Err(error) => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                return Err(error);
            }
        }
    }
    let partitions = partitions
        .into_iter()
        .map(|partition| {
            partition.ok_or_else(|| {
                DataFusionError::Internal("missing materialization partition".into())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut completed = Vec::with_capacity(partitions.len());
    for (partition, manager) in partitions.into_iter().zip(managers) {
        // Completed partitions can have filled memory before another partition
        // triggered spill. Release those buffers too before replay starts.
        completed.push(match partition {
            CachedPartition::Memory(memory)
                if spilling.load(Ordering::Relaxed) && !memory.batches.is_empty() =>
            {
                spill_partition(memory, None, manager, batch_size).await?
            }
            partition => partition,
        });
    }
    Ok(CachedData {
        partitions: completed,
    })
}

fn batch_size_bytes(batch: &RecordBatch, additional_slots: usize) -> Result<usize> {
    batch
        .get_array_memory_size()
        .checked_add(
            additional_slots
                .checked_mul(size_of::<RecordBatch>())
                .ok_or_else(|| {
                    DataFusionError::ResourcesExhausted(
                        "materialization bookkeeping size overflow".into(),
                    )
                })?,
        )
        .and_then(|bytes| {
            bytes.checked_add(batch.num_columns().checked_mul(size_of::<ArrayRef>())?)
        })
        .ok_or_else(|| DataFusionError::ResourcesExhausted("materialization size overflow".into()))
}

async fn blocking<T: Send + 'static>(
    action: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(action).await.map_err(|error| {
        DataFusionError::Execution(format!("materialize spill task failed: {error}"))
    })?
}

async fn spill_partition(
    memory: MemoryPartition,
    mut input: Option<SendableRecordBatchStream>,
    manager: SpillManager,
    batch_size: usize,
) -> Result<CachedPartition> {
    let writer_manager = manager.clone();
    let (mut writer, mut rows) = blocking(move || {
        let mut writer = writer_manager.create_in_progress_file("KQL materialization")?;
        let mut rows = 0_usize;
        for batch in memory.batches {
            rows = rows.checked_add(batch.num_rows()).ok_or_else(|| {
                DataFusionError::ResourcesExhausted("materialization row count overflow".into())
            })?;
            for offset in (0..batch.num_rows()).step_by(batch_size) {
                writer.append_batch(
                    &batch.slice(offset, batch_size.min(batch.num_rows() - offset)),
                )?;
            }
        }
        drop(memory.reservation);
        Ok((writer, rows))
    })
    .await?;
    if let Some(input) = input.as_mut() {
        while let Some(batch) = input.try_next().await? {
            rows = rows.checked_add(batch.num_rows()).ok_or_else(|| {
                DataFusionError::ResourcesExhausted("materialization row count overflow".into())
            })?;
            writer = blocking(move || {
                for offset in (0..batch.num_rows()).step_by(batch_size) {
                    writer.append_batch(
                        &batch.slice(offset, batch_size.min(batch.num_rows() - offset)),
                    )?;
                }
                Ok(writer)
            })
            .await?;
        }
    }
    let file = blocking(move || writer.finish())
        .await?
        .ok_or_else(|| DataFusionError::Internal("materialization spill has no batches".into()))?;
    Ok(CachedPartition::Spill {
        file,
        manager,
        rows,
    })
}

struct CachedStream {
    data: Arc<CachedData>,
    schema: SchemaRef,
    partition: usize,
    index: usize,
    stride: usize,
    active: Option<SendableRecordBatchStream>,
    read_rows: usize,
    finished: bool,
}

impl Stream for CachedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        loop {
            if let Some(active) = self.active.as_mut() {
                match futures::ready!(active.as_mut().poll_next(cx)) {
                    Some(Ok(batch)) => {
                        let expected = match &self.data.partitions[self.partition] {
                            CachedPartition::Spill { rows, .. } => *rows,
                            _ => unreachable!(),
                        };
                        if batch.num_rows() > expected - self.read_rows {
                            self.finished = true;
                            return Poll::Ready(Some(Err(DataFusionError::Execution(
                                "materialization spill contains excess rows".into(),
                            ))));
                        }
                        self.read_rows += batch.num_rows();
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Some(Err(error)) => {
                        self.finished = true;
                        return Poll::Ready(Some(Err(error)));
                    }
                    None => {
                        let expected = match &self.data.partitions[self.partition] {
                            CachedPartition::Spill { rows, .. } => *rows,
                            _ => unreachable!(),
                        };
                        if self.read_rows != expected {
                            self.finished = true;
                            return Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                                "materialization spill ended after {} rows, expected {expected}",
                                self.read_rows,
                            )))));
                        }
                        self.active = None;
                        self.partition += self.stride;
                        self.read_rows = 0;
                    }
                }
            }
            match self.data.partitions.get(self.partition) {
                Some(CachedPartition::Memory(memory)) => {
                    if let Some(batch) = memory.batches.get(self.index).cloned() {
                        self.index += 1;
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    self.partition += self.stride;
                    self.index = 0;
                }
                Some(CachedPartition::Spill { manager, file, .. }) => {
                    match manager.read_spill_as_stream_unbuffered(Arc::clone(file), None) {
                        Ok(active) => self.active = Some(active),
                        Err(error) => {
                            self.finished = true;
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                }
                None => {
                    self.finished = true;
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for CachedStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_an_unfinished_spill_releases_its_files_and_reservation() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()
                .unwrap(),
        );
        let batch = RecordBatch::try_from_iter(vec![(
            "x",
            Arc::new(Int64Array::from_iter_values(0..4096)) as ArrayRef,
        )])
        .unwrap();
        let schema = batch.schema();
        let reservation = MemoryConsumer::new("cancellation fixture").register(&pool);
        reservation
            .try_grow(batch_size_bytes(&batch, 1).unwrap())
            .unwrap();
        let manager = SpillManager::new(
            Arc::clone(&runtime),
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            Arc::clone(&schema),
        );
        let input = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::pending(),
        ));
        let task = tokio::spawn(spill_partition(
            MemoryPartition {
                batches: vec![batch],
                reservation,
            },
            Some(input),
            manager,
            128,
        ));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while runtime.disk_manager.used_disk_space() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let disk = runtime.disk_manager.spilling_progress();
                if pool.reserved() == 0 && disk.current_bytes == 0 && disk.active_files_count == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
