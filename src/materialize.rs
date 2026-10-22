use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
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
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, reset_plan_states};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream, execute_stream_partitioned,
};
use futures::{Stream, TryStreamExt};
use tokio::sync::OnceCell;
use tokio::task::JoinSet;

type Cache = Arc<OnceCell<Result<Arc<CachedData>, Arc<DataFusionError>>>>;

#[derive(Debug)]
struct CachedData {
    partitions: Vec<Vec<RecordBatch>>,
    _reservation: Arc<MemoryReservation>,
}

#[derive(Debug)]
struct MaterializedTable {
    input: LogicalPlan,
    cache: Cache,
}

pub(crate) fn plan(input: LogicalPlan, name: String) -> crate::Result<LogicalPlan> {
    let table = Arc::new(MaterializedTable {
        input,
        cache: Arc::new(OnceCell::new()),
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
            cache: Arc::new(OnceCell::new()),
            properties: Arc::clone(&materialized.properties),
        }) as Arc<dyn ExecutionPlan>)
    } else {
        Transformed::no(Arc::clone(plan))
    }))
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
                .get_or_init(|| async {
                    materialize(input, context)
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
) -> Result<CachedData> {
    let reservation =
        Arc::new(MemoryConsumer::new("KQL materialization").register(context.memory_pool()));
    let input = reset_plan_states(input)?;
    let streams = execute_stream_partitioned(input, context)?;
    let mut tasks = JoinSet::new();
    let mut partitions = vec![Vec::new(); streams.len()];
    for (index, mut stream) in streams.into_iter().enumerate() {
        let reservation = Arc::clone(&reservation);
        tasks.spawn(async move {
            let mut batches = Vec::new();
            while let Some(batch) = stream.try_next().await? {
                let bytes = batch
                    .get_array_memory_size()
                    .checked_add(size_of::<RecordBatch>())
                    .and_then(|bytes| {
                        bytes.checked_add(batch.num_columns().checked_mul(size_of::<ArrayRef>())?)
                    })
                    .ok_or_else(|| {
                        DataFusionError::ResourcesExhausted("materialization size overflow".into())
                    })?;
                reservation.try_grow(bytes)?;
                batches.push(batch);
            }
            Ok::<_, DataFusionError>((index, batches))
        });
    }
    while let Some(result) = tasks.join_next().await {
        let (index, batches) = result.map_err(|error| {
            DataFusionError::Execution(format!("materialize task failed: {error}"))
        })??;
        partitions[index] = batches;
    }
    Ok(CachedData {
        partitions,
        _reservation: reservation,
    })
}

struct CachedStream {
    data: Arc<CachedData>,
    schema: SchemaRef,
    partition: usize,
    index: usize,
    stride: usize,
}

impl Stream for CachedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while let Some(batches) = self.data.partitions.get(self.partition) {
            if let Some(batch) = batches.get(self.index).cloned() {
                self.index += 1;
                return Poll::Ready(Some(Ok(batch)));
            }
            self.partition += self.stride;
            self.index = 0;
        }
        Poll::Ready(None)
    }
}

impl RecordBatchStream for CachedStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
