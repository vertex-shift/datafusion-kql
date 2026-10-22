use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::datasource::provider_as_source;
use datafusion::execution::TaskContext;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, TableType};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, reset_plan_states};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, collect, execute_stream,
};
use futures::TryStreamExt;

use crate::ast::Span;
use crate::expressions::Expressions;

pub(crate) fn plan(
    state: &SessionState,
    name: &str,
    args: Vec<Expr>,
    span: Span,
) -> crate::Result<LogicalPlan> {
    let types = args
        .iter()
        .map(|expr| expr.get_type(&DFSchema::empty()))
        .collect::<Result<Vec<_>>>()?;
    let ty = if matches!(types[0], DataType::Timestamp(_, _))
        && matches!(types[1], DataType::Timestamp(_, _))
        && matches!(types[2], DataType::Duration(_))
    {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    } else if types.iter().all(|ty| matches!(ty, DataType::Duration(_))) {
        DataType::Duration(TimeUnit::Nanosecond)
    } else if types.iter().all(DataType::is_integer) {
        DataType::Int64
    } else if types.iter().all(DataType::is_numeric) {
        DataType::Float64
    } else {
        return Err(crate::Error::syntax(
            "range requires compatible numeric or temporal arguments",
            span,
        ));
    };
    let values = LogicalPlanBuilder::empty(true)
        .project(
            args.into_iter()
                .enumerate()
                .map(|(i, expr)| expr.alias(format!("argument{i}"))),
        )?
        .build()?;
    let table = Arc::new(DeferredRange {
        state: state.clone(),
        values,
        name: name.to_owned(),
        schema: Arc::new(Schema::new(vec![Field::new(name, ty, false)])),
    });
    Ok(
        LogicalPlanBuilder::scan("__kql_deferred_range", provider_as_source(table), None)?
            .build()?,
    )
}

#[derive(Debug)]
struct DeferredRange {
    state: SessionState,
    values: LogicalPlan,
    name: String,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for DeferredRange {
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
        let arguments = state.create_physical_plan(&self.values).await?;
        let plan: Arc<dyn ExecutionPlan> = Arc::new(DeferredRangeExec {
            state: self.state.clone(),
            arguments,
            name: self.name.clone(),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        });
        if let Some(indices) = projection {
            let expressions = indices
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
struct DeferredRangeExec {
    state: SessionState,
    arguments: Arc<dyn ExecutionPlan>,
    name: String,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for DeferredRangeExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KqlDeferredRangeExec")
    }
}

impl ExecutionPlan for DeferredRangeExec {
    fn name(&self) -> &str {
        "KqlDeferredRangeExec"
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
            return Err(DataFusionError::Internal("deferred range is opaque".into()));
        }
        Ok(self)
    }
    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            state: self.state.clone(),
            arguments: reset_plan_states(Arc::clone(&self.arguments))?,
            name: self.name.clone(),
            properties: Arc::clone(&self.properties),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "deferred range has one partition".into(),
            ));
        }
        let arguments = Arc::clone(&self.arguments);
        let state = self.state.clone();
        let name = self.name.clone();
        let stream = futures::stream::once(async move {
            let batches = collect(arguments, Arc::clone(&context)).await?;
            let batch = batches
                .iter()
                .find(|batch| batch.num_rows() > 0)
                .ok_or_else(|| {
                    DataFusionError::Execution("range arguments produced no row".into())
                })?;
            let values = batch
                .columns()
                .iter()
                .map(|column| ScalarValue::try_from_array(column, 0))
                .collect::<Result<Vec<_>>>()?;
            if values.iter().any(ScalarValue::is_null) {
                return Err(DataFusionError::Execution(
                    "range bounds and step must be non-null".into(),
                ));
            }
            let expressions = Expressions {
                state: &state,
                scalars: &HashMap::new(),
                now: 0,
            };
            let plan = crate::ranges::value_plan(&expressions, &name, values, Span::default())
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let plan = state.create_physical_plan(&plan).await?;
            execute_stream(plan, context)
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
