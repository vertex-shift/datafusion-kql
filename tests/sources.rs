use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result, TableReference};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::*;
use datafusion_kql::{SessionContextKqlExt, logical_plan, parse};

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("Value", DataType::Int64, false),
        Field::new("Category", DataType::Utf8, false),
        Field::new("Unused", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            Arc::new(StringArray::from(vec!["a", "b", "a", "b", "a"])),
            Arc::new(Int64Array::from(vec![0; 5])),
        ],
    )
    .unwrap()
}

#[derive(Debug)]
struct Scan {
    projection: Option<Vec<usize>>,
    filters: Vec<String>,
    limit: Option<usize>,
}

#[derive(Debug)]
struct ObservedTable {
    inner: MemTable,
    scans: AtomicUsize,
    observations: Mutex<Vec<Scan>>,
}

#[async_trait]
impl TableProvider for ObservedTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Delegate final filtering to DataFusion, like a lossy storage index.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        self.observations.lock().unwrap().push(Scan {
            projection: projection.cloned(),
            filters: filters.iter().map(ToString::to_string).collect(),
            limit,
        });
        self.inner.scan(state, projection, filters, limit).await
    }
}

#[tokio::test]
async fn planning_is_lazy_and_provider_pushdown_is_preserved() {
    let ctx = SessionContext::new();
    let batch = batch();
    let provider = Arc::new(ObservedTable {
        inner: MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap(),
        scans: AtomicUsize::new(0),
        observations: Mutex::new(Vec::new()),
    });
    ctx.register_table(TableReference::bare("Events"), provider.clone())
        .unwrap();
    let ast = parse("Events | where Value>20 | project Value | take 2").unwrap();
    let frame = ctx.kql_ast(&ast).await.unwrap();
    let plan = logical_plan(&ctx, &ast).await.unwrap();
    let optimized = frame.clone().into_optimized_plan().unwrap();
    assert_eq!(provider.scans.load(Ordering::SeqCst), 0);
    assert!(plan.display_indent().to_string().contains("TableScan"));
    assert!(
        optimized
            .display_indent()
            .to_string()
            .contains("partial_filters=")
    );
    let results = frame.collect().await.unwrap();
    assert_eq!(results.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert!(provider.scans.load(Ordering::SeqCst) > 0);
    let observations = provider.observations.lock().unwrap();
    let scan = observations.last().unwrap();
    assert_eq!(scan.projection, Some(vec![0]));
    assert_eq!(scan.filters.len(), 1);
    assert!(scan.filters[0].contains("Value > Int64(20)"), "{scan:?}");
    assert_eq!(
        scan.limit, None,
        "inexact filters must not get an unsafe limit pushdown"
    );
}

#[tokio::test]
async fn equivalent_queries_produce_identical_native_physical_plans() {
    let ctx = SessionContext::new();
    ctx.register_batch(TableReference::bare("Events"), batch())
        .unwrap();
    let kql = ctx
        .kql("Events | where Value>20 | summarize Total=sum(Value) by Category | top 10 by Total")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let sql = ctx.sql("SELECT \"Category\", coalesce(sum(\"Value\"),0) AS \"Total\" FROM \"Events\" WHERE \"Value\">20 GROUP BY \"Category\" ORDER BY \"Total\" DESC NULLS LAST LIMIT 10")
        .await.unwrap().create_physical_plan().await.unwrap();
    let native = displayable(kql.as_ref()).indent(false).to_string();
    assert_eq!(native, displayable(sql.as_ref()).indent(false).to_string());
    assert!(native.contains("AggregateExec"), "{native}");
}

#[cfg(feature = "parquet")]
#[tokio::test]
async fn parquet_filters_and_projections_reach_the_native_reader() {
    use datafusion::parquet::arrow::ArrowWriter;

    let file = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let batch = batch();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    let ctx = SessionContext::new_with_config(config);
    ctx.register_parquet(
        TableReference::bare("Events"),
        file.path().to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    let frame = ctx
        .kql("Events | where Value>20 | project Value | sort by Value asc")
        .await
        .unwrap();
    let physical = frame.clone().create_physical_plan().await.unwrap();
    let display = displayable(physical.as_ref()).indent(true).to_string();
    assert!(display.contains("projection=[Value]"), "{display}");
    assert!(display.contains("predicate=Value@0 > 20"), "{display}");
    let batches = frame.collect().await.unwrap();
    let actual = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![30, 40, 50]);
}

#[tokio::test]
async fn csv_source_works_without_a_new_storage_layer() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.csv");
    std::fs::write(&path, "Value,Category\n10,a\n20,b\n30,a\n").unwrap();
    let ctx = SessionContext::new();
    ctx.register_csv("events", path.to_str().unwrap(), CsvReadOptions::new())
        .await
        .unwrap();
    let batches = ctx
        .kql("events | where Value>10 | count")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 2);
}
