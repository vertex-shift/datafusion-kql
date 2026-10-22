use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{DataFusionError, TableReference};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

fn context(pool: Option<Arc<dyn MemoryPool>>, fail: bool) -> (SessionContext, Arc<AtomicUsize>) {
    let runtime = RuntimeEnvBuilder::new();
    let runtime = if let Some(pool) = pool {
        runtime.with_memory_pool(pool)
    } else {
        runtime
    };
    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::new().with_target_partitions(4),
        Arc::new(runtime.build().unwrap()),
    );
    let batch = RecordBatch::try_from_iter(vec![(
        "x",
        Arc::new(Int64Array::from_iter_values(1..=20)) as _,
    )])
    .unwrap();
    ctx.register_table(
        TableReference::bare("Events"),
        Arc::new(
            MemTable::try_new(
                batch.schema(),
                (0..4).map(|i| vec![batch.slice(i * 5, 5)]).collect(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    ctx.register_udf(create_udf(
        "observe",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(move |args| {
            let rows = match &args[0] {
                ColumnarValue::Array(array) => array.len(),
                ColumnarValue::Scalar(_) => 1,
            };
            counter.fetch_add(rows, Ordering::SeqCst);
            if fail {
                Err(DataFusionError::Execution("observed input failure".into()))
            } else {
                Ok(args[0].clone())
            }
        }),
    ));
    (ctx, calls)
}

async fn rows(frame: DataFrame) -> Vec<Vec<String>> {
    frame
        .collect()
        .await
        .unwrap()
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column
                            .logical_nulls()
                            .is_some_and(|nulls| nulls.is_null(row))
                        {
                            "<null>".into()
                        } else {
                            array_value_to_string(column, row).unwrap()
                        }
                    })
                    .collect()
            })
        })
        .collect()
}

const FORK: &str = "union (T | where y<=10 | summarize Total=sum(y)),(T | where y>10 | summarize Total=sum(y)) | sort by Total asc";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialization_is_lazy_and_fork_legs_execute_input_once() {
    let (ctx, calls) = context(None, false);
    let frame = ctx
        .kql(&format!(
            "let T=materialize(Events | project y=observe(x)); {FORK}"
        ))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let physical = frame.clone().create_physical_plan().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let result = collect(physical, ctx.task_ctx()).await.unwrap();
    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 20);
    assert_eq!(rows(frame).await, vec![vec!["55"], vec!["155"]]);
    assert_eq!(calls.load(Ordering::SeqCst), 20);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_let_bindings_remain_nonmaterialized() {
    let (ctx, calls) = context(None, false);
    let frame = ctx
        .kql(&format!("let T=Events | project y=observe(x); {FORK}"))
        .await
        .unwrap();
    assert_eq!(rows(frame).await, vec![vec!["55"], vec!["155"]]);
    assert_eq!(calls.load(Ordering::SeqCst), 40);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_consumers_share_one_initialization() {
    let (ctx, calls) = context(None, false);
    let frame = ctx
        .kql("materialize(Events | project y=observe(x)) | summarize Total=sum(y)")
        .await
        .unwrap();
    let (left, right) = tokio::join!(rows(frame.clone()), rows(frame));
    assert_eq!(left, vec![vec!["210"]]);
    assert_eq!(right, left);
    assert_eq!(calls.load(Ordering::SeqCst), 20);
}

#[tokio::test]
async fn unused_materializations_never_execute() {
    let (ctx, calls) = context(None, false);
    let frame = ctx
        .kql("let T=materialize(Events | project y=observe(x)); print x=1")
        .await
        .unwrap();
    assert_eq!(rows(frame).await, vec![vec!["1"]]);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_queries_have_new_snapshots_and_frame_clones_share_their_snapshot() {
    let (ctx, calls) = context(None, false);
    let query = "let T=materialize(Events | project y=observe(x)); T | summarize Total=sum(y)";
    let frame = ctx.kql(query).await.unwrap();
    assert_eq!(rows(frame.clone()).await, vec![vec!["210"]]);
    assert_eq!(rows(frame).await, vec![vec!["210"]]);
    assert_eq!(calls.load(Ordering::SeqCst), 20);
    assert_eq!(rows(ctx.kql(query).await.unwrap()).await, vec![vec!["210"]]);
    assert_eq!(calls.load(Ordering::SeqCst), 40);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_materializations_share_the_captured_inner_source() {
    let (ctx, calls) = context(None, false);
    let frame = ctx.kql("let A=materialize(Events | project y=observe(x)); let B=materialize(A | where y<=10); union (A | summarize Total=sum(y)),(B | summarize Total=sum(y)) | sort by Total asc").await.unwrap();
    assert_eq!(rows(frame).await, vec![vec!["55"], vec!["210"]]);
    assert_eq!(calls.load(Ordering::SeqCst), 20);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialized_as_reuses_the_same_snapshot() {
    let (ctx, calls) = context(None, false);
    let frame = ctx.kql("Events | project y=observe(x) | as hint.materialized=true T | union T | summarize Total=sum(y)").await.unwrap();
    assert_eq!(rows(frame).await, vec![vec!["420"]]);
    assert_eq!(calls.load(Ordering::SeqCst), 20);
    let frame = ctx.kql("Events | project y=observe(x) | as hint.materialized=false T | union T | summarize Total=sum(y)").await.unwrap();
    assert_eq!(rows(frame).await, vec![vec!["420"]]);
    assert_eq!(calls.load(Ordering::SeqCst), 60);
}

#[tokio::test]
async fn schemas_dynamic_tags_and_serialized_ordinals_are_preserved() {
    let ctx = SessionContext::new();
    let frame = ctx.kql("let T=materialize(datatable(x:long,d:dynamic)[2,dynamic([2]),1,dynamic([1])] | sort by x asc | serialize n=row_number()); T | project x,n,d=tostring(d)").await.unwrap();
    assert_eq!(
        rows(frame).await,
        vec![vec!["1", "1", "[1]"], vec!["2", "2", "[2]"]]
    );
    let frame = ctx
        .kql("let T=materialize(range x from 1 to 3 step 1 | project-away *); T | count")
        .await
        .unwrap();
    assert_eq!(rows(frame).await, vec![vec!["3"]]);
}

#[tokio::test]
async fn cache_allocations_are_charged_and_released_with_the_snapshot() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let (ctx, _) = context(Some(Arc::clone(&pool)), false);
    let frame = ctx
        .kql("let T=materialize(Events); T | count")
        .await
        .unwrap();
    assert_eq!(rows(frame.clone()).await, vec![vec!["20"]]);
    assert!(pool.reserved() > 0);
    drop(frame);
    tokio::task::yield_now().await;
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn cache_exhaustion_is_an_explicit_query_error() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64));
    let (ctx, _) = context(Some(Arc::clone(&pool)), false);
    let frame = ctx.kql("materialize(Events)").await.unwrap();
    let error = frame.collect().await.unwrap_err().to_string();
    assert!(error.contains("KQL materialization"), "{error}");
    tokio::task::yield_now().await;
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn input_errors_are_shared_instead_of_becoming_empty_results() {
    let (ctx, calls) = context(None, true);
    let frame = ctx
        .kql("materialize(Events | project y=observe(x))")
        .await
        .unwrap();
    let error = frame.clone().collect().await.unwrap_err().to_string();
    assert!(error.contains("observed input failure"), "{error}");
    tokio::task::yield_now().await;
    let count = calls.load(Ordering::SeqCst);
    assert!(
        frame
            .collect()
            .await
            .unwrap_err()
            .to_string()
            .contains("observed input failure")
    );
    assert_eq!(calls.load(Ordering::SeqCst), count);
}

#[tokio::test]
async fn materialization_is_available_inside_tabular_functions() {
    let (ctx, _) = context(None, false);
    let frame = ctx
        .kql("let f=(){materialize(Events)}; f() | count")
        .await
        .unwrap();
    assert_eq!(rows(frame).await, vec![vec!["20"]]);
}

#[tokio::test]
async fn materialize_rejects_scalar_and_invalid_arguments() {
    let (ctx, _) = context(None, false);
    for query in [
        "materialize()",
        "materialize(Events,Events)",
        "materialize(x=Events)",
        "materialize(1)",
        "Events | as hint.materialized=maybe T",
        "let T=1; Events | as hint.materialized=true T",
        "let T=(){1}; Events | as T",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}
