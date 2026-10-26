use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int64Array, ListArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::{Int8Type, Int64Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{DataFusionError, TableReference};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;
use futures::TryStreamExt;

fn context(pool: Option<Arc<dyn MemoryPool>>, fail: bool) -> (SessionContext, Arc<AtomicUsize>) {
    let runtime = RuntimeEnvBuilder::new().with_disk_manager_builder(
        DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
    );
    let runtime = if let Some(pool) = pool {
        runtime.with_memory_pool(pool)
    } else {
        runtime
    };
    configured_context(
        runtime,
        SessionConfig::new().with_target_partitions(4),
        fail,
    )
}

fn configured_context(
    runtime: RuntimeEnvBuilder,
    config: SessionConfig,
    fail: bool,
) -> (SessionContext, Arc<AtomicUsize>) {
    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime.build().unwrap()));
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

fn large_table(ctx: &SessionContext, rows: usize, partitions: usize) {
    let mut batches = vec![Vec::new(); partitions];
    for (index, start) in (0..rows).step_by(128).enumerate() {
        let batch = RecordBatch::try_from_iter(vec![(
            "x",
            Arc::new(Int64Array::from_iter_values(
                (start..rows.min(start + 128)).map(|i| i as i64),
            )) as _,
        )])
        .unwrap();
        batches[index % partitions].push(batch);
    }
    let schema = batches[0][0].schema();
    ctx.register_table(
        TableReference::bare("Large"),
        Arc::new(MemTable::try_new(schema, batches).unwrap()),
    )
    .unwrap();
}

fn spill_count(plan: &dyn ExecutionPlan) -> usize {
    plan.metrics()
        .and_then(|metrics| metrics.spill_count())
        .unwrap_or(0)
        + plan
            .children()
            .into_iter()
            .map(|child| spill_count(child.as_ref()))
            .sum::<usize>()
}

fn spilling_context(
    memory: usize,
    disk: DiskManagerBuilder,
    partitions: usize,
) -> (SessionContext, Arc<AtomicUsize>, Arc<dyn MemoryPool>) {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(memory));
    let mut config = SessionConfig::new()
        .with_target_partitions(partitions)
        .with_batch_size(128);
    config.options_mut().execution.sort_spill_reservation_bytes = 4 << 10;
    let (ctx, calls) = configured_context(
        RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .with_disk_manager_builder(disk),
        config,
        false,
    );
    (ctx, calls, pool)
}

async fn released(ctx: &SessionContext, pool: &Arc<dyn MemoryPool>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let disk = ctx.runtime_env().disk_manager.spilling_progress();
            if pool.reserved() == 0 && disk.current_bytes == 0 && disk.active_files_count == 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("snapshot resources must be released after cancellation/failure/drop");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn low_memory_snapshots_spill_once_and_replay_for_concurrent_consumers() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 << 10));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_max_temp_directory_size(8 << 20),
        );
    let (ctx, calls) = configured_context(
        runtime,
        SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(128),
        false,
    );
    large_table(&ctx, 20_000, 4);
    let frame=ctx.kql("let T=materialize(Large | project y=observe(x)); union (T | where y<10000 | summarize Total=sum(y)),(T | where y>=10000 | summarize Total=sum(y)) | sort by Total asc").await.unwrap();
    let disk = Arc::clone(&ctx.runtime_env().disk_manager);
    let physical = frame.clone().create_physical_plan().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(disk.used_disk_space(), 0);
    let result = collect(Arc::clone(&physical), ctx.task_ctx())
        .await
        .unwrap();
    let actual = result
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| array_value_to_string(batch.column(0), row).unwrap())
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, ["49995000", "149995000"]);
    assert_eq!(calls.load(Ordering::SeqCst), 20_000);
    assert!(spill_count(physical.as_ref()) > 0);
    assert!(disk.used_disk_space() > 0);
    assert_eq!(disk.spilling_progress().active_files_count, 4);
    let bytes = disk.used_disk_space();
    let (left, right) = tokio::join!(rows(frame.clone()), rows(frame.clone()));
    assert_eq!(left, vec![vec!["49995000"], vec!["149995000"]]);
    assert_eq!(right, left);
    assert_eq!(calls.load(Ordering::SeqCst), 20_000);
    assert_eq!(disk.used_disk_space(), bytes);
    drop(physical);
    drop(frame);
    tokio::task::yield_now().await;
    assert_eq!(disk.used_disk_space(), 0);
    assert_eq!(disk.spilling_progress().active_files_count, 0);
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn enabled_disk_is_unused_when_a_snapshot_fits_in_memory() {
    let (ctx, calls, pool) = spilling_context(1 << 20, DiskManagerBuilder::default(), 4);
    let frame = ctx
        .kql("let T=materialize(Events | project y=observe(x)); T | count")
        .await
        .unwrap();
    let plan = frame.clone().create_physical_plan().await.unwrap();
    collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 20);
    assert_eq!(spill_count(plan.as_ref()), 0);
    assert_eq!(ctx.runtime_env().disk_manager.used_disk_space(), 0);
    assert!(pool.reserved() > 0);
    drop(plan);
    drop(frame);
    released(&ctx, &pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disk_quota_failures_are_shared_and_release_partial_files() {
    let (ctx, calls, pool) = spilling_context(
        16 << 10,
        DiskManagerBuilder::default().with_max_temp_directory_size(4096),
        4,
    );
    large_table(&ctx, 20_000, 4);
    let frame = ctx
        .kql("materialize(Large | project y=observe(x))")
        .await
        .unwrap();
    let error = frame.clone().collect().await.unwrap_err().to_string();
    assert!(error.contains("exceeded the allowable limit"), "{error}");
    released(&ctx, &pool).await;
    let count = calls.load(Ordering::SeqCst);
    let error = frame.collect().await.unwrap_err().to_string();
    assert!(error.contains("exceeded the allowable limit"), "{error}");
    assert_eq!(calls.load(Ordering::SeqCst), count);
    released(&ctx, &pool).await;
}

#[tokio::test]
async fn custom_disk_backend_failures_are_not_replaced_with_empty_data() {
    use datafusion::execution::spill_file::{SpillFile, TempFileFactory};
    struct Refuse;
    impl TempFileFactory for Refuse {
        fn create_temp_file(
            &self,
            description: &str,
        ) -> datafusion::common::Result<Arc<dyn SpillFile>> {
            Err(DataFusionError::Execution(format!(
                "fixture refused {description} spill"
            )))
        }
    }
    let (ctx, calls, pool) = spilling_context(
        0,
        DiskManagerBuilder::default().with_temp_file_factory(Arc::new(Refuse)),
        1,
    );
    let frame = ctx
        .kql("materialize(Events | project y=observe(x))")
        .await
        .unwrap();
    let error = frame.clone().collect().await.unwrap_err().to_string();
    assert!(
        error.contains("fixture refused KQL materialization spill"),
        "{error}"
    );
    released(&ctx, &pool).await;
    let count = calls.load(Ordering::SeqCst);
    assert!(frame.collect().await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), count);
}

#[tokio::test]
async fn empty_and_zero_column_snapshots_keep_exact_row_counts() {
    let (ctx, _, pool) = spilling_context(0, DiskManagerBuilder::default(), 1);
    let empty = ctx
        .kql("let T=materialize(datatable(x:long)[]); T | count")
        .await
        .unwrap();
    assert_eq!(rows(empty).await, vec![vec!["0"]]);
    assert_eq!(ctx.runtime_env().disk_manager.used_disk_space(), 0);
    let frame = ctx
        .kql("let T=materialize(range x from 1 to 513 step 1 | project-away *); union T,T | count")
        .await
        .unwrap();
    assert_eq!(rows(frame.clone()).await, vec![vec!["1026"]]);
    assert!(ctx.runtime_env().disk_manager.used_disk_space() > 0);
    drop(frame);
    released(&ctx, &pool).await;
}

#[tokio::test]
async fn spill_readers_keep_the_snapshot_alive_and_limit_replay_batch_rows() {
    let (ctx, calls, pool) = spilling_context(0, DiskManagerBuilder::default(), 1);
    large_table(&ctx, 1025, 1);
    let frame = ctx
        .kql("materialize(Large | project y=observe(x))")
        .await
        .unwrap();
    let plan = frame.clone().create_physical_plan().await.unwrap();
    let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
    let first = stream.try_next().await.unwrap().unwrap();
    assert!(first.num_rows() <= 128);
    let mut count = first.num_rows();
    drop(frame);
    drop(plan);
    assert!(ctx.runtime_env().disk_manager.used_disk_space() > 0);
    while let Some(batch) = stream.try_next().await.unwrap() {
        assert!(batch.num_rows() <= 128);
        count += batch.num_rows();
    }
    assert_eq!(count, 1025);
    assert_eq!(calls.load(Ordering::SeqCst), 1025);
    drop(stream);
    released(&ctx, &pool).await;
}

#[tokio::test]
async fn rich_dynamic_decimal_guid_temporal_and_serialized_values_survive_spill() {
    let query = "let T=materialize(range x from 513 to 1 step -1 | extend amount=todecimal(x)+decimal(0.25),id=guid(74be27de-1e4e-49d9-b579-fe0b331d3642),t=datetime(2025-01-01),d=1h | extend payload=bag_pack('n',pack_array(amount,id,t,d)) | serialize ordinal=row_number()); T | project x,ordinal,amount,id,t,d,payload";
    let memory = context(None, false)
        .0
        .kql(query)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let (ctx, _, pool) = spilling_context(256 << 10, DiskManagerBuilder::default(), 1);
    let frame = ctx.kql(query).await.unwrap();
    let spilled = frame.clone().collect().await.unwrap();
    let memory = datafusion::arrow::compute::concat_batches(&memory[0].schema(), &memory).unwrap();
    let spilled =
        datafusion::arrow::compute::concat_batches(&spilled[0].schema(), &spilled).unwrap();
    assert_eq!(memory.schema(), spilled.schema());
    assert_eq!(memory, spilled);
    assert!(ctx.runtime_env().disk_manager.used_disk_space() > 0);
    drop(frame);
    released(&ctx, &pool).await;
}

#[tokio::test]
async fn native_lists_and_dictionary_replacement_survive_spill() {
    use datafusion::arrow::array::StringArray;
    let batches = [vec!["a", "b"], vec!["b", "c"]]
        .into_iter()
        .map(|values| {
            let dictionary = DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(0), Some(1), None]),
                Arc::new(StringArray::from(values)),
            )
            .unwrap();
            let lists = ListArray::from_iter_primitive::<Int64Type, _, _>([
                Some(vec![Some(1), None]),
                None,
                Some(vec![Some(i64::MAX)]),
            ]);
            RecordBatch::try_from_iter(vec![
                ("words", Arc::new(dictionary) as ArrayRef),
                ("values", Arc::new(lists) as _),
            ])
            .unwrap()
        })
        .collect::<Vec<_>>();
    let query = "let T=materialize(Collections); T | project word=tostring(words),value=tostring(todynamic(values))";
    let memory = context(None, false).0;
    memory
        .register_table(
            TableReference::bare("Collections"),
            Arc::new(MemTable::try_new(batches[0].schema(), vec![batches.clone()]).unwrap()),
        )
        .unwrap();
    let mut expected = rows(memory.kql(query).await.unwrap()).await;
    expected.sort_unstable();
    let (ctx, _, pool) = spilling_context(0, DiskManagerBuilder::default(), 1);
    ctx.register_table(
        TableReference::bare("Collections"),
        Arc::new(MemTable::try_new(batches[0].schema(), vec![batches]).unwrap()),
    )
    .unwrap();
    let frame = ctx.kql(query).await.unwrap();
    let mut actual = rows(frame.clone()).await;
    actual.sort_unstable();
    assert_eq!(actual, expected);
    assert!(ctx.runtime_env().disk_manager.used_disk_space() > 0);
    drop(frame);
    released(&ctx, &pool).await;
}

#[tokio::test]
async fn truncated_spill_replay_fails_instead_of_silently_losing_rows() {
    let directory = tempfile::tempdir().unwrap();
    let (ctx, calls, pool) = spilling_context(
        0,
        DiskManagerBuilder::default().with_mode(DiskManagerMode::Directories(vec![
            directory.path().to_path_buf(),
        ])),
        1,
    );
    large_table(&ctx, 1025, 1);
    let frame = ctx
        .kql("materialize(Large | project y=observe(x))")
        .await
        .unwrap();
    assert_eq!(rows(frame.clone()).await.len(), 1025);
    let paths = ctx.runtime_env().disk_manager.temp_dir_paths();
    let files = std::fs::read_dir(&paths[0])
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(files.len(), 1);
    std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&files[0])
        .unwrap();
    for _ in 0..2 {
        let error = frame.clone().collect().await.unwrap_err().to_string();
        assert!(
            error.contains("materialization spill ended after 0 rows, expected 1025"),
            "{error}"
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1025);
    drop(frame);
    released(&ctx, &pool).await;
    assert!(std::fs::read_dir(&paths[0]).unwrap().next().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn errors_after_spilling_release_files_and_do_not_reexecute_input() {
    let (ctx, _, pool) = spilling_context(8 << 10, DiskManagerBuilder::default(), 1);
    large_table(&ctx, 10_000, 1);
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    ctx.register_udf(create_udf(
        "late_error",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(move |args| {
            counter.fetch_add(1, Ordering::SeqCst);
            let ColumnarValue::Array(array) = &args[0] else {
                panic!("fixture expects arrays");
            };
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            if array.value(0) >= 2048 {
                Err(DataFusionError::Execution("late upstream failure".into()))
            } else {
                Ok(args[0].clone())
            }
        }),
    ));
    let frame = ctx
        .kql("materialize(Large | project y=late_error(x))")
        .await
        .unwrap();
    let plan = frame.clone().create_physical_plan().await.unwrap();
    let error = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("late upstream failure"), "{error}");
    assert!(spill_count(plan.as_ref()) > 0);
    released(&ctx, &pool).await;
    let count = calls.load(Ordering::SeqCst);
    assert!(
        frame
            .collect()
            .await
            .unwrap_err()
            .to_string()
            .contains("late upstream failure")
    );
    assert_eq!(calls.load(Ordering::SeqCst), count);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_key_rebinding_and_nested_snapshots_can_spill_independently() {
    let (ctx, calls, pool) = spilling_context(64 << 10, DiskManagerBuilder::default(), 1);
    let query = "Events | extend k=x%2 | partition by k (extend a=repeat(x,1000) | mv-expand a to typeof(long) | project y=observe(a) | as hint.materialized=true T | union T | summarize Total=sum(y)) | sort by Total asc";
    assert_eq!(
        rows(ctx.kql(query).await.unwrap()).await,
        vec![vec!["200000"], vec!["220000"]]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 20_000);
    released(&ctx, &pool).await;
    large_table(&ctx, 20_000, 1);
    let frame=ctx.kql("let A=materialize(Large | project y=observe(x)); let B=materialize(A | where y<10000); union (A | summarize Total=sum(y)),(B | summarize Total=sum(y)) | sort by Total asc").await.unwrap();
    assert_eq!(
        rows(frame.clone()).await,
        vec![vec!["49995000"], vec!["199990000"]]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 40_000);
    assert!(ctx.runtime_env().disk_manager.used_disk_space() > 0);
    drop(frame);
    released(&ctx, &pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fair_pool_and_native_spill_compression_preserve_snapshots() {
    use datafusion::common::config::SpillCompression;
    use datafusion::execution::memory_pool::FairSpillPool;
    for compression in [
        SpillCompression::Uncompressed,
        SpillCompression::Lz4Frame,
        SpillCompression::Zstd,
    ] {
        let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(64 << 10));
        let mut config = SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(128);
        config.options_mut().execution.spill_compression = compression;
        let (ctx, calls) = configured_context(
            RuntimeEnvBuilder::new().with_memory_pool(Arc::clone(&pool)),
            config,
            false,
        );
        large_table(&ctx, 20_000, 4);
        let frame = ctx
            .kql("let T=materialize(Large | project y=observe(x)); T | summarize Total=sum(y)")
            .await
            .unwrap();
        assert_eq!(rows(frame.clone()).await, vec![vec!["199990000"]]);
        assert!(ctx.runtime_env().disk_manager.used_disk_space() > 0);
        assert_eq!(rows(frame.clone()).await, vec![vec!["199990000"]]);
        assert_eq!(calls.load(Ordering::SeqCst), 20_000);
        drop(frame);
        released(&ctx, &pool).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_initialization_can_restart_without_publishing_a_partial_snapshot() {
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::catalog::streaming::StreamingTable;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::SendableRecordBatchStream;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::streaming::PartitionStream;
    use futures::StreamExt;
    use tokio::sync::Semaphore;
    #[derive(Debug)]
    struct Gated {
        schema: SchemaRef,
        batches: [RecordBatch; 2],
        gate: Arc<Semaphore>,
        attempts: Arc<AtomicUsize>,
    }
    impl PartitionStream for Gated {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }
        fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            let [first, second] = self.batches.clone();
            let gate = Arc::clone(&self.gate);
            let stream = futures::stream::once(async { Ok(first) }).chain(futures::stream::once(
                async move {
                    gate.acquire_owned().await.unwrap().forget();
                    Ok(second)
                },
            ));
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream,
            ))
        }
    }
    let (ctx, _, pool) = spilling_context(0, DiskManagerBuilder::default(), 1);
    let batches = [1..=128, 129..=256].map(|values| {
        RecordBatch::try_from_iter(vec![(
            "x",
            Arc::new(Int64Array::from_iter_values(values)) as ArrayRef,
        )])
        .unwrap()
    });
    let schema = batches[0].schema();
    let attempts = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let provider = StreamingTable::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Gated {
            schema,
            batches,
            attempts: Arc::clone(&attempts),
            gate: Arc::clone(&gate),
        })],
    )
    .unwrap();
    ctx.register_table(TableReference::bare("Gated"), Arc::new(provider))
        .unwrap();
    let frame = ctx.kql("materialize(Gated)").await.unwrap();
    let pending = frame.clone();
    let task = tokio::spawn(async move { pending.collect().await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while ctx.runtime_env().disk_manager.used_disk_space() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    released(&ctx, &pool).await;
    gate.add_permits(1);
    let restored = rows(frame.clone()).await;
    assert_eq!(restored.len(), 256);
    assert_eq!(
        restored
            .iter()
            .map(|row| row[0].parse::<i64>().unwrap())
            .sum::<i64>(),
        32896
    );
    assert_eq!(rows(frame.clone()).await, restored);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    drop(frame);
    released(&ctx, &pool).await;
}

#[tokio::test]
async fn unbounded_materialization_is_rejected_before_spill_or_execution() {
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::catalog::streaming::StreamingTable;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::SendableRecordBatchStream;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::streaming::PartitionStream;
    #[derive(Debug)]
    struct Endless(SchemaRef);
    impl PartitionStream for Endless {
        fn schema(&self) -> &SchemaRef {
            &self.0
        }
        fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.0),
                futures::stream::pending(),
            ))
        }
    }
    let (ctx, _, pool) = spilling_context(0, DiskManagerBuilder::default(), 1);
    let schema =
        RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap()
            .schema();
    let provider = StreamingTable::try_new(Arc::clone(&schema), vec![Arc::new(Endless(schema))])
        .unwrap()
        .with_infinite_table(true);
    ctx.register_table(TableReference::bare("Endless"), Arc::new(provider))
        .unwrap();
    let frame = ctx.kql("materialize(Endless)").await.unwrap();
    let error = frame.create_physical_plan().await.unwrap_err().to_string();
    assert!(
        error.contains("materialize requires a bounded input"),
        "{error}"
    );
    released(&ctx, &pool).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_key_materializations_are_shared_across_fork_legs_after_rebinding() {
    let (ctx, calls) = context(None, false);
    let frame=ctx.kql("Events | extend k=x%2 | partition by k (extend y=observe(x) | as hint.materialized=true T | union T | summarize Total=sum(y)) | sort by Total asc").await.unwrap();
    assert_eq!(rows(frame).await, vec![vec!["200"], vec!["220"]]);
    assert_eq!(calls.load(Ordering::SeqCst), 20);
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
