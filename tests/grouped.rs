use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

async fn result(ctx: &SessionContext, query: &str) -> Vec<Vec<String>> {
    let frame = ctx
        .kql(query)
        .await
        .unwrap_or_else(|error| panic!("{}\n{query}", error.diagnostic(query)));
    frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{error}\n{query}"))
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.logical_nulls().is_some_and(|n| n.is_null(row)) {
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

async fn check(query: &str, expected: &[&[&str]]) {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let expected = expected
        .iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(result(&ctx, query).await, expected, "{query}");
}

#[tokio::test]
async fn mv_apply_top_is_per_original_row() {
    check("datatable(id:long,a:dynamic)[1,dynamic([1,5,3]),2,dynamic([10,2,5,7])] | mv-apply a to typeof(long) on (top 2 by a) | project id,a | sort by id asc,a desc",
        &[&["1","5"],&["1","3"],&["2","10"],&["2","7"]]).await;
}

#[tokio::test]
async fn mv_apply_restores_unexpanded_columns_after_aggregation() {
    check("datatable(id:long,a:dynamic)[1,dynamic([5,1,3]),2,dynamic([4,10,8,7])] | mv-apply v=a to typeof(long) on (top 2 by v | summarize Top2=make_list(v)) | project id,a=tostring(a),Top2=tostring(Top2) | sort by id asc",
        &[&["1","[5,1,3]","[5,3]"],&["2","[4,10,8,7]","[10,8]"]]).await;
}

#[tokio::test]
async fn mv_apply_zips_columns_with_real_dynamic_null_padding() {
    check("print a=dynamic(['A','B']),b=dynamic([1,2,3]) | mv-apply v=a,w=b to typeof(long) on (extend pair=strcat(v,':',w) | summarize pairs=make_list(pair)) | project pairs=tostring(pairs)",
        &[&["[\"A:1\",\"B:2\",\":3\"]"]]).await;
}

#[tokio::test]
async fn mv_apply_indices_limits_and_type_only_casts_are_preserved() {
    check("print id=1,a=dynamic([4,'5',6,7]) | mv-apply with_itemindex=i a to typeof(long) limit 3 on (where i>=1 | project a,i) | project id,a,i",
        &[&["1","<null>","1"],&["1","6","2"]]).await;
}

#[tokio::test]
async fn mv_apply_handles_empty_null_and_scalar_inputs() {
    check("datatable(id:long,a:dynamic)[1,dynamic([]),2,dynamic(null),3,dynamic(5)] | mv-apply a to typeof(long) on (summarize N=count(),Total=sum(a)) | project id,N,Total | sort by id asc",
        &[&["1","0","0"],&["2","1","0"],&["3","1","5"]]).await;
    check(
        "print a=dynamic([]) | mv-apply a on (take 1) | count",
        &[&["0"]],
    )
    .await;
}

#[tokio::test]
async fn mv_apply_leaves_bags_whole_for_explicit_inner_expansion() {
    check("print a=dynamic({'x':1,'y':2}) | mv-apply p=a on (mv-expand kind=array p | summarize N=count()) | project N", &[&["2"]]).await;
}

#[tokio::test]
async fn mv_apply_serialized_windows_restart_for_each_original_row() {
    check("datatable(id:long,a:dynamic)[1,dynamic([2,4]),2,dynamic([3,6])] | mv-apply a to typeof(long) on (extend n=row_number(),previous=prev(a,1,0)) | project id,a,n,previous | sort by id asc,n asc",
        &[&["1","2","1","0"],&["1","4","2","2"],&["2","3","1","0"],&["2","6","2","3"]]).await;
}

#[tokio::test]
async fn nested_mv_apply_rebinds_native_input_templates() {
    check("print a=dynamic([[1,2],[3]]) | mv-apply a on (mv-apply a to typeof(long) on (summarize Total=sum(a)) | summarize Total=sum(Total)) | project Total", &[&["6"]]).await;
}

#[tokio::test]
async fn group_dependent_materializations_do_not_reuse_another_group() {
    check("datatable(id:long,a:dynamic)[1,dynamic([1,2]),2,dynamic([3,4])] | mv-apply a to typeof(long) on (as hint.materialized=true T | union T | summarize Total=sum(a)) | project id,Total | sort by id asc",
        &[&["1","6"],&["2","14"]]).await;
}

#[tokio::test]
async fn partition_top_and_count_are_per_key_and_drop_keys_when_projected() {
    for hint in ["", "hint.strategy=native", "hint.strategy=shuffle"] {
        check(&format!("datatable(k:string,x:long)['a',1,'b',3,'a',4,'b',2] | partition {hint} by k (top 1 by x) | sort by k asc"),
            &[&["a","4"],&["b","3"]]).await;
        check(&format!("datatable(k:string,x:long)['a',1,'b',3,'a',4] | partition {hint} by k (count) | sort by Count asc"),
            &[&["1"],&["2"]]).await;
    }
}

#[tokio::test]
async fn partition_windows_and_ordered_collections_have_independent_state() {
    check("datatable(k:string,x:long)['a',4,'b',3,'a',1,'b',2] | partition hint.strategy=native by k (sort by x asc | extend n=row_number(),prev=prev(x,1,0)) | sort by k asc,n asc",
        &[&["a","1","1","0"],&["a","4","2","1"],&["b","2","1","0"],&["b","3","2","2"]]).await;
    check("datatable(k:string,x:long)['a',4,'b',3,'a',1,'b',2] | partition hint.strategy=shuffle by k (sort by x desc | summarize k=take_any(k),xs=make_list(x)) | project k,xs=tostring(xs) | sort by k asc",
        &[&["a","[4,1]"],&["b","[3,2]"]]).await;
}

#[tokio::test]
async fn partition_null_keys_and_empty_inputs_are_handled() {
    check("datatable(k:long,x:long)[long(null),1,1,2,long(null),3] | partition hint.strategy=native by k (summarize Total=sum(x)) | sort by Total asc", &[&["2"],&["4"]]).await;
    check(
        "datatable(k:long,x:long)[] | partition hint.strategy=shuffle by k (count) | count",
        &[&["0"]],
    )
    .await;
}

#[tokio::test]
async fn partition_crosses_input_batches_and_physical_partitions() {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(2),
    );
    let make = |values: Vec<i64>| {
        RecordBatch::try_from_iter(vec![
            (
                "k",
                Arc::new(StringArray::from_iter_values(
                    values
                        .iter()
                        .map(|x| if x % 2 == 0 { "even" } else { "odd" }),
                )) as _,
            ),
            ("x", Arc::new(Int64Array::from(values)) as _),
        ])
        .unwrap()
    };
    let one = make(vec![1, 2, 3, 4]);
    let two = make(vec![5, 6, 7, 8]);
    ctx.register_table(
        TableReference::bare("T"),
        Arc::new(MemTable::try_new(one.schema(), vec![vec![one], vec![two]]).unwrap()),
    )
    .unwrap();
    assert_eq!(result(&ctx,"T | partition hint.strategy=native by k (top 2 by x | summarize k=take_any(k),Total=sum(x)) | sort by k asc").await,vec![vec!["even","14"],vec!["odd","12"]]);
}

#[tokio::test]
async fn legacy_partition_enforces_the_global_64_key_limit() {
    check(
        "range x from 1 to 64 step 1 | partition by x (count) | summarize N=sum(Count)",
        &[&["64"]],
    )
    .await;
    let ctx = SessionContext::new();
    let error = ctx
        .kql("range x from 1 to 65 step 1 | partition by x (count) | count")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("64 keys"), "{error}");
    check("range x from 1 to 65 step 1 | partition hint.strategy=native by x (count) | summarize N=sum(Count)", &[&["65"]]).await;
}

#[tokio::test]
async fn grouped_transformations_accept_lexical_functions_and_parameters() {
    check("declare query_parameters(n:long=2); let twice=(x:long){2*x}; print a=dynamic([1,2,3]) | mv-apply a to typeof(long) on (where twice(a)>n | summarize Total=sum(a)) | project Total", &[&["5"]]).await;
}

#[tokio::test]
async fn native_partition_aggregates_use_one_native_grouped_plan() {
    let ctx = SessionContext::new();
    let query = "range x from 1 to 1000 step 1 | extend k=x%100 | partition hint.strategy=native by k (summarize N=count(),Total=sum(x))";
    let frame = ctx.kql(query).await.unwrap();
    let physical = frame.clone().create_physical_plan().await.unwrap();
    let plan = datafusion::physical_plan::displayable(physical.as_ref())
        .indent(false)
        .to_string();
    assert!(plan.contains("AggregateExec"), "{plan}");
    assert!(!plan.contains("KqlApplyExec"), "{plan}");
    assert_eq!(
        frame
            .collect()
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        100
    );
    check("datatable(k:string,x:long)['a',1,'a',2,'b',3] | partition hint.strategy=shuffle by k (summarize Total=sum(x)) | sort by Total asc", &[&["3"],&["3"]]).await;
}

#[tokio::test]
async fn vectorized_mv_apply_matches_native_templates_for_nulls_limits_and_large_integers() {
    let ctx = SessionContext::new();
    for values in [
        "[1,5,3]",
        "[]",
        "[null,'5',3,1]",
        "[9007199254740993,9007199254740991]",
        "[-9223372036854775808,0,1]",
    ] {
        for (order, count) in [("desc nulls last", 2), ("asc nulls first", 3), ("desc", 0)] {
            let suffix = format!(
                "top {count} by v {order} | summarize Total=sum(v),N=count(),Values=make_list(v),Nulls=make_list_with_nulls(v)"
            );
            let prefix =
                format!("print source=dynamic({values}) | mv-apply v=source to typeof(long) on (");
            let finish = ") | project Total,N,Values=tostring(Values),Nulls=tostring(Nulls)";
            let fast = result(&ctx, &format!("{prefix}{suffix}{finish}")).await;
            let template = result(&ctx, &format!("{prefix}where true | {suffix}{finish}")).await;
            assert_eq!(fast, template, "{values} {order} {count}");
        }
    }
    check("print a=dynamic([1.0,3,2.5,'4',null]) | mv-apply v=a to typeof(real) on (top 3 by v asc nulls last | summarize Values=make_list_with_nulls(v)) | project Values=tostring(Values)",
        &[&["[1.0,2.5,null]"]]).await;
}

#[tokio::test]
async fn vectorized_mv_apply_uses_an_arrow_batch_kernel_not_per_row_native_plans() {
    let ctx = SessionContext::new();
    let frame=ctx.kql("print a=dynamic([1,2,3]) | mv-apply v=a to typeof(long) on (top 2 by v | summarize Total=sum(v))").await.unwrap();
    let plan = frame.create_physical_plan().await.unwrap();
    let text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string();
    assert!(!text.contains("KqlApplyExec"), "{text}");
}

#[tokio::test]
async fn vectorized_mv_apply_sum_matches_native_overflow_behavior() {
    let ctx = SessionContext::new();
    let base = "print source=dynamic([9223372036854775807,9223372036854775807]) | mv-apply v=source to typeof(long) on (";
    let suffix = "top 2 by v | summarize Total=sum(v)) | project Total";
    let fast = ctx
        .kql(&format!("{base}{suffix}"))
        .await
        .unwrap()
        .collect()
        .await;
    let native = ctx
        .kql(&format!("{base}where true | {suffix}"))
        .await
        .unwrap()
        .collect()
        .await;
    match (fast, native) {
        (Ok(fast), Ok(native)) => assert_eq!(
            array_value_to_string(fast[0].column(0), 0).unwrap(),
            array_value_to_string(native[0].column(0), 0).unwrap()
        ),
        (Err(_), Err(_)) => {}
        (fast, native) => panic!("optimized/native overflow mismatch: {fast:?} {native:?}"),
    }
}

#[tokio::test]
async fn grouped_subqueries_can_be_reexecuted_without_stale_operator_state() {
    let ctx = SessionContext::new();
    let frame = ctx.kql("range x from 1 to 8 step 1 | extend k=x%2 | partition hint.strategy=native by k (top 2 by x) | sort by x asc").await.unwrap();
    for _ in 0..2 {
        let batches = frame.clone().collect().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    }
}

#[tokio::test]
async fn malformed_grouped_syntax_is_rejected() {
    for query in [
        "print a=dynamic([]) | mv-apply a on ()",
        "print a=dynamic([]) | mv-apply kind=array a on (count)",
        "print a=1 | mv-apply a on (count)",
        "print a=1 | partition by missing (count)",
        "print a=dynamic([]) | partition by a (count)",
        "print a=1 | partition hint.strategy=unknown by a (count)",
    ] {
        assert!(SessionContext::new().kql(query).await.is_err(), "{query}");
    }
}

fn bounded_context(
    memory: usize,
    disk: DiskManagerBuilder,
) -> (SessionContext, Arc<dyn MemoryPool>) {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(memory));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .with_disk_manager_builder(disk)
        .build()
        .unwrap();
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(256);
    config.options_mut().execution.sort_spill_reservation_bytes = 32 << 10;
    (
        SessionContext::new_with_config_rt(config, Arc::new(runtime)),
        pool,
    )
}

fn register_groups(ctx: &SessionContext, count: usize) {
    let batches = (0..count)
        .step_by(256)
        .map(|start| {
            let end = count.min(start + 256);
            RecordBatch::try_from_iter(vec![
                (
                    "x",
                    Arc::new(Int64Array::from_iter_values((start..end).map(|i| i as i64))) as _,
                ),
                (
                    "g",
                    Arc::new(Int64Array::from_iter_values(
                        (start..end).map(|i| (i % 3) as i64),
                    )) as _,
                ),
            ])
            .unwrap()
        })
        .collect::<Vec<_>>();
    ctx.register_table(
        TableReference::bare("T"),
        Arc::new(MemTable::try_new(batches[0].schema(), vec![batches]).unwrap()),
    )
    .unwrap();
}

fn grouped_spills(plan: &dyn ExecutionPlan) -> usize {
    let own = if plan.name() == "KqlApplyExec" {
        plan.metrics()
            .and_then(|metrics| metrics.spill_count())
            .unwrap_or(0)
    } else {
        0
    };
    own + plan
        .children()
        .into_iter()
        .map(|child| grouped_spills(child.as_ref()))
        .sum::<usize>()
}

async fn assert_released(ctx: &SessionContext, pool: &Arc<dyn MemoryPool>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let disk = ctx.runtime_env().disk_manager.spilling_progress();
            if pool.reserved() == 0 && disk.current_bytes == 0 && disk.active_files_count == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("group buffers and native sort resources must be released");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_generic_groups_spill_and_replay_without_stale_template_state() {
    for strategy in ["", "hint.strategy=native", "hint.strategy=shuffle"] {
        let (ctx, pool) = bounded_context(512 << 10, DiskManagerBuilder::default());
        register_groups(&ctx, 180_000);
        let query = format!(
            "T | partition {strategy} by g (extend doubled=2*x | summarize N=count(),Total=sum(doubled)) | summarize Groups=count(),N=sum(N),Total=sum(Total)"
        );
        let plan = ctx
            .kql(&query)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        for _ in 0..2 {
            let batches = collect(
                datafusion::physical_plan::execution_plan::reset_plan_states(Arc::clone(&plan))
                    .unwrap(),
                ctx.task_ctx(),
            )
            .await
            .unwrap();
            for (index, expected) in [(0, "3"), (1, "180000"), (2, "32399820000")] {
                assert_eq!(
                    array_value_to_string(batches[0].column(index), 0).unwrap(),
                    expected
                );
            }
            assert!(
                grouped_spills(plan.as_ref()) > 0,
                "generic group buffering, not just upstream sorting, must spill"
            );
            assert_released(&ctx, &pool).await;
        }
    }
}

#[tokio::test]
async fn row_templates_can_spill_large_arrays_without_losing_restored_columns() {
    let (ctx, pool) = bounded_context(512 << 10, DiskManagerBuilder::default());
    let frame = ctx.kql("print id=7,source=range(1,200000,1) | mv-apply v=source to typeof(long) on (where true | summarize N=count(),Total=sum(v)) | project id,N,Total").await.unwrap();
    let plan = frame.create_physical_plan().await.unwrap();
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    for (index, expected) in [(0, "7"), (1, "200000"), (2, "20000100000")] {
        assert_eq!(
            array_value_to_string(batches[0].column(index), 0).unwrap(),
            expected
        );
    }
    assert!(grouped_spills(plan.as_ref()) > 0);
    assert_released(&ctx, &pool).await;
    let (ctx, pool) = bounded_context(
        512 << 10,
        DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
    );
    let frame = ctx.kql("print source=range(1,200000,1) | mv-apply v=source to typeof(long) on (where true | summarize N=count())").await.unwrap();
    let error = frame.collect().await.unwrap_err().to_string();
    assert!(error.contains("KQL grouped subquery"), "{error}");
    assert_released(&ctx, &pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generic_spill_quotas_fail_explicitly_and_clean_up() {
    let (ctx, pool) = bounded_context(
        0,
        DiskManagerBuilder::default().with_max_temp_directory_size(4096),
    );
    let frame = ctx.kql("print source=range(1,10000,1) | mv-apply v=source to typeof(long) on (where true | summarize N=count())").await.unwrap();
    let error = frame.collect().await.unwrap_err().to_string();
    assert!(error.contains("exceeded the allowable limit"), "{error}");
    assert_released(&ctx, &pool).await;
}

#[tokio::test]
async fn grouped_arrow_boundaries_preserve_null_and_guid_key_identity() {
    for key in ["k", "toguid(k)"] {
        let input = if key == "toguid(k)" {
            "datatable(k:string,x:long)['74be27de-1e4e-49d9-b579-fe0b331d3642',1,'00000000-0000-0000-0000-000000000000',2,'74be27de-1e4e-49d9-b579-fe0b331d3642',3]"
        } else {
            "datatable(k:long,x:long)[long(null),1,1,2,long(null),3]"
        };
        let query = format!(
            "{input} | extend key={key} | partition hint.strategy=native by key (where true | summarize N=count(),Total=sum(x)) | sort by Total asc"
        );
        check(&query, &[&["1", "2"], &["2", "4"]]).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn early_output_limit_releases_group_files() {
    let (ctx, pool) = bounded_context(512 << 10, DiskManagerBuilder::default());
    register_groups(&ctx, 180_000);
    let frame = ctx
        .kql("T | partition hint.strategy=native by g (where true | project x) | take 1")
        .await
        .unwrap();
    assert_eq!(
        frame
            .collect()
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    assert_released(&ctx, &pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn template_errors_after_input_spill_release_all_native_resources() {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::DataFusionError;
    use datafusion::logical_expr::{Volatility, create_udf};
    let (ctx, pool) = bounded_context(512 << 10, DiskManagerBuilder::default());
    register_groups(&ctx, 180_000);
    ctx.register_udf(create_udf(
        "fail_group",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(|_| {
            Err(DataFusionError::Execution(
                "group template fixture failed".into(),
            ))
        }),
    ));
    let plan = ctx.kql("T | partition hint.strategy=native by g (extend v=fail_group(x) | summarize Total=sum(v))")
        .await.unwrap().create_physical_plan().await.unwrap();
    let error = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("group template fixture failed"), "{error}");
    assert!(grouped_spills(plan.as_ref()) > 0);
    assert_released(&ctx, &pool).await;
}

#[tokio::test]
async fn arrow_key_ranges_match_grouped_aggregate_identity_for_reals_across_batches() {
    use datafusion::arrow::array::Float64Array;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
    let make = |keys: Vec<Option<f64>>, values: Vec<i64>| {
        RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Float64Array::from(keys)) as _),
            ("x", Arc::new(Int64Array::from(values)) as _),
        ])
        .unwrap()
    };
    let first = make(vec![Some(-0.0), Some(-f64::NAN), None], vec![1, 2, 3]);
    let second = make(vec![Some(0.0), Some(f64::NAN), None], vec![4, 5, 6]);
    let third = make(
        vec![
            Some(2.0),
            Some(f64::from_bits(f64::NAN.to_bits() + 1)),
            Some(2.0),
        ],
        vec![7, 8, 9],
    );
    ctx.register_table(
        TableReference::bare("RealKeys"),
        Arc::new(MemTable::try_new(first.schema(), vec![vec![first, second, third]]).unwrap()),
    )
    .unwrap();
    let expected = result(
        &ctx,
        "RealKeys | summarize N=count(),Total=sum(x) by k | project N,Total | sort by Total asc",
    )
    .await;
    let actual = result(&ctx,"RealKeys | partition hint.strategy=native by k (where true | summarize N=count(),Total=sum(x)) | sort by Total asc").await;
    assert_eq!(actual, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_native_groups_respect_a_fair_spill_pool() {
    use datafusion::execution::memory_pool::FairSpillPool;
    let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(16 << 20));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build()
        .unwrap();
    let mut config = SessionConfig::new()
        .with_target_partitions(4)
        .with_batch_size(1024);
    config.options_mut().execution.sort_spill_reservation_bytes = 32 << 10;
    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime));
    register_groups(&ctx, 600_000);
    let plan = ctx.kql("T | partition hint.strategy=native by g (extend v=2*x | summarize N=count(),Total=sum(v)) | summarize N=sum(N),Total=sum(Total),Groups=count()")
        .await.unwrap().create_physical_plan().await.unwrap();
    let result = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    for (index, expected) in [(0, "600000"), (1, "359999400000"), (2, "3")] {
        assert_eq!(
            array_value_to_string(result[0].column(index), 0).unwrap(),
            expected
        );
    }
    assert!(grouped_spills(plan.as_ref()) > 0);
    assert_released(&ctx, &pool).await;
}
