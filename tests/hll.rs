use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{ScalarValue, TableReference};
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use datafusion_kql::{QueryParameters, SessionContextKqlExt};

fn context() -> SessionContext {
    SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(512),
    )
}

async fn rows(ctx: &SessionContext, query: &str) -> Vec<Vec<String>> {
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
                    .map(|array| {
                        if array
                            .logical_nulls()
                            .is_some_and(|nulls| nulls.is_null(row))
                        {
                            "<null>".to_owned()
                        } else {
                            array_value_to_string(array, row).unwrap()
                        }
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect()
}

async fn check(query: &str, expected: &[&[&str]]) {
    let expected = expected
        .iter()
        .map(|row| row.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(rows(&context(), query).await, expected, "{query}");
}

#[tokio::test]
async fn distinct_accuracy_and_predicates_preserve_null_semantics() {
    check("datatable(x:long,p:bool)[1,true,1,false,2,true,long(null),true,3,bool(null)] | summarize N=dcount(x),Selected=dcountif(x,p),None=dcountif(x,false,4)",
        &[&["3","2","0"]]).await;
    check(
        "datatable(x:long)[] | summarize N=dcount(x,4),s=hll(x) | project N,decoded=dcount_hll(s)",
        &[&["0", "0"]],
    )
    .await;
    check("datatable(g:string,x:long)['a',long(null),'b',long(null)] | summarize N=dcount(x),s=hll(x,2) by g | project g,N,decoded=dcount_hll(s) | sort by g asc",
        &[&["a","0","0"],&["b","0","0"]]).await;
    for accuracy in 0..=4 {
        check(&format!("range x from 1 to 100 step 1 | summarize N=dcount(x,{accuracy}),s=hll(x,{accuracy}) | project N,decoded=dcount_hll(s)"),
            &[&["100","100"]]).await;
    }
}

#[tokio::test]
async fn published_exact_count_thresholds_remain_exact_across_batches() {
    check("range x from 1 to 1000 step 1 | summarize N=dcount(x,1),s=hll(x,1) | project N,decoded=dcount_hll(s)",
        &[&["1000","1000"]]).await;
    check("range x from 1 to 8000 step 1 | summarize N=dcount(x,2),s=hll(x,2) | project N,decoded=dcount_hll(s)",
        &[&["8000","8000"]]).await;
}

#[tokio::test]
async fn stored_sketches_merge_overlapping_sets_not_just_their_counts() {
    check("range x from 1 to 10 step 1 | extend y=x+5 | summarize a=hll(x),b=hll(y),selected=hll_if(x,x>5) | project overlap=dcount_hll(hll_merge(a,b)),selected=dcount_hll(selected)",
        &[&["15","5"]]).await;
    check("range x from 1 to 1000 step 1 | extend g=x%4 | summarize s=hll(x,2) by g | summarize s=hll_merge(s) | project N=dcount_hll(s)",
        &[&["1000"]]).await;
    check(
        "range x from 1 to 10 step 1 | summarize N=dcount_hll(hll_merge(hll(x),hll(x+5)))",
        &[&["15"]],
    )
    .await;
}

#[tokio::test]
async fn scalar_merge_accepts_two_through_64_arguments_and_ignores_null_sketches() {
    check("range x from 1 to 3 step 1 | summarize s=hll(x) | project N=dcount_hll(hll_merge(s,dynamic(null))),Missing=isnull(dcount_hll(dynamic(null)))",
        &[&["3","true"]]).await;
    check(
        "datatable(s:dynamic)[] | summarize s=hll_merge(s) | project missingValue=isnull(s)",
        &[&["true"]],
    )
    .await;
    let arguments = std::iter::repeat_n("s", 64).collect::<Vec<_>>().join(",");
    check(&format!("range x from 1 to 10 step 1 | summarize s=hll(x) | project N=dcount_hll(hll_merge({arguments}))"), &[&["10"]]).await;
}

#[tokio::test]
async fn accuracy_can_be_bound_by_parameters_and_lexical_functions() {
    check("let accuracy=1+1; let f=(T:(x:long),a:long){T | summarize s=hll(x,a)}; range x from 1 to 10 step 1 | invoke f(accuracy) | project N=dcount_hll(s)",
        &[&["10"]]).await;
    let ctx = context();
    let frame = ctx.kql_with_parameters(
        "declare query_parameters(accuracy:long); range x from 1 to 10 step 1 | summarize N=dcount(x,accuracy)",
        &QueryParameters::from([("accuracy".into(),ScalarValue::Int64(Some(2)))]),
    ).await.unwrap();
    let batches = frame.collect().await.unwrap();
    assert_eq!(
        array_value_to_string(batches[0].column(0), 0).unwrap(),
        "10"
    );
}

#[tokio::test]
async fn dynamic_and_decimal_identity_survives_sketching() {
    check("datatable(d:dynamic)[dynamic(1),dynamic('1'),dynamic(null),dynamic({'a':1}),dynamic({'a':1})] | summarize N=dcount(d,2),s=hll(d,2) | project N,decoded=dcount_hll(s)",
        &[&["3","3"]]).await;
    check("datatable(x:decimal)[decimal(1.00),decimal(1.0),decimal(2.5),decimal(null)] | summarize N=dcount(x),s=hll(x) | project N,decoded=dcount_hll(s)",
        &[&["2","2"]]).await;
    check("datatable(t:datetime)[datetime(2025-01-01),datetime(2025-01-01),datetime(null)] | summarize s=hll(t) | project N=dcount_hll(s)",
        &[&["1"]]).await;
}

#[tokio::test]
async fn dictionary_strings_and_logical_nulls_are_hashed_by_value() {
    use datafusion::arrow::array::{DictionaryArray, Int8Array};
    use datafusion::arrow::datatypes::Int8Type;
    let ctx = context();
    let first: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), Some(2), None]),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
        )
        .unwrap(),
    );
    let second: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), Some(2), None]),
            Arc::new(StringArray::from(vec![Some("b"), Some("a"), None])),
        )
        .unwrap(),
    );
    let first = RecordBatch::try_from_iter(vec![("s", first)]).unwrap();
    let second = RecordBatch::try_from_iter(vec![("s", second)]).unwrap();
    ctx.register_table(
        TableReference::bare("T"),
        Arc::new(MemTable::try_new(first.schema(), vec![vec![first], vec![second]]).unwrap()),
    )
    .unwrap();
    // KQL strings normalize nulls to the empty string, which is a distinct value.
    assert_eq!(
        rows(
            &ctx,
            "T | summarize N=dcount(s),s=hll(s) | project N,decoded=dcount_hll(s)"
        )
        .await,
        vec![vec!["3", "3"]]
    );
}

#[tokio::test]
async fn high_cardinality_counts_are_bounded_estimates_and_merge_identically() {
    let ctx = context();
    for (accuracy, precision) in [(0, 12), (1, 14), (2, 16), (3, 17), (4, 18)] {
        let actual = rows(&ctx,&format!("let T=materialize(range x from 1 to 100000 step 1); let Total=toscalar(T | summarize s=hll(x,{accuracy})); T | extend g=x%8 | summarize s=hll(x,{accuracy}) by g | summarize s=hll_merge(s) | project count=dcount_hll(s),single=dcount_hll(Total)")).await;
        assert_eq!(actual[0][0], actual[0][1], "accuracy={accuracy}");
        let estimate = actual[0][0].parse::<f64>().unwrap();
        let error = (estimate / 100_000.0 - 1.0).abs();
        assert!(
            error <= 6.0 * 1.04 / ((1_u64 << precision) as f64).sqrt(),
            "accuracy={accuracy},estimate={estimate},error={error}"
        );
    }
}

#[tokio::test]
async fn high_cardinality_grouping_keeps_small_sketches_sparse() {
    check("range x from 0 to 99999 step 1 | extend g=x%10000 | summarize N=dcount(x,4),s=hll(x,4) by g | summarize Total=sum(N),Restored=sum(dcount_hll(s)),Groups=count()",
        &[&["100000","100000","10000"]]).await;
}

#[tokio::test]
async fn native_partition_and_pivot_paths_keep_accuracy_arguments() {
    check("range x from 1 to 20 step 1 | extend g=x%2 | partition hint.strategy=native by g(summarize N=dcount(x,2)) | project N | sort by N asc",
        &[&["10"],&["10"]]).await;
    check("datatable(k:string,v:long)['A',1,'A',1,'A',2,'B',3] | evaluate pivot(k,dcount(v,2)):(A:long,B:long)",
        &[&["2","1"]]).await;
}

#[tokio::test]
async fn sketches_roundtrip_through_json_materialization_and_repeated_execution() {
    let ctx = context();
    let frame = ctx.kql("let T=materialize(range x from 1 to 20 step 1 | summarize s=hll(x,2)); T | union T | extend text=tostring(s) | summarize s=hll_merge(text) | project N=dcount_hll(parse_json(tostring(s)))").await.unwrap();
    for _ in 0..2 {
        let batches = frame.clone().collect().await.unwrap();
        assert_eq!(
            array_value_to_string(batches[0].column(0), 0).unwrap(),
            "20"
        );
    }
    check("let T=range x from 1 to 3 step 1 | summarize s=hll(x); let s=toscalar(T); range x from 1 to 3 step 1 | project N=dcount_hll(s),Merged=dcount_hll(hll_merge(s,s))",
        &[&["3","3"],&["3","3"],&["3","3"]]).await;
    check("let s=toscalar(range x from 1 to 10000 step 1 | summarize s=hll(x,2)); range x from 1 to 1000 step 1 | extend N=dcount_hll(s) | summarize Lo=min(N),Hi=max(N),Rows=count() | project same=Lo==Hi,Rows",
        &[&["true","1000"]]).await;
}

#[tokio::test]
async fn equivalent_accuracy_calls_share_only_the_matching_native_state() {
    use datafusion::logical_expr::LogicalPlan;
    let frame = context()
        .kql("range x from 1 to 10 step 1 | summarize A=dcount(x),B=dcount(x,1),C=dcount(x,2)")
        .await
        .unwrap();
    let LogicalPlan::Projection(project) = frame.logical_plan() else {
        panic!("expected result projection");
    };
    let LogicalPlan::Aggregate(aggregate) = project.input.as_ref() else {
        panic!("expected native aggregate");
    };
    assert_eq!(
        aggregate.aggr_expr.len(),
        2,
        "default accuracy1 can share state, accuracy2 cannot"
    );
    let result = frame.collect().await.unwrap();
    for column in result[0].columns() {
        assert_eq!(array_value_to_string(column, 0).unwrap(), "10");
    }
}

#[tokio::test]
async fn hll_default_output_names_follow_scalar_and_aggregate_overloads() {
    let ctx = context();
    let frame = ctx.kql("range x from 1 to 3 step 1 | summarize hll(x),hll_if(x,true),dcount(x),dcountif(x,true)").await.unwrap();
    assert_eq!(
        frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["hll_x", "hll_if_x", "dcount_x", "dcountif_x"]
    );
    let frame = ctx
        .kql("range x from 1 to 3 step 1 | summarize a=hll(x),b=hll(x+1) | project hll_merge(a,b)")
        .await
        .unwrap();
    assert_eq!(frame.schema().field(0).name(), "hll_merged_result");
}

#[tokio::test]
async fn malformed_sketches_and_mismatched_accuracy_fail_explicitly() {
    for query in [
        "print N=dcount_hll(dynamic([]))",
        "print N=dcount_hll('{\"__datafusion_kql_hll_v1\":\"invalid!\"}')",
        "range x from 1 to 10 step 1 | summarize a=hll(x,1),b=hll(x,2) | project s=hll_merge(a,b)",
        "union (range x from 1 to 3 step 1 | summarize s=hll(x,1)),(range x from 1 to 3 step 1 | summarize s=hll(x,2)) | summarize s=hll_merge(s)",
    ] {
        let ctx = context();
        let error = match ctx.kql(query).await {
            Ok(frame) => frame.collect().await.unwrap_err().to_string(),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("HLL") || error.contains("hll"),
            "{query}\n{error}"
        );
    }
}

#[tokio::test]
async fn unsupported_accuracy_arity_and_types_are_rejected_before_execution() {
    let ctx = context();
    for query in [
        "range x from 1 to 2 step 1 | summarize dcount(x,-1)",
        "range x from 1 to 2 step 1 | summarize dcount(x,5)",
        "range x from 1 to 2 step 1 | summarize dcount(x,1.5)",
        "range x from 1 to 2 step 1 | summarize hll(x,long(null))",
        "range x from 1 to 2 step 1 | summarize hll(x,x)",
        "range x from 1 to 2 step 1 | summarize dcountif(x,true,2,3)",
        "range x from 1 to 2 step 1 | summarize hll_if(x)",
        "print N=dcount_hll(1)",
        "print N=dcount_hll()",
        "print N=hll_merge(dynamic(null))",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
    let arguments = std::iter::repeat_n("dynamic(null)", 65)
        .collect::<Vec<_>>()
        .join(",");
    assert!(
        ctx.kql(&format!("print s=hll_merge({arguments})"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn retained_registers_are_charged_to_the_host_memory_pool() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 << 10));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build()
        .unwrap();
    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::new().with_target_partitions(1),
        Arc::new(runtime),
    );
    let batch = RecordBatch::try_from_iter(vec![(
        "x",
        Arc::new(Int64Array::from_iter_values(0..10_000)) as _,
    )])
    .unwrap();
    ctx.register_batch(TableReference::bare("T"), batch)
        .unwrap();
    let frame = ctx.kql("T | summarize N=dcount(x,4)").await.unwrap();
    let error = frame.collect().await.unwrap_err().to_string();
    assert!(
        error.contains("memory")
            || error.contains("Memory")
            || error.contains("Resources exhausted"),
        "{error}"
    );
    tokio::task::yield_now().await;
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn native_aggregation_spills_and_replays_hll_partial_states() {
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::physical_plan::{ExecutionPlan, collect};
    fn spills(plan: &dyn ExecutionPlan) -> usize {
        let current = plan
            .metrics()
            .and_then(|metrics| metrics.spill_count())
            .unwrap_or(0);
        current
            + plan
                .children()
                .into_iter()
                .map(|child| spills(child.as_ref()))
                .sum::<usize>()
    }
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2 << 20));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build()
        .unwrap();
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(128);
    config.options_mut().execution.sort_spill_reservation_bytes = 64 << 10;
    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime));
    let batch = RecordBatch::try_from_iter(vec![
        ("x", Arc::new(Int64Array::from_iter_values(0..200_000)) as _),
        (
            "g",
            Arc::new(Int64Array::from_iter_values(
                (0..200_000).map(|i| i % 20_000),
            )) as _,
        ),
    ])
    .unwrap();
    let batches = (0..batch.num_rows())
        .step_by(128)
        .map(|offset| batch.slice(offset, 128.min(batch.num_rows() - offset)))
        .collect();
    ctx.register_table(
        TableReference::bare("T"),
        Arc::new(MemTable::try_new(batch.schema(), vec![batches]).unwrap()),
    )
    .unwrap();
    let plan = ctx
        .kql("T | summarize N=dcount(x,4) by g | summarize Total=sum(N),Groups=count()")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    assert_eq!(
        array_value_to_string(batches[0].column(0), 0).unwrap(),
        "200000"
    );
    assert_eq!(
        array_value_to_string(batches[0].column(1), 0).unwrap(),
        "20000"
    );
    assert!(
        spills(plan.as_ref()) > 0,
        "the test must force actual native spilling"
    );
    drop(plan);
    tokio::task::yield_now().await;
    assert_eq!(pool.reserved(), 0);
}
