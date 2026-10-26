use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
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
                    .collect()
            })
        })
        .collect()
}

async fn check(query: &str, expected: &[&[&str]]) {
    let expected = expected
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(rows(&context(), query).await, expected, "{query}");
}

#[tokio::test]
async fn sparse_defaults_and_explicit_exact_aggregates_preserve_nearest_ranks() {
    check("datatable(x:long)[9007199254740993,9007199254740995] | summarize a=percentile(x,50),b=percentile_exact(x,50),c=percentiles_array_exact(x,0,50,100) | project a,b,c=tostring(c)",
        &[&["9007199254740993","9007199254740993","[9007199254740993,9007199254740993,9007199254740995]"]]).await;
    check("datatable(x:long,w:long)[10,9223372036854775807,20,9223372036854775807,30,1] | summarize p=percentilew(x,w,50),q=percentilew_exact(x,w,50),a=percentilesw_array_exact(x,w,0,50,100) | project p,q,a=tostring(a)",
        &[&["20","20","[10,20,30]"]]).await;
    check("range x from 1 to 10000 step 1 | summarize p=percentile_exact(x,50),a=percentiles_array_exact(x,0,50,100) | project p,a=tostring(a)",
        &[&["5000","[1,5000,10000]"]]).await;
    check(
        "datatable(x:long,w:long)[10,1,20,8,30,1] | summarize percentilesw_exact(x,w,0,50,100)",
        &[&["10", "20", "30"]],
    )
    .await;
}

#[tokio::test]
async fn weighted_sketches_support_scalar_and_aggregate_merge_aliases() {
    check("range x from 1 to 10 step 1 | summarize a=tdigest(x),b=tdigest(x+10) | project p=percentile_tdigest(tdigest_merge(a,b),50,typeof(long)),q=percentile_tdigest(merge_tdigest(a,b),50,typeof(long)),r=tostring(percentiles_array_tdigest(a,0,50,100))",
        &[&["10","10","[1,5,10]"]]).await;
    check("range x from 1 to 1000 step 1 | extend g=x%4 | summarize s=tdigest(x) by g | summarize s=merge_tdigest(s) | project p=percentile_tdigest(s,50,typeof(long)),bounds=tostring(percentile_array_tdigest(s,dynamic([0,100])))",
        &[&["500","[1,1000]"]]).await;
    check("datatable(x:long,w:long)[10,1,20,8,30,1] | summarize s=tdigest(x,w) | project p=percentile_tdigest(s,50,typeof(int)),a=tostring(percentiles_array_tdigest(s,dynamic([0,50,100]),typeof(int)))",
        &[&["20","[10,20,30]"]]).await;
    check("range x from 1 to 10 step 1 | summarize p=percentile_tdigest(merge_tdigest(tdigest(x),tdigest(x+10)),50,typeof(long))",
        &[&["10"]]).await;
}

#[tokio::test]
async fn temporal_percentiles_preserve_native_and_nested_dynamic_types() {
    check("datatable(t:datetime)[datetime(2025-01-01),datetime(2025-01-02),datetime(2025-01-03)] | summarize p=percentile(t,50),e=percentile_exact(t,50),a=percentiles_array(t,0,100),s=tdigest(t) | project native=gettype(p),same=p==datetime(2025-01-02),exact=e==p,nested=gettype(a[0]),typed=percentile_tdigest(s,50,typeof(datetime))==p,nestedSketch=gettype(percentiles_array_tdigest(s,0,100)[0])",
        &[&["datetime","true","true","datetime","true","datetime"]]).await;
    check("datatable(t:timespan)[1h,2h,3h] | summarize p=percentile(t,50),s=tdigest(t),a=percentiles_array_exact(t,0,100) | project native=gettype(p),same=p==2h,nested=gettype(a[0]),typed=percentile_tdigest(s,50,typeof(timespan))==p,arrayType=gettype(percentiles_array_tdigest(s,dynamic([50]),typeof(timespan))[0])",
        &[&["timespan","true","timespan","true","timespan"]]).await;
}

#[tokio::test]
async fn empty_null_and_zero_weight_inputs_have_explicit_null_results() {
    check("datatable(x:long,w:long)[] | summarize p=percentile(x,50),a=percentiles_array(x,0,100),s=tdigest(x,w) | project pnull=isnull(p),anull=isnull(a),snull=isnull(s),decoded=isnull(percentile_tdigest(s,50)),typed=isnull(percentile_tdigest(s,50,typeof(long)))",
        &[&["true","true","true","true","true"]]).await;
    check("datatable(x:long,w:long)[long(null),1,1,0,2,long(null)] | summarize p=percentilew(x,w,50),s=tdigest(x,w) | project missing=isnull(p),restored=isnull(percentile_tdigest(s,50)),arrayNull=isnull(percentiles_array_tdigest(s,0,50,100))",
        &[&["true","true","true"]]).await;
    check(
        "datatable(s:dynamic)[] | summarize s=tdigest_merge(s) | project missing=isnull(s)",
        &[&["true"]],
    )
    .await;
    check("range x from 1 to 3 step 1 | summarize s=tdigest(x) | project p=percentile_tdigest(tdigest_merge(s,dynamic(null)),50,typeof(long)),allNull=isnull(tdigest_merge(dynamic(null),dynamic(null))),noRank=isnull(percentile_tdigest(s,real(null)))",
        &[&["2","true","true"]]).await;
    check("datatable(x:long,w:long)[10,-1,20,0,30,long(null),40,1] | summarize p=percentilew(x,w,50),e=percentilew_exact(x,w,50),s=tdigest(x,w) | project p,e,decoded=percentile_tdigest(s,50,typeof(long))",
        &[&["40","40","40"]]).await;
}

#[tokio::test]
async fn scalar_ranks_can_vary_by_row_and_accept_query_parameters() {
    check("let s=toscalar(range x from 1 to 10 step 1 | summarize tdigest(x)); datatable(p:real)[0.0,50.0,100.0] | project value=percentile_tdigest(s,p,typeof(long)),a=tostring(percentiles_array_tdigest(s,p))",
        &[&["1","[1]"],&["5","[5]"],&["10","[10]"]]).await;
    let ctx = context();
    let batch = ctx
        .kql("range x from 1 to 10 step 1 | summarize s=tdigest(x)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let sketch = ScalarValue::try_from_array(batch[0].column(0), 0).unwrap();
    let parameters = QueryParameters::from([
        ("s".into(), sketch),
        ("p".into(), ScalarValue::Float64(Some(95.0))),
    ]);
    let batch=ctx.kql_with_parameters(
        "declare query_parameters(s:dynamic,p:real); print p=percentile_tdigest(s,p,typeof(long))",
        &parameters).await.unwrap().collect().await.unwrap();
    assert_eq!(array_value_to_string(batch[0].column(0), 0).unwrap(), "10");
}

#[tokio::test]
async fn sketch_roundtrips_and_frozen_scalar_caches_preserve_results() {
    let ctx = context();
    let frame=ctx.kql("let T=materialize(range x from 1 to 10000 step 1 | summarize s=tdigest(x)); T | union T | extend text=tostring(s) | summarize s=tdigest_merge(text) | project low=percentile_tdigest(parse_json(tostring(s)),0,typeof(long)),high=percentile_tdigest(s,100,typeof(long))").await.unwrap();
    for _ in 0..2 {
        let batch = frame.clone().collect().await.unwrap();
        assert_eq!(array_value_to_string(batch[0].column(0), 0).unwrap(), "1");
        assert_eq!(
            array_value_to_string(batch[0].column(1), 0).unwrap(),
            "10000"
        );
    }
    check("let s=toscalar(range x from 1 to 10000 step 1 | summarize tdigest(x)); range x from 1 to 1000 step 1 | extend P=percentile_tdigest(s,50,typeof(long)),A=tostring(percentiles_array_tdigest(s,0,100)),M=percentile_tdigest(merge_tdigest(s,s),50,typeof(long)) | summarize identical=min(P)==max(P),bounds=take_any(A),merged=min(M)==max(M),Rows=count()",
        &[&["true","[1,10000]","true","1000"]]).await;
}

#[tokio::test]
async fn finite_double_endpoints_roundtrip_through_dynamic_arrays_without_ulp_loss() {
    let ctx = context();
    let value = (99.999_f64).exp();
    let batch = RecordBatch::try_from_iter(vec![(
        "x",
        Arc::new(Float64Array::from(vec![value; 3000])) as ArrayRef,
    )])
    .unwrap();
    ctx.register_batch(TableReference::bare("T"), batch)
        .unwrap();
    let batches=ctx.kql("T | summarize a=percentiles_array(x,0,100),s=tdigest(x) | project p=toreal(a[1]),q=toreal(percentiles_array_tdigest(s,100)[0]),r=toreal(parse_json(tostring(a))[1])").await.unwrap().collect().await.unwrap();
    for column in batches[0].columns() {
        let column = column.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(column.value(0).to_bits(), value.to_bits());
    }
}

#[tokio::test]
async fn expression_local_sketch_functions_preserve_the_host_registry() {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    let ctx = context();
    ctx.register_udf(create_udf(
        "percentile_tdigest",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(777))))),
    ));
    let scalar_count = ctx.state().scalar_functions().len();
    let aggregate_count = ctx.state().aggregate_functions().len();
    assert_eq!(rows(&ctx,"range x from 1 to 3 step 1 | summarize s=tdigest(x) | project p=percentile_tdigest(s,50,typeof(long))").await,vec![vec!["2"]]);
    let host = ctx
        .sql("select percentile_tdigest(1,50)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(array_value_to_string(host[0].column(0), 0).unwrap(), "777");
    assert_eq!(ctx.state().scalar_functions().len(), scalar_count);
    assert_eq!(ctx.state().aggregate_functions().len(), aggregate_count);
}

#[tokio::test]
async fn default_names_and_multi_output_state_sharing_are_stable() {
    use datafusion::logical_expr::LogicalPlan;
    let ctx = context();
    let frame=ctx.kql("range x from 1 to 100 step 1 | summarize tdigest(x),percentiles_exact(x,25,50),percentiles(x,25,50)").await.unwrap();
    assert_eq!(
        frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        [
            "tdigest_x",
            "percentile_exact_x_25",
            "percentile_exact_x_50",
            "percentile_x_25",
            "percentile_x_50"
        ]
    );
    let LogicalPlan::Projection(project) = frame.logical_plan() else {
        panic!("expected projection");
    };
    let LogicalPlan::Aggregate(aggregate) = project.input.as_ref() else {
        panic!("expected aggregate");
    };
    assert_eq!(aggregate.aggr_expr.len(), 3);
    let frame = ctx
        .kql("range x from 1 to 3 step 1 | summarize s=tdigest(x) | project tdigest_merge(s,s)")
        .await
        .unwrap();
    assert_eq!(frame.schema().field(0).name(), "tdigests_merge_result");
    let frame=ctx.kql("range x from 1 to 3 step 1 | summarize tdigest(x) | summarize tdigest_merge(tdigest_x)").await.unwrap();
    assert_eq!(frame.schema().field(0).name(), "merge_tdigests_tdigest_x");
}

#[tokio::test]
async fn grouped_sparse_states_and_native_partition_paths_stay_exact() {
    check("range x from 0 to 99999 step 1 | extend g=x%10000 | summarize P=percentile(x,50),E=percentile_exact(x,50) by g | summarize bad=countif(P!=g+40000 or E!=P),Groups=count()",
        &[&["0","10000"]]).await;
    check("range x from 1 to 20 step 1 | extend g=x%2 | partition hint.strategy=native by g(summarize s=tdigest(x)) | project low=percentile_tdigest(s,0,typeof(long)),high=percentile_tdigest(s,100,typeof(long)) | sort by low asc",
        &[&["1","19"],&["2","20"]]).await;
}

fn register(ctx: &SessionContext, values: &[(f64, i64)]) {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "x",
            Arc::new(Float64Array::from_iter_values(
                values.iter().map(|(value, _)| *value),
            )) as ArrayRef,
        ),
        (
            "w",
            Arc::new(Int64Array::from_iter_values(
                values.iter().map(|(_, weight)| *weight),
            )) as _,
        ),
        (
            "g",
            Arc::new(Int64Array::from_iter_values(
                (0..values.len()).map(|i| (i % 8) as i64),
            )) as _,
        ),
    ])
    .unwrap();
    let mut partitions = vec![Vec::new(); 4];
    for (index, offset) in (0..values.len()).step_by(512).enumerate() {
        partitions[index % 4].push(batch.slice(offset, 512.min(values.len() - offset)));
    }
    ctx.register_table(
        TableReference::bare("T"),
        Arc::new(MemTable::try_new(batch.schema(), partitions).unwrap()),
    )
    .unwrap();
}

fn assert_rank_error(values: &[(f64, i64)], actual: &[f64]) {
    let ranks = [0.0, 1.0, 25.0, 50.0, 75.0, 99.0, 100.0];
    let total = values
        .iter()
        .map(|(_, weight)| *weight as u128)
        .sum::<u128>();
    let minimum = values
        .iter()
        .map(|(value, _)| *value)
        .min_by(f64::total_cmp)
        .unwrap();
    let maximum = values
        .iter()
        .map(|(value, _)| *value)
        .max_by(f64::total_cmp)
        .unwrap();
    assert_eq!(actual.len(), ranks.len());
    assert_eq!(actual[0], minimum);
    assert_eq!(actual[6], maximum);
    for (rank, estimate) in ranks.into_iter().zip(actual) {
        let below = values
            .iter()
            .filter(|(value, _)| value < estimate)
            .map(|(_, weight)| *weight as u128)
            .sum::<u128>();
        let through = values
            .iter()
            .filter(|(value, _)| value <= estimate)
            .map(|(_, weight)| *weight as u128)
            .sum::<u128>();
        let target = (rank / 100.0 * total as f64).ceil().max(1.0);
        let error = if target < below as f64 {
            below as f64 - target
        } else if target > through as f64 {
            target - through as f64
        } else {
            0.0
        };
        assert!(
            error <= total as f64 * 0.01,
            "rank={rank},estimate={estimate},error={error},total={total}"
        );
    }
}

#[tokio::test]
async fn native_weighted_aggregation_and_rollups_meet_measured_rank_error() {
    for distribution in 0..4 {
        let mut values = (0..100_000)
            .map(|i| {
                let value = match distribution {
                    0 => f64::from(i) - 50_000.0,
                    1 => (f64::from(i) / 1000.0).exp(),
                    2 => {
                        if i < 90_000 {
                            0.0
                        } else {
                            f64::from(i)
                        }
                    }
                    _ => -f64::from(i),
                };
                (value, i64::from(i % 7 + 1))
            })
            .collect::<Vec<_>>();
        if distribution == 3 {
            let mut state = 7_u64;
            for i in (1..values.len()).rev() {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                values.swap(i, (state as usize) % (i + 1));
            }
        }
        let ctx = context();
        register(&ctx, &values);
        for query in [
            "T | summarize a=percentilesw_array(x,w,0,1,25,50,75,99,100) | project a=tostring(a)",
            "T | summarize s=tdigest(x,w) by g | summarize s=tdigest_merge(s) | project a=tostring(percentiles_array_tdigest(s,dynamic([0,1,25,50,75,99,100])))",
        ] {
            let result = rows(&ctx, query).await;
            let estimates: Vec<f64> = serde_json::from_str(&result[0][0]).unwrap();
            assert_rank_error(&values, &estimates);
        }
    }
}

#[tokio::test]
async fn compressed_high_magnitude_long_partial_states_preserve_offsets() {
    let ctx = context();
    let origin = i64::MAX - 20_000;
    let first = RecordBatch::try_from_iter(vec![(
        "x",
        Arc::new(Int64Array::from_iter_values((0..6000).map(|i| origin + i))) as ArrayRef,
    )])
    .unwrap();
    ctx.register_table(
        TableReference::bare("T"),
        Arc::new(MemTable::try_new(first.schema(), vec![vec![first]]).unwrap()),
    )
    .unwrap();
    let result=rows(&ctx,"T | summarize s=tdigest(x) | project p=percentile_tdigest(s,50,typeof(long)),low=percentile_tdigest(s,0,typeof(long)),high=percentile_tdigest(s,100,typeof(long))").await;
    assert!(
        (i128::from(result[0][0].parse::<i64>().unwrap()) - i128::from(origin + 2999)).abs() <= 60
    );
    assert_eq!(result[0][1], origin.to_string());
    assert_eq!(result[0][2], (origin + 5999).to_string());
}

#[tokio::test]
async fn nonfinite_samples_do_not_poison_finite_or_endpoint_estimates() {
    check("datatable(x:real)[real(-inf),1.0,2.0,real(inf)] | summarize s=tdigest(x) | project lo=percentile_tdigest(s,0,typeof(real))==real(-inf),p=percentile_tdigest(s,50,typeof(real)),hi=percentile_tdigest(s,100,typeof(real))==real(inf)",
        &[&["true","1.0","true"]]).await;
    check("range x from 1 to 10000 step 1 | extend v=iff(x<5001,real(-inf),real(inf)) | summarize s=tdigest(v) | project lo=percentile_tdigest(s,0,typeof(real))==real(-inf),hi=percentile_tdigest(s,100,typeof(real))==real(inf)",
        &[&["true","true"]]).await;
}

#[tokio::test]
async fn invalid_arguments_and_foreign_states_fail_explicitly() {
    let ctx = context();
    for query in [
        "print p=percentile_tdigest()",
        "print p=percentile_tdigest(dynamic(null),50,typeof(string))",
        "print s=tdigest_merge(dynamic(null))",
        "range x from 1 to 3 step 1 | summarize s=tdigest(x,1.5)",
        "range x from 1 to 3 step 1 | summarize s=tdigest(tostring(x))",
        "range x from 1 to 3 step 1 | summarize p=percentilew(x,1.5,50)",
        "range x from 1 to 3 step 1 | summarize p=percentile_exact(x,101)",
        "range x from 1 to 3 step 1 | summarize p=percentiles_exact(x,x)",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
    let too_many = std::iter::repeat_n("dynamic(null)", 17)
        .collect::<Vec<_>>()
        .join(",");
    assert!(
        ctx.kql(&format!("print s=tdigest_merge({too_many})"))
            .await
            .is_err()
    );
    for query in [
        "print p=percentile_tdigest(dynamic([]),50)",
        "print p=percentile_tdigest('{\"__datafusion_kql_tdigest_v1\":\"invalid!\"}',50)",
        "range x from 1 to 3 step 1 | summarize s=hll(x) | project p=percentile_tdigest(s,50)",
        "range x from 1 to 3 step 1 | summarize a=tdigest(x),b=tdigest(toreal(x)) | project s=tdigest_merge(a,b)",
        "range x from 1 to 3 step 1 | summarize s=tdigest(x) | project p=percentile_tdigest(s,101)",
        "range x from 1 to 3 step 1 | summarize s=tdigest(x) | project p=percentile_tdigest(s,real(nan))",
        "range x from 1 to 3 step 1 | summarize s=tdigest(x) | project p=percentile_tdigest(s,50,typeof(real))",
        "range x from 1 to 3 step 1 | summarize s=tdigest(x) | project p=percentiles_array_tdigest(s,dynamic([]))",
        "range x from 1 to 3 step 1 | summarize s=tdigest(x) | project p=percentiles_array_tdigest(s,dynamic([50,'bad']))",
    ] {
        let error = match ctx.kql(query).await {
            Ok(frame) => frame.collect().await.unwrap_err().to_string(),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("digest") || error.contains("percentile"),
            "{query}\n{error}"
        );
    }
}

#[tokio::test]
async fn host_memory_accounts_bounded_state_and_releases_failed_queries() {
    for (bytes, success) in [(1 << 20, true), (32 << 10, false)] {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build()
            .unwrap();
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new()
                .with_target_partitions(1)
                .with_batch_size(128),
            Arc::new(runtime),
        );
        let batch = RecordBatch::try_from_iter(vec![(
            "x",
            Arc::new(Int64Array::from_iter_values(0..100_000)) as ArrayRef,
        )])
        .unwrap();
        ctx.register_batch(TableReference::bare("T"), batch)
            .unwrap();
        let result = ctx
            .kql("T | summarize p=percentile(x,50)")
            .await
            .unwrap()
            .collect()
            .await;
        if success {
            let batches = result.unwrap();
            let estimate = array_value_to_string(batches[0].column(0), 0)
                .unwrap()
                .parse::<i64>()
                .unwrap();
            assert!((estimate - 49_999).abs() <= 1000);
        } else {
            let error = result.unwrap_err().to_string();
            assert!(
                error.contains("memory")
                    || error.contains("Memory")
                    || error.contains("Resources exhausted"),
                "{error}"
            );
        }
        tokio::task::yield_now().await;
        assert_eq!(pool.reserved(), 0);
    }
}

#[tokio::test]
async fn native_spilling_replays_percentile_states_without_losing_groups() {
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::physical_plan::{ExecutionPlan, collect};
    fn spills(plan: &dyn ExecutionPlan) -> usize {
        plan.metrics()
            .and_then(|metrics| metrics.spill_count())
            .unwrap_or(0)
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
        (
            "x",
            Arc::new(Int64Array::from_iter_values(0..200_000)) as ArrayRef,
        ),
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
    let plan=ctx.kql("T | summarize low=percentile(x,0),high=percentile(x,100) by g | summarize Lo=sum(low),Hi=sum(high),Groups=count()").await.unwrap().create_physical_plan().await.unwrap();
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    for (index, expected) in [(0, "199990000"), (1, "3799990000"), (2, "20000")] {
        assert_eq!(
            array_value_to_string(batches[0].column(index), 0).unwrap(),
            expected
        );
    }
    assert!(
        spills(plan.as_ref()) > 0,
        "the test must force actual native spilling"
    );
    drop(plan);
    tokio::task::yield_now().await;
    assert_eq!(pool.reserved(), 0);
}
