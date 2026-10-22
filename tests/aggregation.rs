use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

async fn check(query: &str, expected: &[&[&str]]) {
    let frame = SessionContext::new()
        .kql(query)
        .await
        .unwrap_or_else(|e| panic!("{}\n{query}", e.diagnostic(query)));
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{e}\n{query}"));
    let actual = batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(|row| {
                b.columns()
                    .iter()
                    .map(|column| {
                        if column.logical_nulls().is_some_and(|n| n.is_null(row)) {
                            "<null>".into()
                        } else {
                            array_value_to_string(column, row).unwrap()
                        }
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    let expected = expected
        .iter()
        .map(|row| row.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{query}");
}

#[tokio::test]
async fn scalar_expressions_over_aggregates_lower_to_native_projection() {
    check(
        "range x from 1 to 4 step 1 | summarize Mean=todouble(sum(x))/count(), HasData=iff(count()>0,'yes','no'), Difference=max(x)-min(x)",
        &[&["2.5","yes","3"]],
    ).await;
    check(
        "datatable(x:long)[] | summarize Ratio=todouble(sum(x))/count(), Default=coalesce(avg(x),0.0)",
        &[&["NaN","0.0"]],
    ).await;
}

#[tokio::test]
async fn aggregate_arithmetic_resolves_group_keys_but_not_input_rows() {
    check(
        "datatable(g:long,x:long)[1,10,1,20,2,30] | summarize Value=sum(x)+g by g | sort by g asc",
        &[&["1", "31"], &["2", "32"]],
    )
    .await;
    assert!(
        SessionContext::new()
            .kql("range x from 1 to 4 step 1 | summarize Invalid=sum(x)+x")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn timespan_sum_and_average_keep_the_duration_type() {
    check(
        "datatable(d:timespan)[1h,2h,timespan(null)] | summarize Total=sum(d), Average=avg(d), Selected=sumif(d,d>1h)",
        &[&["PT10800S","PT5400S","PT7200S"]],
    ).await;
    check(
        "datatable(d:timespan)[] | summarize sum(d),avg(d)",
        &[&["P0D", "<null>"]],
    )
    .await;
}

#[tokio::test]
async fn bounded_list_set_and_conditional_aggregates() {
    check(
        "datatable(x:long)[1,2,2,long(null),3] | summarize a=make_list(x,3), b=make_set(x,2), c=make_list_if(x,x>1,2), d=make_list_with_nulls(x) | project a=tostring(a),b=tostring(b),c=tostring(c),d=tostring(d)",
        &[&["[1,2,2]","[1,2]","[2,2]","[1,2,2,null,3]"]],
    ).await;
}

#[tokio::test]
async fn collection_aggregates_default_to_empty_arrays_and_bags() {
    check(
        "datatable(x:long,d:dynamic)[] | summarize a=make_list(x),b=make_set(x),c=make_bag(d) | project a=tostring(a),b=tostring(b),c=tostring(c)",
        &[&["[]","[]","{}"]],
    ).await;
}

#[tokio::test]
async fn list_order_tracks_sorted_input() {
    check(
        "range x from 1 to 5 step 1 | sort by x desc | summarize a=make_list(x) | project a=tostring(a)",
        &[&["[5,4,3,2,1]"]],
    ).await;
}

#[tokio::test]
async fn bag_aggregation_merges_properties_and_supports_conditions_and_limits() {
    check(
        "datatable(d:dynamic,k:long)[dynamic({'a':1}),1,dynamic({'b':2}),2,dynamic({'a':99,'c':3}),3] | summarize a=make_bag(d),b=make_bag_if(d,k>1,1) | project a=tostring(a),b=tostring(b)",
        &[&["{\"a\":1,\"b\":2,\"c\":3}","{\"b\":2}"]],
    ).await;
}

#[tokio::test]
async fn collection_values_remain_typed_dynamic_and_can_be_expanded() {
    check(
        "range x from 1 to 3 step 1 | summarize Values=make_list(x) | mv-expand Values to typeof(long) | sort by Values asc",
        &[&["1"],&["2"],&["3"]],
    ).await;
    check(
        "datatable(d:dynamic)[dynamic({'x':1}),dynamic({'x':2})] | summarize a=make_list(d) | mv-expand a | project x=tolong(a.x) | sort by x asc",
        &[&["1"],&["2"]],
    ).await;
}

#[tokio::test]
async fn nearest_rank_percentiles_do_not_interpolate_or_round_large_integers() {
    check(
        "datatable(x:long)[1,2,3,4] | summarize percentile(x,0),percentile(x,50),percentile(x,100)",
        &[&["1", "2", "4"]],
    )
    .await;
    check(
        "datatable(x:long)[9007199254740993,9007199254740995] | summarize p=percentile(x,50)",
        &[&["9007199254740993"]],
    )
    .await;
}

#[tokio::test]
async fn multi_percentile_outputs_share_an_aggregate_state() {
    use datafusion::logical_expr::LogicalPlan;
    let ctx = SessionContext::new();
    let frame = ctx
        .kql("range x from 1 to 100 step 1 | summarize percentiles(x,5,50,95)")
        .await
        .unwrap();
    assert_eq!(
        frame
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        ["percentile_x_5", "percentile_x_50", "percentile_x_95"]
    );
    let LogicalPlan::Projection(project) = frame.logical_plan() else {
        panic!("projection expected");
    };
    let LogicalPlan::Aggregate(aggregate) = project.input.as_ref() else {
        panic!("aggregate expected");
    };
    assert_eq!(aggregate.aggr_expr.len(), 1);
    check(
        "range x from 1 to 100 step 1 | summarize percentiles(x,5,50,95)",
        &[&["5", "50", "95"]],
    )
    .await;
}

#[tokio::test]
async fn percentile_arrays_support_constant_dynamic_rank_lists() {
    check(
        "range x from 1 to 10 step 1 | summarize p=percentiles_array(x,dynamic([0,50,100])) | project p=tostring(p)",
        &[&["[1,5,10]"]],
    ).await;
    check(
        "datatable(x:real)[] | summarize p=percentile(x,50),a=percentiles_array(x,50,90) | project p,a=tostring(a)",
        &[&["<null>",""]],
    ).await;
}

#[tokio::test]
async fn weighted_percentiles_use_weights_without_repeating_rows() {
    check(
        "datatable(x:long,w:long)[10,1,20,8,30,1] | summarize p=percentilew(x,w,50),a=percentilesw_array(x,w,0,50,100) | project p,a=tostring(a)",
        &[&["20","[10,20,30]"]],
    ).await;
}

#[tokio::test]
async fn arg_max_and_arg_min_return_correlated_rows() {
    check(
        "datatable(g:string,x:long,label:string)['a',1,'one','a',3,'three','b',2,'two'] | summarize arg_max(x,*) by g | sort by g asc",
        &[&["a","3","three"],&["b","2","two"]],
    ).await;
    check(
        "datatable(x:long,label:string)[long(null),'null',2,'two',1,'one'] | summarize arg_min(x,label)",
        &[&["1","one"]],
    ).await;
}

#[tokio::test]
async fn arg_selection_all_null_keys_still_returns_a_row() {
    check(
        "datatable(x:long,label:string)[long(null),'first',long(null),'second'] | summarize arg_max(x,label)",
        &[&["<null>","first"]],
    ).await;
}

#[tokio::test]
async fn take_any_multiple_values_are_selected_from_one_row() {
    check(
        "datatable(x:long,label:string)[1,'one',2,'two'] | summarize take_any(*)",
        &[&["1", "one"]],
    )
    .await;
    check(
        "datatable(x:long)[1,2,3] | summarize value=take_anyif(x,x>1)",
        &[&["2"]],
    )
    .await;
}

#[tokio::test]
async fn partial_and_final_aggregation_merge_collection_and_percentile_states() {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let first = RecordBatch::try_from_iter(vec![
        ("x", Arc::new(Int64Array::from(vec![1, 2])) as _),
        ("g", Arc::new(StringArray::from(vec!["a", "a"])) as _),
    ])
    .unwrap();
    let second = RecordBatch::try_from_iter(vec![
        ("x", Arc::new(Int64Array::from(vec![3, 4])) as _),
        ("g", Arc::new(StringArray::from(vec!["a", "a"])) as _),
    ])
    .unwrap();
    ctx.register_table(
        TableReference::bare("T"),
        Arc::new(MemTable::try_new(first.schema(), vec![vec![first], vec![second]]).unwrap()),
    )
    .unwrap();
    let batches = ctx
        .kql("T | summarize a=make_set(x),p=percentile(x,50) by g | project g,n=array_length(a),p")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(array_value_to_string(batches[0].column(1), 0).unwrap(), "4");
    assert_eq!(array_value_to_string(batches[0].column(2), 0).unwrap(), "2");
}

#[tokio::test]
async fn invalid_aggregate_parameters_fail_explicitly() {
    let ctx = SessionContext::new();
    for query in [
        "range x from 1 to 2 step 1 | summarize make_list(x,0)",
        "range x from 1 to 2 step 1 | summarize make_set(x,1048577)",
        "range x from 1 to 2 step 1 | summarize percentile(x,101)",
        "range x from 1 to 2 step 1 | summarize percentile(x,-1)",
        "range x from 1 to 2 step 1 | summarize percentile(x,x)",
        "range x from 1 to 2 step 1 | summarize make_list(x,x)",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}

#[tokio::test]
async fn conditional_distinct_and_population_variants_keep_all_modifiers() {
    use datafusion::arrow::array::Float64Array;
    let batches = SessionContext::new().kql(
        "datatable(x:long)[1,2,2,3,long(null)] | summarize N=count_distinctif(x,x>1), V=variancepif(x,x>1), S=stdevpif(x,x>1)",
    ).await.unwrap().collect().await.unwrap();
    assert_eq!(array_value_to_string(batches[0].column(0), 0).unwrap(), "2");
    for (index, expected) in [(1, 2.0_f64 / 9.0), (2, (2.0_f64 / 9.0).sqrt())] {
        let values = batches[0]
            .column(index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((values.value(0) - expected).abs() < 1e-12);
    }
}

#[tokio::test]
async fn dynamic_distinct_counts_ignore_nulls() {
    check(
        "datatable(d:dynamic)[dynamic({'x':1}),dynamic({'x':1}),dynamic({'x':2}),dynamic(null)] | summarize Exact=count_distinct(d),Approx=dcount(d)",
        &[&["2","2"]],
    ).await;
}

#[tokio::test]
async fn take_any_single_column_prefers_nonempty_nonnull_values() {
    check(
        "datatable(x:long)[long(null),1] | summarize take_any(x)",
        &[&["1"]],
    )
    .await;
    check(
        "datatable(x:string)['','yes'] | summarize take_any(x)",
        &[&["yes"]],
    )
    .await;
    check(
        "range x from 1 to 5 step 1 | sort by x desc | summarize a=make_list_if(x,x>1,2) | project a=tostring(a)",
        &[&["[5,4]"]],
    ).await;
}
