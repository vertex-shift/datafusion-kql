use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
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
    let logical = frame.logical_plan().display_indent().to_string();
    let debug = frame.clone();
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{e}\n{query}\n{logical}"));
    let actual = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.logical_nulls().is_some_and(|n| n.is_null(row)) {
                            "<null>".to_owned()
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
        .map(|r| r.iter().map(|v| v.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let physical = if actual != expected {
        let plan = debug.create_physical_plan().await.unwrap();
        datafusion::physical_plan::displayable(plan.as_ref())
            .indent(false)
            .to_string()
    } else {
        String::new()
    };
    assert_eq!(actual, expected, "{query}\n{logical}\n{physical}");
}

#[tokio::test]
async fn serialize_declares_native_row_number_without_leaking_helpers() {
    check(
        "datatable(x:long)[3,1,2] | serialize n=row_number()",
        &[&["3", "1"], &["1", "2"], &["2", "3"]],
    )
    .await;
    let frame = SessionContext::new()
        .kql("range x from 1 to 3 step 1 | serialize | project *")
        .await
        .unwrap();
    assert_eq!(
        frame
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        ["x"]
    );
}

#[tokio::test]
async fn prev_and_next_use_the_serialized_order_and_defaults() {
    check(
        "datatable(x:long)[3,1,2] | sort by x asc | extend p=prev(x),n=next(x),p2=prev(x,2,-1),same=prev(x,0)",
        &[&["1","<null>","2","-1","1"],&["2","1","3","-1","2"],&["3","2","<null>","1","3"]],
    ).await;
}

#[tokio::test]
async fn string_row_offsets_keep_non_null_string_semantics() {
    check(
        "datatable(s:string)['a','b'] | serialize | extend p=prev(s),n=next(s)",
        &[&["a", "", "b"], &["b", "a", ""]],
    )
    .await;
}

#[tokio::test]
async fn serialized_order_survives_projection_renaming_filtering_and_limits() {
    check(
        "datatable(x:long,y:long)[1,30,2,10,3,20] | sort by y asc | project x | project-rename v=x | where v!=3 | take 2 | extend p=prev(v),r=row_number()",
        &[&["2","<null>","1"],&["1","2","2"]],
    ).await;
}

#[tokio::test]
async fn row_number_start_and_restart_are_kql_not_sql_rank() {
    check(
        "datatable(g:string,x:long)['a',1,'a',2,'b',3,'b',4] | serialize | extend n=row_number(10,g!=prev(g)) | project x,n",
        &[&["1","10"],&["2","11"],&["3","10"],&["4","11"]],
    ).await;
}

#[tokio::test]
async fn cumulative_sums_can_restart_on_nested_prev_expressions() {
    check(
        "datatable(g:string,x:long)['a',1,'a',2,'b',3,'b',4] | serialize | extend total=row_cumsum(x,g!=prev(g)) | project x,total",
        &[&["1","1"],&["2","3"],&["3","3"],&["4","7"]],
    ).await;
    check(
        "datatable(x:long)[long(null),1,long(null),2] | serialize | extend total=row_cumsum(x)",
        &[&["<null>", "0"], &["1", "1"], &["<null>", "1"], &["2", "3"]],
    )
    .await;
}

#[tokio::test]
async fn dense_and_min_ranks_count_consecutive_value_runs() {
    check(
        "datatable(x:long)[1,1,2,1,1] | serialize | extend d=row_rank_dense(x),m=row_rank_min(x)",
        &[
            &["1", "1", "1"],
            &["1", "1", "1"],
            &["2", "2", "3"],
            &["1", "3", "4"],
            &["1", "3", "4"],
        ],
    )
    .await;
}

#[tokio::test]
async fn row_functions_in_predicates_are_evaluated_before_filtering() {
    check(
        "range x from 1 to 5 step 1 | sort by x asc | where x-prev(x)>0 | project x,n=row_number()",
        &[&["2", "1"], &["3", "2"], &["4", "3"], &["5", "4"]],
    )
    .await;
}

#[tokio::test]
async fn serialized_bindings_preserve_order_in_nested_queries() {
    check(
        "let t=datatable(x:long)[3,1,2] | sort by x asc; (t | project x) | extend p=prev(x)",
        &[&["1", "<null>"], &["2", "1"], &["3", "2"]],
    )
    .await;
}

#[tokio::test]
async fn serialization_survives_mv_expand_without_duplicate_ordinals() {
    check(
        "datatable(id:long,a:dynamic)[2,dynamic([20,21]),1,dynamic([10,11])] | sort by id asc | mv-expand a to typeof(long) | project a,p=prev(a)",
        &[&["10","<null>"],&["11","10"],&["20","11"],&["21","20"]],
    ).await;
}

#[tokio::test]
async fn dynamic_offsets_do_not_repeat_first_values_for_null_padding() {
    check(
        "datatable(d:dynamic)[dynamic({'x':1}),dynamic(null),dynamic({'x':3})] | serialize | extend p=prev(d),n=next(d) | project d=tostring(d),p=tostring(p),n=tostring(n)",
        &[&["{\"x\":1}","",""],&["","{\"x\":1}","{\"x\":3}"],&["{\"x\":3}","",""]],
    ).await;
    check(
        "datatable(d:dynamic)[dynamic(datetime(2025-01-01)),dynamic(1h)] | serialize | extend p=prev(d) | project type=gettype(p)",
        &[&["null"],&["datetime"]],
    ).await;
}

#[tokio::test]
async fn ordered_collections_remain_ordered_after_sort_key_projection() {
    check(
        "datatable(x:long,y:long)[1,30,2,10,3,20] | sort by y asc | project x | summarize a=make_list(x) | project a=tostring(a)",
        &[&["[2,3,1]"]],
    ).await;
}

#[tokio::test]
async fn unordered_operators_remove_serialization_and_require_a_new_boundary() {
    let ctx = SessionContext::new();
    for query in [
        "range x from 1 to 3 step 1 | extend p=prev(x)",
        "range x from 1 to 3 step 1 | serialize | summarize N=count() | extend p=prev(N)",
        "range x from 1 to 3 step 1 | serialize | distinct x | extend p=prev(x)",
        "let t=range x from 1 to 3 step 1 | serialize; t | union t | extend p=prev(x)",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}

#[tokio::test]
async fn offsets_are_constant_nonnegative_and_restart_flags_are_boolean() {
    let ctx = SessionContext::new();
    for query in [
        "range x from 1 to 3 step 1 | serialize | extend p=prev(x,-1)",
        "range x from 1 to 3 step 1 | serialize | extend p=prev(x,x)",
        "range x from 1 to 3 step 1 | serialize | extend n=row_number(1,42)",
        "range x from 1 to 3 step 1 | serialize | extend n=row_cumsum('x')",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}

#[tokio::test]
async fn helper_names_do_not_hide_user_columns_that_share_the_prefix() {
    check(
        "print __kql_row_order_0=42,__kql_window_column_0=7 | serialize n=row_number() | project *",
        &[&["42", "7", "1"]],
    )
    .await;
}

#[tokio::test]
async fn row_offsets_and_cumulative_sums_cross_batches_and_partitions() {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(2),
    );
    let first =
        RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from(vec![5, 1, 3])) as _)])
            .unwrap();
    let second =
        RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from(vec![6, 2, 4])) as _)])
            .unwrap();
    ctx.register_table(
        TableReference::bare("T"),
        Arc::new(MemTable::try_new(first.schema(), vec![vec![first], vec![second]]).unwrap()),
    )
    .unwrap();
    let batches = ctx
        .kql("T | sort by x asc | extend p=prev(x),s=row_cumsum(x)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let values = batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(|row| {
                [
                    array_value_to_string(b.column(0), row).unwrap(),
                    if b.column(1).is_null(row) {
                        "null".to_owned()
                    } else {
                        array_value_to_string(b.column(1), row).unwrap()
                    },
                    array_value_to_string(b.column(2), row).unwrap(),
                ]
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![
            ["1", "null", "1"],
            ["2", "1", "3"],
            ["3", "2", "6"],
            ["4", "3", "10"],
            ["5", "4", "15"],
            ["6", "5", "21"],
        ]
    );
}

#[tokio::test]
async fn session_windows_restart_at_exact_boundaries_and_after_nulls() {
    check(
        "datatable(t:datetime)[datetime(2025-01-01T00:00:00Z),datetime(2025-01-01T00:02:00Z),datetime(2025-01-01T00:04:00Z),datetime(null),datetime(2025-01-01T00:05:00Z)] | serialize | extend s=row_window_session(t,4m,3m) | project s",
        &[&["2025-01-01T00:00:00"],&["2025-01-01T00:00:00"],&["2025-01-01T00:04:00"],&["<null>"],&["2025-01-01T00:05:00"]],
    ).await;
    check(
        "datatable(t:datetime)[datetime(2025-01-01T00:00:00Z),datetime(2025-01-01T00:05:00Z)] | serialize | extend s=row_window_session(t,1h,5m) | project s",
        &[&["2025-01-01T00:00:00"],&["2025-01-01T00:05:00"]],
    ).await;
}

#[tokio::test]
async fn session_windows_support_restart_predicates_and_bounded_execution() {
    use datafusion::physical_plan::displayable;
    let query = "datatable(g:string,t:datetime)['a',datetime(2025-01-01T00:00:00Z),'a',datetime(2025-01-01T00:01:00Z),'b',datetime(2025-01-01T00:02:00Z)] | sort by t asc | extend s=row_window_session(t,1h,5m,g!=prev(g)) | project g,s";
    check(
        query,
        &[
            &["a", "2025-01-01T00:00:00"],
            &["a", "2025-01-01T00:00:00"],
            &["b", "2025-01-01T00:02:00"],
        ],
    )
    .await;
    let physical = SessionContext::new()
        .kql(query)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let display = displayable(physical.as_ref()).indent(false).to_string();
    assert!(display.contains("BoundedWindowAggExec"), "{display}");
}

#[tokio::test]
async fn repeated_row_function_stages_preserve_user_values_and_order() {
    for _ in 0..8 {
        check(
            "range x from 1 to 4 step 1 | sort by x desc | extend a=prev(x) | project x,a | extend b=next(a),n=row_number()",
            &[&["4","<null>","4","1"],&["3","4","3","2"],&["2","3","2","3"],&["1","2","<null>","4"]],
        ).await;
    }
}
