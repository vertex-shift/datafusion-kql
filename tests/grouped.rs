use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
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
