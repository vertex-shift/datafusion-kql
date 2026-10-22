use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::TableReference;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

async fn result(ctx: &SessionContext, query: &str) -> Vec<Vec<String>> {
    let frame = ctx
        .kql(query)
        .await
        .unwrap_or_else(|e| panic!("{}\n{query}", e.diagnostic(query)));
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{e}\n{query}"));
    batches
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
                    .collect()
            })
        })
        .collect()
}

async fn check(query: &str, expected: &[&[&str]]) {
    let actual = result(&SessionContext::new(), query).await;
    let expected = expected
        .iter()
        .map(|row| row.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{query}");
}

#[tokio::test]
async fn limits_and_top_counts_accept_constant_bindings_and_arithmetic() {
    check(
        "let n=2; range x from 1 to 5 step 1 | take n+1 | count",
        &[&["3"]],
    )
    .await;
    check(
        "let n=2; range x from 1 to 5 step 1 | top n by x",
        &[&["5"], &["4"]],
    )
    .await;
    let ctx = SessionContext::new();
    for query in [
        "let n=-1; range x from 1 to 5 step 1 | take n",
        "let n=2.5; range x from 1 to 5 step 1 | take n",
        "range x from 1 to 5 step 1 | take x",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}

#[tokio::test]
async fn projection_reordering_supports_wildcards_and_natural_numeric_names() {
    check(
        "print b=0,a10=10,a2=2,a1=1 | project-reorder a* granny-asc",
        &[&["1", "2", "10", "0"]],
    )
    .await;
    check(
        "print b=0,a10=10,a2=2,a1=1 | project-reorder a* asc",
        &[&["1", "10", "2", "0"]],
    )
    .await;
    check(
        "print b=0,a10=10,a2=2,a1=1 | project-reorder a* granny-desc",
        &[&["10", "2", "1", "0"]],
    )
    .await;
    check(
        "print a=1,b=2,c=3 | project-reorder b,*,b",
        &[&["2", "1", "3"]],
    )
    .await;
}

#[tokio::test]
async fn getschema_reports_kql_types_and_excludes_internal_order_fields() {
    check(
        "print a=int(1),b=long(2),c=dynamic([1]),d=1h | serialize | getschema",
        &[
            &["a", "0", "System.Int32", "int"],
            &["b", "1", "System.Int64", "long"],
            &["c", "2", "System.Object", "dynamic"],
            &["d", "3", "System.TimeSpan", "timespan"],
        ],
    )
    .await;
}

#[tokio::test]
async fn as_binds_a_lazy_intermediate_tabular_value() {
    check(
        "range x from 1 to 3 step 1 | where x>1 | as Recent | union Recent | count",
        &[&["4"]],
    )
    .await;
    assert!(
        SessionContext::new()
            .kql("let x=1; print a=2 | as x")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn lookup_defaults_to_leftouter_without_deduplicating_left_rows() {
    check(
        "let L=datatable(k:long,v:string)[1,'a',1,'b',2,'c']; let R=datatable(k:long,name:string)[1,'one']; L | lookup R on k | sort by v asc",
        &[&["1","a","one"],&["1","b","one"],&["2","c",""]],
    ).await;
}

#[tokio::test]
async fn lookup_supports_inner_and_different_key_names_and_drops_right_keys() {
    check(
        "let L=datatable(k:long,v:string)[1,'a',2,'b']; let R=datatable(id:long,v:string)[1,'one']; L | lookup kind=inner R on $left.k==$right.id",
        &[&["1","a","one"]],
    ).await;
    assert!(
        SessionContext::new()
            .kql("print k=1 | lookup kind=fullouter (print k=1) on k")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn outer_union_splits_conflicting_types_without_coercing_values() {
    check(
        "let A=datatable(x:long)[1]; let B=datatable(x:string)['hello']; union A,B | project x_long,x_string | sort by x_long asc",
        &[&["<null>","hello"],&["1",""]],
    ).await;
}

#[tokio::test]
async fn union_suffixes_do_not_overwrite_real_input_columns() {
    check(
        "let A=datatable(x:long,x_long:string)[1,'original']; let B=datatable(x:string)['hello']; union A,B | project x_long1,x_long,x_string | sort by x_long1 asc",
        &[&["<null>","","hello"],&["1","original",""]],
    ).await;
}

#[tokio::test]
async fn inner_union_keeps_only_common_name_and_type_pairs() {
    check(
        "let A=datatable(k:long,x:long)[1,10]; let B=datatable(k:long,x:string)[2,'two']; union kind=inner A,B | sort by k asc",
        &[&["1"],&["2"]],
    ).await;
}

#[tokio::test]
async fn union_withsource_preserves_named_source_labels() {
    check(
        "let A=datatable(x:long)[1]; let B=datatable(x:long)[2]; union withsource=Source kind=outer A,B | sort by Source asc",
        &[&["A","1"],&["B","2"]],
    ).await;
    check(
        "let A=datatable(x:long)[1]; let B=datatable(x:long)[2]; A | union withsource=Source B | sort by Source asc",
        &[&["A","1"],&["B","2"]],
    ).await;
}

#[tokio::test]
async fn wildcard_union_resolves_registered_tables_and_labels_each_leg() {
    let ctx = SessionContext::new();
    for (name, value) in [("EventsOne", 1), ("EventsTwo", 2), ("Other", 3)] {
        let batch =
            RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from(vec![value])) as _)])
                .unwrap();
        ctx.register_batch(TableReference::bare(name), batch)
            .unwrap();
    }
    let actual = result(&ctx, "union withsource=Source Events* | sort by Source asc").await;
    assert_eq!(actual, vec![vec!["EventsOne", "1"], vec!["EventsTwo", "2"]]);
    let actual = result(&ctx, "union public.Events* | count").await;
    assert_eq!(actual, vec![vec!["2"]]);
    assert!(ctx.kql("union NoSuchTable*").await.is_err());
}

#[tokio::test]
async fn sample_and_sample_distinct_have_correct_counts_and_uniqueness() {
    check("range x from 1 to 100 step 1 | sample 5 | count", &[&["5"]]).await;
    check("range x from 1 to 3 step 1 | sample 10 | count", &[&["3"]]).await;
    check("range x from 1 to 3 step 1 | sample 0 | count", &[&["0"]]).await;
    check(
        "range x from 1 to 10 step 1 | extend g=x%2 | sample-distinct 3 of g | sort by g asc",
        &[&["0"], &["1"]],
    )
    .await;
}

#[tokio::test]
async fn reordering_preserves_serialized_row_semantics() {
    check("range x from 1 to 3 step 1 | serialize n=row_number() | project-reorder n | extend p=prev(x)",&[
        &["1","1","<null>"],&["2","2","1"],&["3","3","2"],
    ]).await;
}
