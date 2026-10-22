use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::array_value_to_string;
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
    let rows = batches
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
                            "<null>".into()
                        } else {
                            array_value_to_string(array, row).unwrap()
                        }
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    let expected = expected
        .iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(rows, expected, "{query}");
}

const DATA: &str =
    "datatable(r:string,c:string,v:long)['P1','a',10,'P1','b',9,'P2','a',5,'P2','b',15]";

#[tokio::test]
async fn top_hitters_use_exact_native_counts_and_sums() {
    check(
        "datatable(x:string)['a','b','a','c','a','b'] | top-hitters 2 of x",
        &[&["a", "3"], &["b", "2"]],
    )
    .await;
    check(
        &format!("{DATA} | top-hitters 1 of r by v"),
        &[&["P2", "20"]],
    )
    .await;
    check(
        "datatable(x:string)['a','b','a','c','a','b'] | top-hitters 2.9 of x",
        &[&["a", "3"], &["b", "2"]],
    )
    .await;
    check("datatable(d:dynamic)[dynamic([1]),dynamic([2]),dynamic([1])] | top-hitters 1 of d | project d=tostring(d),approximate_count_d", &[&["[1]","2"]]).await;
}

#[tokio::test]
async fn top_nested_single_level_has_named_keys_and_aggregate_columns() {
    check(
        &format!("{DATA} | top-nested 1 of r by sum(v)"),
        &[&["P2", "20"]],
    )
    .await;
    check(
        &format!("{DATA} | top-nested 1 of Region=r by Total=sum(v) asc"),
        &[&["P1", "19"]],
    )
    .await;
}

#[tokio::test]
async fn top_nested_retains_parent_totals_before_child_filtering() {
    check(
        &format!("{DATA} | top-nested 1 of r by sum(v), top-nested 1 of c by sum(v)"),
        &[&["P2", "20", "b", "15"]],
    )
    .await;
    check(
        &format!(
            "{DATA} | top-nested 2 of r by sum(v), top-nested 1 of c by sum(v) | sort by r asc"
        ),
        &[&["P1", "19", "a", "10"], &["P2", "20", "b", "15"]],
    )
    .await;
}

#[tokio::test]
async fn top_nested_omitted_counts_retain_all_groups_and_support_expressions() {
    check(&format!("{DATA} | top-nested of r by Ignore=max(1), top-nested 1 of c by Score=sum(v)*2 | project r,c,Score | sort by r asc"),
        &[&["P1","a","20"],&["P2","b","30"]]).await;
    check(
        "range x from 1 to 4 step 1 | top-nested of bin(x,2) by count() | sort by x asc",
        &[&["0", "1"], &["2", "2"], &["4", "1"]],
    )
    .await;
}

#[tokio::test]
async fn top_nested_others_reaggregates_rejected_rows() {
    check(
        &format!("{DATA} | top-nested 1 of r with others='Other' by sum(v) | sort by r asc"),
        &[&["Other", "19"], &["P2", "20"]],
    )
    .await;
    check(&format!("{DATA} | top-nested 1 of r with others='Other' by sum(v), top-nested 1 of c by sum(v) | sort by r asc"),
        &[&["Other","19","a","10"],&["P2","20","b","15"]]).await;
    check(
        &format!("{DATA} | top-nested 0 of r with others='All' by sum(v)"),
        &[&["All", "39"]],
    )
    .await;
}

#[tokio::test]
async fn top_nested_others_recomputes_nonadditive_aggregates_from_source_rows() {
    check("datatable(k:string,v:long)['a',100,'a',100,'b',10,'c',20,'c',40] | top-nested 1 of k with others='rest' by avg(v) | sort by k asc",
        &[&["a","100.0"],&["rest","23.333333333333332"]]).await;
}

#[tokio::test]
async fn top_nested_null_keys_and_dynamic_group_keys_are_retained() {
    check(
        "datatable(k:long,v:long)[long(null),10,1,2,2,1] | top-nested 1 of k by sum(v)",
        &[&["<null>", "10"]],
    )
    .await;
    check("datatable(k:dynamic,v:long)[dynamic([1]),10,dynamic([1]),5,dynamic([2]),2] | top-nested 1 of k by sum(v) | project k=tostring(k),aggregated_k", &[&["[1]","15"]]).await;
}

#[tokio::test]
async fn top_nested_empty_inputs_and_zero_counts_remain_empty() {
    check(
        "datatable(k:string,v:long)[] | top-nested 2 of k by sum(v) | count",
        &[&["0"]],
    )
    .await;
    check(
        &format!("{DATA} | top-nested 0 of r by sum(v) | count"),
        &[&["0"]],
    )
    .await;
    check(
        &format!("{DATA} | top-nested 2 of r by sum(v), top-nested 0 of c by sum(v) | count"),
        &[&["0"]],
    )
    .await;
}

#[tokio::test]
async fn invalid_top_definitions_fail_explicitly() {
    let ctx = SessionContext::new();
    for query in [
        "print x=1 | top-hitters -1 of x",
        "print x=1 | top-hitters 1 of x by 'wrong'",
        "print x=1 | top-nested 1.5 of x by count()",
        "print x=1 | top-nested 1 of x by Missing",
        "print x=1 | top-nested 1 of x by x=count()",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}
