use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

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
    let frame = SessionContext::new()
        .kql(query)
        .await
        .unwrap_or_else(|error| panic!("{}\n{query}", error.diagnostic(query)));
    let expected = expected
        .iter()
        .map(|row| row.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(rows(frame).await, expected, "{query}");
}

#[tokio::test]
async fn toscalar_selects_the_first_row_and_first_column() {
    check(
        "print n=toscalar(datatable(x:long,y:string)[3,'a',4,'b'])",
        &[&["3"]],
    )
    .await;
    check("let T=range x from 1 to 5 step 1; let n=toscalar(T | count); T | where x>n-2 | sort by x asc", &[&["4"],&["5"]]).await;
    check(
        "let T=range x from 1 to 5 step 1; print n=toscalar(T)",
        &[&["1"]],
    )
    .await;
    check(
        "let F=(){range x from 3 to 4 step 1}; print n=toscalar(F())",
        &[&["3"]],
    )
    .await;
}

#[tokio::test]
async fn empty_scalar_subqueries_have_typed_null_or_empty_string_results() {
    check(
        "print n=toscalar(datatable(x:long)[]), s=toscalar(datatable(x:string)[])",
        &[&["<null>", ""]],
    )
    .await;
    check("print d=gettype(toscalar(datatable(x:dynamic)[])), n=tostring(toscalar(datatable(x:dynamic)[dynamic([1,2])]))", &[&["null","[1,2]"]]).await;
}

#[tokio::test]
async fn scalar_subqueries_work_in_projections_filters_and_aggregates() {
    check("range x from 1 to 4 step 1 | extend n=toscalar(range y from 1 to 3 step 1 | count) | where x<=n | summarize Total=sum(x), N=take_any(n)", &[&["6","3"]]).await;
    check(
        "print n=toscalar(1+2), m=toscalar(print n=toscalar(print x=4)+1)",
        &[&["3", "5"]],
    )
    .await;
    check(
        "declare query_parameters(n:long=3); print value=toscalar(print x=n*2)",
        &[&["6"]],
    )
    .await;
}

#[tokio::test]
async fn toscalar_is_lazy_and_freezes_volatile_expressions_once() {
    let ctx = SessionContext::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    ctx.register_udf(create_udf(
        "observe",
        vec![],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(move |_| {
            Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Int64(Some(
                    counter.fetch_add(1, Ordering::SeqCst) as i64,
                )),
            ))
        }),
    ));
    let frame = ctx
        .kql("let n=toscalar(observe()); range x from 1 to 3 step 1 | project n,n2=n")
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        rows(frame).await,
        vec![vec!["0", "0"], vec!["0", "0"], vec!["0", "0"]]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn scalar_subqueries_do_not_capture_caller_rows() {
    for query in [
        "range x from 1 to 3 step 1 | extend n=toscalar(x)",
        "range x from 1 to 3 step 1 | extend n=toscalar(print y=x)",
        "print x=toscalar()",
        "print x=toscalar(1,2)",
        "print x=toscalar(n=1)",
        "print x=toscalar(print a=1 | project-away *)",
    ] {
        assert!(SessionContext::new().kql(query).await.is_err(), "{query}");
    }
}

#[tokio::test]
async fn views_participate_in_wildcard_discovery_but_ordinary_functions_do_not() {
    check(
        "let T_view=view (){print x=1}; let T_notview=(){print x=2}; union T* | project x",
        &[&["1"]],
    )
    .await;
    check("let A=view (){print x=1}; let B=view (){print x=2}; union withsource=Source * | sort by Source asc", &[&["A","1"],&["B","2"]]).await;
    check("let A=view (){print x=1}; A() | count", &[&["1"]]).await;
}

#[tokio::test]
async fn view_scopes_shadow_providers_and_retain_lexical_values() {
    let ctx = SessionContext::new();
    ctx.register_batch(
        "view1",
        datafusion::arrow::record_batch::RecordBatch::try_from_iter(vec![(
            "x",
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![99])) as _,
        )])
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        rows(
            ctx.kql("let view1=view (){print x=1}; union view*")
                .await
                .unwrap()
        )
        .await,
        vec![vec!["1"]]
    );
    check(
        "let n=3; let A=view (){print x=n}; (let n=99; union A*)",
        &[&["3"]],
    )
    .await;
    for query in [
        "let A=view (x:long){print x}; A(1)",
        "let A=view (){1}; print x=1",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}
