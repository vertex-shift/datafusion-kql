use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::ScalarValue;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::*;
use datafusion_kql::{QueryParameters, SessionContextKqlExt, parse};

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
    let ctx = SessionContext::new();
    let frame = ctx
        .kql(query)
        .await
        .unwrap_or_else(|error| panic!("{}\n{query}", error.diagnostic(query)));
    let actual = rows(frame).await;
    let expected = expected
        .iter()
        .map(|row| row.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{query}");
}

#[tokio::test]
async fn query_parameters_have_defaults_and_can_reuse_an_ast() {
    let query =
        parse("declare query_parameters(n:long=2); range x from 1 to 5 step 1 | where x>n | count")
            .unwrap();
    let ctx = SessionContext::new();
    assert_eq!(
        rows(ctx.kql_ast(&query).await.unwrap()).await,
        vec![vec!["3"]]
    );
    for (value, expected) in [(1, "4"), (4, "1"), (8, "0")] {
        let parameters = QueryParameters::from([("n".into(), ScalarValue::Int64(Some(value)))]);
        assert_eq!(
            rows(
                ctx.kql_ast_with_parameters(&query, &parameters)
                    .await
                    .unwrap()
            )
            .await,
            vec![vec![expected]]
        );
    }
}

#[tokio::test]
async fn parameter_strings_are_data_and_never_query_source() {
    let text = "'; print compromised=true; //";
    let parameters = QueryParameters::from([("text".into(), ScalarValue::Utf8(Some(text.into())))]);
    let ctx = SessionContext::new();
    assert_eq!(
        rows(
            ctx.kql_with_parameters(
                "declare query_parameters(text:string); print Value=text",
                &parameters
            )
            .await
            .unwrap()
        )
        .await,
        vec![vec![text]]
    );
}

#[tokio::test]
async fn parameter_types_nulls_and_literal_defaults_are_checked() {
    check(
        "declare query_parameters(i:int=1,b:bool=bool(null),s:string=string(null)); print i,b,s",
        &[&["1", "<null>", ""]],
    )
    .await;
    let ctx = SessionContext::new();
    for query in [
        "declare query_parameters(n:long); print n",
        "declare query_parameters(n:long='wrong'); print n",
        "declare query_parameters(n:long=1+2); print n",
        "declare query_parameters(n:dynamic=dynamic([1])); print n",
        "declare query_parameters(n:long=1,n:long=2); print n",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
    let parameters = QueryParameters::from([("n".into(), ScalarValue::Utf8(Some("1".into())))]);
    assert!(
        ctx.kql_with_parameters("declare query_parameters(n:long); print n", &parameters)
            .await
            .is_err()
    );
    assert!(
        ctx.kql_with_parameters(
            "print n",
            &QueryParameters::from([("n".into(), ScalarValue::Int64(Some(1)))])
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn parameter_datetimes_timespans_and_dynamic_values_retain_types() {
    let ctx = SessionContext::new();
    let dynamic = ctx
        .kql("print d=dynamic([1,2])")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let value = ScalarValue::try_from_array(dynamic[0].column(0), 0).unwrap();
    let parameters = QueryParameters::from([
        ("t".into(), ScalarValue::TimestampNanosecond(Some(0), None)),
        (
            "d".into(),
            ScalarValue::DurationNanosecond(Some(1_000_000_000)),
        ),
        ("a".into(), value),
    ]);
    assert_eq!(rows(ctx.kql_with_parameters("declare query_parameters(t:datetime,d:timespan,a:dynamic); print t+d,array_length(a)", &parameters).await.unwrap()).await, vec![vec!["1970-01-01T00:00:01","2"]]);
}

#[tokio::test]
async fn scalar_functions_accept_row_values_defaults_and_named_arguments() {
    check(
        "let Add7=(x:long=5){x+7}; range x from 1 to 3 step 1 | project a=Add7(x),b=Add7()",
        &[&["8", "12"], &["9", "12"], &["10", "12"]],
    )
    .await;
    check("let f=(a:long,b:string='b.default',c:long=0){strcat(a,'-',b,'-',c)}; print a=f(12,c=7),b=f(c=7,a=12)", &[&["12-b.default-7","12-b.default-7"]]).await;
}

#[tokio::test]
async fn scalar_captures_are_lexical_and_hygienic() {
    check(
        "let a=3; let f=(x:long){a+x}; print a=99,x=1 | project v=f(x)",
        &[&["4"]],
    )
    .await;
    check(
        "let c=10; let f=(){c}; print __kql_function_capture_0=99 | project v=f()",
        &[&["10"]],
    )
    .await;
    check(
        "let a=3; let f=(){a}; union (let a=99; print v=f()),(print v=f()) | sort by v asc",
        &[&["3"], &["3"]],
    )
    .await;
}

#[tokio::test]
async fn nested_functions_capture_arguments_and_local_bindings() {
    check("let Test=(id:int){let Test2=10; let Test3=10+Test2+id; let Test4=(arg:int){let Test5=20; Test2+Test3+Test5+arg}; Test4(10)}; range x from 1 to Test(10) step 1 | count", &[&["70"]]).await;
    check(
        "let g=(){1}; let f=(){g()}; (let g=(){99}; print v=f())",
        &[&["1"]],
    )
    .await;
}

#[tokio::test]
async fn zero_argument_scalar_functions_work_as_names_and_calls() {
    check("let f=(){123}; print a=f,b=f()", &[&["123", "123"]]).await;
    let frame = SessionContext::new()
        .kql("let f=(){123}; print f")
        .await
        .unwrap();
    assert_eq!(frame.schema().field(0).name(), "f");
}

#[tokio::test]
async fn dynamic_function_parameters_support_properties_and_indices() {
    check("let f=(d:dynamic){tolong(d.values[-1])+tolong(d.offset)}; print v=f(dynamic({'values':[1,3],'offset':5}))", &[&["8"]]).await;
    check("let f=(d:dynamic){gettype(d)}; print v=f(1)", &[&["long"]]).await;
}

#[tokio::test]
async fn function_expansion_exposes_native_aggregate_and_window_calls() {
    check(
        "let f=(x:long){sum(x)*2}; range x from 1 to 3 step 1 | summarize v=f(x)",
        &[&["12"]],
    )
    .await;
    check(
        "let f=(x:long){prev(x,1,0)}; range x from 1 to 3 step 1 | sort by x asc | project v=f(x)",
        &[&["0"], &["1"], &["2"]],
    )
    .await;
}

#[tokio::test]
async fn tabular_functions_accept_typed_inputs_and_parenthesized_queries() {
    check("let MyFilter=(T:(x:long),v:long){T | where x>=v}; MyFilter((range x from 1 to 10 step 1),9) | sort by x asc", &[&["9"],&["10"]]).await;
    check("let T=datatable(x:long,y:string)[1,'a',2,'b']; let f=(T:(x:long)){T}; f(T) | sort by x asc", &[&["1"],&["2"]]).await;
}

#[tokio::test]
async fn invoke_supplies_the_first_tabular_argument() {
    check("let append=(T:(a:string),what:string){T | extend a=strcat(a,' ',what)}; datatable(a:string)['sad','really'] | invoke append(':-)')", &[&["sad :-)"],&["really :-)"]]).await;
    check("let f=(T:(x:long),n:long=2){T | where x>n}; range x from 1 to 4 step 1 | invoke f() | sort by x asc", &[&["3"],&["4"]]).await;
}

#[tokio::test]
async fn wildcard_table_arguments_support_schema_independent_operators() {
    check("let f=(T:(*)){T | distinct *}; datatable(x:long,y:string)[1,'a',1,'a',2,'b'] | invoke f() | sort by x asc", &[&["1","a"],&["2","b"]]).await;
}

#[tokio::test]
async fn tabular_functions_accept_multiple_tables_and_named_scalar_arguments() {
    check("let f=(L:(x:long),R:(x:long),n:long=0){L | join kind=inner (R) on x | where x>n | project x}; f((range x from 1 to 3 step 1),(range x from 2 to 4 step 1),n=2)", &[&["3"]]).await;
}

#[tokio::test]
async fn zero_argument_tabular_functions_work_as_sources_and_bindings() {
    check("let f=(){range x from 1 to 3 step 1}; f | count", &[&["3"]]).await;
    check(
        "let f=(){range x from 1 to 3 step 1}; let T=f(); T | count",
        &[&["3"]],
    )
    .await;
    check(
        "let T=datatable(x:long)[1,2]; let f=(){T}; f() | count",
        &[&["2"]],
    )
    .await;
    check(
        "let f=(){let T=range x from 1 to 3 step 1; T}; f | count",
        &[&["3"]],
    )
    .await;
}

#[tokio::test]
async fn tabular_closures_capture_tables_and_helper_functions() {
    check(
        "let T=datatable(x:long)[1,2]; let f=(){T}; (let T=datatable(x:long)[99]; f() | count)",
        &[&["2"]],
    )
    .await;
    check(
        "let f=(){let g=(){range x from 1 to 3 step 1}; g()}; f() | count",
        &[&["3"]],
    )
    .await;
}

#[tokio::test]
async fn functions_can_declare_request_parameters_in_their_own_scope() {
    let ctx = SessionContext::new();
    let parameters = QueryParameters::from([("n".into(), ScalarValue::Int64(Some(4)))]);
    assert_eq!(
        rows(
            ctx.kql_with_parameters(
                "let f=(x:long){declare query_parameters(n:long=2); x+n}; print v=f(1)",
                &parameters
            )
            .await
            .unwrap()
        )
        .await,
        vec![vec!["5"]]
    );
}

#[tokio::test]
async fn invalid_signatures_calls_and_row_contexts_are_rejected() {
    let ctx = SessionContext::new();
    for query in [
        "let f=(x:long,x:long){x}; print f(1,2)",
        "let f=(x:long=1,y:long){x}; print f()",
        "let f=(x:long,T:(*)){T}; f(1,(print x=1))",
        "let f=(x:long=1+2){x}; print f()",
        "let f=(x:dynamic=dynamic([])){x}; print f()",
        "let f=(x:long){x}; print f()",
        "let f=(x:long){x}; print f(1,2)",
        "let f=(x:long){x}; print f(y=1)",
        "let f=(x:long){x}; print f(x=1,x=2)",
        "let f=(x:long,y:long){x}; print f(x=1,2)",
        "let f=(x:long){x}; print f('wrong')",
        "let f=(){x}; print x=1 | project v=f()",
        "let f=(){f()}; print f()",
        "let f=(){print x=1}; print f()",
        "let f=(){1}; f()",
        "let f=(T:(x:long)){T}; f((print y=1))",
        "let f=(T:(x:long)){T}; f((print x='wrong'))",
        "let f=(n:long){range x from 1 to n step 1}; print x=3 | invoke f(x)",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}

#[tokio::test]
async fn function_expansion_is_bounded() {
    let mut query = String::from("let f0=(x:long){x+x}; ");
    for i in 1..16 {
        query.push_str(&format!(
            "let f{i}=(x:long){{f{}(x)+f{}(x)}}; ",
            i - 1,
            i - 1
        ));
    }
    query.push_str("print f15(1)");
    let error = SessionContext::new()
        .kql(&query)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("limit"), "{error}");
}
