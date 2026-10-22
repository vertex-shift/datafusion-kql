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

#[tokio::test]
async fn bag_unpack_declared_schema_extracts_typed_columns() {
    check("datatable(d:dynamic)[dynamic({'Name':'John','Age':20}),dynamic({'Name':'Dave','Age':40}),dynamic({'Name':'Jasmine','Age':30})] | evaluate bag_unpack(d):(Name:string,Age:long)",
        &[&["John","20"],&["Dave","40"],&["Jasmine","30"]]).await;
}

#[tokio::test]
async fn bag_unpack_wildcards_prefixes_and_named_arguments_are_supported() {
    check("print Description='Student',d=dynamic({'Name':'John','Age':20}) | evaluate bag_unpack(d,'Property_'):(*,Property_Name:string,Property_Age:long)",
        &[&["Student","John","20"]]).await;
    check("print d=dynamic({'Age':20}) | evaluate bag_unpack(d,ignoredProperties=dynamic(['Age'])):(Age:long)", &[&["<null>"]]).await;
}

#[tokio::test]
async fn bag_unpack_conflict_modes_preserve_or_replace_sources() {
    check("print Name='James',d=dynamic({'Name':'John'}) | evaluate bag_unpack(d,ignoredProperties=dynamic(['Name'])):(*,Name:string)", &[&["James"]]).await;
    check("print Name='James',d=dynamic({'Name':'John','Age':20}) | evaluate bag_unpack(d,columnsConflict='keep_source'):(*,Name:string,Age:long)", &[&["James","20"]]).await;
    check("print Name='James',d=dynamic({'Name':'John','Age':20}) | evaluate bag_unpack(d,columnsConflict='replace_source'):(*,Name:string,Age:long)", &[&["John","20"]]).await;
}

#[tokio::test]
async fn bag_unpack_null_missing_mixed_and_nested_values_keep_schema_types() {
    check("datatable(d:dynamic)[dynamic(null),dynamic({'x':'4','s':null,'a':[1,2]}),dynamic({'x':4,'s':'ok'})] | evaluate bag_unpack(d):(x:long,s:string,a:dynamic) | project x,s,a=tostring(a)",
        &[&["<null>","",""],&["<null>","","[1,2]"],&["4","ok",""]]).await;
}

#[tokio::test]
async fn bag_unpack_can_emit_zero_columns_without_losing_rows() {
    check(
        "datatable(d:dynamic)[dynamic({'x':1}),dynamic(null)] | evaluate bag_unpack(d):(*) | count",
        &[&["2"]],
    )
    .await;
    check(
        "print keep=1,d=dynamic({'x':2}) | evaluate bag_unpack(d):(x:long)",
        &[&["2"]],
    )
    .await;
}

#[tokio::test]
async fn bag_unpack_preserves_serialized_rows_and_user_named_helpers() {
    check("datatable(d:dynamic)[dynamic({'x':2}),dynamic({'x':1})] | serialize | evaluate bag_unpack(d):(x:long) | project x,p=prev(x,1,0)", &[&["2","0"],&["1","2"]]).await;
    check("print d=dynamic({'__kql_temporary_0':3}) | evaluate bag_unpack(d):(__kql_temporary_0:long)", &[&["3"]]).await;
}

#[tokio::test]
async fn pivot_count_and_sum_compile_to_native_conditional_aggregates() {
    check("let __kql_temporary_0=100; print k='A',v=2 | evaluate pivot(k,sum(v+__kql_temporary_0)):(A:long)", &[&["102"]]).await;
    let data = "datatable(State:string,Kind:string,Value:long)['A','wind',2,'B','wind',3,'A','rain',5,'A','wind',7]";
    check(&format!("{data} | project State,Kind | evaluate pivot(State):(Kind:string,A:long,B:long) | sort by Kind asc"),
        &[&["rain","1","0"],&["wind","2","1"]]).await;
    check(&format!("{data} | evaluate pivot(State,sum(Value)):(Kind:string,A:long,B:long) | sort by Kind asc"),
        &[&["rain","5","0"],&["wind","9","3"]]).await;
}

#[tokio::test]
async fn pivot_explicit_grouping_and_schema_column_order_are_preserved() {
    check("datatable(State:string,Kind:string,Unused:string,Value:long)['A','wind','x',2,'B','wind','y',3,'A','wind','z',7] | evaluate pivot(State,sum(Value),Kind):(B:long,Kind:string,A:long)",
        &[&["3","wind","9"]]).await;
}

#[tokio::test]
async fn pivot_numeric_keys_and_dynamic_collection_outputs_are_supported() {
    check(
        "datatable(k:long,v:long)[1,2,2,3,1,4] | evaluate pivot(k,sum(v)): (['1']:long,['2']:long)",
        &[&["6", "3"]],
    )
    .await;
    check("datatable(k:string,v:long)['A',2,'B',3,'A',4] | evaluate pivot(k,make_set(v)): (A:dynamic,B:dynamic) | project A=set_has_element(A,2) and set_has_element(A,4),B=tostring(B)", &[&["true","[3]"]]).await;
}

#[tokio::test]
async fn pivot_does_not_silently_drop_values_absent_from_the_output_schema() {
    let frame = SessionContext::new()
        .kql("datatable(k:string)['A','Unexpected'] | evaluate pivot(k):(A:long)")
        .await
        .unwrap();
    let error = frame.collect().await.unwrap_err().to_string();
    assert!(error.contains("not declared in OutputSchema"), "{error}");
}

#[tokio::test]
async fn evaluate_inside_grouped_subqueries_isolatedly_binds_input() {
    check("datatable(id:long,d:dynamic)[1,dynamic([{'x':2},{'x':3}]),2,dynamic([{'x':4}])] | mv-apply p=d on (evaluate bag_unpack(p):(x:long) | summarize Total=sum(x)) | project id,Total | sort by id asc",
        &[&["1","5"],&["2","4"]]).await;
}

#[tokio::test]
async fn invalid_evaluation_plugins_and_schemas_fail_explicitly() {
    let ctx = SessionContext::new();
    for query in [
        "print d=dynamic({'x':1}) | evaluate bag_unpack(d)",
        "print d=dynamic({'x':1}) | evaluate bag_unpack(d): (x:long,x:real)",
        "print d=dynamic({'x':1}) | evaluate bag_unpack(d,'p_'): (x:long)",
        "print x=1,d=dynamic({'x':2}) | evaluate bag_unpack(d): (*,x:long)",
        "print d=1 | evaluate bag_unpack(d):(x:long)",
        "print d=dynamic({}) | evaluate bag_unpack(d,columnsConflict='invalid'):(x:long)",
        "print d=dynamic({}) | evaluate bag_unpack(d,ignoredProperties=dynamic([1])):(x:long)",
        "print k='A' | evaluate pivot(k):(A:real)",
        "print k='A',v=1 | evaluate pivot(k):(*,A:long)",
        "print k='A',v=1 | evaluate pivot(k):(A:long)",
        "print x=1 | evaluate unknown():(x:long)",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}
