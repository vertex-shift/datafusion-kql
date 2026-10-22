use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::TableReference;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

async fn context() -> SessionContext {
    let ctx = SessionContext::new();
    for (name, query) in [
        (
            "A",
            "datatable(Message:string,Code:long,Shared:string,ExtraA:string)['ERROR disk',500,'one','alpha','terror',200,'one','beta','ok',100,'one','gamma']",
        ),
        (
            "B",
            "datatable(Message:string,Shared:string,ExtraB:string)['error network','two','delta','okay','two','epsilon']",
        ),
    ] {
        let frame = ctx.kql(query).await.unwrap();
        ctx.register_table(TableReference::bare(name), frame.into_view())
            .unwrap();
    }
    ctx
}

async fn result(ctx: &SessionContext, query: &str) -> Vec<Vec<String>> {
    let frame = ctx
        .kql(query)
        .await
        .unwrap_or_else(|e| panic!("{}\n{query}", e.diagnostic(query)));
    frame
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{e}\n{query}"))
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
                    .collect()
            })
        })
        .collect()
}

async fn check(query: &str, expected: &[&[&str]]) {
    let expected = expected
        .iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(result(&context().await, query).await, expected, "{query}");
}

#[tokio::test]
async fn search_global_terms_use_boundaries_and_source_labels() {
    check(
        "search 'error' | project ['$table'],Message | sort by ['$table'] asc",
        &[&["A", "ERROR disk"], &["B", "error network"]],
    )
    .await;
    check("search in (A) 'err*' | project Message", &[&["ERROR disk"]]).await;
    check(
        "search in (A) '*err*' | project Message | sort by Message asc",
        &[&["ERROR disk"], &["terror"]],
    )
    .await;
}

#[tokio::test]
async fn search_case_modes_column_terms_and_boolean_combinations() {
    check(
        "search kind=case_sensitive in (A,B) Message:'error' | project Message",
        &[&["error network"]],
    )
    .await;
    check("search in (A,B) ('error' or Message:'terror') and Shared=='one' | project Message | sort by Message asc", &[&["ERROR disk"],&["terror"]]).await;
    check(
        "search in (A) not('error') | project Code | sort by Code asc",
        &[&["100"], &["200"]],
    )
    .await;
}

#[tokio::test]
async fn pipe_search_and_consecutive_search_preserve_the_source_label() {
    check(
        "A | search '*err*' | search 'disk' | project ['$table'],Code",
        &[&["A", "500"]],
    )
    .await;
    check(
        "A | where Code>100 | search * | project Code | sort by Code asc",
        &[&["200"], &["500"]],
    )
    .await;
}

#[tokio::test]
async fn search_combines_numeric_predicates_missing_columns_and_terms() {
    check("search in (A,B) Code>300 or 'network' | project ['$table'],Message,Code | sort by ['$table'] asc",
        &[&["A","ERROR disk","500"],&["B","error network","<null>"]]).await;
    check("search in (A) * has 'disk' | project Code", &[&["500"]]).await;
    check("search in (A) Code==200 | project Message", &[&["terror"]]).await;
}

#[tokio::test]
async fn search_internal_wildcards_suffixes_and_unicode_boundaries() {
    check("datatable(x:string)['alphabet','foo_hello','中a文','lab12pc'] | search 'lab*pc' | project x", &[&["lab12pc"]]).await;
    check(
        "datatable(x:string)['alphabet','foo_hello','中a文'] | search '*bet' | project x",
        &[&["alphabet"]],
    )
    .await;
    check(
        "datatable(x:string)['foo_hello','xhello','中文'] | search 'hello' | project x",
        &[&["foo_hello"]],
    )
    .await;
}

#[tokio::test]
async fn find_smart_schema_packs_only_noncommon_nonpredicate_columns() {
    check("find in (A,B) where 'error' | project source_,Message,Shared,packed=tostring(pack_) | sort by source_ asc",
        &[&["A","ERROR disk","one","{\"Code\":500,\"ExtraA\":\"alpha\"}"],&["B","error network","two","{\"ExtraB\":\"delta\"}"]]).await;
    check("find withsource=Origin in (A,B) where Code>300 or 'network' | project Origin,Code,packed=tostring(pack_) | sort by Origin asc",
        &[&["A","500","{\"ExtraA\":\"alpha\"}"],&["B","<null>","{\"ExtraB\":\"delta\"}"]]).await;
}

#[tokio::test]
async fn find_explicit_projection_and_pack_all_have_stable_types() {
    check("find in (A,B) where 'error' project Message,Code:real,pack_all() | project source_,Message,Code,packed=tostring(column1) | sort by source_ asc",
        &[&["A","ERROR disk","500.0","{\"Code\":500,\"ExtraA\":\"alpha\",\"Message\":\"ERROR disk\",\"Shared\":\"one\"}"],&["B","error network","<null>","{\"ExtraB\":\"delta\",\"Message\":\"error network\",\"Shared\":\"two\"}"]]).await;
}

#[tokio::test]
async fn search_uses_lexical_views_functions_and_request_defaults() {
    check("let V=view (){print Message='error view'}; search in(V*) 'error' | project ['$table'],Message", &[&["V","error view"]]).await;
    check("declare query_parameters(n:long=300); let f=(x:long){x>n}; search in(A) f(Code) | project Code", &[&["500"]]).await;
}

#[tokio::test]
async fn schema_helpers_pack_visible_columns_and_select_existing_values_lazily() {
    check("print bag=tostring(pack_all())", &[&["{}"]]).await;
    check("print x=1,y='',z=long(null) | serialize n=row_number() | project bag=tostring(pack_all()),filtered=tostring(pack_all(true))",
        &[&["{\"n\":1,\"x\":1,\"y\":\"\",\"z\":null}","{\"n\":1,\"x\":1}"]]).await;
    check(
        "print x=1 | project a=column_ifexists('x',missing),b=column_ifexists('missing',2)",
        &[&["1", "2"]],
    )
    .await;
}

#[tokio::test]
async fn invalid_search_definitions_are_explicit_errors() {
    let ctx = context().await;
    for query in [
        "search kind=unknown 'x'",
        "search in(NoSuchTable) 'x'",
        "search in(A) Missing:'x'",
        "find in(A) where Code>1 project Missing",
        "find withsource=Message in(A) where true",
        "print x=1 | project bag=pack_all(1)",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}
