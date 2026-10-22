use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, UnionArray};
use datafusion::arrow::datatypes::{DataType, UnionFields};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{ScalarValue, TableReference};
use datafusion::prelude::*;
use datafusion_kql::{QueryParameters, SessionContextKqlExt};

const A: &str = "74be27de-1e4e-49d9-b579-fe0b331d3642";
const B: &str = "01234567-89ab-cdef-0123-456789abcdef";

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
                        if array.logical_nulls().is_some_and(|n| n.is_null(row)) {
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
    assert_eq!(
        result(&SessionContext::new(), query).await,
        expected,
        "{query}"
    );
}

#[tokio::test]
async fn guid_literals_aliases_and_canonical_formatting_are_native_128_bit_values() {
    check(&format!("print a=tostring(guid({A})),b=tostring(uuid({B})),n=isnull(uniqueid(null)),s=string(guid(null))"),
        &[&[A,B,"true",""]]).await;
    let frame = SessionContext::new()
        .kql(&format!("print id=guid({A})"))
        .await
        .unwrap();
    assert_eq!(
        frame.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    check(
        &format!("print s=strcat('id=',guid({A}))"),
        &[&[&format!("id={A}")]],
    )
    .await;
}

#[tokio::test]
async fn toguid_follows_prefix_optional_hyphen_and_nullable_conversion_rules() {
    check("datatable(s:string)['0123456789abcdef0123456789abcdef','0123456789ab-cdef-0123-456789abcdef','01234567-89AB-CDEF-0123-456789ABCDEF-tail','not-a-guid','0-123456789abcdef0123456789abcdef'] | project g=tostring(toguid(s))",
        &[&[B],&[B],&[B],&[""],&[""]]).await;
    check(
        &format!("print g=tostring(toguid(guid({A}))),n=isnull(toguid(1))"),
        &[&[A, "true"]],
    )
    .await;
}

#[tokio::test]
async fn guid_equality_nulls_and_membership_follow_kql_semantics() {
    check(&format!("print a=guid({A})==guid({A}),b=guid({A})==guid(null),n=guid(null)==guid(null),i=guid({A}) in(guid({B}),guid({A}))"),
        &[&["true","false","<null>","true"]]).await;
}

#[tokio::test]
async fn guid_columns_work_in_datatables_joins_unions_aggregates_and_schema_reporting() {
    check(
        &format!(
            "let T=datatable(g:guid)['{A}',guid(null),guid({A})]; T | summarize N=count_distinct(g)"
        ),
        &[&["1"]],
    )
    .await;
    check(&format!("let L=datatable(g:guid,v:long)[guid({A}),1,guid({B}),2]; let R=datatable(g:guid,n:string)[guid({A}),'found']; L | join kind=inner(R) on g | project n"), &[&["found"]]).await;
    check(
        &format!("datatable(g:guid)[guid({A})] | union(print g=guid({B})) | count"),
        &[&["2"]],
    )
    .await;
    check(
        &format!("print g=guid({A}) | getschema"),
        &[&["g", "0", "System.Guid", "guid"]],
    )
    .await;
}

#[tokio::test]
async fn guid_parameters_and_user_functions_preserve_types() {
    let ctx = SessionContext::new();
    let batch = ctx
        .kql(&format!("print g=guid({A})"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let value = ScalarValue::try_from_array(batch[0].column(0), 0).unwrap();
    let parameters = QueryParameters::from([("id".into(), value)]);
    let frame = ctx
        .kql_with_parameters(
            "declare query_parameters(id:guid); let f=(x:guid){tostring(x)}; print value=f(id)",
            &parameters,
        )
        .await
        .unwrap();
    let batches = frame.collect().await.unwrap();
    assert_eq!(array_value_to_string(batches[0].column(0), 0).unwrap(), A);
    check(
        &format!("let f=(id:guid=guid({B})){{tostring(id)}}; print f()"),
        &[&[B]],
    )
    .await;
}

#[tokio::test]
async fn guid_extraction_and_typed_parse_are_nullable() {
    check(
        &format!("print g=tostring(extract('id=(.*)',1,'id={A}',typeof(guid)))"),
        &[&[A]],
    )
    .await;
    check(
        &format!("print g=tostring(extract_json('$.id','{{\"id\":\"{B}\"}}',typeof(guid)))"),
        &[&[B]],
    )
    .await;
    check(&format!("datatable(s:string)['id={A}','id=bad'] | parse-where s with 'id=' g:guid | project g=tostring(g)"), &[&[A]]).await;
}

#[tokio::test]
async fn dynamic_guid_roots_keep_type_tags_and_window_values() {
    check(&format!("print d=dynamic(guid({A})) | project kind=gettype(d),s=tostring(d),g=tostring(toguid(d))"), &[&["guid",A,A]]).await;
    check(&format!("datatable(g:guid)[guid({A}),guid({B}),guid(null)] | extend d=todynamic(g) | serialize | project kind=gettype(d),previous=tostring(prev(d)),next=tostring(next(d))"),
        &[&["guid","",B],&["guid",A,""],&["null",B,""]]).await;
}

#[tokio::test]
async fn primitive_guid_windows_and_dynamic_equality_do_not_corrupt_null_padding() {
    check(&format!("datatable(g:guid)[guid({A}),guid({B})] | serialize | project p=tostring(prev(g)),n=tostring(next(g))"), &[&["",B],&[A,""]]).await;
    check(&format!("print a=dynamic(guid({A}))==dynamic(guid({A})),b=dynamic(guid({A}))==dynamic('{A}'),c=gettype(coalesce(dynamic(null),guid({A})))"),
        &[&["true","false","guid"]]).await;
}

#[tokio::test]
async fn scalar_dynamic_guid_expansion_does_not_become_an_ordinary_string() {
    check(&format!("print d=dynamic(guid({A})) | mv-expand d to typeof(guid) | project kind=gettype(d),d=tostring(d)"), &[&["guid",A]]).await;
    check(
        &format!("print d=dynamic('{A}') | mv-expand d to typeof(guid) | project n=isnull(d)"),
        &[&["true"]],
    )
    .await;
}

#[tokio::test]
async fn old_dynamic_union_inputs_remain_readable_and_normalize_on_union() {
    let ctx = SessionContext::new();
    let batches = ctx
        .kql(&format!("print d=dynamic('{B}')"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let union = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<UnionArray>()
        .unwrap();
    let DataType::Union(fields, _) = union.data_type() else {
        panic!("expected dynamic union");
    };
    let old = UnionFields::from_iter(
        fields
            .iter()
            .filter(|(id, _)| *id < 9)
            .map(|(id, field)| (id, Arc::clone(field))),
    );
    let arrays = old
        .iter()
        .map(|(id, _)| Arc::clone(union.child(id)))
        .collect();
    let old = UnionArray::try_new(
        old,
        union.type_ids().clone(),
        union.offsets().cloned(),
        arrays,
    )
    .unwrap();
    ctx.register_batch(
        TableReference::bare("Old"),
        RecordBatch::try_from_iter(vec![("d", Arc::new(old) as ArrayRef)]).unwrap(),
    )
    .unwrap();
    assert_eq!(
        result(&ctx, "Old | project kind=gettype(d),g=tostring(toguid(d))").await,
        vec![vec!["string", B]]
    );
    assert_eq!(result(&ctx,&format!("Old | union(print d=dynamic(guid({A}))) | project kind=gettype(d),s=tostring(d) | sort by kind asc")).await,vec![vec!["guid",A],vec!["string",B]]);
}

#[tokio::test]
async fn new_guid_generates_v4_values_and_toscalar_freezes_one_value() {
    let ctx = SessionContext::new();
    let rows=result(&ctx,"let fixed=toscalar(new_guid()); range x from 1 to 10 step 1 | project fixed=tostring(fixed),fresh=tostring(new_guid())").await;
    assert!(rows.iter().all(|row| row[0] == rows[0][0]));
    let unique = rows
        .iter()
        .map(|row| row[1].as_str())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(unique.len(), 10);
    assert!(rows.iter().all(|row| row[1].as_bytes()[14] == b'4'
        && matches!(row[1].as_bytes()[19], b'8' | b'9' | b'a' | b'b')));
}

#[tokio::test]
async fn new_guid_does_not_overwrite_or_depend_on_the_host_uuid_registry_entry() {
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    let ctx = SessionContext::new();
    ctx.register_udf(create_udf(
        "uuid",
        vec![],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|_| {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "host-owned".into(),
            ))))
        }),
    ));
    let rows = result(&ctx, "print generated=tostring(new_guid())").await;
    assert_eq!(rows[0][0].len(), 36);
    let host = ctx
        .sql("select uuid()")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        array_value_to_string(host[0].column(0), 0).unwrap(),
        "host-owned"
    );
}
#[tokio::test]
async fn guid_search_and_pivot_use_canonical_values() {
    check(
        &format!(
            "datatable(g:guid)[guid({A}),guid({B})] | search '74be27de*' | project g=tostring(g)"
        ),
        &[&[A]],
    )
    .await;
    check(
        &format!("datatable(g:guid)[guid({A}),guid({A})] | evaluate pivot(g):(['{A}']:long)"),
        &[&["2"]],
    )
    .await;
}

#[tokio::test]
async fn invalid_guid_literals_fail_before_execution() {
    let ctx = SessionContext::new();
    for query in [
        "print g=guid(bad)",
        "print g=guid(01234567-89ab-cdef-0123-456789abcdef-tail)",
        "print g=guid(01234567--89ab-cdef-0123-456789abcdef)",
        "datatable(g:guid)['bad']",
        "print v=guid(61626364-6566-6768-696a-6b6c6d6e6f70)=='abcdefghijklmnop'",
        "print v=guid(61626364-6566-6768-696a-6b6c6d6e6f70) in('abcdefghijklmnop')",
        "print v=guid(61626364-6566-6768-696a-6b6c6d6e6f70) in(datatable(v:string)['abcdefghijklmnop'])",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}
