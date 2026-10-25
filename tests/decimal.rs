use std::sync::Arc;

use datafusion::arrow::array::{Array, Decimal128Array, Decimal256Array};
use datafusion::arrow::datatypes::i256;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{ScalarValue, TableReference};
use datafusion::prelude::*;
use datafusion_kql::{QueryParameters, SessionContextKqlExt};

async fn rows(ctx: &SessionContext, query: &str) -> Vec<Vec<String>> {
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
    assert_eq!(
        rows(&SessionContext::new(), query).await,
        expected,
        "{query}"
    );
}

#[tokio::test]
async fn decimal_literals_preserve_exact_base_ten_values() {
    check("print a=tostring(decimal(0.1)+decimal(0.2)),b=decimal(0.1)+decimal(0.2)==decimal(0.3),c=tostring(decimal(1e5)),d=tostring(decimal(-1.2300))",
        &[&["0.3","true","100000","-1.23"]]).await;
    check("print a=tostring(decimal(79228162514264337593543950335)),b=tostring(decimal(0.0000000000000000000000000001)),c=tostring(decimal(max)),d=tostring(decimal(min))",
        &[&["79228162514264337593543950335","0.0000000000000000000000000001","79228162514264337593543950335","-79228162514264337593543950335"]]).await;
}

#[tokio::test]
async fn decimal_arithmetic_does_not_round_through_double() {
    check("print a=tostring(decimal(9007199254740993)+1),b=tostring(decimal(1.25)*decimal(2.4)),c=tostring(decimal(1)/decimal(8)),d=tostring(decimal(5.5)%decimal(2))",
        &[&["9007199254740994","3","0.125","1.5"]]).await;
    check(
        "print x=decimal(1.5)+0.25 | project x,kind=gettype(x)",
        &[&["1.75", "real"]],
    )
    .await;
}

#[tokio::test]
async fn decimal_checked_overflow_and_division_by_zero_are_typed_nulls() {
    check("print a=isnull(decimal(max)+1),b=isnull(decimal(max)*2),c=isnull(decimal(1)/decimal(0)),d=isnull(decimal(min)-1),e=tostring(decimal(max)-1)",
        &[&["true","true","true","true","79228162514264337593543950334"]]).await;
}

#[tokio::test]
async fn nullable_decimal_conversion_has_no_success_shaped_fallback() {
    check("datatable(s:string)['123.45678','9007199254740993','invalid','79228162514264337593543950336'] | project x=tostring(todecimal(s)),n=isnull(todecimal(s))",
        &[&["123.45678","false"],&["9007199254740993","false"],&["","true"],&["","true"]]).await;
    check("print a=tostring(todecimal(12)),b=tostring(todecimal(real(1.25))),c=tostring(todecimal(bool(true))),d=isnull(todecimal(guid(null)))",
        &[&["12","1.25","1","true"]]).await;
}

#[tokio::test]
async fn decimal_bins_and_floor_preserve_large_integral_values() {
    check(
        "print a=isnull(bin_at(decimal(1.25),null,0)),b=isnull(bin_at(decimal(1.25),1,null))",
        &[&["true", "true"]],
    )
    .await;
    check("print a=tostring(bin(decimal(9007199254740993.25),decimal(1))),b=tostring(bin(decimal(-1.2),decimal(0.5))),c=tostring(bin_at(decimal(5.5),decimal(2),decimal(0.5))),d=tostring(floor(decimal(-1.1))),e=tostring(ceiling(decimal(-1.1)))",
        &[&["9007199254740993","-1.5","4.5","-2","-1"]]).await;
    check(
        "print a=isnull(bin(decimal(1),decimal(0))),b=isnull(bin(decimal(1),decimal(-1)))",
        &[&["true", "true"]],
    )
    .await;
}

#[tokio::test]
async fn decimal_datatables_unions_and_aggregates_use_native_arrow_plans() {
    check("datatable(x:decimal)['1.25',decimal(2.75),3] | summarize S=sum(x),A=avg(x),N=count_distinct(x) | project S=tostring(S),A=tostring(A),N",
        &[&["7","2.3333333333333333333333333333","3"]]).await;
    check(
        "datatable(x:decimal)[] | summarize S=sum(x),A=avg(x) | project S=tostring(S),A=isnull(A)",
        &[&["0", "true"]],
    )
    .await;
    check("union (print x=decimal(1.25)),(print x=decimal(2.75)) | summarize S=sum(x) | project S=tostring(S)", &[&["4"]]).await;
}

#[tokio::test]
async fn decimal_filters_sorts_membership_and_joins_preserve_numeric_semantics() {
    check("datatable(x:decimal)[decimal(10),decimal(-2),decimal(1.25),decimal(1.5)] | where x in(decimal(1.25),decimal(10),decimal(-2)) | sort by x asc | project x=tostring(x)",
        &[&["-2"],&["1.25"],&["10"]]).await;
    check("let L=datatable(k:decimal,v:string)[decimal(1.0),'a']; let R=datatable(k:decimal,w:string)[decimal(1.00),'b']; L | join kind=inner(R) on k | project v,w", &[&["a","b"]]).await;
}

#[tokio::test]
async fn typed_decimal_parameters_functions_and_schema_reporting_are_supported() {
    let ctx = SessionContext::new();
    let batch = ctx
        .kql("print x=decimal(1.25)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let parameters = QueryParameters::from([(
        "x".into(),
        ScalarValue::try_from_array(batch[0].column(0), 0).unwrap(),
    )]);
    let frame = ctx
        .kql_with_parameters(
            "declare query_parameters(x:decimal); let f=(x:decimal){x*2}; print x=tostring(f(x))",
            &parameters,
        )
        .await
        .unwrap();
    let batch = frame.collect().await.unwrap();
    assert_eq!(array_value_to_string(batch[0].column(0), 0).unwrap(), "2.5");
    check(
        "declare query_parameters(x:decimal=decimal(2.5)); print x=tostring(x)",
        &[&["2.5"]],
    )
    .await;
    check(
        "print x=decimal(1.25) | getschema",
        &[&["x", "0", "System.Decimal", "decimal"]],
    )
    .await;
}

#[tokio::test]
async fn decimal_extraction_and_parse_use_nullable_conversions() {
    check("print d=tostring(extract_json('$.amount','{\"amount\":9007199254740993.123456789}',typeof(decimal))),q=tostring(extract_json('$[\"a.b\"][-1]','{\"a.b\":[0,1.0000000000000000000000000001]}',typeof(decimal)))",
        &[&["9007199254740993.123456789","1.0000000000000000000000000001"]]).await;
    check(
        "print d=tostring(extract('value=(.*)',1,'value=1.25',typeof(decimal)))",
        &[&["1.25"]],
    )
    .await;
    check("datatable(s:string)['x=1.25','x=invalid'] | parse-where s with 'x=' d:decimal | project d=tostring(d)", &[&["1.25"]]).await;
}

#[tokio::test]
async fn dynamic_decimal_roots_keep_their_own_tag() {
    check("print d=dynamic(decimal(1.25)) | project kind=gettype(d),s=tostring(d),n=tostring(todecimal(d))", &[&["decimal","1.25","1.25"]]).await;
    check("datatable(x:decimal)[decimal(1.25),decimal(2.5),decimal(null)] | extend d=todynamic(x) | serialize | project p=tostring(prev(d)),n=tostring(next(d))",
        &[&["","2.5"],&["1.25",""],&["2.5",""]]).await;
    check(
        "print d=dynamic(decimal(1.25)) | mv-expand d to typeof(decimal) | project d=tostring(d)",
        &[&["1.25"]],
    )
    .await;
    check(
        "print d=dynamic('1.25') | mv-expand d to typeof(decimal) | project n=isnull(d)",
        &[&["true"]],
    )
    .await;
}

#[tokio::test]
async fn native_decimal128_inputs_normalize_losslessly_in_unions_and_dynamics() {
    let ctx = SessionContext::new();
    let values = Decimal128Array::from(vec![Some(125), None])
        .with_precision_and_scale(10, 2)
        .unwrap();
    ctx.register_batch(
        TableReference::bare("T"),
        RecordBatch::try_from_iter(vec![("x", Arc::new(values) as _)]).unwrap(),
    )
    .unwrap();
    assert_eq!(
        rows(
            &ctx,
            "T | union(print x=decimal(2.75)) | summarize S=sum(x) | project S=tostring(S)"
        )
        .await,
        vec![vec!["4"]]
    );
    assert_eq!(
        rows(&ctx, "T | project kind=gettype(x),x=tostring(todynamic(x))").await,
        vec![vec!["decimal", "1.25"], vec!["decimal", ""]]
    );
}

#[tokio::test]
async fn native_high_scale_decimals_round_once_with_midpoint_to_even() {
    let ctx = SessionContext::new();
    let values = [
        "100000000000000000000000000005",
        "100000000000000000000000000015",
        "-100000000000000000000000000005",
    ]
    .map(|value| i256::from_string(value).unwrap());
    let array = Decimal256Array::from(values.to_vec())
        .with_precision_and_scale(40, 29)
        .unwrap();
    ctx.register_batch(
        TableReference::bare("T"),
        RecordBatch::try_from_iter(vec![("x", Arc::new(array) as _)]).unwrap(),
    )
    .unwrap();
    assert_eq!(
        rows(&ctx, "T | project x=tostring(todecimal(x))").await,
        vec![
            vec!["1"],
            vec!["1.0000000000000000000000000002"],
            vec!["-1"],
        ]
    );
}

#[tokio::test]
async fn invalid_decimal_literals_and_datatable_values_fail_during_planning() {
    let ctx = SessionContext::new();
    for query in [
        "print d=decimal(invalid)",
        "print d=decimal(79228162514264337593543950336)",
        "datatable(d:decimal)['invalid']",
        "range d from decimal(9007199254740993) to decimal(9007199254740994) step decimal(0.1)",
        "print d=bin(decimal(1.25),1h)",
        "print d=bin_at(decimal(1.25),1,datetime(2025-01-01))",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
    check(
        "print d=isnull(todecimal('max')),m=tostring(decimal(max))",
        &[&["true", "79228162514264337593543950335"]],
    )
    .await;
}
