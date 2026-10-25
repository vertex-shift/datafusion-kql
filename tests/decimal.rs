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
        &[&["79228162514264337593543950335","1E-28","1E+38","-1E+38"]]).await;
}

#[tokio::test]
async fn decimal_arithmetic_does_not_round_through_double() {
    check("print a=tostring(decimal(9007199254740993)+1),b=tostring(decimal(1.25)*decimal(2.4)),c=tostring(decimal(1)/decimal(8)),d=tostring(decimal(5.5)%decimal(2))",
        &[&["9007199254740994","3","0.125","1.5"]]).await;
    check(
        "print x=decimal(1.5)+0.25 | project x=tostring(x),kind=gettype(x)",
        &[&["1.75", "decimal"]],
    )
    .await;
}

#[tokio::test]
async fn decimal_runtime_range_exceeds_frontend_constants_and_division_by_zero_is_null() {
    check("print a=isnull(decimal(max)+1),b=isnull(decimal(max)*2),c=isnull(decimal(1)/decimal(0)),d=isnull(decimal(min)-1),e=tostring(decimal(max)-1)",
        &[&["false","false","true","false","1E+38"]]).await;
    check("datatable(s:string)['1e6144'] | extend x=todecimal(s) | project overflow=tostring(x*10),finite=isnotnull(x)",
        &[&["Infinity","true"]]).await;
}

#[tokio::test]
async fn nullable_decimal_conversion_has_no_success_shaped_fallback() {
    check("datatable(s:string)['123.45678','9007199254740993','invalid','79228162514264337593543950336'] | project x=tostring(todecimal(s)),n=isnull(todecimal(s))",
        &[&["123.45678","false"],&["9007199254740993","false"],&["","true"],&["79228162514264337593543950336","false"]]).await;
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
        &[&["7","2.333333333333333333333333333333333","3"]]).await;
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
async fn wide_runtime_decimals_keep_native_order_grouping_and_distinct_semantics() {
    check("datatable(s:string)['1e100','-1e100','1.0','1.00','0','1e-100','bad'] | extend d=todecimal(s) | summarize N=count_distinct(d),A=dcount(d),Missing=countif(isnull(d)),Lo=min(d),Hi=max(d) | project N,A,Missing,Lo=tostring(Lo),Hi=tostring(Hi)",
        &[&["5","5","1","-1E+100","1E+100"]]).await;
    check("datatable(s:string)['1e100','-1e100','1.0','0','1e-100'] | extend d=todecimal(s) | sort by d asc | project d=tostring(d)",
        &[&["-1E+100"],&["0"],&["1E-100"],&["1"],&["1E+100"]]).await;
    check("let L=datatable(s:string)['1e100','1.00'] | extend k=todecimal(s); let R=datatable(s:string)['1e100','1.0'] | extend k=todecimal(s); L | join kind=inner(R) on k | count", &[&["2"]]).await;
}

#[tokio::test]
async fn decimal_casts_conditionals_and_unary_operations_stay_typed() {
    check("datatable(s:string)['1.25','-2.5','bad'] | extend d=todecimal(s) | project L=tolong(d),R=toreal(d),B=tobool(d),Neg=tostring(-d),Choice=tostring(iff(d>0,d,decimal(0))),Fallback=tostring(coalesce(d,decimal(5)))",
        &[&["1","1.25","true","-1.25","1.25","1.25"],&["-2","-2.5","true","2.5","0","-2.5"],&["<null>","<null>","<null>","","0","5"]]).await;
    check("print A=tostring(abs(decimal(-1.25))),B=tostring(case(true,decimal(1.25),false,decimal(2),decimal(3))),C=decimal(1.25) between(1..2),D=decimal(1.25) in(1,decimal(1.25)),E=decimal(1.25)>1.0",
        &[&["1.25","1.25","true","true","true"]]).await;
}

#[tokio::test]
async fn native_decimal_structs_survive_materialization_and_serialized_offsets() {
    check("let T=materialize(datatable(s:string)['1e100','2.5','bad'] | extend d=todecimal(s)); T | serialize | project p=tostring(prev(d)),n=tostring(next(d))",
        &[&["","2.5"],&["1E+100",""],&["2.5",""]]).await;
}

#[tokio::test]
async fn scalar_and_vector_json_precision_follow_their_reference_paths() {
    check("datatable(s:string)['{\"amount\":true}','{\"amount\":1.234567890123456789012345}','{\"amount\":\"1.234567890123456789012345\"}'] | project d=tostring(extract_json('$.amount',s,typeof(decimal)))",
        &[&[""],&["1.234567890123456789"],&["1.234567890123456789012345"]]).await;
}

#[tokio::test]
async fn decimal_special_values_keep_numeric_comparison_and_ordering_semantics() {
    check("datatable(s:string)['Infinity','1'] | extend d=todecimal(s) | extend N=d*decimal(0) | sort by N asc nulls last | project N=tostring(N)",
        &[&["0"],&["NaN"]]).await;
    check("datatable(s:string)['Infinity','1'] | extend d=todecimal(s) | extend N=d*decimal(0) | sort by N desc nulls first | project N=tostring(N)",
        &[&["NaN"],&["0"]]).await;
    check("datatable(s:string)['Infinity'] | extend N=todecimal(s)*decimal(0) | extend a=pack_array(N) | summarize a=take_any(a) | project kind=gettype(a[0]),value=tostring(todecimal(a[0]))",
        &[&["decimal","NaN"]]).await;
}

#[tokio::test]
async fn old_fixed_scale_dynamic_decimal_tags_remain_readable() {
    use datafusion::arrow::array::{NullArray, UnionArray, new_empty_array};
    use datafusion::arrow::buffer::ScalarBuffer;
    use datafusion::arrow::datatypes::{DataType, UnionFields};
    let ctx = SessionContext::new();
    let sample = ctx
        .kql("print d=dynamic(decimal(1.25))")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let DataType::Union(fields, _) = sample[0].column(0).data_type() else {
        panic!("expected dynamic union");
    };
    let fields = UnionFields::from_iter(
        fields
            .iter()
            .filter(|(id, _)| *id < 13)
            .map(|(id, field)| (id, Arc::clone(field))),
    );
    let mut children = Vec::new();
    for (id, field) in fields.iter() {
        let array: datafusion::arrow::array::ArrayRef = match id {
            0 => Arc::new(NullArray::new(1)),
            10 => Arc::new(
                Decimal256Array::from(vec![
                    i256::from_i128(125) * i256::from_i128(10_i128.pow(26)),
                ])
                .with_precision_and_scale(57, 28)
                .unwrap(),
            ),
            _ => new_empty_array(field.data_type()),
        };
        children.push(array);
    }
    let old = UnionArray::try_new(
        fields,
        ScalarBuffer::from(vec![10_i8, 0]),
        Some(ScalarBuffer::from(vec![0_i32, 0])),
        children,
    )
    .unwrap();
    ctx.register_batch(
        TableReference::bare("Old"),
        RecordBatch::try_from_iter(vec![("d", Arc::new(old) as _)]).unwrap(),
    )
    .unwrap();
    assert_eq!(rows(&ctx,"Old | union(print d=dynamic(decimal(2.5))) | project kind=gettype(d),value=tostring(d) | sort by value asc").await,
        vec![vec!["null",""],vec!["decimal","1.25"],vec!["decimal","2.5"]]);
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
        &[&["9007199254740992","1"]]).await;
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
async fn native_high_scale_decimals_preserve_precision_within_34_digits() {
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
            vec!["1.00000000000000000000000000005"],
            vec!["1.00000000000000000000000000015"],
            vec!["-1.00000000000000000000000000005"],
        ]
    );
}

#[tokio::test]
async fn invalid_decimal_literals_and_datatable_values_fail_during_planning() {
    let ctx = SessionContext::new();
    for query in [
        "print d=decimal(invalid)",
        "datatable(d:decimal)['invalid']",
        "range d from decimal(9007199254740993) to decimal(9007199254740994) step decimal(0.1)",
        "print d=bin(decimal(1.25),1h)",
        "print d=bin_at(decimal(1.25),1,datetime(2025-01-01))",
        "print d=iff(true,decimal(1.25),1)",
        "print d=coalesce(decimal(null),1.0)",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
    check(
        "print d=isnull(todecimal('max')),m=tostring(decimal(max))",
        &[&["true", "1E+38"]],
    )
    .await;
}
