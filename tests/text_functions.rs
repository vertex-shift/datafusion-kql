use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

async fn check(query: &str, expected: &[&[&str]]) {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
    let frame = ctx
        .kql(query)
        .await
        .unwrap_or_else(|error| panic!("{}\n{query}", error.diagnostic(query)));
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{error}\n{query}"));
    let actual = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column
                            .logical_nulls()
                            .is_some_and(|nulls| nulls.is_null(row))
                        {
                            "<null>".to_owned()
                        } else {
                            array_value_to_string(column, row).unwrap()
                        }
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    let expected = expected
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{query}");
}

#[tokio::test]
async fn indexof_matches_all_published_examples_including_negative_starts() {
    check("print a=indexof('abcdefg','cde'),b=indexof('abcdefg','cde',1,4),c=indexof('abcdefg','cde',1,2),d=indexof('abcdefg','cde',3,4),e=indexof('abcdefg','cde',-5),f=indexof(1234567,5,1,4),g=indexof('abcdefg','cde',2,-1),h=indexof('abcdefgabcdefg','cde',1,10,2),i=indexof('abcdefgabcdefg','cde',1,-1,3)",
        &[&["2","2","-1","-1","2","4","2","9","-1"]]).await;
}

#[tokio::test]
async fn indexof_positions_and_lengths_can_vary_by_row() {
    check("datatable(s:string,n:string,start:long,length:long)['abracadabra','abra',0,-1,'abracadabra','abra',1,-1,'abc','b',0,1,'abc','a',9,1,'abc','a',-4,-1,'abc','a',0,-2,'abc','a',long(null),-1] | project p=indexof(s,n,start,length)",
        &[&["0"],&["7"],&["-1"],&["-1"],&["<null>"],&["<null>"],&["<null>"]]).await;
    check("print a=indexof('aaaa','aa',0,-1,2),b=indexof('abc','',0,-1,4),c=indexof('',''),d=indexof('abc','a',0,-1,0),e=indexof('abc','a',0,-1,long(null))",
        &[&["2","3","0","<null>","<null>"]]).await;
}

#[tokio::test]
async fn indexof_counts_unicode_characters_instead_of_utf8_bytes() {
    check("print a=indexof('é中🙂abc','a'),b=indexof('é中🙂abc','中🙂',1,2),c=indexof('é中🙂abc','🙂',-4,1),d=indexof('é中🙂abc','中🙂',1,1)",
        &[&["3","1","2","-1"]]).await;
}

#[tokio::test]
async fn strrep_supports_delimiters_and_scalar_string_conversion() {
    check("print a=strrep('ABC',2),b=strrep(123,3,'.'),c=strrep(3s,2,' '),d=strrep(true,2,'/'),e=strrep(dynamic({'x':1}),2,';')",
        &[&["ABCABC","123.123.123","00:00:03 00:00:03","True/True","{\"x\":1};{\"x\":1}"]]).await;
    check("datatable(s:string,n:long,delimiter:string)['a',3,'-','b',1,'x','c',0,'x','d',-1,'x','e',long(null),'x','',3,'/'] | project s=strrep(s,n,delimiter)",
        &[&["a-a-a"],&["b"],&[""],&[""],&[""],&["//"]]).await;
}

#[tokio::test]
async fn native_and_dynamic_timespan_string_coercion_uses_one_clock_formatter() {
    check("print a=tostring(3s),b=tostring(dynamic(3s)),c=strrep(dynamic(3s),2,' '),d=strcat('elapsed:',3s)",
        &[&["00:00:03","00:00:03","00:00:03 00:00:03","elapsed:00:00:03"]]).await;
    check(
        "datatable(d:timespan)[timespan(null),-1s,1.25d,1ns,1tick] | project value=tostring(d)",
        &[
            &[""],
            &["-00:00:01"],
            &["1.06:00:00"],
            &["00:00:00.000000001"],
            &["00:00:00.0000001"],
        ],
    )
    .await;
}

#[tokio::test]
async fn strrep_is_not_the_dynamic_array_repeat_function() {
    check("print a=gettype(strrep('x',3)),b=gettype(repeat('x',3)),c=strrep('x',1025) | project a,b,n=strlen(c)",
        &[&["string","array","1025"]]).await;
}

#[tokio::test]
async fn regex_replacement_is_global_and_uses_kql_capture_not_dollar_interpolation() {
    check(r#"print a=replace_regex('x=1;y=23',@'(\w)=(\d+)',@'\2:\1'),b=replace_regex('aba','a',@'<\0>'),c=replace_regex('a1 a2',@'a(\d)',@'$\1'),d=replace_regex('aba','a',@'\\')"#,
        &[&["1:x;23:y","<a>b<a>","$1 $2","\\b\\"]]).await;
    check(
        r#"datatable(s:string)['a1','a2','a3'] | project s=replace_regex(s,@'a(\d)',@'item-\1')"#,
        &[&["item-1"], &["item-2"], &["item-3"]],
    )
    .await;
}

#[tokio::test]
async fn regex_replacement_handles_optional_empty_and_multiline_matches() {
    check(r#"print a=replace_regex('b ab',@'(a)?b',@'<\1>'),b=replace_regex('ab','',':'),c=replace_regex('aaaa','aa','x'),d=replace_regex('a\nb',@'(?m)^','>')"#,
        &[&["<> <a>",":a:b:","xx",">a\n>b"]]).await;
}

#[tokio::test]
async fn text_functions_expand_lexical_functions_and_keep_host_registry_entries() {
    check(r#"let f=(s:string){replace_regex(s,@'\d+','#')}; let n=2; print a=f('n=12'),b=indexof('a-a-a','a',0,-1,n),c=strrep('x',n)"#,
        &[&["n=#","2","xx"]]).await;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    use std::sync::Arc;
    let ctx = SessionContext::new();
    ctx.register_udf(create_udf(
        "strrep",
        vec![DataType::Utf8, DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|_| {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "host".into(),
            ))))
        }),
    ));
    let kql = ctx
        .kql("print a=strrep('x',2)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(array_value_to_string(kql[0].column(0), 0).unwrap(), "xx");
    let sql = ctx
        .sql("select strrep('x',2)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(array_value_to_string(sql[0].column(0), 0).unwrap(), "host");
}

#[tokio::test]
async fn invalid_text_function_arguments_fail_explicitly() {
    let ctx = SessionContext::new();
    for query in [
        "print x=indexof('a')",
        "print x=indexof('a','b',1.5)",
        "range x from 1 to 3 step 1 | project p=indexof('aaa','a',0,-1,x)",
        "print x=strrep('a')",
        "print x=strrep('a',1.5)",
        "print x=replace_regex('a','[','x')",
        r#"print x=replace_regex('a','a',@'\1')"#,
        r#"print x=replace_regex('a','a',@'\q')"#,
        "datatable(pattern:string)['a'] | project x=replace_regex('abc',pattern,'x')",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}

#[tokio::test]
async fn large_outputs_error_before_allocation_and_later_rows_do_not_hide_errors() {
    let ctx = SessionContext::new();
    for query in [
        "print x=strrep('a',67108865)",
        "print x=strrep('ab',16777216)",
        "datatable(n:long)[1,67108865] | project x=strrep('x',n)",
        r#"print x=replace_regex(strrep('a',1048576),'a',strrep('b',17))"#,
    ] {
        let error = match ctx.kql(query).await {
            Ok(frame) => frame.collect().await.unwrap_err().to_string(),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("budget") || error.contains("multiplier"),
            "{error}"
        );
    }
}
