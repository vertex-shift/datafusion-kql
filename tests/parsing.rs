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
    let actual = batches
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
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    let expected = expected
        .iter()
        .map(|row| row.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{query}");
}

#[tokio::test]
async fn extract_supports_full_matches_captures_and_type_literals() {
    check(
        r#"print a=extract(@"(\d+)",0,'n=42'),b=extract(@"n=(\d+)",1,'n=42',typeof(long)),c=extract(@"n=(\d+)",1,'none'),d=extract(@"n=(\d+)",1,'none',typeof(long))"#,
        &[&["42","42","","<null>"]],
    ).await;
}

#[tokio::test]
async fn extract_capture_group_can_vary_per_row_and_out_of_range_is_null() {
    check(
        r#"datatable(g:long)[0,1,2,-1,9223372036854775807] | project g,value=extract(@"x=(\d+)",g,'x=42')"#,
        &[&["0","x=42"],&["1","42"],&["2",""],&["-1",""],&["9223372036854775807",""]],
    ).await;
}

#[tokio::test]
async fn extract_all_keeps_single_and_multiple_group_shapes() {
    check(
        r#"print a=tostring(extract_all(@"(\d+)",'a12b34')),b=tostring(extract_all(@"([a-z])(\d+)",'a12b34')),c=tostring(extract_all(@"(\d+)",'none'))"#,
        &[&["[\"12\",\"34\"]","[[\"a\",\"12\"],[\"b\",\"34\"]]",""]],
    ).await;
}

#[tokio::test]
async fn extract_all_selects_named_and_numbered_groups() {
    check(
        r#"print a=tostring(extract_all(@"(?P<letter>[a-z])(?P<number>\d+)",dynamic(['number',1]),'a12b34'))"#,
        &[&["[[\"12\",\"a\"],[\"34\",\"b\"]]"]],
    ).await;
}

#[tokio::test]
async fn extract_json_defaults_to_string_and_supports_typed_paths() {
    check(
        r#"let j='{"a.b":[{"x":1},{"x":"42"}],"name":"Ada"}'; print a=extract_json("$.name",j),t=gettype(extract_json("$.name",j)),b=extract_json("$['a.b'][1].x",j,typeof(long)),c=tostring(extract_json("$['a.b'][0]",j,typeof(dynamic))),d=extract_json("$.missing",j),e=extract_json("$.missing",j,typeof(long))"#,
        &[&["Ada","string","42","{\"x\":1}","","<null>"]],
    ).await;
}

#[tokio::test]
async fn malformed_json_and_invalid_paths_are_not_success_shaped() {
    check(
        r#"print a=extract_json("$",'bad'),b=extract_json("$.x",'bad',typeof(long))"#,
        &[&["", "<null>"]],
    )
    .await;
    let ctx = SessionContext::new();
    for query in [
        r#"print x=extract_json("no root",'{}')"#,
        r#"print x=extract_json("$..x",'{}')"#,
        r#"print x=extract_json("$[*]",'[]')"#,
        r#"print x=extract_json("$[",'[]')"#,
        r#"print x=extract_all("no groups",'text')"#,
        r#"print x=extract_all("(x)",dynamic([2]),'x')"#,
        r#"print x=extract("(",0,'x')"#,
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}

#[tokio::test]
async fn parse_simple_extends_typed_columns_and_preserves_input_rows() {
    check(
        r#"datatable(s:string)['name=Ada, n=42','name=Bob, n=7','bad'] | parse s with 'name=' Name ', n=' Number:long | project Name,Number"#,
        &[&["Ada","42"],&["Bob","7"],&["","<null>"]],
    ).await;
}

#[tokio::test]
async fn strict_parse_invalidates_the_entire_extraction_when_one_type_fails() {
    check(
        r#"datatable(s:string)['name=Ada, n=999999999999999999999','name=Bob, n=bad'] | parse s with 'name=' Name ', n=' Number:long | project Name,Number"#,
        &[&["","<null>"],&["","<null>"]],
    ).await;
}

#[tokio::test]
async fn relaxed_parse_keeps_other_captures_when_conversion_fails() {
    check(
        r#"datatable(s:string)['name=Ada, n=bad','name=Bob, n=7'] | parse kind=relaxed s with 'name=' Name ', n=' Number:long | project Name,Number"#,
        &[&["Ada","<null>"],&["Bob","7"]],
    ).await;
}

#[tokio::test]
async fn parse_where_filters_unsuccessful_rows() {
    check(
        r#"datatable(s:string)['name=Ada, n=42','bad','name=Bob, n=999999999999999999999'] | parse-where s with 'name=' Name ', n=' Number:long | project Name,Number"#,
        &[&["Ada","42"]],
    ).await;
}

#[tokio::test]
async fn parse_regex_flags_and_external_capture_groups_do_not_shift_outputs() {
    check(
        r#"print s='ID7:ADA;42' | parse kind=regex flags=i s with @'id(\d+):' Name ';' Number:long | project Name,Number"#,
        &[&["ADA","42"]],
    ).await;
}

#[tokio::test]
async fn parse_wildcards_and_leading_captures_follow_kql_boundaries() {
    check(
        r#"print s='prefix n=42 suffix' | parse s with * 'n=' Number:long ' suffix' | project Number"#,
        &[&["42"]],
    ).await;
    check(
        r#"print s='Ada:42' | parse s with Name ':' Number:long | project Name,Number"#,
        &[&["Ada", "42"]],
    )
    .await;
    assert!(
        SessionContext::new()
            .kql(r#"print s='a' | parse s with Name *"#)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn parse_datetime_and_timespan_use_nullable_typed_converters() {
    check(
        r#"print s='at=2025-01-01T12:00:00Z; duration=01:30:00' | parse s with 'at=' At:datetime '; duration=' Duration:timespan | project At,Duration"#,
        &[&["2025-01-01T12:00:00","PT5400S"]],
    ).await;
    check(
        r#"print a=todatetime('02/17/2016 08:40:01'),b=totimespan('01:30:00'),c=totimespan('bad')"#,
        &[&["2016-02-17T08:40:01", "PT5400S", "<null>"]],
    )
    .await;
}

#[tokio::test]
async fn parse_replaces_existing_output_columns_without_leaking_helpers() {
    let frame = SessionContext::new()
        .kql(r#"print s='a=42',a=1 | parse s with 'a=' a:long"#)
        .await
        .unwrap();
    assert_eq!(
        frame
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        ["s", "a"]
    );
    check(
        r#"print s='a=42',a=1 | parse s with 'a=' a:long | project a"#,
        &[&["42"]],
    )
    .await;
}
