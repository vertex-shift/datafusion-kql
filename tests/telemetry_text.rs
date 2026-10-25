use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

async fn check(query: &str, expected: &[&[&str]]) {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
    let frame = ctx
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
                    .map(|array| {
                        if array.logical_nulls().is_some_and(|n| n.is_null(row)) {
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
        .map(|row| row.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{query}");
}

#[tokio::test]
async fn base64_helpers_match_published_kusto_examples() {
    check("print a=base64_encode_tostring('Kusto'),b=base64_decode_tostring('S3VzdG8='),c=tostring(base64_decode_toarray('S3VzdG8='))",
        &[&["S3VzdG8=","Kusto","[75,117,115,116,111]"]]).await;
    check(
        "print a=base64_encodestring('Kusto'),b=base64_decodestring('S3VzdG8=')",
        &[&["S3VzdG8=", "Kusto"]],
    )
    .await;
    check("print a=base64_decode_tostring('Zh=='),b=tostring(base64_decode_toarray('Zh==')),c=base64_decode_tostring('Zg='),d=base64_decode_tostring('Zg===')",
        &[&["f","[102]","",""]]).await;
}

#[tokio::test]
async fn base64_preserves_utf8_and_rejects_whitespace_while_decoding() {
    check("datatable(s:string)['hello','中文','Österreich',''] | project s,roundtrip=base64_decode_tostring(base64_encode_tostring(s))",
        &[&["hello","hello"],&["中文","中文"],&["Österreich","Österreich"],&["",""]]).await;
    check(
        r#"print value=base64_decode_tostring(' S3Vz\tdG8=\n')"#,
        &[&[""]],
    )
    .await;
}

#[tokio::test]
async fn malformed_base64_uses_kql_string_defaults_and_dynamic_nulls() {
    check("print a=isnull(base64_decode_tostring('invalid!')),b=isnull(base64_decode_tostring('////')),c=isnull(base64_decode_toarray('invalid!')),d=array_length(base64_decode_toarray('////'))",
        &[&["false","false","true","3"]]).await;
    check(
        "print n=isnull(base64_decode_tostring('U3RyaW5n0KHR0tGA0L7Rh9C60LA='))",
        &[&["false"]],
    )
    .await;
}

#[tokio::test]
async fn url_encoders_differ_in_space_handling_and_round_trip_unicode() {
    check("print a=url_encode('https://www.bing.com/hello world'),b=url_encode_component('https://www.bing.com/hello world'),c=url_decode('https%3a%2f%2fwww.bing.com%2f')",
        &[&["https%3a%2f%2fwww.bing.com%2fhello+world","https%3a%2f%2fwww.bing.com%2fhello%20world","https://www.bing.com/"]]).await;
    check(
        "print a=url_decode(url_encode('中文 + &')),b=url_decode(url_encode_component('中文 + &'))",
        &[&["中文 + &", "中文 + &"]],
    )
    .await;
}

#[tokio::test]
async fn malformed_url_escapes_are_literal_and_invalid_decoded_utf8_retains_input() {
    check(
        "print a=url_decode('a%ZZ+b%2'),b=url_decode('%ff')",
        &[&["a%ZZ b%2", "%ff"]],
    )
    .await;
    check("print ordinary=url_encode(\"!'()*~_-. /:&\"),component=url_encode_component(\"!'()*~_-. /:&\"),mixed=url_decode('hello%20world%ff')",
        &[&["!'()*~%5f%2d.+%2f%3a%26","!'()*~%5f%2d.%20%2f%3a%26","hello%20world%ff"]]).await;
}

#[tokio::test]
async fn parse_url_exposes_documented_components_without_fetching_anything() {
    check("print u=parse_url('scheme://demo:sample@host:1234/this/is/a/path?k1=v1&k2=v2#fragment') | project Scheme=tostring(u.Scheme),Host=tostring(u.Host),Port=tostring(u.Port),Path=tostring(u.Path),User=tostring(u.Username),Password=tostring(u.Password),Query=tostring(u['Query Parameters'].k1),Fragment=tostring(u.Fragment)",
        &[&["scheme","host","1234","/this/is/a/path","demo","sample","v1","fragment"]]).await;
}

#[tokio::test]
async fn parse_urlquery_preserves_raw_text_and_collects_duplicate_values() {
    check("print q=parse_urlquery('k1=v1&k2=hello+world&k3=%E4%B8%AD%E6%96%87&k1=last') | project a=tostring(q['Query Parameters'].k1),b=tostring(q['Query Parameters'].k2),c=tostring(q['Query Parameters'].k3)",
        &[&["[\"v1\",\"last\"]","hello+world","%E4%B8%AD%E6%96%87"]]).await;
    check("print q=parseurlquery('?flag&empty=&encoded%20key=value') | project a=tostring(q['Query Parameters']['encoded%20key']),b=bag_has_key(q['Query Parameters'],'flag'),c=bag_has_key(q['Query Parameters'],'empty')",
        &[&["value","false","false"]]).await;
}

#[tokio::test]
async fn url_parser_uses_raw_reference_components_not_uri_normalization() {
    check("print u=parseurl('https://[2001:db8::1]/resource') | project host=tostring(u.Host),port=tostring(u.Port),path=tostring(u.Path)",
        &[&["[2001:db8:","1]","/resource"]]).await;
    check("print a=isnull(parse_url('not-an-absolute-url')),b=tostring(parse_url('').Host),c=tostring(parse_urlquery('')['Query Parameters'])", &[&["false","","{}"]]).await;
    check("print u=parse_url('HTTPS://Example.COM/a%20b/../c?a=hello+world&b=%20#x%20y') | project scheme=tostring(u.Scheme),host=tostring(u.Host),port=tostring(u.Port),path=tostring(u.Path),a=tostring(u['Query Parameters'].a),b=tostring(u['Query Parameters'].b),fragment=tostring(u.Fragment)",
        &[&["HTTPS","Example.COM","","/a%20b/../c","hello+world","%20","x%20y"]]).await;
}

#[tokio::test]
async fn regex_trim_helpers_remove_matches_not_literal_character_sets() {
    check(r#"print a=trim(@'\s+','   Hello, world!   '),b=trim('--','----text----'),c=trim_start('--','----text----'),d=trim_end('--','----text----')"#,
        &[&["Hello, world!","--text--","--text----","----text--"]]).await;
    check(
        r#"print a=trim(@'(?P<edge>x)','xxvaluExx'),b=trim(@'[^\w]+','-  Te st1// $')"#,
        &[&["xvaluEx", "Te st1"]],
    )
    .await;
}

#[tokio::test]
async fn native_boolean_text_differs_from_dynamic_json_boolean_text() {
    check("datatable(b:bool)[true,false,bool(null)] | project s=tostring(b),concat=strcat('value:',b)",
        &[&["True","value:True"],&["False","value:False"],&["","value:"]]).await;
    check(
        "print a=tostring(dynamic(true)),b=tostring(dynamic(false)),c=tostring(bool(null))",
        &[&["true", "false", ""]],
    )
    .await;
    check("range x from 1 to 1024 step 1 | extend s=tostring(rand()<0.5) | summarize valid=countif(s in('True','False')),missingCount=countif(isempty(s))",
        &[&["1024","0"]]).await;
}

#[tokio::test]
async fn telemetry_helpers_respect_dynamic_and_typed_scalar_conversions() {
    check("print a=base64_decode_tostring(base64_encode_tostring(decimal(1.25))),b=url_decode(url_encode(dynamic('a b')))", &[&["1.25","a b"]]).await;
}

#[tokio::test]
async fn byte_array_budget_is_checked_before_allocating_excess_elements() {
    let ctx = SessionContext::new();
    let frame=ctx.kql("print a=base64_decode_toarray(base64_encode_tostring(strcat_array(repeat('abcd',262145),'')))").await;
    let error = match frame {
        Ok(frame) => frame.collect().await.unwrap_err().to_string(),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("collection budget"), "{error}");
}

#[tokio::test]
async fn invalid_string_helper_arity_and_patterns_fail_explicitly() {
    let ctx = SessionContext::new();
    for query in [
        "print a=base64_decode_tostring()",
        "print a=parse_url('a','b')",
        "print a=trim('text')",
        "print a=trim('[','text')",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}
