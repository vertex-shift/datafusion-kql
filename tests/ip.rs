use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

async fn check(query: &str, expected: &[&[&str]]) {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_batch_size(2)
            .with_target_partitions(4),
    );
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
async fn ipv4_parsing_and_masks_match_published_examples() {
    check("datatable(ip:string)['192.168.1.1','192.168.1.1/24','255.255.255.255/31'] | project value=parse_ipv4(ip)",
        &[&["3232235777"],&["3232235776"],&["4294967294"]]).await;
    check("print a=parse_ipv4_mask('127.0.0.1',24),b=parse_ipv4('255.255.255.255/0'),c=ipv4_netmask_suffix('10.1.2.3'),d=ipv4_netmask_suffix('127.0.0.1/16')",
        &[&["2130706432","0","32","16"]]).await;
}

#[tokio::test]
async fn ipv4_formatting_accepts_numeric_values_and_combines_prefixes() {
    check("datatable(ip:string,mask:long)['192.168.1.1',24,'192.168.1.1',32,'192.168.1.1/24',32,'192.168.1.1/24',-1] | project a=format_ipv4(ip,mask),b=format_ipv4_mask(ip,mask)",
        &[&["192.168.1.0","192.168.1.0/24"],&["192.168.1.1","192.168.1.1/32"],&["192.168.1.0","192.168.1.0/24"],&["",""]]).await;
    check("print a=format_ipv4(3232235777),b=format_ipv4_mask(3232235777,24),c=format_ipv4(4294967295),d=format_ipv4(4294967296)",
        &[&["192.168.1.1","192.168.1.0/24","255.255.255.255",""]]).await;
}

#[tokio::test]
async fn ipv4_comparisons_take_the_least_specific_combined_prefix() {
    check("print a=ipv4_compare('192.168.1.1/30','192.168.1.255/24'),b=ipv4_compare('192.168.1.1','192.168.1.0',31),c=ipv4_compare('192.168.1.1','192.168.1.2'),d=ipv4_is_match('192.168.1.1','192.168.1.2',24)",
        &[&["0","0","-1","true"]]).await;
}

#[tokio::test]
async fn invalid_ipv4_values_and_prefixes_return_nullable_or_empty_results() {
    check("datatable(ip:string)['bad','256.1.2.3','1.2.3','1.2.3.4/33','1.2.3.4/-1','1.2.3.4/1/2','1.2.3.4:80'] | project p=isnull(parse_ipv4(ip)),f=format_ipv4(ip)",
        &[&["true",""],&["true",""],&["true",""],&["true",""],&["true",""],&["true",""],&["true",""]]).await;
    check("print a=ipv4_compare('bad','1.2.3.4'),b=ipv4_is_match('1.2.3.4','1.2.3.4',33),c=parse_ipv4_mask('1.2.3.4',long(null))",
        &[&["<null>","<null>","<null>"]]).await;
}

#[tokio::test]
async fn private_ipv4_detection_uses_rfc1918_not_generic_reserved_ranges() {
    check("datatable(ip:string)['10.1.2.3','172.16.0.1','172.31.255.255','192.168.1.1','172.32.0.1','127.0.0.1','169.254.1.1','8.8.8.8','bad'] | project private=ipv4_is_private(ip)",
        &[&["true"],&["true"],&["true"],&["true"],&["false"],&["false"],&["false"],&["false"],&["<null>"]]).await;
}

#[tokio::test]
async fn ipv4_range_membership_supports_hosts_full_ranges_and_varargs() {
    check("print a=ipv4_is_in_range('192.168.1.1','192.168.1.255/24'),b=ipv4_is_in_range('192.168.1.1','192.168.1.1'),c=ipv4_is_in_any_range('192.168.1.6','10.0.0.0/8','192.168.1.0/24'),d=ipv4_is_in_range('255.255.255.255','0.0.0.0/0')",
        &[&["true","true","true","true"]]).await;
}

#[tokio::test]
async fn ipv4_dynamic_range_arrays_are_cached_without_stale_row_results() {
    check("datatable(ip:string,ranges:dynamic)['10.1.2.3',dynamic(['10.0.0.0/8']),'10.1.2.3',dynamic(['192.168.0.0/16']),'192.168.1.1',dynamic(['192.168.0.0/16']),'8.8.8.8',dynamic([])] | project inside=ipv4_is_in_any_range(ip,ranges)",
        &[&["true"],&["false"],&["true"],&["false"]]).await;
}

#[tokio::test]
async fn invalid_ranges_are_not_silently_ignored_even_if_another_range_matches() {
    check("print a=ipv4_is_in_any_range('10.1.2.3',dynamic(['10.0.0.0/8','invalid'])),b=ipv4_is_in_range('bad','0.0.0.0/0'),c=ipv4_is_in_any_range('10.1.2.3',dynamic([]))",
        &[&["<null>","<null>","false"]]).await;
}

#[tokio::test]
async fn parse_ipv6_produces_full_width_canonical_strings_for_ipv4_and_ipv6() {
    check("datatable(ip:string)['192.168.255.255','192.168.255.255/24','255.255.255.255','FE80::1','::1','bad'] | project parsed=parse_ipv6(ip)",
        &[&["0000:0000:0000:0000:0000:ffff:c0a8:ffff"],&["0000:0000:0000:0000:0000:ffff:c0a8:ff00"],&["0000:0000:0000:0000:0000:ffff:ffff:ffff"],&["fe80:0000:0000:0000:0000:0000:0000:0001"],&["0000:0000:0000:0000:0000:0000:0000:0001"],&[""]]).await;
}

#[tokio::test]
async fn ipv6_comparisons_support_mapped_ipv4_and_ipv6_prefixes() {
    check("print a=ipv6_compare('192.168.1.1','::ffff:c0a8:0101'),b=ipv6_compare('192.168.1.1/24','::ffff:c0a8:01ff'),c=ipv6_compare('fe80::85d:e82c:9446:7994/120','fe80::85d:e82c:9446:7998'),d=ipv6_compare('::192.168.1.1/30','192.168.1.255/24')",
        &[&["0","0","0","0"]]).await;
    check("print a=ipv6_compare('2001:db8::1','2001:db8::2'),b=ipv6_is_match('2001:db8::1','2001:db8::2',64)", &[&["-1","true"]]).await;
}

#[tokio::test]
async fn ipv6_ranges_support_boundary_prefixes_and_nested_ipv4_ranges() {
    check("print a=ipv6_is_in_range('a5e:f127:8a9d:146d:e102:b5d3:c755:abcd','a5e:f127:8a9d:146d:e102:b5d3:c755:0000/112'),b=ipv6_is_in_range('2001:db8::1','::/0'),c=ipv6_is_in_any_range('192.168.1.1',dynamic(['2001:db8::/32','192.168.0.0/16'])),d=ipv6_is_in_range('2001:db8::1','192.168.0.0/16')",
        &[&["true","true","true","false"]]).await;
}

#[tokio::test]
async fn invalid_ipv6_addresses_and_masks_are_handled_explicitly() {
    check("print a=parse_ipv6('fe80::1%eth0'),b=parse_ipv6('[::1]'),c=parse_ipv6('::1/129'),d=ipv6_compare('bad','::1'),e=ipv6_is_in_range('::1','bad')",
        &[&["","","","<null>","<null>"]]).await;
}

#[tokio::test]
async fn overlapping_cidr_ranges_are_merged_without_changing_membership() {
    check("datatable(ip:string)['10.0.0.0','10.127.255.255','10.128.0.0','10.255.255.255','11.0.0.0'] | project inside=ipv4_is_in_any_range(ip,dynamic(['10.0.0.0/9','10.128.0.0/9','10.1.0.0/16']))",
        &[&["true"],&["true"],&["true"],&["true"],&["false"]]).await;
}

#[tokio::test]
async fn dynamic_sources_and_lexical_query_values_are_supported() {
    check("let ranges=dynamic(['10.0.0.0/8','192.168.0.0/16']); let local=(ip:string){ipv4_is_in_any_range(ip,ranges)}; print a=local('10.1.2.3'),b=parse_ipv4(dynamic('127.0.0.1'))",
        &[&["true","2130706433"]]).await;
}

#[tokio::test]
async fn invalid_argument_counts_and_noninteger_masks_fail_in_planning() {
    let ctx = SessionContext::new();
    for query in [
        "print a=parse_ipv4()",
        "print a=parse_ipv4_mask('1.2.3.4')",
        "print a=ipv4_is_in_any_range('1.2.3.4')",
        "print a=ipv6_compare('::1','::2',3.5)",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}
