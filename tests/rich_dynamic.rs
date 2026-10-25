use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

const GUID: &str = "74be27de-1e4e-49d9-b579-fe0b331d3642";

async fn check(query: &str, expected: &[&[&str]]) {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(2),
    );
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
                    .map(|array| {
                        if array
                            .logical_nulls()
                            .is_some_and(|nulls| nulls.is_null(row))
                        {
                            "<null>".to_owned()
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
        .map(|row| {
            row.iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{query}");
}

#[tokio::test]
async fn typed_dynamic_literals_preserve_nested_scalar_types() {
    check(&format!("print d=dynamic({{'items':[datetime(2025-01-01),1h,guid({GUID}),decimal(1.25)]}}) | project a=gettype(d.items[0]),b=gettype(d.items[1]),c=gettype(d.items[2]),e=gettype(d.items[3]),value=tostring(todecimal(d.items[3]))"),
        &[&["datetime","timespan","guid","decimal","1.25"]]).await;
}

#[tokio::test]
async fn runtime_pack_array_bag_pack_and_pack_all_preserve_types() {
    check(&format!("print t=datetime(2025-01-01),g=guid({GUID}),d=decimal(1.25) | extend a=pack_array(t,g,d),b=pack_all() | project a=gettype(a[0]),b=gettype(a[1]),c=gettype(a[2]),dt=gettype(b.t),guid=gettype(b.g),decimal=gettype(b.d)"),
        &[&["datetime","guid","decimal","datetime","guid","decimal"]]).await;
    check("print d=bag_pack('x',pack_array(1h,decimal(2.5))) | project a=gettype(d.x[0]),b=gettype(d.x[1])", &[&["timespan","decimal"]]).await;
}

#[tokio::test]
async fn nested_access_does_not_confuse_user_json_with_internal_encodings() {
    check("print d=dynamic({'v':1,'root':0,'nodes':[[7,123]]}) | project a=gettype(d),b=gettype(d.nodes[0][0]),c=tolong(d.nodes[0][1])", &[&["dictionary","long","123"]]).await;
    check("print a=pack_array(bag_pack('t',datetime(2025-01-01)),dynamic({'t':'2025-01-01T00:00:00Z'})) | project a=gettype(a[0].t),b=gettype(a[1].t)", &[&["datetime","string"]]).await;
}

#[tokio::test]
async fn array_transformations_and_zip_preserve_rich_scalars() {
    check(&format!("print a=pack_array(guid({GUID}),1h),b=pack_array(decimal(1.5)) | extend c=array_concat(a,b),z=zip(a,b) | project a=gettype(array_reverse(c)[0]),b=gettype(array_slice(c,0,1)[1]),c=gettype(z[0][0]),d=gettype(z[0][1]),n=gettype(z[1][1])"),
        &[&["decimal","timespan","guid","decimal","null"]]).await;
    check(
        "print a=repeat(datetime(2025-01-01),2) | project a=gettype(a[0]),n=array_length(a)",
        &[&["datetime", "2"]],
    )
    .await;
}

#[tokio::test]
async fn bag_updates_merges_and_removals_keep_typed_values() {
    check("print a=bag_pack('t',datetime(2025-01-01),'d',decimal(1.25)) | extend b=bag_merge(a,bag_pack('t','string','x',1h)) | extend c=bag_remove_keys(b,dynamic(['d'])) | extend d=bag_set_key(c,'n',decimal(2.5)) | project t=gettype(d.t),x=gettype(d.x),n=gettype(d.n),removed=bag_has_key(d,'d')",
        &[&["datetime","timespan","decimal","false"]]).await;
}

#[tokio::test]
async fn equality_set_functions_and_membership_distinguish_typed_scalars_from_strings() {
    check("print a=pack_array(datetime(2025-01-01)),b=dynamic(['2025-01-01T00:00:00Z']) | extend c=set_union(a,b,a) | project equal=a==b,n=array_length(c),t=gettype(c[0]),s=gettype(c[1]),has=set_has_element(c,datetime(2025-01-01)),index=array_index_of(c,'2025-01-01T00:00:00Z')",
        &[&["false","2","datetime","string","true","1"]]).await;
    check("print a=pack_array(decimal(1.25),1h),b=pack_array(1h) | project a=gettype(set_difference(a,b)[0]),b=gettype(set_intersect(a,b)[0]),m=dynamic(decimal(1.25)) in(pack_array(decimal(1.25)))",
        &[&["decimal","timespan","true"]]).await;
}

#[tokio::test]
async fn expanded_nested_values_have_cast_only_scalar_types_and_real_null_padding() {
    check(&format!("print a=pack_array(guid({GUID})),b=dynamic([1,2]) | mv-expand a to typeof(guid),b to typeof(long) | project a=tostring(a),b,kind=gettype(a)"),
        &[&[GUID,"1","guid"],&["","2","null"]]).await;
    check("print a=pack_array(decimal(1.25),decimal(2.5)) | mv-expand a to typeof(decimal) | project a=tostring(a)", &[&["1.25"],&["2.5"]]).await;
}

#[tokio::test]
async fn bags_expand_and_unpack_without_erasing_child_tags() {
    check("print a=bag_pack('d',decimal(1.25),'t',datetime(2025-01-01)) | mv-expand kind=array a | project key=tostring(a[0]),kind=gettype(a[1]) | sort by key asc",
        &[&["d","decimal"],&["t","datetime"]]).await;
    check(&format!("print a=bag_pack('d',decimal(1.25),'t',datetime(2025-01-01),'g',guid({GUID})) | evaluate bag_unpack(a):(d:decimal,t:datetime,g:guid) | project d=tostring(d),t=getyear(t),g=tostring(g)"),
        &[&["1.25","2025",GUID]]).await;
}

#[tokio::test]
async fn collection_aggregates_retain_nested_types_across_batches() {
    check("datatable(x:datetime)[datetime(2025-01-01),datetime(2025-01-02)] | summarize a=make_list(x),s=make_set(x) | project a=gettype(a[0]),b=gettype(a[1]),n=array_length(s),member=set_has_element(s,datetime(2025-01-02))",
        &[&["datetime","datetime","2","true"]]).await;
    check("datatable(x:decimal)[decimal(1.25),decimal(2.5)] | extend bag=bag_pack(tostring(x),x) | summarize b=make_bag(bag) | project a=gettype(b['1.25']),b=tostring(todecimal(b['2.5']))", &[&["decimal","2.5"]]).await;
}

#[tokio::test]
async fn ordered_collections_and_serialized_offsets_retain_rich_nested_payloads() {
    check("datatable(i:long,d:decimal)[2,decimal(2.5),1,decimal(1.25)] | sort by i asc | summarize a=make_list(d) | project a=gettype(a[0]),first=tostring(todecimal(a[0])),last=tostring(todecimal(a[1]))",
        &[&["decimal","1.25","2.5"]]).await;
    check("range x from 1 to 3 step 1 | extend d=bag_pack('value',decimal(1.25)*x) | serialize | project p=gettype(prev(d).value),n=tostring(todecimal(next(d).value))",
        &[&["null","2.5"],&["decimal","3.75"],&["decimal",""]]).await;
}

#[tokio::test]
async fn rich_collections_are_stable_under_materialization_union_and_parse_json() {
    check("let T=materialize(print a=pack_array(decimal(1.25))); T | union T | extend b=parse_json(a) | project kind=gettype(b[0]),value=tostring(todecimal(b[0]))",
        &[&["decimal","1.25"],&["decimal","1.25"]]).await;
    check("print a=pack_array(1h) | extend text=tostring(a) | extend parsed=parse_json(text) | project original=gettype(a[0]),json=gettype(parsed[0])", &[&["timespan","string"]]).await;
}

#[tokio::test]
async fn temporal_scalar_ranges_keep_child_types_without_losing_subtick_values() {
    check("print a=range(datetime(2025-01-01),datetime(2025-01-01T00:00:02Z),1s),b=range(timespan(1ns),timespan(3ns),timespan(1ns)) | project a=gettype(a[0]),b=gettype(b[1]),n=tolong(b[2])",
        &[&["datetime","timespan","3"]]).await;
}

#[tokio::test]
async fn temporal_make_series_axes_expose_typed_elements() {
    check("datatable(t:datetime)[datetime(2025-01-01),datetime(2025-01-02)] | make-series N=count() on t step 1d | project kind=gettype(t[0]),year=getyear(todatetime(t[1]))",
        &[&["datetime","2025"]]).await;
    check("datatable(t:timespan)[1h,2h] | make-series N=count() on t step 1h | project kind=gettype(t[0]),last=tolong(t[1])",
        &[&["timespan","7200000000000"]]).await;
}

#[tokio::test]
async fn nested_real_values_keep_real_tags_and_nonfinite_values() {
    check("print a=pack_array(real(nan),real(inf),1.0) | project a=gettype(a[0]),b=gettype(a[1]),c=gettype(a[2]),nan=isnan(toreal(a[0]))",
        &[&["real","real","real","true"]]).await;
}

#[tokio::test]
async fn dynamic_distinct_counts_do_not_merge_typed_values_with_equal_json_text() {
    check("union (print a=pack_array(datetime(2025-01-01))),(print a=dynamic(['2025-01-01T00:00:00Z'])) | summarize N=count_distinct(a)", &[&["2"]]).await;
}
