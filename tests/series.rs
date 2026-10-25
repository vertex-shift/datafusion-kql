use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

async fn check(query: &str, expected: &[&[&str]]) {
    let frame = SessionContext::new()
        .kql(query)
        .await
        .unwrap_or_else(|e| panic!("{}\n{query}", e.diagnostic(query)));
    let logical = frame.logical_plan().display_indent().to_string();
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{e}\n{query}\n{logical}"));
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
        .map(|r| r.iter().map(|v| v.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{query}");
}

#[tokio::test]
async fn bin_at_anchors_numeric_and_temporal_values_without_integer_rounding() {
    check(
        "print a=bin_at(7,5,1),b=bin_at(-1,5,1),c=bin_at(9007199254740993,10,0),d=bin_at(datetime(2025-01-01T03:30:00Z),1h,datetime(2025-01-01T00:15:00Z)),e=bin(1,0.5)",
        &[&["6","-4","9007199254740990","2025-01-01T03:15:00","1.0"]],
    ).await;
}

#[tokio::test]
async fn make_series_fills_gaps_and_respects_exclusive_end_bounds() {
    check(
        "datatable(x:long,v:long)[0,10,0,20,2,30] | make-series N=count(),S=sum(v),A=avg(v) default=double(null) on x from 0 to 4 step 1 | project N=tostring(N),S=tostring(S),A=tostring(A),x=tostring(x)",
        &[&["[2,0,1,0]","[30,0,30,0]","[15.0,null,30.0,null]","[0,1,2,3]"]],
    ).await;
}

#[tokio::test]
async fn automatic_bounds_are_computed_per_group_and_keep_nonzero_origins() {
    check(
        "datatable(g:string,x:long)['a',1,'a',3,'b',10,'b',12] | make-series N=count() on x step 2 by g | project g,N=tostring(N),x=tostring(x) | sort by g asc",
        &[&["a","[1,1]","[1,3]"],&["b","[1,1]","[10,12]"]],
    ).await;
}

#[tokio::test]
async fn datetime_axes_are_dynamic_arrays_and_use_anchored_time_buckets() {
    check(
        "datatable(t:datetime)[datetime(2025-01-01T00:01:00Z),datetime(2025-01-01T02:20:00Z)] | make-series N=count() on t from datetime(2025-01-01) to datetime(2025-01-01T03:00:00Z) step 1h | project N=tostring(N),t=tostring(t)",
        &[&["[1,0,1]","[\"2025-01-01T00:00:00Z\",\"2025-01-01T01:00:00Z\",\"2025-01-01T02:00:00Z\"]"]],
    ).await;
}

#[tokio::test]
async fn automatic_datetime_origins_and_timespan_axes_remain_exact() {
    check(
        "datatable(t:datetime)[datetime(2025-01-01T00:01:00Z),datetime(2025-01-01T02:20:00Z)] | make-series N=count() on t step 1h | project first=todatetime(t[0]),last=todatetime(t[2]),N=tostring(N)",
        &[&["2025-01-01T00:01:00","2025-01-01T02:01:00","[1,0,1]"]],
    ).await;
    check(
        "datatable(t:timespan)[1h,3h] | make-series N=count() on t step 1h | project first=totimespan(t[0]),N=tostring(N)",
        &[&["PT3600S","[1,0,1]"]],
    ).await;
}

#[tokio::test]
async fn nonempty_controls_empty_ungrouped_results() {
    check(
        "datatable(x:long)[] | make-series N=count() on x from 0 to 3 step 1 | count",
        &[&["0"]],
    )
    .await;
    check(
        "datatable(x:long)[] | make-series kind=nonempty N=count() default=9 on x from 0 to 3 step 1 | project N=tostring(N),x=tostring(x)",
        &[&["[9,9,9]","[0,1,2]"]],
    ).await;
}

#[tokio::test]
async fn alternate_range_syntax_uses_inclusive_stop_and_epoch_aligned_bins() {
    check(
        "range x from 1 to 5 step 1 | make-series N=count() on x in range(1,5,2) | project N=tostring(N),x=tostring(x)",
        &[&["[1,2,2]","[0,2,4]"]],
    ).await;
}

#[tokio::test]
async fn real_axes_and_aggregate_arithmetic_are_supported() {
    check(
        "datatable(x:real,v:long)[0.5,2,1.0,4] | make-series V=sum(v)*2 on x from 0.0 to 1.25 step 0.25 | project V=tostring(V),x=tostring(x)",
        &[&["[0,0,4,0,8]","[0.0,0.25,0.5,0.75,1.0]"]],
    ).await;
}

#[tokio::test]
async fn series_bounds_keep_large_integer_axes_exact() {
    check(
        "datatable(x:long)[9007199254740993,9007199254740995] | make-series N=count() on x from 9007199254740993 to 9007199254740996 step 1 | project x=tostring(x),N=tostring(N)",
        &[&["[9007199254740993,9007199254740994,9007199254740995]","[1,0,1]"]],
    ).await;
}

#[tokio::test]
async fn invalid_steps_and_nonnumeric_aggregates_fail_explicitly() {
    let ctx = SessionContext::new();
    for query in [
        "range x from 1 to 2 step 1 | make-series N=count() on x step 0",
        "range x from 1 to 2 step 1 | make-series N=count() on x step -1",
        "range x from 1 to 2 step 1 | make-series V=make_list(x) on x step 1",
        "print t=datetime(2025-01-01) | make-series N=count() on t step 1",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
    let query = "range x from 1 to 1 step 1 | make-series N=count() on x from 0 to 1048577 step 1";
    let result = match ctx.kql(query).await {
        Ok(frame) => frame.collect().await.map(|_| ()).map_err(|e| e.to_string()),
        Err(error) => Err(error.to_string()),
    };
    assert!(result.is_err());
}

#[tokio::test]
async fn series_arithmetic_pads_missing_and_nonnumeric_elements() {
    check(
        "print a=tostring(series_add(dynamic([1,2,null,'x']),dynamic([4,5,6]))),b=tostring(series_divide(dynamic([1,2]),dynamic([4,4]))),c=tostring(series_greater(dynamic([1,3]),dynamic([2,2])))",
        &[&["[5,7,null,null]","[0.25,0.5]","[false,true]"]],
    ).await;
}

#[tokio::test]
async fn series_unary_math_and_comparisons_are_elementwise() {
    check(
        "print a=tostring(series_sqrt(dynamic([1,4,null]))),b=tostring(series_abs(dynamic([-2,0,3]))),c=tostring(series_sign(dynamic([-2,0,3])))",
        &[&["[1.0,2.0,null]","[2,0,3]","[-1,0,1]"]],
    ).await;
}

#[tokio::test]
async fn series_fill_functions_preserve_edges_and_integer_precision() {
    check(
        "let a=dynamic([null,1,null,3,null]); print f=tostring(series_fill_forward(a)),b=tostring(series_fill_backward(a)),c=tostring(series_fill_const(a,9)),l=tostring(series_fill_linear(a)),n=tostring(series_fill_linear(a,double(null),false))",
        &[&["[null,1,1,3,3]","[1,1,3,3,null]","[9,1,9,3,9]","[1,1,2,3,3]","[null,1,2,3,null]"]],
    ).await;
    check(
        "print a=tostring(series_fill_linear(dynamic([9007199254740993,null,9007199254740995]))),b=tostring(series_fill_linear(dynamic([null,null]))),c=tostring(series_fill_linear(dynamic([null,null]),double(null),true,double(null)))",
        &[&["[9007199254740993,9007199254740994,9007199254740995]","[0.0,0.0]","[null,null]"]],
    ).await;
}

#[tokio::test]
async fn series_statistics_use_sample_variance_and_original_indices() {
    check(
        "print s=series_stats_dynamic(dynamic([1,null,2,3]),true) | project min=tolong(s.min),mi=tolong(s.min_idx),max=tolong(s.max),ma=tolong(s.max_idx),avg=toreal(s.avg),v=toreal(s.variance),sum=toreal(s.sum),n=tolong(s.len)",
        &[&["1","0","3","3","2.0","1.0","6.0","4"]],
    ).await;
    check(
        "print s=tostring(series_stats_dynamic(dynamic([1,null,2])))",
        &[&[""]],
    )
    .await;
}

#[tokio::test]
async fn linear_fit_returns_a_correlated_bag_and_fitted_series() {
    check(
        "print f=series_fit_line_dynamic(dynamic([1,3,5])) | project slope=toreal(f.slope),b=toreal(f.interception),r=toreal(f.rsquare),v=toreal(f.variance),rv=toreal(f.rvariance),line=tostring(f.line_fit)",
        &[&["2.0","1.0","1.0","4.0","0.0","[1.0,3.0,5.0]"]],
    ).await;
}

#[tokio::test]
async fn fir_and_iir_follow_documented_filter_and_normalization_rules() {
    check(
        "print a=tostring(series_fir(dynamic([1,2,3]),dynamic([1,1]))),b=tostring(series_fir(dynamic([1,2,3]),dynamic([1,-1]))),c=tostring(series_iir(dynamic([1,2,3]),dynamic([1]),dynamic([1,-1]))),invalid=tostring(series_fir(dynamic([1,2,3]),dynamic([1,-1]),true))",
        &[&["[0.5,1.5,2.5]","[1.0,1.0,1.0]","[1.0,3.0,6.0]",""]],
    ).await;
}

#[tokio::test]
async fn make_series_interpolation_and_statistics_compose_end_to_end() {
    check(
        "datatable(x:long,v:real)[0,0.0,2,20.0] | make-series V=avg(v) default=double(null) on x from 0 to 3 step 1 | extend filled=series_fill_linear(V) | extend stats=series_stats_dynamic(filled) | project filled=tostring(filled),avg=toreal(stats.avg),variance=toreal(stats.variance)",
        &[&["[0.0,10.0,20.0]","10.0","100.0"]],
    ).await;
}

#[tokio::test]
async fn series_comparisons_keep_large_integral_values_distinct() {
    check(
        "print greater=tostring(series_greater(dynamic([9007199254740993]),dynamic([9007199254740992]))),equal=tostring(series_equals(dynamic([9007199254740993]),dynamic([9007199254740992]))),less=tostring(series_less_equals(dynamic([9007199254740992]),dynamic([9007199254740993])))",
        &[&["[true]","[false]","[true]"]],
    ).await;
}

#[tokio::test]
async fn fill_placeholders_convert_numeric_representations_without_saturation() {
    check(
        "print a=tostring(series_fill_const(dynamic([-1.0,2.0]),3,-1)),b=tostring(series_fill_forward(dynamic([1,-1,2]),'-1')),c=tostring(series_fill_const(dynamic([null,1]),1e30)),d=tostring(series_fill_linear(dynamic([-1,-1]),-1))",
        &[&["[3.0,2.0]","[1,1,2]","[null,1]","[0,0]"]],
    ).await;
}

#[tokio::test]
async fn make_series_accepts_exactly_the_documented_point_limit() {
    check(
        "datatable(x:long)[] | make-series kind=nonempty N=count() on x from 0 to 1048576 step 1 | project Points=array_length(x),Values=array_length(N)",
        &[&["1048576","1048576"]],
    ).await;
    let query = "print x=0.0 | make-series N=count() on x from 0.0 to 1048577.0 step 1.0";
    let result = match SessionContext::new().kql(query).await {
        Ok(frame) => frame.collect().await.map(|_| ()).map_err(|e| e.to_string()),
        Err(error) => Err(error.to_string()),
    };
    assert!(result.is_err());
}
