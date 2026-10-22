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
        .map(|r| r.iter().map(|v| v.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{query}");
}

#[tokio::test]
async fn calendar_addition_clamps_month_ends_and_leap_days() {
    check(
        "print m=datetime_add('month',1,datetime(2024-01-31)),y=datetime_add('year',1,datetime(2024-02-29)),h=datetime_add('hour',-1,datetime(2025-01-01))",
        &[&["2024-02-29T00:00:00","2025-02-28T00:00:00","2024-12-31T23:00:00"]],
    ).await;
}

#[tokio::test]
async fn datetime_diff_counts_calendar_boundaries_instead_of_elapsed_rounding() {
    check(
        "print d=datetime_diff('day',datetime(2025-01-02T00:00:01Z),datetime(2025-01-01T23:59:59Z)),m=datetime_diff('month',datetime(2025-02-01),datetime(2025-01-31)),h=datetime_diff('hour',datetime(1970-01-01),datetime(1969-12-31T23:59:59Z)),n=datetime_diff('nanosecond',datetime(2025-01-01T00:00:00.0000001Z),datetime(2025-01-01))",
        &[&["1","1","1","100"]],
    ).await;
}

#[tokio::test]
async fn period_boundaries_use_sunday_weeks_and_kusto_ticks() {
    check(
        "print w=startofweek(datetime(2025-01-08T12:34:00Z)),m=startofmonth(datetime(2025-01-08),1),y=startofyear(datetime(2025-06-01)),e=format_datetime(endofday(datetime(2025-01-08)),'yyyy-MM-dd HH:mm:ss.fffffff')",
        &[&["2025-01-05T00:00:00","2025-02-01T00:00:00","2025-01-01T00:00:00","2025-01-08 23:59:59.9999999"]],
    ).await;
}

#[tokio::test]
async fn date_parts_have_kql_integer_types_and_dayofweek_is_a_timespan() {
    use datafusion::arrow::datatypes::DataType;
    let ctx = SessionContext::new();
    let frame=ctx.kql("print y=getyear(datetime(2025-01-02)),m=monthofyear(datetime(2025-01-02)),d=dayofmonth(datetime(2025-01-02)),h=hourofday(datetime(2025-01-02T03:00:00Z))").await.unwrap();
    assert!(
        frame
            .schema()
            .fields()
            .iter()
            .all(|f| f.data_type() == &DataType::Int32)
    );
    check("print w=dayofweek(datetime(2025-01-06)),y=dayofyear(datetime(2025-01-02)),p=datetime_part('month',datetime(2025-01-02))",&[&["PT86400S","2","1"]]).await;
}

#[tokio::test]
async fn unix_conversions_cover_seconds_through_nanoseconds() {
    check(
        "print s=unixtime_seconds_todatetime(1.5),m=unixtime_milliseconds_todatetime(1500),u=unixtime_microseconds_todatetime(1500000),n=unixtime_nanoseconds_todatetime(1500000000),r=datetime_to_unixtime(datetime(1970-01-01T00:00:01.500Z))",
        &[&["1970-01-01T00:00:01.500","1970-01-01T00:00:01.500","1970-01-01T00:00:01.500","1970-01-01T00:00:01.500","1.5"]],
    ).await;
}

#[tokio::test]
async fn datetime_and_timespan_constructors_validate_and_preserve_fractions() {
    check(
        "print d=make_datetime(2024,2,29,12,30,1.25),bad=make_datetime(2025,2,30),span=make_timespan(1,2,3,4.5),hm=make_timespan(1,30)",
        &[&["2024-02-29T12:30:01.250","<null>","PT93784.5S","PT5400S"]],
    ).await;
}

#[tokio::test]
async fn formats_compile_kql_tokens_without_chrono_precision_panics() {
    check(
        "print d=format_datetime(datetime(2025-01-02T03:04:05.1234567Z),\"yyyy-MM-dd'T'HH:mm:ss.fffffffK\"),t=format_timespan(timespan(1.02:03:04.1234567),'d.hh:mm:ss.fffffff'),z=format_datetime(datetime(2025-01-02),'HH:mm:ss.FFFFFFF'),quoted=format_datetime(datetime(2025-01-02),\"yyyy 'year' MM\")",
        &[&["2025-01-02T03:04:05.1234567Z","1.02:03:04.1234567","00:00:00","2025 year 01"]],
    ).await;
    assert!(
        SessionContext::new()
            .kql("print x=format_datetime(datetime(2025-01-01),'ffffffff')")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn calendar_functions_are_nullable_and_work_per_row() {
    check(
        "datatable(t:datetime,n:long)[datetime(2025-01-01),1,datetime(2025-01-02),-1,datetime(null),1] | project t=datetime_add('day',n,t)",
        &[&["2025-01-02T00:00:00"],&["2025-01-01T00:00:00"],&["<null>"]],
    ).await;
}

#[tokio::test]
async fn native_tabular_ranges_cover_datetime_duration_and_real_types() {
    check(
        "range t from datetime(2025-01-01) to datetime(2025-01-03) step 1d | count",
        &[&["3"]],
    )
    .await;
    check(
        "range t from datetime(2025-01-03) to datetime(2025-01-01) step -1d | count",
        &[&["3"]],
    )
    .await;
    check(
        "range d from 1h to 3h step 1h",
        &[&["PT3600S"], &["PT7200S"], &["PT10800S"]],
    )
    .await;
    check(
        "range x from 0.0 to 0.75 step 0.25",
        &[&["0.0"], &["0.25"], &["0.5"], &["0.75"]],
    )
    .await;
}

#[tokio::test]
async fn scalar_range_handles_real_and_temporal_bounds_with_budgets() {
    check(
        "print a=tostring(range(1.0,3.0,1.0)),d=range(datetime(2025-01-01),datetime(2025-01-03),1d) | project a,n=array_length(d),first=todatetime(d[0])",
        &[&["[1.0,2.0,3.0]","3","2025-01-01T00:00:00"]],
    ).await;
}

#[tokio::test]
async fn tiny_limits_on_enormous_time_ranges_remain_lazy() {
    check(
        "range t from datetime(1970-01-01) to datetime(2200-01-01) step 1ns | take 10 | count",
        &[&["10"]],
    )
    .await;
}

#[tokio::test]
async fn temporal_dynamic_conversion_and_subtick_ranges_do_not_lose_precision() {
    check(
        "print a=totimespan(dynamic(1h)),b=totimespan(dynamic('01:30:00')),c=totimespan(dynamic(0.5)),r=range(0ns,2ns,1ns) | project a,b,c,n=array_length(r),value=totimespan(r[1])",
        &[&["PT3600S","PT5400S","PT43200S","3","PT0.000000001S"]],
    ).await;
}
