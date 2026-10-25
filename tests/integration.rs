use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::TableReference;
use datafusion::prelude::*;
use datafusion_kql::{SessionContextKqlExt, parse};

fn rows(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.is_null(row) {
                            "<null>".into()
                        } else {
                            array_value_to_string(column, row).unwrap()
                        }
                    })
                    .collect()
            })
        })
        .collect()
}

async fn check(query: &str, expected: &[&[&str]]) {
    let ctx = SessionContext::new();
    let frame = ctx
        .kql(query)
        .await
        .unwrap_or_else(|e| panic!("{}\n{query}", e.diagnostic(query)));
    let actual = rows(
        &frame
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{e}\n{query}")),
    );
    let expected = expected
        .iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{query}");
}

fn context() -> SessionContext {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("Value", DataType::Int64, false),
        Field::new("Category", DataType::Utf8, false),
        Field::new("Unused", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            Arc::new(StringArray::from(vec!["a", "b", "a", "b", "a"])),
            Arc::new(Int64Array::from(vec![0; 5])),
        ],
    )
    .unwrap();
    ctx.register_batch(TableReference::bare("Events"), batch)
        .unwrap();
    ctx
}

#[tokio::test]
async fn native_range_filter_extend_aggregate_and_top() {
    check(
        "range x from 1 to 10 step 1 | where x % 2 == 0 | extend doubled=x*2 | summarize Total=sum(doubled), N=count()",
        &[&["60", "5"]],
    ).await;
    check(
        "range x from 10 to 1 step -2 | top 2 by x asc",
        &[&["2"], &["4"]],
    )
    .await;
}

#[tokio::test]
async fn datatable_project_extend_and_column_order() {
    let ctx = SessionContext::new();
    let frame = ctx
        .kql("datatable(a:long,b:long)[1,10] | extend a=a*2, c=a+1")
        .await
        .unwrap();
    assert_eq!(
        frame
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        ["b", "a", "c"]
    );
    assert_eq!(
        rows(&frame.collect().await.unwrap()),
        vec![vec!["10", "2", "2"]]
    );
    check(
        "datatable(['a.b']:long, ['space name']:string)[3,'hello'] | project x=['a.b'], ['space name']",
        &[&["3", "hello"]],
    ).await;
}

#[tokio::test]
async fn projection_patterns_and_renames() {
    check(
        "print aa=1, ab=2, b=3 | project-away a* | project-rename c=b",
        &[&["3"]],
    )
    .await;
    check(
        "print aa=1, ab=2, b=3 | project-keep b,a* | project-rename z=ab",
        &[&["1", "2", "3"]],
    )
    .await;
}

#[tokio::test]
async fn scalar_tabular_bindings_and_reusable_ast() {
    let ctx = context();
    let ast = parse("let minimum=15; let t=Events; let selected=t | where Value>minimum; selected | summarize Total=sum(Value) by Category | sort by Category asc").unwrap();
    for _ in 0..2 {
        assert_eq!(
            rows(&ctx.kql_ast(&ast).await.unwrap().collect().await.unwrap()),
            vec![vec!["a", "80"], vec!["b", "60"]]
        );
    }
}

#[tokio::test]
async fn lexical_binding_scope_is_restored_after_subqueries() {
    check(
        "let n=10; (let n=20; print x=n) | union (print x=n) | sort by x asc",
        &[&["10"], &["20"]],
    )
    .await;
}

#[tokio::test]
async fn sort_defaults_and_null_ordering() {
    check(
        "datatable(x:long)[1,long(null),3] | sort by x",
        &[&["3"], &["1"], &["<null>"]],
    )
    .await;
    check(
        "datatable(x:long)[1,long(null),3] | sort by x asc",
        &[&["<null>"], &["1"], &["3"]],
    )
    .await;
    check(
        "datatable(x:long)[1,long(null),3] | sort by x desc nulls first",
        &[&["<null>"], &["3"], &["1"]],
    )
    .await;
}

#[tokio::test]
async fn null_equality_uses_kusto_semantics() {
    check(
        "datatable(x:long)[1,long(null),2] | project x, eq=x==1, ne=x!=1, eqnull=x==long(null), nenull=x!=long(null) | sort by x asc",
        &[&["<null>","false","true","<null>","<null>"], &["1","true","false","false","true"], &["2","false","true","false","true"]],
    ).await;
    check(
        "datatable(x:long)[1,long(null),2] | where x!=1 | sort by x asc",
        &[&["<null>"], &["2"]],
    )
    .await;
    check(
        "datatable(x:long)[1,long(null),2] | where not(x==1) | sort by x asc",
        &[&["<null>"], &["2"]],
    )
    .await;
}

#[tokio::test]
async fn nullable_column_comparisons_and_boolean_logic() {
    check(
        "datatable(a:long,b:long)[long(null),long(null),long(null),1,1,long(null),1,1,1,2] | project eq=a==b, ne=a!=b",
        &[&["<null>","<null>"], &["false","true"], &["false","true"], &["true","false"], &["false","true"]],
    ).await;
    check(
        "print a=bool(null) and false, b=bool(null) or true, c=not(bool(null))",
        &[&["false", "true", "<null>"]],
    )
    .await;
}

#[tokio::test]
async fn membership_preserves_kql_null_semantics() {
    check(
        "datatable(x:long)[long(null),1,2] | project x, a=x in (1), b=x !in (1), c=x in (1,long(null)), d=x !in (1,long(null)) | sort by x asc",
        &[&["<null>","false","true","<null>","<null>"], &["1","true","false","true","false"], &["2","false","true","false","true"]],
    ).await;
    check(
        "print a=2 in (), b=long(null) !in (), c=2 in (long(null))",
        &[&["false", "true", "false"]],
    )
    .await;
    check(
        "print a='ABC' in~ ('abc'), b='Ä' in~ ('ä')",
        &[&["true", "false"]],
    )
    .await;
}

#[tokio::test]
async fn contains_is_literal_and_has_respects_terms() {
    check(
        "datatable(s:string)['North America','northamerica','America north','x%_y'] | project s, a=s contains 'AMERICA', b=s has 'America', c=s has_cs 'America', d=s contains_cs '%_'",
        &[&["North America","true","true","true","false"], &["northamerica","true","false","false","false"], &["America north","true","true","true","false"], &["x%_y","false","false","false","true"]],
    ).await;
    check(
        "print a='Hello' startswith 'he', b='Hello' endswith_cs 'LO', c='x42' matches regex @'\\d+', d='a.b' contains_cs '.'",
        &[&["true","false","true","true"]],
    ).await;
}

#[tokio::test]
async fn string_scalar_functions_and_kql_coalesce() {
    check(
        "print a=strcat('a','b'), b=tolower('ABC'), c=toupper('abc'), d=strlen('hé😊'), e=substring('hello',1,3), f=substring('hello',-2,2), g=coalesce('','fallback'), h=tostring(long(null)), i=coalesce('',''), j=isempty('')",
        &[&["ab","abc","ABC","3","ell","he","fallback","","","true"]],
    ).await;
}

#[tokio::test]
async fn nullable_arrow_strings_are_normalized_without_mutating_sql() {
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_from_iter(vec![(
        "s",
        Arc::new(StringArray::from(vec![Some("a"), None])) as _,
    )])
    .unwrap();
    ctx.register_batch(TableReference::bare("T"), batch)
        .unwrap();
    let kql = ctx
        .kql("T | project s, n=isnull(s), empty=isempty(s)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        rows(&kql),
        vec![vec!["a", "false", "false"], vec!["", "false", "true"]]
    );
    let sql = ctx
        .sql("SELECT s FROM \"T\"")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(sql[0].column(0).is_null(1));
}

#[tokio::test]
async fn aggregation_defaults_names_and_conditional_functions() {
    let ctx = SessionContext::new();
    let frame = ctx.kql("datatable(x:long)[] | summarize count(), sum(x), avg(x), dcount(x), countif(x>0), stdev(x), variance(x)").await.unwrap();
    assert_eq!(
        frame
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        [
            "count_",
            "sum_x",
            "avg_x",
            "dcount_x",
            "countif_",
            "stdev_x",
            "variance_x"
        ]
    );
    assert_eq!(
        rows(&frame.collect().await.unwrap()),
        vec![vec!["0", "0", "NaN", "0", "0", "0.0", "0.0"]]
    );
    check(
        "datatable(x:long)[1,2,2,long(null)] | summarize sumif(x,x>1), avgif(x,x>1), minif(x,x>1), maxif(x,x>1), count_distinct(x), count(x)",
        &[&["4", "2.0", "2", "2", "2", "4"]],
    ).await;
}

#[tokio::test]
async fn grouping_aliases_and_group_only_summarize() {
    check(
        "datatable(x:long)[1,2,2,3] | summarize N=count() by Bucket=bin(x,2) | sort by Bucket asc",
        &[&["0", "1"], &["2", "3"]],
    )
    .await;
    check(
        "datatable(x:long)[1,2,2] | summarize by x | sort by x asc",
        &[&["1"], &["2"]],
    )
    .await;
    check("datatable(x:long)[] | summarize N=count() by x", &[]).await;
}

#[tokio::test]
async fn bin_is_floor_based_and_keeps_large_integer_precision() {
    check(
        "print a=bin(-1,10), b=bin(11,10), c=bin(-9007199254740993,10), d=bin(1,0), e=bin(1,-1), f=bin(-1.5,1.0)",
        &[&["-10","10","-9007199254741000","<null>","<null>","-2.0"]],
    ).await;
}

#[tokio::test]
async fn datetime_timespan_and_time_binning() {
    check(
        "print t=bin(datetime(2025-01-02T03:04:05Z),1h), t2=datetime(1969-12-31T23:59:59Z)+1s, d=timespan(1.02:03:04.1234567)",
        &[&["2025-01-02T03:00:00", "1970-01-01T00:00:00", "PT93784.1234567S"]],
    ).await;
    check(
        "print a=datetime(2025-01-02T03:00:00Z) between (datetime(2025-01-02)..1d)",
        &[&["true"]],
    )
    .await;
}

#[tokio::test]
async fn now_and_ago_are_stable_across_a_query() {
    check(
        "print a=now()==now(), b=ago(1h)==now(-1h)",
        &[&["true", "true"]],
    )
    .await;
}

#[tokio::test]
async fn conversion_errors_are_typed_null_and_integer_division_by_zero_is_null() {
    check(
        "print a=tolong('no'), b=toint('42'), c=todouble('2.5'), d=1/0, e=1%0, f=5/2, g=iff(bool(null),1,2), h=case(false,1,true,2,3)",
        &[&["<null>","42","2.5","<null>","<null>","2","2","2"]],
    ).await;
}

#[tokio::test]
async fn default_join_is_innerunique_and_explicit_inner_preserves_duplicates() {
    let prefix =
        "let L=datatable(k:long,v:string)[1,'a',1,'b']; let R=datatable(k:long,w:long)[1,10,1,20];";
    check(&format!("{prefix} L | join R on k | count"), &[&["2"]]).await;
    check(
        &format!("{prefix} L | join kind=inner (R) on k | count"),
        &[&["4"]],
    )
    .await;
    check(
        "let L=datatable(k:long,v:string)[1,'a']; let R=datatable(k:long,w:long)[1,10]; L | join kind=inner R on k | project k,v,k1,w",
        &[&["1","a","1","10"]],
    ).await;
}

#[tokio::test]
async fn join_flavors_null_keys_and_different_key_names() {
    let prefix = "let L=datatable(k:long,v:string)[1,'a',2,'b',long(null),'c']; let R=datatable(id:long,w:string)[1,'yes',3,'no'];";
    check(
        &format!("{prefix} L | join kind=leftsemi R on $left.k==$right.id"),
        &[&["1", "a"]],
    )
    .await;
    check(
        &format!("{prefix} L | join kind=leftanti R on $left.k==$right.id | sort by k asc"),
        &[&["<null>", "c"], &["2", "b"]],
    )
    .await;
    check(
        &format!(
            "{prefix} L | join kind=leftouter R on $left.k==$right.id | project k,w | sort by k asc"
        ),
        &[&["<null>", ""], &["1", "yes"], &["2", ""]],
    )
    .await;
}

#[tokio::test]
async fn union_aligns_by_name_fills_missing_columns_and_preserves_duplicates() {
    check(
        "let A=datatable(x:long,s:string)[1,'a']; let B=datatable(y:long,x:long)[3,2]; union A,B,A | project x,s,y | sort by x asc",
        &[&["1","a","<null>"], &["1","a","<null>"], &["2","","3"]],
    ).await;
    check(
        "let A=datatable(x:long,s:string)[1,'a']; let B=datatable(y:long,x:long)[3,2]; A | union kind=inner B | sort by x asc",
        &[&["1"],&["2"]],
    ).await;
}

#[tokio::test]
async fn distinct_and_count_are_native() {
    check(
        "datatable(x:long,y:string)[1,'a',1,'a',2,'b'] | distinct * | sort by x asc",
        &[&["1", "a"], &["2", "b"]],
    )
    .await;
    check("datatable(x:long)[] | count", &[&["0"]]).await;
    check("range x from 1 to 100 step 1 | limit 0 | count", &[&["0"]]).await;
}

#[tokio::test]
async fn equivalent_sql_has_the_same_results() {
    let ctx = context();
    let kql = ctx.kql("Events | where Value>15 | summarize N=count(), Total=sum(Value) by Category | order by Total desc | take 2").await.unwrap().collect().await.unwrap();
    let sql = ctx.sql("SELECT \"Category\", count(*) AS \"N\", sum(\"Value\") AS \"Total\" FROM \"Events\" WHERE \"Value\">15 GROUP BY \"Category\" ORDER BY \"Total\" DESC LIMIT 2").await.unwrap().collect().await.unwrap();
    assert_eq!(rows(&kql), rows(&sql));
}

#[tokio::test]
async fn optimizer_pushes_projection_to_the_native_scan() {
    let ctx = context();
    let plan = ctx
        .kql("Events | where Value>15 | project Value | take 2")
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    let display = plan.display_indent().to_string();
    assert!(display.contains("projection=[Value]"), "{display}");
    assert!(!display.contains("Extension:"), "{display}");
    assert!(display.contains("Filter:"), "{display}");
}

#[tokio::test]
async fn caller_function_registry_is_preserved() {
    let ctx = SessionContext::new();
    let custom = datafusion::functions::math::abs()
        .as_ref()
        .clone()
        .with_aliases(["my_abs"]);
    ctx.register_udf(custom);
    let result = ctx
        .kql("print x=my_abs(-7)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(rows(&result), vec![vec!["7"]]);
}

#[tokio::test]
async fn arrow_timestamps_are_usable_without_copying_input() {
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_from_iter(vec![(
        "t",
        Arc::new(TimestampNanosecondArray::from(vec![
            0_i64,
            3_600_000_000_001,
        ])) as _,
    )])
    .unwrap();
    ctx.register_batch(TableReference::bare("T"), batch)
        .unwrap();
    let result = ctx
        .kql("T | summarize N=count() by bin(t,1h) | order by t asc")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        rows(&result),
        vec![
            vec!["1970-01-01T00:00:00", "1"],
            vec!["1970-01-01T01:00:00", "1"]
        ]
    );
}

#[tokio::test]
async fn unsupported_features_and_invalid_names_do_not_silently_fall_back() {
    let ctx = context();
    for query in [
        "Events | project value",
        "events | take 1",
        "Events | summarize unknown_aggregate(Value)",
        "Events | summarize dcount(Value,5)",
        "Events | summarize x=Value",
        "Events | where Category has Value",
        "let x=1; let x=2; print y=x",
        "print x=1 | extend a=x+1,b=a+1",
        "datatable(x:long)[1] | union isfuzzy=true missing",
        "Events | project x=Value,x=Category",
        "Events | project-rename X=missing",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}

#[tokio::test]
async fn expanded_let_expressions_are_bounded() {
    let ctx = SessionContext::new();
    let mut query = String::from("let x0=1;");
    for index in 1..20 {
        query.push_str(&format!("let x{index}=x{}+x{};", index - 1, index - 1));
    }
    query.push_str("print x=x19");
    assert!(
        ctx.kql(&query)
            .await
            .unwrap_err()
            .to_string()
            .contains("planning limits")
    );
}

#[tokio::test]
async fn repeated_aggregates_share_one_native_accumulator() {
    use datafusion::logical_expr::LogicalPlan;

    let ctx = SessionContext::new();
    let frame = ctx
        .kql("range x from 1 to 4 step 1 | summarize A=sum(x), B=sum(x), count(), count()")
        .await
        .unwrap();
    let LogicalPlan::Projection(projection) = frame.logical_plan() else {
        panic!("expected final naming projection");
    };
    let LogicalPlan::Aggregate(aggregate) = projection.input.as_ref() else {
        panic!("expected native aggregation");
    };
    assert_eq!(aggregate.aggr_expr.len(), 2);
    assert_eq!(
        rows(&frame.collect().await.unwrap()),
        vec![vec!["10", "10", "4", "4"]]
    );
}

#[tokio::test]
async fn datetime_differences_and_timespan_arithmetic_are_native_and_typed() {
    check(
            "print d=datetime(2025-01-02)-datetime(2025-01-01), a=1h+30m, b=2h/1h, c=2*30m, n=-timespan(01:00:00), z=1h/0",
            &[&["PT86400S","PT5400S","2.0","PT3600S","-PT3600S","<null>"]],
        ).await;
    check(
            "print a=bin(datetime(2025-01-01),timespan(null)), b=bin(1h,timespan(null)), c=now(timespan(null)), d=ago(timespan(null))",
            &[&["<null>","<null>","<null>","<null>"]],
        ).await;
}

#[tokio::test]
async fn arrow_time_units_are_normalized_before_binning() {
    use datafusion::arrow::array::{DurationMillisecondArray, TimestampMillisecondArray};
    use datafusion::arrow::datatypes::TimeUnit;

    let ctx = SessionContext::new();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "d",
            Arc::new(DurationMillisecondArray::from(vec![1500_i64])) as _,
        ),
        (
            "t",
            Arc::new(TimestampMillisecondArray::from(vec![1_i64])) as _,
        ),
    ])
    .unwrap();
    ctx.register_batch(TableReference::bare("T"), batch)
        .unwrap();
    let frame = ctx
        .kql("T | project d=bin(d,1s), t=bin(t,700us)")
        .await
        .unwrap();
    assert_eq!(
        frame.schema().field(0).data_type(),
        &DataType::Duration(TimeUnit::Nanosecond)
    );
    assert_eq!(
        frame.schema().field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    assert_eq!(
        rows(&frame.collect().await.unwrap()),
        vec![vec!["PT1S", "1970-01-01T00:00:00.000700"]]
    );
}

#[tokio::test]
async fn every_join_flavor_uses_the_expected_row_and_schema_shape() {
    let prefix = "let L=datatable(k:long,v:string)[1,'a',2,'b']; let R=datatable(k:long,w:string)[1,'yes',3,'no'];";
    for (kind, count, columns) in [
        ("innerunique", 1, 4),
        ("inner", 1, 4),
        ("leftouter", 2, 4),
        ("rightouter", 2, 4),
        ("fullouter", 3, 4),
        ("leftsemi", 1, 2),
        ("rightsemi", 1, 2),
        ("leftanti", 1, 2),
        ("rightanti", 1, 2),
    ] {
        let query = format!("{prefix} L | join kind={kind} R on k");
        let ctx = SessionContext::new();
        let frame = ctx.kql(&query).await.unwrap();
        assert_eq!(frame.schema().fields().len(), columns, "{query}");
        let result = frame.collect().await.unwrap();
        assert_eq!(
            result.iter().map(RecordBatch::num_rows).sum::<usize>(),
            count,
            "{query}"
        );
    }
}

#[tokio::test]
async fn join_collision_suffixes_do_not_overwrite_real_columns() {
    check(
            "let L=datatable(k:long,k1:long)[1,10]; let R=datatable(k:long,k2:long)[1,20]; L | join kind=inner R on k | project k,k1,k3,k2",
            &[&["1","10","1","20"]],
        ).await;
}

#[tokio::test]
async fn small_limit_on_a_trillion_row_range_is_lazy() {
    check(
        "range x from 1 to 1000000000000 step 1 | take 10 | count",
        &[&["10"]],
    )
    .await;
}

#[tokio::test]
async fn integer_extremes_and_timespan_precision_do_not_round_through_float() {
    check(
            "print a=-9223372036854775808, b=9223372036854775807, c=timespan(00:00:00.0000001), d=0.0000001s",
            &[&["-9223372036854775808","9223372036854775807","PT0.0000001S","PT0.0000001S"]],
        ).await;
}

#[tokio::test]
async fn invalid_constants_return_errors_without_wrapping_or_materialization() {
    let ctx = SessionContext::new();
    for query in [
        "print d=datetime(9999-01-01)",
        "print t=999999999999999999999d",
        "print x=ago(1)",
        "range x from 1 to 10 step 0",
        "print s='a b' has 'a b'",
        "print a=bin(datetime(2025-01-01),1)",
        "print x=+'not numeric'",
        "print x=+true",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}

#[tokio::test]
async fn tabular_binding_expansion_is_bounded() {
    let mut query = String::from("let t0=print x=1;");
    for index in 1..20 {
        query.push_str(&format!(
            "let t{index}=t{} | union t{};",
            index - 1,
            index - 1
        ));
    }
    query.push_str("t19");
    assert!(
        SessionContext::new()
            .kql(&query)
            .await
            .unwrap_err()
            .to_string()
            .contains("planning limits")
    );
}

#[tokio::test]
async fn arrow_string_representations_have_the_same_non_null_kql_semantics() {
    use datafusion::arrow::array::{LargeStringArray, StringDictionaryBuilder, StringViewArray};
    use datafusion::arrow::datatypes::Int32Type;

    let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
    dictionary.append("a").unwrap();
    dictionary.append_null();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "large",
            Arc::new(LargeStringArray::from(vec![Some("a"), None])) as _,
        ),
        (
            "view",
            Arc::new(StringViewArray::from(vec![Some("a"), None])) as _,
        ),
        ("dict", Arc::new(dictionary.finish()) as _),
    ])
    .unwrap();
    let ctx = SessionContext::new();
    ctx.register_batch(TableReference::bare("T"), batch)
        .unwrap();
    let frame = ctx
        .kql("T | project a=isempty(large), b=isempty(view), c=isempty(dict), d=isnull(dict)")
        .await
        .unwrap();
    assert_eq!(
        rows(&frame.collect().await.unwrap()),
        vec![
            vec!["false", "false", "false", "false"],
            vec!["true", "true", "true", "false"],
        ]
    );
}

#[tokio::test]
async fn kql_scalar_types_have_explicit_arrow_output_types() {
    use datafusion::arrow::datatypes::TimeUnit;

    let ctx = SessionContext::new();
    let frame = ctx.kql(
        "print length=strlen('abc'), i=int(null), l=long(null), r=real(null), b=bool(null), s=tostring(long(null)), t=datetime(null), d=timespan(null)",
    ).await.unwrap();
    let types = frame
        .schema()
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        types,
        vec![
            DataType::Int64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float64,
            DataType::Boolean,
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Duration(TimeUnit::Nanosecond),
        ]
    );
    let frame = ctx
        .kql("datatable(x:long)[] | summarize count(), dcount(x), count_distinct(x)")
        .await
        .unwrap();
    assert!(
        frame
            .schema()
            .fields()
            .iter()
            .all(|f| f.data_type() == &DataType::Int64)
    );
}
