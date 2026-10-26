use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::TableReference;
use datafusion::prelude::*;
use datafusion_kql::{SessionContextKqlExt, parse};

async fn check(ctx: &SessionContext, query: &str, expected: &[&[&str]]) {
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
        .map(|row| row.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{query}");
}

#[tokio::test]
async fn hexadecimal_literals_are_exact_longs_in_every_scalar_context() {
    let ctx = SessionContext::new();
    check(&ctx,"print a=0x2a,b=long(0X2A),c=int(0xff),d=0X1E3,e=0x7fffffffffffffff,f=-0x8000000000000000,g=long(-0X8000000000000000),h=gettype(0xff)",
        &[&["42","42","255","483","9223372036854775807","-9223372036854775808","-9223372036854775808","long"]]).await;
    check(&ctx,"let threshold=0x20; datatable(x:long)[0x10,0x20,0x30] | where x>=threshold | summarize total=sum(x)",
        &[&["80"]]).await;
    check(
        &ctx,
        "range x from 0x1 to 0x3 step 0x1 | project x,member=x in(0x1,0x2)",
        &[&["1", "true"], &["2", "true"], &["3", "false"]],
    )
    .await;
    check(
        &ctx,
        "print d=dynamic([0x1,0X2]) | mv-expand d to typeof(long)",
        &[&["1"], &["2"]],
    )
    .await;
}

#[tokio::test]
async fn spelled_duration_units_use_the_same_checked_integer_scaling() {
    let ctx = SessionContext::new();
    check(&ctx,"print d=1day==1d,days=2days==2d,h=2hours==2h,m=3minutes==3m,s=4seconds==4s,ms=5milli==5ms,us=6micro==6us,t=1ticks==100ns",
        &[&["true","true","true","true","true","true","true","true"]]).await;
    check(&ctx,"print a=timespan(15 seconds)==15s,b=time(2 hours)==2h,c=timespan(1.25 days)==30h,d=timespan(1e2 milliseconds)==100ms,e=timespan(-0.001 microseconds)==-1ns",
        &[&["true","true","true","true","true"]]).await;
    check(&ctx,"datatable(s:string)['1 day','2 hours','-3 minutes','4 seconds','5 milliseconds','6 microseconds'] | project d=tolong(totimespan(s))",
        &[&["86400000000000"],&["7200000000000"],&["-180000000000"],&["4000000000"],&["5000000"],&["6000"]]).await;
}

#[tokio::test]
async fn uppercase_typed_booleans_do_not_make_all_identifiers_case_insensitive() {
    let ctx = SessionContext::new();
    check(
        &ctx,
        "print a=bool(TRUE),b=boolean(False),c=bool(TrUe),d=bool(false),e=bool(null)",
        &[&["true", "false", "true", "false", "<null>"]],
    )
    .await;
    check(
        &ctx,
        "let f=(x:bool){x}; print a=f(bool(TRUE)),b=tobool(dynamic([bool(FALSE)])[0])",
        &[&["true", "false"]],
    )
    .await;
    assert!(ctx.kql("print x=TRUE").await.is_err());
    check(
        &ctx,
        "datatable(['TRUE']:bool)[false] | project value=bool(['TRUE'])",
        &[&["false"]],
    )
    .await;
}

#[tokio::test]
async fn rfc_date_literals_and_conversions_normalize_offsets_to_utc() {
    check(&SessionContext::new(),"print a=datetime(Wed, 02 Oct 2002 08:00:00 +0200)==datetime(2002-10-02T06:00:00Z),b=datetime(Sunday, 06-Nov-1994 08:49:37 +0000)==datetime(1994-11-06T08:49:37Z),c=todatetime('Wed, 02 Oct 2002 08:00:00 +0200')==datetime(2002-10-02T06:00:00Z)",
        &[&["true","true","true"]]).await;
}

#[tokio::test]
async fn malformed_or_out_of_range_literals_never_wrap_or_panic() {
    let ctx = SessionContext::new();
    for query in [
        "print x=0x",
        "print x=0xg",
        "print x=0x1g",
        "print x=0x10000000000000000",
        "print x=0x8000000000000000",
        "print x=-0x8000000000000001",
        "print x=1week",
        "print x=timespan(1 week)",
        "print x=timespan(1e38 days)",
        "print x=datetime(Wed, 02 Oct 9999 08:00:00 +0200)",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}

fn tables() -> SessionContext {
    let ctx = SessionContext::new();
    for name in ["Events", "Events.v1"] {
        ctx.register_batch(
            TableReference::bare(name),
            RecordBatch::try_from_iter(vec![(
                "id",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as _,
            )])
            .unwrap(),
        )
        .unwrap();
    }
    ctx
}

#[tokio::test]
async fn table_database_cluster_wrappers_resolve_only_local_registered_providers() {
    let ctx = tables();
    for source in [
        "table('Events')",
        "database('public').Events",
        "database('public').table('Events')",
        "cluster('datafusion').database('public').Events",
        "cluster('datafusion').database('public').table('Events')",
        "table('Events.v1')",
    ] {
        check(&ctx, &format!("{source} | count"), &[&["3"]]).await;
    }
    check(
        &ctx,
        "let T=table('Events'); T | summarize N=count()",
        &[&["3"]],
    )
    .await;
    check(&ctx, "print N=toscalar(table('Events') | count)", &[&["3"]]).await;
    check(
        &ctx,
        "table('Events') | where id in(database('public').table('Events') | project id) | count",
        &[&["3"]],
    )
    .await;
}

#[tokio::test]
async fn wrapper_sources_keep_join_union_and_source_label_behavior() {
    let ctx = tables();
    check(
        &ctx,
        "table('Events') | join kind=inner (database('public').table('Events')) on id | count",
        &[&["3"]],
    )
    .await;
    check(&ctx,"union withsource=source table('Events'),database('public').table('Events') | summarize N=count() by source | sort by source asc",
        &[&["Events","3"],&["public.Events","3"]]).await;
    let wrapped =
        parse("cluster('datafusion').database('public').table('Events') | project id").unwrap();
    let regular = parse("datafusion.public.Events | project id").unwrap();
    let a = ctx
        .kql_ast(&wrapped)
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    let b = ctx
        .kql_ast(&regular)
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    assert_eq!(a, b, "source spellings must not add execution machinery");
}

#[tokio::test]
async fn wrapper_names_require_literals_and_do_not_fall_back_to_other_catalogs() {
    let ctx = tables();
    for query in [
        "table()",
        "table('')",
        "table(1)",
        "table('Events','hotcache')",
        "let name='Events'; table(name)",
        "cluster('datafusion').Events",
        "cluster('missing').database('public').table('Events')",
        "database('missing').table('Events')",
        "table('missing')",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}
