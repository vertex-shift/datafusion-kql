use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

async fn result(ctx: &SessionContext, query: &str) -> Vec<Vec<String>> {
    let frame = ctx
        .kql(query)
        .await
        .unwrap_or_else(|error| panic!("{}\n{query}", error.diagnostic(query)));
    frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{error}\n{query}"))
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.logical_nulls().is_some_and(|n| n.is_null(row)) {
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
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
    let expected = expected
        .iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(result(&ctx, query).await, expected, "{query}");
}

#[tokio::test]
async fn scan_cumulative_sum_uses_current_step_state() {
    check("range x from 1 to 5 step 1 | scan declare (total:long=0) with (step s: true => total=x+s.total;)",
        &[&["1","1"],&["2","3"],&["3","6"],&["4","10"],&["5","15"]]).await;
}

#[tokio::test]
async fn nonnullable_long_cumulative_scan_uses_native_windows() {
    let ctx = SessionContext::new();
    let frame = ctx
        .kql("range x from 1 to 100 step 1 | scan declare(n:long=0) with(step s:true=>n=s.n+x;)")
        .await
        .unwrap();
    let plan = frame.create_physical_plan().await.unwrap();
    let text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string();
    assert!(text.contains("WindowAggExec"), "{text}");
    assert!(!text.contains("KqlScanExec"), "{text}");
    check("datatable(x:long)[1,long(null),2] | scan declare(n:long=0) with(step s:true=>n=s.n+x;) | project n",
        &[&["1"],&["<null>"],&["<null>"]]).await;
}

#[tokio::test]
async fn scan_conditional_cumulative_resets_are_independent() {
    check("range x from 1 to 5 step 1 | extend y=2*x | scan declare (sx:long=0,sy:long=0) with (step s: true => sx=iff(s.sx>=10,x,x+s.sx),sy=iff(s.sy>=10,y,y+s.sy);) | project sx,sy",
        &[&["1","2"],&["3","6"],&["6","12"],&["10","8"],&["5","18"]]).await;
}

#[tokio::test]
async fn scan_fill_forward_keeps_last_nonempty_string() {
    check("datatable(x:long,event:string)[1,'A',2,'',3,'B',4,'',5,''] | sort by x asc | scan declare (filled:string='') with (step s: true => filled=iff(isempty(event),s.filled,event);) | project filled",
        &[&["A"],&["A"],&["B"],&["B"],&["B"]]).await;
}

#[tokio::test]
async fn scan_sessions_promote_and_clear_previous_state() {
    check("datatable(t:timespan,event:string)[0m,'A',1m,'A',2m,'B',3m,'D',32m,'B',36m,'C',38m,'D',41m,'E',75m,'A'] | sort by t asc | scan with_match_id=id declare (start:timespan) with (step active: true => start=iff(isnull(active.start),t,active.start); step ended output=none: t-active.start>30m;) | project t,start,id",
        &[&["P0D","P0D","0"],&["PT60S","P0D","0"],&["PT120S","P0D","0"],&["PT180S","P0D","0"],&["PT1920S","PT1920S","1"],&["PT2160S","PT1920S","1"],&["PT2280S","PT1920S","1"],&["PT2460S","PT1920S","1"],&["PT4500S","PT4500S","2"]]).await;
}

#[tokio::test]
async fn scan_matches_the_documented_start_middle_stop_sequence() {
    check("datatable(t:timespan,event:string)[0m,'A',1m,'Start',2m,'B',3m,'D',4m,'Stop',6m,'C',8m,'Start',11m,'E',12m,'Stop'] | sort by t asc | scan with_match_id=id with (step first: event=='Start'; step middle: event!='Start' and event!='Stop' and t-first.t<=5m; step last: event=='Stop' and t-first.t<=5m;) | project event,id",
        &[&["Start","0"],&["B","0"],&["D","0"],&["Stop","0"],&["Start","1"],&["E","1"],&["Stop","1"]]).await;
}

#[tokio::test]
async fn scan_assignments_are_simultaneous_and_unqualified_names_are_current_row_values() {
    check("range x from 1 to 3 step 1 | scan declare (a:long=0,b:long=5) with (step s:true => a=s.b,b=s.a;) | project a,b",
        &[&["5","0"],&["0","5"],&["5","0"]]).await;
    check("range x from 1 to 3 step 1 | scan declare (a:long=0,b:long=5) with (step s:true => a=1,b=a;) | project a,b",
        &[&["1","0"],&["1","0"],&["1","0"]]).await;
}

#[tokio::test]
async fn scan_last_emits_final_rows_on_promotion_and_at_end() {
    check(
        "range x from 1 to 4 step 1 | scan with (step s output=last:true;)",
        &[&["4"]],
    )
    .await;
    check("range x from 1 to 5 step 1 | scan with_match_id=id with (step a output=last:x<3; step b:x>=3;) | project x,id",
        &[&["2","0"],&["3","0"],&["4","0"],&["5","0"]]).await;
    check(
        "range x from 1 to 5 step 1 | scan with (step a output=none:x<3; step b output=last:x>=3;)",
        &[&["5"]],
    )
    .await;
}

#[tokio::test]
async fn scan_last_outputs_retain_original_order_for_later_windows() {
    check("range x from 1 to 4 step 1 | scan with (step a output=last:x%2==1; step b:x%2==0;) | extend p=prev(x,1,0) | project x,p",
        &[&["1","0"],&["2","1"],&["3","2"],&["4","3"]]).await;
}

#[tokio::test]
async fn scan_can_match_multiple_steps_on_the_same_input_record() {
    check("range x from 1 to 3 step 1 | scan with_match_id=id with (step a:true; step b:true;) | project x,id",
        &[&["1","0"],&["2","0"],&["2","1"],&["3","1"],&["3","2"]]).await;
}

#[tokio::test]
async fn scan_unmatched_and_null_predicates_do_not_produce_rows() {
    check(
        "range x from 1 to 4 step 1 | scan with (step a:x%2==0;)",
        &[&["2"], &["4"]],
    )
    .await;
    check(
        "range x from 1 to 4 step 1 | scan with (step a:bool(null);) | count",
        &[&["0"]],
    )
    .await;
    check(
        "datatable(x:long)[] | scan with (step a:true;) | count",
        &[&["0"]],
    )
    .await;
}

#[tokio::test]
async fn scan_typed_defaults_and_temporal_assignments_are_retained() {
    check("print x=1 | scan declare (s:string,n:long,t:datetime,d:timespan,b:bool) with (step a:true;) | project s,n,t,d,b",
        &[&["","<null>","<null>","<null>","<null>"]]).await;
    check("datatable(t:datetime,event:string)[datetime(2025-01-01),'start',datetime(2025-01-02),'stop'] | sort by t asc | scan declare (elapsed:timespan) with (step a output=none:event=='start';step b:event=='stop' => elapsed=t-a.t;) | project elapsed", &[&["PT86400S"]]).await;
}

#[tokio::test]
async fn scan_original_columns_and_dynamic_state_can_be_updated() {
    check(
        "range x from 1 to 3 step 1 | scan with (step a:true => x=x*2;)",
        &[&["2"], &["4"], &["6"]],
    )
    .await;
    check("range x from 1 to 2 step 1 | scan declare (d:dynamic=dynamic([])) with (step a:true => d=array_concat(a.d,pack_array(x));) | project d=tostring(d)",
        &[&["[1]"],&["[1,2]"]]).await;
}

#[tokio::test]
async fn partition_scan_state_is_isolated_per_key() {
    check("datatable(k:string,x:long)['a',2,'b',4,'a',1,'b',3] | partition hint.strategy=native by k (sort by x asc | scan declare (n:long=0) with (step s:true => n=s.n+x;)) | sort by k asc,x asc",
        &[&["a","1","1"],&["a","2","3"],&["b","3","3"],&["b","4","7"]]).await;
    check("print a=dynamic([1,2,3]) | mv-apply a to typeof(long) on (scan declare (n:long=0) with (step s:true => n=s.n+a;)) | project n",
        &[&["1"],&["3"],&["6"]]).await;
}

#[tokio::test]
async fn scan_crosses_batches_and_reexecutions_start_with_fresh_state() {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
    let make = |values: Vec<i64>| {
        RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from(values)) as _)]).unwrap()
    };
    let a = make(vec![1, 2, 3]);
    let b = make(vec![4, 5]);
    ctx.register_table(
        TableReference::bare("T"),
        Arc::new(MemTable::try_new(a.schema(), vec![vec![a, b]]).unwrap()),
    )
    .unwrap();
    let frame = ctx
        .kql("T | scan declare(n:long=0) with (step s:true=>n=s.n+x;)")
        .await
        .unwrap();
    for _ in 0..2 {
        let batches = frame.clone().collect().await.unwrap();
        let last = batches.last().unwrap();
        assert_eq!(
            array_value_to_string(last.column(1), last.num_rows() - 1).unwrap(),
            "15"
        );
    }
}

#[tokio::test]
async fn scan_uses_lexical_functions_and_request_parameters() {
    check("declare query_parameters(start:long=10); let f=(x:long){2*x}; range x from 1 to 2 step 1 | scan declare(n:long=start) with(step s:true=>n=s.n+f(x);) | project n", &[&["12"],&["16"]]).await;
}

#[tokio::test]
async fn scan_memory_exhaustion_is_an_explicit_query_error() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build()
        .unwrap();
    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::new().with_batch_size(1),
        Arc::new(runtime),
    );
    let frame=ctx.kql("range x from 1 to 20 step 1 | scan declare(s:string='') with(step a:true=>s=strcat(a.s,'abcdefghijklmnopqrstuvwxyz');) | count").await.unwrap();
    let error = frame.collect().await.unwrap_err().to_string();
    assert!(error.contains("KQL scan state"), "{error}");
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn invalid_scan_definitions_fail_in_planning() {
    for query in [
        "print x=1 | scan with ()",
        "print x=1 | scan with (step a:true;step a:true;)",
        "print x=1 | scan declare(x:long=0) with(step a:true;)",
        "print x=1 | scan with_match_id=x with(step a:true;)",
        "print x=1 | scan with(step a:1;)",
        "print x=1 | scan with(step a:true=>missing=1;)",
        "print x=1 | scan with(step a:true=>x='wrong';)",
        "print x=1 | scan with(step a:true=>x=1,x=2;)",
        "print x=1 | scan with(step a:b.x>0;step b:true;)",
        "print x=1 | scan with(step a:a.missing>0;)",
        "print x=1 | scan with(step a output=invalid:true;)",
    ] {
        assert!(SessionContext::new().kql(query).await.is_err(), "{query}");
    }
}

#[tokio::test]
async fn stateful_scan_results_are_independent_of_output_batch_size() {
    let mut state = 0_i64;
    let mut peak = 0_i64;
    for value in 1..=20_000 {
        state = if state >= 10_000 {
            value
        } else {
            state + value
        };
        peak = peak.max(state);
    }
    for batch_size in [1, 7, 256, 8192] {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(batch_size));
        let query = "range x from 1 to 20000 step 1 | scan declare(n:long=0) with(step s:true=>n=iff(s.n>=10000,x,s.n+x);) | summarize Rows=count(),Peak=max(n),Total=sum(x)";
        assert_eq!(
            result(&ctx, query).await,
            vec![vec![
                "20000".to_owned(),
                peak.to_string(),
                "200010000".to_owned()
            ]]
        );
    }
}
