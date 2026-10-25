use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion::arrow::array::{Int64Array, ListArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::*;
use datafusion_kql::{SessionContextKqlExt, parse};
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

const QUERY: &str =
    "Events | where Value > 42 | summarize Total=sum(Value) by Category | top 10 by Total";
const SQL: &str = "SELECT \"Category\", coalesce(sum(\"Value\"), 0) AS \"Total\" FROM \"Events\" WHERE \"Value\" > 42 GROUP BY \"Category\" ORDER BY \"Total\" DESC NULLS LAST LIMIT 10";
const ROWS: usize = 1_000_000;
const JSON_ROWS: usize = 100_000;
const JSON_QUERY: &str = "JsonEvents | extend Payload=parse_json(Raw) | project Service=tostring(Payload.service), Latency=Payload.latencies | mv-expand Latency to typeof(long) | summarize Total=sum(Latency) by Service";
const SERIES_QUERY: &str = "Events | make-series Total=sum(Value) default=0 on Value from 0 to 100 step 1 by Category | project Category, Smoothed=series_fir(Total, dynamic([1,2,1]))";
const PARTITION_QUERY: &str =
    "Events | partition hint.strategy=native by Category (top 3 by Value) | summarize N=count()";
const SCAN_QUERY: &str = "Events | scan declare(Total:long=0) with(step s:true=>Total=s.Total+Value;) | summarize Peak=max(Total)";
const STATE_SCAN_QUERY: &str = "Events | take 10000 | scan declare(Total:long=0) with(step s:true=>Total=iff(s.Total>10000,Value,s.Total+Value);) | summarize Peak=max(Total)";
const APPLY_QUERY: &str = "JsonEvents | take 1000 | extend P=parse_json(Raw) | mv-apply v=P.latencies to typeof(long) on (top 2 by v | summarize Total=sum(v)) | summarize Total=sum(Total)";
const UNPACK_QUERY: &str = "JsonEvents | extend P=parse_json(Raw) | evaluate bag_unpack(P):(service:string,latencies:dynamic) | summarize Measurements=sum(array_length(latencies)) by service";
const PIVOT_QUERY: &str = "Events | evaluate pivot(Category):(Value:long,api:long,worker:long,frontend:long,scheduler:long) | summarize Total=sum(api)+sum(worker)+sum(frontend)+sum(scheduler)";
const FIELDS_QUERY: &str = "JsonEvents | extend P=parse_json(Raw) | project service=tostring(P.service),latencies=P.latencies | summarize Measurements=sum(array_length(latencies)) by service";
const SEARCH_QUERY: &str = "Events | search Category:'api' and Value>42 | summarize N=count()";
const HITTERS_QUERY: &str = "Events | top-hitters 2 of Category by Value";
const GUID_QUERY: &str =
    "GuidEvents | extend id=toguid(Text) | summarize Distinct=count_distinct(id)";
const DECIMAL_QUERY: &str = "Telemetry | summarize Total=sum(todecimal(Amount)+decimal(0.25)) | project Total=tostring(Total)";
const RICH_QUERY: &str = "Events | take 10000 | extend p=bag_pack('when',datetime(2025-01-01),'amount',decimal(1.25),'row',Value) | extend a=pack_array(p,1h) | extend d=todecimal(a[0].amount),kind=gettype(a[1]) | summarize Total=sum(d),Good=countif(kind=='timespan') | project Total=tostring(Total),Good";
const TERMS_QUERY: &str =
    "Telemetry | where Message has_any(dynamic(['error','warn'])) | summarize N=count()";
const IP_QUERY: &str = "Telemetry | where ipv4_is_in_any_range(IP,dynamic(['10.0.0.0/8','192.168.0.0/16'])) | summarize N=count()";
const ENCODING_QUERY: &str = "Telemetry | extend decoded=base64_decode_tostring(base64_encode_tostring(Message)) | summarize N=countif(decoded==Message)";
const DCOUNT_QUERY: &str = "CardinalityEvents | summarize N=dcount(Id,2)";
const NATIVE_DCOUNT_QUERY: &str = "CardinalityEvents | summarize N=approx_distinct(Id)";
const GROUPED_DCOUNT_QUERY: &str =
    "CardinalityEvents | summarize N=dcount(Id,4) by Group | summarize Total=sum(N),Groups=count()";
const HLL_ROLLUP_QUERY: &str = "CardinalityEvents | summarize s=hll(Id,2) by Shard | summarize s=hll_merge(s) | project N=dcount_hll(s)";
const ARROW_QUERY: &str =
    "ArrowEvents | mv-expand Values to typeof(long) | summarize Total=sum(Values)";
const ARROW_SQL: &str = "SELECT coalesce(sum(v),0) AS \"Total\" FROM (SELECT UNNEST(\"Values\") AS v FROM \"ArrowEvents\")";
const ARROW_DYNAMIC_QUERY: &str = "ArrowEvents | extend d=todynamic(Values) | mv-expand d to typeof(long) | summarize Total=sum(d)";
const INDEXOF_QUERY: &str = "Telemetry | summarize Total=sum(indexof(Message,' '))";
const TEXT_REWRITE_QUERY: &str = "Telemetry | extend Rewritten=replace_regex(strrep(Message,2,';'),@'\\s+','_') | summarize N=countif(indexof(Rewritten,' ')==-1 and indexof(Rewritten,';')>=0)";

fn context() -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let schema = Arc::new(Schema::new(vec![
        Field::new("Value", DataType::Int64, false),
        Field::new("Category", DataType::Utf8, false),
    ]));
    let categories = ["api", "worker", "frontend", "scheduler"];
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..ROWS).map(|i| (i % 100) as i64),
            )),
            Arc::new(StringArray::from_iter_values(
                (0..ROWS).map(|i| categories[i % categories.len()]),
            )),
        ],
    )
    .unwrap();
    let partitions = (0..4)
        .map(|i| vec![batch.slice(i * ROWS / 4, ROWS / 4)])
        .collect();
    ctx.register_table(
        TableReference::bare("Events"),
        Arc::new(MemTable::try_new(schema, partitions).unwrap()),
    )
    .unwrap();
    ctx
}

fn json_context() -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let schema = Arc::new(Schema::new(vec![Field::new("Raw", DataType::Utf8, false)]));
    let payloads = [
        r#"{"service":"api","latencies":[1,5,9]}"#,
        r#"{"service":"worker","latencies":[2,6,10]}"#,
    ];
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from_iter_values(
            (0..JSON_ROWS).map(|i| payloads[i % payloads.len()]),
        ))],
    )
    .unwrap();
    let partitions = (0..4)
        .map(|i| vec![batch.slice(i * JSON_ROWS / 4, JSON_ROWS / 4)])
        .collect();
    ctx.register_table(
        TableReference::bare("JsonEvents"),
        Arc::new(MemTable::try_new(schema, partitions).unwrap()),
    )
    .unwrap();
    ctx
}

fn guid_context() -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let values = [
        "74be27de-1e4e-49d9-b579-fe0b331d3642",
        "01234567-89ab-cdef-0123-456789abcdef",
    ];
    let batch = RecordBatch::try_from_iter(vec![(
        "Text",
        Arc::new(StringArray::from_iter_values(
            (0..JSON_ROWS).map(|i| values[i % 2]),
        )) as _,
    )])
    .unwrap();
    let partitions = (0..4)
        .map(|i| vec![batch.slice(i * JSON_ROWS / 4, JSON_ROWS / 4)])
        .collect();
    ctx.register_table(
        TableReference::bare("GuidEvents"),
        Arc::new(MemTable::try_new(batch.schema(), partitions).unwrap()),
    )
    .unwrap();
    ctx
}

fn telemetry_context() -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let messages = ["ERROR disk", "WARN request", "INFO healthy", "terror"];
    let addresses = ["10.1.2.3", "192.168.1.5", "8.8.8.8", "bad"];
    let amounts = ["0.1", "9007199254740993.25"];
    let batch = RecordBatch::try_from_iter(vec![
        (
            "Message",
            Arc::new(StringArray::from_iter_values(
                (0..JSON_ROWS).map(|i| messages[i % 4]),
            )) as _,
        ),
        (
            "IP",
            Arc::new(StringArray::from_iter_values(
                (0..JSON_ROWS).map(|i| addresses[i % 4]),
            )) as _,
        ),
        (
            "Amount",
            Arc::new(StringArray::from_iter_values(
                (0..JSON_ROWS).map(|i| amounts[i % 2]),
            )) as _,
        ),
    ])
    .unwrap();
    let partitions = (0..4)
        .map(|i| vec![batch.slice(i * JSON_ROWS / 4, JSON_ROWS / 4)])
        .collect();
    ctx.register_table(
        TableReference::bare("Telemetry"),
        Arc::new(MemTable::try_new(batch.schema(), partitions).unwrap()),
    )
    .unwrap();
    ctx
}

fn cardinality_context() -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let batch = RecordBatch::try_from_iter(vec![
        (
            "Id",
            Arc::new(Int64Array::from_iter_values((0..ROWS).map(|i| i as i64))) as _,
        ),
        (
            "Group",
            Arc::new(Int64Array::from_iter_values(
                (0..ROWS).map(|i| (i % 100_000) as i64),
            )) as _,
        ),
        (
            "Shard",
            Arc::new(Int64Array::from_iter_values(
                (0..ROWS).map(|i| (i % 32) as i64),
            )) as _,
        ),
    ])
    .unwrap();
    let partitions = (0..4)
        .map(|i| vec![batch.slice(i * ROWS / 4, ROWS / 4)])
        .collect();
    ctx.register_table(
        TableReference::bare("CardinalityEvents"),
        Arc::new(MemTable::try_new(batch.schema(), partitions).unwrap()),
    )
    .unwrap();
    ctx
}

fn arrow_context() -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let values = ListArray::from_iter_primitive::<Int64Type, _, _>(
        (0..JSON_ROWS).map(|_| Some(vec![Some(1), Some(2), Some(3)])),
    );
    let batch = RecordBatch::try_from_iter(vec![("Values", Arc::new(values) as _)]).unwrap();
    let partitions = (0..4)
        .map(|i| vec![batch.slice(i * JSON_ROWS / 4, JSON_ROWS / 4)])
        .collect();
    ctx.register_table(
        TableReference::bare("ArrowEvents"),
        Arc::new(MemTable::try_new(batch.schema(), partitions).unwrap()),
    )
    .unwrap();
    ctx
}

fn benchmarks(c: &mut Criterion) {
    c.bench_function("parse/kql_pipeline", |b| {
        b.iter(|| parse(black_box(QUERY)).unwrap())
    });

    let runtime = Runtime::new().unwrap();
    let _guard = runtime.enter();
    let ctx = context();
    let parsed = parse(QUERY).unwrap();
    let mut planning = c.benchmark_group("planning");
    planning.bench_function("kql", |b| {
        b.to_async(&runtime)
            .iter(|| async { ctx.kql(black_box(QUERY)).await.unwrap() })
    });
    planning.bench_function("kql_preparsed", |b| {
        b.to_async(&runtime)
            .iter(|| async { ctx.kql_ast(black_box(&parsed)).await.unwrap() })
    });
    planning.bench_function("sql", |b| {
        b.to_async(&runtime)
            .iter(|| async { ctx.sql(black_box(SQL)).await.unwrap() })
    });
    planning.finish();

    let kql = runtime.block_on(async {
        ctx.kql(QUERY)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    });
    let sql = runtime.block_on(async {
        ctx.sql(SQL)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    });
    assert_eq!(
        displayable(kql.as_ref()).indent(false).to_string(),
        displayable(sql.as_ref()).indent(false).to_string(),
        "execution benchmarks must compare identical native physical plans",
    );
    let mut execution = c.benchmark_group("execution");
    execution.throughput(Throughput::Elements(ROWS as u64));
    execution.bench_function("kql", |b| {
        b.to_async(&runtime).iter(|| async {
            let plan = reset_plan_states(Arc::clone(&kql)).unwrap();
            collect(plan, ctx.task_ctx()).await.unwrap()
        })
    });
    execution.bench_function("sql", |b| {
        b.to_async(&runtime).iter(|| async {
            let plan = reset_plan_states(Arc::clone(&sql)).unwrap();
            collect(plan, ctx.task_ctx()).await.unwrap()
        })
    });
    execution.finish();

    let json = json_context();
    let guids = guid_context();
    let telemetry = telemetry_context();
    let cardinality = cardinality_context();
    let arrow = arrow_context();
    let mut distinct_estimate = None;
    let mut workloads = c.benchmark_group("workloads");
    for (name, query, context, rows, expected_rows) in [
        ("json_expand", JSON_QUERY, &json, JSON_ROWS, 2),
        ("make_series_fir", SERIES_QUERY, &ctx, ROWS, 4),
        ("partition_top", PARTITION_QUERY, &ctx, ROWS, 1),
        ("scan_cumulative", SCAN_QUERY, &ctx, ROWS, 1),
        ("scan_stateful", STATE_SCAN_QUERY, &ctx, 10_000, 1),
        ("mv_apply_top", APPLY_QUERY, &json, 1_000, 1),
        ("bag_unpack", UNPACK_QUERY, &json, JSON_ROWS, 2),
        ("pivot", PIVOT_QUERY, &ctx, ROWS, 1),
        ("json_fields", FIELDS_QUERY, &json, JSON_ROWS, 2),
        ("search", SEARCH_QUERY, &ctx, ROWS, 1),
        ("top_hitters", HITTERS_QUERY, &ctx, ROWS, 2),
        ("guid_parse", GUID_QUERY, &guids, JSON_ROWS, 1),
        ("decimal_exact", DECIMAL_QUERY, &telemetry, JSON_ROWS, 1),
        ("rich_nested", RICH_QUERY, &ctx, 10_000, 1),
        ("term_set", TERMS_QUERY, &telemetry, JSON_ROWS, 1),
        ("ip_ranges", IP_QUERY, &telemetry, JSON_ROWS, 1),
        ("encoding", ENCODING_QUERY, &telemetry, JSON_ROWS, 1),
        ("dcount_accuracy", DCOUNT_QUERY, &cardinality, ROWS, 1),
        ("dcount_native", NATIVE_DCOUNT_QUERY, &cardinality, ROWS, 1),
        ("dcount_groups", GROUPED_DCOUNT_QUERY, &cardinality, ROWS, 1),
        ("hll_rollup", HLL_ROLLUP_QUERY, &cardinality, ROWS, 1),
        ("arrow_expand", ARROW_QUERY, &arrow, JSON_ROWS, 1),
        ("arrow_expand_sql", ARROW_SQL, &arrow, JSON_ROWS, 1),
        (
            "arrow_dynamic_expand",
            ARROW_DYNAMIC_QUERY,
            &arrow,
            JSON_ROWS,
            1,
        ),
        ("indexof", INDEXOF_QUERY, &telemetry, JSON_ROWS, 1),
        ("text_rewrite", TEXT_REWRITE_QUERY, &telemetry, JSON_ROWS, 1),
    ] {
        let plan = runtime.block_on(async {
            let frame = if name == "arrow_expand_sql" {
                context.sql(query).await.unwrap()
            } else {
                context.kql(query).await.unwrap()
            };
            frame.create_physical_plan().await.unwrap()
        });
        let native = displayable(plan.as_ref()).indent(false).to_string();
        if name == "arrow_expand" {
            assert!(
                native.contains("__kql_native_expand_list"),
                "typed Arrow expansion must not use the JSON conversion path"
            );
        }
        if name == "partition_top" {
            assert!(
                !native.contains("KqlApplyExec"),
                "partition benchmark must use the vectorized fast path"
            );
        }
        if name == "mv_apply_top" {
            assert!(
                !native.contains("KqlApplyExec"),
                "array-top benchmark must use batch kernels"
            );
        }
        if name == "scan_cumulative" {
            assert!(
                native.contains("WindowAggExec") && !native.contains("KqlScanExec"),
                "cumulative benchmark must use native windows"
            );
        }
        let result = runtime.block_on(async {
            collect(
                reset_plan_states(Arc::clone(&plan)).unwrap(),
                context.task_ctx(),
            )
            .await
            .unwrap()
        });
        assert_eq!(
            result.iter().map(RecordBatch::num_rows).sum::<usize>(),
            expected_rows,
            "unexpected result cardinality for {name}",
        );
        if matches!(
            name,
            "arrow_expand"
                | "arrow_expand_sql"
                | "arrow_dynamic_expand"
                | "indexof"
                | "text_rewrite"
        ) {
            let expected = match name {
                "indexof" => 300_000,
                "text_rewrite" => JSON_ROWS as i64,
                _ => (JSON_ROWS * 6) as i64,
            };
            assert_eq!(
                datafusion::common::ScalarValue::try_from_array(result[0].column(0), 0).unwrap(),
                datafusion::common::ScalarValue::Int64(Some(expected)),
                "incorrect result for {name}"
            );
        }
        if matches!(name, "dcount_accuracy" | "dcount_native" | "hll_rollup") {
            let value =
                datafusion::common::ScalarValue::try_from_array(result[0].column(0), 0).unwrap();
            let estimate = match value {
                datafusion::common::ScalarValue::Int64(Some(value)) => value as f64,
                datafusion::common::ScalarValue::UInt64(Some(value)) => value as f64,
                _ => panic!("distinct benchmark must return a numeric count"),
            };
            let precision = if name == "dcount_native" { 14 } else { 16 };
            assert!(
                (estimate / ROWS as f64 - 1.0).abs()
                    <= 6.0 * 1.04 / ((1_u64 << precision) as f64).sqrt(),
                "{name} exceeds its six-sigma error check: {estimate}"
            );
            if name == "dcount_accuracy" {
                distinct_estimate = Some(value);
            } else if name == "hll_rollup" {
                assert_eq!(
                    Some(value),
                    distinct_estimate,
                    "merged sketches must equal the one-pass estimate"
                );
            }
        }
        if name == "dcount_groups" {
            assert_eq!(
                datafusion::common::ScalarValue::try_from_array(result[0].column(0), 0).unwrap(),
                datafusion::common::ScalarValue::Int64(Some(ROWS as i64))
            );
            assert_eq!(
                datafusion::common::ScalarValue::try_from_array(result[0].column(1), 0).unwrap(),
                datafusion::common::ScalarValue::Int64(Some(100_000))
            );
        }
        if matches!(name, "pivot" | "search") {
            let expected = if name == "pivot" {
                ROWS as i64
            } else {
                140_000
            };
            assert_eq!(
                datafusion::common::ScalarValue::try_from_array(result[0].column(0), 0).unwrap(),
                datafusion::common::ScalarValue::Int64(Some(expected))
            );
        }
        if name == "guid_parse" {
            assert_eq!(
                datafusion::common::ScalarValue::try_from_array(result[0].column(0), 0).unwrap(),
                datafusion::common::ScalarValue::Int64(Some(2))
            );
        }
        if matches!(name, "term_set" | "ip_ranges" | "encoding") {
            let expected = if name == "encoding" {
                JSON_ROWS as i64
            } else {
                (JSON_ROWS / 2) as i64
            };
            assert_eq!(
                datafusion::common::ScalarValue::try_from_array(result[0].column(0), 0).unwrap(),
                datafusion::common::ScalarValue::Int64(Some(expected))
            );
        }
        if name == "decimal_exact" {
            assert_eq!(
                datafusion::common::ScalarValue::try_from_array(result[0].column(0), 0).unwrap(),
                datafusion::common::ScalarValue::Utf8(Some("450359962737049692500".into()))
            );
        }
        if name == "rich_nested" {
            assert_eq!(
                datafusion::common::ScalarValue::try_from_array(result[0].column(0), 0).unwrap(),
                datafusion::common::ScalarValue::Utf8(Some("12500".into()))
            );
            assert_eq!(
                datafusion::common::ScalarValue::try_from_array(result[0].column(1), 0).unwrap(),
                datafusion::common::ScalarValue::Int64(Some(10_000))
            );
        }
        if matches!(name, "bag_unpack" | "json_fields") {
            for batch in &result {
                for row in 0..batch.num_rows() {
                    assert_eq!(
                        datafusion::common::ScalarValue::try_from_array(batch.column(1), row)
                            .unwrap(),
                        datafusion::common::ScalarValue::Int64(Some(150_000))
                    );
                }
            }
        }
        workloads.throughput(Throughput::Elements(rows as u64));
        workloads.bench_function(name, |b| {
            b.to_async(&runtime).iter(|| async {
                collect(
                    reset_plan_states(Arc::clone(&plan)).unwrap(),
                    context.task_ctx(),
                )
                .await
                .unwrap()
            })
        });
    }
    workloads.finish();
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
