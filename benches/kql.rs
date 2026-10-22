use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
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
    let mut workloads = c.benchmark_group("workloads");
    for (name, query, context, rows, expected_rows) in [
        ("json_expand", JSON_QUERY, &json, JSON_ROWS, 2),
        ("make_series_fir", SERIES_QUERY, &ctx, ROWS, 4),
    ] {
        let plan = runtime.block_on(async {
            context
                .kql(query)
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap()
        });
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
