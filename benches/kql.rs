use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
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
        b.to_async(&runtime)
            .iter(|| async { collect(Arc::clone(&kql), ctx.task_ctx()).await.unwrap() })
    });
    execution.bench_function("sql", |b| {
        b.to_async(&runtime)
            .iter(|| async { collect(Arc::clone(&sql), ctx.task_ctx()).await.unwrap() })
    });
    execution.finish();
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
