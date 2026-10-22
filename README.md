# datafusion-kql

A **native Kusto Query Language (KQL) frontend for Apache DataFusion**. Write
pipe-oriented queries against existing DataFusion tables and receive ordinary,
lazy `DataFrame`s backed by DataFusion's optimizer and vectorized Arrow engine.
No DataFusion fork, SQL translation, or separate execution engine.

**Status:** typed-tabular and dynamic/JSON query support, targeting **DataFusion
55.2.x** and **Rust 1.94+**. This is not yet a complete implementation of KQL.
See the [compatibility contract](docs/compatibility.md) for supported constructs
and known semantic differences.

## Run it

```bash
cargo run --example quickstart
cargo run --example telemetry_json
```

The example uses an inline `datatable`, filters requests, aggregates by service,
and prints the native logical plan and results. No Azure account or service is
required.

`telemetry_json` parses JSON telemetry, reads nested properties, expands latency
arrays with native `Unnest`, and aggregates the expanded measurements.

## Extend an existing DataFusion session

While this crate is unpublished, add it as a local path dependency alongside a
matching DataFusion version:

```toml
[dependencies]
datafusion = "55.2"
datafusion-kql = { path = "../datafusion-kql" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use datafusion::prelude::*;
use datafusion::common::TableReference;
use datafusion_kql::SessionContextKqlExt;

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    let ctx = SessionContext::new();
    ctx.register_parquet(TableReference::bare("Events"), "events.parquet", ParquetReadOptions::default())
        .await?;

    let frame = ctx.kql(r#"
        let threshold = 100;
        Events
        | where Duration > threshold
        | summarize Requests=count(), Average=avg(Duration) by Service
        | top 10 by Requests
    "#).await?;

    // Planning is lazy. Execution starts with collect, show, or execute_stream.
    frame.show().await?;
    Ok(())
}
```

CSV, JSON, memory tables, catalogs, custom `TableProvider`s, registered functions,
object stores, and runtime settings are the same ones used by your existing
session. KQL does not install or overwrite global/session functions. SQL can
continue to be used independently.

KQL names are case-sensitive. DataFusion may SQL-normalize plain `&str` table
references to lowercase; use `TableReference::bare("Events")` when registering a
mixed-case name, or use lowercase names consistently.

Use `frame.execute_stream().await?` to consume results without collecting them
all in memory. The default `parquet` feature enables DataFusion's Parquet
provider; disable default features when the host does not need it.

## Parse once, plan against the current session

```rust,no_run
use datafusion::prelude::*;
use datafusion_kql::{SessionContextKqlExt, logical_plan, parse};

# async fn example(ctx: &SessionContext) -> datafusion_kql::Result<()> {
let ast = parse("Events | where Value > 42 | project Value | take 100")?;
let frame = ctx.kql_ast(&ast).await?;
let plan = logical_plan(ctx, &ast).await?;
println!("{}", plan.display_indent());
# Ok(())
# }
```

The AST can be reused. Each planning call resolves current catalogs and gets a
new query-stable `now()`/`ago()` instant. Do not cache executable plans across
schema changes or expect an already planned timestamp literal to advance.
`parse_with_options` exposes parser resource limits. Parse/unsupported errors
carry byte spans; `error.diagnostic(query)` adds a line/column and caret when
the application chooses to display query text.

## What is implemented

Typed literals, scalar/tabular `let`, `print`, `datatable`, integer `range`,
filtering, projection, extension, renaming, sorting, top-k, distinct, aggregation,
joins (including default `innerunique`), and name-aligned unions. Common scalar
functions and string predicates lower to native DataFusion expressions.
KQL-specific null equality, non-null strings, aggregate defaults, and sort
defaults are handled explicitly.

Dynamic literals, JSON parsing, property/index access, bag/array/set functions,
and `mv-expand` are supported with typed Arrow unions and native batch kernels.
Multiple expanded columns zip with null padding rather than forming an
accidental Cartesian product.

Typed `parse`/`parse-where`, regex/JSON extraction, collection and percentile
aggregations, correlated `arg_min`/`arg_max`, and scalar expressions over
aggregates are also supported.

`mv-apply`, `make-series`, management commands, hints, and complete
Kusto-service conformance are future milestones—not silently emulated.

## Architecture and performance

```text
KQL -> lexer / AST -> DataFusion LogicalPlan / Expr
                              -> native optimizer -> Arrow execution
```

Ordinary filters, projections, joins, aggregations, and limits remain visible to
DataFusion's optimizer. Dynamic/JSON operations use expression-local Arrow batch
kernels, not a separate row-oriented query interpreter, and do not overwrite the
session's function registry. `has` currently
uses native regex scanning, not a Kusto-style inverted index. Normalizing
nullable Arrow strings can add work; non-null source schemas avoid that cost.

[Architecture and delivery plan](docs/architecture.md) documents the research,
design choices, and next compatibility milestones.
[Performance methodology](docs/performance.md) explains the benchmarks and how
to compare equivalent KQL and SQL without conflating planning and execution.

```bash
cargo bench --bench kql
```

There is no universal speed guarantee: actual performance depends on data
layout, providers, predicates, memory budgets, and session configuration.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo check --lib --no-default-features
```

Tests cover parser diagnostics/limits, KQL-specific semantics, SQL result parity,
optimizer behavior, Arrow integration, and Parquet. SQL is enabled only by the
development dependency for comparison; the frontend does not require
DataFusion's SQL feature at runtime.

## License

[Apache-2.0](LICENSE).
