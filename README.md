# datafusion-kql

A **native Kusto Query Language (KQL) frontend for Apache DataFusion**. Write
pipe-oriented queries against existing DataFusion tables and receive ordinary,
lazy `DataFrame`s backed by DataFusion's optimizer and vectorized Arrow engine.
No DataFusion fork, SQL translation, or separate execution engine.

**Status:** version 0.11 typed-tabular, dynamic/JSON, grouped/stateful, window and time-series
support, targeting **DataFusion 55.2.x** and **Rust 1.94+**. This is not yet a
complete implementation of KQL.
See the [compatibility contract](docs/compatibility.md) for supported constructs
and known semantic differences.

## Run it

```bash
cargo run --example quickstart
cargo run --example telemetry_json
cargo run --example time_series
cargo run --example functions
cargo run --example grouped_scan
cargo run --example schema_plugins
cargo run --example telemetry_functions
cargo run --example distinct_sketches
cargo run --example arrow_interop
cargo run --example percentile_sketches
cargo run --example materialized_snapshots
```

The example uses an inline `datatable`, filters requests, aggregates by service,
and prints the native logical plan and results. No Azure account or service is
required.

`telemetry_json` parses JSON telemetry, reads nested properties, expands latency
arrays with native `Unnest`, and aggregates the expanded measurements.
`arrow_interop` queries native Arrow list columns directly, using a local table
selector, hexadecimal threshold, regex normalization, and typed buffer expansion.
`materialized_snapshots` reuses a larger-than-memory snapshot with the host's
bounded memory pool and native temporary spill storage.

## Extend an existing DataFusion session

The crates.io package is named **`datafusion-kql-native`**; the Rust library
remains **`datafusion_kql`**. The registry name `datafusion-kql` belongs to a
separate project. Until the first publication, use a local dependency alias
alongside the matching DataFusion version:

```toml
[dependencies]
datafusion = "55.2"
datafusion-kql = { package = "datafusion-kql-native", path = "../datafusion-kql" }
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

Typed literals, scalar/tabular `let`, `print`, `datatable`, numeric/temporal `range`,
filtering, projection, extension, renaming, sorting, top-k, distinct, aggregation,
joins (including default `innerunique`), and name-aligned unions. Common scalar
functions and string predicates lower to native DataFusion expressions.
KQL-specific null equality, non-null strings, aggregate defaults, and sort
defaults are handled explicitly.

Dynamic literals, JSON parsing, property/index access, bag/array/set functions,
and `mv-expand` are supported with typed Arrow unions and native batch kernels.
Multiple expanded columns zip with null padding rather than forming an
accidental Cartesian product.
Native Arrow lists, list views, structs and string-keyed maps also work with
dynamic operations and expansion, preserving nested temporal/decimal/GUID types.
Simple typed list expansion stays in checked Arrow buffers and native Unnest.

Typed `parse`/`parse-where`, regex/JSON extraction, collection and percentile
aggregations, correlated `arg_min`/`arg_max`, and scalar expressions over
aggregates are also supported.

Serialized row sets support `prev`, `next`, restart-aware row numbers,
cumulative sums and ranks, and bounded session windows. Hidden order tracking
preserves semantics through projections, filters, and expansion without adding
window overhead to ordinary analytic queries.

Lookup, schema introspection, projection reordering, sampling, wildcard table
sources, and source-labelled/type-splitting unions extend the tabular surface.
Limits and top-k counts can use constant `let` bindings and arithmetic.
`make-series` supports sparse native aggregation, exact time/numeric axes,
defaults and group bounds; series functions add interpolation, statistics,
line fitting, arithmetic and FIR/IIR processing.
Calendar arithmetic, period boundaries, Unix conversions, constructors, and
compiled KQL date/time formatting are supported. Both tabular and scalar ranges
handle numeric, datetime, and timespan values.

Query parameters use typed request values rather than source interpolation:
`declare query_parameters(...)` works with `kql_with_parameters` and
`kql_ast_with_parameters`. Query-defined scalar and tabular functions support
typed/default/named arguments, lexical captures, nested scalar definitions,
tabular schemas, and `invoke`. Functions expand into ordinary native
expressions/plans, without installing anything in the session registry.
Parameterless `view` functions participate in wildcard discovery, and lazy
`toscalar` supports staged scalar calculations from tabular subqueries.
Row-independent scalar functions can contain tabular local bindings and scalar
subqueries. Runtime scalar values can drive lazy native table ranges, and
`in`/`!in` support tabular queries with KQL-specific null semantics.

`materialize(...)` and `as hint.materialized=true` create lazy shared Arrow
snapshots for repeated subqueries. Input partitions execute in parallel once,
and cached buffers are charged to the host's DataFusion memory pool. When that
pool cannot retain the snapshot, native Arrow IPC spill/replay uses the host's
disk manager, quotas, and compression settings. Small snapshots stay in memory;
disk-disabled or exhausted configurations still fail explicitly. Snapshots
belong to the planned query/DataFrame, including its clones; plan a new query
for a fresh snapshot. See the compatibility contract for lifetime, cancellation,
and working-memory
boundaries.

`mv-apply` runs native per-row subqueries over zipped dynamic arrays. `partition`
supports per-key transformations, with vectorized fast paths for common
aggregate/top-k workloads and reusable native templates for general subqueries.
`scan` adds typed state machines, step promotion, match IDs, simultaneous
assignments, output modes, and native cumulative-window fast paths.
Cross-table `search`/`find`, smart record packing, exact heavy-hitter estimates,
and hierarchical `top-nested` selection are supported. Schema-declared
`evaluate bag_unpack` and `evaluate pivot` keep JSON extraction and conditional
aggregation lazy and native.
GUID/UUID values use native 128-bit Arrow storage, with literals, nullable
conversion, generation, canonical formatting, typed parameters, and root dynamic
GUID tags. Decimal arithmetic uses a 34-digit decimal128 runtime with
reference-backed scalar/vector conversion semantics, and nested dynamic
datetime/timespan/GUID/decimal values retain their types across access,
collections, expansion, aggregation, and serialized offsets. Existing dynamic
tag numbers and previous input schemas remain compatible.
Term-list/prefix/suffix searches, IPv4/IPv6 and CIDR functions, URL/query
parsing, percent/base64 encodings, and regex trimming cover common telemetry
queries. Compiled matchers and network range caches are bounded.
`indexof` implements zero-based optional-range/occurrence semantics, `strrep`
supports delimiters, and global `replace_regex` uses KQL capture rewrites.
Hexadecimal integers, spelled duration units, RFC date forms and literal
`table`/`database`/`cluster` source selectors close common interoperability gaps.
Source selectors resolve local DataFusion providers; they are not ADX connections.

`dcount`/`dcountif` support accuracy levels 0-4 with bounded HyperLogLog states
and exact small-set counting. `hll`/`hll_if`, scalar and aggregate `hll_merge`,
and `dcount_hll` support reusable daily-to-weekly distinct-count rollups.
Sketches use a versioned local format, not Kusto-service binary compatibility.
Native vectorized grouped accumulators keep small groups sparse instead of
allocating a full register array per key.

Percentile families now use bounded exact-small/t-digest-large states instead
of retaining every input sample. `tdigest`, scalar/aggregate `tdigest_merge`
and `merge_tdigest`, and typed percentile accessors support distribution rollups.
Longs and temporal values preserve their logical types; weighted rank arithmetic
does not round counts through `f64`. Explicit `*_exact` percentile extensions
retain the previous sample-storing behavior when exact ranks are required.
See the compatibility contract for the intentional approximation change,
strict sketch validation, and local-format boundary.

Advanced signal processing, management commands, additional hints, and complete
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
HLL regressions separately cover exact thresholds, probabilistic error checks,
partial-state/filter/merge equivalence, malformed sketches, and host memory
accounting. Large-population and memory tests remain local algorithm checks;
the small-set live reference fixtures are recorded separately.

`tests/conformance.rs` compares 68 synthetic fixtures with captured public Kusto
help-cluster results by default, and provides an opt-in live suite for a
configured emulator or ADX test database. The formerly ignored decimal
precision and JSON conversion cases are now ordinary regressions. Seven live
t-digest captures cover weighted accessors, null defaults, temporal types,
merge aliases, nonpositive weights, and array shape. The nine previously
source-only HLL/string/literal cases now have live captures, together with three
additional `indexof` edge fixtures that corrected window, overlap, empty-match
and negative-start semantics. These finite captures do not certify
identical large-population estimates or interoperable service blobs. CI also exercises curl
transport against a self-owned loopback fixture, **not a Kusto engine**.
See the compatibility contract for capture provenance, live-test setup, and
remaining conformance boundaries.

Building the decimal128 runtime requires a C compiler for the bundled
`libdecnumber` dependency. Decimal output uses an opaque Arrow struct rather
than a fixed-scale Arrow decimal; use KQL `tostring` for display and see the
compatibility contract before migrating persisted 0.6 schemas.

## Crate releases

CI validates Rust 1.94/stable and builds a verified `.crate` archive with
`cargo publish --dry-run`. Version tags trigger a release workflow that
revalidates the exact commit, requires it to be merged into `main`, and waits
for approval in the protected `crates-io` environment before publishing.
Subsequent releases use short-lived crates.io OIDC credentials. The first
publication requires a separately configured bootstrap API token; no registry
credentials are stored in the source tree.

See [the release guide](docs/releasing.md) for first-publish setup, trusted
publisher configuration, dry runs, and version-tag releases. No crate is
represented as published merely because the dry-run CI succeeds.

## License

[Apache-2.0](LICENSE).
