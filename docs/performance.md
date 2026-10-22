# Performance methodology

The performance goal is low frontend overhead followed by **the same native
DataFusion execution machinery** as equivalent SQL/DataFrame queries. This does
not imply that every KQL construct produces the same plan as SQL: nullable
equality/string semantics and `innerunique` require additional expressions or
operators.

## Reproduce

```bash
cargo bench --bench kql
```

For a short local sample rather than a stable performance baseline:

```bash
cargo bench --bench kql -- --warm-up-time 0.1 --measurement-time 0.2 --sample-size 10
```

Criterion stores machine-readable results under `target/criterion`. Use the same
Rust version, host, CPU power mode, build profile, allocator, and DataFusion
configuration for comparisons. The default bench profile is optimized. Short
samples are smoke measurements, not regression thresholds or production
capacity estimates.

## What is measured

| Benchmark | Work inside the timed region |
| --- | --- |
| `parse/kql_pipeline` | Lexing and AST allocation for a filter/aggregate/top pipeline |
| `planning/kql` | Parsing and construction of a lazy native DataFrame |
| `planning/kql_preparsed` | AST reuse and construction of a lazy native DataFrame |
| `planning/sql` | Equivalent SQL parsing and construction of a lazy DataFrame |
| `execution/kql` | Execute an already-created physical plan over 1,000,000 rows |
| `execution/sql` | Execute the equivalent SQL physical plan over the same data |
| `workloads/json_expand` | Parse 100,000 JSON payloads, extract fields, expand three latencies per payload, and aggregate by service |
| `workloads/make_series_fir` | Aggregate 1,000,000 rows into four 100-point series and apply a normalized three-tap FIR filter |

The fixture uses two non-null columns, four in-memory partitions, a filter,
grouped sum, and top-k. Source data and physical planning are outside the
execution measurements. Each iteration collects the small final aggregate
result, not a million-row output. Execution numbers therefore measure a
particular memory-resident analytic workload, not Parquet I/O, joins, string
search, remote catalogs, or all of KQL.

The JSON and time-series workloads also use four in-memory partitions and
prebuilt physical plans. Their throughput counts **input** rows, not expanded
elements or generated points. They measure supported KQL workloads without a SQL
comparison; they are not evidence of a speed advantage over another language or
engine.

Each execution iteration uses DataFusion's `reset_plan_states` to create fresh
operator state from the cached physical plan. This reset is included in the
timing for both languages. Stateful repartition operators must not be executed
repeatedly by simply cloning their `Arc`.

## Measured 0.2 baseline

The expanded 0.2.0 library at commit `fcf7b55` was measured on 2026-10-22 with
an Apple M4 (arm64), Rust 1.95.0, DataFusion 55.2.0, the default optimized bench
profile, and four target partitions:

```bash
cargo bench --locked --bench kql -- --warm-up-time 1 --measurement-time 3 --sample-size 30
```

| Benchmark | Criterion estimate | 95% confidence interval |
| --- | --- | --- |
| Parse KQL | 1.90 us | 1.89-1.91 us |
| Plan KQL, including parsing | 36.84 us | 36.78-36.90 us |
| Plan a previously parsed AST | 34.25 us | 34.12-34.37 us |
| Plan equivalent SQL | 91.06 us | 90.77-91.33 us |
| Execute KQL over 1,000,000 rows | 4.39 ms | 4.24-4.60 ms |
| Execute equivalent SQL over 1,000,000 rows | 4.08 ms | 4.01-4.16 ms |
| JSON parse/expand/aggregate, 100,000 payloads | 88.65 ms | 86.12-91.33 ms |
| Make-series and FIR, 1,000,000 rows | 13.89 ms | 13.67-14.13 ms |

This corresponds to about 228 million input rows/second for the numeric KQL
execution fixture, 1.13 million input JSON payloads/second (three expanded
measurements each), and 72.0 million input rows/second for the series fixture.
The numeric KQL/SQL benchmark asserts identical displayed native physical plans.
KQL planning was cheaper for this query; the SQL execution estimate was slightly
faster in this run. The execution timing contains neither language frontend.
Do not infer an execution-speed advantage from the choice of query language.

Both KQL and SQL measured faster than the initial baseline below, so these
shared-host runs do not isolate code changes from host load, cache state, or
compiler/environment effects. They are local measurements, not universal
throughput guarantees or statistically controlled release-to-release
regression claims.

## Initial baseline

The initial baseline below was measured at commit `6add036`, before the 0.2
coverage expansion, on 2026-10-22 with an Apple M4 (arm64), Rust 1.95.0, DataFusion 55.2.0,
the default optimized bench profile, and four target partitions:

```bash
cargo bench --locked --bench kql -- --warm-up-time 1 --measurement-time 3 --sample-size 30
```

| Benchmark | Criterion estimate | 95% confidence interval |
| --- | --- | --- |
| Parse KQL | 2.43 us | 2.36-2.50 us |
| Plan KQL, including parsing | 46.48 us | 45.49-47.38 us |
| Plan a previously parsed AST | 41.47 us | 40.57-42.56 us |
| Plan equivalent SQL | 137.92 us | 133.07-141.64 us |
| Execute KQL over 1,000,000 rows | 7.93 ms | 6.85-8.97 ms |
| Execute equivalent SQL over 1,000,000 rows | 6.85 ms | 6.39-7.49 ms |

The benchmark asserts identical displayed native physical plans before measuring
execution, including the sum's empty-input default and top-k ordering. The KQL
execution estimate corresponds to approximately 126 million input rows/second
for this fixture. The frontend has lower measured planning cost than SQL here;
there is **no demonstrated execution-speed advantage over SQL**. Execution
intervals overlap, and the shared host showed substantial scheduling variance
across runs. These are a reproducible local baseline, not universal performance
claims or a stable regression budget.

## Correctness gates

Performance is not a substitute for semantic verification:

- Compare actual results and schemas where KQL and SQL are equivalent.
- Assert native projection/filter pushdown and top-k plans.
- Exercise KQL null/string/default semantics separately rather than treating SQL
  as the oracle for language differences.
- Keep queries lazy: building a DataFrame must not scan a provider.
- Exercise Parquet with early predicates and narrow projections.

## Production guidance

Register real `TableProvider`s rather than copying data through inline tables.
Prefer Parquet with suitable row groups/partitioning for persistent analytics.
Use non-null string fields when the data guarantees it; adapting nullable Arrow
strings to KQL's non-null string semantics can inhibit string filter pushdown.
Use `_cs` string operators when appropriate to avoid case folding. `has` currently
does a regex scan and does not create an inverted index.

Configure target partitions, batch sizes, bounded memory pools, spill storage,
and application-level timeouts on the embedding DataFusion runtime. Keep
filters/projections early, inspect `into_optimized_plan()`/physical plans, and
benchmark representative data rather than optimizing solely for inline examples.

Use `materialize` only when reusing a sufficiently expensive common subquery.
It buffers the full selected result and adds a cache boundary. Reduce rows and
columns inside it, and configure a bounded memory pool. A planned query and its
clones reuse the snapshot, so a benchmark of cold materialization must create a
fresh query per iteration; resetting physical operator state alone deliberately
does not invalidate that query's snapshot.

The crate does not change process-wide compiler flags, allocate an unbounded plan
cache, force materialization, replace the host allocator, or override session
optimizer settings. Host applications remain in control of those decisions.
