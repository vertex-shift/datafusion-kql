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
| `workloads/partition_top` | Native per-category top-three windows over 1,000,000 rows, followed by a count |
| `workloads/scan_cumulative` | Native cumulative-window scan over 1,000,000 nonnullable longs, followed by a maximum |
| `workloads/scan_stateful` | Conditional state-machine scan over 10,000 rows, followed by a maximum |
| `workloads/mv_apply_top` | Parse 1,000 payloads and run native per-row top-two/aggregation subqueries |
| `workloads/bag_unpack` | Parse/unpack 100,000 JSON payloads with a declared schema and aggregate array lengths by service |
| `workloads/json_fields` | Equivalent service/latency extraction through ordinary property access over the same payloads |
| `workloads/pivot` | Validate four pivot values and conditionally aggregate 1,000,000 rows, then check total row count |
| `workloads/search` | Column-restricted regex term search and a numeric filter over 1,000,000 rows |
| `workloads/top_hitters` | Exact native weighted heavy hitters over 1,000,000 rows |

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

## Measured 0.4 operator workloads

The new operators at commit `bef5a54` were measured on 2026-10-22 on the same
Apple M4/arm64 host, Rust 1.95.0, DataFusion 55.2.0, optimized bench profile,
and four target partitions:

```bash
cargo bench --locked --bench kql -- \
  'workloads/(bag_unpack|json_fields|pivot|search|top_hitters)' \
  --warm-up-time 1 --measurement-time 3 --sample-size 30
```

| Benchmark | Criterion estimate | 95% confidence interval |
| --- | --- | --- |
| Declared bag unpack, 100,000 JSON payloads | 39.67 ms | 38.14-41.10 ms |
| Equivalent ordinary property extraction | 45.57 ms | 44.87-46.29 ms |
| Strict-schema pivot, 1,000,000 rows | 5.97 ms | 5.91-6.03 ms |
| Column-restricted search, 1,000,000 rows | 27.26 ms | 26.83-27.72 ms |
| Exact weighted heavy hitters, 1,000,000 rows | 2.15 ms | 2.12-2.19 ms |

The JSON fixture has two requested fields and verifies identical measurement
totals for unpacking and ordinary extraction. It is not a general guarantee
about JSON inputs with different widths/types. The pivot fixture validates all
four allowed string buckets, groups by numeric value, and asserts a final
one-million-row total. The search fixture combines a literal term regex with a
numeric filter. Heavy hitters uses exact native aggregation, not a Kusto sketch.
These measurements correspond to approximately 2.52 million unpacked payloads,
167.5 million pivot input rows, 36.7 million search input rows, and 464.9 million
heavy-hitter input rows per second for these memory-resident fixtures only.

No result is a claim of an execution advantage over an equivalent native
DataFusion plan or of performance for every KQL operator. Shared-host timing
variance and all earlier comparison limits still apply.

## Measured 0.3 baseline

Version 0.3.0 at commit `81f01d7` was measured on 2026-10-22 on the same Apple
M4/arm64 host, Rust 1.95.0, DataFusion 55.2.0, optimized bench profile, and four
target partitions, using the 1-second warm-up/3-second target/30-sample command
below. Criterion extended collection for workloads needing more time to collect
30 samples.

| Benchmark | Criterion estimate | 95% confidence interval |
| --- | --- | --- |
| Parse KQL | 1.30 us | 1.28-1.31 us |
| Plan KQL, including parsing | 25.32 us | 24.76-26.10 us |
| Plan a previously parsed AST | 24.36 us | 23.93-24.92 us |
| Plan equivalent SQL | 58.44 us | 57.63-59.42 us |
| Execute KQL over 1,000,000 rows | 4.54 ms | 4.33-4.84 ms |
| Execute equivalent SQL over 1,000,000 rows | 4.10 ms | 4.05-4.15 ms |
| JSON parse/expand/aggregate, 100,000 payloads | 95.37 ms | 92.71-98.05 ms |
| Make-series and FIR, 1,000,000 rows | 13.78 ms | 13.57-14.01 ms |
| Partition top-three, 1,000,000 rows | 52.91 ms | 52.31-53.58 ms |
| Cumulative scan, 1,000,000 rows | 116.21 ms | 114.51-118.04 ms |
| Conditional stateful scan, 10,000 rows | 273.47 ms | 267.67-280.60 ms |
| Per-array top/aggregate, 1,000 payloads | 1.44 ms | 1.42-1.47 ms |

The numeric execution fixture processes about 220 million input rows/second;
the vectorized array-top fixture processes about 695,000 payloads/second. The
benchmark asserts native cumulative-window/partition fast paths and identical
SQL/KQL numeric physical plans. SQL's execution estimate was faster in this run;
the choice of frontend itself does not accelerate an identical physical plan.

The first 0.3 array-top implementation at `76e30e5` measured 144.73 ms
(141.72-147.89 ms) for the same 1,000-payload workload because it initialized a
native subquery template per input row. The batch Arrow specialization removes
that setup cost. Differential cases cover empty/null/mixed-type arrays,
integer precision, native overflow behavior, ordering, and limits.

**General state machines are not a high-throughput substitute for native
aggregates/windows.** The conditional scan fixture remains about 36,600 input
rows/second here. Prefer `row_cumsum`, native aggregates, or the exact
cumulative-scan fast path when equivalent. More general `mv-apply` subqueries
also retain native-template initialization overhead; their performance should
not be inferred from the specialized array-top fixture.

The shared host showed material timing variation, including in unchanged SQL
code. Cross-run changes are not statistically controlled code-only comparisons.
These are workload-specific local measurements, not universal guarantees.

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
