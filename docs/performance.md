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

The fixture uses two non-null columns, four in-memory partitions, a filter,
grouped sum, and top-k. Source data and physical planning are outside the
execution measurements. Each iteration collects the small final aggregate
result, not a million-row output. Execution numbers therefore measure a
particular memory-resident analytic workload, not Parquet I/O, joins, string
search, remote catalogs, or all of KQL.

Each execution iteration uses DataFusion's `reset_plan_states` to create fresh
operator state from the cached physical plan. This reset is included in the
timing for both languages. Stateful repartition operators must not be executed
repeatedly by simply cloning their `Arc`.

## Measured local baseline

Measured on 2026-10-22 with an Apple M4 (arm64), Rust 1.95.0, DataFusion 55.2.0,
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

The crate does not change process-wide compiler flags, allocate an unbounded plan
cache, force materialization, replace the host allocator, or override session
optimizer settings. Host applications remain in control of those decisions.
