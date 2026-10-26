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
| `workloads/guid_parse` | Parse 100,000 canonical GUID strings to native 128-bit values and verify two distinct IDs |
| `workloads/decimal_exact` | Parse/add 100,000 decimals, aggregate natively, and verify an exact total beyond `2^53` |
| `workloads/rich_nested` | Pack/access typed nested values over 10,000 rows; verify exact amounts and preserved timespan tags |
| `workloads/term_set` | Case-insensitive compiled term-set matching over 100,000 telemetry strings |
| `workloads/ip_ranges` | Cached merged-CIDR membership over 100,000 IPv4 strings, including invalid addresses |
| `workloads/encoding` | UTF-8 base64 encode/decode over 100,000 strings and verify every round trip |
| `workloads/dcount_accuracy` | Accuracy-2 HLL over 1,000,000 unique longs; assert its six-sigma error bound |
| `workloads/dcount_native` | Native DataFusion `approx_distinct` over the same 1,000,000 longs, with its own precision/error check |
| `workloads/dcount_groups` | Accuracy-4 distinct counts over 100,000 ten-value groups; assert the exact one-million total and group count |
| `workloads/hll_rollup` | Build 32 accuracy-2 sketches over 1,000,000 rows, merge them, and assert equality with the one-pass HLL estimate |
| `workloads/arrow_expand` | Expand 100,000 native Arrow three-long lists through the typed buffer path and assert a 600,000 total |
| `workloads/arrow_expand_sql` | Native SQL Unnest/sum over the same Arrow lists and exact total |
| `workloads/arrow_dynamic_expand` | Explicitly convert those lists to dynamic, expand, and assert the same total |
| `workloads/indexof` | Zero-based string search over 100,000 telemetry messages; assert the exact position sum |
| `workloads/text_rewrite` | Delimited repetition, regex rewriting and two substring checks over 100,000 messages |
| `workloads/bounded_percentiles` | Five bounded percentile ranks over 1,000,000 unique longs; assert exact extrema and at most 1% interior rank error |
| `workloads/exact_percentiles` | The explicit sample-storing exact extension over the same five ranks and rows; assert exact nearest ranks |
| `workloads/tdigest_rollup` | Build 32 weighted-distribution sketches over 1,000,000 rows, merge and read five ranks; assert extrema and rank error |
| `workloads/percentile_groups` | Medians over 100,000 ten-value groups; assert every sparse median and exact group count |
| `materialize/memory_cold` | Plan/initialize a fresh one-million-row snapshot, read two filtered fork legs and aggregate; 64 MiB pool, disk disabled |
| `materialize/memory_warm` | Reset native operators and reuse the completed in-memory snapshot for the same forked aggregates |
| `materialize/spill_cold` | Plan/initialize the same snapshot under a 256 KiB pool, using native disk spill and fresh files each iteration |
| `materialize/spill_warm` | Replay the completed disk-backed snapshot through independent fork readers without reinitializing input |

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

## Measured 0.11 materialized snapshots

The snapshot workloads were measured on 2026-10-26 on the Apple M4/arm64 host,
Rust 1.95.0, DataFusion 55.2.0, the optimized bench profile and four target
partitions. No other local compilation ran during measurement:

```bash
cargo bench --locked --bench kql -j 2 -- '^materialize/' \
  --warm-up-time 2 --measurement-time 5 --sample-size 30
```

| Snapshot path | Criterion estimate | 95% confidence interval |
| --- | --- | --- |
| Cold in-memory initialization and forked aggregation | 1.99 ms | 1.82-2.20 ms |
| Warm in-memory replay and forked aggregation | 0.522 ms | 0.506-0.544 ms |
| Cold disk-backed initialization and forked aggregation | 21.34 ms | 19.23-23.30 ms |
| Warm disk-backed replay and forked aggregation | 2.33 ms | 2.26-2.41 ms |

The source contains 1,000,000 unique longs in independently allocated 8,192-row
Arrow batches. Each query uses two filtered fork legs and checks a total of
`499999500000` and count of `1000000`. The in-memory path uses a 64 MiB host pool
with disks disabled; the spill path uses a 256 KiB pool, native temporary IPC,
uncompressed encoding and a 64 MiB disk quota. Preflight asserts actual disk
usage and four live files for the spill path, none for the resident path, exact
results on initial and repeat reads, stable file usage during warm replay, and
complete reservation/file cleanup. The remaining benchmark preflights still
execute, including numeric SQL/KQL physical-plan equality.

**Cold** includes a fresh query's parsing, optimization, physical planning,
snapshot initialization, forked reads, aggregation and snapshot cleanup for
each iteration. **Warm** reuses the completed snapshot and cached physical plan
with native operator-state reset. Thus the difference is not an isolated
measurement of cache lookup overhead or a speed advantage over DataFusion.
Throughput counts original source rows once; each fork reads the cached source,
and the complementary predicates together return one million rows.

The source fixture itself is memory-resident, and warm spill reads can use the
OS page cache. These numbers do not represent remote storage or uncached
physical-disk latency. A 256 KiB host reservation limit is **not a 256 KiB
process-RAM claim**: source arrays, native IPC and downstream working buffers
have their separate memory requirements. Criterion extended the cold spill
sampling period beyond five seconds and reported timing outliers on the shared
host. Measure representative providers, value widths, compression and fan-out
before treating these results as capacity guidance.

## Measured 0.10 bounded percentile workloads

The distribution workloads were measured on 2026-10-26 on the Apple M4/arm64
host, Rust 1.95.0, DataFusion 55.2.0, optimized bench profile, and four target
partitions. Compilation and the other local checks completed before the final
measurement:

```bash
cargo bench --locked --bench kql -j 2 -- \
  'workloads/(bounded_percentiles|exact_percentiles|tdigest_rollup|percentile_groups)' \
  --warm-up-time 2 --measurement-time 8 --sample-size 30
```

| Benchmark | Criterion estimate | 95% confidence interval | Input rows/second |
| --- | --- | --- | --- |
| Five bounded percentiles, 1,000,000 unique longs | 12.11 ms | 11.90-12.30 ms | 82.6 million |
| Five explicit exact percentiles, same rows/ranks | 44.12 ms | 42.19-46.02 ms | 22.7 million |
| Build/merge 32 t-digests and read five ranks, 1,000,000 rows | 20.78 ms | 20.24-21.23 ms | 48.1 million |
| Medians over 100,000 ten-value groups | 226.48 ms | 221.42-232.17 ms | 4.42 million |

Ranks are 0, 50, 95, 99 and 100. Preflight asserts exact extrema and at most
1% interior **rank** error for bounded/rollup results; the exact extension must
return every precise nearest rank. The rollup includes actual dynamic sketch
serialization, native merge and accessor costs, not an average of precomputed
percentiles. Direct and rolled-up approximate values need not be identical.
The grouped fixture checks every sparse median and the exact group count.
All benchmark preflights still run, including the original numeric KQL/SQL
physical-plan equality assertion.

The bounded path is about 3.6 times faster than the sample-storing exact
extension on this fixture, with a different accuracy/memory contract. This is
not an execution-speed claim against DataFusion or a universal speed guarantee.
Exact-small states promote after 2,048 input entries; compressed states cap
centroids and pending input independently of row count. Local tests check
retained state bounds, native memory-pool charging and actual spill/replay.
The number of groups is still data-dependent, and the many-group result shows
that allocation/serialization costs remain meaningful scaling work.

An earlier sample overlapped local compilation and measured roughly 18/73/27 ms
for the first three workloads. The final table avoids that self-contention,
but the shared host still varied and the grouped run reported an outlier.
Criterion extended sampling when its time estimate exceeded the requested
eight seconds. These are memory-resident local measurements; the separate
small synthetic Kusto captures do not certify million-row accuracy or identical
service digests.

## Measured 0.9 interoperability workloads

The new Arrow/string paths at `8ba286f` were measured on 2026-10-25 on the same Apple
M4/arm64 host, Rust 1.95.0, DataFusion 55.2.0, optimized bench profile, and four
target partitions:

```bash
cargo bench --locked --bench kql -j 2 -- \
  'workloads/(arrow_expand|arrow_expand_sql|arrow_dynamic_expand|indexof|text_rewrite)' \
  --warm-up-time 1 --measurement-time 3 --sample-size 30
```

| Benchmark | Criterion estimate | 95% confidence interval |
| --- | --- | --- |
| Typed Arrow list expansion/sum, 100,000 lists | 1.10 ms | 1.09-1.12 ms |
| Native SQL Unnest/sum, same lists | 0.872 ms | 0.859-0.890 ms |
| Explicit dynamic conversion/expansion, same lists | 25.15 ms | 24.90-25.41 ms |
| Optional-index search, 100,000 strings | 1.75 ms | 1.74-1.77 ms |
| Repeat/regex rewrite/search, 100,000 strings | 35.59 ms | 33.37-37.88 ms |

Each list contains three longs; all three expansion plans assert the exact
600,000 total, so throughput counts original lists, not expanded rows. The
typed KQL path processes about 91 million lists/second on this fixture while
retaining checked collection bounds. The untyped/general dynamic route supports
broader mixed/rich data but is much more expensive. SQL is faster here, and these
three plans are not identical: do not attribute an execution advantage to KQL.

The string fixture contains four repeating messages and asserts a 300,000
index-position sum plus 100,000 successful rewrite checks. Regex compilation,
source construction and physical planning are outside the timed region.
Every benchmark preflight still executes, including the original numeric
KQL/SQL physical-plan equality assertion. Some runs reported timing outliers
on the shared host. These are local memory-resident workload measurements,
not production capacity guarantees or new live Kusto conformance evidence.
An earlier implementation sample measured 1.03 ms for typed expansion and
23.67 ms for the explicit dynamic path; the difference reinforces the shared-host
comparison limits. The final source also avoids repeatedly rescanning ASCII
classification during occurrence searches.

## Measured 0.8 bounded distinct-count workloads

The HLL workloads were measured on 2026-10-25 on the same Apple M4/arm64 host,
Rust 1.95.0, DataFusion 55.2.0, optimized bench profile, and four target
partitions:

```bash
cargo bench --locked --bench kql -j 2 -- \
  'workloads/(dcount_accuracy|dcount_native|dcount_groups|hll_rollup)' \
  --warm-up-time 1 --measurement-time 3 --sample-size 30
```

| Benchmark | Criterion estimate | 95% confidence interval | Input rows/second |
| --- | --- | --- | --- |
| Accuracy-2 distinct count, 1,000,000 unique longs | 1.93 ms | 1.90-1.97 ms | 517 million |
| Native DataFusion approximate distinct, same input | 0.487 ms | 0.482-0.496 ms | 2.05 billion |
| Accuracy-4 counts over 100,000 ten-value groups | 65.59 ms | 64.95-66.23 ms | 15.25 million |
| Build/merge 32 accuracy-2 sketches, 1,000,000 rows | 41.24 ms | 40.03-42.53 ms | 24.25 million |

The direct and rollup estimates must be identical. Both are checked against a
six-sigma error interval using 65,536 registers; the native baseline has its own
16,384-register error check. These are different precision/hashing/state paths,
not identical physical plans or an execution-speed advantage over DataFusion.
The native baseline is faster here. The ordinary numeric KQL/SQL benchmark still
requires identical native plans before execution.

The many-group fixture asserts exactly 100,000 groups and a one-million distinct
total. Each group stays sparse, avoiding a 256 KiB register allocation per key.
The rollup includes dynamic sketch serialization/decoding and merge costs; it is
not just adding precomputed counts. Separate regressions force native aggregation
to spill under a 2 MiB host pool and assert exact results plus nonzero spill
metrics after replay. That 0.8 milestone did not add snapshot spilling; 0.11 adds
the separate materialized-snapshot path.

The shared host varied materially between runs, including for unchanged native
DataFusion code. The native baseline also reported timing outliers. These are
memory-resident local measurements, not universal throughput or statistical
accuracy guarantees. These million-row workloads have not been run against
Kusto; the later four small-set HLL reference captures are recorded separately.

## Measured 0.7 decimal128 workloads

The wider decimal128 runtime was measured on 2026-10-25 on the same Apple
M4/arm64 host, Rust 1.95.0, DataFusion 55.2.0, optimized bench profile, and four
target partitions:

```bash
cargo bench --locked --bench kql -j 2 -- \
  'workloads/(decimal_exact|rich_nested)' \
  --warm-up-time 1 --measurement-time 3 --sample-size 30
```

| Benchmark | Criterion estimate | 95% confidence interval | Input rows/second |
| --- | --- | --- | --- |
| Exact decimal parse/add/sum, 100,000 rows | 8.90 ms | 8.71-9.14 ms | 11.24 million |
| Rich nested pack/access/aggregate, 10,000 rows | 94.99 ms | 87.22-102.75 ms | 105,300 |

The same exact totals/type checks and numeric SQL/KQL plan-equivalence assertion
ran as preflight. Decimal execution measured faster than the bounded 0.6 profile
(10.42 ms); the rich-value intervals overlap and Criterion detected no significant
change. This is broader arithmetic semantics without a demonstrated throughput
regression on these fixtures, not proof that every decimal query is faster.
Rich nested processing remains substantially more expensive than primitive
operations. Both runs reported timing outliers on the shared host.

## Measured 0.6 type and telemetry workloads

These workloads were measured on 2026-10-25 on the same Apple M4/arm64 host,
Rust 1.95.0, DataFusion 55.2.0, optimized bench profile, and four target
partitions:

```bash
cargo bench --locked --bench kql -j 2 -- \
  'workloads/(decimal_exact|rich_nested|term_set|ip_ranges|encoding)' \
  --warm-up-time 1 --measurement-time 3 --sample-size 30
```

| Benchmark | Criterion estimate | 95% confidence interval | Input rows/second |
| --- | --- | --- | --- |
| Exact decimal parse/add/sum, 100,000 rows | 10.42 ms | 10.30-10.57 ms | 9.60 million |
| Rich nested pack/access/aggregate, 10,000 rows | 95.49 ms | 93.10-98.01 ms | 104,700 |
| Compiled term-set matching, 100,000 rows | 6.19 ms | 6.02-6.36 ms | 16.1 million |
| Merged CIDR membership, 100,000 rows | 2.83 ms | 2.77-2.90 ms | 35.3 million |
| UTF-8 base64 round trips, 100,000 rows | 2.71 ms | 2.42-3.03 ms | 36.9 million |

The decimal/rich rows above were remeasured after removing repeated scalar
expansion/conversion and replacing linear decimal trailing-zero normalization
with a bounded binary search over cached powers of ten. At the original
0.6 implementation (`85b8c34`), the same fixtures measured 108.05 ms
(106.23-109.87 ms) and 143.53 ms (136.80-151.33 ms), respectively. These
shared-host runs indicate about 10.4 times faster decimal execution and
1.5 times faster rich nested execution for these fixtures, not universal
speedups or a controlled release-to-release comparison. The term/IP
measurements use the original implementation; these decimal changes do not
affect their kernels.

The encoding row was remeasured at `f43cbba`, after the reference-backed
base64 corrections also removed an unnecessary per-value compacting buffer.
The original fixture at `85b8c34` measured 6.09 ms (6.00-6.18 ms), so the
new estimate is about 2.25 times faster. All workload assertions and the
numeric SQL/KQL plan-equivalence check ran during the measurement. The
30-sample run reported three timing outliers, reflected in its wider interval;
the same shared-host and workload-specific comparison limits apply.

All five workloads assert their actual results before timing. The decimal
total is exactly `450359962737049692500`, beyond binary64 integer precision.
The rich fixture constructs a source-dependent bag on every row and verifies
an exact `12500` amount total plus 10,000 preserved timespan tags; it cannot
be replaced with a constant bag by the optimizer. It remains substantially
more expensive than ordinary primitive operations. These measurements cover
the current bounded decimal implementation, not full reference-engine
decimal range/precision; see the compatibility contract.

Term and CIDR matchers are warm: expression-local caches survive
`reset_plan_states`. The fixtures contain four repeating messages/addresses,
two repeating decimal strings, and small final aggregates, not unbounded
dictionary/cardinality growth. They do not measure index-assisted search,
persistent I/O, high-cardinality grouping, or arbitrary rich documents.
No result implies an execution advantage over an equivalent native DataFusion
plan. The numeric SQL/KQL physical-plan equality assertion still runs, and
all workload result assertions execute even when the timing filter selects
only some benchmarks.

## Measured 0.5 GUID workload

At commit `32a4944`, parsing 100,000 valid GUID strings and performing exact
distinct counting measured **1.14 ms** (95% interval **1.12-1.16 ms**), about
87.5 million input rows/second on the same Apple M4/Rust 1.95/DataFusion 55.2
configuration with four target partitions:

```bash
cargo bench --locked --bench kql -- workloads/guid_parse \
  --warm-up-time 1 --measurement-time 3 --sample-size 30
```

The fixture repeats two valid canonical IDs and verifies exactly two distinct
128-bit values. It measures conversion and a small exact hash state, not
high-cardinality aggregation, UUID generation, persistent I/O, or arbitrary
malformed strings. The shared-host and workload-specific comparison limits
below apply.

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
It retains the full selected result and adds a cache boundary. Reduce rows and
columns inside it, and configure bounded memory and temporary disk budgets.
Snapshots that exceed their host-pool reservation spill through DataFusion's
native IPC machinery, respecting host directories, quotas and compression.
Replays stream at the configured batch-row size, but wide values and the native
IPC/source/downstream working sets still require memory. Resident snapshots are
not subsequently evicted on unrelated query pressure. A planned query and its
clones reuse the snapshot, so a benchmark of cold materialization must create a
fresh query per iteration; resetting physical operator state alone deliberately
does not invalidate that query's snapshot.

The crate does not change process-wide compiler flags, allocate an unbounded plan
cache, force materialization, replace the host allocator, or override session
optimizer settings. Host applications remain in control of those decisions.
