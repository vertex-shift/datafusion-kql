# Native KQL frontend: design and delivery plan

## Research and architecture

KQL is a typed, tabular, pipe-oriented language. Its operators transform the
schema at each stage; name resolution must therefore happen against the current
pipeline, not the original input. It is not just alternate SQL syntax. Important
differences include case-sensitive identifiers, case-insensitive string search,
term-based `has`, typed nulls, non-null strings, default descending sorts,
`innerunique` joins, and aggregate output names/defaults.

Apache DataFusion 55.2.0 provides the appropriate extension boundary:
`LogicalPlanBuilder` and `Expr`. A language frontend can create ordinary plans
and return a `DataFrame` without modifying DataFusion or calling its SQL parser.
Those plans participate in normal analysis, predicate/projection pushdown,
constant folding, join selection, partitioned aggregation, and physical
optimization.

```text
KQL text -> borrowed lexer tokens -> typed AST -> schema-aware planner
                                                    |
                                       DataFusion LogicalPlan / Expr
                                                    |
                            DataFusion analyzer and logical optimizer
                                                    |
                              native vectorized Arrow execution
```

There is no row-oriented query interpreter, SQL string generation, custom
execution engine, or implicit query execution during planning. Dynamic/JSON
operations use expression-local Arrow batch kernels without installing global
functions. Dynamic values use compact dense unions; nulls and strings retain
distinct types. Zipped expansion builds one list of struct rows and uses native
DataFusion `Unnest`, avoiding both Cartesian expansion and null-index padding
of union arrays. Existing
`TableProvider`s, catalogs, runtime configuration, memory limits, object stores,
and user-defined functions remain owned by the embedding DataFusion application.
SQL is a development dependency for differential testing, not a runtime
dependency of this frontend.

## Delivery sequence

1. Implement a span-aware lexer, bounded parser, public AST, and diagnostics.
2. Lower typed tabular KQL, scalar expressions, `let` bindings, aggregations,
   joins, and unions into native DataFusion plans.
3. Verify language semantics independently of SQL where KQL differs. Compare
   equivalent SQL results and plans where the semantics coincide. Verify
   optimizer pushdown, lazy execution, custom providers/functions, and Parquet.
4. Deliver runnable embedding examples, a compatibility contract, automated CI,
   and reproducible parse/planning/execution benchmarks.

The first release is a documented compatibility subset, not a claim to implement
the entire Kusto service or every KQL operator. Unsupported features must return
errors rather than silently execute with different semantics.

## Performance strategy

- Borrow ordinary token text while lexing; decode escaped strings only when
  necessary and build an owned, reusable AST.
- Keep ordinary predicates, arithmetic, sorting, and aggregations in native
  DataFusion expressions so optimizer rules can inspect them.
- Use native string/regex kernels; implement KQL-specific dynamic semantics in
  batch kernels while retaining standard DataFusion operators around them.
- Reuse Arrow buffers and existing table providers; do not collect input data in
  the frontend.
- Keep limits lazy; let DataFusion select top-k, hash joins, and partial/final
  aggregates.
- Deduplicate equivalent aggregate calls before planning to share native
  accumulator states, independent of their requested output aliases.
- Represent tabular `let` bindings as plans, not eagerly collected tables.
- Make parsing reusable separately from schema-aware planning. Do not introduce
  a global plan cache with stale schemas, sessions, or `now()` values.
- Measure parser overhead, complete planning overhead against SQL, and execution
  against equivalent SQL on the same data and session configuration. Report
  measurements, not unverified speed claims or universal performance guarantees.

## Later compatibility milestones

- Dynamic JSON values, property access, bag/array/set functions, and `mv-expand`
  are implemented, together with bounded collection aggregation, nearest-rank
  percentiles, correlated row-selection aggregates, and aggregate arithmetic.
  Typed `parse`/`parse-where`, regex extraction and JSONPath extraction are also
  implemented. `mv-apply` compiles reusable native subquery templates with
  per-row Arrow input binding, specializing common typed-array top/aggregate
  transformations into batch Arrow kernels. Continue with broader specialized
  parsing.
- Serialization, offsets, row numbering, restart-aware cumulative sums/ranks,
  and bounded session windows are implemented. `make-series`, common series
  arithmetic/interpolation/statistics, line fitting, and FIR/IIR processing are
  also implemented. `scan` uses a bounded native state machine with compiled
  scalar expressions, and `partition` uses vectorized window/aggregate fast
  paths or reusable native subquery templates. Continue with advanced signal
  processing and wider conformance fixtures.
- UTC calendar arithmetic, constructors, formatting, Unix conversions, and lazy
  numeric/datetime/timespan ranges are implemented. Full Kusto datetime/timespan
  range and richer dynamic scalar types remain compatibility milestones.
- Native 128-bit GUIDs, nullable conversion, UUID-v4 generation, typed query
  integration, and root dynamic GUID tags are implemented. Existing dynamic
  tag numbers and older dense-union inputs remain compatible. Decimal
  values now use checked coefficient/scale arithmetic with lossless native
  decimal storage. Rich nested non-JSON values use appended typed collection
  tags and a bounded nonrecursive wire representation; ordinary JSON retains
  its lightweight path. Complete reference certification and full temporal
  ranges remain milestones.
- Lookup, schema introspection, projection reordering, sampling, wildcard table
  discovery, source-labelled unions and conflicting-type schema splitting are
  implemented. Cross-table search/find, smart row packing, hierarchical
  top-nested (including reaggregated others), and exact heavy-hitter estimates
  are implemented. Continue with Kusto-specific approximate algorithms and hints.
- Explicit-schema bag_unpack and pivot compile to batch struct extraction and
  native conditional aggregation. Runtime pivot schema mismatches are explicit
  errors; implicit data-dependent schema discovery and additional service
  plugins remain compatibility milestones.
- Typed request parameters and lexically captured scalar/tabular functions with
  `invoke` are implemented through native expression/plan expansion. Continue
  with wider constant-argument contexts. Lazy scalar subqueries, lexical
  scalar-query functions, runtime native range bounds, tabular membership,
  and wildcard-discovered lexical views are implemented.
- Explicit materialization uses a lazy native table source and a shared Arrow
  cache boundary. It preserves parallel input execution, reports retained
  memory to the host pool, and does not change the host's planner or registry.
  Snapshots are scoped to planned queries, including DataFrame clones.
- Management commands, remote Kusto connectors, visualization metadata,
  CLI/Python bindings, and language tooling.
- Conformance fixtures against a real Kusto cluster and production workload
  benchmarks before claiming complete compatibility.

## Primary references

- [DataFusion architecture and language extension points](https://docs.rs/datafusion/55.2.0/datafusion/)
- [Building logical plans](https://datafusion.apache.org/library-user-guide/building-logical-plans.html)
- [KQL query language](https://learn.microsoft.com/en-us/kusto/query/)
- [KQL null semantics](https://learn.microsoft.com/en-us/kusto/query/scalar-data-types/null-values)
- [KQL string operators](https://learn.microsoft.com/en-us/kusto/query/datatypes-string-operators)
- [KQL summarize semantics](https://learn.microsoft.com/en-us/kusto/query/summarize-operator)
- [KQL sort defaults](https://learn.microsoft.com/en-us/kusto/query/sort-operator)
