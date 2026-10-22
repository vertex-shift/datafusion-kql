# KQL compatibility contract

This is a native frontend for **DataFusion 55.2.x**, not a Kusto server. Version
0.1 implements the typed tabular and dynamic subset below. Keywords, function names, table
names, and column names are case-sensitive. Unsupported syntax/functions return
an error; there is no SQL fallback.

## Sources and bindings

| Construct | Supported behavior |
| --- | --- |
| Tables | Existing session table providers; one-, two-, and three-component catalog paths |
| Escaped identifiers | `['column name']`, including literal dots |
| `let` | Scalar expressions, tabular plans, aliases, nested lexical scopes |
| Parenthesized queries | Nested pipelines and scoped `let` statements |
| `print` | One row of scalar expressions |
| `datatable` | Explicit `bool`, `int`, `long`, `real`/`double`, `string`, `datetime`, `timespan`, `dynamic` columns |
| `range` | Inclusive, lazy native `generate_series`; non-null `long` bounds/step, including descending ranges |
| `union` | Source form and pipe form |

Tabular bindings are inlined plans, **not materialized results**. Reusing one can
rescan its inputs. Scalar bindings retain expressions, matching KQL's binding
rather than assignment model. Use a new pipe to reference a newly extended
column; assignments in a single `extend` resolve against its input.

## Tabular operators

| Operators | Notes |
| --- | --- |
| `where`, `filter` | Native boolean expressions; null predicates drop rows |
| `project`, `extend` | Named expressions, column selection, `*`; replacement columns move to the end with `extend` |
| `project-away`, `project-keep` | Column names and `*` patterns; preserve input column order |
| `project-rename` | `New=Old`; preserve input column order |
| `summarize` | Named aggregate calls and arbitrary scalar grouping expressions; group-only form |
| `sort`, `order` | Default **descending/nulls last**; ascending defaults to nulls first; explicit null placement |
| `top` | Literal nonnegative count, one sort expression; default descending |
| `take`, `limit` | Literal nonnegative counts; no ordering guarantee without a sort |
| `distinct` | Scalar expressions or `*` |
| `count` | One `Count:long` result column, including zero for empty input |
| `join` | Default `innerunique`; explicit `inner`, `leftouter`, `rightouter`, `fullouter`, left/right semi and anti |
| `union` | Default outer; inner intersection; align by name and preserve duplicate rows |
| `mv-expand`, `mvexpand` | Arrays and bags, zipped multiple columns, per-row limits, `with_itemindex`, cast-only `to typeof(...)` |
| `parse`, `parse-where` | Simple, regex, and relaxed patterns; typed captures, regex flags, and strict failure filtering |

Joins accept a table or parenthesized query on the right. Keys can be `on Key`
or `$left.Key == $right.OtherKey`, with comma/`and` conjunctions. The default
`innerunique` deduplicates the left by its keys using native `DistinctOn`.
Selection among duplicate left rows is unspecified. Null join keys do not match.
Both key columns remain in ordinary join outputs; conflicting right names get
numeric suffixes. Suffix selection avoids overwriting other output columns.

Union's conflicting-type schema splitting is **not implemented**: the same name
with different non-string Arrow types errors instead of silently coercing data.
Use explicit conversions before union. UTF-8 Arrow string representations align
to `Utf8`. Missing strings become empty strings; other missing values are typed
nulls.

## Expressions and functions

- Literals: signed 64-bit integers, floating point, booleans, single/double quoted
  strings, backslash escapes, verbatim `@` strings, typed null constructors,
  `datetime(...)`, and `timespan(...)`.
- Timespans: `d`, `h`, `m`, `s`, `ms`, `us`, `ns`, `tick`/`ticks`,
  `microsecond(s)`, decimal quantities, and `[d.]hh:mm:ss[.fraction]`.
  Bare numbers inside `timespan(...)` represent days. Conversion uses integer
  arithmetic, not floating point.
- Arithmetic `+ - * / %`, comparisons `== != < <= > >=`, `and`, `or`, unary
  `!`, `not()`, unary signs, and parentheses.
- Inclusive `between`/`!between`, including a timespan upper offset for datetimes.
- Scalar-constant-list `in`, `!in`, `in~`, `!in~`. Case-insensitive list membership
  folds ASCII characters only.
- `contains`, `startswith`, `endswith` and negations are case-insensitive;
  `_cs` variants are case-sensitive. Substrings are literal, not SQL LIKE
  patterns. `has`/`has_cs` use Unicode alphanumeric term boundaries and a
  constant single alphanumeric right-hand term. Multi-term/punctuated needles
  are rejected. `matches regex` uses DataFusion's Rust-regex
  dialect, not every .NET regex construct supported by Kusto.
- `now([offset])`, `ago(offset)`: one consistent instant per planning call,
  folded into timestamp literals for predicate pushdown.
- `bin(value,size)` / two-argument `floor`: numeric, timespan, and timestamp
  binning; negative numbers round down and nonpositive sizes produce null.
  Integer binning preserves precision beyond `2^53`; timestamp and timespan bin
  sizes must be scalar constants.
- `iff`/`iif`, `case`, `isnull`, `isnotnull`, `isempty`, `isnotempty`, `coalesce`.
  String coalesce skips empty strings.
- `tostring`, `tolong`, `toint`, `toreal`/`todouble`, `tobool`, `todatetime`,
  scalar type constructors; failed conversions use Arrow `TRY_CAST` semantics.
- `strcat`, `strlen`, `tolower`, `toupper`, `substring` (zero-based),
  `replace_string`, `extract`, `extract_all`/`extractall`, `extract_json`/`extractjson`,
  `totimespan`/`totime`, and `todate`.
- Other names already registered in the session scalar registry are callable
  with **their DataFusion-defined semantics**. This is an extension point, not
  a promise that every SQL function has Kusto semantics. The frontend does not
  install or overwrite session functions.

## Dynamic values and collection expansion

`dynamic(...)` supports nested arrays and property bags with single- or
double-quoted strings, scalar literals, and nulls. Dynamic values are real
Arrow **dense unions**, distinguishing null, bool, long, real, string, array,
bag, datetime, and timespan. Arrays and bags use JSON-marked UTF-8 child buffers;
they are not confused with ordinary string columns or normalized to empty
strings. Nested non-JSON scalar types serialize as strings inside collections.

`parse_json`/`todynamic`, dotted properties, arbitrary string-key subscripts,
zero-based and negative array indices, `gettype`, and scalar conversion
functions are implemented. As in KQL, malformed JSON deliberately becomes a
dynamic string holding the original text. Missing properties/indices produce
dynamic nulls. `dynamic(...)` itself must be a constant expression; use
`parse_json`, `pack_array`, or `bag_pack` for runtime values.

Functions: `array_length`, `pack_array`, `bag_pack`/`pack`/`pack_dictionary`,
`bag_keys`, `bag_has_key`, `bag_merge` (leftmost wins), root-key `bag_set_key`
and `bag_remove_keys`, `array_concat`, `array_slice` (inclusive end),
`array_index_of`, `set_has_element`, `array_reverse`, `array_sum`, `set_union`,
`set_intersect`, `set_difference`, `zip`, `repeat`, integer `range()`,
`strcat_array`, and `split`. Array-producing builders and expansion reject
collections exceeding 1,048,576 elements instead of allocating without a bound.
JSONPath writes and noninteger scalar `range()` bounds are not yet implemented.

`mv-expand` lowers to one batch kernel producing zipped struct rows and one
native DataFusion `Unnest`. Each source is expanded in parallel, shorter
collections are padded with real dynamic nulls, and other columns are retained.
Null produces one row; empty arrays/bags produce none. Bags support default
single-entry bag expansion and `kind=array` key/value pairs. `with_itemindex`
is zero-based. The default per-row limit is 2,147,483,647 (legacy `mvexpand`:
128), subject to the collection resource budget.

`to typeof(...)` checks underlying dynamic types and does not parse strings into
numbers. For example, `"2"` becomes null under `to typeof(long)`; use `tolong`
after an untyped expansion to request conversion.

## Parsing and extraction

`extract(regex, group, source [, typeof(type)])` uses native regex and array
expressions. Group 0 is the complete match; capture indices can vary by row.
Missing/out-of-range captures produce empty strings or typed nulls.
`extract_all` compiles its constant pattern once, supports numbered/named group
selection, returns one- or two-dimensional dynamic arrays, and returns dynamic
null for no matches. It requires 1-16 capture groups and enforces the collection
budget.

`extract_json(path, source [, typeof(type)])` defaults to **string**, as in
Kusto's published language signatures. Explicit `typeof(dynamic)` retains
dynamic values. JSONPath supports the root, dotted keys, quoted-key subscripts,
and literal array indices; recursive descent and wildcard selectors error.
Malformed JSON produces empty strings or typed nulls, not a retained invalid
string as with `parse_json`.

`parse` performs one compiled regex match per input row to obtain all captures,
then uses native nullable conversions and projections. Simple/regex modes
invalidate every capture when a required type fails; relaxed mode retains
other captures. `parse-where` drops unsuccessful rows. Delimiters, leading/trailing
wildcards, captures, optional scalar types, and regex flags are supported.
Captures must be separated by delimiters; a wildcard cannot immediately follow
a string capture. Dynamic capture types are not supported; extract a string and
apply `parse_json`. Patterns are bounded to 256 segments.

Nullable datetime conversion accepts ISO-8601 and common US log timestamps.
Timespan conversion accepts the same unit/day-clock formats as literals.
Regex execution uses Rust regex syntax, not unsupported .NET backreferences or
look-around.

## Aggregations

`count`, `countif`, `sum`, `sumif`, `avg`, `avgif`, `min`, `minif`, `max`,
`maxif`, `dcount`, `dcountif`, `count_distinct`/`count_distinctif`, sample/population
standard deviation and variance (including conditional variants) are lowered
to native aggregates. Registered DataFusion aggregate functions are also
callable in `summarize`.

Timespan `sum`, `sumif`, `avg`, and `avgif` preserve duration output types.
Other input overloads follow DataFusion's supported Arrow types.

`count()` counts rows, including rows with nulls; `count(column)` also counts all
rows. Native filters implement conditional aggregates. Counts are signed
`long`. Sum, sample variance, and sample standard deviation default to zero for
empty/all-null inputs. Other numeric aggregates use DataFusion defaults; notably
empty `avg` is null (some Kusto workloads expose NaN here). Group-less summarize
returns a row on empty input; grouped summarize returns none.

Standard aggregate names such as `count_`, `sum_Value`, and grouping
`bin(Timestamp,1h)` -> `Timestamp` are generated. Repeated generated names receive
numeric suffixes. Use explicit aliases when output names are part of an API.
Scalar expressions over aggregate calls are supported directly, including
arithmetic, conditionals, casts, and grouping keys. Equivalent aggregate calls
share native accumulator state.

`make_list`, `make_list_if`, `make_list_with_nulls`, `make_set`, `make_set_if`,
`make_bag`, and `make_bag_if` return dynamic collections. Unordered collection
accumulators enforce `maxSize` while accumulating/merging, report memory usage to
DataFusion, and support partial/final aggregation. Ordered lists use native
order-sensitive `array_agg`, preserving ordering across partitions before
applying the output limit. That ordered path can buffer more than `maxSize`;
configure the host's memory budget. Keep sort keys available until summarize.
Empty inputs return `[]` or `{}`. Null list/set inputs are ignored unless
`make_list_with_nulls` is used. Legacy `makelist`/`makeset` default to 128 items;
modern functions default to, and are capped at, 1,048,576 items.

`take_any`, `take_anyif`, `arg_max`, and `arg_min` support row selection.
Multi-column and wildcard selections use one native struct-valued aggregate,
so returned columns come from the same row. Ties are unspecified.

`percentile`, `percentiles`, `percentiles_array` and weighted variants
`percentilew`, `percentilesw`, `percentilesw_array` implement nearest-rank
selection, not linear interpolation. Multiple requested percentiles share one
state. Integer samples preserve precision beyond `2^53`; zero weights are
ignored and negative weights fail conversion. The implementation currently
stores samples with memory accounting and performs exact rank selection rather
than using Kusto's approximate t-digest. It therefore uses more memory for large
groups. Ranks must be constant and within 0-100; at most 1024 ranks are allowed.
Multi-column percentiles use generated output names; rename them afterward.

`dcount` uses DataFusion's approximate-distinct algorithm, **not Kusto's
accuracy-configurable HLL implementation**. Accuracy arguments are rejected.
Use `count_distinct` for exact counts. Dynamic distinct counts use canonical JSON
representations and continue to ignore nulls.

## Type and execution boundaries

- KQL's nullable equality differs from SQL: one-null equality is false,
  one-null inequality is true, and two nulls produce `bool(null)`. Lowering
  preserves this in projected expressions, filters, negation, and membership.
- Strings cannot be null in KQL. Nullable Arrow strings from tables and outer
  joins are normalized with native `coalesce`, without mutating the underlying
  provider or changing SQL queries in the same session. This can add work and
  restrict string-predicate pushdown for nullable source fields. Non-null source
  schemas avoid that cost. UTF-8, large UTF-8, UTF-8 views, and dictionary-encoded
  strings are accepted; dictionary strings are decoded with a native cast.
- `datetime` and `timespan` literals use Arrow signed 64-bit **nanoseconds**.
  This does not cover Kusto's full year 0001–9999 datetime range or its full
  timespan range; out-of-range literals error rather than wrap.
- Numeric overflow, Unicode casing/collation, casts, regex dialect, floating-point
  NaN/infinity ordering, and approximate aggregation can differ from the Kusto
  service. No string term index is installed: `has` scans Arrow batches using
  native regex kernels, rather than promising Kusto's indexed search complexity.
- Only one final tabular result is supported. Management commands,
  parameter declarations, user-defined KQL functions, `materialize`, `render`,
  hints, `make-series`, window/serialization operators, and `mv-apply` are future
  work. See [the delivery plan](architecture.md).
- Default parsing limits: 1 MiB of query text, nesting/expression depth 64,
  and 128 operators per pipeline. `parse_with_options` permits changing these
  within the parser's supported nesting bounds. Expanded expression and plan
  budgets also reject pathological `let` expansion. Input datasets are not
  capped by these frontend limits; configure DataFusion runtime memory/spilling
  and application-level query timeouts separately.
