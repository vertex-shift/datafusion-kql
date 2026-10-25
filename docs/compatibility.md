# KQL compatibility contract

This is a native frontend for **DataFusion 55.2.x**, not a Kusto server. Version
0.5 implements the typed tabular and dynamic subset below. Keywords, function
names, table names, and column names are case-sensitive. Unsupported
syntax/functions return an error; there is no SQL fallback.

## Sources and bindings

| Construct | Supported behavior |
| --- | --- |
| Tables | Existing session table providers; one-, two-, and three-component catalog paths |
| Table patterns | `*` wildcards in the selected catalog/schema; discovery is limited to 1024 matching providers |
| Escaped identifiers | `['column name']`, including literal dots |
| `let` | Scalar expressions, tabular plans, aliases, nested lexical scopes |
| Parenthesized queries | Nested pipelines and scoped `let` statements |
| `print` | One row of scalar expressions |
| `datatable` | Explicit `bool`, `int`, `long`, `real`/`double`, `string`, `datetime`, `timespan`, `guid`/`uuid`/`uniqueid`, `dynamic` columns |
| `range` | Inclusive lazy native `generate_series`; integer, real, datetime, and timespan sequences, including descending ranges |
| `union` | Source form and pipe form |
| `materialize(...)` | Lazy shared Arrow snapshot, usable directly or in a tabular binding |

Ordinary tabular bindings are inlined plans, **not materialized results**.
Reusing one can rescan its inputs; use explicit `materialize` when needed.
Scalar bindings retain expressions, matching KQL's binding
rather than assignment model. Use a new pipe to reference a newly extended
column; assignments in a single `extend` resolve against its input.

## Parameters and query-defined functions

`declare query_parameters(name:type [= literal], ...);` binds values from the
`QueryParameters` map passed to `kql_with_parameters`,
`kql_ast_with_parameters`, or `logical_plan_with_parameters`. Values are native
typed `ScalarValue`s, not strings interpolated into query text. Missing required
parameters and incompatible types error. Literal defaults and typed nulls are
supported; dynamic defaults are rejected. Undeclared request values do not
become variables. The same parsed AST can be reused with different request maps.

`let F=(parameters){ ... };` defines lexically scoped scalar/tabular functions.
Scalar functions inline expressions before aggregate/window planning, retaining
native optimization and function-registry isolation. Local scalar bindings and
nested scalar functions capture definition-time lexical values, not caller
columns with matching names. Zero-argument functions support bare-name syntax.
Default values must be literals; named arguments may omit/reorder optional
parameters. Required parameters precede optional parameters, and tabular
parameters precede scalar parameters.

Tabular parameters accept explicit column schemas or `(*)`. Explicit schemas
check/project the declared columns. Wildcard inputs retain their actual schema.
Tabular calls accept names or parenthesized queries, and `invoke F(...)` supplies
the input as the first tabular argument. Tabular-function scalar arguments must
be row-independent constants. Scalar/table result contexts, missing/duplicate
arguments, and recursion/forward references fail explicitly. Functions have at
most 128 parameters; scalar expansion has depth/node budgets.

`let V=view (){ ... };` declares a parameterless tabular view. Views participate
in wildcard source/union discovery, shadow providers with the same name, and
retain lexical captures. Ordinary functions do not participate in wildcards.

Scalar functions containing tabular local statements and scalar subqueries are
planned in their captured lexical scope. Their arguments must be row-independent;
passing a caller column into such a function fails rather than turning the
subquery into a per-row scan. Nested helper functions and default/named arguments
retain those rules. Kusto's static opaque-schema restrictions for `(*)`
parameters remain compatibility work.

## Scalar subqueries

`toscalar(expression)` freezes a row-independent scalar expression or the first
row/first column of a tabular query. Native scalar-subquery plans and lazy
materialization keep query construction nonexecuting and share evaluated values
across uses. Empty tabular inputs yield typed nulls (empty strings for strings).
Nested subqueries, scalar bindings, registered functions, and request parameters
are supported. Caller-row correlation is rejected, rather than being evaluated
once per row with different semantics.

Table-valued function calls and bare table bindings can be used as arguments.
Scalar subqueries can supply tabular range bounds/steps. The range evaluates its
scalar arguments at execution time, then streams the ordinary native
`generate_series` provider; planning remains lazy, even for very large ranges
followed by `take`. Null/zero/invalid runtime arguments fail explicitly.
Other frontend-constant arguments, such as collection limits and format
patterns, still require values that can be folded without executing a subquery.

`in`, `!in`, `in~`, and `!in~` accept a tabular query, named table, or tabular
function and compare against its first column. Native mark joins implement
membership without row-by-row table scans. Null nonmatches are false for
nonnull left values, while a null left value remains null for a nonempty set
and false for an empty set; negation preserves those distinctions. Nullable
left inputs share a memory-accounted RHS snapshot for the existence/emptiness
checks. Case-insensitive variants fold ASCII only. Dynamic operands must first
be cast to scalar values; nested dynamic flattening for tabular membership is
not implemented.

## Tabular operators

| Operators | Notes |
| --- | --- |
| `where`, `filter` | Native boolean expressions; null predicates drop rows |
| `project`, `extend` | Named expressions, column selection, `*`; replacement columns move to the end with `extend` |
| `project-away`, `project-keep` | Column names and `*` patterns; preserve input column order |
| `project-rename` | `New=Old`; preserve input column order |
| `project-reorder` | Explicit columns, wildcard ordering, and natural `granny-asc`/`granny-desc` ordering |
| `summarize` | Named aggregate calls and arbitrary scalar grouping expressions; group-only form |
| `sort`, `order` | Default **descending/nulls last**; ascending defaults to nulls first; explicit null placement |
| `top` | Constant integer expression for count, one sort expression; default descending |
| `top-hitters` | Native exact count/sum top-k estimates; preserves KQL output names but not Count-Min-Sketch implementation |
| `top-nested` | Hierarchical per-parent top selection, ancestor aggregates, optional counts, aliases, and others buckets |
| `take`, `limit` | Constant integer expressions; no ordering guarantee without a sort |
| `sample`, `sample-distinct` | Native random top-k sampling; distinct sampling returns unique projected values |
| `as` | Lazy intermediate binding, or a shared snapshot with `hint.materialized=true` |
| `invoke` | Apply a query-defined tabular function with the input as its first argument |
| `getschema` | KQL schema rows, excluding hidden helpers; does not execute the input |
| `distinct` | Scalar expressions or `*` |
| `count` | One `Count:long` result column, including zero for empty input |
| `join` | Default `innerunique`; explicit `inner`, `leftouter`, `rightouter`, `fullouter`, left/right semi and anti |
| `lookup` | Default leftouter or explicit inner, preserves left duplicates, suppresses right lookup keys |
| `union` | Name/type alignment, conflicting-type suffixes, inner intersection, wildcard legs, `withsource` |
| `mv-expand`, `mvexpand` | Arrays and bags, zipped multiple columns, per-row limits, `with_itemindex`, cast-only `to typeof(...)` |
| `mv-apply` | Per-input-row native subqueries over zipped arrays, indices, type-only casts, and limits |
| `partition` | Implicit-source per-key transformations with legacy/native/shuffle strategies |
| `search`, `find` | Cross-table/wildcard text predicates, source labels, smart packing, explicit find projection |
| `evaluate` | Explicit-schema `bag_unpack` and `pivot`, without replacing the session registry |
| `scan` | Typed declared state, step conditions/promotion, simultaneous assignments, match IDs, and all/last/none output |
| `parse`, `parse-where` | Simple, regex, and relaxed patterns; typed captures, regex flags, and strict failure filtering |
| `serialize` | Freezes row order and supports calculated columns and native window expressions |
| `make-series` | Numeric/datetime/timespan axes, per-group bounds, defaults, nonempty results, and alternate range syntax |

Joins accept a table or parenthesized query on the right. Keys can be `on Key`
or `$left.Key == $right.OtherKey`, with comma/`and` conjunctions. The default
`innerunique` deduplicates the left by its keys using native `DistinctOn`.
Selection among duplicate left rows is unspecified. Null join keys do not match.
Both key columns remain in ordinary join outputs; conflicting right names get
numeric suffixes. Suffix selection avoids overwriting other output columns.

Union keeps distinct name/type pairs and suffixes conflicting types, such as
`x_long` and `x_string`, without coercing one into the other. Generated names
avoid overwriting actual columns. Inner unions retain only common name/type
pairs. UTF-8 representations align to `Utf8`; smaller signed integers and real
representations normalize losslessly to KQL scalar widths. Missing strings
become empty strings; other missing values are typed nulls.

`withsource` labels named and wildcard legs and uses the initial source name for
parenthesized pipelines. It errors on a conflicting requested output name.
Fuzzy/best-effort union resolution is not implemented because the DataFrame API
has no warning-result channel; missing providers are never silently ignored.

Lookup uses DataFusion's join selection rather than forcing Kusto's broadcast
strategy or its service-specific right-side size limit.

## Materialized snapshots

`materialize(T | ...)` and `as hint.materialized=true Name` create an opaque,
lazy native source boundary. Logical and physical planning do not collect input
rows. The first consumer executes the native input partitions in parallel;
other consumers await and reuse the same Arrow buffers. Consumer filters and
projections remain outside the snapshot, so different fork legs cannot cache
incompatible subsets. Put common reducing operators inside `materialize`.
Ordinary `let` and `as hint.materialized=false` remain nonmaterialized.

A snapshot belongs to the **planned query/DataFrame**, not the session or
process. Cloning the DataFrame, creating another physical plan from it, or
collecting it again reuses that snapshot. Call `kql`/`kql_ast` again to obtain a
fresh snapshot, just as a new planning call obtains a fresh `now()` instant.
This explicitly defines repeated-DataFrame execution, an API concept without
a direct Kusto request equivalent.

Retained buffers and batch bookkeeping use a native DataFusion
`MemoryConsumer`. Exhausting the host memory pool fails the query; input errors
are retained and surfaced to all consumers rather than converted to empty data.
Dropping the snapshot releases its memory reservation. Configure a bounded
host pool for production: the extension does not install Kusto's cluster-wide
5 GB service cache limit or a separate allocator. Materialized buffers do not
spill. Unbounded input plans are rejected.

## Cross-table search and find

`search [kind=...] [in (...)] predicate` discovers registered providers and
lexical views, without scanning data while planning. Pipe-form search uses the
current input. Plain string terms search all visible columns, `Column:"term"`
restricts a term, and leading/trailing/internal asterisks implement suffix,
prefix, substring, or bounded wildcard matching. Boolean combinations, explicit
comparisons/regex predicates, source labels, and case modes are supported.
Search always supplies `$table`; consecutive searches preserve that label.
There is no inverted term index: these are native batch regex scans.

`find [withsource=Name] [in (...)] [where] predicate` defaults its source label
to `source_`. Smart projection retains explicit predicate columns and common
columns, packing other fields into dynamic `pack_`. Explicit `project
Column[:type], ... [, pack_all()]` fixes the output schema; its optional
`column1` bag contains the original record, not post-cast values. Missing
provider fields are handled explicitly, and conflicting projected types require
a declared type. Unknown columns/providers and output-name conflicts error.

Qualified DataFusion catalogs/schemas and provider wildcards are supported;
Kusto-service `cluster()`/`database()` discovery syntax is not. Predicate
coercion for a column with different input types follows each provider's native
DataFusion analysis rather than promising complete Kusto cross-type inference.

## Expressions and functions

- Literals: signed 64-bit integers, floating point, booleans, single/double quoted
  strings, backslash escapes, verbatim `@` strings, typed null constructors,
  `datetime(...)`, and `timespan(...)`.
- GUID literals (`guid(...)`, `uuid(...)`, `uniqueid(...)`) and typed nulls;
  nullable `toguid`, volatile `new_guid`, and canonical `tostring` formatting.
- Timespans: `d`, `h`, `m`, `s`, `ms`, `us`, `ns`, `tick`/`ticks`,
  `microsecond(s)`, decimal quantities, and `[d.]hh:mm:ss[.fraction]`.
  Bare numbers inside `timespan(...)` represent days. Conversion uses integer
  arithmetic, not floating point.
- Arithmetic `+ - * / %`, comparisons `== != < <= > >=`, `and`, `or`, unary
  `!`, `not()`, unary signs, and parentheses.
- Inclusive `between`/`!between`, including a timespan upper offset for datetimes.
- Scalar-constant-list and tabular-query `in`, `!in`, `in~`, `!in~`. Case-insensitive list membership
  folds ASCII characters only.
- `contains`, `startswith`, `endswith` and negations are case-insensitive;
  `_cs` variants are case-sensitive. Substrings are literal, not SQL LIKE
  patterns. `has`/`has_cs` use Unicode alphanumeric term boundaries and a
  constant single alphanumeric right-hand term. Multi-term/punctuated needles
  are rejected. `matches regex` uses DataFusion's Rust-regex
  dialect, not every .NET regex construct supported by Kusto.
- `hasprefix`, `hassuffix`, their `_cs` variants, and negations match literal
  term prefixes/suffixes with Unicode alphanumeric boundaries. `has_any` and
  `has_all` accept constant scalar/dynamic term lists and lazy tabular term
  sources; table sources use the first column. Compiled matcher caches retain
  at most one runtime term set. Distinct-term budgets are 10,000 and 256
  respectively, with a 1 MiB total term-text budget; excess input errors rather
  than silently truncating. These are batch scans, not an inverted index.
- `now([offset])`, `ago(offset)`: one consistent instant per planning call,
  folded into timestamp literals for predicate pushdown.
- `bin(value,size)` / two-argument `floor`: numeric, timespan, and timestamp
  binning; negative numbers round down and nonpositive sizes produce null.
  Integer binning preserves precision beyond `2^53`; timestamp and timespan bin
  sizes must be scalar constants.
- `iff`/`iif`, `case`, `isnull`, `isnotnull`, `isempty`, `isnotempty`, `coalesce`.
  String coalesce skips empty strings.
- Schema-aware `column_ifexists(name, fallback)` and `pack_all([ignore_null_empty])`;
  internal helper columns are not packed, and unused fallbacks are not resolved.
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
bag, datetime, timespan, guid, and decimal. Ordinary arrays/bags keep JSON-marked
UTF-8 child buffers. Collections containing richer scalars use appended typed
array/bag tags and a versioned, bounded internal encoding; old tag numbers and
older dense-union input schemas remain readable. They are not confused with
ordinary strings or with user JSON objects that resemble internal encodings.

Nested datetimes, timespans, GUIDs, decimals, real tags, and nonfinite reals
retain their types through property/index access, packing, collection updates,
set operations, expansion, materialization, aggregation/merge states, and
serialized row offsets. External JSON formatting still serializes non-JSON
scalar types as strings; explicitly reparsing that JSON does not recover the
lost type information. Internal typed encodings are never recognized from
ordinary user JSON. Typed collections enforce depth 128, per-collection element
limits, and a total-node budget; malformed references and reused child nodes
fail explicitly.

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
`set_intersect`, `set_difference`, `zip`, `repeat`, `range()`,
`strcat_array`, and `split`. Array-producing builders and expansion reject
collections exceeding 1,048,576 elements instead of allocating without a bound.
JSONPath writes are not yet implemented. Scalar `range()` supports integers,
reals, datetimes, and timespans and validates its collection budget before
allocating integral/temporal sequences.

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

## Native GUID values

GUIDs use `FixedSizeBinary(16)` in canonical RFC-4122 byte order. Such Arrow
fields are interpreted as KQL GUIDs; use `tostring` for lowercase hyphenated
display rather than Arrow's generic binary formatter. Typed datatables,
parameters/functions, equality/membership, joins/unions, distinct aggregates,
schema inspection, regex/JSON extraction, and `parse` captures support GUIDs.
Direct typed comparisons require GUID or null operands; strings must be
converted explicitly, not compared to the GUID's raw 16-byte storage.

`toguid` reads 32 hexadecimal digits, allowing optional nonrepeated hyphens
after digit positions 8, 12, 16, and 20, and ignores any remaining suffix as
documented by Kusto. Failed conversion yields a typed null. Literals require
the entire literal to be valid. `new_guid` embeds DataFusion's volatile UUID-v4
generator without overwriting or depending on a host registry override.
`toscalar(new_guid())` freezes a generated value for the planned query.

Dynamic GUID roots retain their own tag through conversion, null-padding-safe
row offsets, and scalar expansion. The GUID tag is appended after existing tags;
older dense-union input schemas remain readable and normalize during union.
GUIDs nested inside typed arrays/bags retain GUID tags; only explicit external
JSON serialization represents them as strings.

## IP address functions

`parse_ipv4`, `parse_ipv4_mask`, `format_ipv4`, `format_ipv4_mask`, and
`ipv4_netmask_suffix` support dotted-quad/CIDR inputs and explicit prefix masks.
Numeric formatting accepts unsigned 32-bit values in a KQL long. Comparisons
(`ipv4_compare`/`ipv4_is_match`, `ipv6_compare`/`ipv6_is_match`) combine the least
specific supplied prefixes. `parse_ipv6` produces eight zero-padded lowercase
hexadecimal groups and maps ordinary IPv4 inputs to IPv4-mapped IPv6 addresses.
IPv6 text uses IPv6 prefix lengths; bare IPv4 text uses IPv4 prefix lengths.

`ipv4_is_in_range`, `ipv4_is_in_any_range`, and their IPv6 counterparts accept
host/CIDR strings and range arrays. Overlapping intervals are merged once per
range set, with binary-search membership and a bounded one-entry cache for
row-varying sets. Range JSON is bounded to 1 MiB. Invalid addresses, masks, or
range entries yield nullable numeric/bool results; string formatters yield
empty strings. Invalid ranges are not silently discarded just because another
range matched. `ipv4_is_private` checks RFC-1918 ranges, not all reserved,
loopback, or link-local addresses.

## Decimal values

`decimal(...)` supports exact base-ten literals, scientific notation, `min`,
`max`, and typed nulls; `todecimal` performs nullable runtime conversion. Values
use a signed 96-bit coefficient and scale 0-28, consistent with the decimal
constant representation in Microsoft's public Kusto language frontend.
Arithmetic uses checked decimal operations, not an intermediate `f64`.
Overflow and division by zero produce typed decimal nulls.

The Arrow representation is `Decimal256(57,28)`. The wider native storage lets
one column retain both the full integer range and the smallest decimal fraction
without a per-row custom numeric object; it does not expand the accepted KQL
value range. Sorting, equality, hashing, joins, and grouped states remain native
DataFusion operations. Sum/average results normalize to the decimal range, with
single midpoint-to-even rounding when reducing excess native scale.
`tostring` uses normalized non-exponential formatting.

Typed datatables/parameters/functions, nullable parsing/extraction, numeric
arithmetic, exact binning/floor/ceiling, native decimal inputs, unions, root
dynamic tags, and serialized offsets are supported. Mixed decimal/real
arithmetic promotes to real. Complete decimal edge-case parity still requires
an authorized Kusto reference run; the documented local implementation and
source evidence are not represented as a completed live comparison.

## Grouped tabular subqueries

`mv-apply ... on (...)` expands each original row into an independent subtable,
runs a native DataFusion transformation, and restores unexpanded source columns
that the subquery did not return. Arrays zip with null padding. Original indices,
per-row expansion limits, type-only casts, empty/null/scalar inputs, per-row
top-k, aggregates, nested expansions, and independent serialized windows are
supported. Property bags stay whole until an explicit inner `mv-expand`;
`mv-apply` has no bag-expansion parameter.

`partition [hint.strategy=...] by Key (...)` runs an implicit-source
transformation for each key. Null keys form a group. Legacy mode (the default)
enforces the 64-key limit globally; explicit native/shuffle modes use native
hash repartitioning and spill-capable per-partition sorting, without that key
limit. Native/shuffle forms reject explicit-source operators. Legacy explicit
`{ query-with-source }` syntax and service-specific concurrency/spread hints are
not implemented.

Simple native/shuffle `top`, `count`, and `summarize` transformations compile to
one vectorized partitioned-window/grouped-aggregate plan, without instantiating
one plan per key. More general transformations compile their physical template
once, bind each subtable's Arrow batches to its native input, and reset native
operator state between groups. Groups stream independently across hash
partitions; retaining one group's input is charged to the host memory pool.
Large individual groups require enough memory; those retained batches do not
spill even though the upstream native sort can.

This general path does not reparse KQL or run a separate query interpreter, but
native-plan initialization has overhead for very many tiny groups. Prefer
ordinary `summarize`/windows or the vectorized partition fast paths where
equivalent. Group-local materialization rebinds its input and cache per group;
unrelated outer snapshots remain shared.

Single-array typed-long/real `mv-apply (top ...)` uses a batch Arrow top-k
kernel. Top-k followed by row counts, long sums, or ordered lists also stays in
batch expressions, avoiding one physical-plan initialization per source row.
Mixed scalar types, empty arrays, null placement, output limits, and exact
large integers retain the general template's semantics. Item-index, multiple
arrays, arbitrary predicates, and more complex aggregates use the general path.

## Stateful scans

`scan [with_match_id=...] [declare(...)] with (step ...; ...)` is a native
streaming execution source. Conditions and assignments are compiled once using
DataFusion's scalar expressions and type coercion; the scan owns the sequential
state machine rather than interpreting KQL expressions per row.

Each input row checks steps in reverse order. A successful promotion transfers
and clears the previous step's sequence before the earlier steps are evaluated.
Self-state updates retain the sequence's match ID; an inactive first step starts
a new zero-based match ID. Assignments are simultaneous and can update source
columns as well as declared columns. Unqualified names mean the current input
row/default declarations; `Step.Column` accesses that step's captured state.
Later-step references, duplicate declarations/steps/assignments, incompatible
types, and nonboolean conditions error.

`output=none` suppresses output without suppressing state changes.
`output=last` retains a sequence's last matching row until promotion/replacement
or end of input. Hidden ordinals restore source-row order, including multiple
matches on one row and delayed output, before downstream row windows or final
results. Missing input order is unspecified; sort explicitly when order matters.
State resets independently inside each `partition`/`mv-apply` subtable.

The operator retains bounded step history rather than every input row, with
at most 128 steps, 128 declarations, and 128 assignments per step. Variable-size
state and buffered outputs are charged to the host memory pool. State does not
spill. A nonnullable-long, zero-initialized additive single-step scan uses native
cumulative windows instead; nullable inputs use the state machine so null
propagation is not accidentally changed into SQL SUM's null-skipping behavior.
Compute row-window expressions in an earlier `extend`, not directly inside
step conditions/assignments. Full cross-service scan conformance still requires
real-Kusto differential fixtures.

## Hierarchical top values

`top-nested [N] of [Name=]Expression [with others=constant] by
[Name=]Aggregate [asc|desc]` supports up to 32 levels. Each parent is selected
using its full source-row aggregate before selecting its children, so pruning
children does not shrink the displayed parent total. Omitted counts retain all
groups; null and dynamic group keys are retained. Native grouped aggregation,
per-parent windows, null-safe joins, and query-local snapshots implement the
hierarchy without executing a separate subquery per key.

Others buckets retain rejected source rows and recompute the aggregate, rather
than incorrectly adding averages or percentile results. Nested others buckets
can be refined by subsequent levels. Labels must convert to the key's type.
Generated output names are `aggregated_<key>` unless explicitly named.
Output rows can grow rapidly with depth and omitted limits; the ordinary
expanded-plan budgets and host memory/spill configuration still apply.

`top-hitters N of Value [by Weight]` currently computes exact native counts or
sums and top-k selection. Its result is a valid exact estimate with KQL-style
`approximate_count_*`/`approximate_sum_*` names, but it does not implement
Kusto's bounded-memory Count-Min-Sketch or promise identical approximate results.
Exact states can consume more memory for high-cardinality inputs.

## Schema-aware evaluation plugins

`evaluate bag_unpack(Column [,prefix] [,columnsConflict] [,ignoredProperties])
: (OutputSchema)` supports declared scalar/dynamic fields, optional input `*`,
prefixes, ignored-key arrays, and `error`/`keep_source`/`replace_source` conflict
modes. Input bags are decoded once per batch row into a struct of requested
slots, rather than reparsed separately for every output field. Extraction is
cast-only: invalid slot types become typed nulls, not parsed numbers. Missing
strings normalize to empty strings. The bag input is removed, rows are retained,
and serialized order survives output projection.

`evaluate pivot(Column [,Aggregate] [,GroupingColumns...]) : (OutputSchema)`
lowers supported aggregates to native conditional grouped aggregation.
Default grouping excludes the pivot and aggregate input columns. Complete
schema declarations fix output order/types; unseen runtime bucket values and
declared-type mismatches fail explicitly instead of being silently dropped.
Supported aggregates are count, sum, min, max, avg, dcount, stdev, variance,
take_any, and list/set/bag collection builders.

Both plugins require an explicit output schema. Implicit data-dependent schema
discovery would execute input during lazy query construction, so it is not
silently enabled. Other Kusto plugins and service distribution hints remain
unsupported and return errors.

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

## Calendar and formatting functions

Calendar helpers include date components (`getyear`, `getmonth`,
`monthofyear`, `dayofmonth`, `dayofyear`, `hourofday`, `week_of_year`,
`datetime_part`), timespan-valued `dayofweek`, `datetime_add`, `datetime_diff`,
period starts/ends for day/week/month/year, `make_datetime`, `make_timespan`,
and Unix conversions from seconds through nanoseconds. Weeks start on Sunday
for period boundaries. Calendar differences count crossed period boundaries;
month/year addition clamps leap days and month ends. Ends subtract one Kusto
tick (100 ns), not one nanosecond. Nulls and unrepresentable constructor results
remain typed nulls.

`format_datetime` and `format_timespan` compile constant KQL-style format tokens
instead of passing incompatible `.NET` precision patterns into Chrono. Common
date/day/month/time tokens, quoted/escaped literals, AM/PM, UTC offsets, and
1-7 required/optional fractional digits are supported. Unsupported tokens
produce planning errors. Date/time arithmetic uses UTC and remains subject to
the signed nanosecond range described below.

## Serialized rows and windows

`serialize`, `sort`, and `top` establish serialized row sets. `prev`, `next`,
`row_number`, `row_cumsum`, `row_rank_dense`, `row_rank_min`, and
`row_window_session` require serialized input and lower to native DataFusion
window plans. Offset defaults, explicit fallback values, custom starting row
numbers, restart predicates, and nested calls such as `g != prev(g)` are
supported. Ranks count consecutive runs in the serialized stream, rather than
silently using SQL's global dense-rank semantics.

Internal ordinality survives projections (including dropped sort keys), filters,
renaming, parsing, limits, nested tabular bindings, and collection expansion.
Helpers never appear in wildcard output or the public result schema.
Expansion establishes distinct ordinals for child records. Unordered operators
such as joins, unions, distinct and summarize clear serialization; explicitly
sort or serialize again before using row functions.

`row_window_session` uses a causal, bounded-memory native partition evaluator.
Its first/neighbor distance limits can vary by row, equality with a limit starts
a new session, restart predicates split sessions, and null timestamps reset the
state. Dynamic offsets preserve primitive type tags and use a lossless internal
wire representation to avoid Arrow union null-padding limitations.

Order tracking is enabled only for queries that need serialized row behavior or
ordered collections. Ordinary filter/aggregate/top-k queries retain native-plan
parity with SQL. Serialization can limit parallelism, and restart-aware windows
may require additional repartitioning/sorting.

## Time-series generation and processing

`make-series` uses native grouped aggregation for bins, a second grouped
aggregation for sparse records, and a batch kernel for array filling. It does
not cross-join a dense time grid with every input row. Explicit and automatically
inferred per-group bounds, group expressions, multiple numeric aggregations,
aggregate arithmetic, default values, empty/nonempty ungrouped results, and
integer/real/datetime/timespan axes are supported. Main-syntax upper bounds are
exclusive; alternate `in range(...)` uses inclusive stop and epoch-aligned bins.
Arrays are limited to 1,048,576 points. Integer axes retain precision beyond
`2^53`; typed `bin_at` also uses exact wide arithmetic for integral origins.

Explicit starting bounds avoid the extra per-group minimum window needed for
automatic origins. Filter inputs early and configure host memory budgets.
Datetime/timespan axis elements retain typed scalar values. `todatetime` and
`totimespan` remain valid for explicit conversion, but `gettype` now reports the
actual type rather than `string`. JSON rendering uses the same canonical
RFC-3339 datetime formatter as other typed dynamic values, so zero fractions
are no longer padded solely for make-series axes.

Series processors include element-wise arithmetic and numeric comparisons;
absolute value, trigonometric/exponential/logarithmic functions, square root,
floor/ceiling/sign; forward/backward/constant/linear filling;
`series_stats_dynamic`; `series_fit_line_dynamic`; and FIR/IIR filters.
Missing or nonnumeric arithmetic elements produce nulls, including padding for
unequal lengths. Linear interpolation retains integral types without rounding
large endpoints through floating point. Statistics use sample variance and
original positions, with explicit nonfinite-value handling. FIR normalization,
centering, and IIR denominator rules follow KQL conventions.

Generic FIR/IIR filtering has a per-row budget of 67,108,864 multiply-add
operations and errors beyond it. Seasonal decomposition, FFT/period detection,
forecasting, and multiple-output tuple variants remain future work. Numeric
JSON serialization of nonfinite values retains the broader dynamic
representation limitations described below.

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
  `render`, additional hints, legacy explicit-source partition syntax, and
  advanced signal-processing operators are future work.
  Operators/functions not listed in this contract are not covered merely
  because they exist in Kusto. Additional areas include other `evaluate`
  plugins, implicit plugin schema discovery, graph operators,
  external-data syntax, full datetime/timespan ranges, and full scalar-function
  conformance. See [the delivery plan](architecture.md).
- Default parsing limits: 1 MiB of query text, nesting/expression depth 64,
  and 128 operators per pipeline. `parse_with_options` permits changing these
  within the parser's supported nesting bounds. Expanded expression and plan
  budgets also reject pathological `let` expansion. Input datasets are not
  capped by these frontend limits; configure DataFusion runtime memory/spilling
  and application-level query timeouts separately.

## Opt-in Kusto reference checks

The conformance suite in `tests/conformance.rs` runs synthetic inline-data
fixtures locally by default. It covers counts/nulls, expansion, per-row/per-key
queries, scans, hierarchical tops, schema-declared plugins, scalar subqueries,
ranges, GUIDs, parsing, and series shape. Local fixture checks are **not**
evidence that those fixtures have passed against a Kusto engine.

To compare the same fixtures against an explicitly configured reference:

```bash
# Existing local emulator: no bearer token is read or transmitted.
export KUSTO_CLUSTER_URI=http://127.0.0.1:8080
export KUSTO_DATABASE=TestDatabase
cargo test --locked --test conformance live_kusto_conformance -- --ignored --nocapture
```

For an existing ADX test database, set `KUSTO_CLUSTER_URI` to its HTTPS cluster
root and provide `KUSTO_DATABASE` and `KUSTO_BEARER_TOKEN` through the caller's
environment. No database, cluster, or credential is provisioned automatically.
Fixtures only issue read-only query requests. The test requires curl with
`--fail-with-body` support and does not add an HTTP client to the library's
runtime dependencies.

The client requests non-progressive Kusto v2 frames, rejects failed/cancelled or
incomplete datasets, and checks result column names/types before comparing row
multisets. Explicit string projections avoid incidental Arrow-vs-JSON display
differences. HTTPS credentials are kept out of process arguments in a temporary
header file that is removed on drop. Curl configuration files are disabled and
`KUSTO_BEARER_TOKEN` is removed from curl's environment. Plain HTTP is restricted
to loopback.

The separate `reference_transport_local_http` test uses a self-owned synthetic
HTTP fixture and verifies serialized request properties, read-only headers, and
the client parser. It does not emulate KQL or certify reference parity. CI runs
this transport check explicitly; the actual live Kusto test remains ignored
until a caller configures and requests it.

The manual **Kusto reference conformance** workflow provides a Linux/x64
reference environment without needing an Azure cluster or host Docker daemon.
It uses a pinned Microsoft emulator digest, a loopback-only port, a 4 GB/2 CPU
container, an isolated disposable database, and unconditional container cleanup.
It never runs on pushes or pull requests. Its `accept_emulator_license` input
defaults to false and must be explicitly set to true by a caller who accepts
the linked Microsoft terms. This workflow is prepared but has not been run or
represented as passing merely because ordinary CI succeeds.
