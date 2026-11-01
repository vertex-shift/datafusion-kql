# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

Delegated after the user was unavailable for the stack question: static HTML,
CSS and small JavaScript, published by GitHub Actions to GitHub Pages. The
library itself remains a Rust extension for Apache DataFusion.

## Users

The website addresses Rust developers and data engineers evaluating or
embedding KQL in an existing DataFusion application. This audience follows the
repository's documented API and the user's native-KQL project brief; more
specific audience research remains open.

## Product Purpose

Make pipe-oriented Kusto Query Language available through existing DataFusion
tables, catalogs, object stores and runtime configuration. The website should
make the mechanism, source-first adoption path and compatibility boundaries
easy to understand.

## Positioning

KQL compiles to ordinary DataFusion LogicalPlan and Expr values, then uses the
native optimizer and Arrow execution. There is no DataFusion fork, generated
SQL or separate execution engine.

## Operating Context

Users register providers in a SessionContext, call kql(), and execute a lazy
DataFrame. Source, compatibility documentation, examples, performance
methodology and contribution workflows are available in the public repository.

## Capabilities and Constraints

- Version 0.14 targets DataFusion 55.2.x and Rust 1.94+.
- Typed pipelines, dynamic values, grouped/stateful operators, windows, series
  analytics, bounded sketches and native spill-aware snapshots are implemented.
- Full KQL and Kusto-service parity are not complete.
- Local HLL/t-digest formats are not Kusto-service binary formats.
- Wide temporal ranges, some query/service surfaces and additional advanced
  series capabilities remain roadmap work.
- The package is datafusion-kql-native; the Rust import is datafusion_kql.
  The similarly named published datafusion-kql package is another project.
- No registry release has been performed by this project session. Source-first
  setup is the demonstrated adoption path, not an install-from-crates.io claim.
- Main is protected by pull requests and CI without administrator bypasses.

## Brand Commitments

Use the existing datafusion-kql project name, the vertex-shift repository and
Apache-2.0 license. Keep language direct and technical. Do not imply affiliation
with the Apache Software Foundation or Microsoft, or full service certification.
There is no established logo or visual identity to preserve.

## Evidence on Hand

README.md, docs/compatibility.md, docs/architecture.md and docs/performance.md
provide product truth. Thirteen Rust examples and the tests provide synthetic
query demonstrations. There are 68 captured Kusto fixtures and eight separately
labeled source/oracle fixtures. Benchmarks are local, hardware-specific samples.
No customer testimonials, usage statistics or universal performance guarantees
are available.

## Product Principles

- Explain the native extension boundary before listing features.
- Make runnable source examples the route to adoption.
- Distinguish supported behavior, extension profiles and remaining gaps.
- Link measured claims to their methodology and original source.
- Keep the website fast, accessible and usable without JavaScript.
