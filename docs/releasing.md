# Publishing the crate

The package name is **`datafusion-kql-native`**. The repository remains
`vertex-shift/datafusion-kql`, and the Rust library target remains
`datafusion_kql`. The registry name `datafusion-kql` is already used by the
unrelated `irtimmer/rust-kql` project. The new name was unregistered when this
pipeline was prepared; a registry name is not reserved until publication.

After the first version is published, consumers can preserve the existing
dependency/import spelling with:

```toml
[dependencies]
datafusion = "55.2"
datafusion-kql = { package = "datafusion-kql-native", version = "0.11" }
```

```rust
use datafusion_kql::SessionContextKqlExt;
```

## Pipeline and protections

`.github/workflows/ci.yml` runs on branch pushes and pull requests and is
reusable by the release workflow. Its Rust 1.94/stable jobs retain formatting,
strict Clippy, tests, minimal-feature checking, and all examples. A separate
package job tests the release guards, validates package identity, and runs
`cargo package --locked` plus `cargo publish --dry-run --locked`. The explicit
package command creates the stable `target/package/*.crate` artifact; Cargo's
publish dry-run can use temporary staging paths. Cargo builds the extracted
archive rather than trusting the checkout alone. CI uploads the `.crate` and its SHA-256
checksum as a 14-day artifact. This step never uploads to crates.io.

`.github/workflows/release.yml` runs for pushed `v*` tags or manual dispatch.
Manual dispatch defaults to **dry-run mode**. Actual publication requires:

- A tag exactly matching `v<the Cargo.toml version>`.
- A clean, immutable checkout of that tag's commit.
- That commit already being an ancestor of the fetched `origin/main`.
- Successful reusable CI for the exact same commit, including package verification.
- Approval in the `crates-io` GitHub environment.

The environment permits only `main` and `v*` tags and requires review by
`adityavaish`; administrator bypass is disabled. Self-approval is permitted so
a single maintainer can operate releases. Environment protection is GitHub
configuration, not YAML: preserve it when moving/forking this repository.
All actions in these two workflows are pinned by commit SHA. Publishing uses
`contents: read`; `id-token: write` is limited to the publishing job.
The release guard is repeated before obtaining registry credentials.

Only one release workflow runs at a time. Mismatched tags, unmerged commits,
invalid package identity, failed checks, authentication errors, and duplicate
registry versions fail explicitly. The workflow does not overwrite versions,
retag commits, automatically accept registry terms, or silently fall back from
OIDC to a long-lived token. The publishing job also builds its canonical archive
before obtaining credentials. After an upload, it verifies the registry's crate
name, version, repository, and checksum against the generated archive.

## First publication

[crates.io trusted publishing](https://crates.io/docs/trusted-publishing)
currently requires an **already published crate**. Initial publication needs
an API token from an authorized crates.io account; a GitHub login/token does
not provide registry publishing permission.

1. Merge the pipeline and package-name changes into `main`. Use a crates.io
   account with the required verified contact information and authorization.
2. Create a short-lived crates.io API token with permission for this initial
   publication, restricted to `datafusion-kql-native` where supported.
   Store it as **`CRATES_IO_BOOTSTRAP_TOKEN`** under GitHub repository
   **Settings > Environments > crates-io > Environment secrets**. Do not put it
   in source, a workflow input, a command argument, or chat. The CLI can prompt
   for it without embedding it in command history:

   ```bash
   gh secret set CRATES_IO_BOOTSTRAP_TOKEN \
     --repo vertex-shift/datafusion-kql --env crates-io
   ```

3. Tag the merged version, using the actual manifest version:

   ```bash
   git fetch origin
   git tag v0.11.0 origin/main
   git push origin v0.11.0
   ```

   Pushing this first tag also schedules the normal OIDC release. **Cancel
   that automatic run in Actions before starting the bootstrap run**, rather
   than approving it: trusted publishing is not configured yet, and release
   concurrency would otherwise queue the manual run behind it.

4. Run **Publish crate** manually from `main`, with `tag=v0.11.0`,
   `dry_run=false`, and `bootstrap=true`:

   ```bash
   gh workflow run release.yml --repo vertex-shift/datafusion-kql --ref main \
     -f tag=v0.11.0 -F dry_run=false -F bootstrap=true
   ```

5. Inspect CI and approve the `crates-io` deployment. The bootstrap path
   requires the environment secret explicitly; an absent token fails with a
   setup diagnostic. Registry publication is irreversible in the ordinary
   release flow, so approve only the intended commit/version.

No bootstrap token was supplied while preparing this pipeline, and no first
version was published as part of its dry-run verification.

## Configure trusted publishing

After the initial publication, open the crate's crates.io settings and add a
GitHub trusted publisher with these exact fields:

| Field | Value |
| --- | --- |
| Repository owner | `vertex-shift` |
| Repository name | `datafusion-kql` |
| Workflow filename | `release.yml` |
| Environment | `crates-io` |

The official `rust-lang/crates-io-auth-action` exchanges GitHub OIDC identity
for a short-lived registry token and revokes it at job completion. Configure
this publisher before attempting an automatic release. Revoke the bootstrap
token on crates.io and remove `CRATES_IO_BOOTSTRAP_TOKEN` from the GitHub
environment once it is no longer needed.

## Subsequent releases

Update `Cargo.toml` and `Cargo.lock`, documentation as appropriate, and merge
the release commit through normal CI. Push a matching tag at the merged
commit, for example:

```bash
git fetch origin
git tag v0.11.1 origin/main
git push origin v0.11.1
```

The tag workflow runs all checks and waits for environment approval, then uses
OIDC to publish. It cannot publish a commit that exists only on a feature
branch. Do not move an existing release tag or reuse a published version.
If Cargo reports a timeout after uploading, inspect the registry and its
checksum before retrying: an upload may already have succeeded.

## Dry runs

For a local package build with no upload:

```bash
cargo publish --dry-run --locked --registry crates-io \
  --package datafusion-kql-native
```

To exercise release checks and the complete reusable CI against an existing
merged version tag without authenticating or requiring deployment approval:

```bash
gh workflow run release.yml --repo vertex-shift/datafusion-kql --ref main \
  -f tag=v0.11.0 -F dry_run=true
```

The archive intentionally includes source, tests, examples, benches, public
documentation, README, license, and lockfile. GitHub workflows, release scripts,
credentials, and the build directory are excluded. Package-guard tests use only
temporary local Git repositories and mocked registry responses.
