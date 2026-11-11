# CI and releases

## Workflow

`.github/workflows/ci.yml` runs for pull requests targeting main and pushes to
main. Both run `mise run ci`; only pushes to main also build the container,
without publishing it. Pull requests do not build container images. CI also
exposes `workflow_call` so release checks use the same commands against the
tagged commit, with the test container build skipped.

`.github/workflows/release.yml` runs for version tags matching `v[0-9]*`. It first
calls CI, then publishes the crate and builds the image in parallel. The chart is released separately by
`.github/workflows/release-chart.yml` on `chart-v[0-9]*` tags. That workflow checks
versions, lints the chart, verifies its default image exists in GHCR, and packages
and publishes the chart. It does not build or publish the application.
The workflows use separate concurrency groups.

Tool downloads are cached by mise-action. rust-cache separately caches Cargo
registry data and build outputs; it does not cache the rustup toolchain. uv's
cache is stored separately and pruned before saving. Container builds use the
BuildKit GitHub Actions cache. Every job must also work on a cold cache.

Actions are pinned to explicit release tags, not commit SHAs. Their versions
were checked against upstream release pages when this workflow was added.

## Image and chart releases

Push an application tag `v<version>` to publish the crate and image to
`ghcr.io/mentaljam/python-index-proxy`. Push `chart-v<version>` to publish a chart
to `oci://ghcr.io/mentaljam/charts/python-index-proxy`. GHCR publication uses the
repository's GITHUB_TOKEN with `packages: write`, scoped to publishing jobs.
Package visibility and repository access are configured in GitHub Packages.

Application and chart have independent release versions:

- `Cargo.toml` and Cargo.lock define the application version for crates.io and Docker.
- `Chart.yaml.version` defines the chart version.
- `Chart.yaml.appVersion` selects the default, already published application image.
  It need not match the current Cargo.toml.

`mise run release-check`, included at the start of `mise run ci`, validates all
three version strings and the application's Cargo.lock entry. On release tags,
it checks `v<version>` against Cargo.toml or `chart-v<version>` against the chart
version. Versions must be SemVer without `+` build metadata; prereleases are
supported. The check uses uv with PyYAML declared in its inline dependencies.

A chart-only change requires only a chart version bump. Publishing an application
does not automatically update or release the chart. To adopt a new application
version, update appVersion and bump the chart version, including any required
template or configuration changes. A compatible image update normally warrants
a chart patch; features and breaking chart configuration changes determine the
chart's own version independently. Version bumps belong to release preparation,
not every intermediate PR.

Chart packaging uses the committed metadata without version overrides. Published
tags use the exact version without the Git tag's leading `v`. Stable releases also update the container image
`latest` tag; prereleases do not. Do not overwrite
released version tags; use the image digest for strict content pinning.

Helm first creates `target/helm/python-index-proxy-<version>.tgz`, including
Chart.yaml, values.yaml, values.schema.json, templates, documentation and examples.
`helm push` uploads that package to GHCR as an OCI artifact. Helm infers the final
repository name and version tag from the package metadata.

```sh
mise run chart-package
# Set VERSION to the version just packaged.
# Publishing tasks use GHCR_TOKEN and GITHUB_ACTOR for registry authentication.
mise run chart-login
mise run chart-push "$VERSION"
```

During installation Helm downloads the package and validates the supplied values
against its embedded schema before rendering Kubernetes resources. The schema URL
comment in values.yaml is for editors; Helm uses the file inside the package.
See the chart README for OCI installation examples.


Release images are built for Linux AMD64 and ARM64; ordinary CI builds AMD64.

## crates.io Trusted Publishing

The crate must already exist on crates.io; an initial bootstrap publication is
separate from the automated release. Configure its GitHub Trusted Publisher with:

- Owner: `mentaljam`
- Repository: `python-index-proxy`
- Workflow filename: `release.yml`
- Environment: leave unset (the publishing job does not use a GitHub environment)

The release job requests an OIDC token with `id-token: write` and exchanges it
through `rust-lang/crates-io-auth-action`. No persistent crates.io token or GitHub
secret is needed. See the [Trusted Publishing guide](https://crates.io/docs/trusted-publishing).

After the shared CI checks, `cargo package --locked` verifies that the packaged
sources build. Only then does the job obtain the short-lived token and run
`cargo publish --locked --no-verify`, avoiding a second verification build while
the token is active. No source changes occur between packaging and publishing.
The action revokes the token when the job finishes.

## Release procedure

For an application release:

1. Update Cargo.toml and refresh Cargo.lock. The version must not already be
   published on crates.io.
2. Run `mise run ci`, commit, and run `cargo package --locked` from the clean
   checkout to verify the distributable package.
3. Push the commit and matching `v<version>` tag to publish the crate and image.
4. Check both publishing jobs: publication across registries is not atomic.

For a chart release:

1. Update Chart.yaml.version. When adopting a new application, also update
   appVersion after its image has been published.
2. Run `mise run release-check` and `mise run chart-check`, then commit.
3. Push the commit and matching `chart-v<version>` tag. The workflow verifies the
   default image exists before publishing the chart.

On partial failure, rerun only failed jobs. Do not move published release tags
or overwrite released artifacts. Source fixes require a new version of the
affected artifact; a successfully published crate version cannot be published
again. Application prereleases do not update the container's `latest` tag.
Chart prereleases are independent and use their exact chart version.
