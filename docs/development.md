# Development and GitHub Actions

Tool versions and commands are defined in `mise.toml`. Install mise, review and
trust this repository's configuration (`mise trust`), then run `mise install`.
The configuration pins Rust (with rustfmt and clippy), Python, uv, Helm, Ruff and ty.
Cargo.lock and e2e/uv.lock pin project dependencies separately.

Project commands are exposed as mise tasks.

```sh
mise tasks ls
mise run build --release
mise run run
mise run run settings.local.toml
mise run check
mise run test
mise run test --features metrics
mise run e2e -n 2
mise run chart-check
mise run chart-package
mise run ci
```

`run` accepts an optional `config` argument, defaulting to `settings.local.toml`.
`build` and `test` forward extra arguments to Cargo; `e2e` forwards them to pytest.
`ci` first checks the shared project version, then runs formatting/lint/type
checks, default and metrics Rust tests, pip/uv e2e, and Helm linting. These run
sequentially because e2e rebuilds the same executable
with filesystem-only features. E2E tests create their package fixtures locally;
only normal Cargo/uv dependency installation needs external package sources.

## Workflow

`.github/workflows/ci.yml` runs for pull requests targeting main and pushes to
main. Both run `mise run ci`; only pushes to main also build the container,
without publishing it. Pull requests do not build container images. CI also
exposes `workflow_call` so release checks use the same commands against the
tagged commit, with the test container build skipped.

`.github/workflows/release.yml` runs for version tags matching `v[0-9]*`. It first
calls CI, then publishes the image, and finally packages and publishes the chart.
The chart job waits for the image publication to succeed. The workflows use
separate concurrency groups so a release does not conflict with its nested CI.

Tool downloads are cached by mise-action. rust-cache separately caches Cargo
registry data and build outputs; it does not cache the rustup toolchain. uv's
cache is stored separately and pruned before saving. Container builds use the
BuildKit GitHub Actions cache. Every job must also work on a cold cache.

Actions are pinned to explicit release tags, not commit SHAs. Their versions
were checked against upstream release pages when this workflow was added.

## Image and chart releases

Push a version tag to publish an image to
`ghcr.io/mentaljam/python-index-proxy` and a chart to
`oci://ghcr.io/mentaljam/charts/python-index-proxy`. Both use the repository's
GITHUB_TOKEN with `packages: write`, scoped to the publishing jobs. Package
visibility and repository access are configured in GitHub Packages.

Application and chart use one shared release version:

- `Cargo.toml` package version;
- `Chart.yaml` `version`;
- `Chart.yaml` `appVersion`.

`mise run release-check`, included at the start of `mise run ci`, requires all
three to match and checks that Cargo.lock contains the current package version.
The check runs through uv with PyYAML declared in the script's inline dependencies.
On release tags it also requires the tag to be `v` followed by that version.
Versions must be SemVer without `+` build metadata, which Docker tags do not
support. Prerelease versions are supported.

Before a release, update the versions in Cargo.toml and Chart.yaml together and
refresh Cargo.lock. Bump the shared version even for a chart-only change: every
release publishes both an image and a chart. Version bumps belong to release
preparation, not every intermediate PR. Compatibility covers application behavior
and the chart's public configuration.

Chart packaging uses the committed metadata without version overrides. Published
tags use the exact version without the Git tag's leading `v`. Do not overwrite
released version tags; use the image digest for strict content pinning.

Helm first creates `target/helm/python-index-proxy-<version>.tgz`, including
Chart.yaml, values.yaml, values.schema.json, templates, documentation and examples.
`helm push` uploads that package to GHCR as an OCI artifact. Helm infers the final
repository name and version tag from the package metadata.

```sh
mise run chart-package
# Publishing tasks use GHCR_TOKEN and GITHUB_ACTOR for registry authentication.
mise run chart-login
mise run chart-push 0.1.0
```

During installation Helm downloads the package and validates the supplied values
against its embedded schema before rendering Kubernetes resources. The schema URL
comment in values.yaml is for editors; Helm uses the file inside the package.
See the chart README for OCI installation examples.

The current Dockerfile builds Linux AMD64.

Python checks run separately from the repository root and from `e2e`.
`ruff.toml` defines shared lint rules; `e2e/pyproject.toml` extends it with
test-specific exceptions. Both ty invocations use the root `ty.toml`.
The root check explicitly includes the extensionless `mise-tasks/release-check`
script and uses its uv-managed inline-dependency environment (Python 3.15 target).
The e2e check uses its own project environment and Python requirement.

`fmt` and `check` aggregate independent `:rust`, `:python` and `:e2e` tasks
through mise dependencies, so those tasks may run in parallel. Each task has an
explicit working directory and can be invoked separately, for example
`mise run check:e2e`. Commands within each task remain sequential, including
preparing the script environment before checking its types. The top-level `ci`
stages still run sequentially.
