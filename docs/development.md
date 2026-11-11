# Development

Tool versions and commands are defined in [mise.toml](../mise.toml). Install mise,
review and trust the repository configuration (`mise trust`), then run
`mise install`. Cargo.lock and e2e/uv.lock pin project dependencies separately.

```sh
mise tasks ls
mise run build --release
mise run run settings.toml
mise run check
mise run test
mise run test --features metrics
mise run e2e -n 2
mise run chart-check
mise run ci
```

`run` defaults to `settings.local.toml`, which uses the
[local S3 setup](operation.md#local-s3-development). Pass a filesystem configuration
for a standalone process. `build` and `test` forward Cargo arguments; `e2e` forwards
pytest arguments and builds its own filesystem-only executable.

`ci` checks release-version consistency, formatting/lint/types, default and metrics
Rust tests, pip/uv e2e and Helm examples. Its stages run sequentially because e2e
rebuilds the same executable with different features. Formatting/check subtasks
may run in parallel; use `mise run check:e2e` for a focused check.

## Code map

| Area | Source |
| --- | --- |
| Configuration and annotated template | `src/settings.rs`, `src/storage/mod.rs` |
| HTTP routes and client responses | `src/http/`, `src/index/` |
| Source requests and artifact fills | `src/upstream/`, `src/artifact/` |
| State, coordination and shutdown | `src/state/`, `src/coordination.rs`, `src/shutdown/` |
| Logs and metrics | `src/logging/`, `src/telemetry/` |
| Client/process tests | `e2e/tests/` |
| Kubernetes deployment | `charts/python-index-proxy/` |

Rust tests cover internal behavior; [e2e tests](../e2e/README.md) exercise public
HTTP and process boundaries with real clients. Use relevant existing checks for
the changed area and report limits: local tests do not qualify a cloud backend
or Kubernetes deployment.

## Maintaining documentation

Keep user-facing behavior in [docs](README.md), chart-specific behavior in its
README, and test procedures in the e2e README. Update the owning page when behavior
changes, linking to it instead of duplicating the explanation. Use the generated
settings template for the complete configuration reference. Keep dependency and
tool versions in manifests; document protocol versions only when they explain
compatibility. Do not treat historical audits as current contracts.

See [releases](releases.md) for CI and publication responsibilities.

## Container builds

From a repository checkout:

```sh
docker build -t python-index-proxy:local .
# Add an optional storage backend:
docker build --build-arg CARGO_FEATURES=default,metrics,storage-webdav \
  -t python-index-proxy:local .
```

Default Cargo features remain enabled by this Dockerfile. Native backends may
need additional build dependencies. Use the [container run example](../README.md#run-with-docker)
with `python-index-proxy:local` in place of the published image reference.
