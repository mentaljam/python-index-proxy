# Working in this repository

Read [README.md](README.md) for the product and [docs/README.md](docs/README.md)
for the documentation map. [Development](docs/development.md) describes code
ownership and checks; the chart and e2e directories have their own README files.

- Use the tasks in `mise.toml`; keep tool/dependency versions in manifests and
  lockfiles, not copied into guides.
- Update the documentation page that owns changed behavior and link to it from
  other entry points. Keep general documentation focused on capabilities.
- Configuration fields and template generation live in `src/settings.rs` and
  `src/storage/mod.rs`; keep documented examples consistent with them.
- Run checks appropriate to the change. `mise run ci` is the full local sequence;
  it builds different feature sets sequentially. See the e2e README before
  running client tests directly.
- Distinguish source inspection, local tests and live deployment verification.
  Do not claim cloud or Kubernetes behavior from filesystem tests or chart lint.
