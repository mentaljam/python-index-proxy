# pip / uv end-to-end tests

Tests run the real proxy and pip/uv clients against a local package server.
Wheels and metadata fixtures are generated locally; test package fixtures are not
downloaded from public indexes. This is an independent uv project.

## Run

Use the tools pinned in [mise.toml](../mise.toml). The test Python needs `venv` and
`ensurepip`; its bundled pip is used for client environments. uv comes from PATH.
The Python requirement and test dependencies are declared in [pyproject.toml](pyproject.toml).

From the repository root:

```sh
mise run e2e
mise run e2e -n 2 -q
mise run e2e -k 'cold_install or restart'
```

The task builds the filesystem-only executable before pytest. Tests never build
it themselves. For direct pytest use, first run `mise run build-e2e`, then:

```sh
cd e2e
uv run --locked --no-python-downloads pytest -q
# Or use a prebuilt executable:
uv run --locked --no-python-downloads pytest --proxy-binary /path/to/python-index-proxy
```

Parallel execution defaults to `-n auto`; use `-n 0` for serial execution or
choose a worker count based on local CPU/disk limits. Each test has isolated
storage, client environments and upstream state.

Run `mise run check:e2e` for formatting, lint and type checks, or `mise run check`
for all project static checks.

## Coverage and diagnostics

The suite covers client installation and selection, cached downloads, restart and
cross-process artifact access, source policies, URL/context validation, response
formats, streaming cancellation, concurrency budgets, timeouts and shutdown.
Direct-upstream controls establish that fixtures are consumable independently of
the proxy. Client caches and automatic Python downloads are disabled; caller
configuration and environment are isolated. Tests assert intended behavior without
expected-failure markers.

Upstream request journals establish whether artifacts were fetched again.
Each installation uses a fresh environment and verifies installed behavior.
Internal parser, storage fault injection and coordination invariants remain in
Rust tests rather than being duplicated through process fixtures.

Processes have startup, command and shutdown deadlines and fixture cleanup.
Failures include client output, proxy logs and upstream requests. Pytest temporary
directories retain configuration, logs, artifacts and client environments subject
to pytest retention settings.

## Verification boundary

These tests use filesystem storage and local package sources. They do not qualify
live cloud storage, signed-URL downloads, distributed fill coordination,
Kubernetes, sdist builds or installations from public indexes. Passing process
tests does not establish browser-equivalent parsing of arbitrary malformed HTML.

For application behavior, see [configuration](../docs/configuration.md),
[protocol](../docs/protocol.md), [storage](../docs/storage.md) and
[operation](../docs/operation.md).
