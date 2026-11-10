# pip / uv end-to-end tests

This directory is an independent uv project with `package = false`. Tests run the
real proxy binary and real pip / uv clients against a local HTTP package index.
Wheels and PEP 658 metadata are generated locally using the Python standard
library; no package fixtures are downloaded from PyPI.

## Run

Prerequisites: Rust/Cargo, uv, and an installed Python >= 3.12 with `venv` and
`ensurepip`. The Python used by pytest also supplies the interpreter and bundled
pip for client environments. uv is taken from `PATH`. Client versions therefore
depend on the test environment; `uv.lock` pins pytest, pytest-xdist, Ruff and ty dependencies.

From this directory:

```sh
uv sync --locked --no-python-downloads
uv run --locked --no-python-downloads pytest -q
```

Or from the repository root:

```sh
uv run --project e2e --locked --no-python-downloads pytest -c e2e/pyproject.toml e2e/tests
```

The session builds the proxy once with
`cargo build --locked --no-default-features --features storage-fs`. Normal Cargo
and uv dependency setup may require network access. Once dependencies are cached,
the suite only needs loopback TCP sockets. To use a prebuilt executable:

```sh
uv run --locked --no-python-downloads pytest --proxy-binary ../target/debug/python-index-proxy
```

Parallel execution is opt-in through pytest-xdist. Start with two workers:

```sh
uv run --locked --no-python-downloads pytest -n 2 -q
```

The controller builds the proxy once before starting workers and passes the same
absolute executable path to every worker. `--proxy-binary` also works with
parallel runs and skips Cargo entirely. Serial runs retain a lazy session build.
Each test keeps its own client environments, upstream server and artifact root.

Compare worker counts with the same prebuilt executable to exclude build time:

```sh
uv run --locked --no-python-downloads pytest -n 0 --proxy-binary ../target/debug/python-index-proxy
uv run --locked --no-python-downloads pytest -n 2 --proxy-binary ../target/debug/python-index-proxy
uv run --locked --no-python-downloads pytest -n 4 --proxy-binary ../target/debug/python-index-proxy
```

`-n 0` runs serially. More workers compete for CPU and disk while creating venvs
and running installers, so choose the worker count from local measurements rather
than assuming `-n auto` is fastest. Parallelism does not suppress expected failures.

Run a subset using ordinary pytest selectors:

```sh
uv run --locked --no-python-downloads pytest -k 'cold_install or restart'
uv run --locked --no-python-downloads pytest -k html5_unquoted
```

## Static checks

Run from this directory:

```sh
uv run --locked --no-python-downloads ruff check .
uv run --locked --no-python-downloads ruff format --check .
uv run --locked --no-python-downloads ty check
```

Configuration lives in `pyproject.toml`. Ruff enables all stable rules, with
documented exceptions for tests and formatter compatibility. Annotation checks
remain enabled. ty checks `tests/` with all diagnostics treated as errors.
Both tools target the project's minimum Python version, 3.12.

## Coverage

- Control installations directly from upstream, including HTML with unquoted
  `href`, prove both clients can consume the fixture independently of the proxy.
- Cold installation resolves a wheel dependency and verifies installed versions
  and module behavior.
- Repeat installation uses a fresh venv with client caching disabled. Upstream
  artifact routes return 503, and the request journal must show no new wheel
  downloads.
- Restart retains artifact storage and discards the in-memory index cache.
  Package pages remain available upstream, but artifact downloads are forbidden.
- A saved artifact URL works after restart even when all upstream routes fail;
  no new upstream requests are allowed.
- A non-root `base_url`, PEP 658 metadata (and its absence), relative links after
  redirects, and query parameters exercise client compatibility.
- A wheel with a 12 MiB payload exercises streaming and subsequent cache reads;
  installed payload size and SHA-256 must match.
- Wrong artifact checksums and nonexistent packages must fail without installing
  the requested distribution.
- Missing packages must return HTTP 404; an unavailable sole upstream must return
  a server error rather than masquerading as a missing package, and recover once
  the upstream serves the package again.
- Disconnecting an HTTP consumer after streamed bytes are observed must abort
  the fill and remove temporary artifacts while upstream is still paused. A
  fresh pip/uv installation must then fetch correct bytes and populate the cache.
- Two independent pip/uv environments request the same large wheel while its
  upstream response is paused. Both installations must finish with correct
  module behavior and cached bytes. A local forwarding observer confirms both
  client requests have been sent before release; this is not an assertion about
  internal lock scheduling. The completed installations must share exactly one
  upstream GET for their common wheel within this replica. This fixture advertises
  PEP 658 metadata so uv does not need separate, intentionally canceled wheel probes.
- Real clients skip incompatible `data-requires-python` candidates and yanked
  candidates for an unconstrained requirement. Direct-upstream controls use the
  same candidate pages, then proxy installations verify the chosen version and
  that the ineligible wheel body was not fetched.

The attribute-selection expectations follow the
[Simple Repository API](https://packaging.python.org/en/latest/specifications/simple-repository-api/)
and [file-yanking specification](https://packaging.python.org/en/latest/specifications/file-yanking/).
Exact version pins may select yanked releases; these tests deliberately do not
pin the candidate version.

The added scenarios exercise client selection and process/transport boundaries.
They do not repeat Rust tests of byte weighting, malformed identifiers, parser
mapping IDs, internal lock ordering, writer-close injection, or cold-state source
reconstruction.

The upstream GET journal decodes percent-encoded paths before route matching and
logging, while retaining query strings. HEAD probes receive the same response
headers without bodies and are not counted as artifact downloads. Client configuration, index settings,
proxy settings, active virtual environments, and Python paths from the caller's
environment are excluded. Each installation uses a new venv; client caches and
automatic Python downloads are disabled. pip version checks are also disabled.

Processes have startup/command/shutdown deadlines and are cleaned up in fixture
finalizers. Failures include client output, proxy logs and upstream requests.
pytest temporary directories retain `settings.toml`, `proxy.log`, artifacts and
client environments for inspection, subject to pytest's temporary-file retention.

## Verification boundary

The proxy uses html5gum to extract anchors without a DOM. Rust tests cover
raw-text/RCDATA contexts and adjacent or unterminated anchors; this does not
claim browser-equivalent tree construction for arbitrary malformed documents.

The HTML5 unquoted-href cases are expected to pass through the proxy, as do
installation, cancellation cleanup, and upstream error-classification scenarios.
Tests keep assertions on expected behavior and do not use `xfail` markers.

This suite covers filesystem storage and HTML indexes. It does not qualify real
S3/presigned redirects, other cloud backends, distributed fill coordination,
Kubernetes, sdist builds, or installation from public indexes.
