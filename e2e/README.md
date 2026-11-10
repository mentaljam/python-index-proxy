# pip / uv end-to-end tests

This directory is an independent uv project with `package = false`. Tests run the
real proxy binary and real pip / uv clients against a local HTTP package index.
Wheels and PEP 658 metadata are generated locally using the Python standard
library; no package fixtures are downloaded from PyPI.

## Index roots

`/{index}/` is an informational HTML page with pip and uv connection examples.
It uses the resolved public URL and does not contact or expose upstreams.
`path_prefix` replaces the previous `base_url` configuration key and controls
application routing (default `/`). Optional `public_url` supplies the external
HTTP(S) application URL for landing links, artifact links and connection examples.
It must not contain credentials, query parameters or a fragment. Both settings
receive a trailing slash when the slash is omitted; their paths may differ for ingress rewrites.
For example, `path_prefix = "/"` and `public_url = "https://packages.example.org/proxy/"`
support an ingress that strips `/proxy/` before forwarding requests.

Without `public_url`, the application constructs its public base as
`http://{bind}{path_prefix}`, retaining the configured bind address (including
wildcard addresses). Set `public_url` when clients need a different host, HTTPS
or an externally rewritten path. Request headers do not determine the public URL.
Unknown index names return 404, and index names remain case-sensitive.
Use the trailing slash; the slashless root is not redirected.

Project enumeration is not supported. The informational root does not implement
the Simple Repository API's complete project-list requirement; package detail
and artifact routes remain available for requests for named projects.

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
- A saved, not-yet-downloaded artifact URL works after an actual process restart.
  The first file request reconstructs its source; the client does not fetch a new
  index page first. This also checks encoded `+` filenames, query-preserving
  metadata requests, context/filename validation and removed legacy routes.
- A saved ready-artifact URL works after restart even when all upstream routes
  fail; no new upstream requests are allowed.
- Two proxy processes use separate homes/logs and one shared filesystem artifact
  root. The second process reconstructs a link from the first, and the first
  then reads the completed shared object while upstream is unavailable.
- Multiple sources skip 404 projects in either order. Any source's 503 causes
  HTTP 502 for page and cold artifact recovery routes, even with a healthy peer.
  After recovery the complete candidate set and both artifact bodies are returned.
- Invalid configuration fails before binding its port, with field-specific
  diagnostics that do not echo source credentials. Nested/percent-encoded path
  prefixes work, including HTML escaping for ampersands.
- Package aliases such as `Foo__..Bar` use the canonical `foo-bar` upstream,
  page cache, artifact context and storage key without redirecting. Saved alias
  artifact URLs also work after restart; artifact filenames remain unchanged.
  Configured index names remain case-sensitive. Invalid names return 404
  without upstream requests, including when an artifact is already cached.
- A non-root `path_prefix`, PEP 658 metadata (and its absence), relative links after
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
- Saturated index-refresh and artifact-fill budgets return 503 with
  `Retry-After: 1`, without starting unrelated upstream work. Cached responses
  remain available, concurrent same-key requests return the same bytes with one
  upstream GET, and new work succeeds after the active transfer completes.
- Plain and gzip-compressed index pages exceeding the decoded-body limit return
  502 without caching partial results. Stalled index and artifact reads also
  return 502 before response streaming starts; both paths recover on retry.
- Real clients skip incompatible `data-requires-python` candidates and yanked
  candidates for an unconstrained requirement. Direct-upstream controls use the
  same candidate pages, then proxy installations verify the chosen version and
  that the ineligible wheel body was not fetched.

The attribute-selection expectations follow the
[Simple Repository API](https://packaging.python.org/en/latest/specifications/simple-repository-api/)
and [file-yanking specification](https://packaging.python.org/en/latest/specifications/file-yanking/).
Exact version pins may select yanked releases; these tests deliberately do not
pin the candidate version.

Project names are validated and canonicalized with `uv-normalize` (currently
requiring Rust >= 1.97). Existing artifacts stored under noncanonical project
names are left untouched; requests use the canonical key and may download again.

The added scenarios exercise client selection and process/transport boundaries.
They do not repeat Rust tests of byte weighting, malformed identifiers, parser
mapping IDs, internal lock ordering, or writer-close injection.

### Migration from Rust tests

`test_process_contracts.py` owns the external scenarios formerly combined in the
Rust cold-replica regression: cold source reconstruction, cross-replica ready-file
access, and HTTP context validation. Rust retains separate tests for an explicitly
injected stale cache page and for a page too large for its configured byte budget.
The mixed-source 404/success and success/503 HTTP cases also moved to process e2e.
Rust retains the broader single-status/body/connection error matrix and direct
assertions that a failed merge leaves no cache entry and recovery populates it.
These tests call the running binary through public URLs; they do not expose Rust
internals or duplicate the proxy's identifier/storage-key algorithms.

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
