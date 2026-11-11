# Logging

The proxy writes application logs to stdout, for interactive use, service managers
such as systemd, and container log collectors. It does not send logs over the
network. `log_format = "text"` is the default; `log_format = "json"` emits one
JSON object per line. `PIPR__LOG_FORMAT` overrides the file setting. Filtering uses
`RUST_LOG`, with INFO as the default.

## HTTP requests

The proxy generates a new request ID for each request and returns it in the
`X-Request-ID` response header. Client-supplied IDs are ignored. IDs identify
requests; they are not authentication tokens or distributed trace IDs.

Each request produces one `HTTP request finished` event, subject to filtering.
The event fields are the same in both output formats. JSON events put these
fields under `fields`:

| Field | Meaning |
| --- | --- |
| `request_id` | Server-generated ID, also used in the request span. |
| `method` | HTTP method. |
| `status` | Response status, if headers were produced. |
| `duration_ms` | Elapsed milliseconds through response-body completion or cancellation. |
| `outcome` | `completed`, `cancelled`, or `body_error`. |
| `index` | Validated index name, when available. |
| `package` | Validated, normalized package name, when available. |
| `cache` | `index` or `artifact`, when a cache lookup was attempted. |
| `cache_result` | `hit`, `miss`, `shared`, `lookup`, `error`, or `not_applicable`. |
| `error` | Safe error category, when available. |

`hit` means this request found a cached object. `miss` means it needed to fetch
or refresh an object; it does not guarantee that this fetch succeeded. `shared`
means a concurrent request provided the refreshed index or filled the artifact
cache while this request waited. `error` means the initial artifact storage
lookup failed; `lookup` means it had not finished when the request was cancelled.
Internal index lookups for artifact downloads do not replace the
artifact cache result.

Duration measures application response handling and body production, including
time waiting for the consumer. It does not guarantee receipt by the remote
client. A failed response body can retain status 200 because headers were already
produced; use `outcome` and `error` as well as `status`. Cancellation before
response headers has no status. Empty bodies finish when the response is built.

Successful requests and ordinary 4xx responses use INFO; cancelled requests use
WARN; application errors, 5xx responses, and failed bodies use ERROR. Request
fields on the summary do not depend on enabling INFO spans, so an ERROR-only
filter retains the request context on errors.

Request context follows background artifact downloads. Distributed tracing is
not implemented; no `trace_id` or `span_id` is fabricated.

## Sensitive data

Application request events do not record raw URLs, paths, query strings,
filenames, headers, signed redirects, or arbitrary incoming request IDs. Index
and package fields come from validated identifiers. Application error events
use bounded categories instead of formatting third-party errors or their source
chains. This also avoids leaking secrets embedded in non-URL error messages.
Categories include `resource_limit`, `upstream_timeout`, `upstream_connect`,
`upstream_request`, `upstream_index`, `storage_permission_denied`,
`storage_not_found`, `storage_configuration`, `storage`, `io`, `internal`, and
`response_body`.

Configuration loading errors are text on stderr because the logging format may
not yet be known. These diagnostics identify a schema field when available;
they do not quote rejected values, source lines, arbitrary error messages, or
configuration file paths. This intentionally reduces diagnostic detail.

The safe diagnostic policy covers application-owned events. Third-party dependency
events are not sanitized or suppressed by the application; their levels are
controlled by `RUST_LOG`. To enable detailed application logging without enabling
dependency diagnostics, use `RUST_LOG=python_index_proxy=debug`.

Optional Prometheus instrumentation is documented in [metrics.md](metrics.md).
It records metrics independently of log filtering and output format.
