# Prometheus metrics

Metrics are optional and disabled in the default Cargo build:

```sh
cargo build --locked --features metrics
# Filesystem storage only, with metrics:
cargo build --locked --no-default-features --features metrics
```

The Dockerfile enables metrics by default (`CARGO_FEATURES=default,metrics`).
To build a container without metrics, use `--build-arg CARGO_FEATURES=default`.
No runtime configuration or additional port is required. With the feature enabled,
`GET {path_prefix}metrics` on the application's listen address returns Prometheus
text format (`text/plain; version=0.0.4`). With `path_prefix = "/"`, the endpoint
is `/metrics`; with `path_prefix = "/proxy/"`, it is `/proxy/metrics`. The endpoint
follows `path_prefix`; `public_url` does not affect routing. Configure scraping
and reverse-proxy routing for that path accordingly. Scrapes do not contribute to
application HTTP metrics.
The route has the same network access controls as the application's listener.

Counters and gauges are cumulative/per-process; durations and sizes are classic
histograms with explicit buckets, allowing aggregation across replicas. It does
not emit client-calculated quantiles, summaries or native histograms. Duration
buckets range from 1 ms through 300 s, and size buckets from 1 KiB through 1 GiB;
each histogram also has a `+Inf` bucket. Exporter upkeep runs every five seconds,
even without scrapes, to aggregate pending histogram observations.

Metrics are independent of `RUST_LOG` and log format. Without the feature, the
telemetry API uses empty implementations: no metrics recorder, additional timers,
label construction, cache listeners, storage instrumentation or export route.
The metrics dependencies are absent from the active dependency graph. Conditional
compilation is confined to `src/telemetry/mod.rs`.

## HTTP and cache

| Metric | Type | Labels |
| --- | --- | --- |
| `pipr_http_requests_total` | Counter | `route`, `method`, `status`, `outcome`, `error` |
| `pipr_http_requests_in_flight` | Gauge | `route`, `method` |
| `pipr_http_response_headers_seconds` | Histogram | `route`, `method` |
| `pipr_http_request_duration_seconds` | Histogram | `route`, `method` |
| `pipr_http_response_body_bytes_total` | Counter | `route`, `method` |
| `pipr_cache_lookups_total` | Counter | `cache`, `result` |
| `pipr_artifact_deliveries_total` | Counter | `mode` |
| `pipr_index_cache_entries` | Gauge | none |
| `pipr_index_cache_weight_bytes` | Gauge | none |
| `pipr_index_cache_capacity_bytes` | Gauge | none |
| `pipr_index_cache_removals_total` | Counter | `cause` |

`route` is the matched route template, including the configured application prefix,
or `unmatched`. Arbitrary HTTP method tokens map to `OTHER`. Raw request paths,
package names, filenames, hashes, URLs, request IDs and arbitrary error messages
are never labels. Status is the numeric response status, or `none` if cancelled
before response headers. `error` uses the bounded categories described in
[logging.md](logging.md), or `none`.

Each request is counted once when its body completes, fails or is dropped.
`outcome` is `completed`, `body_error` or `cancelled`. HTTP 200 can accompany
`body_error`; use both status and outcome when measuring successful requests.
The full duration includes time waiting for the consumer, and does not confirm
receipt by the remote client. Body bytes count data produced at the application's
body boundary, after application compression; they exclude headers, transport
framing and bytes fetched directly from storage following a redirect. Header
latency means time until the response is produced, not network time to first byte.

Cache results are counted once per HTTP request, separately for `index` pages
and `artifact` files. Internal index lookups needed for an artifact do not produce
additional cache lookup counts. Results match the HTTP log summary: `hit`, `miss`,
`shared`, `error` or `lookup`. `shared` means waiting for another request's work;
`miss` does not imply that fetching or saving succeeded. `lookup` means the storage
lookup was still pending at cancellation. A storage failure is an `error`, not a
cache miss.

Artifact delivery modes are `redirect`, `stream` and `bytes`. These count selection
of the delivery mode, not successful transfers. In particular, a redirect only
confirms that a signed URL was returned; storage-side metrics are needed to measure
the subsequent download.

Cache entry count and weight are approximate in-memory cache snapshots. Weight covers keys,
rendered HTML/JSON and source mappings, excluding allocator overhead and in-flight
work. Removal causes are `expired`, `explicit`, `replaced` and `size`; notification
is asynchronous. These metrics do not inventory artifact storage or process RSS.

## Work and upstream

| Metric | Type | Labels |
| --- | --- | --- |
| `pipr_operations_total` | Counter | `operation`, `outcome` |
| `pipr_operations_in_flight` | Gauge | `operation` |
| `pipr_operation_duration_seconds` | Histogram | `operation`, `outcome` |
| `pipr_work_rejected_total` | Counter | `kind` |
| `pipr_work_slots_used` | Gauge | `kind` |
| `pipr_work_slots_limit` | Gauge | `kind` |
| `pipr_upstream_responses_total` | Counter | `kind`, `status` |
| `pipr_upstream_body_bytes_total` | Counter | none |

Internal operations are:

- `index_wait`, `artifact_wait`: acquisition of the per-key lock, including
  uncontended acquisitions. An operation's in-flight gauge includes waiters.
- `index_refresh`: fetching, parsing and rendering a project's pages across its
  configured sources; once per actual refresh, excluding shared waiters.
- `artifact_fill`: actual upstream fetch and cache fill, including upload cleanup;
  excludes cache hits and shared waiters.
- `upstream_artifact_headers`: an artifact request through response headers.
- `upstream_index`: one source page through its complete body, excluding parsing.
- `upstream_read`: one active body read, including the final EOF read; excludes
  time between reads spent writing storage or waiting for the consumer.

Operation outcomes include `success`, `error`, `timeout` and `cancelled`.
`index_refresh` and `artifact_fill` can return `not_found`.
`upstream_index` returns `skipped` for source responses skipped by index policy
(4xx except 408/429). `upstream_artifact_headers` returns `http_error` for a
non-success status. A drop before explicit completion records `cancelled` and
releases the in-flight gauge. Nested operations measure overlapping intervals;
do not sum their durations as independent stages.

Work kinds are `index` and `artifact`. Rejections count denied independent work,
not every HTTP request receiving a shared failure. Used slots are read from the
actual semaphores on scrape, so a blocking index parser that survives cancellation
continues to occupy its slot. Limits and cache gauges apply separately to each
replica.

Upstream response kinds are `index` and `artifact`. Status counts cover response
headers, independently of later body success. Body bytes include decompressed
index and artifact content, including partial/failed transfers. These metrics
aggregate configured sources; they do not expose upstream URLs or hosts.

## Storage

| Metric | Type | Labels |
| --- | --- | --- |
| `pipr_storage_operations_in_flight` | Gauge | `backend`, `operation` |
| `pipr_storage_operation_duration_seconds` | Histogram | `backend`, `operation` |
| `pipr_storage_ttfb_seconds` | Histogram | `backend`, `operation` |
| `pipr_storage_operation_bytes` | Histogram | `backend`, `operation` |
| `pipr_storage_processed_bytes_total` | Counter | `backend`, `operation` |
| `pipr_storage_errors_total` | Counter | `backend`, `operation`, `error` |

OpenDAL's observation layer instruments the operator, including streaming reader
and writer lifetimes. Only bounded backend, operation and error-kind identifiers
are retained; namespace, root and object paths are omitted. Writer close/abort
are part of the write lifetime, not separate operation series. Sizes and processed
bytes are recorded when the observed operation is dropped and include partial
transfers. They measure storage API activity, not HTTP wire traffic or stored
capacity. Normal cache probes returning `NotFound` also increment storage errors;
exclude that error kind when alerting on backend failures. An interrupted operation
may appear as `Unexpected` according to OpenDAL's observation semantics. Duration
histogram counts follow OpenDAL's observations and are not a universal counter of
all attempted operations; inspect errors as well.

## Example queries

```promql
# HTTP requests per second, excluding scrapes.
sum(rate(pipr_http_requests_total[5m]))

# HTTP failures, including failed bodies after successful headers.
sum(rate(pipr_http_requests_total{status=~"5.."}[5m])
  or rate(pipr_http_requests_total{outcome="body_error"}[5m]))

# p95 request duration by route across replicas.
histogram_quantile(0.95,
  sum by (route, le) (rate(pipr_http_request_duration_seconds_bucket[5m])))

# Artifact cache hits, excluding shared fills and unresolved/error lookups.
sum(rate(pipr_cache_lookups_total{cache="artifact",result="hit"}[5m]))
/ sum(rate(pipr_cache_lookups_total{cache="artifact",result=~"hit|miss|shared"}[5m]))

# Concurrency utilization per replica and work kind.
pipr_work_slots_used / pipr_work_slots_limit
```

Series are created lazily on first observation; an unseen event counter can be
absent rather than zero. The `up` metric is supplied by Prometheus, not the proxy.
CPU, RSS, restarts, storage capacity and actual redirected download traffic belong
to process/container and storage monitoring.
