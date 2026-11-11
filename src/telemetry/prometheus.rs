use std::time::{Duration, Instant};

use axum::{
    Router,
    extract::{MatchedPath, Request, State as AxumState},
    http::header,
    response::IntoResponse,
    routing::get,
};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use opendal::Operator;
use opendal_layer_observe_metrics_common::{
    MetricLabels, MetricValue, MetricsIntercept, MetricsLayer,
};

use crate::state::State;

pub(crate) fn install(router: Router<State>, path_prefix: &str) -> anyhow::Result<Router<State>> {
    // Explicit buckets ensure aggregatable histograms rather than exporter-default summaries.
    let handle = PrometheusBuilder::new()
        .set_buckets(&[
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1., 2.5, 5., 10., 30., 60., 300.,
        ])?
        .set_buckets_for_metric(
            Matcher::Suffix("_bytes".into()),
            &[
                1024.,
                16384.,
                65536.,
                262144.,
                1048576.,
                5242880.,
                16777216.,
                67108864.,
                268435456.,
                1073741824.,
            ],
        )?
        .install_recorder()?;
    describe();
    // Upkeep must run even when nobody scrapes: histogram samples otherwise accumulate.
    let upkeep = handle.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            upkeep.run_upkeep();
        }
    });
    Ok(router.route(
        &format!("{path_prefix}metrics"),
        get(move |AxumState(state): AxumState<State>| {
            let handle = handle.clone();
            async move {
                snapshot(&state);
                (
                    [(
                        header::CONTENT_TYPE,
                        "text/plain; version=0.0.4; charset=utf-8",
                    )],
                    handle.render(),
                )
                    .into_response()
            }
        }),
    ))
}

fn describe() {
    describe_counter!(
        "pipr_http_requests_total",
        "Requests ending in completed, cancelled or body_error; status alone does not indicate success."
    );
    describe_gauge!(
        "pipr_http_requests_in_flight",
        "Requests whose response bodies have not finished."
    );
    describe_histogram!(
        "pipr_http_response_headers_seconds",
        "Time from request start until response headers are produced."
    );
    describe_histogram!(
        "pipr_http_request_duration_seconds",
        "Time through body completion, failure or cancellation; includes consumer backpressure."
    );
    describe_counter!(
        "pipr_http_response_body_bytes_total",
        "Body bytes produced by the application; excludes redirected storage traffic."
    );
    describe_counter!(
        "pipr_cache_lookups_total",
        "Final cache result per HTTP request, excluding internal index lookups for artifacts."
    );
    describe_counter!(
        "pipr_artifact_deliveries_total",
        "Selected artifact delivery mode; redirects do not confirm a completed download."
    );
    describe_counter!(
        "pipr_operations_total",
        "Internal operations by outcome; cancelled operations did not finish normally."
    );
    describe_gauge!(
        "pipr_operations_in_flight",
        "Internal operations currently running or waiting."
    );
    describe_histogram!(
        "pipr_operation_duration_seconds",
        "Internal operation wall time; upstream_read excludes time between reads."
    );
    describe_counter!(
        "pipr_work_rejected_total",
        "Independent work rejected because its concurrency limit was reached."
    );
    describe_gauge!(
        "pipr_work_slots_used",
        "Occupied concurrency slots, including blocking index parsing."
    );
    describe_gauge!(
        "pipr_work_slots_limit",
        "Configured per-replica concurrency limit."
    );
    describe_counter!(
        "pipr_upstream_responses_total",
        "Upstream response headers by request kind and HTTP status."
    );
    describe_counter!(
        "pipr_upstream_body_bytes_total",
        "Bytes read from upstream index and artifact response bodies after decompression."
    );
    describe_gauge!(
        "pipr_index_cache_entries",
        "Approximate number of cached project pages."
    );
    describe_gauge!(
        "pipr_index_cache_weight_bytes",
        "Approximate cache key and payload weight, excluding allocator overhead."
    );
    describe_gauge!(
        "pipr_index_cache_capacity_bytes",
        "Configured cache key and payload byte budget, excluding allocator overhead."
    );
    describe_counter!(
        "pipr_index_cache_removals_total",
        "Cache removals by cause, reported asynchronously by Moka."
    );
    describe_counter!(
        "pipr_storage_errors_total",
        "Storage operation failures, including normal NotFound probes."
    );
    describe_counter!(
        "pipr_storage_processed_bytes_total",
        "Bytes processed by storage operations, including partial transfers."
    );
    describe_gauge!(
        "pipr_storage_operations_in_flight",
        "Storage operations currently executing."
    );
    describe_histogram!(
        "pipr_storage_operation_duration_seconds",
        "OpenDAL operation lifetime, including stream lifetime where applicable."
    );
    describe_histogram!("pipr_storage_ttfb_seconds", "OpenDAL time to first byte.");
    describe_histogram!(
        "pipr_storage_operation_bytes",
        "Bytes processed per OpenDAL operation."
    );
}

fn snapshot(state: &State) {
    let data = &state.0;
    gauge!("pipr_index_cache_entries").set(data.index_cache.entry_count() as f64);
    gauge!("pipr_index_cache_capacity_bytes")
        .set(data.index_cache.policy().max_capacity().unwrap_or(0) as f64);
    gauge!("pipr_index_cache_weight_bytes").set(data.index_cache.weighted_size() as f64);
    for (kind, limit, permits) in [
        (
            "index",
            data.limits.max_index_refreshes,
            &data.index_permits,
        ),
        (
            "artifact",
            data.limits.max_artifact_downloads,
            &data.artifact_permits,
        ),
    ] {
        gauge!("pipr_work_slots_limit", "kind" => kind).set(limit as f64);
        gauge!("pipr_work_slots_used", "kind" => kind)
            .set(limit.saturating_sub(permits.available_permits()) as f64);
    }
}

pub(crate) fn index_cache(builder: super::IndexCacheBuilder) -> super::IndexCacheBuilder {
    builder.eviction_listener(|_, _, cause| {
        use moka::notification::RemovalCause;
        let cause = match cause {
            RemovalCause::Expired => "expired",
            RemovalCause::Explicit => "explicit",
            RemovalCause::Replaced => "replaced",
            RemovalCause::Size => "size",
        };
        counter!("pipr_index_cache_removals_total", "cause" => cause).increment(1);
    })
}

pub(crate) fn rejected(kind: &'static str) {
    counter!("pipr_work_rejected_total", "kind" => kind).increment(1);
}
pub(crate) fn upstream_response(kind: &'static str, status: u16) {
    counter!("pipr_upstream_responses_total", "kind" => kind, "status" => status.to_string())
        .increment(1);
}
pub(crate) fn upstream_bytes(bytes: usize) {
    counter!("pipr_upstream_body_bytes_total").increment(bytes as u64);
}
pub(crate) fn delivery(kind: &'static str) {
    counter!("pipr_artifact_deliveries_total", "mode" => kind).increment(1);
}

pub(crate) struct Http(Option<HttpData>);
struct HttpData {
    route: String,
    method: &'static str,
    started: Instant,
    status: Option<u16>,
    bytes: u64,
}
impl Http {
    pub(crate) fn start(request: &Request) -> Self {
        let route = request
            .extensions()
            .get::<MatchedPath>()
            .map_or("unmatched", MatchedPath::as_str);
        // MatchedPath contains the configured prefix; match the literal endpoint segment.
        if route.ends_with("/metrics") {
            return Self(None);
        }
        // Arbitrary HTTP method tokens and unmatched paths must not create new series.
        let method = match request.method().as_str() {
            "GET" => "GET",
            "HEAD" => "HEAD",
            "POST" => "POST",
            "PUT" => "PUT",
            "DELETE" => "DELETE",
            "PATCH" => "PATCH",
            "OPTIONS" => "OPTIONS",
            "CONNECT" => "CONNECT",
            "TRACE" => "TRACE",
            _ => "OTHER",
        };
        gauge!("pipr_http_requests_in_flight", "route" => route.to_owned(), "method" => method)
            .increment(1.);
        Self(Some(HttpData {
            route: route.to_owned(),
            method,
            started: Instant::now(),
            status: None,
            bytes: 0,
        }))
    }
    pub(crate) fn headers(&mut self, status: u16) {
        if let Some(data) = &mut self.0 {
            data.status = Some(status);
            histogram!("pipr_http_response_headers_seconds", "route" => data.route.clone(), "method" => data.method)
                .record(data.started.elapsed().as_secs_f64());
        }
    }
    pub(crate) fn bytes(&mut self, bytes: usize) {
        if let Some(data) = &mut self.0 {
            data.bytes = data.bytes.saturating_add(bytes as u64);
        }
    }
    pub(crate) fn finish(
        &mut self,
        outcome: &'static str,
        cache: Option<&'static str>,
        cache_result: Option<&'static str>,
        error: Option<&'static str>,
    ) {
        let Some(data) = self.0.take() else {
            return;
        };
        let labels = [
            metrics::Label::new("route", data.route),
            metrics::Label::new("method", data.method),
        ];
        gauge!("pipr_http_requests_in_flight", labels.iter()).decrement(1.);
        histogram!("pipr_http_request_duration_seconds", labels.iter())
            .record(data.started.elapsed().as_secs_f64());
        counter!("pipr_http_response_body_bytes_total", labels.iter()).increment(data.bytes);
        let mut labels = labels.to_vec();
        labels.extend([
            metrics::Label::new(
                "status",
                data.status.map_or_else(|| "none".into(), |v| v.to_string()),
            ),
            metrics::Label::new("outcome", outcome),
            metrics::Label::new("error", error.unwrap_or("none")),
        ]);
        counter!("pipr_http_requests_total", labels.iter()).increment(1);
        if let Some(cache) = cache {
            counter!("pipr_cache_lookups_total", "cache" => cache, "result" => cache_result.unwrap_or("lookup")).increment(1);
        }
    }
}

pub(crate) struct Operation {
    kind: &'static str,
    started: Instant,
    finished: bool,
}
impl Operation {
    pub(crate) fn start(kind: &'static str) -> Self {
        gauge!("pipr_operations_in_flight", "operation" => kind).increment(1.);
        Self {
            kind,
            started: Instant::now(),
            finished: false,
        }
    }
    pub(crate) fn finish(&mut self, outcome: &'static str) {
        if self.finished {
            return;
        }
        self.finished = true;
        gauge!("pipr_operations_in_flight", "operation" => self.kind).decrement(1.);
        counter!("pipr_operations_total", "operation" => self.kind, "outcome" => outcome)
            .increment(1);
        histogram!("pipr_operation_duration_seconds", "operation" => self.kind, "outcome" => outcome)
            .record(self.started.elapsed().as_secs_f64());
    }
    pub(crate) fn result<T>(&mut self, result: &anyhow::Result<T>) {
        self.finish(match result {
            Ok(_) => "success",
            Err(error)
                if error.chain().any(|source| {
                    source.is::<tokio::time::error::Elapsed>()
                        || source
                            .downcast_ref::<reqwest::Error>()
                            .is_some_and(reqwest::Error::is_timeout)
                }) =>
            {
                "timeout"
            }
            Err(_) => "error",
        });
    }
}
impl Drop for Operation {
    fn drop(&mut self) {
        self.finish("cancelled");
    }
}

pub(crate) fn storage(operator: Operator) -> Operator {
    operator.layer(MetricsLayer::new(StorageMetrics))
}

#[derive(Debug, Clone)]
struct StorageMetrics;
impl MetricsIntercept for StorageMetrics {
    fn observe(&self, labels: MetricLabels, value: MetricValue) {
        // Only bounded backend/operation/error identifiers; omit namespace and root.
        let common = [("backend", labels.scheme), ("operation", labels.operation)];
        match value {
            MetricValue::OperationDurationSeconds(v) => {
                histogram!("pipr_storage_operation_duration_seconds", &common)
                    .record(v.as_secs_f64())
            }
            MetricValue::OperationTtfbSeconds(v) => {
                histogram!("pipr_storage_ttfb_seconds", &common).record(v.as_secs_f64())
            }
            MetricValue::OperationBytes(v) => {
                histogram!("pipr_storage_operation_bytes", &common).record(v as f64);
                counter!("pipr_storage_processed_bytes_total", &common).increment(v);
            }
            MetricValue::OperationErrorsTotal => {
                counter!("pipr_storage_errors_total", "backend" => labels.scheme, "operation" => labels.operation, "error" => labels.error.map_or("unknown", |e| e.into_static())).increment(1);
            }
            MetricValue::OperationExecuting(v) => {
                gauge!("pipr_storage_operations_in_flight", &common).increment(v as f64)
            }
            _ => {}
        }
    }
}
