//! Request summaries and deliberately bounded, credential-free error diagnostics.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::{Body, Bytes, HttpBody},
    extract::Request,
    http::{HeaderValue, StatusCode},
    middleware::Next,
    response::Response,
};
use http_body::{Frame, SizeHint};
use tracing::{Instrument, Span};

tokio::task_local! {
    static REQUEST: Arc<Mutex<Fields>>;
}

#[derive(Default)]
struct Fields {
    index: Option<String>,
    package: Option<String>,
    cache: Option<&'static str>,
    cache_result: Option<&'static str>,
    error: Option<&'static str>,
}

fn update(update: impl FnOnce(&mut Fields)) {
    let _ = REQUEST.try_with(|fields| {
        update(
            &mut fields
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
    });
}

pub(crate) fn package(index: &str, package: &str) {
    update(|fields| {
        fields.index = Some(index.to_owned());
        fields.package = Some(package.to_owned());
    });
}

pub(crate) fn index(index: &str) {
    update(|fields| fields.index = Some(index.to_owned()));
}

pub(crate) fn cache(kind: &'static str, result: &'static str) {
    update(|fields| {
        // An artifact miss may need the index cache to find its source URL.
        // The HTTP summary describes the artifact cache, not that internal lookup.
        if fields.cache != Some("artifact") || kind == "artifact" {
            fields.cache = Some(kind);
            fields.cache_result = Some(result);
        }
    });
}

pub(crate) fn record_error(error: &anyhow::Error) {
    update(|fields| {
        fields.error = Some(error_kind(error));
        if fields.cache_result == Some("lookup") {
            fields.cache_result = Some("error");
        }
    });
}

/// Capture before spawning: Tokio tasks do not inherit task-local request data.
pub(crate) fn inherit_context<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let context = REQUEST.try_with(Arc::clone).ok();
    let future = future.in_current_span();
    async move {
        match context {
            Some(context) => REQUEST.scope(context, future).await,
            None => future.await,
        }
    }
}

fn request_id() -> String {
    static INSTANCE: OnceLock<String> = OnceLock::new();
    static SEQUENCE: AtomicU64 = AtomicU64::new(1);
    let instance = INSTANCE.get_or_init(|| {
        let started = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        format!("{:x}-{:x}", started.as_nanos(), std::process::id())
    });
    format!("{instance}-{:x}", SEQUENCE.fetch_add(1, Ordering::Relaxed))
}

pub(crate) async fn request(request: Request, next: Next) -> Response {
    let request_id = request_id();
    let mut summary = Summary {
        span: tracing::info_span!("http_request", request_id = %request_id),
        request_id,
        method: request.method().to_string(),
        started: Instant::now(),
        status: None,
        fields: Arc::new(Mutex::new(Fields::default())),
        finished: false,
    };
    let mut response = REQUEST
        .scope(
            summary.fields.clone(),
            next.run(request).instrument(summary.span.clone()),
        )
        .await;
    // Always generate our own ID: arbitrary client headers may themselves contain secrets.
    if let Ok(value) = HeaderValue::from_str(&summary.request_id) {
        response.headers_mut().insert("x-request-id", value);
    }
    summary.status = Some(response.status());
    let (parts, body) = response.into_parts();
    if body.is_end_stream() {
        summary.finish("completed", None);
        Response::from_parts(parts, body)
    } else {
        Response::from_parts(
            parts,
            Body::new(LoggedBody {
                body: Box::pin(body),
                summary,
            }),
        )
    }
}

struct Summary {
    span: Span,
    request_id: String,
    method: String,
    started: Instant,
    status: Option<StatusCode>,
    fields: Arc<Mutex<Fields>>,
    finished: bool,
}

impl Summary {
    fn finish(&mut self, outcome: &'static str, error: Option<&'static str>) {
        if self.finished {
            return;
        }
        self.finished = true;
        let fields = self
            .fields
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let status = self.status.map(|status| status.as_u16());
        let error = fields.error.or(error);
        let duration_ms = self.started.elapsed().as_secs_f64() * 1000.0;
        macro_rules! emit {
            ($level:expr) => {
                tracing::event!(
                    parent: &self.span,
                    $level,
                    request_id = %self.request_id,
                    method = %self.method,
                    status,
                    duration_ms,
                    outcome,
                    index = fields.index.as_deref(),
                    package = fields.package.as_deref(),
                    cache = fields.cache,
                    cache_result = fields.cache_result.unwrap_or("not_applicable"),
                    error,
                    "HTTP request finished"
                )
            };
        }
        if error.is_some() || self.status.is_some_and(|status| status.is_server_error()) {
            emit!(tracing::Level::ERROR);
        } else if outcome != "completed" {
            emit!(tracing::Level::WARN);
        } else {
            emit!(tracing::Level::INFO);
        }
    }
}

impl Drop for Summary {
    fn drop(&mut self) {
        self.finish("cancelled", None);
    }
}

struct LoggedBody {
    body: Pin<Box<Body>>,
    summary: Summary,
}

impl HttpBody for LoggedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let this = self.get_mut();
        let result = REQUEST.sync_scope(this.summary.fields.clone(), || {
            let _entered = this.summary.span.enter();
            this.body.as_mut().poll_frame(cx)
        });
        match &result {
            Poll::Ready(Some(Err(_))) => this.summary.finish("body_error", Some("response_body")),
            Poll::Ready(None) => this.summary.finish("completed", None),
            Poll::Ready(Some(Ok(_))) if this.body.is_end_stream() => {
                this.summary.finish("completed", None);
            }
            _ => {}
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

/// Do not format third-party errors: even source chains can contain signed URLs,
/// backend credentials, response bodies, or request headers.
pub(crate) fn error_kind(error: &anyhow::Error) -> &'static str {
    if error.is::<crate::coordination::ResourceLimit>() {
        return "resource_limit";
    }
    for source in error.chain() {
        if let Some(error) = source.downcast_ref::<reqwest::Error>() {
            return if error.is_timeout() {
                "upstream_timeout"
            } else if error.is_connect() {
                "upstream_connect"
            } else {
                "upstream_request"
            };
        }
        if let Some(error) = source.downcast_ref::<opendal::Error>() {
            return match error.kind() {
                opendal::ErrorKind::PermissionDenied => "storage_permission_denied",
                opendal::ErrorKind::NotFound => "storage_not_found",
                opendal::ErrorKind::ConfigInvalid => "storage_configuration",
                _ => "storage",
            };
        }
        if source.is::<std::io::Error>() {
            return "io";
        }
    }
    if error.is::<crate::index::UpstreamIndexError>() {
        "upstream_index"
    } else {
        "internal"
    }
}

pub(crate) fn config_error(error: &config::ConfigError) -> &'static str {
    use config::ConfigError;
    match error {
        ConfigError::At { key: Some(key), .. } | ConfigError::Type { key: Some(key), .. } => {
            // Only schema-owned field names are safe; parser messages can quote
            // complete source lines and serde errors can echo rejected values.
            match key.split(['.', '[']).next().unwrap_or("") {
                "bind" => "bind",
                "log_format" => "log_format (expected text or json)",
                "path_prefix" => "path_prefix",
                "public_url" => "public_url",
                "indices" => "indices",
                "index_cache" => "index_cache",
                "limits" => "limits",
                "upstream" => "upstream",
                "storage_http" => "storage_http",
                "storage" => "storage",
                _ => "invalid field",
            }
        }
        ConfigError::FileParse { .. } => "could not parse configuration file",
        ConfigError::NotFound(key) if key == "storage" => "missing storage",
        _ => "could not load configuration",
    }
}

#[cfg(test)]
mod tests;
