use std::{io::Write, time::Duration};

use axum::{Router, extract::Path, routing::get};
use serde_json::Value;
use tower::ServiceExt;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::layer::SubscriberExt;

use super::*;

#[derive(Clone, Default)]
struct Output(Arc<Mutex<Vec<u8>>>);

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Output {
    fn records(&self) -> Vec<Value> {
        String::from_utf8(self.0.lock().unwrap().clone())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

fn subscriber(output: Output) -> impl tracing::Subscriber + Send + Sync {
    tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .json()
            .with_ansi(false)
            .with_writer(move || output.clone()),
    )
}

fn router(router: Router) -> Router {
    router.layer(axum::middleware::from_fn(request))
}

async fn consume(mut body: Body) {
    while let Some(frame) = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        frame.unwrap();
    }
}

#[tokio::test]
async fn concurrent_requests_keep_context_and_generated_ids_separate() {
    let output = Output::default();
    async {
        let app = router(Router::new().route(
            "/{index}/{package}",
            get(|Path((index, name)): Path<(String, String)>| async move {
                package(&index, &name);
                cache("index", if name == "first" { "hit" } else { "miss" });
                let task = tokio::spawn(inherit_context(async {
                    tokio::task::yield_now().await;
                    cache("index", "shared");
                }));
                if name == "second" {
                    task.await.unwrap();
                } else {
                    task.abort();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                "ok"
            }),
        ));
        let call = |uri: &'static str| {
            let app = app.clone();
            async move {
                let response = app
                    .oneshot(
                        Request::builder()
                            .uri(uri)
                            .header("x-request-id", "secret-client-id")
                            .header("authorization", "Bearer secret-auth")
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let id = response.headers()["x-request-id"]
                    .to_str()
                    .unwrap()
                    .to_owned();
                consume(response.into_body()).await;
                id
            }
        };
        let (first, second) = tokio::join!(
            call("/private/first?token=secret-query"),
            call("/public/second")
        );
        assert_ne!(first, second);
        let records = output.records();
        assert_eq!(records.len(), 2);
        for (id, index, name, result) in [
            (first, "private", "first", "hit"),
            (second, "public", "second", "shared"),
        ] {
            let fields = &records
                .iter()
                .find(|row| row["fields"]["request_id"] == id)
                .unwrap()["fields"];
            assert_eq!(fields["index"], index);
            assert_eq!(fields["package"], name);
            assert_eq!(fields["cache_result"], result);
            assert_eq!(fields["status"], 200);
            assert_eq!(fields["method"], "GET");
            assert_eq!(fields["outcome"], "completed");
        }
        assert!(!format!("{records:?}").contains("secret-"));
    }
    .with_subscriber(subscriber(output.clone()))
    .await;
}

struct ChannelBody(tokio::sync::mpsc::UnboundedReceiver<Result<Frame<Bytes>, std::io::Error>>);

impl HttpBody for ChannelBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        self.0.poll_recv(cx)
    }
}

#[tokio::test]
async fn streaming_summary_waits_for_eof_and_preserves_frames_and_duration() {
    let output = Output::default();
    async {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let receiver = Arc::new(Mutex::new(Some(receiver)));
        let app = router(Router::new().route(
            "/",
            get(move || {
                let receiver = receiver.lock().unwrap().take().unwrap();
                async { Body::new(ChannelBody(receiver)) }
            }),
        ));
        let response = app.oneshot(Request::new(Body::empty())).await.unwrap();
        assert!(
            output.records().is_empty(),
            "headers are not stream completion"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
        sender
            .send(Ok(Frame::data(Bytes::from_static(b"payload"))))
            .unwrap();
        let mut trailers = axum::http::HeaderMap::new();
        trailers.insert("x-check", HeaderValue::from_static("preserved"));
        sender.send(Ok(Frame::trailers(trailers))).unwrap();
        drop(sender);
        let mut body = response.into_body();
        let data = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(data.into_data().unwrap(), "payload");
        let trailers = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(trailers.into_trailers().unwrap()["x-check"], "preserved");
        consume(body).await;
        let records = output.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["fields"]["outcome"], "completed");
        assert!(records[0]["fields"]["duration_ms"].as_f64().unwrap() >= 25.0);
    }
    .with_subscriber(subscriber(output.clone()))
    .await;
}

#[tokio::test]
async fn dropped_or_failed_bodies_emit_one_summary_without_error_contents() {
    for failed in [false, true] {
        let output = Output::default();
        async {
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            let receiver = Arc::new(Mutex::new(Some(receiver)));
            let app = router(Router::new().route(
                "/",
                get(move || {
                    let receiver = receiver.lock().unwrap().take().unwrap();
                    async { Body::new(ChannelBody(receiver)) }
                }),
            ));
            let response = app.oneshot(Request::new(Body::empty())).await.unwrap();
            if failed {
                sender
                    .send(Err(std::io::Error::other(
                        "https://user:secret@example.test/?token=secret",
                    )))
                    .unwrap();
                let mut body = response.into_body();
                assert!(
                    std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                        .await
                        .unwrap()
                        .is_err()
                );
                drop(body);
            } else {
                drop(response);
            }
            let records = output.records();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0]["fields"]["status"], 200);
            assert_eq!(
                records[0]["fields"]["outcome"],
                if failed { "body_error" } else { "cancelled" }
            );
            assert!(!format!("{records:?}").contains("secret"));
        }
        .with_subscriber(subscriber(output.clone()))
        .await;
    }
}

#[tokio::test]
async fn real_router_logs_cache_hits_and_unmatched_requests_without_raw_paths() {
    let output = Output::default();
    async {
        let (state, root) =
            crate::test_support::index_failure_state(vec!["http://127.0.0.1:1/{package}/".into()]);
        state
            .0
            .index_cache
            .insert("default/pkg".into(), crate::test_support::page(b"cached"))
            .await;
        let app = router(crate::http::get_router("/prefix/").with_state(state));
        for (uri, expected) in [
            ("/prefix/default/Pkg/?token=secret", 200),
            ("/secret-path?token=secret", 404),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), expected);
            consume(response.into_body()).await;
        }
        let records = output.records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["fields"]["package"], "pkg");
        assert_eq!(records[0]["fields"]["cache_result"], "hit");
        assert_eq!(records[1]["fields"]["status"], 404);
        assert_eq!(records[1]["fields"]["cache_result"], "not_applicable");
        assert!(!format!("{records:?}").contains("secret"));
        let _ = std::fs::remove_dir_all(root);
    }
    .with_subscriber(subscriber(output.clone()))
    .await;
}

#[tokio::test]
async fn error_only_filter_keeps_request_fields_without_info_spans() {
    let output = Output::default();
    async {
        let app = router(Router::new().route(
            "/",
            get(|| async {
                package("private", "pkg");
                cache("index", "miss");
                record_error(
                    &anyhow::anyhow!("private-token").context(crate::index::UpstreamIndexError),
                );
                StatusCode::BAD_GATEWAY
            }),
        ));
        let response = app.oneshot(Request::new(Body::empty())).await.unwrap();
        let id = response.headers()["x-request-id"]
            .to_str()
            .unwrap()
            .to_owned();
        consume(response.into_body()).await;
        let records = output.records();
        assert_eq!(records.len(), 1);
        let fields = &records[0]["fields"];
        assert_eq!(fields["request_id"], id);
        assert_eq!(fields["package"], "pkg");
        assert_eq!(fields["cache_result"], "miss");
        assert_eq!(fields["status"], 502);
        assert_eq!(fields["error"], "upstream_index");
        assert!(!format!("{records:?}").contains("private-token"));
    }
    .with_subscriber(subscriber(output.clone()).with(tracing::level_filters::LevelFilter::ERROR))
    .await;
}

#[test]
fn error_diagnostics_never_format_arbitrary_error_messages_or_configuration_values() {
    let secret = "https://user:secret@example.test/?token=secret";
    let error = anyhow::anyhow!(secret).context("also secret");
    assert_eq!(error_kind(&error), "internal");
    let error = anyhow::Error::new(opendal::Error::new(
        opendal::ErrorKind::PermissionDenied,
        secret,
    ));
    assert_eq!(error_kind(&error), "storage_permission_denied");
    for config in [
        format!("log_format = '{secret}'"),
        format!("bind = '{secret}'"),
        format!("public_url = '{secret}'"),
        format!("storage = '{secret}'"),
        format!("[storage.fs]\nroot = '{secret}"),
    ] {
        let error = config::Config::builder()
            .add_source(config::File::from_str(&config, config::FileFormat::Toml))
            .build()
            .and_then(|config| config.try_deserialize::<crate::settings::Settings>())
            .err()
            .unwrap();
        assert!(!config_error(&error).contains("secret"));
    }
}
