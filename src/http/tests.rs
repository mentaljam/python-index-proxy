use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use futures::TryStreamExt;

use crate::test_support::index_failure_state;

use super::*;

#[derive(Debug, Clone)]
struct StatFailureLayer {
    failure: Arc<std::sync::Mutex<Option<opendal::ErrorKind>>>,
    writes: Arc<std::sync::atomic::AtomicUsize>,
}

impl opendal::raw::Layer for StatFailureLayer {
    fn apply_service(&self, inner: opendal::raw::Servicer) -> opendal::raw::Servicer {
        Arc::new(StatFailureService {
            inner,
            controls: self.clone(),
        })
    }
}

#[derive(Debug)]
struct StatFailureService {
    inner: opendal::raw::Servicer,
    controls: StatFailureLayer,
}

impl opendal::raw::Service for StatFailureService {
    type Reader = opendal::raw::oio::Reader;
    type Writer = opendal::raw::oio::Writer;
    type Lister = opendal::raw::oio::Lister;
    type Deleter = opendal::raw::oio::Deleter;
    type Copier = opendal::raw::oio::Copier;

    fn info(&self) -> opendal::raw::ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> opendal::Capability {
        self.inner.capability()
    }

    async fn stat(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: opendal::raw::OpStat,
    ) -> opendal::Result<opendal::raw::RpStat> {
        if let Some(kind) = *self.controls.failure.lock().unwrap() {
            return Err(opendal::Error::new(kind, "injected artifact stat failure"));
        }
        self.inner.stat(ctx, path, args).await
    }

    async fn create_dir(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: opendal::raw::OpCreateDir,
    ) -> opendal::Result<opendal::raw::RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    fn read(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: opendal::raw::OpRead,
    ) -> opendal::Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }

    fn write(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: opendal::raw::OpWrite,
    ) -> opendal::Result<Self::Writer> {
        self.controls
            .writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.write(ctx, path, args)
    }

    fn delete(&self, ctx: &opendal::OperationContext) -> opendal::Result<Self::Deleter> {
        self.inner.delete(ctx)
    }

    fn list(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: opendal::raw::OpList,
    ) -> opendal::Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &opendal::OperationContext,
        from: &str,
        to: &str,
        args: opendal::raw::OpCopy,
        opts: opendal::raw::OpCopier,
    ) -> opendal::Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args, opts)
    }

    async fn rename(
        &self,
        ctx: &opendal::OperationContext,
        from: &str,
        to: &str,
        args: opendal::raw::OpRename,
    ) -> opendal::Result<opendal::raw::RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: opendal::raw::OpPresign,
    ) -> opendal::Result<opendal::raw::RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

#[tokio::test]
async fn artifact_stat_errors_do_not_trigger_upstream_or_cache_writes() {
    use axum::{http::StatusCode, response::IntoResponse, routing::get};
    use std::sync::atomic::{AtomicUsize, Ordering};

    tokio::time::timeout(Duration::from_secs(10), async {
        let requests = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new().route(
            "/{*path}",
            get({
                let requests = requests.clone();
                move |uri: axum::http::Uri| {
                    let requests = requests.clone();
                    async move {
                        requests.fetch_add(1, Ordering::SeqCst);
                        if uri.path() == "/simple/pkg/" {
                            "<a href='/files/pkg.whl'>pkg.whl</a>".into_response()
                        } else {
                            b"complete wheel".as_slice().into_response()
                        }
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let upstream = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (state, root) = index_failure_state(vec![format!("{endpoint}/simple/{{package}}/")]);
        let controls = StatFailureLayer {
            failure: Arc::new(std::sync::Mutex::new(None)),
            writes: Arc::new(AtomicUsize::new(0)),
        };
        {
            let mut data = state.0.write().await;
            data.storage = data.storage.clone().layer(controls.clone());
        }
        let unknown_id = "0".repeat(64);
        for kind in [
            opendal::ErrorKind::PermissionDenied,
            opendal::ErrorKind::Unexpected,
        ] {
            *controls.failure.lock().unwrap() = Some(kind);
            let error = match state
                .get_file(
                    "default".into(),
                    "pkg".into(),
                    unknown_id.clone(),
                    "pkg.whl".into(),
                )
                .await
            {
                Err(error) => error,
                Ok(_) => panic!("storage failure must propagate"),
            };
            assert_eq!(error.downcast_ref::<opendal::Error>().unwrap().kind(), kind);
            let response = super::get_file(
                axum::extract::State(state.clone()),
                axum::extract::Path((
                    "default".into(),
                    "pkg".into(),
                    unknown_id.clone(),
                    "pkg.whl".into(),
                )),
            )
            .await;
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(
                requests.load(Ordering::SeqCst),
                0,
                "storage errors must not contact upstream"
            );
            assert_eq!(
                controls.writes.load(Ordering::SeqCst),
                0,
                "storage errors must not attempt writes"
            );
            let data = state.0.read().await;
            assert!(data.index_cache.get("default/pkg").await.is_none());
            assert!(data.download_locks.is_empty());
        }
        *controls.failure.lock().unwrap() = None;
        let html = state
            .get_detail("default".into(), "pkg".into())
            .await
            .unwrap()
            .unwrap();
        let html = std::str::from_utf8(&html).unwrap();
        let href = html
            .split("href=\"")
            .nth(1)
            .unwrap()
            .split('"')
            .next()
            .unwrap();
        let id = href.split('/').nth(4).unwrap().to_owned();
        state.0.read().await.index_cache.invalidate_all();
        let File::Bytes(bytes) = state
            .get_file("default".into(), "pkg".into(), id.clone(), "pkg.whl".into())
            .await
            .unwrap()
        else {
            panic!("NotFound must fill storage");
        };
        assert_eq!(bytes, b"complete wheel".as_slice());
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        assert_eq!(controls.writes.load(Ordering::SeqCst), 1);
        state.0.read().await.index_cache.invalidate_all();
        let File::Stream(stream) = state
            .get_file("default".into(), "pkg".into(), id, "pkg.whl".into())
            .await
            .unwrap()
        else {
            panic!("existing artifact must stream from storage");
        };
        assert_eq!(
            stream.try_collect::<Vec<_>>().await.unwrap().concat(),
            b"complete wheel"
        );
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        assert_eq!(controls.writes.load(Ordering::SeqCst), 1);
        upstream.abort();
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("storage error handling must complete");
}

#[tokio::test]
async fn upstream_index_failures_return_bad_gateway_without_caching_partial_pages() {
    use axum::{extract::Path, http::StatusCode, response::IntoResponse, routing::get};
    use std::sync::atomic::{AtomicU16, Ordering};

    tokio::time::timeout(Duration::from_secs(10), async {
        let recovering_status = Arc::new(AtomicU16::new(503));
        let app = axum::Router::new().route(
            "/{source}/{package}/",
            get({
                let recovering_status = recovering_status.clone();
                move |Path((source, _)): Path<(String, String)>| {
                    let recovering_status = recovering_status.clone();
                    async move {
                        if source == "body-error" {
                            return axum::body::Body::from_stream(futures::stream::iter([Err::<
                                Bytes,
                                _,
                            >(
                                std::io::Error::other("upstream body interrupted"),
                            )]))
                            .into_response();
                        }
                        if source == "invalid-url" {
                            return "<a href=\"http://[invalid\">invalid URL</a>".into_response();
                        }
                        let code = if source == "recover" {
                            recovering_status.load(Ordering::SeqCst)
                        } else {
                            source.parse().unwrap()
                        };
                        let status = StatusCode::from_u16(code).unwrap();
                        (status, format!("<a href='{source}.whl'>{source}.whl</a>")).into_response()
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let upstream = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        // Mixed-source HTTP behavior is covered by process e2e; keep the
        // status/error matrix and private cache assertions here.
        let cases = [
            (vec!["404"], StatusCode::NOT_FOUND),
            (vec!["404", "404"], StatusCode::NOT_FOUND),
            (vec!["403"], StatusCode::BAD_GATEWAY),
            (vec!["429"], StatusCode::BAD_GATEWAY),
            (vec!["500"], StatusCode::BAD_GATEWAY),
            (vec!["503"], StatusCode::BAD_GATEWAY),
            (vec!["302"], StatusCode::BAD_GATEWAY),
            (vec!["body-error"], StatusCode::BAD_GATEWAY),
            (vec!["invalid-url"], StatusCode::INTERNAL_SERVER_ERROR),
        ];
        for (sources, expected) in cases {
            let urls = sources
                .iter()
                .map(|source| format!("{endpoint}/{source}/{{package}}/"))
                .collect();
            let (state, root) = index_failure_state(urls);
            let response = super::get_detail(
                axum::extract::State(state.clone()),
                Path(("default".into(), "pkg".into())),
            )
            .await;
            assert_eq!(response.status(), expected, "sources: {sources:?}");
            assert!(
                state
                    .0
                    .read()
                    .await
                    .index_cache
                    .get("default/pkg")
                    .await
                    .is_none()
            );
            let response = super::get_file(
                axum::extract::State(state.clone()),
                Path((
                    "default".into(),
                    "pkg".into(),
                    "a".repeat(64),
                    "file.whl".into(),
                )),
            )
            .await;
            assert_eq!(
                response.status(),
                expected,
                "cold artifact sources: {sources:?}"
            );
            drop(state);
            std::fs::remove_dir_all(root).unwrap();
        }

        let (state, root) = index_failure_state(vec![
            format!("{endpoint}/200/{{package}}/"),
            format!("{endpoint}/recover/{{package}}/"),
        ]);
        assert!(
            state
                .get_detail("default".into(), "pkg".into())
                .await
                .is_err()
        );
        assert!(
            state
                .0
                .read()
                .await
                .index_cache
                .get("default/pkg")
                .await
                .is_none()
        );
        recovering_status.store(200, Ordering::SeqCst);
        let detail = state
            .package_detail("default", "pkg", None)
            .await
            .unwrap()
            .unwrap();
        let mut filenames: Vec<_> = detail
            .files
            .values()
            .map(|file| file.filename.as_str())
            .collect();
        filenames.sort();
        assert_eq!(filenames, ["200.whl", "recover.whl"]);
        assert!(
            state
                .0
                .read()
                .await
                .index_cache
                .get("default/pkg")
                .await
                .is_some()
        );
        drop(state);
        upstream.abort();
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("upstream index failure matrix must complete");
}

#[tokio::test]
async fn upstream_connection_failure_is_bad_gateway_for_index_and_artifact() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let (state, root) = index_failure_state(vec![format!("{endpoint}/{{package}}/")]);
    let index = super::get_detail(
        axum::extract::State(state.clone()),
        axum::extract::Path(("default".into(), "pkg".into())),
    )
    .await;
    assert_eq!(index.status(), axum::http::StatusCode::BAD_GATEWAY);
    let file = super::get_file(
        axum::extract::State(state.clone()),
        axum::extract::Path((
            "default".into(),
            "pkg".into(),
            "a".repeat(64),
            "file.whl".into(),
        )),
    )
    .await;
    assert_eq!(file.status(), axum::http::StatusCode::BAD_GATEWAY);
    drop(state);
    std::fs::remove_dir_all(root).unwrap();
}
