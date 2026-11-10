use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;

use crate::{artifact::File, index::ArtifactSource};

use super::*;

#[tokio::test]
async fn cold_replica_recovers_context_links_and_cached_artifacts_survive_upstream_loss() {
    tokio::time::timeout(Duration::from_secs(15), async {
        use axum::{routing::get, extract::Path, http::StatusCode, response::IntoResponse};
        let index_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let app = axum::Router::new()
            .route("/simple/{package}/", get({
                let count = index_requests.clone();
                move |Path(package): Path<String>| {
                    let count = count.clone();
                    async move {
                        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        if package != "pkg" { return StatusCode::NOT_FOUND.into_response(); }
                        "<a href='../../files/pkg%2Bcpu.whl?download=1&amp;x=2#sha256=abc'>pkg+cpu.whl</a>".into_response()
                    }
                }
            }))
            .route("/files/{file}", get(|Path(file): Path<String>, uri: axum::http::Uri| async move {
                if uri.query() != Some("download=1&x=2") { return StatusCode::BAD_REQUEST.into_response(); }
                match file.as_str() {
                    "pkg+cpu.whl" => "wheel bytes".into_response(),
                    "pkg+cpu.whl.metadata" => "metadata bytes".into_response(),
                    _ => StatusCode::NOT_FOUND.into_response(),
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let upstream = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let root = std::env::temp_dir().join(format!("pipr-context-{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        let make_state = |capacity: &str| {
            let mut settings = Settings::template();
            settings.indices = [("default".into(), vec![format!("{endpoint}/simple/{{package}}/")])].into();
            // Even an entry too large for cache admission must work for the current request.
            settings.index_cache = toml::from_str(&format!("max_capacity = '{capacity}'")).unwrap();
            let mut config = opendal::services::FsConfig::default();
            config.root = Some(root.to_str().unwrap().into());
            settings.storage = crate::storage::Storage::Fs(config);
            State::try_from(settings).unwrap()
        };
        let first = make_state("1 B");
        let html = first.get_detail("default".into(), "pkg".into()).await.unwrap().unwrap();
        let html = std::str::from_utf8(&html).unwrap();
        let href = html.split("href=\"").nth(1).unwrap().split('"').next().unwrap();
        assert!(href.starts_with("/packages/default/pkg/"));
        assert!(href.ends_with("/pkg%2Bcpu%2Ewhl#sha256=abc"));
        let path = href.split('#').next().unwrap().to_owned();
        let id = path.split('/').nth(4).unwrap().to_owned();
        drop(first);
        let second = make_state("1 B");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_endpoint = format!("http://{}", listener.local_addr().unwrap());
        let router = crate::http::get_router("/").with_state(second.clone());
        let proxy = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = reqwest::Client::new();
        let response = client.get(format!("{proxy_endpoint}{path}")).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap().as_ref(), b"wheel bytes");
        assert_eq!(index_requests.load(std::sync::atomic::Ordering::SeqCst), 2);
        // A still-live page from another replica can predate this artifact.
        let stale = make_state("1 MiB");
        let stale_detail = Arc::new(PackageDetail {
            html: Bytes::from_static(b"old page"),
            files: [("f".repeat(64), ArtifactSource { url: format!("{endpoint}/files/old.whl"), filename: "old.whl".into() })].into(),
        });
        stale.0.read().await.index_cache.insert("default/pkg".into(), stale_detail).await;
        let File::Bytes(bytes) = stale.get_file("default".into(), "pkg".into(), id.clone(), "pkg+cpu.whl.metadata".into()).await.unwrap() else { panic!("stale page must refresh for an unknown artifact ID"); };
        assert_eq!(bytes.as_ref(), b"metadata bytes");
        assert_eq!(index_requests.load(std::sync::atomic::Ordering::SeqCst), 3);
        drop(stale);
        let metadata = client.get(format!("{proxy_endpoint}{path}.metadata")).send().await.unwrap();
        assert_eq!(metadata.status(), StatusCode::OK);
        assert_eq!(metadata.bytes().await.unwrap().as_ref(), b"metadata bytes");
        for wrong_path in [
            format!("/packages/default/pkg/{id}/wrong.whl"),
            format!("/packages/default/other/{id}/pkg%2Bcpu%2Ewhl"),
            format!("/packages/unknown/pkg/{id}/pkg%2Bcpu%2Ewhl"),
            format!("/packages/default/pkg/{}/pkg%2Bcpu%2Ewhl", "0".repeat(64)),
            format!("/packages/{id}/pkg%2Bcpu%2Ewhl"),
        ] {
            assert_eq!(client.get(format!("{proxy_endpoint}{wrong_path}")).send().await.unwrap().status(), StatusCode::NOT_FOUND, "{wrong_path}");
        }
        upstream.abort();
        let _ = upstream.await;
        let third = make_state("1 B");
        let File::Stream(stream) = third.get_file("default".into(), "pkg".into(), id, "pkg+cpu.whl".into()).await.unwrap() else { panic!("fresh replica must serve storage hit"); };
        assert_eq!(stream.try_collect::<Vec<_>>().await.unwrap().concat(), b"wheel bytes");
        proxy.abort();
        let _ = proxy.await;
        drop(second);
        drop(third);
        std::fs::remove_dir_all(root).unwrap();
    }).await.expect("context recovery must complete");
}
