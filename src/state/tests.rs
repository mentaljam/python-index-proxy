use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use bytes::Bytes;

use crate::{
    artifact::File,
    index::{ArtifactSource, PackageDetail},
    settings::Settings,
    state::State,
    test_support::index_failure_state,
};

#[tokio::test]
async fn uncacheable_package_details_remain_usable_for_the_current_request() {
    tokio::time::timeout(Duration::from_secs(10), async {
        use axum::routing::get;
        let requests = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new()
            .route(
                "/simple/pkg/",
                get({
                    let requests = requests.clone();
                    move || {
                        let requests = requests.clone();
                        async move {
                            requests.fetch_add(1, Ordering::SeqCst);
                            "<a href='/files/pkg.whl'>pkg.whl</a>"
                        }
                    }
                }),
            )
            .route("/files/pkg.whl", get(|| async { "wheel bytes" }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let upstream = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let root = std::env::temp_dir().join(format!(
            "pipr-uncacheable-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let mut settings = crate::settings::Settings::for_test();
        settings.indices = [(
            "default".into(),
            vec![format!("{endpoint}/simple/{{package}}/")].into(),
        )]
        .into();
        settings.index_cache = toml::from_str("max_capacity = '1 B'").unwrap();
        let mut storage = opendal::services::FsConfig::default();
        storage.root = Some(root.to_str().unwrap().into());
        settings.storage = crate::storage::Storage::Fs(storage);
        let state = crate::state::State::try_from(settings).unwrap();
        let detail = state
            .package_detail("default", "pkg", None)
            .await
            .unwrap()
            .unwrap();
        let id = detail.files.keys().next().unwrap().clone();
        let cache = state.0.index_cache.clone();
        cache.run_pending_tasks().await;
        assert!(cache.get("default/pkg").await.is_none());
        let File::Bytes(bytes) = state
            .get_file("default".into(), "pkg".into(), id, "pkg.whl".into())
            .await
            .unwrap()
        else {
            panic!("an uncacheable page must still resolve the current artifact request");
        };
        assert_eq!(bytes.as_ref(), b"wheel bytes");
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        cache.run_pending_tasks().await;
        assert!(cache.get("default/pkg").await.is_none());
        upstream.abort();
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("uncacheable source resolution must complete");
}

#[tokio::test]
async fn stale_cached_page_refreshes_for_an_unknown_artifact_identifier() {
    tokio::time::timeout(Duration::from_secs(10), async {
        use axum::routing::get;
        let requests = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new()
            .route(
                "/simple/pkg/",
                get({
                    let requests = requests.clone();
                    move || {
                        let requests = requests.clone();
                        async move {
                            requests.fetch_add(1, Ordering::SeqCst);
                            "<a href='/files/pkg.whl'>pkg.whl</a>"
                        }
                    }
                }),
            )
            .route(
                "/files/pkg.whl.metadata",
                get(|| async { "metadata bytes" }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let upstream = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (state, root) = index_failure_state(vec![format!("{endpoint}/simple/{{package}}/")]);
        let fresh = state
            .package_detail("default", "pkg", None)
            .await
            .unwrap()
            .unwrap();
        let id = fresh.files.keys().next().unwrap().clone();
        let stale = Arc::new(PackageDetail {
            json: Bytes::new(),
            html: Bytes::from_static(b"old page"),
            files: [(
                "f".repeat(64),
                ArtifactSource {
                    url: format!("{endpoint}/files/old.whl"),
                    filename: "old.whl".into(),
                },
            )]
            .into(),
        });
        assert!(!stale.files.contains_key(&id));
        let cache = state.0.index_cache.clone();
        cache.insert("default/pkg".into(), stale.clone()).await;
        assert!(Arc::ptr_eq(
            &cache.get("default/pkg").await.unwrap(),
            &stale
        ));
        let File::Bytes(bytes) = state
            .get_file(
                "default".into(),
                "pkg".into(),
                id.clone(),
                "pkg.whl.metadata".into(),
            )
            .await
            .unwrap()
        else {
            panic!("stale page must refresh for an unknown artifact ID");
        };
        assert_eq!(bytes.as_ref(), b"metadata bytes");
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        assert!(
            cache
                .get("default/pkg")
                .await
                .unwrap()
                .files
                .contains_key(&id)
        );
        upstream.abort();
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("stale page recovery must complete");
}

#[test]
fn default_public_url_uses_the_configured_socket_address_and_normalized_prefix() {
    let root = std::env::temp_dir().join(format!("pipr-public-url-{}", std::process::id()));
    for (bind, expected) in [
        ("127.0.0.1:3000", "http://127.0.0.1:3000/proxy/"),
        ("[::1]:3000", "http://[::1]:3000/proxy/"),
        ("0.0.0.0:3000", "http://0.0.0.0:3000/proxy/"),
        ("[::]:3000", "http://[::]:3000/proxy/"),
    ] {
        let mut settings: Settings = toml::from_str(&format!(
            "bind = '{bind}'\npath_prefix = '/proxy'\n[storage.fs]\nroot = 'artifacts'"
        ))
        .unwrap();
        let mut config = opendal::services::FsConfig::default();
        config.root = Some(root.to_str().unwrap().into());
        settings.storage = crate::storage::Storage::Fs(config);
        let state = State::try_from(settings).unwrap();
        assert_eq!(state.0.public_url, expected);
    }
    if root.exists() {
        std::fs::remove_dir_all(root).unwrap();
    }
}
