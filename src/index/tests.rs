use std::time::Duration;

use moka::future::Cache;

use crate::{
    settings::Settings,
    test_support::{index_failure_state, page},
};

use super::*;

#[tokio::test]
async fn concurrent_index_refreshes_share_uncacheable_stale_missing_and_error_results() {
    use axum::{http::StatusCode, response::IntoResponse, routing::get};
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    tokio::time::timeout(Duration::from_secs(15), async {
        for (status, stale, cancel_owner) in [
            (200, false, false),
            (200, true, false),
            (404, false, false),
            (503, false, false),
            (200, false, true),
        ] {
            let requests = Arc::new(AtomicUsize::new(0));
            let current_status = Arc::new(AtomicUsize::new(status));
            let paused = Arc::new(AtomicBool::new(true));
            let started = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let app = axum::Router::new().route(
                "/simple/pkg/",
                get({
                    let (requests, current_status, paused, started, release) = (
                        requests.clone(),
                        current_status.clone(),
                        paused.clone(),
                        started.clone(),
                        release.clone(),
                    );
                    move || {
                        let (requests, current_status, paused, started, release) = (
                            requests.clone(),
                            current_status.clone(),
                            paused.clone(),
                            started.clone(),
                            release.clone(),
                        );
                        async move {
                            requests.fetch_add(1, Ordering::SeqCst);
                            if paused.swap(false, Ordering::SeqCst) {
                                started.notify_one();
                                release.notified().await;
                            }
                            (
                                StatusCode::from_u16(current_status.load(Ordering::SeqCst) as u16)
                                    .unwrap(),
                                "<a href='/files/pkg.whl'>pkg.whl</a>",
                            )
                                .into_response()
                        }
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let upstream = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let (mut state, root) =
                index_failure_state(vec![format!("{endpoint}/simple/{{package}}/")]);
            // A one-byte budget cannot retain any parsed page. The shared in-flight
            // result must still reach concurrent callers, including missing-ID refreshes.
            Arc::get_mut(&mut state.0).unwrap().index_cache = Cache::builder()
                .max_capacity(if stale { 1024 * 1024 } else { 1 })
                .weigher(|_: &String, value: &Arc<PackageDetail>| value.byte_len() as u32)
                .build();
            if stale {
                let cache = state.0.index_cache.clone();
                cache
                    .insert("default/pkg".into(), page(b"stale page"))
                    .await;
                cache.run_pending_tasks().await;
                assert!(cache.get("default/pkg").await.is_some());
            }
            let first = tokio::spawn({
                let state = state.clone();
                async move {
                    state
                        .package_detail("default", "pkg", Some("missing-id"))
                        .await
                }
            });
            started.notified().await;
            let mut second = Box::pin(state.package_detail("default", "pkg", Some("missing-id")));
            assert!(futures::poll!(&mut second).is_pending());
            if cancel_owner {
                first.abort();
                assert!(first.await.err().unwrap().is_cancelled());
                let second = second.await.unwrap().unwrap();
                assert_eq!(second.files.len(), 1);
                release.notify_one();
            } else {
                release.notify_one();
                let (first, second) = tokio::join!(first, second);
                let first = first.unwrap();
                match status {
                    200 => {
                        let first = first.unwrap().unwrap();
                        let second = second.unwrap().unwrap();
                        assert!(Arc::ptr_eq(&first, &second));
                        assert_eq!(first.files.len(), 1);
                    }
                    404 => assert!(first.unwrap().is_none() && second.unwrap().is_none()),
                    503 => {
                        assert!(first.err().unwrap().is::<UpstreamIndexError>());
                        assert!(second.err().unwrap().is::<UpstreamIndexError>());
                    }
                    _ => unreachable!(),
                }
            }
            let wave_requests = if cancel_owner { 2 } else { 1 };
            assert_eq!(requests.load(Ordering::SeqCst), wave_requests);
            assert!(state.0.index_refreshes.is_empty());
            current_status.store(200, Ordering::SeqCst);
            state.0.index_cache.run_pending_tasks().await;
            assert!(
                state
                    .package_detail("default", "pkg", Some("missing-id"))
                    .await
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                requests.load(Ordering::SeqCst),
                wave_requests + 1,
                "no result is negatively cached across independent waves"
            );
            upstream.abort();
            drop(state);
            std::fs::remove_dir_all(root).unwrap();
        }
    })
    .await
    .expect("shared index refreshes must finish");
}

#[tokio::test]
async fn slow_index_fetch_does_not_block_unrelated_requests() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let body_started = Arc::new(tokio::sync::Notify::new());
        let release_body = Arc::new(tokio::sync::Notify::new());
        let app = axum::Router::new().route(
            "/{package}/",
            axum::routing::get({
                let body_started = body_started.clone();
                let release_body = release_body.clone();
                move |axum::extract::Path(package): axum::extract::Path<String>| {
                    let body_started = body_started.clone();
                    let release_body = release_body.clone();
                    async move {
                        axum::body::Body::from_stream(async_stream::stream! {
                            if package == "slow" {
                                body_started.notify_one();
                                release_body.notified().await;
                            }
                            yield Ok::<_, std::io::Error>(Bytes::from(format!(
                                "<a href=\"{package}.whl\">{package}.whl</a>",
                            )));
                        })
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let upstream = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let root = std::env::temp_dir().join(format!(
            "pipr-index-lock-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut settings = Settings::for_test();
        settings.indices = [("default".into(), vec![format!("{endpoint}/{{package}}/")])].into();
        let mut config = opendal::services::FsConfig::default();
        config.root = Some(root.join("artifacts").to_str().unwrap().into());
        settings.storage = crate::storage::Storage::Fs(config);
        let state = State::try_from(settings).unwrap();
        let slow = tokio::spawn({
            let state = state.clone();
            async move { state.get_detail("default".into(), "slow".into()).await }
        });
        body_started.notified().await;
        let indices = state.get_indices_list();
        assert!(std::str::from_utf8(&indices).unwrap().contains("default"));
        let fast = tokio::time::timeout(
            Duration::from_secs(1),
            state.get_detail("default".into(), "fast".into()),
        )
        .await
        .expect("slow upstream body must not block another package")
        .unwrap()
        .unwrap();
        assert!(
            std::str::from_utf8(&fast)
                .unwrap()
                .contains("/fast%2Ewhl\"")
        );
        release_body.notify_one();
        let slow = slow.await.unwrap().unwrap().unwrap();
        assert!(
            std::str::from_utf8(&slow)
                .unwrap()
                .contains("/slow%2Ewhl\"")
        );
        let mut urls = Vec::new();
        for package in ["fast", "slow"] {
            let detail = state
                .package_detail("default", package, None)
                .await
                .unwrap()
                .unwrap();
            urls.extend(detail.files.values().map(|source| source.url.clone()));
        }
        urls.sort();
        assert_eq!(
            urls,
            [
                format!("{endpoint}/fast/fast.whl"),
                format!("{endpoint}/slow/slow.whl")
            ]
        );
        upstream.abort();
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("index concurrency regression must finish");
}

#[tokio::test]
async fn index_cache_capacity_counts_key_and_payload_bytes() {
    let root = std::env::temp_dir().join(format!(
        "pipr-cache-capacity-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut settings = Settings::for_test();
    settings.index_cache = toml::from_str("max_capacity = '12 B'").unwrap();
    let mut config = opendal::services::FsConfig::default();
    config.root = Some(root.join("artifacts").to_str().unwrap().into());
    settings.storage = crate::storage::Storage::Fs(config);
    let state = State::try_from(settings).unwrap();
    let cache = state.0.index_cache.clone();

    // Four UTF-8 key bytes and eight payload bytes exactly fill the budget.
    cache.insert("é/x".into(), page(b"12345678")).await;
    cache.run_pending_tasks().await;
    assert_eq!(cache.weighted_size(), 12);
    assert_eq!(cache.get("é/x").await.unwrap().html.as_ref(), b"12345678");
    cache.invalidate_all();
    cache.run_pending_tasks().await;

    for (key, value) in [("key", page(b"1234567890")), ("long-cache-key", page(b""))] {
        cache.insert(key.into(), value).await;
        cache.run_pending_tasks().await;
        assert!(
            cache.get(key).await.is_none(),
            "oversized entry must not remain: {key}"
        );
    }

    let mappings_only = Arc::new(PackageDetail {
        html: Bytes::new(),
        files: [(
            "abc".into(),
            ArtifactSource {
                url: "1234".into(),
                filename: "12345".into(),
            },
        )]
        .into(),
    });
    cache.insert("x".into(), mappings_only).await;
    cache.run_pending_tasks().await;
    assert!(
        cache.get("x").await.is_none(),
        "mapping IDs, URLs and filenames must share the byte budget"
    );

    // Each entry fits separately, but the pair must not remain together.
    cache.insert("a".into(), page(b"12345678")).await;
    cache.run_pending_tasks().await;
    assert!(cache.get("a").await.is_some());
    cache.insert("b".into(), page(b"12345678")).await;
    cache.run_pending_tasks().await;
    assert_eq!(cache.entry_count(), 1);
    assert_eq!(cache.weighted_size(), 9);

    drop(cache);
    drop(state);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn template_placeholder_replacement_cannot_change_a_normalized_host() {
    let template =
        IndexUrlTemplate::parse("https://PIPRPACKAGEMARKER.example/{package}?token=private")
            .unwrap();
    assert_eq!(
        template.package_url("foo-bar").unwrap().as_str(),
        "https://piprpackagemarker.example/foo%2Dbar/?token=private",
    );
}
