use std::sync::{Arc, atomic::Ordering};

use crate::{
    index::{ArtifactSource, PackageDetail},
    settings::Settings,
    test_support::index_failure_state,
};

use super::download::CHUNK_SIZE;
use super::*;

#[tokio::test]
async fn artifact_waiters_queue_before_upstream_response_headers() {
    use axum::routing::get;
    use std::sync::atomic::AtomicUsize;
    tokio::time::timeout(Duration::from_secs(10), async {
        let requests = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let app = axum::Router::new()
            .route(
                "/simple/pkg/",
                get(|| async { "<a href='/files/pkg.whl'>pkg.whl</a>" }),
            )
            .route(
                "/files/pkg.whl",
                get({
                    let (requests, started, release) =
                        (requests.clone(), started.clone(), release.clone());
                    move || {
                        let (requests, started, release) =
                            (requests.clone(), started.clone(), release.clone());
                        async move {
                            if requests.fetch_add(1, Ordering::SeqCst) == 0 {
                                started.notify_one();
                                release.notified().await;
                            }
                            "complete artifact"
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let upstream = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (state, root) = index_failure_state(vec![format!("{endpoint}/simple/{{package}}/")]);
        let detail = state
            .package_detail("default", "pkg", None)
            .await
            .unwrap()
            .unwrap();
        let id = detail.files.keys().next().unwrap().clone();
        let owner = tokio::spawn({
            let state = state.clone();
            let id = id.clone();
            async move {
                state
                    .get_file("default".into(), "pkg".into(), id, "pkg.whl".into())
                    .await
            }
        });
        started.notified().await;
        let mut waiter =
            Box::pin(state.get_file("default".into(), "pkg".into(), id.clone(), "pkg.whl".into()));
        let path = artifact_path("default", "pkg", &id, "pkg.whl");
        loop {
            assert!(futures::poll!(&mut waiter).is_pending());
            assert_eq!(
                requests.load(Ordering::SeqCst),
                1,
                "waiter must not fetch while owner's headers are pending"
            );
            let locks = state.0.download_locks.clone();
            if locks.has_waiters(&path) {
                break;
            }
            tokio::task::yield_now().await;
        }
        release.notify_one();
        let File::Bytes(first) = owner.await.unwrap().unwrap() else {
            panic!("small owner response must be buffered");
        };
        let File::Stream(second) = waiter.await.unwrap() else {
            panic!("waiter must use completed storage entry");
        };
        assert_eq!(first, b"complete artifact".as_slice());
        assert_eq!(
            second.try_collect::<Vec<_>>().await.unwrap().concat(),
            b"complete artifact"
        );
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert!(state.0.download_locks.is_empty());
        upstream.abort();
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("pre-header artifact coordination must finish");
}

#[tokio::test]
async fn artifact_waiters_recheck_storage_and_independent_objects_make_progress() {
    coordinated_artifact_downloads(false).await;
}

#[tokio::test]
async fn cancelled_artifact_owner_allows_queued_request_to_retry() {
    coordinated_artifact_downloads(true).await;
}

async fn coordinated_artifact_downloads(cancel_owner: bool) {
    use axum::{extract::Path, response::IntoResponse, routing::get};
    use std::sync::atomic::AtomicUsize;
    tokio::time::timeout(Duration::from_secs(15), async {
        let requests = Arc::new(AtomicUsize::new(0));
        let metadata_requests = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let bytes = Bytes::from(vec![42; CHUNK_SIZE * 2 + 17]);
        let app = axum::Router::new()
            .route(
                "/simple/{package}/",
                get(|| async { "<a href='/files/pkg.whl'>pkg.whl</a>" }),
            )
            .route(
                "/files/{file}",
                get({
                    let (requests, metadata_requests, started, release, bytes) = (
                        requests.clone(),
                        metadata_requests.clone(),
                        started.clone(),
                        release.clone(),
                        bytes.clone(),
                    );
                    move |Path(file): Path<String>| {
                        let (requests, metadata_requests, started, release, bytes) = (
                            requests.clone(),
                            metadata_requests.clone(),
                            started.clone(),
                            release.clone(),
                            bytes.clone(),
                        );
                        async move {
                            if file.ends_with(".metadata") {
                                metadata_requests.fetch_add(1, Ordering::SeqCst);
                                return "metadata bytes".into_response();
                            }
                            let first = requests.fetch_add(1, Ordering::SeqCst) == 0;
                            axum::body::Body::from_stream(async_stream::stream! {
                                if first {
                                    yield Ok::<_, std::io::Error>(bytes.slice(..CHUNK_SIZE + 1));
                                    started.notify_one();
                                    release.notified().await;
                                    yield Ok(bytes.slice(CHUNK_SIZE + 1..));
                                } else {
                                    yield Ok::<_, std::io::Error>(bytes);
                                }
                            })
                            .into_response()
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let upstream = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (state, root) = index_failure_state(vec![format!("{endpoint}/simple/{{package}}/")]);
        let detail = state
            .package_detail("default", "pkg", None)
            .await
            .unwrap()
            .unwrap();
        let id = detail.files.keys().next().unwrap().clone();
        let File::Stream(mut owner) = state
            .get_file("default".into(), "pkg".into(), id.clone(), "pkg.whl".into())
            .await
            .unwrap()
        else {
            panic!("large artifact must stream");
        };
        let first_chunk = owner.try_next().await.unwrap().unwrap();
        started.notified().await;
        let mut waiter =
            Box::pin(state.get_file("default".into(), "pkg".into(), id.clone(), "pkg.whl".into()));
        assert!(futures::poll!(&mut waiter).is_pending());
        let path = artifact_path("default", "pkg", &id, "pkg.whl");
        loop {
            let locks = state.0.download_locks.clone();
            if locks.has_waiters(&path) {
                break;
            }
            tokio::task::yield_now().await;
            assert!(futures::poll!(&mut waiter).is_pending());
        }
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        if cancel_owner {
            drop(owner);
            let File::Stream(stream) = waiter.await.unwrap() else {
                panic!("waiter must retry cancelled fill");
            };
            assert_eq!(
                stream.try_collect::<Vec<_>>().await.unwrap().concat(),
                bytes.as_ref()
            );
            assert_eq!(requests.load(Ordering::SeqCst), 2);
        } else {
            let File::Bytes(metadata) = state
                .get_file(
                    "default".into(),
                    "pkg".into(),
                    id.clone(),
                    "pkg.whl.metadata".into(),
                )
                .await
                .unwrap()
            else {
                panic!("metadata must not wait for wheel");
            };
            assert_eq!(metadata, b"metadata bytes".as_slice());
            assert_eq!(metadata_requests.load(Ordering::SeqCst), 1);
            // The same source in another package context has a distinct object key.
            let File::Stream(other) = state
                .get_file("default".into(), "other".into(), id, "pkg.whl".into())
                .await
                .unwrap()
            else {
                panic!("other context must progress");
            };
            assert_eq!(
                other.try_collect::<Vec<_>>().await.unwrap().concat(),
                bytes.as_ref()
            );
            release.notify_one();
            let mut chunks = vec![first_chunk];
            chunks.extend(owner.try_collect::<Vec<_>>().await.unwrap());
            assert_eq!(chunks.concat(), bytes.as_ref());
            let File::Stream(stream) = waiter.await.unwrap() else {
                panic!("completed artifact must be served from storage");
            };
            assert_eq!(
                stream.try_collect::<Vec<_>>().await.unwrap().concat(),
                bytes.as_ref()
            );
            assert_eq!(
                requests.load(Ordering::SeqCst),
                2,
                "only one fetch per distinct object"
            );
        }
        release.notify_one();
        assert!(state.0.download_locks.is_empty());
        assert_eq!(std::fs::read_dir(root.join(".tmp")).unwrap().count(), 0);
        upstream.abort();
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("coordinated artifact downloads must finish");
}

#[tokio::test]
async fn get_file_rejects_malformed_identifiers_before_storage_lookup() {
    let root = std::env::temp_dir().join(format!(
        "pipr-invalid-identifiers-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut settings = Settings::template();
    let mut config = opendal::services::FsConfig::default();
    config.root = Some(root.join("artifacts").to_str().unwrap().into());
    settings.storage = crate::storage::Storage::Fs(config);
    let state = State::try_from(settings).unwrap();
    let storage = state.0.storage.clone();
    let nonhex = "g".repeat(64);
    storage
        .write(
            format!("default/package/gg/gg/{nonhex}/package%2Ewhl").as_str(),
            "invalid-key artifact",
        )
        .await
        .unwrap();
    let invalid = [
        String::new(),
        "a".into(),
        "abc".into(),
        "a".repeat(63),
        "a".repeat(65),
        nonhex,
        "é".repeat(32),
        format!("€{}", "a".repeat(61)),
        format!("aé{}", "a".repeat(61)),
    ];
    for hash in invalid {
        assert!(
            matches!(
                state
                    .get_file(
                        "default".into(),
                        "package".into(),
                        hash.clone(),
                        "package.whl".into()
                    )
                    .await
                    .unwrap(),
                File::None
            ),
            "malformed identifier must not resolve: {hash:?}"
        );
    }
    // A valid cached identifier must still resolve without a database mapping.
    let hash = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    storage
        .write(
            format!("default/package/ab/cd/{hash}/package%2Ewhl").as_str(),
            "valid artifact",
        )
        .await
        .unwrap();
    let File::Stream(stream) = state
        .get_file(
            "default".into(),
            "package".into(),
            hash.into(),
            "package.whl".into(),
        )
        .await
        .unwrap()
    else {
        panic!("valid cached identifier must stream")
    };
    let chunks: Vec<Bytes> = stream.try_collect().await.unwrap();
    assert_eq!(chunks.concat(), b"valid artifact");
    drop(state);
    std::fs::remove_dir_all(root).unwrap();
}

async fn failed_download_fixture(
    body: Bytes,
    truncated: bool,
) -> (State, std::path::PathBuf, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/package%2Ewhl", listener.local_addr().unwrap());
    let upstream = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                assert!(request.len() < 8192, "request headers must be bounded");
                request.push(socket.read_u8().await.unwrap());
            }
            // Close each connection so retries receive a fresh response.
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len() + usize::from(truncated),
            );
            socket.write_all(header.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
            socket.shutdown().await.unwrap();
        }
    });
    let root = std::env::temp_dir().join(format!(
        "pipr-failed-download-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut settings = Settings::template();
    let mut config = opendal::services::FsConfig::default();
    config.root = Some(root.join("artifacts").to_str().unwrap().into());
    settings.storage = crate::storage::Storage::Fs(config);
    let state = State::try_from(settings).unwrap();
    let detail = Arc::new(PackageDetail {
        html: Bytes::new(),
        files: [(
            "abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab".into(),
            ArtifactSource {
                url,
                filename: "package.whl".into(),
            },
        )]
        .into(),
    });
    state
        .0
        .index_cache
        .insert("default/package".into(), detail)
        .await;
    (state, root, upstream)
}

#[tokio::test]
async fn failed_writer_close_aborts_cleans_up_and_allows_retry() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let body = Bytes::from(vec![42; CHUNK_SIZE * 2]);
        let (state, root, upstream) = failed_download_fixture(body.clone(), false).await;
        let File::Stream(mut stream) = state
            .get_file("default".into(), "package".into(), "abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab".into(), "package.whl".into())
            .await
            .unwrap()
        else {
            panic!("large download must stream")
        };
        // Start the writer, then prevent its final atomic rename from succeeding.
        assert!(!stream.try_next().await.unwrap().unwrap().is_empty());
        let object = root.join("artifacts/default/package/ab/cd/abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab/package%2Ewhl");
        std::fs::create_dir_all(&object).unwrap();
        assert!(stream.try_collect::<Vec<_>>().await.is_err());
        assert!(state.0.download_locks.is_empty());
        assert_eq!(
            std::fs::read_dir(root.join("artifacts/.tmp"))
                .unwrap()
                .count(),
            0,
            "failed writer must abort and remove its temporary artifact"
        );
        std::fs::remove_dir(&object).unwrap();
        let File::Stream(stream) = state
            .get_file("default".into(), "package".into(), "abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab".into(), "package.whl".into())
            .await
            .unwrap()
        else {
            panic!("retry must download again")
        };
        let chunks = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(chunks.concat(), body.as_ref());
        assert_eq!(std::fs::read(&object).unwrap(), body.as_ref());
        assert!(state.0.download_locks.is_empty());
        upstream.abort();
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("download failure handling must finish");
}

#[tokio::test]
async fn upstream_body_failure_aborts_without_publishing_and_cleans_up() {
    tokio::time::timeout(Duration::from_secs(10), async {
        // Both the initial buffered read and a read after streaming has started can fail.
        for size in [64, CHUNK_SIZE * 2] {
            let (state, root, upstream) =
                failed_download_fixture(Bytes::from(vec![42; size]), true).await;
            match state
                .get_file("default".into(), "package".into(), "abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab".into(), "package.whl".into())
                .await
            {
                Err(_) if size < CHUNK_SIZE => {}
                Ok(File::Stream(stream)) if size > CHUNK_SIZE => {
                    assert!(stream.try_collect::<Vec<_>>().await.is_err());
                }
                _ => panic!("truncated response must fail"),
            }
            assert!(state.0.download_locks.is_empty());
            assert!(
                !root
                    .join("artifacts/default/package/ab/cd/abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab/package%2Ewhl")
                    .exists()
            );
            assert_eq!(
                std::fs::read_dir(root.join("artifacts/.tmp"))
                    .unwrap()
                    .count(),
                0
            );
            upstream.abort();
            drop(state);
            std::fs::remove_dir_all(root).unwrap();
        }
    })
    .await
    .expect("download failure handling must finish");
}

#[tokio::test]
async fn waiting_download_allows_active_download_to_finish() {
    let root = std::env::temp_dir().join(format!(
        "pipr-download-lock-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut settings = Settings::template();
    let mut config = opendal::services::FsConfig::default();
    config.root = Some(root.join("artifacts").to_str().unwrap().into());
    settings.storage = crate::storage::Storage::Fs(config);
    let state = State::try_from(settings).unwrap();
    let hash = "abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab";
    let locks = state.0.download_locks.clone();
    let path = artifact_path("default", "package", hash, "package.whl");
    let lease = locks.lease(path);
    let active_download = lease.entry.lock().await;

    let mut waiting_download = Box::pin(state.get_file(
        "default".into(),
        "package".into(),
        hash.into(),
        "package.whl".into(),
    ));
    // Polling to Pending puts the request at the held file lock without sleeps.
    assert!(futures::poll!(&mut waiting_download).is_pending());
    let storage = state.0.storage.clone();
    storage
        .write(
            "default/package/ab/cd/abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab/package%2Ewhl",
            Bytes::from_static(b"finished artifact"),
        )
        .await
        .unwrap();
    drop(active_download);
    drop(lease);

    let file = tokio::time::timeout(Duration::from_secs(1), &mut waiting_download)
        .await
        .expect("waiting request must resume after the active download")
        .unwrap();
    let File::Stream(stream) = file else {
        panic!("filesystem cache hit must stream")
    };
    let chunks: Vec<Bytes> = stream.try_collect().await.unwrap();
    assert_eq!(chunks.concat(), b"finished artifact");
    drop(waiting_download);
    drop(state);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn filesystem_cache_hit_streams_exact_bytes_without_database_mapping() {
    let root = std::env::temp_dir().join(format!(
        "pipr-storage-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut config = opendal::services::FsConfig::default();
    config.root = Some(root.to_str().unwrap().into());
    let storage = crate::storage::Storage::Fs(config).operator().unwrap();
    let path = "default/package/ab/cd/identifier/package%2Ewhl";
    let mut writer = storage
        .writer_with(path)
        .content_type("application/octet-stream")
        .chunk(CHUNK_SIZE)
        .await
        .unwrap();
    writer.write(Bytes::from_static(b"cached ")).await.unwrap();
    assert!(
        storage.stat(path).await.is_err(),
        "unfinished artifact must stay hidden"
    );
    writer
        .write(Bytes::from_static(b"wheel bytes"))
        .await
        .unwrap();
    writer.close().await.unwrap();
    let File::Stream(stream) = cached_file(&storage, path).await.unwrap() else {
        panic!("filesystem cache hits must stream")
    };
    let chunks: Vec<Bytes> = stream.try_collect().await.unwrap();
    assert_eq!(chunks.concat(), b"cached wheel bytes");
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "storage-s3")]
#[tokio::test]
async fn s3_cache_hit_presigns_existing_object_path() {
    opendal::install_default();
    let storage: crate::storage::Storage = toml::from_str(
        "[s3]\nbucket = 'packages'\nregion = 'us-east-1'\nendpoint = 'https://objects.example.test'\naccess_key_id = 'test-key'\nsecret_access_key = 'test-secret'\ndisable_config_load = true\ndisable_ec2_metadata = true\n"
    ).unwrap();
    let storage = storage.operator().unwrap();
    let File::PresignedUrl(url) = cached_file(&storage, "ab/cd/package.whl").await.unwrap() else {
        panic!("S3 cache hits must redirect when signature needs no extra headers")
    };
    let url = Url::parse(&url).unwrap();
    assert_eq!(url.host_str(), Some("objects.example.test"));
    assert_eq!(url.path(), "/packages/ab/cd/package.whl");
    assert!(url.query_pairs().any(|(name, _)| name == "X-Amz-Signature"));
    assert!(
        url.query_pairs()
            .any(|(name, value)| name == "X-Amz-Expires" && value == "600")
    );
}
