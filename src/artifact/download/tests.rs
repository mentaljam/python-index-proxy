use std::time::Duration;

use reqwest::Client;

use crate::{settings::Settings, state::State};

use super::*;

async fn cancellation_fixture(
    prefix_size: usize,
) -> (
    State,
    std::path::PathBuf,
    reqwest::Response,
    tokio::task::JoinHandle<()>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/file", listener.local_addr().unwrap());
    let upstream = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(socket.read_u8().await.unwrap());
            assert!(request.len() < 8192);
        }
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    CHUNK_SIZE * 4
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        socket.write_all(&vec![42; prefix_size]).await.unwrap();
        // No further body is sent. Cancellation must close the upstream socket.
        assert_eq!(socket.read(&mut [0]).await.unwrap(), 0);
    });
    let root = std::env::temp_dir().join(format!(
        "pipr-cancel-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut settings = Settings::template();
    let mut config = opendal::services::FsConfig::default();
    config.root = Some(root.to_str().unwrap().into());
    settings.storage = crate::storage::Storage::Fs(config);
    let state = State::try_from(settings).unwrap();
    let response = Client::new().get(url).send().await.unwrap();
    (state, root, response, upstream)
}

#[tokio::test]
async fn dropped_initial_reply_or_unpolled_body_aborts_and_releases_registration() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for prefix_size in [0, CHUNK_SIZE + 1] {
            let (state, root, response, upstream) = cancellation_fixture(prefix_size).await;
            let (reply, result) = oneshot::channel();
            let download_state = state.0.clone();
            let mut download = Box::pin(async move {
                download_response(
                    &download_state,
                    "artifact.whl".into(),
                    async move { Ok(Some(response)) },
                    reply,
                )
                .await
            });
            assert!(futures::poll!(&mut download).is_pending());
            if prefix_size == 0 {
                assert!(state.0.download_locks.contains_key("artifact.whl"));
                drop(result);
                download.await;
            } else {
                let owner = tokio::spawn(download);
                let File::Stream(stream) = result.await.unwrap().unwrap() else {
                    panic!("large body must stream");
                };
                // Activate the writer without polling the body itself.
                while std::fs::read_dir(root.join(".tmp"))
                    .unwrap()
                    .next()
                    .is_none()
                {
                    tokio::task::yield_now().await;
                }
                drop(stream);
                owner.await.unwrap();
            }
            upstream.await.unwrap();
            assert!(state.0.download_locks.is_empty());
            assert!(!root.join("artifact.whl").exists());
            assert_eq!(std::fs::read_dir(root.join(".tmp")).unwrap().count(), 0);
            drop(state);
            std::fs::remove_dir_all(root).unwrap();
        }
    })
    .await
    .expect("cancelled owner must stop upstream and finish cleanup");
}

#[tokio::test]
async fn cancelled_waiter_preserves_active_download_registration() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (state, root, response, upstream) = cancellation_fixture(0).await;
        let locks = state.0.download_locks.clone();
        let lease = locks.lease("artifact.whl".into());
        let active = lease.entry.lock().await;
        let (reply, result) = oneshot::channel();
        let download_state = state.0.clone();
        let mut download = Box::pin(async move {
            download_response(
                &download_state,
                "artifact.whl".into(),
                async move { Ok(Some(response)) },
                reply,
            )
            .await
        });
        assert!(futures::poll!(&mut download).is_pending());
        drop(result);
        download.await;
        upstream.await.unwrap();
        assert!(locks.contains_key("artifact.whl"));
        drop(active);
        drop(lease);
        assert!(state.0.download_locks.is_empty());
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("cancelled waiter must retain registration until safe cleanup");
}

#[tokio::test]
async fn upstream_read_timeout_aborts_and_releases_download_budget() {
    tokio::time::timeout(Duration::from_secs(5), async {
        use futures::TryStreamExt;

        for prefix in [0, CHUNK_SIZE + 1] {
            let (mut state, root, response, upstream) = cancellation_fixture(prefix).await;
            std::sync::Arc::get_mut(&mut state.0)
                .unwrap()
                .upstream
                .read_timeout = Duration::from_millis(50);
            let permits = state.0.artifact_permits.clone();
            let (reply, result) = oneshot::channel();
            let download_state = state.0.clone();
            let owner = tokio::spawn(async move {
                download_response(
                    &download_state,
                    "artifact.whl".into(),
                    async move { Ok(Some(response)) },
                    reply,
                )
                .await
            });
            let error = match result.await.unwrap() {
                Err(error) => error,
                Ok(File::Stream(stream)) => stream.try_collect::<Vec<_>>().await.unwrap_err(),
                Ok(_) => panic!("stalled transfer must fail"),
            };
            assert!(error.is::<crate::index::UpstreamIndexError>());
            owner.await.unwrap();
            upstream.await.unwrap();
            assert_eq!(permits.available_permits(), 16);
            assert!(state.0.download_locks.is_empty());
            assert!(!root.join("artifact.whl").exists());
            if prefix > 0 {
                assert_eq!(std::fs::read_dir(root.join(".tmp")).unwrap().count(), 0);
            }
            if root.exists() {
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    })
    .await
    .expect("read timeout must clean up stalled transfers");
}
