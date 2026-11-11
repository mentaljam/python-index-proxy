use std::sync::Arc;
#[cfg(feature = "storage-s3")]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "storage-s3")]
use axum::{Router, body::Bytes, http::StatusCode, response::IntoResponse};
use opendal::raw::oio::ReadStream;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use super::*;

fn deadlines() -> StorageHttp {
    StorageHttp {
        connect_timeout: Duration::from_secs(1),
        response_header_timeout: Duration::from_millis(60),
        read_timeout: Duration::from_millis(60),
    }
}

#[cfg(feature = "storage-s3")]
async fn serve(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (endpoint, task)
}

#[cfg(feature = "storage-s3")]
fn s3(endpoint: &str) -> crate::storage::Storage {
    toml::from_str(&format!(
        "[s3]\nbucket = 'packages'\nregion = 'us-east-1'\nendpoint = '{endpoint}'\nskip_signature = true\ndisable_config_load = true\ndisable_ec2_metadata = true\n"
    ))
    .unwrap()
}

#[cfg(feature = "storage-s3")]
#[tokio::test]
async fn storage_stat_timeout_and_permission_errors_do_not_fetch_upstream_and_can_recover() {
    let storage_requests = Arc::new(AtomicUsize::new(0));
    let upstream_requests = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route(
            "/packages/{*key}",
            axum::routing::head({
                let requests = storage_requests.clone();
                move || {
                    let turn = requests.fetch_add(1, Ordering::SeqCst);
                    async move {
                        match turn {
                            0 => {
                                tokio::time::sleep(Duration::from_millis(250)).await;
                                StatusCode::NOT_FOUND.into_response()
                            }
                            1 => StatusCode::FORBIDDEN.into_response(),
                            _ => [(::http::header::CONTENT_LENGTH, "7")].into_response(),
                        }
                    }
                }
            }),
        )
        .route(
            "/upstream/{*key}",
            axum::routing::get({
                let requests = upstream_requests.clone();
                move || {
                    requests.fetch_add(1, Ordering::SeqCst);
                    async { "unexpected upstream access" }
                }
            }),
        );
    let (endpoint, server) = serve(router).await;
    let mut settings = crate::settings::Settings::for_test();
    settings.storage = s3(&endpoint);
    settings.storage_http = deadlines();
    settings.indices = [(
        "default".into(),
        vec![format!("{endpoint}/upstream/{{package}}/")],
    )]
    .into();
    let state = crate::state::State::try_from(settings).unwrap();
    for expected in ["storage HTTP timeout reached", "PermissionDenied"] {
        let error = state
            .get_file(
                "default".into(),
                "pkg".into(),
                "a".repeat(64),
                "pkg.whl".into(),
            )
            .await
            .err()
            .expect("storage failure must fail the request");
        assert!(error.to_string().contains(expected), "{error:#}");
    }
    let recovered = state
        .get_file(
            "default".into(),
            "pkg".into(),
            "a".repeat(64),
            "pkg.whl".into(),
        )
        .await
        .unwrap();
    assert!(matches!(recovered, crate::artifact::File::PresignedUrl(_)));
    assert_eq!(
        storage_requests.load(Ordering::SeqCst),
        3,
        "storage requests must not be retried"
    );
    assert_eq!(
        upstream_requests.load(Ordering::SeqCst),
        0,
        "storage failures must not become cache misses"
    );
    server.abort();
}

async fn request(listener: &TcpListener) -> TcpStream {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut headers = Vec::new();
    while !headers.ends_with(b"\r\n\r\n") {
        headers.push(socket.read_u8().await.unwrap());
    }
    socket
}

#[tokio::test]
async fn stalled_storage_body_times_out_and_a_new_request_recovers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/object", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut stalled = request(&listener).await;
        stalled
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nabc")
            .await
            .unwrap();
        let mut recovered = request(&listener).await;
        recovered
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nabcdef")
            .await
            .unwrap();
    });
    let transport = transport(deadlines()).unwrap();
    let mut body = transport
        .fetch(Request::get(&endpoint).body(Buffer::new()).unwrap())
        .await
        .unwrap()
        .into_body();
    assert_eq!(body.read().await.unwrap().to_bytes(), "abc");
    let error = tokio::time::timeout(Duration::from_secs(2), body.read())
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("storage_http::read"), "{error}");
    let response = transport
        .fetch(Request::get(&endpoint).body(Buffer::new()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        response.into_body().to_buffer().await.unwrap().to_bytes(),
        "abcdef"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn storage_reads_allow_slow_consumers_and_progress_beyond_each_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/object", listener.local_addr().unwrap());
    let release = Arc::new(tokio::sync::Notify::new());
    let server = tokio::spawn({
        let release = release.clone();
        async move {
            let mut socket = request(&listener).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n")
                .await
                .unwrap();
            release.notified().await;
            for letter in b"bcdefg" {
                tokio::time::sleep(Duration::from_millis(20)).await;
                socket
                    .write_all(&[b'1', b'\r', b'\n', *letter, b'\r', b'\n'])
                    .await
                    .unwrap();
            }
            socket.write_all(b"0\r\n\r\n").await.unwrap();
        }
    });
    let transport = transport(deadlines()).unwrap();
    let mut body = transport
        .fetch(Request::get(endpoint).body(Buffer::new()).unwrap())
        .await
        .unwrap()
        .into_body();
    let mut collected = body.read().await.unwrap().to_vec();
    tokio::time::sleep(Duration::from_millis(150)).await;
    release.notify_one();
    loop {
        let chunk = body.read().await.unwrap();
        if chunk.is_empty() {
            break;
        }
        collected.extend(chunk.to_vec());
    }
    assert_eq!(collected, b"abcdefg");
    server.await.unwrap();
}

#[cfg(feature = "storage-s3")]
#[tokio::test]
async fn storage_upload_header_timeout_returns_through_writer_and_new_upload_recovers() {
    let requests = Arc::new(AtomicUsize::new(0));
    let router = Router::new().route(
        "/packages/probe.whl",
        axum::routing::put({
            let requests = requests.clone();
            move |body: Bytes| {
                let turn = requests.fetch_add(1, Ordering::SeqCst);
                async move {
                    assert_eq!(body, "complete wheel");
                    if turn == 0 {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                    StatusCode::OK
                }
            }
        }),
    );
    let (endpoint, server) = serve(router).await;
    let storage = s3(&endpoint).operator(deadlines()).unwrap();
    let mut writer = storage.writer("probe.whl").await.unwrap();
    writer.write("complete wheel").await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), writer.close())
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        error.to_string().contains("storage_http::response_headers"),
        "{error}"
    );
    // Cleanup may fail for a backend after a failed close, but it must return.
    let _ = tokio::time::timeout(Duration::from_secs(2), writer.abort())
        .await
        .unwrap();
    let mut retry = storage.writer("probe.whl").await.unwrap();
    retry.write("complete wheel").await.unwrap();
    retry.close().await.unwrap();
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    server.abort();
}
