use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

use super::*;

async fn socket_fixture() -> (Url, tokio::net::TcpListener) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    (
        format!("http://{}/", listener.local_addr().unwrap())
            .parse()
            .unwrap(),
        listener,
    )
}

async fn request(listener: tokio::net::TcpListener) -> tokio::net::TcpStream {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut buffer = Vec::new();
    while !buffer.ends_with(b"\r\n\r\n") {
        buffer.push(socket.read_u8().await.unwrap());
    }
    socket
}

#[tokio::test]
async fn header_deadline_and_active_read_deadline_are_independent() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (url, listener) = socket_fixture().await;
        let server = tokio::spawn(async move {
            let mut socket = request(listener).await;
            assert_eq!(socket.read(&mut [0]).await.unwrap(), 0);
        });
        let settings = Upstream {
            response_header_timeout: Duration::from_millis(50),
            ..Upstream::default()
        };
        let error = send(&Client::new(), url, settings).await.unwrap_err();
        assert!(error.is::<UpstreamIndexError>());
        assert!(format!("{error:#}").contains("header timeout"));
        server.await.unwrap();

        let (url, listener) = socket_fixture().await;
        let (release, wait) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut socket = request(listener).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\na")
                .await
                .unwrap();
            wait.await.unwrap();
            socket.write_all(b"b").await.unwrap();
            // Keep the server connection alive until the response is dropped.
            let _ = socket.read(&mut [0]).await;
        });
        let settings = Upstream {
            read_timeout: Duration::from_millis(50),
            ..Upstream::default()
        };
        let mut response = send(&Client::new(), url, settings).await.unwrap();
        assert_eq!(chunk(&mut response, settings).await.unwrap().unwrap(), "a");
        // Time spent without polling upstream is downstream/storage backpressure.
        tokio::time::sleep(Duration::from_millis(100)).await;
        release.send(()).unwrap();
        assert_eq!(chunk(&mut response, settings).await.unwrap().unwrap(), "b");
        drop(response);
        server.abort();

        let (url, listener) = socket_fixture().await;
        let server = tokio::spawn(async move {
            let mut socket = request(listener).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n")
                .await
                .unwrap();
            assert_eq!(socket.read(&mut [0]).await.unwrap(), 0);
        });
        let mut response = send(&Client::new(), url, settings).await.unwrap();
        let error = chunk(&mut response, settings).await.unwrap_err();
        assert!(error.is::<UpstreamIndexError>());
        assert!(format!("{error:#}").contains("read timeout"));
        drop(response);
        server.await.unwrap();
    })
    .await
    .expect("header/read deadlines must bound stalled upstreams");
}

#[tokio::test]
async fn total_index_deadline_stops_continuously_progressing_body() {
    let (url, listener) = socket_fixture().await;
    let server = tokio::spawn(async move {
        let mut socket = request(listener).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .await
            .unwrap();
        loop {
            if socket.write_all(b"1\r\nx\r\n").await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    let settings = Upstream {
        read_timeout: Duration::from_millis(200),
        index_download_timeout: Duration::from_millis(100),
        ..Upstream::default()
    };
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        index_page(&Client::new(), url, settings, 1024),
    )
    .await
    .expect("total deadline must stop a progressing body")
    .unwrap_err();
    assert!(error.is::<UpstreamIndexError>());
    assert!(format!("{error:#}").contains("download timeout"));
    server.abort();
}

#[tokio::test]
async fn chunked_page_limit_accepts_exact_boundary_and_rejects_one_byte_more() {
    for (body, accepted) in [("abcd", true), ("abcde", false)] {
        let (url, listener) = socket_fixture().await;
        let server = tokio::spawn(async move {
            let mut socket = request(listener).await;
            socket.write_all(format!("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{body}\r\n0\r\n\r\n", body.len()).as_bytes()).await.unwrap();
        });
        let result = index_page(&Client::new(), url, Upstream::default(), 4).await;
        if accepted {
            assert_eq!(result.unwrap().unwrap().content, "abcd");
        } else {
            let error = result.unwrap_err();
            assert!(error.is::<UpstreamIndexError>());
            assert!(format!("{error:#}").contains("exceeds 4 bytes"));
        }
        server.await.unwrap();
    }
}
