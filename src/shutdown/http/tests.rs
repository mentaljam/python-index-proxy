use std::time::Duration;

use bytes::Bytes;

use super::*;

#[tokio::test]
async fn shutdown_interrupts_pending_response_bodies_and_releases_their_registration() {
    let shutdown = Shutdown::default();
    let body = Body::from_stream(futures::stream::pending::<Result<Bytes, std::io::Error>>());
    let mut stream = response_body(body, shutdown.clone(), shutdown.token()).into_data_stream();
    assert!(futures::poll!(stream.next()).is_pending());
    shutdown.cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("shutdown must interrupt a pending response body")
            .unwrap()
            .is_err()
    );
    drop(stream);
    tokio::time::timeout(Duration::from_secs(1), shutdown.finish(async { Ok(()) }))
        .await
        .expect("dropping an interrupted body must release its work registration")
        .unwrap();
}
