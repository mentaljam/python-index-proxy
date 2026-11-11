use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use futures::StreamExt;
use tokio::{net::TcpListener, signal};
use tokio_util::{sync::CancellationToken, task::task_tracker::TaskTrackerToken};

use super::Shutdown;

impl Shutdown {
    pub(crate) async fn serve(&self, listener: TcpListener, router: Router) -> anyhow::Result<()> {
        let router = router.layer(middleware::from_fn_with_state(self.clone(), requests));
        let stop_accepting = CancellationToken::new();
        let server = axum::serve(listener, router)
            .with_graceful_shutdown(stop_accepting.clone().cancelled_owned());
        let mut server = std::pin::pin!(std::future::IntoFuture::into_future(server));
        tokio::select! {
            result = &mut server => result?,
            _ = shutdown_signal() => {
                stop_accepting.cancel();
                self.finish(server).await?;
            }
        }
        tracing::info!("Finished");
        Ok(())
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tracing::info!("Started");

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("Stopping");
}

async fn requests(State(shutdown): State<Shutdown>, request: Request, next: Next) -> Response {
    let work = shutdown.token();
    let Some(response) = shutdown.until_cancelled(next.run(request)).await else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let (parts, body) = response.into_parts();
    Response::from_parts(parts, response_body(body, shutdown, work))
}

fn response_body(body: Body, shutdown: Shutdown, work: TaskTrackerToken) -> Body {
    let mut body = body.into_data_stream();
    let stream = async_stream::stream! {
        // Axum can signal connection completion before dropping its response.
        // Keep the body counted until it completes or is dropped.
        let _work = work;
        loop {
            match shutdown.until_cancelled(body.next()).await {
                Some(Some(chunk)) => yield chunk,
                Some(None) => break,
                None => {
                    yield Err(axum::Error::new(std::io::Error::other("Application shutting down")));
                    break;
                }
            }
        }
    };
    Body::from_stream(stream)
}

#[cfg(test)]
mod tests;
