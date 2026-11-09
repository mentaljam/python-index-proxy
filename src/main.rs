use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use state::File;
use tokio::{net::TcpListener, signal};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::settings::Settings;

// mod models;
mod settings;
mod state;
mod storage;

type AppState = State<state::State>;

#[tokio::main]
async fn main() {
    opendal::install_default();
    // Install an explicit provider for the HTTP clients.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    if std::env::args().any(|a| &a == "--print-settings-template") {
        let template = toml::to_string_pretty(&Settings::template()).unwrap();
        print!("{template}");
        std::process::exit(0)
    }

    if let Err(err) = serve().await {
        tracing::error!("{err:#}");
        std::process::exit(1);
    }
}

async fn serve() -> anyhow::Result<()> {
    let settings = Settings::new()?;

    let listener = TcpListener::bind(&settings.bind).await?;
    tracing::info!("Listening on http://{}", settings.bind);

    let router = get_router(&settings.base_url);
    let state: state::State = settings.try_into()?;

    axum::serve(listener, router.with_state(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tracing::info!("Finished");

    Ok(())
}

fn get_router(base_url: &str) -> Router<state::State> {
    let router = Router::new()
        .route("/", get(list_indices))
        .route("/{index}/{package}/", get(get_detail))
        .route("/packages/{index}/{package}/{hash}/{file}", get(get_file));

    if base_url == "/" {
        router
    } else {
        Router::new().nest(base_url, router)
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

async fn list_indices(State(state): AppState) -> Response {
    let body = state.get_indices_list().await;
    Response::builder()
        .header(header::CONTENT_TYPE, "text/html")
        .body(body.into())
        .unwrap()
}

async fn get_detail(
    State(state): AppState,
    Path((index, package)): Path<(String, String)>,
) -> Response {
    match state.get_detail(index, package).await {
        Ok(Some(index)) => ([(header::CONTENT_TYPE, "text/html")], index).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            tracing::error!("{err:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn get_file(
    State(state): AppState,
    Path((index, package, hash, file)): Path<(String, String, String, String)>,
) -> Response {
    match state.get_file(index, package, hash, file).await {
        Ok(File::PresignedUrl(url)) => {
            (StatusCode::TEMPORARY_REDIRECT, [(header::LOCATION, url)]).into_response()
        }
        Ok(File::Bytes(bytes)) => bytes.into_response(),
        Ok(File::Stream(stream)) => Body::from_stream(stream).into_response(),
        Ok(File::None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            tracing::error!("{err:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
