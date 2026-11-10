use tokio::{net::TcpListener, signal};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::{http::get_router, settings::Settings};

// mod models;
mod artifact;
mod coordination;
mod http;
mod index;
mod paths;
mod settings;
mod state;
mod storage;
mod upstream;

#[cfg(test)]
mod test_support;

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
    let bind = settings.bind;
    let router = get_router(&settings.path_prefix);
    let state: state::State = settings.try_into()?;

    let listener = TcpListener::bind(bind).await?;
    tracing::info!("Listening on http://{bind}");

    axum::serve(listener, router.with_state(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tracing::info!("Finished");

    Ok(())
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
