use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::{http::get_router, settings::Settings};

// mod models;
mod artifact;
mod coordination;
mod http;
mod index;
mod paths;
mod settings;
mod shutdown;
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
        #[cfg(feature = "config-template")]
        {
            print!("{}", Settings::template());
            return;
        }
        #[cfg(not(feature = "config-template"))]
        {
            tracing::error!("--print-settings-template requires the config-template feature");
            std::process::exit(1);
        }
    }

    shutdown::exit(serve().await);
}

async fn serve() -> anyhow::Result<()> {
    let settings = Settings::new()?;
    let bind = settings.bind;
    let path_prefix = settings.path_prefix.clone();
    let state: state::State = settings.try_into()?;
    let shutdown = state.0.shutdown.clone();
    let router = get_router(&path_prefix);

    let listener = TcpListener::bind(bind).await?;
    tracing::info!("Listening on http://{bind}");

    shutdown.serve(listener, router.with_state(state)).await
}
