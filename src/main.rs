use tokio::net::TcpListener;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    http::get_router,
    settings::{LogFormat, Settings},
};

mod artifact;
mod coordination;
mod http;
mod index;
mod logging;
mod models;
mod paths;
mod settings;
mod shutdown;
mod state;
mod storage;
mod telemetry;
mod upstream;

#[cfg(test)]
mod test_support;

#[tokio::main]
async fn main() {
    #[cfg(feature = "config-template")]
    if std::env::args().any(|a| &a == "--print-settings-template") {
        print!("{}", Settings::template());
        return;
    }

    opendal::install_default();
    // Install an explicit provider for the HTTP clients.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let settings = match Settings::new() {
        Ok(settings) => settings,
        Err(err) => {
            eprintln!("Invalid configuration: {}", logging::config_error(&err));
            std::process::exit(1);
        }
    };

    let subscriber = tracing_subscriber::registry().with(
        EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .from_env_lossy(),
    );
    match settings.log_format {
        LogFormat::Text => subscriber.with(tracing_subscriber::fmt::layer()).init(),
        LogFormat::Json => subscriber
            .with(tracing_subscriber::fmt::layer().json())
            .init(),
    }

    shutdown::exit(serve(settings).await);
}

async fn serve(settings: Settings) -> anyhow::Result<()> {
    let bind = settings.bind;
    let path_prefix = settings.path_prefix.clone();
    let state: state::State = settings.try_into()?;
    let shutdown = state.0.shutdown.clone();
    let router = telemetry::install(get_router(&path_prefix), &path_prefix)?;

    let listener = TcpListener::bind(bind).await?;
    tracing::info!(bind = %bind, "Listening");

    shutdown.serve(listener, router.with_state(state)).await
}
