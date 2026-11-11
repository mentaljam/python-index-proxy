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
mod models;
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
            eprintln!("{err}");
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
    let router = get_router(&path_prefix);

    let listener = TcpListener::bind(bind).await?;
    tracing::info!("Listening on http://{bind}");

    shutdown.serve(listener, router.with_state(state)).await
}
