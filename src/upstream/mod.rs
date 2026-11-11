use anyhow::Context;
use bytes::{Bytes, BytesMut};
use reqwest::{Client, Response};
use url::Url;

use crate::{index::UpstreamIndexError, models::IndexFormat, settings::Upstream};

pub(crate) async fn send(
    client: &Client,
    url: Url,
    settings: Upstream,
) -> anyhow::Result<Response> {
    tokio::time::timeout(settings.response_header_timeout, client.get(url).send())
        .await
        .context("Upstream response header timeout")
        .context(UpstreamIndexError)?
        .context(UpstreamIndexError)
}

pub(crate) async fn chunk(
    response: &mut Response,
    settings: Upstream,
) -> anyhow::Result<Option<Bytes>> {
    // Start the deadline only while polling upstream, excluding storage writes
    // and downstream backpressure between reads.
    tokio::time::timeout(settings.read_timeout, response.chunk())
        .await
        .context("Upstream read timeout")
        .context(UpstreamIndexError)?
        .context(UpstreamIndexError)
}

#[derive(Debug)]
pub(crate) struct IndexPage {
    pub(crate) url: Url,
    pub(crate) format: IndexFormat,
    pub(crate) content: Bytes,
}

pub(crate) async fn index_page(
    client: &Client,
    url: Url,
    settings: Upstream,
    max_size: u64,
) -> anyhow::Result<Option<IndexPage>> {
    tokio::time::timeout(settings.index_download_timeout, async {
        let mut response = tokio::time::timeout(
            settings.response_header_timeout,
            client
                .get(url)
                .header(reqwest::header::ACCEPT, crate::models::INDEX_ACCEPT)
                .send(),
        )
        .await
        .context("Upstream response header timeout")??;
        let status = response.status();
        if status.is_client_error()
            && !matches!(
                status,
                reqwest::StatusCode::REQUEST_TIMEOUT | reqwest::StatusCode::TOO_MANY_REQUESTS
            )
        {
            return Ok(None);
        }
        anyhow::ensure!(
            response.status().is_success(),
            "Upstream index {} returned {}",
            response.url(),
            response.status()
        );
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        let format = IndexFormat::from_content_type(content_type)?;
        let url = response.url().clone();
        let mut content = BytesMut::new();
        while let Some(chunk) = chunk(&mut response, settings).await? {
            anyhow::ensure!(
                (content.len() as u64).saturating_add(chunk.len() as u64) <= max_size,
                "Upstream index page exceeds {max_size} bytes"
            );
            content.extend_from_slice(&chunk);
        }
        Ok(Some(IndexPage {
            url,
            format,
            content: content.freeze(),
        }))
    })
    .await
    .context("Upstream index download timeout")
    .context(UpstreamIndexError)?
    .context(UpstreamIndexError)
}

#[cfg(test)]
mod tests;
