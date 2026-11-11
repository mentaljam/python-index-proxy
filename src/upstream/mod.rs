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
    let mut operation = crate::telemetry::Operation::start("upstream_artifact_headers");
    let result = tokio::time::timeout(settings.response_header_timeout, client.get(url).send())
        .await
        .context("Upstream response header timeout")
        .context(UpstreamIndexError)
        .and_then(|response| response.context(UpstreamIndexError));
    if let Ok(response) = &result {
        crate::telemetry::upstream_response("artifact", response.status().as_u16());
        operation.finish(if response.status().is_success() {
            "success"
        } else {
            "http_error"
        });
    } else {
        operation.result(&result);
    }
    result
}

pub(crate) async fn chunk(
    response: &mut Response,
    settings: Upstream,
) -> anyhow::Result<Option<Bytes>> {
    // Start the deadline only while polling upstream, excluding storage writes
    // and downstream backpressure between reads.
    let mut operation = crate::telemetry::Operation::start("upstream_read");
    let result = tokio::time::timeout(settings.read_timeout, response.chunk())
        .await
        .context("Upstream read timeout")
        .context(UpstreamIndexError)
        .and_then(|response| response.context(UpstreamIndexError));
    if let Ok(Some(bytes)) = &result {
        crate::telemetry::upstream_bytes(bytes.len());
    }
    operation.result(&result);
    result
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
    let mut operation = crate::telemetry::Operation::start("upstream_index");
    let result = tokio::time::timeout(settings.index_download_timeout, async {
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
        crate::telemetry::upstream_response("index", status.as_u16());
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
            "Upstream index returned HTTP {}",
            response.status().as_u16()
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
    .context(UpstreamIndexError)
    .and_then(|page| page.context(UpstreamIndexError));
    match &result {
        Ok(None) => operation.finish("skipped"),
        _ => operation.result(&result),
    }
    result
}

#[cfg(test)]
mod tests;
