use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use reqwest::Client;
use tokio::sync::Mutex;
use url::Url;

use crate::{
    coordination::ResourceLimit,
    paths::{encode_segment, valid_filename},
    settings::Upstream,
    state::State,
};

mod parser;

use parser::DetailParser;

#[derive(Debug)]
pub struct UpstreamIndexError;

impl std::fmt::Display for UpstreamIndexError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Failed to fetch upstream package index")
    }
}

impl State {
    pub async fn get_detail(
        &self,
        index_slug: String,
        package: String,
    ) -> anyhow::Result<Option<Bytes>> {
        Ok(self
            .package_detail(&index_slug, &package, None)
            .await?
            .map(|detail| detail.html.clone()))
    }

    pub(crate) async fn package_detail(
        &self,
        index_slug: &str,
        package: &str,
        artifact_id: Option<&str>,
    ) -> anyhow::Result<Option<Arc<PackageDetail>>> {
        if !valid_filename(index_slug) || !valid_filename(package) {
            return Ok(None);
        }
        let (index_cache, http_client, base_url, url_templates) = {
            let data = self.0.read().await;
            (
                data.index_cache.clone(),
                data.http_client.clone(),
                data.base_url.clone(),
                data.indices.get(index_slug).cloned(),
            )
        };
        let cache_key = format!("{}/{}", encode_segment(index_slug), encode_segment(package));
        if let Some(detail) = index_cache.get(&cache_key).await
            && artifact_id.is_none_or(|id| detail.files.contains_key(id))
        {
            return Ok(Some(detail));
        }
        let Some(url_templates) = url_templates else {
            return Ok(None);
        };
        let refreshes = self.0.read().await.index_refreshes.clone();
        let lease = refreshes.lease(cache_key.clone());
        let revision = lease.entry.revision.load(Ordering::Acquire);
        let mut previous = lease.entry.result.lock().await;
        if lease.entry.revision.load(Ordering::Acquire) != revision
            && let Some(result) = previous.as_ref()
        {
            return share_refresh_result(result);
        }
        // Another request may have populated the cache before this lease was acquired.
        if let Some(detail) = index_cache.get(&cache_key).await
            && artifact_id.is_none_or(|id| detail.files.contains_key(id))
        {
            return Ok(Some(detail));
        }
        let (permits, upstream, max_size) = {
            let data = self.0.read().await;
            (
                data.index_permits.clone(),
                data.upstream,
                data.limits.max_index_page_size,
            )
        };
        let result = match permits.try_acquire_owned() {
            Ok(permit) => {
                Self::fetch_package_detail(
                    index_slug,
                    package,
                    &base_url,
                    url_templates,
                    http_client,
                    upstream,
                    max_size,
                    Arc::new(permit),
                )
                .await
            }
            Err(_) => Err(anyhow::Error::new(ResourceLimit)),
        };
        if let Ok(Some(detail)) = &result {
            index_cache.insert(cache_key, detail.clone()).await;
        }
        let result = result.map_err(Arc::new);
        let shared = share_refresh_result(&result);
        *previous = Some(result);
        lease.entry.revision.fetch_add(1, Ordering::Release);
        shared
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Explicit immutable refresh inputs and owned permit"
    )]
    async fn fetch_package_detail(
        index_slug: &str,
        package: &str,
        base_url: &str,
        url_templates: Vec<IndexUrlTemplate>,
        http_client: Client,
        upstream: Upstream,
        max_size: u64,
        permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> anyhow::Result<Option<Arc<PackageDetail>>> {
        let mut parser = DetailParser::new(base_url, index_slug, package);
        for template in url_templates {
            let Some((final_url, content)) = crate::upstream::index_page(
                &http_client,
                template.package_url(package)?,
                upstream,
                max_size,
            )
            .await?
            else {
                continue;
            };
            // A started blocking parse survives cancellation of its async caller.
            let parsing_permit = permit.clone();
            // Parsing is CPU-bound and must not monopolize an async worker.
            parser = tokio::task::spawn_blocking(move || {
                let _permit = parsing_permit;
                parser.parse_html(&final_url, &content)?;
                Ok::<_, anyhow::Error>(parser)
            })
            .await??;
        }
        Ok(parser.finalize().map(Arc::new))
    }
}

type RefreshResult = Result<Option<Arc<PackageDetail>>, Arc<anyhow::Error>>;

#[derive(Default)]
pub(crate) struct IndexRefresh {
    revision: AtomicU64,
    result: Mutex<Option<RefreshResult>>,
}

#[derive(Debug)]
struct SharedRefreshError(Arc<anyhow::Error>);

impl std::fmt::Display for SharedRefreshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for SharedRefreshError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}

fn share_refresh_result(result: &RefreshResult) -> anyhow::Result<Option<Arc<PackageDetail>>> {
    result.clone().map_err(|error| {
        let upstream = error.is::<UpstreamIndexError>();
        let limited = error.is::<ResourceLimit>();
        let error = anyhow::Error::new(SharedRefreshError(error));
        if limited {
            error.context(ResourceLimit)
        } else if upstream {
            error.context(UpstreamIndexError)
        } else {
            error
        }
    })
}

#[derive(Clone)]
pub(crate) struct IndexUrlTemplate(pub(crate) String);

impl IndexUrlTemplate {
    fn package_url(&self, package: &str) -> Result<Url, url::ParseError> {
        self.0
            .replace("{package}", &encode_segment(package))
            .parse()
    }
}

pub(crate) struct ArtifactSource {
    pub(crate) url: String,
    pub(crate) filename: String,
}
pub(crate) struct PackageDetail {
    pub(crate) html: Bytes,
    pub(crate) files: HashMap<String, ArtifactSource>,
}
impl PackageDetail {
    pub(crate) fn byte_len(&self) -> usize {
        self.files
            .iter()
            .fold(self.html.len(), |size, (hash, source)| {
                size.saturating_add(hash.len())
                    .saturating_add(source.url.len())
                    .saturating_add(source.filename.len())
            })
    }
}

pub(crate) fn format_indices_list(
    base_url: &str,
    indices: &HashMap<String, Vec<String>>,
) -> String {
    let mut list = "<!DOCTYPE html>\n<html>\n<body>".into();
    for index in indices.keys() {
        list = format!("{list}\n<a href=\"{base_url}{index}/\">{index}</a><br/>");
    }
    list.push_str("\n</body>\n</html>");
    list
}

#[cfg(test)]
mod tests;
