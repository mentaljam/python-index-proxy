use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use tokio::sync::Mutex;
use url::Url;
use uv_normalize::PackageName;

use crate::{
    coordination::ResourceLimit,
    paths::{encode_segment, valid_index_name},
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
    pub(crate) fn get_index_info(&self, index: &str) -> Option<String> {
        if !self.0.indices.contains_key(index) {
            return None;
        }
        let url = format!("{}{index}/", self.0.public_url);
        // Quote shell metacharacters before escaping the HTML text.
        let quoted_url = format!("'{}'", url.replace('\'', "'\\''")).replace('&', "&amp;");
        Some(format!(
            include_str!("./index.html"),
            index = index,
            quoted_url = quoted_url
        ))
    }

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
        if !valid_index_name(index_slug) {
            return Ok(None);
        }
        let Ok(package) = package.parse::<PackageName>() else {
            return Ok(None);
        };
        let package = package.as_str();
        let data = &self.0;
        let index_cache = &data.index_cache;
        let url_templates = data.indices.get(index_slug);
        let cache_key = format!("{}/{}", encode_segment(index_slug), encode_segment(package));
        if let Some(detail) = index_cache.get(&cache_key).await
            && artifact_id.is_none_or(|id| detail.files.contains_key(id))
        {
            return Ok(Some(detail));
        }
        let Some(url_templates) = url_templates else {
            return Ok(None);
        };
        let lease = data.index_refreshes.lease(cache_key.clone());
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
        let result = match data.index_permits.clone().try_acquire_owned() {
            Ok(permit) => {
                self.fetch_package_detail(index_slug, package, url_templates, Arc::new(permit))
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

    async fn fetch_package_detail(
        &self,
        index_slug: &str,
        package: &str,
        url_templates: &[IndexUrlTemplate],
        permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> anyhow::Result<Option<Arc<PackageDetail>>> {
        let data = &self.0;
        let mut parser = DetailParser::new(&data.public_url, index_slug, package);
        for template in url_templates {
            let Some((final_url, content)) = crate::upstream::index_page(
                &data.http_client,
                template.package_url(package)?,
                data.upstream,
                data.limits.max_index_page_size,
            )
            .await?
            else {
                continue;
            };
            // A started blocking parse survives cancellation of its async caller.
            let parsing_permit = permit.clone();
            // Parsing is CPU-bound and must not monopolize an async worker.
            parser = data
                .shutdown
                .spawn_blocking(move || {
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
    pub(crate) fn parse(template: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(
            template.matches("{package}").count() == 1
                && !template.replace("{package}", "").contains(['{', '}'])
                && !template
                    .chars()
                    .any(|character| character.is_whitespace() || character.is_control())
                && !template.contains('\\')
                && template
                    .split_once("://")
                    .is_some_and(|(scheme, authority)| {
                        (scheme.eq_ignore_ascii_case("http")
                            || scheme.eq_ignore_ascii_case("https"))
                            && !authority.is_empty()
                            && !authority.starts_with('/')
                    }),
            "expected an absolute HTTP(S) URL with exactly one {{package}} placeholder and no whitespace or backslashes"
        );
        // The marker must not occur in credentials/query or elsewhere in the
        // original URL when replacing its canonical serialization below.
        let mut marker = "piprpackagemarker".to_owned();
        while template.contains(&marker) {
            marker.push('x');
        }
        let mut url: Url = template.replace("{package}", &marker).parse()?;
        anyhow::ensure!(
            matches!(url.scheme(), "http" | "https")
                && url.has_host()
                && url.fragment().is_none()
                && url.path().contains(&marker),
            "expected an absolute HTTP(S) URL with {{package}} in its path and no fragment"
        );
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
        let marker_start = url[..url::Position::BeforePath].len()
            + url.path().find(&marker).ok_or_else(|| {
                anyhow::anyhow!("missing path placeholder after URL normalization")
            })?;
        let mut template = url.to_string();
        template.replace_range(marker_start..marker_start + marker.len(), "{package}");
        Ok(Self(template))
    }

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
    link_base: &str,
    indices: &HashMap<String, Vec<String>>,
) -> String {
    let mut list = "<!DOCTYPE html>\n<html>\n<body>".into();
    let link_base = link_base.replace('&', "&amp;");
    for index in indices.keys() {
        list = format!("{list}\n<a href=\"{link_base}{index}/\">{index}</a><br/>");
    }
    list.push_str("\n</body>\n</html>");
    list
}

#[cfg(test)]
mod tests;
