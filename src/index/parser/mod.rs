use url::Url;

use crate::models::IndexFormat;

use super::{UpstreamIndexError, project::ParsedProject};

mod html;
mod json;

pub(super) fn parse(
    format: IndexFormat,
    package_url: &Url,
    package: &str,
    content: &[u8],
) -> anyhow::Result<ParsedProject> {
    match format {
        IndexFormat::Html => html::parse(package_url, content),
        IndexFormat::JsonV1 => json::parse(package_url, package, content)
            .map_err(|error| error.context(UpstreamIndexError)),
    }
}

#[cfg(test)]
mod tests;
