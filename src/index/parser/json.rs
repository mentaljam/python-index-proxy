use url::Url;

use crate::{models::Detail, paths::valid_filename};

use super::super::project::{ParsedFile, ParsedProject};

pub(super) fn parse(
    package_url: &Url,
    package: &str,
    content: &[u8],
) -> anyhow::Result<ParsedProject> {
    let detail: Detail = serde_json::from_slice(content)?;
    let version = detail.meta.api_version.split_once('.');
    anyhow::ensure!(
        version.is_some_and(|(major, minor)| major == "1"
            && !minor.is_empty()
            && minor.bytes().all(|byte| byte.is_ascii_digit())),
        "Unsupported upstream Simple API version"
    );
    let normalized = detail.name.parse::<uv_normalize::PackageName>()?;
    anyhow::ensure!(
        normalized.as_str() == package,
        "Upstream project name mismatch"
    );
    let files = detail
        .files
        .into_iter()
        .map(|file| {
            anyhow::ensure!(
                valid_filename(&file.metadata.filename),
                "Invalid upstream filename"
            );
            let source_url = package_url.join(&file.url)?;
            anyhow::ensure!(
                matches!(source_url.scheme(), "http" | "https"),
                "Unsupported artifact URL scheme"
            );
            Ok(ParsedFile {
                source_url,
                metadata: file.metadata,
                html: None,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(ParsedProject {
        files,
        versions: detail
            .versions
            .map(|versions| versions.into_iter().collect()),
        project_status: detail.project_status,
        exists: true,
    })
}
