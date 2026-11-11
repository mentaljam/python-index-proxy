use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

use crate::paths::encode_segment;

use super::{ArtifactSource, PackageDetail};

mod html;
mod json;

/// One source's project data, independent of the response representation.
pub(super) struct ParsedProject {
    pub(super) files: Vec<ParsedFile>,
    /// None means unknown; Some(empty) means a known empty set.
    pub(super) versions: Option<BTreeSet<String>>,
    pub(super) project_status: Option<ProjectStatus>,
    /// JSON can identify an existing project even when it has no files.
    pub(super) exists: bool,
}

pub(super) struct ParsedFile {
    pub(super) source_url: Url,
    pub(super) metadata: FileMetadata,
    pub(super) html: Option<HtmlLink>,
}

/// Presentation details that cannot be recovered from standard file metadata.
pub(super) struct HtmlLink {
    pub(super) text: String,
    /// Additional attributes and source spellings that override normalized values.
    pub(super) extra_attributes: Vec<(String, String)>,
}

#[derive(Default)]
pub(super) struct PackageProject {
    project: Option<ParsedProject>,
}

impl PackageProject {
    pub(super) fn merge(&mut self, mut source: ParsedProject) {
        let Some(project) = &mut self.project else {
            self.project = Some(source);
            return;
        };
        project.files.append(&mut source.files);
        project.exists |= source.exists;
        match (&mut project.versions, source.versions) {
            (Some(versions), Some(incoming)) => versions.extend(incoming),
            _ => project.versions = None,
        }
        // A merged project only exposes a status shared by every source.
        if project.project_status != source.project_status {
            project.project_status = None;
        }
    }

    pub(super) fn prepare(
        self,
        link_base: &str,
        index: &str,
        package: &str,
    ) -> Option<PreparedProject> {
        let project = self.project.filter(|project| project.exists)?;
        let link_base = format!(
            "{}packages/{}/{}/",
            link_base,
            encode_segment(index),
            encode_segment(package)
        );
        let files = project
            .files
            .into_iter()
            .map(|file| {
                let ParsedFile {
                    mut source_url,
                    metadata,
                    html,
                } = file;
                // Match HTML identities when a JSON URL omits the distribution hash.
                if source_url.fragment().is_none()
                    && let Some((algorithm, digest)) = metadata
                        .hashes
                        .get_key_value("sha256")
                        .or_else(|| metadata.hashes.first_key_value())
                {
                    source_url.set_fragment(Some(&format!("{algorithm}={digest}")));
                }
                let id = hex::encode(Sha256::digest(source_url.as_str().as_bytes()));
                let fragment = source_url.fragment().map(str::to_owned);
                source_url.set_fragment(None);
                let url = format!("{}{}/{}", link_base, id, encode_segment(&metadata.filename));
                let source = ArtifactSource {
                    url: source_url.to_string(),
                    filename: metadata.filename.clone(),
                };
                PreparedFile {
                    id,
                    source,
                    url,
                    fragment,
                    metadata,
                    html,
                }
            })
            .collect();
        Some(PreparedProject {
            name: package.to_owned(),
            files,
            versions: project.versions,
            project_status: project.project_status,
        })
    }
}

pub(super) struct PreparedProject {
    pub(super) name: String,
    pub(super) files: Vec<PreparedFile>,
    pub(super) versions: Option<BTreeSet<String>>,
    pub(super) project_status: Option<ProjectStatus>,
}

impl PreparedProject {
    pub(super) fn render(self) -> anyhow::Result<PackageDetail> {
        let html = self.render_html().into();
        let json = self.render_json()?.into();
        let files = self
            .files
            .into_iter()
            .map(|file| (file.id, file.source))
            .collect();
        Ok(PackageDetail { html, json, files })
    }
}

pub(super) struct PreparedFile {
    pub(super) id: String,
    pub(super) source: ArtifactSource,
    pub(super) url: String,
    pub(super) fragment: Option<String>,
    pub(super) metadata: FileMetadata,
    pub(super) html: Option<HtmlLink>,
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct ProjectStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) reason: Option<String>,
}

/// Standard distribution metadata shared by both Simple API representations.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct FileMetadata {
    pub(crate) filename: String,
    pub(crate) hashes: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) requires_python: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) yanked: Option<Yanked>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) core_metadata: Option<Metadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) dist_info_metadata: Option<Metadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) gpg_sig: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) upload_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) provenance: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum Yanked {
    Bool(bool),
    Reason(String),
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum Metadata {
    Bool(bool),
    Hashes(BTreeMap<String, String>),
}
