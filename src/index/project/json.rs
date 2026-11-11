use std::collections::BTreeSet;

use serde::Serialize;

use crate::{
    index::project::{FileMetadata, PreparedProject, ProjectStatus},
    models::Meta,
};

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct Response<'a> {
    meta: Meta,
    name: &'a str,
    files: Vec<File<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    versions: Option<&'a BTreeSet<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    project_status: Option<&'a ProjectStatus>,
}

#[derive(Serialize)]
struct File<'a> {
    url: &'a str,
    #[serde(flatten)]
    metadata: &'a FileMetadata,
}

impl PreparedProject {
    pub(super) fn render_json(&self) -> anyhow::Result<Vec<u8>> {
        let complete =
            self.versions.is_some() && self.files.iter().all(|file| file.metadata.size.is_some());
        let response = Response {
            meta: Meta {
                api_version: if complete { "1.4" } else { "1.0" }.to_owned(),
            },
            name: &self.name,
            files: self
                .files
                .iter()
                .map(|file| File {
                    url: &file.url,
                    metadata: &file.metadata,
                })
                .collect(),
            versions: self.versions.as_ref(),
            project_status: self.project_status.as_ref(),
        };
        Ok(serde_json::to_vec(&response)?)
    }
}
