use serde::{Deserialize, Serialize};

use crate::index::project::ProjectStatus;

pub(crate) const JSON_TYPE: &str = "application/vnd.pypi.simple.v1+json";
pub(crate) const HTML_TYPE: &str = "application/vnd.pypi.simple.v1+html";
pub(crate) const INDEX_ACCEPT: &str = "application/vnd.pypi.simple.v1+json, application/vnd.pypi.simple.v1+html;q=0.2, text/html;q=0.01";

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct Detail {
    pub(crate) meta: Meta,
    pub(crate) name: String,
    pub(crate) files: Vec<File>,
    #[serde(default)]
    pub(crate) versions: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) project_status: Option<ProjectStatus>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct Meta {
    pub(crate) api_version: String,
}

#[derive(Deserialize)]
pub(crate) struct File {
    pub(crate) url: String,
    #[serde(flatten)]
    pub(crate) metadata: crate::index::project::FileMetadata,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum IndexFormat {
    Html,
    JsonV1,
}

impl IndexFormat {
    pub(crate) fn from_content_type(content_type: &str) -> anyhow::Result<Self> {
        let content_type = content_type.split(';').next().unwrap_or("").trim();
        if content_type.eq_ignore_ascii_case(JSON_TYPE) {
            Ok(Self::JsonV1)
        } else if content_type.is_empty()
            || content_type.eq_ignore_ascii_case("text/html")
            || content_type.eq_ignore_ascii_case("text/plain")
            || content_type.eq_ignore_ascii_case(HTML_TYPE)
        {
            Ok(Self::Html)
        } else {
            anyhow::bail!("Unsupported upstream index content type: {content_type}")
        }
    }
}
