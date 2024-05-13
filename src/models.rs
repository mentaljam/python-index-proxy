use std::collections::HashMap;

use http::HeaderValue;
use serde::{Deserialize, Serialize};

const VND_PYPI_SIMPLE_V1_JSON: &[u8] = b"application/vnd.pypi.simple.v1+json";
const ACCEPT: HeaderValue = HeaderValue::from_static("application/vnd.pypi.simple.v1+json, application/vnd.pypi.simple.v1+html;q=0.2, text/html;q=0.01");

pub enum Format {
    Html,
    JsonV1,
}

impl From<&HeaderValue> for Format {
    fn from(value: &HeaderValue) -> Self {
        if value == VND_PYPI_SIMPLE_V1_JSON {
            Self::JsonV1
        } else {
            Self::Html
        }
    }
}

impl From<Format> for HeaderValue {
    fn from(_value: Format) -> Self {
        ACCEPT
    }
}

// #[derive(Deserialize, Serialize)]
// pub struct Index {
//     meta: Meta,
//     projects: Vec<Project>,
// }

#[derive(Deserialize, Serialize)]
pub struct Detail<'a> {
    pub meta: Meta<'a>,
    /// Normalized Name
    pub name: &'a str,
    pub files: Vec<File<'a>>,
}

#[derive(Deserialize, Serialize)]
pub struct Meta<'a> {
    pub api_version: &'a str,
}

// #[derive(Deserialize, Serialize)]
// pub struct Project {
//     /// Normalized or Non-Normalized Name
//     name: String,
//     /// Computed in JSON, Included in HTML
//     url: Option<String>,
// }

#[derive(Deserialize, Serialize)]
pub struct File<'a> {
    pub filename: &'a str,
    pub url: &'a str,
    /// Limited to a len() of 1 in HTML
    pub hashes: HashMap<&'a str, &'a str>,
    pub gpg_sig: Option<bool>,
    pub requires_python: Option<&'a str>,
    pub yanked: Yanked<'a>,
    /// Limited to a len() of 1 in HTML
    pub dist_info_metadata: DistInfoMetadata<'a>,
}

#[derive(Deserialize, Serialize)]
pub enum Yanked<'a> {
    Bool(bool),
    String(&'a str),
}

#[derive(Deserialize, Serialize)]
#[serde(bound(deserialize = "'de: 'a"))]
pub enum DistInfoMetadata<'a> {
    Bool(bool),
    Map(HashMap<&'a str, &'a str>),
}
