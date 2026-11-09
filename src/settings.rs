use std::{collections::HashMap, net::SocketAddr, path::PathBuf, time::Duration};

use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Settings {
    #[serde(default = "defaults::bind")]
    pub bind: SocketAddr,
    #[serde(default = "defaults::base_url", deserialize_with = "de::base_url")]
    pub base_url: String,
    #[serde(default = "defaults::db_path")]
    pub db_path: PathBuf,
    #[serde(default = "defaults::indices", deserialize_with = "de::indices")]
    pub indices: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub index_cache: Cache,
    pub storage: crate::storage::Storage,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let file = match std::env::args().nth(1) {
            Some(arg) => File::from(std::path::PathBuf::from(arg)).required(true),
            None => File::with_name("python_index_proxy").required(false),
        };

        Config::builder()
            .add_source(file)
            .add_source(Environment::with_prefix("pipr"))
            .build()?
            .try_deserialize()
    }

    pub fn template() -> Self {
        Self {
            bind: defaults::bind(),
            base_url: defaults::base_url(),
            db_path: defaults::db_path(),
            indices: defaults::indices(),
            index_cache: Cache::default(),
            storage: crate::storage::Storage::filesystem_template(),
        }
    }
}

#[derive(Deserialize, Serialize, Default)]
#[serde(default)]
pub struct Cache {
    pub ttl: CacheTtl,
    pub max_capacity: MaxCacheCapacity,
}

#[derive(Deserialize, Serialize)]
#[serde(transparent)]
pub struct CacheTtl(#[serde(with = "humantime_serde")] Duration);

impl Default for CacheTtl {
    fn default() -> Self {
        Self(Duration::from_secs(60 * 60))
    }
}

impl From<CacheTtl> for Duration {
    fn from(value: CacheTtl) -> Self {
        value.0
    }
}

#[derive(Deserialize, Serialize)]
#[serde(transparent)]
pub struct MaxCacheCapacity(#[serde(with = "byte_unit_serde")] u64);

impl Default for MaxCacheCapacity {
    fn default() -> Self {
        Self(1024 * 1024 * 1024)
    }
}

impl From<MaxCacheCapacity> for u64 {
    fn from(value: MaxCacheCapacity) -> Self {
        value.0
    }
}

mod de {
    use std::collections::HashMap;

    use serde::{
        Deserialize,
        de::{Deserializer, Error},
    };

    pub fn base_url<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut base_url = String::deserialize(deserializer)?;
        let base_url: String = match base_url.chars().last() {
            Some('/') => base_url,
            Some(_) => {
                base_url.push('/');
                base_url
            }
            None => "/".into(),
        };

        Ok(base_url)
    }

    pub fn indices<'de, D>(deserializer: D) -> Result<HashMap<String, Vec<String>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut indices = HashMap::<String, Vec<String>>::deserialize(deserializer)?;
        for (index, urls) in indices.iter_mut() {
            for url in urls.iter_mut() {
                if url.is_empty() || !url.contains("{package}") {
                    return Err(D::Error::custom(format_args!(
                        "`indices` must be a map of index names and lists of valid URLs with \
a `{{package}}` placeholder: found invalid URL \"{url}\" for index \"{index}\"",
                    )));
                } else if !url.ends_with('/') {
                    url.push('/');
                }
            }
        }
        Ok(indices)
    }
}

mod defaults {
    use super::*;

    pub fn bind() -> SocketAddr {
        "127.0.0.1:3000".parse().unwrap()
    }

    pub fn base_url() -> String {
        "/".into()
    }

    pub fn db_path() -> PathBuf {
        "python-index-proxy.redb".into()
    }

    pub fn indices() -> HashMap<String, Vec<String>> {
        [(
            "default".into(),
            vec!["https://pypi.org/simple/{package}/".into()],
        )]
        .into()
    }
}
