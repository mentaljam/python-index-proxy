use std::{collections::HashMap, net::SocketAddr, path::PathBuf, time::Duration};

use config::{Config, ConfigError, Environment, File};
use s3::{creds::Credentials, Region};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(default)]
pub struct Settings {
    pub bind: SocketAddr,
    #[serde(deserialize_with = "de::base_url")]
    pub base_url: String,
    pub db_path: PathBuf,
    #[serde(deserialize_with = "de::indices")]
    pub indices: HashMap<String, Vec<String>>,
    pub index_cache: Cache,
    pub s3: S3,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:3000".parse().unwrap(),
            base_url: "/".into(),
            db_path: "python-index-proxy.redb".into(),
            indices: [(
                "default".into(),
                vec!["https://pypi.org/simple/{package}/".into()],
            )]
            .into(),
            index_cache: Cache::default(),
            s3: S3::default(),
        }
    }
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
            s3: S3 {
                endpoint: Some(String::new()),
                credentials: Credentials::new(Some(""), Some(""), Some(""), Some(""), Some(""))
                    .unwrap(),
                ..Default::default()
            },
            ..Default::default()
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

#[derive(Deserialize, Serialize)]
#[serde(default)]
pub struct S3 {
    pub region: Region,
    pub endpoint: Option<String>,
    pub bucket: String,
    pub credentials: Credentials,
}

impl Default for S3 {
    fn default() -> Self {
        Self {
            region: Region::WaUsEast1,
            endpoint: None,
            bucket: env!("CARGO_PKG_NAME").into(),
            credentials: Credentials::default()
                .unwrap_or_else(|_| Credentials::anonymous().unwrap()),
        }
    }
}

mod de {
    use std::collections::HashMap;

    use serde::{
        de::{Deserializer, Error},
        Deserialize,
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
