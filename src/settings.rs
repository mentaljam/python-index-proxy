use std::{collections::HashMap, net::SocketAddr, time::Duration};

use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Settings {
    #[serde(default = "defaults::bind")]
    pub bind: SocketAddr,
    #[serde(default = "defaults::base_url", deserialize_with = "de::base_url")]
    pub base_url: String,
    #[serde(default = "defaults::indices", deserialize_with = "de::indices")]
    pub indices: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub index_cache: Cache,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub upstream: Upstream,
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
            indices: defaults::indices(),
            index_cache: Cache::default(),
            limits: Limits::default(),
            upstream: Upstream::default(),
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
        Self(128 * 1024 * 1024)
    }
}

impl From<MaxCacheCapacity> for u64 {
    fn from(value: MaxCacheCapacity) -> Self {
        value.0
    }
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Limits {
    #[serde(with = "byte_unit_serde")]
    pub max_index_page_size: u64,
    pub max_index_refreshes: usize,
    pub max_artifact_downloads: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_index_page_size: 32 * 1024 * 1024,
            max_index_refreshes: 4,
            max_artifact_downloads: 16,
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Copy)]
#[serde(default)]
pub struct Upstream {
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub response_header_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub read_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub index_download_timeout: Duration,
}

impl Default for Upstream {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            response_header_timeout: Duration::from_secs(30),
            read_timeout: Duration::from_secs(30),
            index_download_timeout: Duration::from_secs(60),
        }
    }
}

impl Settings {
    pub(crate) fn validate_resources(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.index_cache.max_capacity.0 > 0,
            "index_cache.max_capacity must be positive"
        );
        anyhow::ensure!(
            !self.index_cache.ttl.0.is_zero(),
            "index_cache.ttl must be positive"
        );
        anyhow::ensure!(
            self.limits.max_index_page_size > 0
                && usize::try_from(self.limits.max_index_page_size).is_ok(),
            "limits.max_index_page_size must be positive and fit this platform"
        );
        for (name, count) in [
            ("max_index_refreshes", self.limits.max_index_refreshes),
            ("max_artifact_downloads", self.limits.max_artifact_downloads),
        ] {
            anyhow::ensure!(
                count > 0 && count <= tokio::sync::Semaphore::MAX_PERMITS,
                "limits.{name} must be between 1 and {}",
                tokio::sync::Semaphore::MAX_PERMITS
            );
        }
        for (name, duration) in [
            ("connect_timeout", self.upstream.connect_timeout),
            (
                "response_header_timeout",
                self.upstream.response_header_timeout,
            ),
            ("read_timeout", self.upstream.read_timeout),
            (
                "index_download_timeout",
                self.upstream.index_download_timeout,
            ),
        ] {
            anyhow::ensure!(
                !duration.is_zero() && std::time::Instant::now().checked_add(duration).is_some(),
                "upstream.{name} must be positive and fit this platform's clock"
            );
        }
        Ok(())
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

    pub fn indices() -> HashMap<String, Vec<String>> {
        [(
            "default".into(),
            vec!["https://pypi.org/simple/{package}/".into()],
        )]
        .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_defaults_and_partial_overrides() {
        let settings: Settings = toml::from_str("[storage.fs]\nroot = 'artifacts'\n[limits]\nmax_index_refreshes = 2\n[upstream]\nread_timeout = '250ms'").unwrap();
        assert_eq!(settings.index_cache.max_capacity.0, 128 * 1024 * 1024);
        assert_eq!(settings.index_cache.ttl.0, Duration::from_secs(3600));
        assert_eq!(settings.limits.max_index_page_size, 32 * 1024 * 1024);
        assert_eq!(settings.limits.max_index_refreshes, 2);
        assert_eq!(settings.limits.max_artifact_downloads, 16);
        assert_eq!(settings.upstream.read_timeout, Duration::from_millis(250));
        assert_eq!(settings.upstream.connect_timeout, Duration::from_secs(5));
        assert_eq!(
            settings.upstream.response_header_timeout,
            Duration::from_secs(30)
        );
        assert_eq!(
            settings.upstream.index_download_timeout,
            Duration::from_secs(60)
        );
        settings.validate_resources().unwrap();
    }

    #[test]
    fn reject_zero_resource_limits_and_unrepresentable_concurrency() {
        for fragment in [
            "[index_cache]\nmax_capacity = '0 B'",
            "[index_cache]\nttl = '0s'",
            "[limits]\nmax_index_page_size = '0 B'",
            "[limits]\nmax_index_refreshes = 0",
            "[limits]\nmax_artifact_downloads = 0",
            "[upstream]\nconnect_timeout = '0s'",
            "[upstream]\nresponse_header_timeout = '0s'",
            "[upstream]\nread_timeout = '0s'",
            "[upstream]\nindex_download_timeout = '0s'",
        ] {
            let settings: Settings =
                toml::from_str(&format!("[storage.fs]\nroot = 'artifacts'\n{fragment}")).unwrap();
            assert!(
                settings.validate_resources().is_err(),
                "accepted {fragment}"
            );
        }
        let mut settings = Settings::template();
        settings.limits.max_index_refreshes = usize::MAX;
        assert!(settings.validate_resources().is_err());
    }
}
