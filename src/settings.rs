use std::{collections::HashMap, net::SocketAddr, time::Duration};

use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};

use crate::storage::Storage;

#[derive(Deserialize, Serialize)]
#[cfg_attr(feature = "config-template", derive(toml_example::TomlExample))]
pub struct Settings {
    /// HTTP listen address, including the port; IPv6 addresses use [address]:port.
    /// This address is also used in generated links when public_url is omitted.
    #[serde(default = "defaults::bind")]
    #[cfg_attr(feature = "config-template", toml_example(default = "127.0.0.1:3000"))]
    pub bind: SocketAddr,

    /// Application route prefix, starting with '/'; a trailing slash is added if absent.
    /// Use a URI path without a host, query, fragment, dot segments or routing captures.
    #[serde(
        default = "defaults::path_prefix",
        deserialize_with = "de::path_prefix"
    )]
    pub path_prefix: String,

    /// External HTTP(S) base URL for links and commands, without credentials, query or fragment.
    /// A trailing slash is added; its path may differ from path_prefix for reverse-proxy rewrites.
    /// When omitted, uses http://{bind}{path_prefix}, preserving the configured bind address.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "de::public_url"
    )]
    #[cfg_attr(
        feature = "config-template",
        toml_example(default = "https://example.com")
    )]
    pub public_url: Option<String>,

    /// Case-sensitive ASCII index names (letters/digits, internal '.', '_' and '-') map to HTTP(S) sources.
    /// Templates need exactly one {package} in the path; query strings and URL credentials are preserved.
    /// Sources merge without priority: 4xx except 408/429 are skipped; other upstream failures return 502 without caching partial results.
    #[serde(default = "defaults::indices", deserialize_with = "de::indices")]
    #[cfg_attr(feature = "config-template", toml_example(default = { default = ["https://pypi.org/simple/{package}/"] }))]
    pub indices: HashMap<String, Vec<String>>,

    /// In-memory cache of project pages and source mappings, separate for each replica.
    /// Entries are lost on restart; this cache does not store artifact bytes.
    #[cfg_attr(feature = "config-template", toml_example(nesting))]
    #[serde(default)]
    pub index_cache: Cache,

    /// Concurrent-work limits apply separately to each replica.
    /// Requests for the same in-flight object share its work instead of consuming extra slots.
    /// New independent work beyond these limits receives 503 with Retry-After: 1.
    #[cfg_attr(feature = "config-template", toml_example(nesting))]
    #[serde(default)]
    pub limits: Limits,

    /// Upstream HTTP timeouts; storage operations use their backend settings.
    /// Artifact downloads have no total deadline while upstream data continues to arrive.
    #[cfg_attr(feature = "config-template", toml_example(nesting))]
    #[serde(default)]
    pub upstream: Upstream,

    /// Exactly one backend is required; available backends depend on the build's storage features.
    /// Select one OpenDAL backend using its native configuration.
    #[cfg_attr(feature = "config-template", toml_example(nesting))]
    pub storage: Storage,
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

    #[cfg(feature = "config-template")]
    pub fn template() -> String {
        <Self as toml_example::TomlExample>::toml_example()
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self {
            bind: defaults::bind(),
            path_prefix: defaults::path_prefix(),
            public_url: None,
            indices: defaults::indices(),
            index_cache: Cache::default(),
            limits: Limits::default(),
            upstream: Upstream::default(),
            storage: crate::storage::Storage::filesystem_template(),
        }
    }
}

#[derive(Deserialize, Serialize, Default)]
#[cfg_attr(feature = "config-template", derive(toml_example::TomlExample))]
#[serde(default)]
pub struct Cache {
    /// Maximum cache lifetime after insertion or replacement; reads do not extend it.
    /// Expired pages are fetched again on demand.
    #[cfg_attr(feature = "config-template", toml_example(default = "1h"))]
    pub ttl: CacheTtl,
    /// Per-replica byte budget for cache keys, rendered HTML and source mappings.
    /// Excludes allocator overhead and in-flight work; this is not a process memory limit.
    #[cfg_attr(feature = "config-template", toml_example(default = "128 MiB"))]
    pub max_capacity: MaxCacheCapacity,
}

#[derive(Deserialize, Serialize)]
#[serde(transparent)]
pub struct CacheTtl(
    #[serde(
        serialize_with = "humantime_serde::serialize",
        deserialize_with = "de::positive_duration"
    )]
    Duration,
);

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
pub struct MaxCacheCapacity(
    #[serde(
        serialize_with = "byte_unit_serde::serialize",
        deserialize_with = "de::positive_bytes"
    )]
    u64,
);

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
#[cfg_attr(feature = "config-template", derive(toml_example::TomlExample))]
#[serde(default)]
pub struct Limits {
    /// Maximum body size of each upstream project page after decompression.
    /// Exceeding it returns 502 without caching a partial page.
    #[cfg_attr(feature = "config-template", toml_example(default = "32 MiB"))]
    #[serde(
        serialize_with = "byte_unit_serde::serialize",
        deserialize_with = "de::page_size"
    )]
    pub max_index_page_size: u64,
    /// Concurrent project refreshes across distinct index/package pairs.
    /// A slot covers fetching all configured sources and parsing their pages.
    #[serde(deserialize_with = "de::concurrency")]
    pub max_index_refreshes: usize,
    /// Concurrent artifact cache fills across distinct objects; cache hits do not consume slots.
    /// A fill retains its slot through completion or cleanup after failure/cancellation.
    #[serde(deserialize_with = "de::concurrency")]
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
#[cfg_attr(feature = "config-template", derive(toml_example::TomlExample))]
#[serde(default)]
pub struct Upstream {
    /// Deadline for establishing an upstream connection, including TLS negotiation.
    /// Applies to both project-page requests and artifact downloads.
    #[cfg_attr(feature = "config-template", toml_example(default = "5s"))]
    #[serde(
        serialize_with = "humantime_serde::serialize",
        deserialize_with = "de::timeout"
    )]
    pub connect_timeout: Duration,
    /// Deadline from starting an upstream request until its response headers arrive.
    /// Includes connection establishment and redirects, but excludes reading the response body.
    #[cfg_attr(feature = "config-template", toml_example(default = "30s"))]
    #[serde(
        serialize_with = "humantime_serde::serialize",
        deserialize_with = "de::timeout"
    )]
    pub response_header_timeout: Duration,
    /// Deadline for each active read of upstream response data.
    /// Time spent writing to storage or waiting for the client does not count.
    #[cfg_attr(feature = "config-template", toml_example(default = "30s"))]
    #[serde(
        serialize_with = "humantime_serde::serialize",
        deserialize_with = "de::timeout"
    )]
    pub read_timeout: Duration,
    /// Total project-page download deadline per source, from request start through the full body.
    /// Includes connection, headers and body reads; excludes parsing and does not limit artifact downloads.
    #[cfg_attr(feature = "config-template", toml_example(default = "60s"))]
    #[serde(
        serialize_with = "humantime_serde::serialize",
        deserialize_with = "de::timeout"
    )]
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

mod de {
    use serde::{Deserializer, de::Error};

    use super::*;

    pub fn path_prefix<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
        let value = String::deserialize(deserializer)?;
        parse_path_prefix(&value).map_err(D::Error::custom)
    }

    pub fn public_url<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<String>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .as_deref()
            .map(parse_public_url)
            .transpose()
            .map_err(D::Error::custom)
    }

    pub fn indices<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<HashMap<String, Vec<String>>, D::Error> {
        let mut indices = HashMap::<String, Vec<String>>::deserialize(deserializer)?;
        for (index, templates) in &mut indices {
            if !crate::paths::valid_index_name(index) {
                return Err(D::Error::custom(
                    "indices contains an invalid name: expected ASCII alphanumeric endpoints and only letters, digits, '.', '_' or '-'",
                ));
            }
            for (position, template) in templates.iter_mut().enumerate() {
                // Do not include raw URLs: they can contain credentials/query secrets.
                *template = crate::index::IndexUrlTemplate::parse(template)
                    .map_err(|error| {
                        D::Error::custom(format!("indices.{index}[{position}]: {error}"))
                    })?
                    .0;
            }
        }
        Ok(indices)
    }

    pub fn positive_bytes<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let value = byte_unit_serde::deserialize(deserializer)?;
        if value == 0 {
            return Err(D::Error::custom("byte size must be positive"));
        }
        Ok(value)
    }

    pub fn page_size<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let value = positive_bytes(deserializer)?;
        if usize::try_from(value).is_err() {
            return Err(D::Error::custom("index page size must fit this platform"));
        }
        Ok(value)
    }

    pub fn concurrency<'de, D: Deserializer<'de>>(deserializer: D) -> Result<usize, D::Error> {
        let value = usize::deserialize(deserializer)?;
        if value == 0 || value > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(D::Error::custom(format!(
                "concurrency must be between 1 and {}",
                tokio::sync::Semaphore::MAX_PERMITS
            )));
        }
        Ok(value)
    }

    pub fn positive_duration<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Duration, D::Error> {
        let value: Duration = humantime_serde::deserialize(deserializer)?;
        if value.is_zero() {
            return Err(D::Error::custom("duration must be positive"));
        }
        Ok(value)
    }

    pub fn timeout<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let value = positive_duration(deserializer)?;
        if std::time::Instant::now().checked_add(value).is_none() {
            return Err(D::Error::custom("timeout must fit this platform's clock"));
        }
        Ok(value)
    }
}

fn parse_public_url(value: &str) -> anyhow::Result<String> {
    // Keep diagnostics independent of the value: rejected URLs may contain secrets.
    anyhow::ensure!(
        !value
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
            && !value.contains('\\')
            && value.split_once("://").is_some_and(|(scheme, rest)| {
                (scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https"))
                    && !rest.is_empty()
                    && !rest.starts_with('/')
                    && !rest
                        .split(['/', '?', '#'])
                        .next()
                        .unwrap_or("")
                        .contains('@')
            }),
        "public_url must be an absolute HTTP(S) URL without credentials, query or fragment"
    );
    let mut url: url::Url = value
        .parse()
        .map_err(|_| anyhow::anyhow!("public_url is not a valid URL"))?;
    anyhow::ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.has_host()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "public_url must be an absolute HTTP(S) URL without credentials, query or fragment"
    );
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url.into())
}

fn parse_path_prefix(value: &str) -> anyhow::Result<String> {
    anyhow::ensure!(
        value.starts_with('/')
            && !value.starts_with("//")
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || b"/-._~!$&'()*+,;=:@%".contains(&byte)
            }),
        "path_prefix must be an absolute URI path prefix without a host, query, fragment or routing captures"
    );
    if value != "/" {
        for segment in value.trim_end_matches('/').split('/').skip(1) {
            let mut bytes = segment.bytes();
            while let Some(byte) = bytes.next() {
                if byte == b'%' {
                    anyhow::ensure!(
                        bytes.next().is_some_and(|byte| byte.is_ascii_hexdigit())
                            && bytes.next().is_some_and(|byte| byte.is_ascii_hexdigit()),
                        "path_prefix contains an invalid percent escape"
                    );
                }
            }
            let decoded = percent_encoding::percent_decode_str(segment)
                .decode_utf8()
                .map_err(|_| anyhow::anyhow!("path_prefix contains invalid UTF-8"))?;
            anyhow::ensure!(
                !decoded.is_empty()
                    && decoded != "."
                    && decoded != ".."
                    && !decoded.starts_with([':', '*'])
                    && !decoded.contains(['/', '\\', '{', '}'])
                    && !decoded.chars().any(char::is_control),
                "path_prefix must contain nonempty safe path segments, without dot segments or routing captures"
            );
        }
        anyhow::ensure!(
            !value.contains("//"),
            "path_prefix must not contain empty path segments"
        );
    }
    Ok(if value.ends_with('/') {
        value.to_owned()
    } else {
        format!("{value}/")
    })
}

mod defaults {
    use super::*;

    pub fn bind() -> SocketAddr {
        "127.0.0.1:3000".parse().unwrap()
    }

    pub fn path_prefix() -> String {
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

    fn parse_settings(source: &str) -> Result<Settings, ConfigError> {
        Config::builder()
            .add_source(File::from_str(source, config::FileFormat::Toml))
            .build()?
            .try_deserialize()
    }

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
        assert!(settings.public_url.is_none());
    }

    #[test]
    fn public_url_is_optional_independent_and_rejects_secrets_without_echoing_them() {
        let settings = parse_settings(
            "path_prefix = '/internal'\npublic_url = 'HTTPS://Packages.Example.test/external/a&b'\n[storage.fs]\nroot = 'artifacts'",
        ).unwrap();
        assert_eq!(settings.path_prefix, "/internal/");
        assert_eq!(
            settings.public_url.as_deref(),
            Some("https://packages.example.test/external/a&b/")
        );
        for value in [
            "",
            "/relative/",
            "//example.test/",
            "https:/example.test/",
            "https:///example.test/",
            "ftp://example.test/",
            "https://user:private-password@example.test/",
            "https://@example.test/",
            "https://example.test/?token=private-password",
            "https://example.test/#private-password",
            "https://example.test/?",
            "https://example.test/#",
            "https://example.test/ space",
            "https://example.test/\\path",
        ] {
            let source = format!(
                "public_url = {}\n[storage.fs]\nroot = 'artifacts'",
                toml::Value::String(value.into())
            );
            let error = match parse_settings(&source) {
                Ok(_) => panic!("accepted invalid public_url"),
                Err(error) => format!("{error:#}"),
            };
            assert!(error.contains("public_url"));
            assert!(!error.contains("private-password"));
            assert!(!error.contains("example.test"));
        }
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
            assert!(
                parse_settings(&format!("[storage.fs]\nroot = 'artifacts'\n{fragment}")).is_err(),
                "accepted {fragment}"
            );
        }
        let invalid = tokio::sync::Semaphore::MAX_PERMITS + 1;
        for field in ["max_index_refreshes", "max_artifact_downloads"] {
            assert!(
                parse_settings(&format!(
                    "[storage.fs]\nroot = 'artifacts'\n[limits]\n{field} = {invalid}"
                ))
                .is_err()
            );
        }
        assert!(parse_settings("[storage.fs]\nroot = 'artifacts'\n[upstream]\nread_timeout = '18446744073709551615s'").is_err());
    }

    #[test]
    fn deserialize_and_normalize_path_templates() {
        let settings = parse_settings(
            "path_prefix = '/nested/a&b/%7Eroot'\n[indices]\n'Private_1.example' = ['https://user:secret@example.test/simple/{package}?token=private']\n[storage.fs]\nroot = 'artifacts'",
        ).unwrap();
        assert_eq!(settings.path_prefix, "/nested/a&b/%7Eroot/");
        let template = &settings.indices["Private_1.example"][0];
        assert_eq!(
            template,
            "https://user:secret@example.test/simple/{package}/?token=private"
        );
        assert!(!settings.indices.contains_key("private_1.example"));
    }

    #[test]
    fn serialized_template_deserializes_with_the_same_defaults() {
        let serialized = toml::to_string(&Settings::for_test()).unwrap();
        let parsed = parse_settings(&serialized).unwrap();
        assert_eq!(toml::to_string(&parsed).unwrap(), serialized);
    }

    #[cfg(feature = "config-template")]
    #[test]
    fn documented_template_preserves_defaults_and_illustrates_optional_public_url() {
        let text = Settings::template();
        assert!(text.contains(
            "# Select one OpenDAL backend using its native configuration.\n[storage.fs]\nroot = \"artifacts\"\natomic_write_dir = \"artifacts/.tmp\"\n"
        ));
        assert!(!text.contains("storage ="));
        let parsed = parse_settings(&text).unwrap();
        assert_eq!(
            toml::to_string(&parsed).unwrap(),
            toml::to_string(&Settings::for_test()).unwrap()
        );
        assert!(text.contains("# public_url = \"https://example.com\""));
        let enabled = text.replace("# public_url =", "public_url =");
        let parsed = parse_settings(&enabled).unwrap();
        assert_eq!(parsed.public_url.as_deref(), Some("https://example.com/"));
    }

    #[test]
    fn reject_invalid_prefixes_indices_and_source_templates_without_secrets() {
        for prefix in [
            "",
            "relative",
            "https://example.test/",
            "//example.test/",
            "/api?x=1",
            "/api#fragment",
            "/api/./",
            "/api/../",
            "/api/%2e%2e/",
            "/api//",
            "/api/{capture}/",
            "/:capture/",
            "/*capture/",
            "/api/%2f/",
            "/api/%5c/",
            "/api/%00/",
            "/api/%ff/",
            "/api/%zz/",
            "/api/<tag>/",
            "/api/ space/",
        ] {
            let source = format!(
                "path_prefix = {}\n[storage.fs]\nroot = 'artifacts'",
                toml::Value::String(prefix.into())
            );
            assert!(
                parse_settings(&source).is_err(),
                "accepted prefix {prefix:?}"
            );
        }
        for index in [
            "", ".", "..", "a/", "a\\b", "-start", "end_", " space", "name!", "α",
        ] {
            let source = format!(
                "[storage.fs]\nroot = 'artifacts'\n[indices]\n{} = []",
                toml::Value::String(index.into())
            );
            assert!(parse_settings(&source).is_err(), "accepted index {index:?}");
        }
        for template in [
            "https://user:secret@example.test/simple/",
            "https://user:secret@example.test/{package}/{package}/",
            "https://user:secret@example.test/{other}/{package}/",
            "https://user:secret@example.test/?package={package}",
            "https://user:secret@{package}.example.test/",
            "https://user:secret@example.test/{package}/#fragment",
            "ftp://user:secret@example.test/{package}/",
            "https:/user:secret@example.test/{package}/",
            "https:///example.test/{package}/",
            "https://user:secret@/{package}/",
            "https://user:secret@[invalid]/{package}/",
            "https://user:secret@example.test/{package}/../",
        ] {
            let source = format!(
                "[storage.fs]\nroot = 'artifacts'\n[indices]\nprivate = [{}]",
                toml::Value::String(template.into())
            );
            let error = match parse_settings(&source) {
                Ok(_) => panic!("accepted invalid template"),
                Err(error) => format!("{error:#}"),
            };
            assert!(error.contains("indices.private[0]"));
            assert!(!error.contains("user:secret"));
            assert!(!error.contains("example.test"));
        }
    }
}
