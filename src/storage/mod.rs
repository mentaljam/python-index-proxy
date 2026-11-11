use opendal::{Builder, Operator, services};
use serde::{Deserialize, Serialize};

use crate::settings::StorageHttp;

mod http;

// Use each backend's native configuration so TOML values retain their types.
macro_rules! storage_backends {
    ($(($feature:literal, $name:literal, $variant:ident)),* $(,)?) => {
        #[derive(Deserialize, Serialize)]
        pub enum Storage {
            #[serde(rename = "fs")]
            Fs(<services::Fs as Builder>::Config),
            $(
                #[cfg(feature = $feature)]
                #[serde(rename = $name)]
                $variant(Box<<services::$variant as Builder>::Config>),
            )*
        }

        impl Storage {
            pub fn operator(self, http: StorageHttp) -> anyhow::Result<Operator> {
                let op = match self {
                    Self::Fs(mut config) => {
                        if config.atomic_write_dir.is_none() {
                            let root = config.root.as_deref().unwrap_or("/");
                            config.atomic_write_dir = Some(
                                std::path::Path::new(root).join(".tmp")
                                    .to_string_lossy().into_owned()
                            );
                        }
                        Operator::from_config(config)?
                    },
                    $(
                        #[cfg(feature = $feature)]
                        Self::$variant(config) => Operator::from_config(*config)?,
                    )*
                };
                let capabilities = op.info().capability();
                anyhow::ensure!(
                    capabilities.read && capabilities.write && capabilities.stat,
                    "artifact storage must support read, write, and stat"
                );
                let transport = http::transport(http)?;
                let context = op.base_context().with_http_transport(transport);
                Ok(op.with_context(context))
            }
        }
    };
}

storage_backends! {
    ("storage-aliyun-drive", "aliyun_drive", AliyunDrive),
    ("storage-alluxio", "alluxio", Alluxio),
    ("storage-azblob", "azblob", Azblob),
    ("storage-azdls", "azdls", Azdls),
    ("storage-azfile", "azfile", Azfile),
    ("storage-b2", "b2", B2),
    ("storage-cacache", "cacache", Cacache),
    ("storage-cloudflare-kv", "cloudflare_kv", CloudflareKv),
    ("storage-compfs", "compfs", Compfs),
    ("storage-cos", "cos", Cos),
    ("storage-d1", "d1", D1),
    ("storage-dashmap", "dashmap", Dashmap),
    ("storage-dbfs", "dbfs", Dbfs),
    ("storage-dropbox", "dropbox", Dropbox),
    ("storage-etcd", "etcd", Etcd),
    ("storage-foundationdb", "foundationdb", Foundationdb),
    ("storage-foyer", "foyer", Foyer),
    ("storage-ftp", "ftp", Ftp),
    ("storage-gcs", "gcs", Gcs),
    ("storage-gdrive", "gdrive", Gdrive),
    ("storage-ghac", "ghac", Ghac),
    ("storage-github", "github", Github),
    ("storage-goosefs", "goosefs", Goosefs),
    ("storage-gridfs", "gridfs", Gridfs),
    ("storage-hdfs", "hdfs", Hdfs),
    ("storage-hdfs-native", "hdfs_native", HdfsNative),
    ("storage-hf", "hf", Hf),
    ("storage-http", "http", Http),
    ("storage-ipfs", "ipfs", Ipfs),
    ("storage-ipmfs", "ipmfs", Ipmfs),
    ("storage-koofr", "koofr", Koofr),
    ("storage-lakefs", "lakefs", Lakefs),
    ("storage-memcached", "memcached", Memcached),
    ("storage-memory", "memory", Memory),
    ("storage-mini-moka", "mini_moka", MiniMoka),
    ("storage-moka", "moka", Moka),
    ("storage-mongodb", "mongodb", Mongodb),
    ("storage-monoiofs", "monoiofs", Monoiofs),
    ("storage-mysql", "mysql", Mysql),
    ("storage-obs", "obs", Obs),
    ("storage-onedrive", "onedrive", Onedrive),
    ("storage-opfs", "opfs", Opfs),
    ("storage-oss", "oss", Oss),
    ("storage-pcloud", "pcloud", Pcloud),
    ("storage-persy", "persy", Persy),
    ("storage-postgresql", "postgresql", Postgresql),
    ("storage-redb", "redb", Redb),
    ("storage-redis", "redis", Redis),
    ("storage-rocksdb", "rocksdb", Rocksdb),
    ("storage-s3", "s3", S3),
    ("storage-seafile", "seafile", Seafile),
    ("storage-sftp", "sftp", Sftp),
    ("storage-sled", "sled", Sled),
    ("storage-sqlite", "sqlite", Sqlite),
    ("storage-surrealdb", "surrealdb", Surrealdb),
    ("storage-swift", "swift", Swift),
    ("storage-tikv", "tikv", Tikv),
    ("storage-tos", "tos", Tos),
    ("storage-upyun", "upyun", Upyun),
    ("storage-vercel-artifacts", "vercel_artifacts", VercelArtifacts),
    ("storage-vercel-blob", "vercel_blob", VercelBlob),
    ("storage-webdav", "webdav", Webdav),
    ("storage-webhdfs", "webhdfs", Webhdfs),
    ("storage-yandex-disk", "yandex_disk", YandexDisk),
}

impl Storage {
    #[cfg(test)]
    pub fn filesystem_template() -> Self {
        let mut config = services::FsConfig::default();
        config.root = Some("artifacts".into());
        config.atomic_write_dir = Some("artifacts/.tmp".into());
        Self::Fs(config)
    }
}

#[cfg(feature = "config-template")]
impl toml_example::TomlExample for Storage {
    fn toml_example() -> String {
        Self::toml_example_with_prefix("", ("[", "]"), "")
    }

    fn toml_example_with_prefix(label: &str, label_format: (&str, &str), prefix: &str) -> String {
        let section = if label.is_empty() {
            "fs".to_owned()
        } else {
            format!("{label}.fs")
        };
        format!(
            "{prefix}{}{section}{}\n{prefix}root = \"artifacts\"\n{prefix}atomic_write_dir = \"artifacts/.tmp\"\n",
            label_format.0,
            label_format.1.trim_end_matches('\n')
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_requires_exactly_one_backend() {
        assert!(toml::from_str::<Storage>("[fs]\nroot = 'artifacts'\n[unknown]\n").is_err());
        assert!(toml::from_str::<Storage>("[unknown]\n").is_err());
    }

    #[test]
    fn settings_require_explicit_storage() {
        for text in ["", "[s3]\nbucket = 'old-cache'\n"] {
            let error = toml::from_str::<crate::settings::Settings>(text)
                .err()
                .expect("storage selection must be explicit");
            assert!(error.to_string().contains("storage"));
        }
    }

    #[test]
    fn old_s3_section_is_ignored_when_storage_is_configured() {
        let settings: crate::settings::Settings =
            toml::from_str("[s3]\nbucket = 'unused'\n[storage.fs]\nroot = 'artifacts'\n").unwrap();
        assert!(matches!(settings.storage, Storage::Fs(_)));
        assert_eq!(settings.bind.to_string(), "127.0.0.1:3000");
        assert_eq!(settings.path_prefix, "/");
        assert_eq!(
            settings.indices["default"].sources,
            ["https://pypi.org/simple/{package}/"]
        );
    }

    #[test]
    fn explicit_filesystem_template_roundtrips() {
        let text = toml::to_string(&crate::settings::Settings::for_test()).unwrap();
        let decoded: crate::settings::Settings = toml::from_str(&text).unwrap();
        assert!(matches!(decoded.storage, Storage::Fs(_)));
    }

    #[cfg(feature = "storage-s3")]
    #[test]
    fn s3_options_keep_native_types() {
        let storage: Storage = toml::from_str(
            "[s3]\nbucket = 'packages'\nregion = 'us-east-1'\nskip_signature = true\n",
        )
        .unwrap();
        let Storage::S3(config) = storage else {
            panic!("expected S3")
        };
        assert!(config.skip_signature);
        assert!(
            toml::from_str::<Storage>("[s3]\nbucket = 'packages'\nskip_signature = 'true'\n")
                .is_err()
        );
    }
}
