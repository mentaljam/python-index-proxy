use std::{collections::HashMap, path::Path, pin::Pin, sync::Arc, time::Duration};

use anyhow::Context;
use bytes::{Bytes, BytesMut};
use futures::{Stream, TryStreamExt};
use moka::future::Cache;
use opendal::Operator;
use quick_xml::{events::Event, reader::Reader as XmlReader};
use redb::{Database, ReadableDatabase, Table, TableDefinition, WriteTransaction};
use reqwest::Client;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;
use url::Url;

use crate::settings::Settings;

const FILE_LINK_TABLE: TableDefinition<&str, String> = TableDefinition::new("file_link");
const CHUNK_SIZE: usize = 5 * 1024 * 1024;
const CHUNK_BUFFER_SIZE: usize = CHUNK_SIZE + 512 * 1024;
// const VND_PYPI_SIMPLE_V1_JSON: &str = "application/vnd.pypi.simple.v1+json";

#[derive(Clone)]
pub struct State(Arc<RwLock<StateData>>);

impl State {
    pub async fn get_indices_list(&self) -> String {
        self.0.read().await.indices_list.clone()
    }

    pub async fn get_detail(
        &self,
        index_slug: String,
        package: String,
    ) -> anyhow::Result<Option<Bytes>> {
        let data = self.0.read().await;
        let cache_key = format!("{index_slug}/{package}");
        let index = data.index_cache.get(&cache_key).await;
        if index.is_some() {
            return Ok(index);
        }

        drop(data);
        let data = self.0.write().await;
        let txn = data.db.begin_write()?;
        let mut parser = DetailParser::new(&data.base_url, &txn)?;
        let url_templates = match data.indices.get(&index_slug) {
            Some(u) => u,
            None => return Ok(None),
        };

        for url_template in url_templates {
            let package_url = url_template.package_url(&package)?;
            let response = data
                .http_client
                .get(package_url.clone())
                // .header(header::ACCEPT, VND_PYPI_SIMPLE_V1_JSON)
                .send()
                .await?;
            if response.status().is_success() {
                // let content_type = response
                //     .headers()
                //     .get(header::CONTENT_TYPE)
                //     .map(header::HeaderValue::to_owned);
                let details = response.bytes().await?;
                // match content_type {
                //     Some(h) if h == VND_PYPI_SIMPLE_V1_JSON => {
                //         parser.parse_json(&package_url, details).await?
                //     }
                //     _ => parser.parse_html(&package_url, details).await?,
                // }
                parser.parse_html(&package_url, details).await?
            }
        }

        let index = parser.finalize();
        txn.commit()?;
        if let Some(index) = &index {
            data.index_cache.insert(cache_key, index.clone()).await;
        }
        Ok(index)
    }

    pub async fn get_file(&self, hash: String, file: String) -> anyhow::Result<File> {
        tracing::debug!("processing request for {file}");
        let object_path = format!("{}/{}/{hash}/{file}", &hash[0..2], &hash[2..4]);

        let data = self.0.read().await;
        if let Some(rw_lock) = data.download_locks.get(&hash) {
            tracing::debug!("already downloading {file}, waiting...");
            let _ = rw_lock.read().await;
        }

        if data.storage.stat(&object_path).await.is_ok() {
            tracing::debug!("serving cached {file}");
            return cached_file(&data.storage, &object_path).await;
        }

        let mut url = match data
            .db
            .begin_read()?
            .open_table(FILE_LINK_TABLE)?
            .get(hash.as_str())?
        {
            Some(url) => url.value(),
            None => return Ok(File::None),
        };

        if file.ends_with(".metadata") {
            url.push_str(".metadata");
        }

        let response = data.http_client.get(&url).send().await?;
        if !response.status().is_success() {
            anyhow::bail!("Failed to get {url}")
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .map_or("application/octet-stream", |h| {
                h.to_str().unwrap_or("application/octet-stream")
            })
            .to_owned();

        drop(data);
        let file_lock = self
            .0
            .write()
            .await
            .download_locks
            .entry(hash.clone())
            .or_default()
            .clone();

        tracing::debug!("downloading {file}");
        let mut buffered_reader = BufferedResponseReader(response);
        let buf = match buffered_reader.chunk().await? {
            Some(buf) if buf.len() < CHUNK_SIZE - 1 => {
                let _file_write_guard = file_lock.write().await;
                self.0
                    .read()
                    .await
                    .storage
                    .write(&object_path, buf.clone())
                    .await?;
                self.0.write().await.download_locks.remove(&hash);
                return Ok(File::Bytes(buf));
            }
            Some(buf) => buf,
            None => return Ok(File::None),
        };

        let data_ptr = self.0.clone();
        let stream = async_stream::stream! {
            let _file_write_guard = file_lock.write().await;
            let data = data_ptr.read().await;
            let mut writer = data.storage.writer_with(&object_path)
                .content_type(&content_type).chunk(CHUNK_SIZE).await?;

            if let Err(err) = writer.write(buf.clone()).await {
                let _ = writer.abort().await;
                Err(err)?;
            }
            yield Ok(buf);

            loop {
                let next = match buffered_reader.chunk().await {
                    Ok(next) => next,
                    Err(err) => {
                        let _ = writer.abort().await;
                        Err(err)?;
                        unreachable!();
                    }
                };
                let Some(buf) = next else { break };
                if let Err(err) = writer.write(buf.clone()).await {
                    let _ = writer.abort().await;
                    Err(err)?;
                }
                yield Ok(buf);
            }
            if let Err(err) = writer.close().await {
                let _ = writer.abort().await;
                Err(err)?;
            }

            // drop(file_write_guard);
            drop(data);
            data_ptr.write().await.download_locks.remove(&hash);
        };

        Ok(File::Stream(Box::pin(stream)))
    }
}

impl TryFrom<Settings> for State {
    type Error = anyhow::Error;

    fn try_from(settings: Settings) -> Result<Self, Self::Error> {
        let Settings {
            base_url,
            db_path,
            index_cache,
            indices,
            storage,
            ..
        } = settings;

        let db = open_database(&db_path)?;

        let index_cache = Cache::builder()
            .time_to_live(index_cache.ttl.into())
            .max_capacity(index_cache.max_capacity.into())
            .build();

        let indices_list = format_indices_list(&base_url, &indices);

        let indices = indices
            .into_iter()
            .map(|(slug, urls)| (slug, urls.into_iter().map(IndexUrlTemplate).collect()))
            .collect();

        let storage = storage.operator()?;

        let data = StateData {
            base_url,
            db,
            http_client: Client::new(),
            index_cache,
            indices,
            indices_list,
            storage,
            download_locks: HashMap::new(),
        };

        Ok(Self(Arc::new(data.into())))
    }
}

fn open_database(path: &Path) -> anyhow::Result<Database> {
    match Database::create(path) {
        Ok(db) => return Ok(db),
        Err(redb::DatabaseError::UpgradeRequired(2)) => {}
        Err(err) => return Err(err.into()),
    }

    // Keep the legacy database locked while backing it up and upgrading it.
    let mut legacy = redb_legacy::Database::open(path)
        .with_context(|| format!("Failed to open legacy database {}", path.display()))?;
    let mut backup_path = path.as_os_str().to_owned();
    backup_path.push(".v2.bak");
    let backup_path = std::path::PathBuf::from(backup_path);

    tracing::info!(database = %path.display(), backup = %backup_path.display(), "Backing up v2 database before migration");
    let mut source = std::fs::File::open(path)?;
    let mut backup = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&backup_path)
        .with_context(|| {
            format!(
                "Failed to create database backup {}; existing backups are never overwritten",
                backup_path.display()
            )
        })?;
    backup.set_permissions(source.metadata()?.permissions())?;
    std::io::copy(&mut source, &mut backup)
        .with_context(|| format!("Failed to copy database to {}", backup_path.display()))?;
    backup
        .sync_all()
        .with_context(|| format!("Failed to sync database backup {}", backup_path.display()))?;
    drop(backup);
    drop(source);

    legacy.upgrade().with_context(|| {
        format!(
            "Failed to migrate database {}; backup: {}",
            path.display(),
            backup_path.display()
        )
    })?;
    drop(legacy);

    let db = Database::open(path)
        .with_context(|| format!("Failed to open migrated database {}", path.display()))?;
    tracing::info!(database = %path.display(), "Database migrated from v2 to v3");
    Ok(db)
}

pub enum File {
    PresignedUrl(String),
    Bytes(Bytes),
    Stream(Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send>>),
    None,
}

async fn cached_file(storage: &Operator, path: &str) -> anyhow::Result<File> {
    if storage.info().capability().presign_read {
        let request = storage.presign_read(path, Duration::from_secs(600)).await?;
        // A redirect cannot supply arbitrary signed headers to the client.
        if request.method() == http::Method::GET
            && request
                .header()
                .keys()
                .all(|name| name == http::header::HOST)
        {
            return Ok(File::PresignedUrl(request.uri().to_string()));
        }
    }
    let stream = storage
        .reader(path)
        .await?
        .into_bytes_stream(..)
        .await?
        .map_err(anyhow::Error::from);
    Ok(File::Stream(Box::pin(stream)))
}

struct StateData {
    base_url: String,
    db: Database,
    http_client: Client,
    index_cache: Cache<String, Bytes>,
    indices: HashMap<String, Vec<IndexUrlTemplate>>,
    indices_list: String,
    storage: Operator,
    download_locks: HashMap<String, Arc<RwLock<()>>>,
}

struct IndexUrlTemplate(String);

impl IndexUrlTemplate {
    fn package_url(&self, package: &str) -> Result<Url, url::ParseError> {
        self.0.replace("{package}", package).parse()
    }
}

struct DetailParser<'tnx> {
    file_links: Table<'tnx, &'static str, String>,
    link_count: usize,
    html: Vec<u8>,
    buf: Vec<u8>,
    anchor_start: String,
}

impl<'txn> DetailParser<'txn> {
    fn new(base_url: &str, txn: &'txn WriteTransaction) -> anyhow::Result<Self> {
        Ok(Self {
            file_links: txn.open_table(FILE_LINK_TABLE)?,
            link_count: 0,
            html: Vec::from(b"<!DOCTYPE html>\n<html>\n<body>\n"),
            buf: Vec::new(),
            anchor_start: format!(r#"<a href="{}packages/"#, base_url),
        })
    }

    // async fn parse_json(&mut self, package_url: &Url, content: Bytes) -> anyhow::Result<()> {
    //     let detail: Detail = serde_json::from_slice(&content)?;
    //     Ok(())
    // }

    async fn parse_html(&mut self, package_url: &Url, content: Bytes) -> anyhow::Result<()> {
        let Self {
            file_links,
            link_count,
            html,
            buf,
            anchor_start,
        } = self;

        let mut reader = XmlReader::from_reader(&*content);
        reader.config_mut().trim_text(true);
        reader.config_mut().check_end_names = false;
        let mut in_a = false;

        loop {
            match reader.read_event_into_async(buf).await? {
                Event::Start(e) if e.name().as_ref() == "a" => {
                    in_a = true;
                    if let Some(href) = e.try_get_attribute("href")? {
                        let url = href.value.split('#').next().unwrap();
                        if let Some(file) = url.rsplit('/').next() {
                            let hash = Sha256::digest(href.value.as_bytes());
                            let hex = hex::encode(hash);

                            let attributes = e.attributes().filter(|a| {
                                a.as_ref()
                                    .map(|a| a.key.as_ref() != "href")
                                    .unwrap_or(false)
                            });

                            html.extend(anchor_start.as_bytes());
                            html.extend(hex.as_bytes());
                            html.push(b'/');
                            html.extend(file.as_bytes());
                            html.push(b'"');
                            for attr in attributes {
                                let attr = attr?;
                                html.push(b' ');
                                html.extend(attr.key.as_ref().as_bytes());
                                html.extend(br#"=""#);
                                html.extend(attr.value.as_bytes());
                                html.push(b'"');
                            }
                            html.push(b'>');

                            let url = make_absolute(package_url, url)?;
                            file_links.insert(hex.as_str(), &url)?;
                            *link_count += 1;
                        }
                    }
                }
                Event::End(e) if in_a && e.name().as_ref() == "a" => {
                    in_a = false;
                    html.extend(b"</a><br/>\n");
                }
                Event::Text(text) if in_a => {
                    html.extend(text.as_bytes());
                }
                Event::GeneralRef(reference) if in_a => {
                    html.push(b'&');
                    html.extend(reference.as_bytes());
                    html.push(b';');
                }
                Event::Eof => break,
                _ => (),
            }
            buf.clear();
        }

        Ok(())
    }

    fn finalize(self) -> Option<Bytes> {
        let Self {
            mut html,
            link_count,
            ..
        } = self;
        if link_count > 0 {
            html.extend(b"</body>\n</html>");
            Some(html.into())
        } else {
            None
        }
    }
}

struct BufferedResponseReader(reqwest::Response);

impl BufferedResponseReader {
    async fn chunk(&mut self) -> reqwest::Result<Option<Bytes>> {
        let mut buf = BytesMut::with_capacity(CHUNK_BUFFER_SIZE);
        while let Some(chunk) = self.0.chunk().await? {
            buf.extend(chunk);
            if buf.len() > CHUNK_SIZE {
                break;
            }
        }
        if buf.is_empty() {
            Ok(None)
        } else {
            Ok(Some(buf.into()))
        }
    }
}

fn make_absolute(package_url: &Url, url: &str) -> anyhow::Result<String> {
    let url = if Url::parse(url).is_ok() {
        url.to_owned()
    } else {
        package_url.join(url)?.to_string()
    };
    Ok(url)
}

fn format_indices_list(base_url: &str, indices: &HashMap<String, Vec<String>>) -> String {
    let mut list = "<!DOCTYPE html>\n<html>\n<body>".into();
    for index in indices.keys() {
        list = format!("{list}\n<a href=\"{base_url}{index}/\">{index}</a><br/>");
    }
    list.push_str("\n</body>\n</html>");
    list
}

#[cfg(test)]
mod storage_tests {
    use super::*;
    use futures::TryStreamExt;

    #[tokio::test]
    async fn filesystem_cache_hit_streams_exact_bytes_without_database_mapping() {
        let root = std::env::temp_dir().join(format!(
            "pipr-storage-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut config = opendal::services::FsConfig::default();
        config.root = Some(root.to_str().unwrap().into());
        let storage = crate::storage::Storage::Fs(config).operator().unwrap();
        let path = "ab/cd/identifier/package.whl";
        let mut writer = storage
            .writer_with(path)
            .content_type("application/octet-stream")
            .chunk(CHUNK_SIZE)
            .await
            .unwrap();
        writer.write(Bytes::from_static(b"cached ")).await.unwrap();
        assert!(
            storage.stat(path).await.is_err(),
            "unfinished artifact must stay hidden"
        );
        writer
            .write(Bytes::from_static(b"wheel bytes"))
            .await
            .unwrap();
        writer.close().await.unwrap();
        let File::Stream(stream) = cached_file(&storage, path).await.unwrap() else {
            panic!("filesystem cache hits must stream")
        };
        let chunks: Vec<Bytes> = stream.try_collect().await.unwrap();
        assert_eq!(chunks.concat(), b"cached wheel bytes");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(feature = "storage-s3")]
    #[tokio::test]
    async fn s3_cache_hit_presigns_existing_object_path() {
        opendal::install_default();
        let storage: crate::storage::Storage = toml::from_str(
            "[s3]\nbucket = 'packages'\nregion = 'us-east-1'\nendpoint = 'https://objects.example.test'\naccess_key_id = 'test-key'\nsecret_access_key = 'test-secret'\ndisable_config_load = true\ndisable_ec2_metadata = true\n"
        ).unwrap();
        let storage = storage.operator().unwrap();
        let File::PresignedUrl(url) = cached_file(&storage, "ab/cd/package.whl").await.unwrap()
        else {
            panic!("S3 cache hits must redirect when signature needs no extra headers")
        };
        let url = Url::parse(&url).unwrap();
        assert_eq!(url.host_str(), Some("objects.example.test"));
        assert_eq!(url.path(), "/packages/ab/cd/package.whl");
        assert!(url.query_pairs().any(|(name, _)| name == "X-Amz-Signature"));
        assert!(
            url.query_pairs()
                .any(|(name, value)| name == "X-Amz-Expires" && value == "600")
        );
    }
}
