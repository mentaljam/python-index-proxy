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
        if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Ok(File::None);
        }
        tracing::debug!("processing request for {file}");
        let object_path = format!("{}/{}/{hash}/{file}", &hash[0..2], &hash[2..4]);

        let download_lock = {
            let data = self.0.read().await;
            data.download_locks.get(&hash).cloned()
        };
        if let Some(rw_lock) = download_lock {
            tracing::debug!("already downloading {file}, waiting...");
            // The active download needs the state write lock before releasing this lock.
            let _ = rw_lock.read().await;
        }

        let data = self.0.read().await;
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
        let buf = match buffered_reader.chunk().await {
            Ok(Some(buf)) if buf.len() < CHUNK_SIZE - 1 => {
                let _file_write_guard = file_lock.write().await;
                let storage = self.0.read().await.storage.clone();
                let result = storage.write(&object_path, buf.clone()).await;
                remove_download_lock(&self.0, &hash, &file_lock).await;
                result?;
                return Ok(File::Bytes(buf));
            }
            Ok(Some(buf)) => buf,
            Ok(None) => {
                remove_download_lock(&self.0, &hash, &file_lock).await;
                return Ok(File::None);
            }
            Err(err) => {
                remove_download_lock(&self.0, &hash, &file_lock).await;
                return Err(err.into());
            }
        };

        let data_ptr = self.0.clone();
        let stream = async_stream::stream! {
            let _file_write_guard = file_lock.write().await;
            let storage = data_ptr.read().await.storage.clone();
            // Finish cleanup before exposing an error: the consumer may stop polling then.
            let result: anyhow::Result<()> = 'upload: {
                let mut writer = match storage.writer_with(&object_path)
                    .content_type(&content_type).chunk(CHUNK_SIZE).await {
                    Ok(writer) => writer,
                    Err(err) => break 'upload Err(err.into()),
                };

                if let Err(err) = writer.write(buf.clone()).await {
                    let _ = writer.abort().await;
                    break 'upload Err(err.into());
                }
                yield Ok(buf);

                loop {
                    let next = match buffered_reader.chunk().await {
                        Ok(next) => next,
                        Err(err) => {
                            let _ = writer.abort().await;
                            break 'upload Err(err.into());
                        }
                    };
                    let Some(buf) = next else { break };
                    if let Err(err) = writer.write(buf.clone()).await {
                        let _ = writer.abort().await;
                        break 'upload Err(err.into());
                    }
                    yield Ok(buf);
                }
                if let Err(err) = writer.close().await {
                    let _ = writer.abort().await;
                    break 'upload Err(err.into());
                }
                Ok(())
            };
            remove_download_lock(&data_ptr, &hash, &file_lock).await;
            if let Err(err) = result {
                yield Err(err);
            }
        };

        Ok(File::Stream(Box::pin(stream)))
    }
}

async fn remove_download_lock(state: &RwLock<StateData>, hash: &str, lock: &Arc<RwLock<()>>) {
    let mut data = state.write().await;
    if data
        .download_locks
        .get(hash)
        .is_some_and(|current| Arc::ptr_eq(current, lock))
    {
        data.download_locks.remove(hash);
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
            // Account for key and payload bytes, not allocator or cache overhead.
            .weigher(|key: &String, value: &Bytes| {
                u32::try_from(key.len().saturating_add(value.len())).unwrap_or(u32::MAX)
            })
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
                        let (url, fragment) = href
                            .value
                            .split_once('#')
                            .map_or((href.value.as_ref(), None), |(url, fragment)| {
                                (url, Some(fragment))
                            });
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
                            if let Some(fragment) = fragment {
                                html.push(b'#');
                                html.extend(fragment.as_bytes());
                            }
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
    use redb::ReadableTable;

    #[tokio::test]
    async fn index_cache_capacity_counts_key_and_payload_bytes() {
        let root = std::env::temp_dir().join(format!(
            "pipr-cache-capacity-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut settings = Settings::template();
        settings.db_path = root.join("metadata.redb");
        settings.index_cache = toml::from_str("max_capacity = '12 B'").unwrap();
        let mut config = opendal::services::FsConfig::default();
        config.root = Some(root.join("artifacts").to_str().unwrap().into());
        settings.storage = crate::storage::Storage::Fs(config);
        let state = State::try_from(settings).unwrap();
        let cache = state.0.read().await.index_cache.clone();

        // Four UTF-8 key bytes and eight payload bytes exactly fill the budget.
        cache
            .insert("é/x".into(), Bytes::from_static(b"12345678"))
            .await;
        cache.run_pending_tasks().await;
        assert_eq!(cache.weighted_size(), 12);
        assert_eq!(cache.get("é/x").await.as_deref(), Some(&b"12345678"[..]));
        cache.invalidate_all();
        cache.run_pending_tasks().await;

        for (key, value) in [
            ("key", Bytes::from_static(b"1234567890")),
            ("long-cache-key", Bytes::new()),
        ] {
            cache.insert(key.into(), value).await;
            cache.run_pending_tasks().await;
            assert!(
                cache.get(key).await.is_none(),
                "oversized entry must not remain: {key}"
            );
        }

        // Each entry fits separately, but the pair must not remain together.
        cache
            .insert("a".into(), Bytes::from_static(b"12345678"))
            .await;
        cache.run_pending_tasks().await;
        assert!(cache.get("a").await.is_some());
        cache
            .insert("b".into(), Bytes::from_static(b"12345678"))
            .await;
        cache.run_pending_tasks().await;
        assert_eq!(cache.entry_count(), 1);
        assert_eq!(cache.weighted_size(), 9);

        drop(cache);
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn rewritten_file_links_preserve_fragments_without_changing_fetch_urls() {
        let root = std::env::temp_dir().join(format!(
            "pipr-link-fragments-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&root).unwrap();
        let db = Database::create(root.join("metadata.redb")).unwrap();
        let package_url = Url::parse("https://index.example.test/simple/package/").unwrap();
        let cases = [
            (
                "https://files.example.test/package.whl#sha256=ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                "package.whl#sha256=ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                "https://files.example.test/package.whl",
                "6a16a8955b2babddb0c3ad773fc7a6add3f0b2689d00db155a068757728981f8",
            ),
            (
                "../../files/package.tar.gz#sha256=ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                "package.tar.gz#sha256=ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                "https://index.example.test/files/package.tar.gz",
                "07bdb75e783010a2edc6fd45fe244662ecba7e483d2ca856053bbe2482c728a5",
            ),
            (
                "package.zip",
                "package.zip",
                "https://index.example.test/simple/package/package.zip",
                "325f0f32c85c47a2de246674622845902200ac65e955be08942b31496c5e7ae1",
            ),
            (
                "package.zip#custom=value#extra",
                "package.zip#custom=value#extra",
                "https://index.example.test/simple/package/package.zip",
                "c30b441a46793feb524598937ee624b7c1d69b3a5beef092d1ea859d7925b5ab",
            ),
        ];
        for (href, expected_file, expected_upstream, expected_identifier) in cases {
            let txn = db.begin_write().unwrap();
            let mut parser = DetailParser::new("/proxy/", &txn).unwrap();
            parser
                .parse_html(
                    &package_url,
                    format!(r#"<a href="{href}">package</a>"#).into(),
                )
                .await
                .unwrap();
            let html = String::from_utf8(parser.finalize().unwrap().to_vec()).unwrap();
            let rewritten = html
                .split("href=\"")
                .nth(1)
                .unwrap()
                .split('"')
                .next()
                .unwrap();
            let (identifier, file) = rewritten
                .strip_prefix("/proxy/packages/")
                .unwrap()
                .split_once('/')
                .unwrap();
            assert_eq!(identifier, expected_identifier);
            assert_eq!(file, expected_file);
            assert_eq!(
                txn.open_table(FILE_LINK_TABLE)
                    .unwrap()
                    .get(identifier)
                    .unwrap()
                    .unwrap()
                    .value(),
                expected_upstream
            );
            txn.commit().unwrap();
        }
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn get_file_rejects_malformed_identifiers_before_storage_lookup() {
        let root = std::env::temp_dir().join(format!(
            "pipr-invalid-identifiers-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut settings = Settings::template();
        settings.db_path = root.join("metadata.redb");
        let mut config = opendal::services::FsConfig::default();
        config.root = Some(root.join("artifacts").to_str().unwrap().into());
        settings.storage = crate::storage::Storage::Fs(config);
        let state = State::try_from(settings).unwrap();
        let storage = state.0.read().await.storage.clone();
        let nonhex = "g".repeat(64);
        storage
            .write(
                format!("gg/gg/{nonhex}/package.whl").as_str(),
                "invalid-key artifact",
            )
            .await
            .unwrap();
        let invalid = [
            String::new(),
            "a".into(),
            "abc".into(),
            "a".repeat(63),
            "a".repeat(65),
            nonhex,
            "é".repeat(32),
            format!("€{}", "a".repeat(61)),
            format!("aé{}", "a".repeat(61)),
        ];
        for hash in invalid {
            assert!(
                matches!(
                    state
                        .get_file(hash.clone(), "package.whl".into())
                        .await
                        .unwrap(),
                    File::None
                ),
                "malformed identifier must not resolve: {hash:?}"
            );
        }
        // A valid cached identifier must still resolve without a database mapping.
        let hash = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        storage
            .write(
                format!("ab/cd/{hash}/package.whl").as_str(),
                "valid artifact",
            )
            .await
            .unwrap();
        let File::Stream(stream) = state
            .get_file(hash.into(), "package.whl".into())
            .await
            .unwrap()
        else {
            panic!("valid cached identifier must stream")
        };
        let chunks: Vec<Bytes> = stream.try_collect().await.unwrap();
        assert_eq!(chunks.concat(), b"valid artifact");
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    }

    async fn failed_download_fixture(
        body: Bytes,
        truncated: bool,
    ) -> (State, std::path::PathBuf, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/package.whl", listener.local_addr().unwrap());
        let upstream = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    assert!(request.len() < 8192, "request headers must be bounded");
                    request.push(socket.read_u8().await.unwrap());
                }
                // Close each connection so retries receive a fresh response.
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len() + usize::from(truncated),
                );
                socket.write_all(header.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
                socket.shutdown().await.unwrap();
            }
        });
        let root = std::env::temp_dir().join(format!(
            "pipr-failed-download-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut settings = Settings::template();
        settings.db_path = root.join("metadata.redb");
        let mut config = opendal::services::FsConfig::default();
        config.root = Some(root.join("artifacts").to_str().unwrap().into());
        settings.storage = crate::storage::Storage::Fs(config);
        let state = State::try_from(settings).unwrap();
        {
            let data = state.0.read().await;
            let txn = data.db.begin_write().unwrap();
            txn.open_table(FILE_LINK_TABLE)
                .unwrap()
                .insert(
                    "abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab",
                    url,
                )
                .unwrap();
            txn.commit().unwrap();
        }
        (state, root, upstream)
    }

    #[tokio::test]
    async fn failed_writer_close_aborts_cleans_up_and_allows_retry() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let body = Bytes::from(vec![42; CHUNK_SIZE * 2]);
            let (state, root, upstream) = failed_download_fixture(body.clone(), false).await;
            let File::Stream(mut stream) = state
                .get_file("abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab".into(), "package.whl".into())
                .await
                .unwrap()
            else {
                panic!("large download must stream")
            };
            // Start the writer, then prevent its final atomic rename from succeeding.
            assert!(!stream.try_next().await.unwrap().unwrap().is_empty());
            let object = root.join("artifacts/ab/cd/abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab/package.whl");
            std::fs::create_dir_all(&object).unwrap();
            assert!(stream.try_collect::<Vec<_>>().await.is_err());
            assert!(state.0.read().await.download_locks.is_empty());
            assert_eq!(
                std::fs::read_dir(root.join("artifacts/.tmp"))
                    .unwrap()
                    .count(),
                0,
                "failed writer must abort and remove its temporary artifact"
            );
            std::fs::remove_dir(&object).unwrap();
            let File::Stream(stream) = state
                .get_file("abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab".into(), "package.whl".into())
                .await
                .unwrap()
            else {
                panic!("retry must download again")
            };
            let chunks = stream.try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(chunks.concat(), body.as_ref());
            assert_eq!(std::fs::read(&object).unwrap(), body.as_ref());
            assert!(state.0.read().await.download_locks.is_empty());
            upstream.abort();
            drop(state);
            std::fs::remove_dir_all(root).unwrap();
        })
        .await
        .expect("download failure handling must finish");
    }

    #[tokio::test]
    async fn upstream_body_failure_aborts_without_publishing_and_cleans_up() {
        tokio::time::timeout(Duration::from_secs(10), async {
            // Both the initial buffered read and a read after streaming has started can fail.
            for size in [64, CHUNK_SIZE * 2] {
                let (state, root, upstream) =
                    failed_download_fixture(Bytes::from(vec![42; size]), true).await;
                match state
                    .get_file("abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab".into(), "package.whl".into())
                    .await
                {
                    Err(_) if size < CHUNK_SIZE => {}
                    Ok(File::Stream(stream)) if size > CHUNK_SIZE => {
                        assert!(stream.try_collect::<Vec<_>>().await.is_err());
                    }
                    _ => panic!("truncated response must fail"),
                }
                assert!(state.0.read().await.download_locks.is_empty());
                assert!(
                    !root
                        .join("artifacts/ab/cd/abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab/package.whl")
                        .exists()
                );
                assert_eq!(
                    std::fs::read_dir(root.join("artifacts/.tmp"))
                        .unwrap()
                        .count(),
                    0
                );
                upstream.abort();
                drop(state);
                std::fs::remove_dir_all(root).unwrap();
            }
        })
        .await
        .expect("download failure handling must finish");
    }

    #[tokio::test]
    async fn waiting_download_allows_active_download_to_finish() {
        let root = std::env::temp_dir().join(format!(
            "pipr-download-lock-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut settings = Settings::template();
        settings.db_path = root.join("metadata.redb");
        let mut config = opendal::services::FsConfig::default();
        config.root = Some(root.join("artifacts").to_str().unwrap().into());
        settings.storage = crate::storage::Storage::Fs(config);
        let state = State::try_from(settings).unwrap();
        let hash = "abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab";
        let file_lock = Arc::new(RwLock::new(()));
        let active_download = file_lock.write().await;
        state
            .0
            .write()
            .await
            .download_locks
            .insert(hash.into(), file_lock.clone());

        let mut waiting_download = Box::pin(state.get_file(hash.into(), "package.whl".into()));
        // Polling to Pending puts the request at the held file lock without sleeps.
        assert!(futures::poll!(&mut waiting_download).is_pending());
        let storage = state.0.read().await.storage.clone();
        storage
            .write(
                "ab/cd/abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab/package.whl",
                Bytes::from_static(b"finished artifact"),
            )
            .await
            .unwrap();
        // The active download must be able to clean up while retaining its file lock.
        let mut data = tokio::time::timeout(Duration::from_secs(1), state.0.write())
            .await
            .expect("waiting request must not block active download cleanup");
        data.download_locks.remove(hash);
        drop(data);
        drop(active_download);

        let file = tokio::time::timeout(Duration::from_secs(1), &mut waiting_download)
            .await
            .expect("waiting request must resume after the active download")
            .unwrap();
        let File::Stream(stream) = file else {
            panic!("filesystem cache hit must stream")
        };
        let chunks: Vec<Bytes> = stream.try_collect().await.unwrap();
        assert_eq!(chunks.concat(), b"finished artifact");
        drop(waiting_download);
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    }

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
