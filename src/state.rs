use std::{collections::HashMap, sync::Arc};

use bytes::{Bytes, BytesMut};
use futures::Stream;
use moka::future::Cache;
use quick_xml::{events::Event, reader::Reader as XmlReader};
use redb::{Database, ReadableDatabase, Table, TableDefinition, WriteTransaction};
use reqwest::Client;
use s3::{Bucket, Region};
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

    pub async fn get_file(
        &self,
        hash: String,
        file: String,
    ) -> anyhow::Result<File<impl Stream<Item = anyhow::Result<Bytes>> + use<>>> {
        tracing::debug!("processing request for {file}");
        let s3_path = format!("{}/{}/{hash}/{file}", &hash[0..2], &hash[2..4]);

        let data = self.0.read().await;
        if let Some(rw_lock) = data.download_locks.get(&hash) {
            tracing::debug!("already downloading {file}, waiting...");
            let _ = rw_lock.read().await;
        }

        if data.bucket.head_object(&s3_path).await.is_ok() {
            tracing::debug!("serving cached {file}");
            return data
                .bucket
                .presign_get(s3_path, 600, None)
                .map_err(anyhow::Error::from)
                .map(File::PresignedUrl);
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
                    .bucket
                    .put_object(&s3_path, &buf[..])
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
            let mut part_number = 2u32;
            let mut parts = Vec::new();
            let mp_upload = data
                .bucket
                .initiate_multipart_upload(&s3_path, &content_type)
                .await?;

            yield Ok(buf.clone());

            match data
                .bucket
                .put_multipart_chunk(
                    buf.into(),
                    &s3_path,
                    part_number,
                    &mp_upload.upload_id,
                    &content_type,
                )
                .await
            {
                Ok(part) => parts.push(part),
                Err(err) => {
                    tracing::error!("Failed to put multipart chunk #{part_number} to S3: {err}")
                }
            }

            part_number += 1;

            while let Some(buf) = buffered_reader.chunk().await? {
                yield Ok(buf.clone());

                match data
                    .bucket
                    .put_multipart_chunk(
                        buf.into(),
                        &s3_path,
                        part_number,
                        &mp_upload.upload_id,
                        &content_type,
                    )
                    .await
                {
                    Ok(part) => parts.push(part),
                    Err(err) => {
                        tracing::error!("Failed to put multipart chunk #{part_number} to S3: {err}")
                    }
                }

                part_number += 1;
            }


            if let Err(err) = data
                .bucket
                .complete_multipart_upload(&s3_path, &mp_upload.upload_id, parts)
                .await
            {
                tracing::error!("Failed to save file to S3: {err}");
            }

            // drop(file_write_guard);
            drop(data);
            data_ptr.write().await.download_locks.remove(&hash);
        };

        Ok(File::Stream(stream))
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
            s3,
            ..
        } = settings;

        let db = Database::create(db_path)?;

        let index_cache = Cache::builder()
            .time_to_live(index_cache.ttl.into())
            .max_capacity(index_cache.max_capacity.into())
            .build();

        let region = match s3.endpoint {
            Some(endpoint) => Region::Custom {
                region: s3.region.to_string(),
                endpoint,
            },
            None => s3.region,
        };

        let indices_list = format_indices_list(&base_url, &indices);

        let indices = indices
            .into_iter()
            .map(|(slug, urls)| (slug, urls.into_iter().map(IndexUrlTemplate).collect()))
            .collect();

        let bucket = Bucket::new(&s3.bucket, region.clone(), s3.credentials)?.with_path_style();

        let data = StateData {
            base_url,
            db,
            http_client: Client::new(),
            index_cache,
            indices,
            indices_list,
            bucket,
            download_locks: HashMap::new(),
        };

        Ok(Self(Arc::new(data.into())))
    }
}

pub enum File<S> {
    PresignedUrl(String),
    Bytes(Bytes),
    Stream(S),
    None,
}

struct StateData {
    base_url: String,
    db: Database,
    http_client: Client,
    index_cache: Cache<String, Bytes>,
    indices: HashMap<String, Vec<IndexUrlTemplate>>,
    indices_list: String,
    bucket: Bucket,
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
