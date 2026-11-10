use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use anyhow::Context;
use bytes::{Bytes, BytesMut};
use futures::{Stream, TryStreamExt};
use html5gum::{DefaultEmitter, StartTag, State as HtmlState, Token, Tokenizer};
use moka::future::Cache;
use opendal::Operator;
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use reqwest::Client;
use sha2::{Digest, Sha256};
use tokio::sync::{RwLock, mpsc, oneshot};
use url::Url;

use crate::settings::Settings;

const CHUNK_SIZE: usize = 5 * 1024 * 1024;
const CHUNK_BUFFER_SIZE: usize = CHUNK_SIZE + 512 * 1024;
// const VND_PYPI_SIMPLE_V1_JSON: &str = "application/vnd.pypi.simple.v1+json";

#[derive(Debug)]
pub struct UpstreamIndexError;

impl std::fmt::Display for UpstreamIndexError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Failed to fetch upstream package index")
    }
}

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
        Ok(self
            .package_detail(&index_slug, &package, None)
            .await?
            .map(|detail| detail.html.clone()))
    }

    async fn package_detail(
        &self,
        index_slug: &str,
        package: &str,
        artifact_id: Option<&str>,
    ) -> anyhow::Result<Option<Arc<PackageDetail>>> {
        if !valid_filename(index_slug) || !valid_filename(package) {
            return Ok(None);
        }
        let (index_cache, http_client, base_url, url_templates) = {
            let data = self.0.read().await;
            (
                data.index_cache.clone(),
                data.http_client.clone(),
                data.base_url.clone(),
                data.indices.get(index_slug).cloned(),
            )
        };
        let cache_key = format!("{}/{}", encode_segment(index_slug), encode_segment(package));
        if let Some(detail) = index_cache.get(&cache_key).await
            && artifact_id.is_none_or(|id| detail.files.contains_key(id))
        {
            return Ok(Some(detail));
        }
        let Some(url_templates) = url_templates else {
            return Ok(None);
        };
        let mut parser = DetailParser::new(&base_url, index_slug, package);
        for template in url_templates {
            let response = http_client
                .get(template.package_url(package)?)
                .send()
                .await
                .context(UpstreamIndexError)?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                continue;
            }
            if !response.status().is_success() {
                return Err(anyhow::anyhow!(
                    "Upstream index {} returned {}",
                    response.url(),
                    response.status()
                )
                .context(UpstreamIndexError));
            }
            let final_url = response.url().clone();
            let content = response.bytes().await.context(UpstreamIndexError)?;
            // Parsing is CPU-bound and must not monopolize an async worker.
            parser = tokio::task::spawn_blocking(move || {
                parser.parse_html(&final_url, &content)?;
                Ok::<_, anyhow::Error>(parser)
            })
            .await??;
        }
        let detail = parser.finalize().map(Arc::new);
        if let Some(detail) = &detail {
            index_cache.insert(cache_key, detail.clone()).await;
        }
        Ok(detail)
    }

    pub async fn get_file(
        &self,
        index: String,
        package: String,
        hash: String,
        file: String,
    ) -> anyhow::Result<File> {
        if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Ok(File::None);
        }
        if !valid_filename(&index)
            || !valid_filename(&package)
            || !valid_filename(&file)
            || !self.0.read().await.indices.contains_key(&index)
        {
            return Ok(File::None);
        }
        tracing::debug!("processing request for {file}");
        let object_path = artifact_path(&index, &package, &hash, &file);

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

        let http_client = data.http_client.clone();
        drop(data);
        let Some(detail) = self.package_detail(&index, &package, Some(&hash)).await? else {
            return Ok(File::None);
        };
        let Some(source) = detail.files.get(&hash) else {
            return Ok(File::None);
        };
        let mut url = Url::parse(&source.url)?;
        if file != source.filename {
            if file != format!("{}.metadata", source.filename) {
                return Ok(File::None);
            }
            url.set_path(&format!("{}.metadata", url.path()));
        }

        let response = http_client.get(url.clone()).send().await?;
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

        let (reply, result) = oneshot::channel();
        let state = self.0.clone();
        tokio::spawn(async move {
            download_response(state, hash, object_path, content_type, response, reply).await;
        });
        result.await?
    }
}

// The task owns the writer until close/abort completes. Dropping an HTTP body only
// closes its receiver; it never cancels an in-flight OpenDAL write operation.
async fn download_response(
    state: Arc<RwLock<StateData>>,
    hash: String,
    path: String,
    content_type: String,
    response: reqwest::Response,
    reply: oneshot::Sender<anyhow::Result<File>>,
) {
    let mut output = DownloadOutput {
        reply: Some(reply),
        body: None,
    };
    let file_lock = tokio::select! {
        biased;
        _ = output.closed() => return,
        lock = async { state.write().await.download_locks.entry(hash.clone()).or_default().clone() } => lock,
    };
    let mut file_guard = tokio::select! {
        biased;
        _ = output.closed() => None,
        guard = file_lock.clone().write_owned() => Some(guard),
    };
    let mut writer = None;
    let result = if file_guard.is_some() {
        let storage = state.read().await.storage.clone();
        transfer_response(
            response,
            &storage,
            &path,
            &content_type,
            &mut output,
            &mut writer,
        )
        .await
    } else {
        drop(response);
        // A cancelled waiter must not remove the active owner's registration.
        // Wait only for cleanup; no upstream work continues here.
        file_guard = Some(file_lock.clone().write_owned().await);
        Ok(None)
    };
    // The response has been dropped before cleanup, stopping upstream reads.
    if let Some(mut writer) = writer
        && let Err(err) = writer.abort().await
    {
        tracing::error!("Failed to abort artifact upload: {err}");
    }
    remove_download_lock(&state, &hash, &file_lock).await;
    drop(file_guard);
    // Publish errors only after cleanup: consumers need not poll after an error.
    output.finish(result).await;
}

async fn transfer_response(
    response: reqwest::Response,
    storage: &Operator,
    path: &str,
    content_type: &str,
    output: &mut DownloadOutput,
    writer: &mut Option<opendal::Writer>,
) -> anyhow::Result<Option<File>> {
    let mut reader = BufferedResponseReader(response);
    let first = tokio::select! {
        biased;
        _ = output.closed() => return Ok(None),
        chunk = reader.chunk() => chunk?,
    };
    let Some(mut buf) = first else {
        return Ok(Some(File::None));
    };
    let small = buf.len() < CHUNK_SIZE - 1;
    if !small && !output.start_stream() {
        return Ok(None);
    }
    // Writer operations are deliberately not raced against cancellation: OpenDAL
    // does not guarantee they can be dropped safely. Check the consumer afterwards.
    let active_writer = writer.insert(
        storage
            .writer_with(path)
            .content_type(content_type)
            .chunk(CHUNK_SIZE)
            .await?,
    );
    if output.is_closed() {
        return Ok(None);
    }
    loop {
        active_writer.write(buf.clone()).await?;
        if output.is_closed() {
            return Ok(None);
        }
        if small {
            active_writer.close().await?;
            writer.take();
            return Ok(Some(File::Bytes(buf)));
        }
        if !output.send_chunk(buf).await {
            return Ok(None);
        }
        let next = tokio::select! {
            biased;
            _ = output.closed() => return Ok(None),
            chunk = reader.chunk() => chunk?,
        };
        match next {
            Some(chunk) => buf = chunk,
            None => break,
        }
    }
    if output.is_closed() {
        return Ok(None);
    }
    active_writer.close().await?;
    writer.take();
    Ok(None)
}

type DownloadChunk = anyhow::Result<(Bytes, oneshot::Sender<()>)>;

struct DownloadOutput {
    reply: Option<oneshot::Sender<anyhow::Result<File>>>,
    body: Option<mpsc::Sender<DownloadChunk>>,
}

impl DownloadOutput {
    fn is_closed(&self) -> bool {
        match &self.body {
            Some(body) => body.is_closed(),
            None => self.reply.as_ref().is_none_or(oneshot::Sender::is_closed),
        }
    }

    async fn closed(&mut self) {
        if let Some(body) = &self.body {
            body.closed().await;
        } else if let Some(reply) = &mut self.reply {
            reply.closed().await;
        }
    }

    fn start_stream(&mut self) -> bool {
        let (sender, mut receiver) = mpsc::channel::<DownloadChunk>(1);
        let stream = async_stream::stream! {
            while let Some(chunk) = receiver.recv().await {
                match chunk {
                    Ok((bytes, consumed)) => {
                        yield Ok(bytes);
                        let _ = consumed.send(());
                    }
                    Err(err) => { yield Err(err); break; }
                }
            }
        };
        self.body = Some(sender);
        let Some(reply) = self.reply.take() else {
            return false;
        };
        reply.send(Ok(File::Stream(Box::pin(stream)))).is_ok()
    }

    async fn send_chunk(&self, bytes: Bytes) -> bool {
        let (consumed, acknowledgement) = oneshot::channel();
        let Some(body) = &self.body else {
            return false;
        };
        if body.send(Ok((bytes, consumed))).await.is_err() {
            return false;
        }
        // Do not prefetch: resume upstream reads only when the body asks for more.
        acknowledgement.await.is_ok()
    }

    async fn finish(mut self, result: anyhow::Result<Option<File>>) {
        match result {
            Ok(Some(file)) => {
                if let Some(reply) = self.reply.take() {
                    let _ = reply.send(Ok(file));
                }
            }
            Err(err) => {
                if let Some(reply) = self.reply.take() {
                    let _ = reply.send(Err(err));
                } else if let Some(body) = self.body.take() {
                    let _ = body.send(Err(err)).await;
                }
            }
            Ok(None) => {}
        }
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
            index_cache,
            indices,
            storage,
            ..
        } = settings;

        let index_cache = Cache::builder()
            .time_to_live(index_cache.ttl.into())
            .max_capacity(index_cache.max_capacity.into())
            // Account for key, rendered HTML and source mappings, not allocator overhead.
            .weigher(|key: &String, value: &Arc<PackageDetail>| {
                u32::try_from(key.len().saturating_add(value.byte_len())).unwrap_or(u32::MAX)
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
    http_client: Client,
    index_cache: Cache<String, Arc<PackageDetail>>,
    indices: HashMap<String, Vec<IndexUrlTemplate>>,
    indices_list: String,
    storage: Operator,
    download_locks: HashMap<String, Arc<RwLock<()>>>,
}

#[derive(Clone)]
struct IndexUrlTemplate(String);

impl IndexUrlTemplate {
    fn package_url(&self, package: &str) -> Result<Url, url::ParseError> {
        self.0
            .replace("{package}", &encode_segment(package))
            .parse()
    }
}

enum AnchorState {
    Outside,
    Inside(PendingAnchor),
}

struct PendingAnchor {
    id: String,
    source: ArtifactSource,
    fragment: Option<String>,
    attributes: Vec<(String, String)>,
    text: String,
}

struct DetailParser {
    file_links: HashMap<String, ArtifactSource>,
    link_count: usize,
    html: Vec<u8>,
    anchor_start: String,
}

impl DetailParser {
    fn new(base_url: &str, index: &str, package: &str) -> Self {
        Self {
            file_links: HashMap::new(),
            link_count: 0,
            html: Vec::from(b"<!DOCTYPE html>\n<html>\n<body>\n"),
            anchor_start: format!(
                r#"<a href="{}packages/{}/{}/"#,
                base_url,
                encode_segment(index),
                encode_segment(package)
            ),
        }
    }

    fn parse_html(&mut self, package_url: &Url, content: &[u8]) -> anyhow::Result<()> {
        // Context-aware tokenization keeps apparent tags inside script/style and
        // RCDATA elements as text without constructing a DOM.
        let mut emitter = DefaultEmitter::default();
        emitter.naively_switch_states(true);
        let mut state = AnchorState::Outside;
        let mut tokenizer = Tokenizer::new_with_emitter(std::str::from_utf8(content)?, emitter);
        while let Some(token) = tokenizer.next() {
            match token? {
                Token::StartTag(tag) if tag.name.as_ref() == b"a" => {
                    self.finish_anchor(&mut state);
                    state = match Self::prepare_anchor(package_url, tag)? {
                        Some(anchor) => AnchorState::Inside(anchor),
                        None => AnchorState::Outside,
                    };
                }
                Token::EndTag(tag) if tag.name.as_ref() == b"a" => {
                    self.finish_anchor(&mut state);
                }
                Token::String(text) => {
                    if let AnchorState::Inside(anchor) = &mut state {
                        anchor.text.push_str(std::str::from_utf8(&text)?);
                    }
                }
                Token::StartTag(tag) if tag.name.as_ref() == b"noframes" => {
                    // html5gum 0.9.1 naive_next_state spells this tag "noframe".
                    tokenizer.set_state(HtmlState::RawText);
                }
                // HTML parse errors are recoverable diagnostics, not reader errors.
                _ => {}
            }
        }
        self.finish_anchor(&mut state);
        Ok(())
    }

    fn prepare_anchor(
        package_url: &Url,
        tag: StartTag<()>,
    ) -> anyhow::Result<Option<PendingAnchor>> {
        let Some(href) = tag.attributes.get(b"href".as_slice()) else {
            return Ok(None);
        };
        let mut source_url = package_url.join(std::str::from_utf8(href)?)?;
        let id = hex::encode(Sha256::digest(source_url.as_str().as_bytes()));
        let fragment = source_url.fragment().map(str::to_owned);
        source_url.set_fragment(None);
        let file = source_url
            .path_segments()
            .and_then(|mut parts| parts.next_back())
            .unwrap_or("");
        let filename = percent_decode_str(file).decode_utf8()?.into_owned();
        if !valid_filename(&filename) {
            return Ok(None);
        }
        let mut attributes = Vec::new();
        for (name, value) in &tag.attributes {
            if name.as_ref() != b"href" {
                attributes.push((
                    std::str::from_utf8(name)?.to_owned(),
                    std::str::from_utf8(value)?.to_owned(),
                ));
            }
        }
        Ok(Some(PendingAnchor {
            id,
            source: ArtifactSource {
                url: source_url.to_string(),
                filename,
            },
            fragment,
            attributes,
            text: String::new(),
        }))
    }

    fn finish_anchor(&mut self, state: &mut AnchorState) {
        let AnchorState::Inside(anchor) = std::mem::replace(state, AnchorState::Outside) else {
            return;
        };
        self.html.extend(self.anchor_start.as_bytes());
        self.html.extend(anchor.id.as_bytes());
        self.html.push(b'/');
        self.html
            .extend(encode_segment(&anchor.source.filename).as_bytes());
        if let Some(fragment) = anchor.fragment {
            self.html.push(b'#');
            append_html_escaped(&mut self.html, &fragment);
        }
        self.html.push(b'"');
        for (name, value) in anchor.attributes {
            self.html.push(b' ');
            append_html_escaped(&mut self.html, &name);
            self.html.extend(b"=\"");
            append_html_escaped(&mut self.html, &value);
            self.html.push(b'"');
        }
        self.html.push(b'>');
        append_html_escaped(&mut self.html, &anchor.text);
        self.html.extend(b"</a><br/>\n");
        self.file_links.insert(anchor.id, anchor.source);
        self.link_count += 1;
    }

    fn finalize(self) -> Option<PackageDetail> {
        let Self {
            mut html,
            link_count,
            file_links,
            ..
        } = self;
        if link_count > 0 {
            html.extend(b"</body>\n</html>");
            Some(PackageDetail {
                html: html.into(),
                files: file_links,
            })
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

fn append_html_escaped(output: &mut Vec<u8>, text: &str) {
    for character in text.chars() {
        let escaped = match character {
            '&' => "&amp;",
            '<' => "&lt;",
            '>' => "&gt;",
            '"' => "&quot;",
            '\'' => "&#39;",
            _ => {
                let mut bytes = [0; 4];
                output.extend(character.encode_utf8(&mut bytes).as_bytes());
                continue;
            }
        };
        output.extend(escaped.as_bytes());
    }
}

fn encode_segment(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn valid_filename(value: &str) -> bool {
    !value.is_empty() && value != "." && value != ".." && !value.contains(['/', '\\', '\0'])
}

fn artifact_path(index: &str, package: &str, hash: &str, file: &str) -> String {
    format!(
        "{}/{}/{}/{}/{hash}/{}",
        encode_segment(index),
        encode_segment(package),
        &hash[..2],
        &hash[2..4],
        encode_segment(file)
    )
}

struct ArtifactSource {
    url: String,
    filename: String,
}
struct PackageDetail {
    html: Bytes,
    files: HashMap<String, ArtifactSource>,
}
impl PackageDetail {
    fn byte_len(&self) -> usize {
        self.files
            .iter()
            .fold(self.html.len(), |size, (hash, source)| {
                size.saturating_add(hash.len())
                    .saturating_add(source.url.len())
                    .saturating_add(source.filename.len())
            })
    }
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

    fn index_failure_state(sources: Vec<String>) -> (State, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "pipr-index-failure-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let mut settings = Settings::template();
        settings.indices = [("default".into(), sources)].into();
        let mut config = opendal::services::FsConfig::default();
        config.root = Some(root.to_str().unwrap().into());
        settings.storage = crate::storage::Storage::Fs(config);
        (State::try_from(settings).unwrap(), root)
    }

    #[tokio::test]
    async fn upstream_index_failures_return_bad_gateway_without_caching_partial_pages() {
        use axum::{extract::Path, http::StatusCode, response::IntoResponse, routing::get};
        use std::sync::atomic::{AtomicU16, Ordering};

        tokio::time::timeout(Duration::from_secs(10), async {
            let recovering_status = Arc::new(AtomicU16::new(503));
            let app = axum::Router::new().route(
                "/{source}/{package}/",
                get({
                    let recovering_status = recovering_status.clone();
                    move |Path((source, _)): Path<(String, String)>| {
                        let recovering_status = recovering_status.clone();
                        async move {
                            if source == "body-error" {
                                return axum::body::Body::from_stream(futures::stream::iter([
                                    Err::<Bytes, _>(std::io::Error::other(
                                        "upstream body interrupted",
                                    )),
                                ]))
                                .into_response();
                            }
                            if source == "invalid-url" {
                                return "<a href=\"http://[invalid\">invalid URL</a>"
                                    .into_response();
                            }
                            let code = if source == "recover" {
                                recovering_status.load(Ordering::SeqCst)
                            } else {
                                source.parse().unwrap()
                            };
                            let status = StatusCode::from_u16(code).unwrap();
                            (status, format!("<a href='{source}.whl'>{source}.whl</a>"))
                                .into_response()
                        }
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let upstream = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let cases = [
                (vec!["404"], StatusCode::NOT_FOUND),
                (vec!["404", "404"], StatusCode::NOT_FOUND),
                (vec!["404", "200"], StatusCode::OK),
                (vec!["200", "404"], StatusCode::OK),
                (vec!["403"], StatusCode::BAD_GATEWAY),
                (vec!["429"], StatusCode::BAD_GATEWAY),
                (vec!["500"], StatusCode::BAD_GATEWAY),
                (vec!["503"], StatusCode::BAD_GATEWAY),
                (vec!["302"], StatusCode::BAD_GATEWAY),
                (vec!["200", "503"], StatusCode::BAD_GATEWAY),
                (vec!["503", "200"], StatusCode::BAD_GATEWAY),
                (vec!["body-error"], StatusCode::BAD_GATEWAY),
                (vec!["invalid-url"], StatusCode::INTERNAL_SERVER_ERROR),
            ];
            for (sources, expected) in cases {
                let urls = sources
                    .iter()
                    .map(|source| format!("{endpoint}/{source}/{{package}}/"))
                    .collect();
                let (state, root) = index_failure_state(urls);
                let response = crate::get_detail(
                    axum::extract::State(state.clone()),
                    Path(("default".into(), "pkg".into())),
                )
                .await;
                assert_eq!(response.status(), expected, "sources: {sources:?}");
                if expected != StatusCode::OK {
                    assert!(
                        state
                            .0
                            .read()
                            .await
                            .index_cache
                            .get("default/pkg")
                            .await
                            .is_none()
                    );
                }
                let response = crate::get_file(
                    axum::extract::State(state.clone()),
                    Path((
                        "default".into(),
                        "pkg".into(),
                        "a".repeat(64),
                        "file.whl".into(),
                    )),
                )
                .await;
                let expected_file = if expected == StatusCode::OK {
                    StatusCode::NOT_FOUND
                } else {
                    expected
                };
                assert_eq!(
                    response.status(),
                    expected_file,
                    "cold artifact sources: {sources:?}"
                );
                drop(state);
                std::fs::remove_dir_all(root).unwrap();
            }

            let (state, root) = index_failure_state(vec![
                format!("{endpoint}/200/{{package}}/"),
                format!("{endpoint}/recover/{{package}}/"),
            ]);
            assert!(
                state
                    .get_detail("default".into(), "pkg".into())
                    .await
                    .is_err()
            );
            assert!(
                state
                    .0
                    .read()
                    .await
                    .index_cache
                    .get("default/pkg")
                    .await
                    .is_none()
            );
            recovering_status.store(200, Ordering::SeqCst);
            let detail = state
                .package_detail("default", "pkg", None)
                .await
                .unwrap()
                .unwrap();
            let mut filenames: Vec<_> = detail
                .files
                .values()
                .map(|file| file.filename.as_str())
                .collect();
            filenames.sort();
            assert_eq!(filenames, ["200.whl", "recover.whl"]);
            assert!(
                state
                    .0
                    .read()
                    .await
                    .index_cache
                    .get("default/pkg")
                    .await
                    .is_some()
            );
            drop(state);
            upstream.abort();
            std::fs::remove_dir_all(root).unwrap();
        })
        .await
        .expect("upstream index failure matrix must complete");
    }

    #[tokio::test]
    async fn upstream_connection_failure_is_bad_gateway_for_index_and_artifact() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let (state, root) = index_failure_state(vec![format!("{endpoint}/{{package}}/")]);
        let index = crate::get_detail(
            axum::extract::State(state.clone()),
            axum::extract::Path(("default".into(), "pkg".into())),
        )
        .await;
        assert_eq!(index.status(), axum::http::StatusCode::BAD_GATEWAY);
        let file = crate::get_file(
            axum::extract::State(state.clone()),
            axum::extract::Path((
                "default".into(),
                "pkg".into(),
                "a".repeat(64),
                "file.whl".into(),
            )),
        )
        .await;
        assert_eq!(file.status(), axum::http::StatusCode::BAD_GATEWAY);
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    }

    fn page(html: &'static [u8]) -> Arc<PackageDetail> {
        Arc::new(PackageDetail {
            html: Bytes::from_static(html),
            files: HashMap::new(),
        })
    }

    #[tokio::test]
    async fn cold_replica_recovers_context_links_and_cached_artifacts_survive_upstream_loss() {
        tokio::time::timeout(Duration::from_secs(15), async {
            use axum::{routing::get, extract::Path, http::StatusCode, response::IntoResponse};
            let index_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let app = axum::Router::new()
                .route("/simple/{package}/", get({
                    let count = index_requests.clone();
                    move |Path(package): Path<String>| {
                        let count = count.clone();
                        async move {
                            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            if package != "pkg" { return StatusCode::NOT_FOUND.into_response(); }
                            "<a href='../../files/pkg%2Bcpu.whl?download=1&amp;x=2#sha256=abc'>pkg+cpu.whl</a>".into_response()
                        }
                    }
                }))
                .route("/files/{file}", get(|Path(file): Path<String>, uri: axum::http::Uri| async move {
                    if uri.query() != Some("download=1&x=2") { return StatusCode::BAD_REQUEST.into_response(); }
                    match file.as_str() {
                        "pkg+cpu.whl" => "wheel bytes".into_response(),
                        "pkg+cpu.whl.metadata" => "metadata bytes".into_response(),
                        _ => StatusCode::NOT_FOUND.into_response(),
                    }
                }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let upstream = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let root = std::env::temp_dir().join(format!("pipr-context-{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
            let make_state = |capacity: &str| {
                let mut settings = Settings::template();
                settings.indices = [("default".into(), vec![format!("{endpoint}/simple/{{package}}/")])].into();
                // Even an entry too large for cache admission must work for the current request.
                settings.index_cache = toml::from_str(&format!("max_capacity = '{capacity}'")).unwrap();
                let mut config = opendal::services::FsConfig::default();
                config.root = Some(root.to_str().unwrap().into());
                settings.storage = crate::storage::Storage::Fs(config);
                State::try_from(settings).unwrap()
            };
            let first = make_state("1 B");
            let html = first.get_detail("default".into(), "pkg".into()).await.unwrap().unwrap();
            let html = std::str::from_utf8(&html).unwrap();
            let href = html.split("href=\"").nth(1).unwrap().split('"').next().unwrap();
            assert!(href.starts_with("/packages/default/pkg/"));
            assert!(href.ends_with("/pkg%2Bcpu%2Ewhl#sha256=abc"));
            let path = href.split('#').next().unwrap().to_owned();
            let id = path.split('/').nth(4).unwrap().to_owned();
            drop(first);
            let second = make_state("1 B");
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let proxy_endpoint = format!("http://{}", listener.local_addr().unwrap());
            let router = crate::get_router("/").with_state(second.clone());
            let proxy = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
            let client = reqwest::Client::new();
            let response = client.get(format!("{proxy_endpoint}{path}")).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.bytes().await.unwrap().as_ref(), b"wheel bytes");
            assert_eq!(index_requests.load(std::sync::atomic::Ordering::SeqCst), 2);
            // A still-live page from another replica can predate this artifact.
            let stale = make_state("1 MiB");
            let stale_detail = Arc::new(PackageDetail {
                html: Bytes::from_static(b"old page"),
                files: [("f".repeat(64), ArtifactSource { url: format!("{endpoint}/files/old.whl"), filename: "old.whl".into() })].into(),
            });
            stale.0.read().await.index_cache.insert("default/pkg".into(), stale_detail).await;
            let File::Bytes(bytes) = stale.get_file("default".into(), "pkg".into(), id.clone(), "pkg+cpu.whl.metadata".into()).await.unwrap() else { panic!("stale page must refresh for an unknown artifact ID"); };
            assert_eq!(bytes.as_ref(), b"metadata bytes");
            assert_eq!(index_requests.load(std::sync::atomic::Ordering::SeqCst), 3);
            drop(stale);
            let metadata = client.get(format!("{proxy_endpoint}{path}.metadata")).send().await.unwrap();
            assert_eq!(metadata.status(), StatusCode::OK);
            assert_eq!(metadata.bytes().await.unwrap().as_ref(), b"metadata bytes");
            for wrong_path in [
                format!("/packages/default/pkg/{id}/wrong.whl"),
                format!("/packages/default/other/{id}/pkg%2Bcpu%2Ewhl"),
                format!("/packages/unknown/pkg/{id}/pkg%2Bcpu%2Ewhl"),
                format!("/packages/default/pkg/{}/pkg%2Bcpu%2Ewhl", "0".repeat(64)),
                format!("/packages/{id}/pkg%2Bcpu%2Ewhl"),
            ] {
                assert_eq!(client.get(format!("{proxy_endpoint}{wrong_path}")).send().await.unwrap().status(), StatusCode::NOT_FOUND, "{wrong_path}");
            }
            upstream.abort();
            let _ = upstream.await;
            let third = make_state("1 B");
            let File::Stream(stream) = third.get_file("default".into(), "pkg".into(), id, "pkg+cpu.whl".into()).await.unwrap() else { panic!("fresh replica must serve storage hit"); };
            assert_eq!(stream.try_collect::<Vec<_>>().await.unwrap().concat(), b"wheel bytes");
            proxy.abort();
            let _ = proxy.await;
            drop(second);
            drop(third);
            std::fs::remove_dir_all(root).unwrap();
        }).await.expect("context recovery must complete");
    }

    #[tokio::test]
    async fn slow_index_fetch_does_not_block_unrelated_requests() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let body_started = Arc::new(tokio::sync::Notify::new());
            let release_body = Arc::new(tokio::sync::Notify::new());
            let app = axum::Router::new().route(
                "/{package}/",
                axum::routing::get({
                    let body_started = body_started.clone();
                    let release_body = release_body.clone();
                    move |axum::extract::Path(package): axum::extract::Path<String>| {
                        let body_started = body_started.clone();
                        let release_body = release_body.clone();
                        async move {
                            axum::body::Body::from_stream(async_stream::stream! {
                                if package == "slow" {
                                    body_started.notify_one();
                                    release_body.notified().await;
                                }
                                yield Ok::<_, std::io::Error>(Bytes::from(format!(
                                    "<a href=\"{package}.whl\">{package}.whl</a>",
                                )));
                            })
                        }
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let upstream = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let root = std::env::temp_dir().join(format!(
                "pipr-index-lock-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
            ));
            std::fs::create_dir_all(&root).unwrap();
            let mut settings = Settings::template();
            settings.indices =
                [("default".into(), vec![format!("{endpoint}/{{package}}/")])].into();
            let mut config = opendal::services::FsConfig::default();
            config.root = Some(root.join("artifacts").to_str().unwrap().into());
            settings.storage = crate::storage::Storage::Fs(config);
            let state = State::try_from(settings).unwrap();
            let slow = tokio::spawn({
                let state = state.clone();
                async move { state.get_detail("default".into(), "slow".into()).await }
            });
            body_started.notified().await;
            let indices = tokio::time::timeout(Duration::from_secs(1), state.get_indices_list())
                .await
                .expect("slow upstream body must not block unrelated state reads");
            assert!(indices.contains("default"));
            let fast = tokio::time::timeout(
                Duration::from_secs(1),
                state.get_detail("default".into(), "fast".into()),
            )
            .await
            .expect("slow upstream body must not hold the database write transaction")
            .unwrap()
            .unwrap();
            assert!(
                std::str::from_utf8(&fast)
                    .unwrap()
                    .contains("/fast%2Ewhl\"")
            );
            release_body.notify_one();
            let slow = slow.await.unwrap().unwrap().unwrap();
            assert!(
                std::str::from_utf8(&slow)
                    .unwrap()
                    .contains("/slow%2Ewhl\"")
            );
            let mut urls = Vec::new();
            for package in ["fast", "slow"] {
                let detail = state
                    .package_detail("default", package, None)
                    .await
                    .unwrap()
                    .unwrap();
                urls.extend(detail.files.values().map(|source| source.url.clone()));
            }
            urls.sort();
            assert_eq!(
                urls,
                [
                    format!("{endpoint}/fast/fast.whl"),
                    format!("{endpoint}/slow/slow.whl")
                ]
            );
            upstream.abort();
            drop(state);
            std::fs::remove_dir_all(root).unwrap();
        })
        .await
        .expect("index concurrency regression must finish");
    }

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
        settings.index_cache = toml::from_str("max_capacity = '12 B'").unwrap();
        let mut config = opendal::services::FsConfig::default();
        config.root = Some(root.join("artifacts").to_str().unwrap().into());
        settings.storage = crate::storage::Storage::Fs(config);
        let state = State::try_from(settings).unwrap();
        let cache = state.0.read().await.index_cache.clone();

        // Four UTF-8 key bytes and eight payload bytes exactly fill the budget.
        cache.insert("é/x".into(), page(b"12345678")).await;
        cache.run_pending_tasks().await;
        assert_eq!(cache.weighted_size(), 12);
        assert_eq!(cache.get("é/x").await.unwrap().html.as_ref(), b"12345678");
        cache.invalidate_all();
        cache.run_pending_tasks().await;

        for (key, value) in [("key", page(b"1234567890")), ("long-cache-key", page(b""))] {
            cache.insert(key.into(), value).await;
            cache.run_pending_tasks().await;
            assert!(
                cache.get(key).await.is_none(),
                "oversized entry must not remain: {key}"
            );
        }

        let mappings_only = Arc::new(PackageDetail {
            html: Bytes::new(),
            files: [(
                "abc".into(),
                ArtifactSource {
                    url: "1234".into(),
                    filename: "12345".into(),
                },
            )]
            .into(),
        });
        cache.insert("x".into(), mappings_only).await;
        cache.run_pending_tasks().await;
        assert!(
            cache.get("x").await.is_none(),
            "mapping IDs, URLs and filenames must share the byte budget"
        );

        // Each entry fits separately, but the pair must not remain together.
        cache.insert("a".into(), page(b"12345678")).await;
        cache.run_pending_tasks().await;
        assert!(cache.get("a").await.is_some());
        cache.insert("b".into(), page(b"12345678")).await;
        cache.run_pending_tasks().await;
        assert_eq!(cache.entry_count(), 1);
        assert_eq!(cache.weighted_size(), 9);

        drop(cache);
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn html5_links_preserve_decoded_attributes_text_and_source_urls() {
        let base = Url::parse("https://index.example/simple/pkg/").unwrap();
        let mut parser = DetailParser::new("/proxy/", "default", "pkg");
        parser
            .parse_html(
                &base,
                br#"<!doctype html><HTML><HEAD>
            <meta charset=utf-8><TITLE>Packages</TITLE></HEAD><BODY>
            <A HREF=first.whl DATA-REQUIRES-PYTHON='&gt;=3.12' DATA-YANKED
               DATA-CORE-METADATA='sha256=abc' DATA-DIST-INFO-METADATA='sha256=abc'
               TITLE='a &quot;quote&quot; &amp; &lt;tag&gt;'>first<span>.whl</span></A><br>
            <a>not a file</a>
            <a href='second.whl?x=1&amp;y=2#sha256=ab&amp;extra=cd'>second&#46;whl &amp; &lt;tag&gt;
            "#,
            )
            .unwrap();
        let detail = parser.finalize().unwrap();
        assert_eq!(detail.files.len(), 2);
        let html = std::str::from_utf8(&detail.html).unwrap();
        assert!(html.contains("data-requires-python=\"&gt;=3.12\""));
        assert!(html.contains("data-yanked=\"\""));
        assert!(html.contains("data-core-metadata=\"sha256=abc\""));
        assert!(html.contains("data-dist-info-metadata=\"sha256=abc\""));
        assert!(html.contains("title=\"a &quot;quote&quot; &amp; &lt;tag&gt;\""));
        assert!(html.contains(">first.whl</a>"));
        assert!(html.contains(">second.whl &amp; &lt;tag&gt;"));
        assert!(html.contains("/second%2Ewhl#sha256=ab&amp;extra=cd\""));
        assert!(!html.contains("not a file"));
        let mut urls: Vec<_> = detail
            .files
            .values()
            .map(|source| source.url.as_str())
            .collect();
        urls.sort();
        assert_eq!(
            urls,
            [
                "https://index.example/simple/pkg/first.whl",
                "https://index.example/simple/pkg/second.whl?x=1&y=2",
            ]
        );
    }

    #[test]
    fn tokenizer_ignores_apparent_links_in_raw_text_and_rcdata() {
        let base = Url::parse("https://index.example/simple/pkg/").unwrap();
        for tag in [
            "script", "style", "title", "textarea", "iframe", "xmp", "noembed", "noframes",
        ] {
            let mut parser = DetailParser::new("/", "default", "pkg");
            let input = format!(
                "<{tag}><a href=fake.whl>fake.whl</a></{tag}><a href=real.whl>real.whl</a>"
            );
            parser.parse_html(&base, input.as_bytes()).unwrap();
            let detail = parser.finalize().unwrap();
            assert_eq!(detail.files.len(), 1, "{tag}");
            let source = detail.files.values().next().unwrap();
            assert_eq!(
                source.url, "https://index.example/simple/pkg/real.whl",
                "{tag}"
            );
            let html = std::str::from_utf8(&detail.html).unwrap();
            assert!(html.contains(">real.whl</a>"), "{tag}");
            assert!(!html.contains("fake.whl"), "{tag}");
        }
    }

    #[test]
    fn tokenizer_closes_adjacent_and_unterminated_anchors() {
        let base = Url::parse("https://index.example/simple/pkg/").unwrap();
        let mut parser = DetailParser::new("/", "default", "pkg");
        parser.parse_html(&base,
            b"<a href=first.whl>first<span>.whl</span><a href=second.whl>second.whl<a>not a file</a><a href=last.whl>last.whl"
        ).unwrap();
        let detail = parser.finalize().unwrap();
        assert_eq!(detail.files.len(), 3);
        let html = std::str::from_utf8(&detail.html).unwrap();
        for filename in ["first.whl", "second.whl", "last.whl"] {
            assert!(html.contains(&format!(">{filename}</a><br/>")), "{html}");
        }
        assert_eq!(html.matches("</a>").count(), 3);
        assert!(!html.contains("not a file"));
    }

    #[test]
    fn ignored_anchor_targets_end_the_previous_link_without_leaking_text() {
        let base = Url::parse("https://index.example/simple/pkg/").unwrap();
        let mut parser = DetailParser::new("/", "default", "pkg");
        parser.parse_html(&base, b"outside</a><a href=first.whl>first.whl<a href=directory/>ignored</a>outside<a href=last.whl>last.whl</a>outside").unwrap();
        let detail = parser.finalize().unwrap();
        let html = std::str::from_utf8(&detail.html).unwrap();
        assert_eq!(detail.files.len(), 2);
        assert!(html.contains(">first.whl</a><br/>"));
        assert!(html.contains(">last.whl</a><br/>"));
        assert_eq!(html.matches("</a>").count(), 2);
        assert!(!html.contains("outside"));
        assert!(!html.contains("ignored"));
    }

    #[test]
    fn rewritten_file_links_preserve_fragments_and_resolve_sources() {
        let base = Url::parse("https://index.example.test/simple/pkg/").unwrap();
        let mut parser = DetailParser::new("/proxy/", "default", "pkg");
        parser.parse_html(&base, b"<a href='../../files/pkg.whl?download=1&amp;x=2#sha256=abc'>pkg.whl</a><a href='plain.zip'>plain.zip</a>").unwrap();
        let detail = parser.finalize().unwrap();
        let html = std::str::from_utf8(&detail.html).unwrap();
        assert!(html.contains("/proxy/packages/default/pkg/"));
        assert!(html.contains("/pkg%2Ewhl#sha256=abc\""));
        assert!(html.contains("/plain%2Ezip\""));
        let source = detail
            .files
            .values()
            .find(|source| source.filename == "pkg.whl")
            .unwrap();
        assert_eq!(
            source.url,
            "https://index.example.test/files/pkg.whl?download=1&x=2"
        );
        let mut other = DetailParser::new("/", "default", "pkg");
        other
            .parse_html(
                &Url::parse("https://other.example/simple/pkg/").unwrap(),
                b"<a href='plain.zip'>plain.zip</a>",
            )
            .unwrap();
        let other = other.finalize().unwrap();
        let first_id = detail
            .files
            .iter()
            .find(|(_, source)| source.filename == "plain.zip")
            .unwrap()
            .0;
        assert!(
            !other.files.contains_key(first_id),
            "relative links on different hosts must not collide"
        );
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
        let mut config = opendal::services::FsConfig::default();
        config.root = Some(root.join("artifacts").to_str().unwrap().into());
        settings.storage = crate::storage::Storage::Fs(config);
        let state = State::try_from(settings).unwrap();
        let storage = state.0.read().await.storage.clone();
        let nonhex = "g".repeat(64);
        storage
            .write(
                format!("default/package/gg/gg/{nonhex}/package%2Ewhl").as_str(),
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
                        .get_file(
                            "default".into(),
                            "package".into(),
                            hash.clone(),
                            "package.whl".into()
                        )
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
                format!("default/package/ab/cd/{hash}/package%2Ewhl").as_str(),
                "valid artifact",
            )
            .await
            .unwrap();
        let File::Stream(stream) = state
            .get_file(
                "default".into(),
                "package".into(),
                hash.into(),
                "package.whl".into(),
            )
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

    async fn cancellation_fixture(
        prefix_size: usize,
    ) -> (
        State,
        std::path::PathBuf,
        reqwest::Response,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/file", listener.local_addr().unwrap());
        let upstream = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(socket.read_u8().await.unwrap());
                assert!(request.len() < 8192);
            }
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        CHUNK_SIZE * 4
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(&vec![42; prefix_size]).await.unwrap();
            // No further body is sent. Cancellation must close the upstream socket.
            assert_eq!(socket.read(&mut [0]).await.unwrap(), 0);
        });
        let root = std::env::temp_dir().join(format!(
            "pipr-cancel-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut settings = Settings::template();
        let mut config = opendal::services::FsConfig::default();
        config.root = Some(root.to_str().unwrap().into());
        settings.storage = crate::storage::Storage::Fs(config);
        let state = State::try_from(settings).unwrap();
        let response = Client::new().get(url).send().await.unwrap();
        (state, root, response, upstream)
    }

    #[tokio::test]
    async fn dropped_initial_reply_or_unpolled_body_aborts_and_releases_registration() {
        tokio::time::timeout(Duration::from_secs(10), async {
            for prefix_size in [0, CHUNK_SIZE + 1] {
                let (state, root, response, upstream) = cancellation_fixture(prefix_size).await;
                let (reply, result) = oneshot::channel();
                let mut download = Box::pin(download_response(
                    state.0.clone(),
                    "test-id".into(),
                    "artifact.whl".into(),
                    "application/octet-stream".into(),
                    response,
                    reply,
                ));
                assert!(futures::poll!(&mut download).is_pending());
                if prefix_size == 0 {
                    assert!(state.0.read().await.download_locks.contains_key("test-id"));
                    drop(result);
                    download.await;
                } else {
                    let owner = tokio::spawn(download);
                    let File::Stream(stream) = result.await.unwrap().unwrap() else {
                        panic!("large body must stream");
                    };
                    // Activate the writer without polling the body itself.
                    while std::fs::read_dir(root.join(".tmp"))
                        .unwrap()
                        .next()
                        .is_none()
                    {
                        tokio::task::yield_now().await;
                    }
                    drop(stream);
                    owner.await.unwrap();
                }
                upstream.await.unwrap();
                assert!(state.0.read().await.download_locks.is_empty());
                assert!(!root.join("artifact.whl").exists());
                assert_eq!(std::fs::read_dir(root.join(".tmp")).unwrap().count(), 0);
                drop(state);
                std::fs::remove_dir_all(root).unwrap();
            }
        })
        .await
        .expect("cancelled owner must stop upstream and finish cleanup");
    }

    #[tokio::test]
    async fn cancelled_waiter_preserves_active_download_registration() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (state, root, response, upstream) = cancellation_fixture(0).await;
            let lock = Arc::new(RwLock::new(()));
            let active = lock.clone().write_owned().await;
            state
                .0
                .write()
                .await
                .download_locks
                .insert("test-id".into(), lock.clone());
            let (reply, result) = oneshot::channel();
            let mut download = Box::pin(download_response(
                state.0.clone(),
                "test-id".into(),
                "artifact.whl".into(),
                "application/octet-stream".into(),
                response,
                reply,
            ));
            assert!(futures::poll!(&mut download).is_pending());
            drop(result);
            assert!(futures::poll!(&mut download).is_pending());
            upstream.await.unwrap();
            assert!(Arc::ptr_eq(
                state.0.read().await.download_locks.get("test-id").unwrap(),
                &lock
            ));
            drop(active);
            download.await;
            assert!(state.0.read().await.download_locks.is_empty());
            drop(state);
            std::fs::remove_dir_all(root).unwrap();
        })
        .await
        .expect("cancelled waiter must retain registration until safe cleanup");
    }

    async fn failed_download_fixture(
        body: Bytes,
        truncated: bool,
    ) -> (State, std::path::PathBuf, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/package%2Ewhl", listener.local_addr().unwrap());
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
        let mut config = opendal::services::FsConfig::default();
        config.root = Some(root.join("artifacts").to_str().unwrap().into());
        settings.storage = crate::storage::Storage::Fs(config);
        let state = State::try_from(settings).unwrap();
        let detail = Arc::new(PackageDetail {
            html: Bytes::new(),
            files: [(
                "abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab".into(),
                ArtifactSource {
                    url,
                    filename: "package.whl".into(),
                },
            )]
            .into(),
        });
        state
            .0
            .read()
            .await
            .index_cache
            .insert("default/package".into(), detail)
            .await;
        (state, root, upstream)
    }

    #[tokio::test]
    async fn failed_writer_close_aborts_cleans_up_and_allows_retry() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let body = Bytes::from(vec![42; CHUNK_SIZE * 2]);
            let (state, root, upstream) = failed_download_fixture(body.clone(), false).await;
            let File::Stream(mut stream) = state
                .get_file("default".into(), "package".into(), "abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab".into(), "package.whl".into())
                .await
                .unwrap()
            else {
                panic!("large download must stream")
            };
            // Start the writer, then prevent its final atomic rename from succeeding.
            assert!(!stream.try_next().await.unwrap().unwrap().is_empty());
            let object = root.join("artifacts/default/package/ab/cd/abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab/package%2Ewhl");
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
                .get_file("default".into(), "package".into(), "abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab".into(), "package.whl".into())
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
                    .get_file("default".into(), "package".into(), "abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab".into(), "package.whl".into())
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
                        .join("artifacts/default/package/ab/cd/abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab/package%2Ewhl")
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

        let mut waiting_download = Box::pin(state.get_file(
            "default".into(),
            "package".into(),
            hash.into(),
            "package.whl".into(),
        ));
        // Polling to Pending puts the request at the held file lock without sleeps.
        assert!(futures::poll!(&mut waiting_download).is_pending());
        let storage = state.0.read().await.storage.clone();
        storage
            .write(
                "default/package/ab/cd/abcd0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab/package%2Ewhl",
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
        let path = "default/package/ab/cd/identifier/package%2Ewhl";
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
