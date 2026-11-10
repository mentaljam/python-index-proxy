use std::{pin::Pin, time::Duration};

use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use opendal::Operator;
use tokio::sync::oneshot;
use url::Url;
use uv_normalize::PackageName;

use crate::{
    paths::{artifact_path, valid_filename, valid_index_name},
    state::State,
};

mod download;

use download::{download_response, stored_file};

impl State {
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
        let Ok(package) = PackageName::from_owned(package) else {
            return Ok(File::None);
        };
        let package = package.to_string();
        if !valid_index_name(&index)
            || !valid_filename(&file)
            || !self.0.read().await.indices.contains_key(&index)
        {
            return Ok(File::None);
        }
        tracing::debug!("processing request for {file}");
        let object_path = artifact_path(&index, &package, &hash, &file);

        let storage = self.0.read().await.storage.clone();
        if let Some(file) = stored_file(&storage, &object_path).await? {
            return Ok(file);
        }
        let (reply, result) = oneshot::channel();
        let state = self.clone();
        let fetch = {
            let state = self.clone();
            async move {
                state
                    .artifact_response(&index, &package, &hash, &file)
                    .await
            }
        };
        tokio::spawn(async move {
            download_response(state.0, object_path, fetch, reply).await;
        });
        result.await?
    }

    async fn artifact_response(
        &self,
        index: &str,
        package: &str,
        hash: &str,
        file: &str,
    ) -> anyhow::Result<Option<reqwest::Response>> {
        let (http_client, upstream) = {
            let data = self.0.read().await;
            (data.http_client.clone(), data.upstream)
        };
        let Some(detail) = self.package_detail(index, package, Some(hash)).await? else {
            return Ok(None);
        };
        let Some(source) = detail.files.get(hash) else {
            return Ok(None);
        };
        let mut url = Url::parse(&source.url)?;
        if file != source.filename {
            if file != format!("{}.metadata", source.filename) {
                return Ok(None);
            }
            url.set_path(&format!("{}.metadata", url.path()));
        }

        let response = crate::upstream::send(&http_client, url.clone(), upstream).await?;
        if !response.status().is_success() {
            anyhow::bail!("Failed to get {url}")
        }

        Ok(Some(response))
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
        if request.method() == ::http::Method::GET
            && request
                .header()
                .keys()
                .all(|name| name == ::http::header::HOST)
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

#[cfg(test)]
mod tests;
