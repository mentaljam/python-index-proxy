use std::{pin::Pin, time::Duration};

use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use opendal::Operator;
use tokio::sync::oneshot;
use uv_normalize::PackageName;

use crate::{
    paths::{artifact_path, valid_filename},
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
        if !valid_filename(&file) || !self.0.indices.contains_key(&index) {
            return Ok(File::None);
        }
        crate::logging::package(&index, package.as_str());
        let object_path = artifact_path(&index, package.as_str(), &hash, &file);

        crate::logging::cache("artifact", "lookup");
        if let Some(file) = stored_file(&self.0.storage, &object_path).await? {
            crate::logging::cache("artifact", "hit");
            return Ok(file);
        }
        crate::logging::cache("artifact", "miss");
        let (reply, result) = oneshot::channel();
        let state = self.clone();
        self.0
            .shutdown
            .spawn(crate::logging::inherit_context(async move {
                let fetch = state.artifact_response(&index, &package, &hash, &file);
                download_response(&state.0, object_path, fetch, reply).await;
            }));
        result.await?
    }

    async fn artifact_response(
        &self,
        index: &str,
        package: &PackageName,
        hash: &str,
        file: &str,
    ) -> anyhow::Result<Option<reqwest::Response>> {
        let Some(detail) = self.package_detail(index, package, Some(hash)).await? else {
            return Ok(None);
        };
        let Some(source) = detail.files.get(hash) else {
            return Ok(None);
        };
        let mut url = source.url.clone();
        if file != source.filename {
            if file != format!("{}.metadata", source.filename) {
                return Ok(None);
            }
            url.set_path(&format!("{}.metadata", url.path()));
        }

        let response =
            crate::upstream::send(&self.0.http_client, url.clone(), self.0.upstream).await?;
        if !response.status().is_success() {
            anyhow::bail!(
                "Upstream artifact returned HTTP {}",
                response.status().as_u16()
            )
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
