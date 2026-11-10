use std::future::Future;

use bytes::{Bytes, BytesMut};
use opendal::Operator;
use tokio::sync::{mpsc, oneshot};

use crate::{coordination::ResourceLimit, settings::Upstream, state::StateData};

use super::{File, cached_file};

pub(super) const CHUNK_SIZE: usize = 5 * 1024 * 1024;
const CHUNK_BUFFER_SIZE: usize = CHUNK_SIZE + 512 * 1024;

// The task owns the writer until close/abort completes. Dropping an HTTP body only
// closes its receiver; it never cancels an in-flight OpenDAL write operation.
pub(super) async fn download_response(
    state: &StateData,
    path: String,
    fetch: impl Future<Output = anyhow::Result<Option<reqwest::Response>>> + Send,
    reply: oneshot::Sender<anyhow::Result<File>>,
) {
    let mut output = DownloadOutput {
        reply: Some(reply),
        body: None,
    };
    let storage = &state.storage;
    let lease = state.download_locks.lease(path.clone());
    let file_guard = tokio::select! {
        biased;
        _ = output.closed() => return,
        guard = lease.entry.lock() => guard,
    };
    let mut writer = None;
    let mut permit = None;
    let result = async {
        // Same-key waiters can use a completed object even while other fills
        // exhaust the global budget. Only a new fill needs a permit.
        if let Some(file) = stored_file(storage, &path).await? {
            return Ok(Some(file));
        }
        permit = Some(
            state
                .artifact_permits
                .clone()
                .try_acquire_owned()
                .map_err(|_| ResourceLimit)?,
        );
        fill_artifact(
            storage,
            &path,
            fetch,
            &mut output,
            &mut writer,
            state.upstream,
        )
        .await
    }
    .await;
    if let Some(mut writer) = writer
        && let Err(err) = writer.abort().await
    {
        tracing::error!("Failed to abort artifact upload: {err}");
    }
    drop(permit);
    drop(file_guard);
    drop(lease);
    // Publish errors only after cleanup: consumers need not poll after an error.
    output.finish(result).await;
}

pub(super) async fn stored_file(storage: &Operator, path: &str) -> anyhow::Result<Option<File>> {
    match storage.stat(path).await {
        Ok(_) => Ok(Some(cached_file(storage, path).await?)),
        Err(err) if err.kind() == opendal::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

async fn fill_artifact(
    storage: &Operator,
    path: &str,
    fetch: impl Future<Output = anyhow::Result<Option<reqwest::Response>>>,
    output: &mut DownloadOutput,
    writer: &mut Option<opendal::Writer>,
    upstream: Upstream,
) -> anyhow::Result<Option<File>> {
    let response = tokio::select! {
        biased;
        _ = output.closed() => return Ok(None),
        response = fetch => response?,
    };
    let Some(response) = response else {
        return Ok(Some(File::None));
    };
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_owned();
    transfer_response(
        response,
        storage,
        path,
        &content_type,
        output,
        writer,
        upstream,
    )
    .await
}

async fn transfer_response(
    response: reqwest::Response,
    storage: &Operator,
    path: &str,
    content_type: &str,
    output: &mut DownloadOutput,
    writer: &mut Option<opendal::Writer>,
    upstream: Upstream,
) -> anyhow::Result<Option<File>> {
    let mut reader = BufferedResponseReader(response, upstream);
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

struct BufferedResponseReader(reqwest::Response, Upstream);

impl BufferedResponseReader {
    async fn chunk(&mut self) -> anyhow::Result<Option<Bytes>> {
        let mut buf = BytesMut::with_capacity(CHUNK_BUFFER_SIZE);
        while let Some(chunk) = crate::upstream::chunk(&mut self.0, self.1).await? {
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

#[cfg(test)]
mod tests;
