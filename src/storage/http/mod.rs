use std::time::Duration;

use ::http::{Request, Response};
use futures::StreamExt;
use opendal::{Buffer, Error, ErrorKind, HttpBody, HttpTransport, HttpTransporter};
use opendal_http_transport_reqwest::ReqwestTransport;

use crate::settings::StorageHttp;

pub(super) fn transport(settings: StorageHttp) -> anyhow::Result<HttpTransporter> {
    let client = reqwest::Client::builder()
        .connect_timeout(settings.connect_timeout)
        .build()?;
    Ok(HttpTransporter::new(StorageTransport {
        inner: ReqwestTransport::new(client),
        settings,
    }))
}

struct StorageTransport {
    inner: ReqwestTransport,
    settings: StorageHttp,
}

fn timeout_error(operation: &'static str, duration: Duration) -> Error {
    Error::new(ErrorKind::Unexpected, "storage HTTP timeout reached")
        .with_operation(operation)
        .with_context("timeout", duration.as_secs_f64().to_string())
        .set_temporary()
}

impl HttpTransport for StorageTransport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        // Cancel only the transport request: OpenDAL's writer receives an error
        // normally and can restore its state before our caller attempts abort.
        // Request bodies are already buffered, so upload time does not include
        // waiting for the upstream server or the package client to produce data.
        let response = tokio::time::timeout(
            self.settings.response_header_timeout,
            self.inner.fetch(request),
        )
        .await
        .map_err(|_| {
            timeout_error(
                "storage_http::response_headers",
                self.settings.response_header_timeout,
            )
        })??;
        let read_timeout = self.settings.read_timeout;
        Ok(response.map(|body| {
            body.map_inner(|mut stream| {
                Box::new(Box::pin(async_stream::stream! {
                    // Start the timer only when another chunk is requested.
                    // A slow consumer can leave this generator suspended at
                    // yield for any duration without using the read budget.
                    loop {
                        match tokio::time::timeout(read_timeout, stream.next()).await {
                            Ok(Some(Ok(buffer))) => yield Ok(buffer),
                            Ok(Some(Err(error))) => {
                                yield Err(error);
                                break;
                            }
                            Ok(None) => break,
                            Err(_) => {
                                yield Err(timeout_error("storage_http::read", read_timeout));
                                break;
                            }
                        }
                    }
                }))
            })
        }))
    }
}

#[cfg(test)]
mod tests;
