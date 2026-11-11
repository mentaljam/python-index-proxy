use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use tower_http::compression::CompressionLayer;

use crate::{artifact::File, index::UpstreamIndexError, state};

type AppState = State<state::State>;

pub(crate) fn get_router(path_prefix: &str) -> Router<state::State> {
    let router = Router::new()
        .route("/", get(list_indices))
        .route("/{index}/", get(index_info))
        .route("/{index}/{package}/", get(get_detail))
        .layer(CompressionLayer::new())
        .route("/packages/{index}/{package}/{hash}/{file}", get(get_file));

    if path_prefix == "/" {
        router
    } else {
        Router::new().nest(path_prefix, router)
    }
}

async fn list_indices(State(state): AppState) -> Response {
    let body = state.get_indices_list();
    Response::builder()
        .header(header::CONTENT_TYPE, "text/html")
        .body(body.into())
        .unwrap()
}

async fn index_info(State(state): AppState, Path(index): Path<String>) -> Response {
    match state.get_index_info(&index) {
        Some(page) => ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], page).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn get_detail(
    State(state): AppState,
    Path((index, package)): Path<(String, String)>,
) -> Response {
    match state.get_detail(index, package).await {
        Ok(Some(index)) => ([(header::CONTENT_TYPE, "text/html")], index).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            tracing::error!("{err:?}");
            error_response(&err)
        }
    }
}

async fn get_file(
    State(state): AppState,
    Path((index, package, hash, file)): Path<(String, String, String, String)>,
) -> Response {
    match state.get_file(index, package, hash, file).await {
        Ok(File::PresignedUrl(url)) => {
            (StatusCode::TEMPORARY_REDIRECT, [(header::LOCATION, url)]).into_response()
        }
        Ok(File::Bytes(bytes)) => bytes.into_response(),
        Ok(File::Stream(stream)) => Body::from_stream(stream).into_response(),
        Ok(File::None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            tracing::error!("{err:?}");
            error_response(&err)
        }
    }
}

fn error_status(error: &anyhow::Error) -> StatusCode {
    if error.is::<crate::coordination::ResourceLimit>() {
        StatusCode::SERVICE_UNAVAILABLE
    } else if error.is::<UpstreamIndexError>() {
        StatusCode::BAD_GATEWAY
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

fn error_response(error: &anyhow::Error) -> Response {
    let status = error_status(error);
    if status == StatusCode::SERVICE_UNAVAILABLE {
        (status, [(header::RETRY_AFTER, "1")]).into_response()
    } else {
        status.into_response()
    }
}

#[cfg(test)]
mod tests;
