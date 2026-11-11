use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
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
        Some(page) => {
            crate::logging::index(&index);
            ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], page).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn get_detail(
    State(state): AppState,
    Path((index, package)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let Some(content_type) = negotiate(&headers) else {
        return (StatusCode::NOT_ACCEPTABLE, [(header::VARY, "Accept")]).into_response();
    };
    let Ok(package) = package.parse::<crate::package_name::PackageName>() else {
        return (StatusCode::NOT_FOUND, [(header::VARY, "Accept")]).into_response();
    };
    let mut response = match state.package_detail(&index, &package, None).await {
        Ok(Some(detail)) => {
            let body = if content_type == crate::models::JSON_TYPE {
                detail.json.clone()
            } else {
                detail.html.clone()
            };
            ([(header::CONTENT_TYPE, content_type)], body).into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => error_response(&err),
    };
    response
        .headers_mut()
        .append(header::VARY, header::HeaderValue::from_static("Accept"));
    response
}

// Specific ranges override wildcards, including explicit q=0 exclusions.
fn negotiate(headers: &HeaderMap) -> Option<&'static str> {
    if !headers.contains_key(header::ACCEPT) {
        return Some("text/html");
    }
    let mut selected = None;
    let mut best_quality = 0u16;
    for candidate in [
        crate::models::JSON_TYPE,
        crate::models::HTML_TYPE,
        "text/html",
    ] {
        let mut preference: Option<(u8, u16)> = None;
        for value in headers.get_all(header::ACCEPT) {
            let Ok(value) = value.to_str() else {
                continue;
            };
            for range in value.split(',') {
                let mut parts = range.trim().split(';');
                let media = parts.next().unwrap_or("").trim();
                let specificity = if media.eq_ignore_ascii_case(candidate)
                    || (candidate != "text/html"
                        && media.eq_ignore_ascii_case(&candidate.replace(".v1+", ".latest+")))
                {
                    2
                } else if media == "*/*" {
                    0
                } else if media.eq_ignore_ascii_case(if candidate.starts_with("application/") {
                    "application/*"
                } else {
                    "text/*"
                }) {
                    1
                } else {
                    continue;
                };
                let mut quality = 1000;
                for parameter in parts {
                    if let Some((name, value)) = parameter.trim().split_once('=') {
                        if name.trim().eq_ignore_ascii_case("q") {
                            quality = parse_quality(value.trim()).unwrap_or(0);
                        } else {
                            quality = 0;
                        }
                    } else {
                        quality = 0;
                    }
                }
                if preference.is_none_or(|(specific, q)| {
                    specificity > specific || (specificity == specific && quality > q)
                }) {
                    preference = Some((specificity, quality));
                }
            }
        }
        let quality = preference.map_or(0, |(_, quality)| quality);
        if quality > best_quality {
            best_quality = quality;
            selected = Some(candidate);
        }
    }
    selected
}

fn parse_quality(value: &str) -> Option<u16> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if fraction.len() > 3 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    match whole {
        "0" => Some(fraction.parse::<u16>().unwrap_or(0) * 10u16.pow(3 - fraction.len() as u32)),
        "1" if fraction.bytes().all(|byte| byte == b'0') => Some(1000),
        _ => None,
    }
}

async fn get_file(
    State(state): AppState,
    Path((index, package, hash, file)): Path<(String, String, String, String)>,
) -> Response {
    match state.get_file(index, package, hash, file).await {
        Ok(File::PresignedUrl(url)) => {
            crate::telemetry::delivery("redirect");
            (StatusCode::TEMPORARY_REDIRECT, [(header::LOCATION, url)]).into_response()
        }
        Ok(File::Bytes(bytes)) => {
            crate::telemetry::delivery("bytes");
            bytes.into_response()
        }
        Ok(File::Stream(stream)) => {
            crate::telemetry::delivery("stream");
            Body::from_stream(stream).into_response()
        }
        Ok(File::None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => error_response(&err),
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
    crate::logging::record_error(error);
    let status = error_status(error);
    if status == StatusCode::SERVICE_UNAVAILABLE {
        (status, [(header::RETRY_AFTER, "1")]).into_response()
    } else {
        status.into_response()
    }
}

#[cfg(test)]
mod tests;
