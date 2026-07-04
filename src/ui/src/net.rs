//! Runtime API base resolution + the login/logout HTTP calls (gloo-net fetch).

use gloo_net::http::Request;
use loom_ui_core::{
    AuthError, DatasetDetail, DatasetRow, ObjectsPage, PreviewData, TypeDetail,
    parse_dataset_detail, parse_datasets, parse_objects_page, parse_preview, parse_type_detail,
    status_to_error, url,
};
use wasm_bindgen::JsValue;

/// Read `window.LOOM_CONFIG.apiBase` (shipped default ""), so the same bundle is
/// same-origin (empty) or points at a detached query-api (overridden config.js).
pub fn api_base() -> String {
    let cfg = js_sys::Reflect::get(
        &web_sys::window().unwrap().into(),
        &JsValue::from_str("LOOM_CONFIG"),
    )
    .ok()
    .filter(|v| !v.is_undefined() && !v.is_null());
    cfg.and_then(|c| js_sys::Reflect::get(&c, &JsValue::from_str("apiBase")).ok())
        .and_then(|v| v.as_string())
        .unwrap_or_default()
}

#[derive(serde::Serialize)]
struct LoginBody<'a> {
    username: &'a str,
    password: &'a str,
}

#[derive(serde::Deserialize)]
struct TokenResp {
    token: String,
}

/// POST /auth/login. Ok(token) on 200, else the mapped AuthError.
pub async fn login(base: &str, username: &str, password: &str) -> Result<String, AuthError> {
    let req = Request::post(&url(base, "/auth/login"))
        .json(&LoginBody { username, password })
        .map_err(|_| AuthError::Network)?;
    let resp = req.send().await.map_err(|_| AuthError::Network)?;
    if resp.status() == 200 {
        let body: TokenResp = resp.json().await.map_err(|_| AuthError::Network)?;
        Ok(body.token)
    } else {
        Err(status_to_error(resp.status()))
    }
}

/// POST /auth/logout with the bearer token. Best-effort; errors are ignored.
pub async fn logout(base: &str, token: &str) {
    let _ = Request::post(&url(base, "/auth/logout"))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await;
}

/// Why a governed `GET` failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchError {
    /// The bearer token is missing/expired (HTTP 401) — the caller should log out.
    Unauthorized,
    /// The request never completed (transport / decode failure).
    Network,
    /// The server responded with an unexpected non-401 status.
    Server(u16),
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthorized => write!(f, "your session has expired — please sign in again"),
            Self::Server(c) => write!(f, "server error ({c})"),
            Self::Network => write!(f, "could not reach the server"),
        }
    }
}

fn fetch_status_err(status: u16) -> FetchError {
    if status == 401 {
        FetchError::Unauthorized
    } else {
        FetchError::Server(status)
    }
}

/// Percent-encode a cursor value for use in a query string.
fn encode_cursor(c: &str) -> String {
    js_sys::encode_uri_component(c).into()
}

/// GET /ontology/types with the bearer token. Decodes `{"types": [...]}`.
pub async fn fetch_types(base: &str, token: &str) -> Result<Vec<String>, FetchError> {
    let resp = Request::get(&url(base, "/ontology/types"))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 200 {
        return Err(fetch_status_err(resp.status()));
    }
    let body: serde_json::Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(body
        .get("types")
        .and_then(|t| t.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(ToOwned::to_owned))
                .collect()
        })
        .unwrap_or_default())
}

/// GET /objects/{type_name}?limit=&cursor= with the bearer token. `cursor` is
/// percent-encoded when present; `limit` is always sent.
pub async fn fetch_page(
    base: &str,
    token: &str,
    type_name: &str,
    cursor: Option<&str>,
    limit: u32,
) -> Result<ObjectsPage, FetchError> {
    let mut path = format!("/objects/{type_name}?limit={limit}");
    if let Some(c) = cursor {
        path.push_str(&format!("&cursor={}", encode_cursor(c)));
    }
    let resp = Request::get(&url(base, &path))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 200 {
        return Err(fetch_status_err(resp.status()));
    }
    let body: serde_json::Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(parse_objects_page(&body))
}

/// GET /ontology/types/{name} with the bearer token.
pub async fn fetch_type_detail(
    base: &str,
    token: &str,
    type_name: &str,
) -> Result<TypeDetail, FetchError> {
    let resp = Request::get(&url(base, &format!("/ontology/types/{type_name}")))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 200 {
        return Err(fetch_status_err(resp.status()));
    }
    let body: serde_json::Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(parse_type_detail(&body))
}

/// GET /datasets with the bearer token.
pub async fn fetch_datasets(base: &str, token: &str) -> Result<Vec<DatasetRow>, FetchError> {
    let resp = Request::get(&url(base, "/datasets"))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 200 {
        return Err(fetch_status_err(resp.status()));
    }
    let body: serde_json::Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(parse_datasets(&body))
}

/// GET /datasets/{schema}/{table} with the bearer token.
pub async fn fetch_dataset_detail(
    base: &str,
    token: &str,
    schema: &str,
    table: &str,
) -> Result<DatasetDetail, FetchError> {
    let resp = Request::get(&url(base, &format!("/datasets/{schema}/{table}")))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 200 {
        return Err(fetch_status_err(resp.status()));
    }
    let body: serde_json::Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(parse_dataset_detail(&body))
}

/// GET /lineage/datasets/{namespace}/{name}/{dir} → the closure's (namespace,name)
/// pairs. `dir` is "upstream" or "downstream". Decodes `{datasets:[{namespace,name}]}`
/// defensively — a missing/absent `datasets` array yields an empty vec.
pub async fn fetch_lineage(
    base: &str,
    token: &str,
    namespace: &str,
    name: &str,
    dir: &str,
) -> Result<Vec<(String, String)>, FetchError> {
    let path = format!("/lineage/datasets/{namespace}/{name}/{dir}");
    let resp = Request::get(&url(base, &path))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 200 {
        return Err(fetch_status_err(resp.status()));
    }
    let body: serde_json::Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(body
        .get("datasets")
        .and_then(|d| d.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|d| {
                    let ns = d.get("namespace")?.as_str()?.to_string();
                    let nm = d.get("name")?.as_str()?.to_string();
                    Some((ns, nm))
                })
                .collect()
        })
        .unwrap_or_default())
}

/// GET /datasets/{schema}/{table}/preview?limit= with the bearer token.
pub async fn fetch_preview(
    base: &str,
    token: &str,
    schema: &str,
    table: &str,
    limit: u32,
) -> Result<PreviewData, FetchError> {
    let resp = Request::get(&url(
        base,
        &format!("/datasets/{schema}/{table}/preview?limit={limit}"),
    ))
    .header("Authorization", &format!("Bearer {token}"))
    .send()
    .await
    .map_err(|_| FetchError::Network)?;
    if resp.status() != 200 {
        return Err(fetch_status_err(resp.status()));
    }
    let body: serde_json::Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(parse_preview(&body))
}
