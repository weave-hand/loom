//! Runtime API base resolution + the login/logout HTTP calls (gloo-net fetch).

use gloo_net::http::Request;
use loom_ui_core::{
    AuthError, DatasetDetail, DatasetRow, PreviewData, RunRow, TransformDefView, TransformSummary,
    TypeDetail, lineage_closure_path, parse_dataset_detail, parse_datasets, parse_preview,
    parse_runs, parse_transform_def, parse_transform_list, parse_type_detail, status_to_error, url,
};
use serde_json::Value;
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

/// Why a governed request failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchError {
    /// The bearer token is missing/expired (HTTP 401) — the caller should log out.
    Unauthorized,
    /// Authenticated but lacking the required role (HTTP 403).
    Forbidden,
    /// The request never completed (transport / decode failure).
    Network,
    /// The server rejected the request with a message (e.g. HTTP 400 validation).
    Rejected(String),
    /// The server responded with an unexpected status.
    Server(u16),
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthorized => write!(f, "your session has expired — please sign in again"),
            Self::Forbidden => write!(f, "this action requires the admin role"),
            Self::Rejected(msg) => write!(f, "{msg}"),
            Self::Server(c) => write!(f, "server error ({c})"),
            Self::Network => write!(f, "could not reach the server"),
        }
    }
}

fn fetch_status_err(status: u16) -> FetchError {
    match status {
        401 => FetchError::Unauthorized,
        403 => FetchError::Forbidden,
        s => FetchError::Server(s),
    }
}

/// Map a non-success write response to an error, reading the body on 400 so the
/// server's validation message can be surfaced.
async fn write_status_err(resp: gloo_net::http::Response) -> FetchError {
    match resp.status() {
        401 => FetchError::Unauthorized,
        403 => FetchError::Forbidden,
        400 => {
            let msg = resp.text().await.unwrap_or_default();
            if msg.is_empty() {
                FetchError::Rejected("request rejected".to_string())
            } else {
                FetchError::Rejected(msg)
            }
        }
        s => FetchError::Server(s),
    }
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

/// GET the upstream/downstream closure for a catalog dataset `{schema, table}`. loom
/// keys lineage datasets by the canonical loom ref `{loom, "schema.table"}`, not the
/// catalog `{schema, table}` address, so the path is built via
/// [`lineage_closure_path`] (querying `/lineage/datasets/<schema>/<table>/…` matches no
/// stored edge, collapsing the DAG to the current node). `dir` is "upstream" or
/// "downstream". Decodes `{datasets:[{namespace,name}]}` defensively — a missing/absent
/// `datasets` array yields an empty vec.
pub async fn fetch_lineage(
    base: &str,
    token: &str,
    schema: &str,
    table: &str,
    dir: &str,
) -> Result<Vec<(String, String)>, FetchError> {
    let path = lineage_closure_path(schema, table, dir);
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

/// GET /admin/transforms with the bearer token.
pub async fn list_transforms(base: &str, token: &str) -> Result<Vec<TransformSummary>, FetchError> {
    let resp = Request::get(&url(base, "/admin/transforms"))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 200 {
        return Err(fetch_status_err(resp.status()));
    }
    let body: Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(parse_transform_list(&body))
}

/// GET /admin/transforms/{name} with the bearer token.
pub async fn get_transform(
    base: &str,
    token: &str,
    name: &str,
) -> Result<TransformDefView, FetchError> {
    let resp = Request::get(&url(base, &format!("/admin/transforms/{name}")))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 200 {
        return Err(fetch_status_err(resp.status()));
    }
    let body: Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(parse_transform_def(&body))
}

/// GET /admin/transforms/{name}/runs with the bearer token (newest first).
pub async fn list_runs(base: &str, token: &str, name: &str) -> Result<Vec<RunRow>, FetchError> {
    let resp = Request::get(&url(base, &format!("/admin/transforms/{name}/runs")))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 200 {
        return Err(fetch_status_err(resp.status()));
    }
    let body: Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(parse_runs(&body))
}

/// POST /admin/transforms — define/redefine (expects 201).
pub async fn define_transform(base: &str, token: &str, def: &Value) -> Result<(), FetchError> {
    let resp = Request::post(&url(base, "/admin/transforms"))
        .header("Authorization", &format!("Bearer {token}"))
        .json(def)
        .map_err(|_| FetchError::Network)?
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() == 201 {
        Ok(())
    } else {
        Err(write_status_err(resp).await)
    }
}

/// POST /admin/roles/{role}/grants — add one coarse grant (expects 2xx). Generic
/// role-grant POST; the admin surface implies the caller holds the reserved
/// `admin` role.
#[expect(
    dead_code,
    reason = "kept as a generic role-grant helper now that the transform-output \
        self-grant call site (its only caller) moved server-side; \
        see iss-catalog-lineage-acl-asymmetry task 4"
)]
pub async fn post_role_grant(
    base: &str,
    token: &str,
    role: &str,
    grant: &Value,
) -> Result<(), FetchError> {
    let resp = Request::post(&url(base, &format!("/admin/roles/{role}/grants")))
        .header("Authorization", &format!("Bearer {token}"))
        .json(grant)
        .map_err(|_| FetchError::Network)?
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if (200..300).contains(&resp.status()) {
        Ok(())
    } else {
        Err(write_status_err(resp).await)
    }
}

/// DELETE /admin/transforms/{name} — idempotent delete (expects 200).
pub async fn delete_transform(base: &str, token: &str, name: &str) -> Result<(), FetchError> {
    let resp = Request::delete(&url(base, &format!("/admin/transforms/{name}")))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() == 200 {
        Ok(())
    } else {
        Err(fetch_status_err(resp.status()))
    }
}

/// POST /admin/transforms/{name}/run — run a saved transform now (expects 202 {run_id}).
pub async fn run_transform(base: &str, token: &str, name: &str) -> Result<String, FetchError> {
    let resp = Request::post(&url(base, &format!("/admin/transforms/{name}/run")))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 202 {
        return Err(write_status_err(resp).await);
    }
    let body: Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(body
        .get("run_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string())
}

/// POST /admin/transforms/run — run an ad-hoc body (expects 202 {run_id}).
pub async fn run_adhoc(base: &str, token: &str, body_json: &Value) -> Result<String, FetchError> {
    let resp = Request::post(&url(base, "/admin/transforms/run"))
        .header("Authorization", &format!("Bearer {token}"))
        .json(body_json)
        .map_err(|_| FetchError::Network)?
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 202 {
        return Err(write_status_err(resp).await);
    }
    let body: Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(body
        .get("run_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string())
}
