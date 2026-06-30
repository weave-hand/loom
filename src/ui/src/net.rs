//! Runtime API base resolution + the login/logout HTTP calls (gloo-net fetch).

use gloo_net::http::Request;
use loom_ui_core::{AuthError, status_to_error, url};
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
