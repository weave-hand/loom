//! Optional static-file serving for the "tight" deploy where query-api serves the
//! UI bundle itself (same origin, no CORS). Disabled unless a directory is given.
//! Also provides an optional CORS layer for the "detached" deploy where the UI
//! runs on its own origin.

use std::path::PathBuf;

use axum::Router;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderValue, Method};
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};

/// Attach a `ServeDir` SPA fallback rooted at `ui_dir` (serving `index.html` for
/// unmatched routes). When `ui_dir` is `None`, the router is returned unchanged so
/// the pure-API deploy is unaffected. Real routes on `router` take precedence —
/// the static service is only the *fallback*.
pub fn with_static(router: Router, ui_dir: Option<PathBuf>) -> Router {
    match ui_dir {
        Some(dir) => {
            let index = dir.join("index.html");
            let serve = ServeDir::new(dir).fallback(ServeFile::new(index));
            router.fallback_service(serve)
        }
        None => router,
    }
}

/// Parse `LOOM_CORS_ALLOWED_ORIGINS` (comma-separated origins) into a trimmed,
/// non-empty list. Blank input ⇒ empty (no CORS).
#[must_use]
pub fn parse_allowed_origins(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}

/// Wrap `router` in a `CorsLayer` permitting the given origins (for the detached
/// deploy). Empty `origins` ⇒ the router is returned unchanged (tight deploy adds
/// nothing). Allows GET/POST/OPTIONS and the `Authorization`/`Content-Type` headers
/// so the UI's preflight + Bearer calls pass.
pub fn with_cors(router: Router, origins: &[String]) -> Router {
    if origins.is_empty() {
        return router;
    }
    let parsed: Vec<HeaderValue> = origins
        .iter()
        .filter_map(|o| o.parse::<HeaderValue>().ok())
        .collect();
    let layer = CorsLayer::new()
        .allow_origin(parsed)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([AUTHORIZATION, CONTENT_TYPE]);
    router.layer(layer)
}
