//! Optional static-file serving for the "tight" deploy where query-api serves the
//! UI bundle itself (same origin, no CORS). Disabled unless a directory is given.

use std::path::PathBuf;

use axum::Router;
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
