//! Shared OpenAPI serving seam. `with_openapi` mounts a STATIC `GET /openapi.json` (the
//! document as JSON) and `GET /docs` (the Scalar UI, spec baked inline) for services with no
//! runtime-derived content (ingest). `with_openapi_provider` mounts a DYNAMIC `/openapi.json`
//! that regenerates per request from a provider closure (query-api merges live ontology
//! operations each request) and a `/docs` that loads that live spec by URL. The bearer
//! security scheme is registered by `register_bearer_scheme` on both paths so every
//! documented operation can reference it by `BEARER_SCHEME_NAME`.

use std::future::Future;

use axum::Router;
use axum::response::{Html, Json};
use axum::routing::get;
use utoipa::openapi::OpenApi;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa_scalar::{Scalar, Servable};

/// The OpenAPI `securityScheme` key for loom's bearer-session auth. Handler
/// `#[utoipa::path(security(("bearer_auth" = [])))]` attributes reference this name.
pub const BEARER_SCHEME_NAME: &str = "bearer_auth";

/// Minimal Scalar page that loads the live spec from `/openapi.json` at view time (so the
/// rendered UI reflects runtime-generated ontology operations). Scalar's viewer is fetched
/// from its CDN, matching the static path's no-build-asset posture.
const SCALAR_DOCS_HTML: &str = r#"<!doctype html>
<html>
  <head>
    <title>loom API</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
  </head>
  <body>
    <script id="api-reference" data-url="/openapi.json"></script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
  </body>
</html>"#;

/// Register loom's bearer-session security scheme on a document's components. Shared by the
/// static (`with_openapi`) and dynamic (`with_openapi_provider`) serving paths so documented
/// operations resolve their `security(...)` reference.
pub fn register_bearer_scheme(doc: &mut OpenApi) {
    let components = doc
        .components
        .get_or_insert_with(utoipa::openapi::Components::new);
    components.add_security_scheme(
        BEARER_SCHEME_NAME,
        SecurityScheme::Http(
            HttpBuilder::new()
                .scheme(HttpAuthScheme::Bearer)
                .bearer_format("opaque")
                .build(),
        ),
    );
}

/// Mount a STATIC OpenAPI document + Scalar docs UI (spec baked inline). Registers the bearer
/// scheme first. Used by services with no runtime-derived content (ingest). Both routes are
/// public (un-gated) — API docs need no token.
#[must_use = "the returned Router must be used to serve requests"]
pub fn with_openapi(router: Router, mut doc: OpenApi) -> Router {
    register_bearer_scheme(&mut doc);
    // Scalar embeds the (cloned) doc inline in the /docs HTML; /openapi.json serves it as JSON.
    let scalar: Router = Scalar::with_url("/docs", doc.clone()).into();
    let json_doc = doc;
    router
        .route(
            "/openapi.json",
            get(move || {
                let d = json_doc.clone();
                async move { Json(d) }
            }),
        )
        .merge(scalar)
}

/// Mount a DYNAMIC OpenAPI document: `/openapi.json` calls `provider` per request (applying
/// the bearer scheme to whatever it returns), and `/docs` loads that live spec by URL. Used by
/// query-api, whose document merges live ontology operations each request. Both routes are
/// public (un-gated), matching the static path.
#[must_use = "the returned Router must be used to serve requests"]
pub fn with_openapi_provider<F, Fut>(router: Router, provider: F) -> Router
where
    F: Fn() -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = OpenApi> + Send + 'static,
{
    router
        .route(
            "/openapi.json",
            get(move || {
                let provider = provider.clone();
                async move {
                    let mut doc = provider().await;
                    register_bearer_scheme(&mut doc);
                    Json(doc)
                }
            }),
        )
        .route("/docs", get(|| async { Html(SCALAR_DOCS_HTML) }))
}
