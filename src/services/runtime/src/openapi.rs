//! Shared OpenAPI serving seam. `with_openapi` mounts `GET /openapi.json` (the
//! document as JSON) and `GET /docs` (the Scalar UI, which loads its viewer from a
//! CDN at view time and embeds the spec inline — no build-time asset download). The
//! bearer security scheme is registered here once so every documented operation can
//! reference it by `BEARER_SCHEME_NAME`.

use axum::Router;
use axum::response::Json;
use axum::routing::get;
use utoipa::openapi::OpenApi;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa_scalar::{Scalar, Servable};

/// The OpenAPI `securityScheme` key for loom's bearer-session auth. Handler
/// `#[utoipa::path(security(("bearer_auth" = [])))]` attributes reference this name.
pub const BEARER_SCHEME_NAME: &str = "bearer_auth";

/// Mount the OpenAPI document + Scalar docs UI onto `router`. Registers the bearer
/// security scheme on the document's components first, so documented operations
/// resolve their `security(...)` reference. The document is returned as a value by
/// each service's `build_openapi()` — the ontology hook for the slice-2 `.extend`.
#[must_use = "the returned Router must be used to serve requests"]
pub fn with_openapi(router: Router, mut doc: OpenApi) -> Router {
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
    // Scalar embeds the (cloned) doc inline in the /docs HTML; /openapi.json serves it
    // as JSON for tooling/codegen. Both are public (un-gated) — API docs need no token.
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
