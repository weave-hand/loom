//! The shared `with_openapi` seam: mounts `/openapi.json` (the doc as JSON, with a
//! bearer security scheme registered) and `/docs` (the Scalar UI). Built over an empty
//! router + a tiny hand-built doc — the seam is state-free, so no AppState/Postgres.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;
use utoipa::openapi::{InfoBuilder, OpenApiBuilder, PathsBuilder};

fn sample_doc() -> utoipa::openapi::OpenApi {
    OpenApiBuilder::new()
        .info(InfoBuilder::new().title("test").version("0.0.0").build())
        .paths(PathsBuilder::new().build())
        .build()
}

#[tokio::test]
async fn openapi_json_is_served_and_valid() {
    let app = service_runtime::with_openapi(Router::new(), sample_doc());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/openapi.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    // Deserializes as an OpenAPI document.
    let doc: utoipa::openapi::OpenApi = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(doc.info.title, "test");
    // The bearer security scheme is registered by the seam.
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        json["components"]["securitySchemes"][service_runtime::BEARER_SCHEME_NAME].is_object(),
        "bearer security scheme must be registered"
    );
}

#[tokio::test]
async fn docs_ui_is_served_as_html() {
    let app = service_runtime::with_openapi(Router::new(), sample_doc());
    let resp = app
        .oneshot(Request::builder().uri("/docs").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.starts_with("text/html"), "got content-type {ct}");
}

#[tokio::test]
async fn provider_serves_openapi_json_with_bearer_scheme() {
    // A provider that returns a doc carrying a marker path, proving per-request generation.
    let app = service_runtime::with_openapi_provider(Router::new(), || async {
        let mut d = sample_doc();
        d.paths.paths.insert(
            "/live/marker".to_string(),
            utoipa::openapi::path::PathItem::new(
                utoipa::openapi::path::HttpMethod::Get,
                utoipa::openapi::path::OperationBuilder::new().build(),
            ),
        );
        d
    });

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/openapi.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    // The provider's marker path is present (per-request generation ran).
    assert!(json["paths"]["/live/marker"].is_object());
    // The bearer security scheme was applied by the seam, not the provider.
    assert!(
        json["components"]["securitySchemes"][service_runtime::BEARER_SCHEME_NAME].is_object(),
        "bearer security scheme must be registered by the seam"
    );
}

#[tokio::test]
async fn provider_serves_docs_html_pointing_at_the_live_spec() {
    let app = service_runtime::with_openapi_provider(Router::new(), || async { sample_doc() });
    let resp = app
        .oneshot(Request::builder().uri("/docs").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(
        html.contains("/openapi.json"),
        "docs must load the live spec by URL"
    );
}
