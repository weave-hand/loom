use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use query_api::web_static::with_static;
use tower::ServiceExt;

fn write_index(dir: &std::path::Path) {
    std::fs::write(dir.join("index.html"), "<!doctype html><title>loom</title>").unwrap();
}

#[tokio::test]
async fn serves_index_at_root_when_dir_set() {
    let tmp = std::env::temp_dir().join(format!("loomui-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();
    write_index(&tmp);
    let app = with_static(Router::new(), Some(tmp.clone()));
    let res = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let _cleanup = std::fs::remove_dir_all(&tmp);
}

#[tokio::test]
async fn unknown_route_falls_back_to_index() {
    let tmp = std::env::temp_dir().join(format!("loomui-fb-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();
    write_index(&tmp);
    let app = with_static(Router::new(), Some(tmp.clone()));
    let res = app
        .oneshot(
            Request::builder()
                .uri("/some/spa/route")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK); // SPA fallback to index.html
    let _cleanup = std::fs::remove_dir_all(&tmp);
}

#[tokio::test]
async fn real_api_route_is_not_shadowed_by_fallback() {
    let tmp = std::env::temp_dir().join(format!("loomui-api-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();
    write_index(&tmp);
    let api = Router::new().route("/objects/foo", get(|| async { "api" }));
    let app = with_static(api, Some(tmp.clone()));
    let res = app
        .oneshot(
            Request::builder()
                .uri("/objects/foo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), 64).await.unwrap();
    assert_eq!(&body[..], b"api");
    let _cleanup = std::fs::remove_dir_all(&tmp);
}

#[tokio::test]
async fn no_dir_means_404_at_root() {
    let app = with_static(Router::new(), None);
    let res = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}
