//! Route tests: GET /datasets lists the live mirror tables and
//! GET /datasets/{schema}/{table} composes the current snapshot with the schema at it;
//! an unknown table 404s. No socket is bound (tower oneshot); the catalog is seeded via
//! the memory fake's `seed_catalog` (the pg `list_tables` path is contract-tested).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{SubjectId, TableRef};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, Rows, ServingEngine, ServingError, SqlValue};
use service_runtime::Subject;
use tower::ServiceExt;

struct StubServing;

#[async_trait]
impl ServingEngine for StubServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
    ) -> std::result::Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec![],
            rows: vec![],
        })
    }
}

struct StubAction;

#[async_trait]
impl ActionEngine for StubAction {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
    ) -> std::result::Result<control_plane_core::SnapshotId, ServingError> {
        Ok(control_plane_core::SnapshotId(0))
    }

    async fn overwrite_table(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _rows: &[Vec<SqlValue>],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
    ) -> std::result::Result<control_plane_core::SnapshotId, ServingError> {
        Err(ServingError::Engine("overwrite_table unsupported".into()))
    }
}

fn app(cp: MemoryControlPlane) -> axum::Router {
    router(AppState {
        cp: Arc::new(cp),
        serving: Arc::new(StubServing),
        action_engine: Arc::new(StubAction),
        default_limit: 1000,
        naming: query_api::lineage_filter::local_naming(),
    })
}

struct CannedServing;

#[async_trait]
impl ServingEngine for CannedServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
    ) -> std::result::Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec!["id".into(), "note".into()],
            rows: vec![
                vec![SqlValue::Int(1), SqlValue::Text("a".into())],
                vec![SqlValue::Int(2), SqlValue::Null],
            ],
        })
    }
}

fn app_canned(cp: MemoryControlPlane) -> axum::Router {
    router(AppState {
        cp: Arc::new(cp),
        serving: Arc::new(CannedServing),
        action_engine: Arc::new(StubAction),
        default_limit: 1000,
        naming: query_api::lineage_filter::local_naming(),
    })
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let mut req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    req.extensions_mut()
        .insert(Subject(SubjectId("analyst".into())));
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// Seed `main.events` (two columns, two append snapshots); returns the cp and the
/// latest (second) snapshot id.
fn seeded() -> (MemoryControlPlane, i64) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    let table = TableRef {
        schema: "main".into(),
        name: "events".into(),
    };
    let cols = vec![
        ("id".to_string(), "Long".to_string(), false),
        ("note".to_string(), "String".to_string(), true),
    ];
    let snapshots = cp.seed_catalog(&table, &cols, &[3, 2]);
    let latest = snapshots.last().expect("seeded snapshots").0;
    (cp, latest)
}

#[tokio::test(flavor = "multi_thread")]
async fn datasets_lists_the_seeded_table() {
    let (cp, _) = seeded();
    let app = app(cp);
    let (status, json) = get(&app, "/datasets").await;
    assert_eq!(status, StatusCode::OK);
    let ds = &json["datasets"][0];
    assert_eq!(ds["schema"], "main");
    assert_eq!(ds["name"], "events");
    assert_eq!(ds["project"], "main");
    assert!(
        ds["updated"].as_str().is_some_and(|t| !t.is_empty()),
        "updated must be a non-empty RFC3339 string, got {json}"
    );
    assert_eq!(json["datasets"].as_array().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn dataset_detail_composes_snapshot_and_columns() {
    let (cp, latest) = seeded();
    let app = app(cp);
    let (status, json) = get(&app, "/datasets/main/events").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["table"]["schema"], "main");
    assert_eq!(json["table"]["name"], "events");
    assert_eq!(json["snapshot_id"], latest);
    assert!(
        json["snapshot_time"]
            .as_str()
            .is_some_and(|t| !t.is_empty()),
        "snapshot_time must be a non-empty RFC3339 string, got {json}"
    );
    assert_eq!(
        json["columns"],
        serde_json::json!([
            { "name": "id", "ty": "Long", "nullable": false },
            { "name": "note", "ty": "String", "nullable": true },
        ])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_dataset_is_404() {
    let (cp, _) = seeded();
    let app = app(cp);
    let (status, _) = get(&app, "/datasets/main/no-such-table").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn dataset_preview_returns_sampled_rows() {
    let (cp, _) = seeded();
    let app = app_canned(cp);
    let (status, json) = get(&app, "/datasets/main/events/preview?limit=5").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["columns"], serde_json::json!(["id", "note"]));
    assert_eq!(json["sampled"], serde_json::json!(true));
    assert_eq!(json["rows"], serde_json::json!([["1", "a"], ["2", ""]]));
}

#[tokio::test(flavor = "multi_thread")]
async fn dataset_preview_rejects_bad_limit() {
    let (cp, _) = seeded();
    let app = app_canned(cp);
    let (status, _) = get(&app, "/datasets/main/events/preview?limit=nope").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
