//! The /actions/:name route enforces per-property model constraints: a value violating
//! a `range`/`pattern` constraint is rejected with a structured 422 BEFORE any write, a
//! conforming insert reaches the write engine (201), and an ACL denial stays a distinct
//! 403. In-memory: validation fires before storage, so a stub serving engine + a
//! recording write engine suffice (no fixture). The recorder proves the violating path
//! writes nothing non-vacuously.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, ControlPlane, Effect, LengthConstraint,
    ObjectType, Ontology, ParamDef, PolicyTarget, PropertyConstraints, PropertyDef,
    RangeConstraint, RoleId, SnapshotId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, Rows, ServingEngine, ServingError, SqlValue};
use serde_json::json;
use service_runtime::Subject;
use tower::ServiceExt;

struct StubServing;

#[async_trait]
impl ServingEngine for StubServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
        _at: Option<control_plane_core::SnapshotId>,
    ) -> Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec![],
            rows: vec![],
        })
    }
}

/// Records how many times `write_object` was called, so a test can assert the violating
/// path wrote nothing while the conforming path wrote once.
#[derive(Clone)]
struct RecordingEngine {
    writes: Arc<AtomicUsize>,
}

#[async_trait]
impl ActionEngine for RecordingEngine {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        _nullable: &[bool],
        _event: control_plane_core::LineageEvent,
        _jobs: &[control_plane_core::NewJob],
    ) -> Result<SnapshotId, ServingError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(SnapshotId(1))
    }

    async fn overwrite_table(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _rows: &[Vec<SqlValue>],
        _logical_types: &[String],
        _nullable: &[bool],
        _event: control_plane_core::LineageEvent,
        _jobs: &[control_plane_core::NewJob],
    ) -> Result<SnapshotId, ServingError> {
        Err(ServingError::Engine("overwrite_table unsupported".into()))
    }
}

fn param(name: &str, ty: &str, required: bool) -> ParamDef {
    let p = ParamDef::new(name, ty);
    if required { p.required() } else { p }
}

/// Seed a `Widget(id Long [>=1], code String [^[A-Z]+$, len 1..=4])` type + an insert
/// action + a coarse Write grant for the `analyst` subject.
async fn seed() -> MemoryControlPlane {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(
        ObjectType::build("Widget", ("main", "widget"))
            .add_prop(
                PropertyDef::new("id", "Long")
                    .required()
                    .constrained(PropertyConstraints {
                        range: Some(RangeConstraint {
                            min: Some(1.0),
                            max: None,
                        }),
                        ..PropertyConstraints::default()
                    }),
            )
            .add_prop(PropertyDef::new("code", "String").required().constrained(
                PropertyConstraints {
                    pattern: Some("^[A-Z]+$".into()),
                    length: Some(LengthConstraint {
                        min: Some(1),
                        max: Some(4),
                    }),
                    one_of: None,
                    range: None,
                },
            ))
            .identity("id")
            .done(),
    )
    .await
    .unwrap();
    cp.define_action(ActionDef::single_step(
        ActionName("createWidget".into()),
        TypeName("Widget".into()),
        ActionKind::Insert,
        vec![param("id", "Long", true), param("code", "String", true)],
        vec![],
    ))
    .await
    .unwrap();

    let analyst = SubjectId("analyst".into());
    let writer = RoleId("writer".into());
    cp.define_subject(&analyst).await.unwrap();
    cp.define_role(&writer).await.unwrap();
    cp.assign_role(&analyst, &writer).await.unwrap();
    cp.grant(
        &writer,
        Action::Write,
        PolicyTarget::Type(TypeName("Widget".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    cp
}

async fn post_json(
    cp: MemoryControlPlane,
    writes: Arc<AtomicUsize>,
    subject: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let (status, text) = post_json_raw(cp, writes, subject, body).await;
    let json = if text.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

/// Like `post_json`, but returns the raw response body text. `BadParams` renders a
/// plain-text `ParamError` `Display` (not JSON), which `post_json`'s `serde_json` parse
/// would collapse to `Null`; this surfaces it so a test can assert the offending param name.
async fn post_json_raw(
    cp: MemoryControlPlane,
    writes: Arc<AtomicUsize>,
    subject: &str,
    body: serde_json::Value,
) -> (StatusCode, String) {
    let state = AppState {
        cp: Arc::new(cp) as Arc<dyn ControlPlane>,
        serving: Arc::new(StubServing),
        action_engine: Arc::new(RecordingEngine { writes }),
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
        naming: query_api::lineage_filter::local_naming(),
    };
    let app = router(state);
    let mut req = Request::builder()
        .method("POST")
        .uri("/actions/createWidget")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    req.extensions_mut()
        .insert(Subject(SubjectId(subject.into())));
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn pattern_violation_is_422_and_writes_nothing() {
    let cp = seed().await;
    let writes = Arc::new(AtomicUsize::new(0));
    // `code: "ab"` is lowercase → fails the `^[A-Z]+$` pattern.
    let (status, body) = post_json(
        cp,
        writes.clone(),
        "analyst",
        json!({"id": "5", "code": "ab"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "body: {body}");
    let violations = body["violations"].as_array().expect("violations array");
    assert!(
        violations
            .iter()
            .any(|v| v["property"] == "code" && v["rule"] == "pattern"),
        "expected a code/pattern violation, got {body}"
    );
    assert_eq!(
        writes.load(Ordering::SeqCst),
        0,
        "a constraint-violating insert must write nothing"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn range_violation_is_422() {
    let cp = seed().await;
    let writes = Arc::new(AtomicUsize::new(0));
    // `id: 0` is below the `>= 1` range.
    let (status, body) = post_json(
        cp,
        writes.clone(),
        "analyst",
        json!({"id": "0", "code": "AB"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "body: {body}");
    let violations = body["violations"].as_array().expect("violations array");
    assert!(
        violations
            .iter()
            .any(|v| v["property"] == "id" && v["rule"] == "range"),
        "expected an id/range violation, got {body}"
    );
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn conforming_insert_reaches_the_write_engine() {
    let cp = seed().await;
    let writes = Arc::new(AtomicUsize::new(0));
    let (status, body) = post_json(
        cp,
        writes.clone(),
        "analyst",
        json!({"id": "5", "code": "AB"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");
    assert_eq!(
        writes.load(Ordering::SeqCst),
        1,
        "a conforming insert reaches the write engine exactly once"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn acl_denial_is_403_distinct_from_constraint_422() {
    // An unknown subject has no Write grant → coarse-gate denial (403), never reaching
    // constraint validation, even with a constraint-violating body.
    let cp = seed().await;
    let writes = Arc::new(AtomicUsize::new(0));
    let (status, _body) = post_json(
        cp,
        writes.clone(),
        "stranger",
        json!({"id": "0", "code": "ab"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_required_param_is_422() {
    // A well-formed JSON body that omits the required `code` param fails SEMANTIC
    // validation → 422 (was 400), aligning with the constraint-violation 422 above. The
    // plain-text body names the offending param.
    let cp = seed().await;
    let writes = Arc::new(AtomicUsize::new(0));
    let (status, body) = post_json_raw(cp, writes.clone(), "analyst", json!({"id": "5"})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "body: {body}");
    assert!(
        body.contains("code"),
        "expected the body to name the missing `code` param, got {body}"
    );
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn mistyped_param_is_422() {
    // `id` must be a Long (JSON string); passing a JSON number is a semantic type mismatch.
    let cp = seed().await;
    let writes = Arc::new(AtomicUsize::new(0));
    let (status, body) = post_json(
        cp,
        writes.clone(),
        "analyst",
        json!({"id": 5, "code": "AB"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "body: {body}");
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_body_is_400() {
    // A JSON array is not an action-envelope object → the request itself is malformed → 400.
    let cp = seed().await;
    let writes = Arc::new(AtomicUsize::new(0));
    let (status, _body) = post_json(cp, writes.clone(), "analyst", json!([1, 2, 3])).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}
