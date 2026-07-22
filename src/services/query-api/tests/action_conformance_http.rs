//! The /actions/:name route maps a misconfigured action to 500 with the descriptive conformance
//! body (NOT the opaque catch-all 500), and a conformant action to 201 CREATED. In-memory: the
//! conformance check fails before any storage access, so a stub serving engine + a no-op action
//! engine suffice (no fixture).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, ControlPlane, Effect, ObjectType, Ontology,
    ParamDef, PolicyTarget, PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, Rows, ServingEngine, ServingError, SqlValue};
use serde_json::json;
use service_runtime::Subject;
use tower::ServiceExt;

/// Serving engine that is never called on the action path (reads only). Returns empty.
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

/// No-op write engine: accepts the insert (the conformant path reaches it).
struct OkEngine;

#[async_trait]
impl ActionEngine for OkEngine {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        _nullable: &[bool],
        _event: control_plane_core::LineageEvent,
        _jobs: &[control_plane_core::NewJob],
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        Ok(control_plane_core::SnapshotId(1))
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
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        Err(ServingError::Engine("overwrite_table unsupported".into()))
    }
}

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    let p = PropertyDef::new(name, ty);
    if required { p.required() } else { p }
}

fn param(name: &str, ty: &str, required: bool) -> ParamDef {
    let p = ParamDef::new(name, ty);
    if required { p.required() } else { p }
}

async fn seeded_state() -> AppState {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(
        ObjectType::build("Widget", ("main", "widget"))
            .add_prop(prop("id", "Long", true))
            .add_prop(prop("name", "String", false))
            .identity("id")
            .done(),
    )
    .await
    .unwrap();
    cp.define_action(ActionDef::single_step(
        ActionName("createWidget".into()),
        TypeName("Widget".into()),
        ActionKind::Insert,
        vec![param("id", "Long", true), param("name", "String", false)],
        vec![],
    ))
    .await
    .unwrap();
    cp.define_action(ActionDef::single_step(
        ActionName("createBad".into()),
        TypeName("Widget".into()),
        ActionKind::Insert,
        vec![
            param("id", "Long", true),
            param("name", "String", false),
            param("naem", "String", false),
        ],
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

    AppState {
        cp: Arc::new(cp) as Arc<dyn ControlPlane>,
        serving: Arc::new(StubServing),
        action_engine: Arc::new(OkEngine),
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
        naming: query_api::lineage_filter::local_naming(),
    }
}

async fn post(state: AppState, action: &str, body: serde_json::Value) -> (StatusCode, String) {
    let app = router(state);
    let mut req = Request::builder()
        .method("POST")
        .uri(format!("/actions/{action}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    req.extensions_mut()
        .insert(Subject(SubjectId("analyst".into())));
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn misconfigured_action_is_500_with_descriptive_body() {
    let (status, body) = post(seeded_state().await, "createBad", json!({"id": "1"})).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "body: {body}");
    assert!(
        body.contains("parameter `naem` matches no property of type `Widget`"),
        "descriptive conformance body, not opaque: {body}"
    );
    assert_ne!(
        body, "internal error",
        "must not be the opaque catch-all body"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn conformant_action_is_201() {
    let (status, body) = post(
        seeded_state().await,
        "createWidget",
        json!({"id": "42", "name": "gadget"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");
}
