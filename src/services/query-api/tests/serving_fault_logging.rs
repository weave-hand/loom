//! Verifies that serving-tier faults log the detail server-side and return an
//! opaque "internal error" 500 — no SQL fragments or internal names leak to the
//! caller. Exercises the `internal_error` helper via the `get_object` HTTP route
//! using an in-memory control plane and a stub serving engine that always faults.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, ControlPlane, Effect, ObjectType, Ontology, PolicyTarget, PropertyDef, RoleId,
    SnapshotId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, Rows, ServingEngine, ServingError, SqlValue};
use service_runtime::Subject;
use tower::ServiceExt;
use tracing_test::traced_test;

struct FaultEngine;

#[async_trait]
impl ServingEngine for FaultEngine {
    async fn fetch_rows(&self, _sql: &str, _params: &[SqlValue]) -> Result<Rows, ServingError> {
        Err(ServingError::Engine("fault-detail-boom".into()))
    }
}

struct NullAction;

#[async_trait]
impl ActionEngine for NullAction {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
    ) -> Result<SnapshotId, ServingError> {
        Ok(SnapshotId(0))
    }

    async fn overwrite_table(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _rows: &[Vec<SqlValue>],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
    ) -> Result<SnapshotId, ServingError> {
        Err(ServingError::Engine("overwrite_table unsupported".into()))
    }
}

async fn build_faulting_app() -> axum::Router {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_secs(5)));
    cp.define_type(ObjectType {
        name: TypeName("FaultType".into()),
        properties: vec![PropertyDef {
            name: "id".into(),
            ty: "long".into(),
            required: false,
            constraints: control_plane_core::PropertyConstraints::default(),
        }],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "fault_type".into(),
        },
        identity: Some("id".into()),
    })
    .await
    .unwrap();
    let role = RoleId("alice-role".into());
    let subj = SubjectId("alice".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName("FaultType".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    router(AppState {
        cp: cp as Arc<dyn ControlPlane>,
        serving: Arc::new(FaultEngine),
        action_engine: Arc::new(NullAction),
        default_limit: 1000,
        naming: query_api::lineage_filter::local_naming(),
    })
}

#[tokio::test]
#[traced_test]
async fn serving_fault_logs_detail_and_returns_opaque_500() {
    let app = build_faulting_app().await;
    let mut req = Request::builder()
        .uri("/objects/FaultType")
        .body(Body::empty())
        .unwrap();
    req.extensions_mut()
        .insert(Subject(SubjectId("alice".into())));
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let body_str = std::str::from_utf8(&body).unwrap();
    assert_eq!(body_str, "internal error");
    assert!(
        !body_str.contains("fault-detail-boom"),
        "fault detail must not leak to client"
    );
    assert!(logs_contain("fault-detail-boom"));
}
