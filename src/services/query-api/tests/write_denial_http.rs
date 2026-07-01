//! The /actions/:name route renders a fine-grained Write-policy denial as a
//! structured 403 body (column vs row_filter), keeps Allow at 201, and leaves the
//! coarse-gate denial as a bodyless 403. In-memory: the denial fires before any
//! storage access, so a stub serving engine + no-op write engine suffice (no fixture).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, CompareOp, ControlPlane, Effect, ObjectType,
    Ontology, ParamDef, Policy, PolicyTarget, PropertyDef, RoleId, RowFilter, ScalarValue,
    SubjectId, TableRef, TypeName,
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
    async fn fetch_rows(&self, _sql: &str, _params: &[SqlValue]) -> Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec![],
            rows: vec![],
        })
    }
}

/// No-op write engine: the conforming (Allow) path reaches it; the denial paths do not.
struct OkEngine;

#[async_trait]
impl ActionEngine for OkEngine {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        Ok(control_plane_core::SnapshotId(1))
    }

    async fn overwrite_table(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _rows: &[Vec<SqlValue>],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        Err(ServingError::Engine("overwrite_table unsupported".into()))
    }
}

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
        constraints: control_plane_core::PropertyConstraints::default(),
    }
}
fn param(name: &str, ty: &str, required: bool) -> ParamDef {
    ParamDef {
        name: name.into(),
        ty: ty.into(),
        required,
        binds: None,
    }
}

/// Seed the type + action + coarse Write grant; return the cp (so the caller can
/// `set_policy` the fine-grained policy under test) and the writer role.
async fn seed() -> (MemoryControlPlane, RoleId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(ObjectType {
        name: TypeName("Widget".into()),
        properties: vec![prop("id", "Long", true), prop("name", "String", false)],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "widget".into(),
        },
        identity: Some("id".into()),
    })
    .await
    .unwrap();
    cp.define_action(ActionDef {
        name: ActionName("createWidget".into()),
        target: TypeName("Widget".into()),
        parameters: vec![param("id", "Long", true), param("name", "String", false)],
        kind: ActionKind::Insert,
        assignments: vec![],
    })
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
    (cp, writer)
}

async fn post_json(
    cp: MemoryControlPlane,
    subject: &str,
    action: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let state = AppState {
        cp: Arc::new(cp) as Arc<dyn ControlPlane>,
        serving: Arc::new(StubServing),
        action_engine: Arc::new(OkEngine),
        default_limit: 1000,
    };
    let app = router(state);
    let mut req = Request::builder()
        .method("POST")
        .uri(format!("/actions/{action}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    req.extensions_mut()
        .insert(Subject(SubjectId(subject.into())));
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

#[tokio::test(flavor = "multi_thread")]
async fn column_denial_body_names_the_column() {
    let (cp, writer) = seed().await;
    cp.set_policy(
        &writer,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(TypeName("Widget".into())),
            row_filter: None,
            deny_columns: vec!["name".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    let (status, body) = post_json(
        cp,
        "analyst",
        "createWidget",
        json!({"id": "1", "name": "x"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "body: {body}");
    assert_eq!(
        body,
        json!({ "error": "write_denied", "reason": "column", "column": "name" })
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn row_filter_denial_body_discloses_no_predicate() {
    let (cp, writer) = seed().await;
    cp.set_policy(
        &writer,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(TypeName("Widget".into())),
            row_filter: Some(RowFilter::Compare {
                property: "name".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("gadget".into()),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    let (status, body) = post_json(
        cp,
        "analyst",
        "createWidget",
        json!({"id": "1", "name": "widget"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "body: {body}");
    assert_eq!(
        body,
        json!({ "error": "write_denied", "reason": "row_filter" })
    );
    assert!(
        body.get("column").is_none(),
        "no column on a row-filter denial"
    );
    // The predicate string ("gadget") must never appear in the caller-facing body.
    assert!(
        !body.to_string().contains("gadget"),
        "row_filter predicate must not leak to the caller: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn allowed_write_is_201() {
    let (cp, writer) = seed().await;
    cp.set_policy(
        &writer,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(TypeName("Widget".into())),
            row_filter: Some(RowFilter::Compare {
                property: "name".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("gadget".into()),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    let (status, _body) = post_json(
        cp,
        "analyst",
        "createWidget",
        json!({"id": "1", "name": "gadget"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
}

#[tokio::test(flavor = "multi_thread")]
async fn coarse_gate_denial_is_bodyless_403() {
    // A subject with no coarse Write grant: the coarse gate denies, unchanged by
    // this slice — a bodyless 403 (not the structured write_denied body).
    let (cp, _writer) = seed().await;
    let stranger = SubjectId("stranger".into());
    cp.define_subject(&stranger).await.unwrap();
    let (status, body) = post_json(
        cp,
        "stranger",
        "createWidget",
        json!({"id": "1", "name": "gadget"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        body,
        serde_json::Value::Null,
        "coarse-gate 403 stays bodyless"
    );
}
