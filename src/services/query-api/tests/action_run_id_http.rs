//! post_action sets an X-Loom-Run-Id header on the 201 equal to the action's run_id.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, ControlPlane, Effect, LineageEvent, ObjectType,
    Ontology, ParamDef, PolicyTarget, PropertyDef, RoleId, RunId, SnapshotId, SubjectId, TableRef,
    TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, Rows, ServingEngine, ServingError, SqlValue};
use service_runtime::Subject;
use tower::ServiceExt;

/// Records the run_id of the single event it is handed.
struct CapturingEngine {
    run_id: Mutex<Option<RunId>>,
}
#[async_trait]
impl ActionEngine for CapturingEngine {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        event: LineageEvent,
    ) -> Result<SnapshotId, ServingError> {
        *self.run_id.lock().unwrap() = Some(event.run_id);
        Ok(SnapshotId(1))
    }

    async fn overwrite_table(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _rows: &[Vec<SqlValue>],
        _logical_types: &[String],
        _event: LineageEvent,
    ) -> Result<SnapshotId, ServingError> {
        Err(ServingError::Engine("overwrite_table unsupported".into()))
    }
}

/// No-op read engine: the action path never queries it, so a plain `rust_test`
/// needs no real serving engine (the production `EngineServingClient` requires a
/// live engine socket, which this unit test deliberately avoids).
struct NoServing;
#[async_trait]
impl ServingEngine for NoServing {
    async fn fetch_rows(&self, _sql: &str, _params: &[SqlValue]) -> Result<Rows, ServingError> {
        Ok(Rows::default())
    }
}

async fn seeded() -> (MemoryControlPlane, SubjectId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(ObjectType {
        name: TypeName("Widget".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "name".into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
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
        parameters: vec![
            ParamDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                binds: None,
            },
            ParamDef {
                name: "name".into(),
                ty: "String".into(),
                required: false,
                binds: None,
            },
        ],
        kind: ActionKind::Insert,
        assignments: vec![],
    })
    .await
    .unwrap();
    let subj = SubjectId("writer".into());
    let role = RoleId("writers".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Write,
        PolicyTarget::Type(TypeName("Widget".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    (cp, subj)
}

#[tokio::test(flavor = "multi_thread")]
async fn created_response_carries_run_id_header() {
    let (cp, subj) = seeded().await;
    let engine = Arc::new(CapturingEngine {
        run_id: Mutex::new(None),
    });
    // A serving engine is required by AppState but the action path never reads it.
    let serving: Arc<dyn ServingEngine> = Arc::new(NoServing);
    let app = router(AppState {
        cp: Arc::new(cp) as Arc<dyn ControlPlane>,
        serving,
        action_engine: engine.clone(),
        default_limit: 1000,
    });
    let mut req = Request::builder()
        .method("POST")
        .uri("/actions/createWidget")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"id":"42","name":"gadget"}"#))
        .unwrap();
    req.extensions_mut().insert(Subject(subj.clone()));
    let res = app.oneshot(req).await.unwrap();

    assert_eq!(res.status(), StatusCode::CREATED);
    let header = res
        .headers()
        .get("X-Loom-Run-Id")
        .expect("X-Loom-Run-Id present")
        .to_str()
        .unwrap()
        .to_string();
    let captured = engine.run_id.lock().unwrap().expect("engine saw the event");
    assert_eq!(
        header,
        captured.0.to_string(),
        "header equals the action's run_id"
    );
}
