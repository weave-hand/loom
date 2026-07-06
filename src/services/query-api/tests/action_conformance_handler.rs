//! run_action runs the conformance check after the coarse Write gate and before the insert:
//! a misconfigured action is rejected with ActionError::Misconfigured and the engine is never
//! called; a Write-denied subject still gets Forbidden (gate precedes conformance); a conformant
//! action runs the happy path, delivers the LineageEvent to the engine (atomic seam),
//! and the returned run_id matches the event's run_id.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, ControlPlane, DatasetRef, Effect, LineageEvent,
    ObjectType, Ontology, PageReq, ParamDef, PolicyTarget, PropertyDef, RoleId, SnapshotId,
    SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::action::{ActionDeps, ActionError, run_action};
use query_api::serving::{ActionEngine, Rows, ServingError, SqlValue};
use serde_json::json;

/// An ActionEngine that records every LineageEvent it is handed (the atomic seam).
struct RecordingEngine {
    events: Mutex<Vec<LineageEvent>>,
}

impl RecordingEngine {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }
    fn events(&self) -> Vec<LineageEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait]
impl ActionEngine for RecordingEngine {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        event: LineageEvent,
    ) -> Result<SnapshotId, ServingError> {
        self.events.lock().unwrap().push(event);
        Ok(SnapshotId(1))
    }

    async fn overwrite_table(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _rows: &[Vec<SqlValue>],
        _logical_types: &[String],
        event: LineageEvent,
    ) -> Result<SnapshotId, ServingError> {
        self.events.lock().unwrap().push(event);
        Ok(SnapshotId(1))
    }
}

/// A no-op `ServingEngine` stub for action handler tests that only exercise write
/// paths and never issue read queries against the serving engine.
struct NullServing;

#[async_trait]
impl query_api::serving::ServingEngine for NullServing {
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

/// MemoryControlPlane with a Widget type (id Long req, name String opt), a conformant
/// `createWidget` action, a misconfigured `createBad` action (extra param `naem`), and a
/// subject `analyst` granted Action::Write on Widget.
async fn seeded() -> (MemoryControlPlane, SubjectId) {
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
        // `naem` matches no property; `id`/`name` are fine.
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
    (cp, analyst)
}

#[tokio::test(flavor = "multi_thread")]
async fn misconfigured_action_is_rejected_before_insert() {
    let (cp, subj) = seeded().await;
    let engine = RecordingEngine::new();
    let null_serving = NullServing;
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &null_serving,
    };
    let body = json!({"id": "1", "name": "g", "naem": "x"});
    let err = run_action("createBad", body.as_object().unwrap(), &subj, &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, ActionError::Misconfigured(m) if m.contains("matches no property")),
        "expected Misconfigured, got {err:?}"
    );
    assert!(
        engine.events().is_empty(),
        "no write for a misconfigured action"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn write_denied_subject_is_forbidden_not_misconfigured() {
    // The coarse Write gate precedes conformance: an ungranted subject sees Forbidden, never
    // learning the action is also misconfigured.
    let (cp, _granted) = seeded().await;
    let stranger = SubjectId("stranger".into());
    cp.define_subject(&stranger).await.unwrap();
    let engine = RecordingEngine::new();
    let null_serving = NullServing;
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &null_serving,
    };
    let body = json!({"id": "1", "name": "g", "naem": "x"});
    let err = run_action("createBad", body.as_object().unwrap(), &stranger, &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, ActionError::Forbidden),
        "expected Forbidden (gate precedes conformance), got {err:?}"
    );
    assert!(engine.events().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn conformant_action_runs_the_insert() {
    let (cp, subj) = seeded().await;
    let engine = RecordingEngine::new();
    let null_serving = NullServing;
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &null_serving,
    };
    let body = json!({"id": "42", "name": "gadget"});
    let (rows, run_id) = run_action("createWidget", body.as_object().unwrap(), &subj, &deps)
        .await
        .unwrap();
    assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
    let events = engine.events();
    assert_eq!(events.len(), 1, "conformant action writes once");
    // The returned run_id IS the run_id of the event handed to the engine (atomic seam).
    assert_eq!(events[0].run_id, run_id);
    assert_eq!(
        events[0].outputs,
        vec![DatasetRef::from(&TypeName("Widget".into()))],
        "lineage names the target type's dataset"
    );
    // run_action no longer emits separately: nothing landed in the control plane's
    // own lineage (only the engine received the event, in its atomic commit).
    let found = cp
        .lineage()
        .events_for(&run_id, PageReq::unbounded())
        .await
        .unwrap();
    assert!(
        found.items.is_empty(),
        "no separate best-effort emit on the handler path"
    );
}
