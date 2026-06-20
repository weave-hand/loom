//! run_action runs the conformance check after the coarse Write gate and before the insert:
//! a misconfigured action is rejected with ActionError::Misconfigured and the engine is never
//! called; a Write-denied subject still gets Forbidden (gate precedes conformance); a conformant
//! action runs the happy path and calls the engine once.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, ActionDef, ActionName, Effect, ObjectType, Ontology, ParamDef, PolicyTarget,
    PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::action::{ActionDeps, ActionError, run_action};
use query_api::serving::{ActionEngine, ServingError, SqlValue};
use serde_json::json;

/// An ActionEngine that records how many times insert_row was called.
struct RecordingEngine {
    calls: Mutex<u32>,
}

impl RecordingEngine {
    fn new() -> Self {
        Self {
            calls: Mutex::new(0),
        }
    }
    fn calls(&self) -> u32 {
        *self.calls.lock().unwrap()
    }
}

#[async_trait]
impl ActionEngine for RecordingEngine {
    async fn insert_row(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
    ) -> Result<(), ServingError> {
        *self.calls.lock().unwrap() += 1;
        Ok(())
    }
}

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}

fn param(name: &str, ty: &str, required: bool) -> ParamDef {
    ParamDef {
        name: name.into(),
        ty: ty.into(),
        required,
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
    cp.define_action(ActionDef {
        name: ActionName("createWidget".into()),
        target: TypeName("Widget".into()),
        parameters: vec![param("id", "Long", true), param("name", "String", false)],
    })
    .await
    .unwrap();
    cp.define_action(ActionDef {
        name: ActionName("createBad".into()),
        target: TypeName("Widget".into()),
        // `naem` matches no property; `id`/`name` are fine.
        parameters: vec![
            param("id", "Long", true),
            param("name", "String", false),
            param("naem", "String", false),
        ],
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
    (cp, analyst)
}

#[tokio::test(flavor = "multi_thread")]
async fn misconfigured_action_is_rejected_before_insert() {
    let (cp, subj) = seeded().await;
    let engine = RecordingEngine::new();
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
    };
    let body = json!({"id": "1", "name": "g", "naem": "x"});
    let err = run_action("createBad", body.as_object().unwrap(), &subj, &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, ActionError::Misconfigured(m) if m.contains("matches no property")),
        "expected Misconfigured, got {err:?}"
    );
    assert_eq!(
        engine.calls(),
        0,
        "insert must not run for a misconfigured action"
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
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
    };
    let body = json!({"id": "1", "name": "g", "naem": "x"});
    let err = run_action("createBad", body.as_object().unwrap(), &stranger, &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, ActionError::Forbidden),
        "expected Forbidden (gate precedes conformance), got {err:?}"
    );
    assert_eq!(engine.calls(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn conformant_action_runs_the_insert() {
    let (cp, subj) = seeded().await;
    let engine = RecordingEngine::new();
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
    };
    let body = json!({"id": "42", "name": "gadget"});
    let rows = run_action("createWidget", body.as_object().unwrap(), &subj, &deps)
        .await
        .unwrap();
    assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(engine.calls(), 1, "conformant action inserts once");
}
