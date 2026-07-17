//! Multi-step `run_action` orchestration: a two-step action resolves every step's row
//! (threading the cross-step binding env so a child step's `StepRef` reads the parent's
//! just-resolved identity), governs each step, then commits ALL steps in ONE atomic
//! `write_steps` call carrying a single lineage event whose `outputs` list every target.
//!
//! Unit-level (memory control plane + a stub `ActionEngine` recording the write_steps call).
//! Governance-denial and real-Iceberg atomicity paths are Task 6's e2e.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, ActionStep, Assignment, DatasetRef, Effect,
    LineageEvent, ObjectType, Ontology, ParamDef, PolicyTarget, RoleId, SnapshotId, SubjectId,
    TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::action::{ActionDeps, run_action};
use query_api::serving::{ActionEngine, Rows, ServingError, SqlValue, StepWrite, WriteMode};
use serde_json::json;

/// A single `StepWrite` captured out of a `write_steps` call (the struct is not `Clone`).
#[derive(Clone)]
struct RecordedStep {
    table: TableRef,
    columns: Vec<String>,
    rows: Vec<Vec<SqlValue>>,
    mode: WriteMode,
}

/// An `ActionEngine` recording every `write_steps` call (steps + lineage event) and
/// counting any single-object write (`write_object`/`overwrite_table`) — which a
/// multi-step action must NEVER take.
struct MultiEngine {
    steps_calls: Mutex<Vec<(Vec<RecordedStep>, LineageEvent)>>,
    single_calls: Mutex<usize>,
}

impl MultiEngine {
    fn new() -> Self {
        Self {
            steps_calls: Mutex::new(Vec::new()),
            single_calls: Mutex::new(0),
        }
    }
}

#[async_trait]
impl ActionEngine for MultiEngine {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        _event: LineageEvent,
        _jobs: &[control_plane_core::NewJob],
    ) -> Result<SnapshotId, ServingError> {
        *self.single_calls.lock().unwrap() += 1;
        Ok(SnapshotId(1))
    }

    async fn overwrite_table(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _rows: &[Vec<SqlValue>],
        _logical_types: &[String],
        _event: LineageEvent,
        _jobs: &[control_plane_core::NewJob],
    ) -> Result<SnapshotId, ServingError> {
        *self.single_calls.lock().unwrap() += 1;
        Ok(SnapshotId(1))
    }

    async fn write_steps(
        &self,
        writes: &[StepWrite],
        event: LineageEvent,
        _jobs: &[control_plane_core::NewJob],
    ) -> Result<SnapshotId, ServingError> {
        let recorded = writes
            .iter()
            .map(|w| RecordedStep {
                table: w.table.clone(),
                columns: w.columns.clone(),
                rows: w.rows.clone(),
                mode: w.mode,
            })
            .collect();
        self.steps_calls.lock().unwrap().push((recorded, event));
        Ok(SnapshotId(7))
    }
}

/// A no-op `ServingEngine` — the Insert-only action under test never reads.
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

fn tn(s: &str) -> TypeName {
    TypeName(s.into())
}

fn param(name: &str, ty: &str, required: bool) -> ParamDef {
    let p = ParamDef::new(name, ty);
    if required { p.required() } else { p }
}

fn param_bound(name: &str, ty: &str, required: bool, binds: &str) -> ParamDef {
    let p = ParamDef::new(name, ty).binds(binds);
    if required { p.required() } else { p }
}

/// Order: id (Long, required, identity), note (String, optional).
fn order() -> ObjectType {
    ObjectType::build("Order", ("main", "order"))
        .prop_req("id", "Long")
        .prop("note", "String")
        .identity("id")
        .done()
}

/// LineItem: orderId (Long, required), sku (String, required).
fn line_item() -> ObjectType {
    ObjectType::build("LineItem", ("main", "line_item"))
        .prop_req("orderId", "Long")
        .prop_req("sku", "String")
        .done()
}

/// `createOrderWithLine`: step0 inserts an Order (param `orderId`→property `id`, bound `order`);
/// step1 inserts a LineItem whose `orderId` is `@order.id` (a StepRef into the parent's resolved
/// identity) and whose `sku` is a caller param.
fn action() -> ActionDef {
    ActionDef {
        name: ActionName("createOrderWithLine".into()),
        steps: vec![
            ActionStep {
                target: tn("Order"),
                kind: ActionKind::Insert,
                parameters: vec![param_bound("orderId", "Long", true, "id")],
                assignments: vec![],
                bind: Some("order".into()),
            },
            ActionStep {
                target: tn("LineItem"),
                kind: ActionKind::Insert,
                parameters: vec![param("sku", "String", true)],
                assignments: vec![Assignment::step_ref("orderId", "order", "id")],
                bind: Some("li".into()),
            },
        ],
        downstream: Vec::new(),
        description: None,
    }
}

/// MemoryControlPlane with Order + LineItem types, the `createOrderWithLine` action, and a
/// subject `analyst` granted Action::Write on BOTH target types.
async fn seeded() -> (MemoryControlPlane, SubjectId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(order()).await.unwrap();
    cp.define_type(line_item()).await.unwrap();
    cp.define_action(action()).await.unwrap();

    let analyst = SubjectId("analyst".into());
    let writer = RoleId("writer".into());
    cp.define_subject(&analyst).await.unwrap();
    cp.define_role(&writer).await.unwrap();
    cp.assign_role(&analyst, &writer).await.unwrap();
    for ty in ["Order", "LineItem"] {
        cp.grant(
            &writer,
            Action::Write,
            PolicyTarget::Type(tn(ty)),
            Effect::Allow,
        )
        .await
        .unwrap();
    }
    (cp, analyst)
}

#[tokio::test(flavor = "multi_thread")]
async fn two_step_action_resolves_stepref_and_writes_once() {
    let (cp, subj) = seeded().await;
    let engine = MultiEngine::new();
    let null_serving = NullServing;
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &null_serving,
    };

    let body = json!({ "orderId": "100", "sku": "ABC" });
    let (_rows, run_id, _kind) = run_action(
        "createOrderWithLine",
        body.as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("multi-step action should succeed");

    // Exactly ONE atomic write_steps call; no single-object write path taken.
    assert_eq!(
        *engine.single_calls.lock().unwrap(),
        0,
        "multi-step action must not take the single-object write path"
    );
    let calls = engine.steps_calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "one atomic write_steps call for the action");
    let (steps, event) = &calls[0];

    // Two StepWrites: one per target table (Order, LineItem), both Append.
    assert_eq!(steps.len(), 2, "two per-target writes");
    let order_w = steps
        .iter()
        .find(|s| s.table.name == "order")
        .expect("an Order StepWrite");
    let li_w = steps
        .iter()
        .find(|s| s.table.name == "line_item")
        .expect("a LineItem StepWrite");
    assert_eq!(order_w.mode, WriteMode::Append);
    assert_eq!(li_w.mode, WriteMode::Append);

    // The child's `orderId` resolved from the parent's just-minted identity (@order.id = 100).
    let oi = li_w
        .columns
        .iter()
        .position(|c| c == "orderId")
        .expect("LineItem row carries orderId");
    let row = li_w.rows.first().expect("one LineItem row");
    assert_eq!(
        row.get(oi),
        Some(&SqlValue::Int(100)),
        "LineItem.orderId is the parent Order's resolved identity"
    );

    // The single lineage event names the returned run_id and lists BOTH targets in outputs.
    assert_eq!(
        event.run_id, run_id,
        "returned run_id is the event's run_id"
    );
    assert_eq!(
        event.outputs,
        vec![
            DatasetRef::from(&tn("Order")),
            DatasetRef::from(&tn("LineItem")),
        ],
        "lineage outputs list every step's target dataset"
    );
}
