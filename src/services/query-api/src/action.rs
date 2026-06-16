//! Action handler logic: invoke a named ontology action to insert one new typed object.
//! Governed by Action::Write; executes the inline write via the ActionEngine; emits
//! best-effort type-named lineage (a documented dangling slice — a failure to emit does
//! NOT fail the action).

use control_plane_core::{
    Action, ActionName, ControlPlane, ControlPlaneError, DatasetRef, Decision, EventType,
    LineageEvent, PageReq, PolicyTarget, RunId, SubjectId,
};
use serde_json::Value;
use uuid::Uuid;

use crate::handler::ObjectRows;
use crate::params::{ParamError, parse_params};
use crate::serving::{ActionEngine, SqlValue};
use crate::write_filter::{self, WriteVerdict};

pub struct ActionDeps<'a> {
    pub cp: &'a dyn ControlPlane,
    pub action_engine: &'a dyn ActionEngine,
}

#[derive(Debug, thiserror::Error)]
pub enum ActionError {
    #[error("unknown action: {0}")]
    UnknownAction(String),
    #[error("forbidden")]
    Forbidden,
    #[error("bad parameters: {0}")]
    BadParams(#[from] ParamError),
    #[error(transparent)]
    ControlPlane(#[from] ControlPlaneError),
    #[error(transparent)]
    Serving(#[from] crate::serving::ServingError),
}

/// Run one action: insert a new instance of the action's target type from `body`.
/// Returns the created object as a single-row `ObjectRows` (rendered by the caller).
pub async fn run_action(
    action_name: &str,
    body: &serde_json::Map<String, Value>,
    subject: &SubjectId,
    deps: &ActionDeps<'_>,
) -> Result<ObjectRows, ActionError> {
    // 1. Resolve the action.
    let action = deps
        .cp
        .ontology()
        .get_action(&ActionName(action_name.to_string()))
        .await
        .map_err(|e| match e {
            ControlPlaneError::NotFound(_) => ActionError::UnknownAction(action_name.to_string()),
            other => ActionError::ControlPlane(other),
        })?;

    // 2. Resolve the target type (for its table + property logical types). A missing
    //    target here is a broken ActionDef (internal inconsistency), not a client error —
    //    propagate as a ControlPlane fault (-> 500), not a 404.
    let target = deps.cp.ontology().get_type(&action.target).await?;

    // 3. Govern: deny-by-default Write on the target type. First live use of Action::Write.
    let policy_target = PolicyTarget::Type(action.target.clone());
    if deps
        .cp
        .acl()
        .check(subject, Action::Write, &policy_target)
        .await?
        == Decision::Deny
    {
        return Err(ActionError::Forbidden);
    }

    // 4. Parse + validate the typed params (ordered by the action's parameter list).
    let pairs = parse_params(&action.parameters, body)?;
    let columns: Vec<String> = pairs.iter().map(|(c, _)| c.clone()).collect();
    let values: Vec<SqlValue> = pairs.iter().map(|(_, v)| v.clone()).collect();

    // 4b. Fine-grained Write policy: deny-write-column + row-filter-on-insert. The
    //     subject already cleared the coarse Write gate; now enforce the row/column
    //     policy against the concrete row. Fail-closed (deny on UNKNOWN). The HTTP
    //     body stays a generic 403; the reason is logged only.
    //
    //     `parse_params` materializes every optional parameter the caller OMITTED as an
    //     explicit `SqlValue::Null` pair, so `columns` carries those too. The gate must
    //     judge only the columns the caller actually SET — an omitted optional column is
    //     not "setting" it — so gate on the non-null pairs. (The insert below still uses
    //     the full pair list; inserting NULL for the omitted optionals is correct.)
    let (set_columns, set_values): (Vec<String>, Vec<SqlValue>) = pairs
        .iter()
        .filter(|(_, v)| !matches!(v, SqlValue::Null))
        .cloned()
        .unzip();
    let write_policies = deps
        .cp
        .acl()
        .policies_for(subject, Action::Write, &policy_target, PageReq::unbounded())
        .await?;
    match write_filter::check_write_policy(&write_policies.items, &set_columns, &set_values) {
        WriteVerdict::Allow => {}
        WriteVerdict::DenyColumn(col) => {
            tracing::info!(action = action_name, column = %col, "write denied: policy denies column");
            return Err(ActionError::Forbidden);
        }
        WriteVerdict::DenyRow => {
            tracing::info!(
                action = action_name,
                "write denied: row fails write policy filter"
            );
            return Err(ActionError::Forbidden);
        }
    }

    // 5. Inline insert via the engine.
    deps.action_engine
        .insert_row(&target.table, &columns, &values)
        .await?;

    // 6. Best-effort, type-named lineage. A failure here is logged, NOT fatal (the
    //    documented dangling slice): the snapshot stands even if lineage didn't land.
    let snapshot_id = deps
        .cp
        .catalog()
        .current_snapshot(&target.table)
        .await
        .inspect_err(|e| {
            tracing::warn!(action = action_name, error = %e, "snapshot lookup failed; lineage snapshot_id will be null")
        })
        .ok()
        .map(|s| s.id.0);
    let event = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(&action.target)],
        payload: serde_json::json!({ "action": action_name, "snapshot_id": snapshot_id }),
    };
    if let Err(e) = deps.cp.lineage().emit(event).await {
        tracing::warn!(action = action_name, error = %e, "action lineage emit failed (dangling)");
    }

    // 7. Return the created object: the validated columns + values, with logical types
    //    from the target type's properties (in column order), for typed JSON rendering.
    let logical_types = columns
        .iter()
        .map(|c| {
            target
                .properties
                .iter()
                .find(|p| &p.name == c)
                .map(|p| p.ty.clone())
                .unwrap_or_default()
        })
        .collect();
    Ok(ObjectRows {
        columns,
        logical_types,
        rows: vec![values],
    })
}
