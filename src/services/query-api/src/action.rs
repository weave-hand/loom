//! Action handler logic: invoke a named ontology action to insert one new typed object.
//! Governed by Action::Write; executes an ATOMIC write via the ActionEngine
//! (`write_object`), which commits the row and its lineage event in one transaction,
//! and returns the action's `run_id`.

use control_plane_core::{
    Action, ActionDef, ActionName, ControlPlane, ControlPlaneError, DatasetRef, Decision,
    EventType, LineageEvent, ObjectType, PageReq, PolicyTarget, RunId, SubjectId, resolve_logical,
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
    /// The action's definition does not conform to its target type (a server-side
    /// configuration fault). Carries a descriptive message naming every violation, surfaced
    /// to the operator (distinct from the opaque catch-all 500) so the ActionDef can be fixed.
    #[error("action misconfigured: {0}")]
    Misconfigured(String),
    #[error(transparent)]
    ControlPlane(#[from] ControlPlaneError),
    #[error(transparent)]
    Serving(#[from] crate::serving::ServingError),
}

/// Validate that `action`'s parameters conform to `target`'s properties: every parameter names a
/// real property of a compatible logical type (same `BaseType`), and every required property is
/// covered by a required parameter. Pure; collects ALL violations into one message so an operator
/// sees every problem at once. `Ok(())` if conformant, else `ActionError::Misconfigured`.
pub fn check_conformance(action: &ActionDef, target: &ObjectType) -> Result<(), ActionError> {
    let target_name = &target.name.0;
    let mut violations: Vec<String> = Vec::new();

    // Rules 1 & 2: every param names a real property, of a compatible (same-BaseType) logical type.
    for p in &action.parameters {
        match target.properties.iter().find(|prop| prop.name == p.name) {
            None => violations.push(format!(
                "parameter `{}` matches no property of type `{}`",
                p.name, target_name
            )),
            Some(prop) => {
                let prop_base = resolve_logical(&prop.ty);
                let param_base = resolve_logical(&p.ty);
                if prop_base.is_none() {
                    violations.push(format!(
                        "property `{}` of type `{}` has unknown logical type `{}`",
                        prop.name, target_name, prop.ty
                    ));
                } else if param_base.is_none() {
                    violations.push(format!(
                        "parameter `{}` has unknown logical type `{}`",
                        p.name, p.ty
                    ));
                } else if prop_base != param_base {
                    violations.push(format!(
                        "parameter `{}` type `{}` is incompatible with property `{}` type `{}`",
                        p.name, p.ty, prop.name, prop.ty
                    ));
                }
            }
        }
    }

    // Rule 3: every required property is covered by a required parameter.
    for prop in &target.properties {
        if prop.required {
            match action.parameters.iter().find(|p| p.name == prop.name) {
                None => violations.push(format!(
                    "required property `{}` of type `{}` is not covered by any parameter",
                    prop.name, target_name
                )),
                Some(p) if !p.required => violations.push(format!(
                    "required property `{}` is covered by optional parameter `{}` (it could be omitted, writing NULL)",
                    prop.name, p.name
                )),
                Some(_) => {}
            }
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(ActionError::Misconfigured(format!(
            "action `{}` does not conform to type `{}`: {}",
            action.name.0,
            target_name,
            violations.join("; ")
        )))
    }
}

/// Run one action: insert a new instance of the action's target type from `body`.
/// Returns the created object as a single-row `ObjectRows` plus the `RunId` so the
/// caller can locate the action's lineage (committed atomically with the row).
pub async fn run_action(
    action_name: &str,
    body: &serde_json::Map<String, Value>,
    subject: &SubjectId,
    deps: &ActionDeps<'_>,
) -> Result<(ObjectRows, RunId), ActionError> {
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

    // 3b. Conformance: the action's parameters must mirror the target type's properties (names,
    //     compatible logical types, required-property coverage). A misconfigured ActionDef is
    //     surfaced here as a clear error instead of an opaque insert-time fault. Runs after the
    //     Write gate (no definition-validity leak to unauthorized callers) and before any insert.
    check_conformance(&action, &target)?;

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

    // 5. Expand to the target type's FULL property set (declared order): the parsed
    //    value when the action set the column, else NULL. The loom-owned Parquet
    //    write must carry every column so the file schema matches the table (part-1
    //    relied on DuckDB defaulting unspecified columns to NULL).
    use std::collections::HashMap;
    let parsed: HashMap<&str, &SqlValue> = pairs.iter().map(|(c, v)| (c.as_str(), v)).collect();
    let mut full_columns: Vec<String> = Vec::with_capacity(target.properties.len());
    let mut full_values: Vec<SqlValue> = Vec::with_capacity(target.properties.len());
    let mut full_logical: Vec<String> = Vec::with_capacity(target.properties.len());
    for p in &target.properties {
        full_columns.push(p.name.clone());
        full_values.push(
            parsed
                .get(p.name.as_str())
                .copied()
                .cloned()
                .unwrap_or(SqlValue::Null),
        );
        full_logical.push(p.ty.clone());
    }

    // 6. Mint the run id and build the lineage event UP FRONT, so the caller owns the
    //    run_id and hands it to the engine, which commits row + event atomically.
    //    inputs=[] (a create-from-params action has no upstream datasets). The old
    //    post-hoc snapshot_id payload is dropped: the event now commits WITH the
    //    snapshot, so their linkage is structural, not a best-effort breadcrumb.
    let run_id = RunId(Uuid::new_v4());
    let event = LineageEvent {
        run_id,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(&action.target)],
        payload: serde_json::json!({ "action": action_name }),
    };

    // 7. Atomic write: row + lineage in one transaction (no dangling slice). On any
    //    failure the Tx rolls back — no snapshot, no lineage, no partial state.
    deps.action_engine
        .write_object(
            &target.table,
            &full_columns,
            &full_values,
            &full_logical,
            event,
        )
        .await?;

    // 8. Return the created object (action-provided columns only, as part-1 returns)
    //    plus the run_id so the caller can locate the action's lineage.
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
    Ok((
        ObjectRows {
            columns,
            logical_types,
            rows: vec![values],
        },
        run_id,
    ))
}
