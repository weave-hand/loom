//! Action handler logic: invoke a named ontology action to insert one new typed object.
//! Governed by Action::Write; executes an ATOMIC write via the ActionEngine
//! (`write_object`), which commits the row and its lineage event in one transaction,
//! and returns the action's `run_id`.

use std::collections::BTreeMap;

use control_plane_core::{
    Action, ActionDef, ActionKind, ActionName, ConstraintViolation, ControlPlane,
    ControlPlaneError, DatasetRef, Decision, EventType, LineageEvent, ObjectType, PageReq, Policy,
    PolicyTarget, PropertyValidator, RunId, SubjectId, resolve_logical,
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
    pub serving: &'a dyn crate::serving::ServingEngine,
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
    /// A fine-grained Write policy denied the concrete insert (column or row-filter).
    /// Carries the caller-scoped reason for the structured `403` body; the predicate,
    /// policy id, and role stay server-side (logged only). Distinct from the unit
    /// `Forbidden`, which is the coarse Write-gate denial.
    #[error("write denied")]
    WriteDenied(WriteDenialReason),
    /// One or more inserted values violate their property's declared constraints. Carries
    /// every violation (property + rule) for the structured `422` body. Distinct from the
    /// `403` ACL `WriteDenied` — a constraint violation is malformed data, not a denial.
    #[error("constraint violation")]
    ConstraintViolation(Vec<ConstraintViolation>),
    /// The targeted object does not exist (no live row for the supplied identity).
    #[error("object not found")]
    NotFound,
    /// The mutation is unsupported for this target type (e.g. a vector-bearing type,
    /// which the scalar copy-on-write path cannot rewrite without dropping vectors).
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error(transparent)]
    ControlPlane(#[from] ControlPlaneError),
    #[error(transparent)]
    Serving(#[from] crate::serving::ServingError),
}

/// The caller-scoped reason a fine-grained Write policy denied an insert, rendered
/// into the structured `403` body. Discloses only what the caller already supplied
/// (the offending column name) — never the `row_filter` predicate, policy id, or
/// role, which stay server-side (logged only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteDenialReason {
    /// An inserted column is denied by a Write policy. Names the column (the caller
    /// supplied it, so naming it discloses nothing new).
    Column(String),
    /// The inserted row fails a Write policy's `row_filter`. The predicate itself
    /// is never disclosed.
    RowFilter,
}

impl WriteDenialReason {
    /// Map a `WriteVerdict` to its caller-scoped reason. `Allow` has no reason
    /// (`None`); a denying verdict maps to the matching `WriteDenialReason`.
    pub fn from_verdict(verdict: WriteVerdict) -> Option<Self> {
        match verdict {
            WriteVerdict::Allow => None,
            WriteVerdict::DenyColumn(col) => Some(WriteDenialReason::Column(col)),
            WriteVerdict::DenyRow => Some(WriteDenialReason::RowFilter),
        }
    }

    /// The caller-scoped `403` JSON body: a stable machine-readable
    /// `error: "write_denied"` tag plus `reason` (`"column"` | `"row_filter"`) and,
    /// for column denials, the offending `column`.
    pub fn to_body(&self) -> serde_json::Value {
        match self {
            WriteDenialReason::Column(col) => serde_json::json!({
                "error": "write_denied",
                "reason": "column",
                "column": col,
            }),
            WriteDenialReason::RowFilter => serde_json::json!({
                "error": "write_denied",
                "reason": "row_filter",
            }),
        }
    }
}

/// Validate that `action`'s parameters conform to `target`'s properties. Dispatches on
/// `action.kind`: Insert enforces full required-property coverage; Update/Delete enforce
/// identity-based mutate rules. Pure; collects ALL violations into one message so an operator
/// sees every problem at once. `Ok(())` if conformant, else `ActionError::Misconfigured`.
pub fn check_conformance(action: &ActionDef, target: &ObjectType) -> Result<(), ActionError> {
    use control_plane_core::ActionKind;
    match action.kind {
        ActionKind::Insert => check_insert_conformance(action, target),
        ActionKind::Update => check_mutate_conformance(action, target, true),
        ActionKind::Delete => check_mutate_conformance(action, target, false),
    }
}

/// Rules 1 & 2 (shared): every param's BOUND property (`binds`, else its own name) is a real
/// property of a compatible (same-BaseType) logical type. Violations are appended to
/// `violations`.
fn check_param_property_types(
    action: &ActionDef,
    target: &ObjectType,
    violations: &mut Vec<String>,
) {
    let target_name = &target.name.0;
    for p in &action.parameters {
        let bound = p.binds_property();
        match target.properties.iter().find(|prop| prop.name == bound) {
            None if p.binds.is_none() => violations.push(format!(
                "parameter `{}` matches no property of type `{}`",
                p.name, target_name
            )),
            None => violations.push(format!(
                "parameter `{}` binds property `{bound}`, which is not a property of type `{}`",
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
}

/// Rule 2b & 3 (shared): every constant assignment names a real property and coerces to that
/// property's logical type, and no property is written twice — by two params, a param and a
/// constant, or two constants. Violations are appended to `violations`.
fn check_assignments_and_binds(
    action: &ActionDef,
    target: &ObjectType,
    violations: &mut Vec<String>,
) {
    let target_name = &target.name.0;
    let mut bound: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let dup = |prop: &str| {
        format!("property `{prop}` of type `{target_name}` is written by more than one parameter/constant")
    };
    for p in &action.parameters {
        let prop = p.binds_property();
        if !bound.insert(prop) {
            violations.push(dup(prop));
        }
    }
    for a in &action.assignments {
        match target.properties.iter().find(|prop| prop.name == a.property) {
            None => violations.push(format!(
                "constant assignment names property `{}`, which is not a property of type `{target_name}`",
                a.property
            )),
            Some(prop) => {
                if let Err(e) = crate::params::validate_const(&a.property, &prop.ty, &a.value) {
                    violations.push(format!("constant for property `{}`: {e}", a.property));
                }
            }
        }
        if !bound.insert(&a.property) {
            violations.push(dup(&a.property));
        }
    }
}

/// INSERT conformance: rules 1 & 2 (param/constant name+type), rule 2b/3 (constants +
/// no double-bind), plus rule 4: every required property is covered by exactly one of
/// {a required param binding it, a constant assignment}.
fn check_insert_conformance(action: &ActionDef, target: &ObjectType) -> Result<(), ActionError> {
    let target_name = &target.name.0;
    let mut violations: Vec<String> = Vec::new();

    check_param_property_types(action, target, &mut violations);
    check_assignments_and_binds(action, target, &mut violations);

    // Rule 4: every required property is covered by a required param binding it, or a constant.
    for prop in &target.properties {
        if !prop.required {
            continue;
        }
        let by_required_param = action
            .parameters
            .iter()
            .any(|p| p.binds_property() == prop.name && p.required);
        let by_constant = action.assignments.iter().any(|a| a.property == prop.name);
        if by_required_param || by_constant {
            continue;
        }
        if let Some(p) = action
            .parameters
            .iter()
            .find(|p| p.binds_property() == prop.name && !p.required)
        {
            violations.push(format!(
                "required property `{}` is covered by optional parameter `{}` (it could be omitted, writing NULL)",
                prop.name, p.name
            ));
        } else {
            violations.push(format!(
                "required property `{}` of type `{target_name}` is not covered by any parameter or constant",
                prop.name
            ));
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(ActionError::Misconfigured(format!(
            "action `{}` does not conform to type `{target_name}`: {}",
            action.name.0,
            violations.join("; ")
        )))
    }
}

/// UPDATE/DELETE conformance. Both require a declared `identity` on the target and a required
/// parameter naming it; every parameter must name a real property of compatible type.
/// DELETE takes ONLY the identity parameter (no extras). UPDATE relaxes required-property
/// coverage (PATCH semantics) — only the supplied params are validated.
fn check_mutate_conformance(
    action: &ActionDef,
    target: &ObjectType,
    is_update: bool,
) -> Result<(), ActionError> {
    let target_name = &target.name.0;
    let mut violations: Vec<String> = Vec::new();

    check_param_property_types(action, target, &mut violations);
    check_assignments_and_binds(action, target, &mut violations);

    match &target.identity {
        None => violations.push(format!(
            "type `{target_name}` has no declared identity; UPDATE/DELETE require one"
        )),
        Some(idprop) => {
            match action
                .parameters
                .iter()
                .find(|p| p.binds_property() == idprop)
            {
                None => violations.push(format!(
                    "UPDATE/DELETE on `{target_name}` requires a parameter for the identity property `{idprop}`"
                )),
                Some(p) if !p.required => violations.push(format!(
                    "identity parameter `{idprop}` must be required"
                )),
                Some(_) => {}
            }
            if !is_update {
                for p in &action.parameters {
                    if p.binds_property() != idprop {
                        violations.push(format!(
                            "DELETE on `{target_name}` takes only the identity parameter; `{}` is extra",
                            p.name
                        ));
                    }
                }
                if !action.assignments.is_empty() {
                    violations.push(format!(
                        "DELETE on `{target_name}` takes no constant assignments"
                    ));
                }
            }
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(ActionError::Misconfigured(format!(
            "action `{}` does not conform to type `{target_name}`: {}",
            action.name.0,
            violations.join("; ")
        )))
    }
}

/// Run one named action against its target type from `body`. Dispatches on the action's
/// `kind`: `Insert` creates a new instance; `Update`/`Delete` mutate or remove one existing
/// instance located by the target type's declared identity (whole-table copy-on-write).
/// Returns the affected object as a single-row `ObjectRows` plus the `RunId` so the caller
/// can locate the action's lineage (committed atomically with the new snapshot).
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
    //     compatible logical types, required-property/identity coverage). A misconfigured ActionDef
    //     is surfaced here as a clear error instead of an opaque write-time fault. Runs after the
    //     Write gate (no definition-validity leak to unauthorized callers) and before any write.
    check_conformance(&action, &target)?;

    // 4. Dispatch on the mutation kind. The coarse Write gate + conformance above are shared;
    //    the fine-grained write policy and the actual write differ per kind.
    match action.kind {
        ActionKind::Insert => run_insert(&action, &target, body, subject, deps).await,
        ActionKind::Update => run_mutate(&action, &target, body, subject, deps, true).await,
        ActionKind::Delete => run_mutate(&action, &target, body, subject, deps, false).await,
    }
}

/// INSERT: parse the body into a new row, gate it through the fine-grained Write policy
/// (deny-column over the set columns + row-filter on the inserted row), then atomically
/// commit the row and its lineage event via `write_object`.
async fn run_insert(
    action: &ActionDef,
    target: &ObjectType,
    body: &serde_json::Map<String, Value>,
    subject: &SubjectId,
    deps: &ActionDeps<'_>,
) -> Result<(ObjectRows, RunId), ActionError> {
    let action_name = action.name.0.as_str();
    let policy_target = PolicyTarget::Type(action.target.clone());

    // 4. Parse + validate the typed params (ordered by the action's parameter list).
    let pairs = parse_params(&action.parameters, body)?;
    let columns: Vec<String> = pairs.iter().map(|(c, _)| c.clone()).collect();
    let values: Vec<SqlValue> = pairs.iter().map(|(_, v)| v.clone()).collect();

    // 4b. Fine-grained Write policy: deny-write-column + row-filter-on-insert. The
    //     subject already cleared the coarse Write gate; now enforce the row/column
    //     policy against the concrete row. Fail-closed (deny on UNKNOWN). A denial
    //     maps to a structured `WriteDenied` reason rendered into the 403 body
    //     (caller-scoped: column name only, never the predicate), and is still
    //     logged server-side.
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
    let verdict =
        write_filter::check_write_policy(&write_policies.items, &set_columns, &set_values);
    if let Some(reason) = WriteDenialReason::from_verdict(verdict) {
        match &reason {
            WriteDenialReason::Column(col) => {
                tracing::info!(action = action_name, column = %col, "write denied: policy denies column");
            }
            WriteDenialReason::RowFilter => {
                tracing::info!(
                    action = action_name,
                    "write denied: row fails write policy filter"
                );
            }
        }
        return Err(ActionError::WriteDenied(reason));
    }

    // 4c. Per-value constraint validation: reject values violating their property's
    //     declared constraints with a structured 422 (distinct from the 403 ACL denial).
    //     An omitted optional (NULL) carries no value to check. The same `core` validator
    //     drives the ingest land path, so both write paths enforce identical rules.
    let mut cviol: Vec<ConstraintViolation> = Vec::new();
    for (col, val) in &pairs {
        let Some(prop) = target.properties.iter().find(|p| &p.name == col) else {
            continue;
        };
        if prop.constraints.is_empty() {
            continue;
        }
        let validator = PropertyValidator::new(prop)?;
        match val {
            SqlValue::Text(s) => validator.check_str(s, &mut cviol),
            #[expect(
                clippy::cast_precision_loss,
                reason = "range bounds are f64; i64->f64 is acceptable for validation"
            )]
            SqlValue::Int(i) => validator.check_num(*i as f64, &mut cviol),
            SqlValue::Double(d) => validator.check_num(*d, &mut cviol),
            SqlValue::Bool(_) | SqlValue::Date(_) | SqlValue::Timestamp(_) | SqlValue::Null => {}
        }
    }
    if !cviol.is_empty() {
        tracing::info!(
            action = action_name,
            count = cviol.len(),
            "insert rejected: constraint violation"
        );
        return Err(ActionError::ConstraintViolation(cviol));
    }

    // 5. Expand to the target type's FULL property set (declared order): the parsed
    //    value when the action set the column, else NULL. The loom-owned Parquet
    //    write must carry every column so the file schema matches the table (part-1
    //    unspecified columns default to NULL).
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

/// Reject UPDATE/DELETE on a type with any vector property: the scalar copy-on-write
/// read/write path cannot represent list columns, so a whole-table rewrite would drop
/// every other row's vectors (data loss). Lifted when an Arrow-native COW read leg lands.
fn ensure_cow_supported(target: &ObjectType) -> Result<(), ActionError> {
    for p in &target.properties {
        if let Some(control_plane_core::BaseType::Vector(_)) =
            control_plane_core::resolve_logical(&p.ty)
        {
            return Err(ActionError::Unsupported(format!(
                "UPDATE/DELETE not supported on type `{}`: it has a vector column (`{}`)",
                target.name.0, p.name
            )));
        }
    }
    Ok(())
}

/// `SELECT "c1", "c2", ... FROM "schema"."table"` over all properties (identifiers
/// double-quoted, embedded quotes doubled). The privileged, ACL-unfiltered full-table
/// read backing copy-on-write. Column order = property order.
fn select_all_sql(target: &ObjectType) -> String {
    let q = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let cols = target
        .properties
        .iter()
        .map(|p| q(&p.name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {cols} FROM {}.{}",
        q(&target.table.schema),
        q(&target.table.name)
    )
}

/// Does the candidate row pass EVERY policy's `row_filter`? Evaluates ONLY the row-filter
/// leg (via [`write_filter::eval`]) against a FULL row — all property columns aligned to
/// their values — so unset columns keep their existing values rather than reading as NULL.
/// Deliberately NOT `check_write_policy`, which would also run deny-column over all columns
/// (correct for INSERT, wrong for a whole-row mutate verdict). A policy with no `row_filter`
/// adds no constraint; no policies ⇒ admitted.
fn row_filter_admits(policies: &[Policy], columns: &[String], values: &[SqlValue]) -> bool {
    let row: BTreeMap<&str, &SqlValue> = columns
        .iter()
        .map(|c| c.as_str())
        .zip(values.iter())
        .collect();
    policies.iter().all(|p| match &p.row_filter {
        Some(f) => write_filter::eval(f, &row) == Some(true),
        None => true,
    })
}

/// UPDATE/DELETE via whole-table copy-on-write. Locates the row by the target type's
/// declared identity through a privileged (ACL-unfiltered) full-table read, applies the
/// mutation in memory (DELETE drops the row; UPDATE PATCHes the named non-identity columns),
/// governs the affected row(s), then atomically rewrites the full live row set + lineage via
/// `overwrite_table`. Returns the affected object (UPDATE: the new version; DELETE: the
/// removed row) + run id. `NotFound` if no live row matches the supplied identity.
async fn run_mutate(
    action: &ActionDef,
    target: &ObjectType,
    body: &serde_json::Map<String, Value>,
    subject: &SubjectId,
    deps: &ActionDeps<'_>,
    is_update: bool,
) -> Result<(ObjectRows, RunId), ActionError> {
    ensure_cow_supported(target)?;
    let action_name = action.name.0.as_str();
    let policy_target = PolicyTarget::Type(action.target.clone());

    // The identity property + the supplied identity value (parsed/typed via the params).
    let idprop = target.identity.clone().ok_or_else(|| {
        ActionError::Misconfigured(format!("type `{}` has no declared identity", target.name.0))
    })?;
    let pairs = parse_params(&action.parameters, body)?;
    let id_value = pairs
        .iter()
        .find(|(c, _)| c == &idprop)
        .map(|(_, v)| v.clone())
        .ok_or_else(|| {
            ActionError::Misconfigured(format!("missing identity parameter `{idprop}`"))
        })?;

    // The target type's full ordered property set (column names + logical types).
    let columns: Vec<String> = target.properties.iter().map(|p| p.name.clone()).collect();
    let logical: Vec<String> = target.properties.iter().map(|p| p.ty.clone()).collect();
    let id_idx = columns.iter().position(|c| c == &idprop).ok_or_else(|| {
        ActionError::Misconfigured(format!("identity `{idprop}` is not a property"))
    })?;

    // Privileged full-table read (ACL-unfiltered): COW must see every live row to rewrite
    // the table without dropping rows the caller cannot read.
    let live = deps
        .serving
        .fetch_rows(&select_all_sql(target), &[])
        .await?;

    // Locate the target row. The identity is a primary key, so at most one live match.
    let mut matches = live
        .rows
        .iter()
        .enumerate()
        .filter(|(_, r)| r.get(id_idx) == Some(&id_value))
        .map(|(i, _)| i);
    let target_idx = match matches.next() {
        None => return Err(ActionError::NotFound),
        Some(i) => i,
    };
    if matches.next().is_some() {
        // >1 live row for a primary key is a corrupt invariant, not a client error.
        return Err(ActionError::ControlPlane(ControlPlaneError::Backend(
            format!("identity `{idprop}` matches more than one live row").into(),
        )));
    }
    let existing = live
        .rows
        .get(target_idx)
        .cloned()
        .ok_or_else(|| ActionError::NotFound)?;

    // The PATCH columns the caller actually SET (non-identity, non-null). An omitted optional
    // param materializes as `SqlValue::Null`; PATCH semantics leave it untouched, so it is
    // excluded both from the column-denial check and from the in-memory overwrite.
    let set_pairs: Vec<(String, SqlValue)> = pairs
        .iter()
        .filter(|(c, v)| c != &idprop && !matches!(v, SqlValue::Null))
        .cloned()
        .collect();

    // Compute the resulting row (UPDATE = existing with the SET columns overwritten).
    let new_row: Option<Vec<SqlValue>> = is_update.then(|| {
        let mut row = existing.clone();
        for (col, val) in &set_pairs {
            if let Some(ci) = columns.iter().position(|c| c == col)
                && let Some(slot) = row.get_mut(ci)
            {
                *slot = val.clone();
            }
        }
        row
    });

    // Fine-grained Write policy on the affected row(s), with the legs isolated (a whole-row
    // mutate verdict, not the INSERT gate).
    let write_policies = deps
        .cp
        .acl()
        .policies_for(subject, Action::Write, &policy_target, PageReq::unbounded())
        .await?;
    let policies = &write_policies.items;

    // The existing row must pass every row-filter (both UPDATE and DELETE).
    if !row_filter_admits(policies, &columns, &existing) {
        tracing::info!(
            action = action_name,
            "mutate denied: existing row fails write policy filter"
        );
        return Err(ActionError::WriteDenied(WriteDenialReason::RowFilter));
    }
    if let Some(row) = &new_row {
        // Deny-column over the SET columns only (UPDATE writes those columns).
        if let Some(col) = set_pairs.iter().map(|(c, _)| c).find(|c| {
            policies
                .iter()
                .any(|p| p.deny_columns.iter().any(|d| d == *c))
        }) {
            tracing::info!(action = action_name, column = %col, "update write denied: policy denies column");
            return Err(ActionError::WriteDenied(WriteDenialReason::Column(
                col.clone(),
            )));
        }
        // The resulting row must also pass every row-filter.
        if !row_filter_admits(policies, &columns, row) {
            tracing::info!(
                action = action_name,
                "update denied: resulting row fails write policy filter"
            );
            return Err(ActionError::WriteDenied(WriteDenialReason::RowFilter));
        }
    }

    // Build the new full live set: existing rows minus the target (DELETE) or with the
    // target replaced by its new version (UPDATE).
    let mut rows: Vec<Vec<SqlValue>> = live.rows.clone();
    match &new_row {
        Some(row) => {
            if let Some(slot) = rows.get_mut(target_idx) {
                slot.clone_from(row);
            }
        }
        None => {
            if target_idx < rows.len() {
                rows.remove(target_idx);
            }
        }
    }

    // Lineage + atomic copy-on-write commit.
    let run_id = RunId(Uuid::new_v4());
    let op = if is_update { "update" } else { "delete" };
    let event = LineageEvent {
        run_id,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![DatasetRef::from(&action.target)],
        outputs: vec![DatasetRef::from(&action.target)],
        payload: serde_json::json!({ "action": action_name, "op": op }),
    };
    deps.action_engine
        .overwrite_table(&target.table, &columns, &rows, &logical, event)
        .await?;

    // Return the affected object (UPDATE: the new version; DELETE: the removed values).
    let returned = new_row.unwrap_or(existing);
    Ok((
        ObjectRows {
            columns,
            logical_types: logical,
            rows: vec![returned],
        },
        run_id,
    ))
}
