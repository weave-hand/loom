//! Action handler logic: invoke a named ontology action to insert one new typed object.
//! Governed by Action::Write; executes an ATOMIC write via the ActionEngine
//! (`write_object`), which commits the row and its lineage event in one transaction,
//! and returns the action's `run_id`.

use std::collections::BTreeMap;

use control_plane_core::{
    Action, ActionDef, ActionKind, ActionName, ActionStep, ConstraintViolation, ControlPlane,
    ControlPlaneError, DatasetRef, Decision, EventType, LineageEvent, ObjectType, PageReq, Policy,
    PolicyTarget, PropertyDef, PropertyValidator, RunId, SubjectId, resolve_logical,
};
use serde_json::Value;
use uuid::Uuid;

use crate::governed::Projection;
use crate::handler::ObjectRows;
use crate::params::ParamError;
use crate::serving::{ActionEngine, ServingError, SqlValue};
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

/// The request-time clock threaded to `resolve_action_row` for `now()` in computed assignments.
fn request_now() -> time::PrimitiveDateTime {
    let n = time::OffsetDateTime::now_utc();
    time::PrimitiveDateTime::new(n.date(), n.time())
}

/// The cross-step binding environment: each earlier bound step's `bind` name mapped to the set
/// of property names on that step's target, used to validate a `StepRef { bind, prop }`.
type BoundBinds = std::collections::BTreeMap<String, std::collections::HashSet<String>>;

/// Validate that a single-step `action`'s parameters conform to `target`'s properties, against an
/// EMPTY cross-step binding environment — the single-step convenience entry (checks only the
/// first step). Dispatches on the step's kind: Insert enforces full required-property coverage;
/// Update/Delete enforce identity-based mutate rules. Multi-step actions are validated via
/// [`check_conformance_steps`], which resolves every step's target and threads the growing bind
/// environment. Pure; collects ALL violations into one message. `Ok(())` if conformant, else
/// `ActionError::Misconfigured`.
pub fn check_conformance(action: &ActionDef, target: &ObjectType) -> Result<(), ActionError> {
    let step = action
        .steps
        .first()
        .ok_or_else(|| ActionError::Misconfigured("action has no steps".into()))?;
    let bound_binds = BoundBinds::new();
    check_step(step, target, &bound_binds, &action.name.0)
}

/// Validate every step of `action` against its resolved `target` (aligned by index to
/// `action.steps`), threading a growing set of `bind -> that step's target property-set`. Each
/// step runs the single-step conformance checks against its OWN target; a `StepRef { bind, prop }`
/// assignment additionally requires `bind` to name a strictly-earlier bound step and `prop` to be
/// a real property of that step's target. A step's own `bind` is added to the environment only
/// AFTER its checks, so a self-reference is rejected. Pure. `Ok(())` if every step conforms, else
/// the first non-conforming step's `ActionError::Misconfigured`.
pub fn check_conformance_steps(
    action: &ActionDef,
    targets: &[ObjectType],
) -> Result<(), ActionError> {
    if action.steps.len() != targets.len() {
        return Err(ActionError::Misconfigured(format!(
            "action `{}` has {} steps but {} resolved targets",
            action.name.0,
            action.steps.len(),
            targets.len()
        )));
    }
    let mut bound_binds = BoundBinds::new();
    for (step, target) in action.steps.iter().zip(targets) {
        check_step(step, target, &bound_binds, &action.name.0)?;
        if let Some(b) = &step.bind {
            bound_binds.insert(
                b.clone(),
                target.properties.iter().map(|p| p.name.clone()).collect(),
            );
        }
    }
    Ok(())
}

/// Dispatch one step's conformance on its `kind` against `target`, with the cross-step binding
/// environment `bound_binds` (bind -> that step's target property-set) available for `StepRef`
/// validation. `action_name` is threaded only for the violation message.
fn check_step(
    step: &ActionStep,
    target: &ObjectType,
    bound_binds: &BoundBinds,
    action_name: &str,
) -> Result<(), ActionError> {
    match step.kind {
        ActionKind::Insert => check_insert_conformance(step, target, bound_binds, action_name),
        ActionKind::Update => {
            check_mutate_conformance(step, target, true, bound_binds, action_name)
        }
        ActionKind::Delete => {
            check_mutate_conformance(step, target, false, bound_binds, action_name)
        }
    }
}

/// Rules 1 & 2 (shared): every param's BOUND property (`binds`, else its own name) is a real
/// property of a compatible (same-BaseType) logical type. Violations are appended to
/// `violations`.
fn check_param_property_types(
    step: &ActionStep,
    target: &ObjectType,
    violations: &mut Vec<String>,
) {
    let target_name = &target.name.0;
    for p in &step.parameters {
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

/// A `crate::expr::TypeEnv` view over an action's params and the target's properties, tracking
/// which properties have been resolved *earlier* in declared assignment order (for the
/// forward-`@ref` rule). Params seed the resolved set (they are resolved before any assignment).
struct ConformanceEnv<'a> {
    step: &'a ActionStep,
    target: &'a ObjectType,
    resolved: std::collections::HashSet<String>,
}

impl crate::expr::TypeEnv for ConformanceEnv<'_> {
    fn param_type(&self, name: &str) -> Option<control_plane_core::BaseType> {
        self.step
            .parameters
            .iter()
            .find(|p| p.name == name)
            .and_then(|p| control_plane_core::resolve_logical(&p.ty))
    }
    fn prop_type(&self, name: &str) -> Option<control_plane_core::BaseType> {
        self.target
            .properties
            .iter()
            .find(|p| p.name == name)
            .and_then(|p| control_plane_core::resolve_logical(&p.ty))
    }
    fn prop_resolved(&self, name: &str) -> bool {
        self.resolved.contains(name)
    }
}

/// Parse + type-check one expression assignment against `env`, checking the inferred type is
/// assignable to `prop`. Appends a clear violation per failure mode.
fn check_expr_assignment(
    src: &str,
    prop: &PropertyDef,
    env: &ConformanceEnv<'_>,
    target_name: &str,
    property: &str,
    violations: &mut Vec<String>,
) {
    let expr = match crate::expr::parse_expr(src) {
        Ok(e) => e,
        Err(e) => {
            violations.push(format!("expression for property `{property}`: {e}"));
            return;
        }
    };
    let inferred = match crate::expr::typecheck(&expr, env) {
        Ok(t) => t,
        Err(e) => {
            violations.push(format!("expression for property `{property}`: {e}"));
            return;
        }
    };
    let Some(prop_base) = control_plane_core::resolve_logical(&prop.ty) else {
        violations.push(format!(
            "property `{property}` of type `{target_name}` has unknown logical type `{}`",
            prop.ty
        ));
        return;
    };
    if !crate::expr::assignable(inferred, prop_base) {
        violations.push(format!(
            "expression for property `{property}` has type {} which is not assignable to `{}`",
            inferred.canonical_name(),
            prop.ty
        ));
    }
}

/// Rule 2b & 3 (shared): every constant assignment names a real property and coerces to that
/// property's logical type, every Expr assignment type-checks against the action's params and
/// earlier-resolved properties (in declared order), and no property is written twice — by two
/// params, a param and an assignment, or two assignments. Violations are appended to
/// `violations`.
fn check_assignments_and_binds(
    step: &ActionStep,
    target: &ObjectType,
    bound_binds: &BoundBinds,
    violations: &mut Vec<String>,
) {
    let target_name = &target.name.0;
    let mut written: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let dup = |prop: &str| {
        format!(
            "property `{prop}` of type `{target_name}` is written by more than one parameter/constant"
        )
    };
    for p in &step.parameters {
        let prop = p.binds_property();
        if !written.insert(prop) {
            violations.push(dup(prop));
        }
    }

    // Seed the resolved-property set with everything a param binds (params resolve before any
    // assignment); assignments then resolve in declared order.
    let mut env = ConformanceEnv {
        step,
        target,
        resolved: step
            .parameters
            .iter()
            .map(|p| p.binds_property().to_string())
            .collect(),
    };

    for a in &step.assignments {
        match target
            .properties
            .iter()
            .find(|prop| prop.name == a.property)
        {
            None => violations.push(format!(
                "assignment names property `{}`, which is not a property of type `{target_name}`",
                a.property
            )),
            Some(prop) => match &a.source {
                control_plane_core::AssignmentSource::Const(v) => {
                    if let Err(e) = crate::params::validate_const(&a.property, &prop.ty, v) {
                        violations.push(format!("constant for property `{}`: {e}", a.property));
                    }
                }
                control_plane_core::AssignmentSource::Expr(src) => {
                    check_expr_assignment(src, prop, &env, target_name, &a.property, violations);
                }
                control_plane_core::AssignmentSource::StepRef {
                    bind,
                    prop: ref_prop,
                } => match bound_binds.get(bind) {
                    None => violations.push(format!(
                        "assignment for property `{}` references step `{bind}`, which is not a strictly-earlier bound step",
                        a.property
                    )),
                    Some(props) if !props.contains(ref_prop) => violations.push(format!(
                        "assignment for property `{}` references property `{ref_prop}` of step `{bind}`, which is not a property of that step's target",
                        a.property
                    )),
                    Some(_) => {}
                },
            },
        }
        if !written.insert(&a.property) {
            violations.push(dup(&a.property));
        }
        // This property is now resolved for any later `@ref`.
        env.resolved.insert(a.property.clone());
    }
}

/// INSERT conformance: rules 1 & 2 (param/constant name+type), rule 2b/3 (constants +
/// no double-bind), plus rule 4: every required property is covered by exactly one of
/// {a required param binding it, a constant assignment}.
fn check_insert_conformance(
    step: &ActionStep,
    target: &ObjectType,
    bound_binds: &BoundBinds,
    action_name: &str,
) -> Result<(), ActionError> {
    let target_name = &target.name.0;
    let mut violations: Vec<String> = Vec::new();

    check_param_property_types(step, target, &mut violations);
    check_assignments_and_binds(step, target, bound_binds, &mut violations);

    // Rule 4: every required property is covered by a required param binding it, or a constant.
    for prop in &target.properties {
        if !prop.required {
            continue;
        }
        let by_required_param = step
            .parameters
            .iter()
            .any(|p| p.binds_property() == prop.name && p.required);
        let by_assignment = step.assignments.iter().any(|a| a.property == prop.name);
        if by_required_param || by_assignment {
            continue;
        }
        if let Some(p) = step
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
            "action `{action_name}` does not conform to type `{target_name}`: {}",
            violations.join("; ")
        )))
    }
}

/// UPDATE/DELETE conformance. Both require a declared `identity` on the target and a required
/// parameter naming it; every parameter must name a real property of compatible type.
/// DELETE takes ONLY the identity parameter (no extras). UPDATE relaxes required-property
/// coverage (PATCH semantics) — only the supplied params are validated.
fn check_mutate_conformance(
    step: &ActionStep,
    target: &ObjectType,
    is_update: bool,
    bound_binds: &BoundBinds,
    action_name: &str,
) -> Result<(), ActionError> {
    let target_name = &target.name.0;
    let mut violations: Vec<String> = Vec::new();

    check_param_property_types(step, target, &mut violations);
    check_assignments_and_binds(step, target, bound_binds, &mut violations);

    match &target.identity {
        None => violations.push(format!(
            "type `{target_name}` has no declared identity; UPDATE/DELETE require one"
        )),
        Some(idprop) => {
            match step
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
                for p in &step.parameters {
                    if p.binds_property() != idprop {
                        violations.push(format!(
                            "DELETE on `{target_name}` takes only the identity parameter; `{}` is extra",
                            p.name
                        ));
                    }
                }
                if !step.assignments.is_empty() {
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
            "action `{action_name}` does not conform to type `{target_name}`: {}",
            violations.join("; ")
        )))
    }
}

/// Run one named action against its target type from `body`. Dispatches on the action's
/// `kind`: `Insert` creates a new instance; `Update`/`Delete` mutate or remove one existing
/// instance located by the target type's declared identity (O(change) inline-delta
/// copy-on-write). Returns the affected object as a single-row `ObjectRows` plus the `RunId`
/// so the caller can locate the action's lineage (committed atomically with the new snapshot).
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

    // 2. Resolve EVERY step's target type up front (for their tables + property logical types,
    //    and so cross-step conformance can see each step's target). A missing target here is a
    //    broken ActionDef (internal inconsistency), not a client error — propagate as a
    //    ControlPlane fault (-> 500), not a 404. The sole/first step still drives the coarse gate
    //    and single-step execution below (Task 5 iterates every step for execution).
    let mut targets: Vec<ObjectType> = Vec::with_capacity(action.steps.len());
    for step in &action.steps {
        targets.push(deps.cp.ontology().get_type(&step.target).await?);
    }
    let step = action
        .steps
        .first()
        .ok_or_else(|| ActionError::Misconfigured("action has no steps".into()))?;
    let target = targets
        .first()
        .ok_or_else(|| ActionError::Misconfigured("action has no steps".into()))?
        .clone();

    // 3. Govern: deny-by-default Write on the target type. First live use of Action::Write.
    let policy_target = PolicyTarget::Type(step.target.clone());
    if deps
        .cp
        .acl()
        .check(subject, Action::Write, &policy_target)
        .await?
        == Decision::Deny
    {
        return Err(ActionError::Forbidden);
    }

    // 3b. Conformance: each step's parameters/assignments must mirror ITS target type's properties
    //     (names, compatible logical types, required-property/identity coverage), and every
    //     `StepRef` must reference a strictly-earlier bound step's real property. A misconfigured
    //     ActionDef is surfaced here as a clear error instead of an opaque write-time fault. Runs
    //     after the Write gate (no definition-validity leak to unauthorized callers), before any write.
    check_conformance_steps(&action, &targets)?;

    // 4. Dispatch on the mutation kind. The coarse Write gate + conformance above are shared;
    //    the fine-grained write policy and the actual write differ per kind.
    match step.kind {
        ActionKind::Insert => run_insert(&action, &target, body, subject, deps).await,
        ActionKind::Update => run_mutate(&action, &target, body, subject, deps, true).await,
        ActionKind::Delete => run_mutate(&action, &target, body, subject, deps, false).await,
    }
}

/// Collect every declared-constraint violation over resolved `(property, value)` write
/// pairs, using the same `core` [`PropertyValidator`] that gates the ingest land path —
/// both write paths enforce identical rules. A NULL cell carries no value to check
/// (omitted optionals pass); a pair naming no property is skipped (conformance rejects
/// that shape upstream); a property with no declared constraints is skipped. Pure; the
/// unit-test seam for the constraint phase.
///
/// NOTE: named `value_constraint_violations`, not the register's `validate_constraints` —
/// that name is `control_plane_core::validate_constraints`, the define-time *declaration*
/// validator, and shadowing it here would invite exactly the wrong import.
pub fn value_constraint_violations(
    target: &ObjectType,
    pairs: &[(String, SqlValue)],
) -> Result<Vec<ConstraintViolation>, ActionError> {
    let mut cviol: Vec<ConstraintViolation> = Vec::new();
    for (col, val) in pairs {
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
    Ok(cviol)
}

/// Expand resolved write pairs to the target type's FULL property set (declared order):
/// the parsed value when the action set the column, else NULL. The loom-owned Parquet
/// write must carry every column so the file schema matches the table (part-1
/// unspecified columns default to NULL). Returns the parallel
/// `(columns, values, logical_types)`. Pure; the unit-test seam for the expansion phase.
pub fn expand_to_full_row(
    target: &ObjectType,
    pairs: &[(String, SqlValue)],
) -> (Vec<String>, Vec<SqlValue>, Vec<String>) {
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
    (full_columns, full_values, full_logical)
}

/// The shared response epilogue of both write paths: the affected object as a
/// single-row `ObjectRows`, its logical types zipped per column via the governance
/// layer's [`Projection`] (`of_columns` reuses `prop_ty`; an unknown column zips to
/// `""` exactly as the old inline lookups did). INSERT echoes the action-provided
/// columns; UPDATE/DELETE echo the full property set.
pub fn affected_object(
    target: &ObjectType,
    columns: Vec<String>,
    row: Vec<SqlValue>,
) -> ObjectRows {
    Projection::of_columns(target, columns).object_rows(vec![row])
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
    let step = action
        .steps
        .first()
        .ok_or_else(|| ActionError::Misconfigured("action has no steps".into()))?;
    let policy_target = PolicyTarget::Type(step.target.clone());

    // 4. Resolve the write row: parse+validate the typed params, remap each to its bound
    //    property, and append the action's constant assignments (property-keyed pairs). The
    //    single-step path has no prior-step bindings, so the step env is empty (Task 5 populates
    //    it across steps).
    let now = request_now();
    let step_env = crate::params::StepEnv::new();
    let pairs = crate::params::resolve_action_row(action, target, body, now, &step_env)?;
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
    let cviol = value_constraint_violations(target, &pairs)?;
    if !cviol.is_empty() {
        tracing::info!(
            action = action_name,
            count = cviol.len(),
            "insert rejected: constraint violation"
        );
        return Err(ActionError::ConstraintViolation(cviol));
    }

    // 5. Expand to the target type's FULL property set (declared order).
    let (full_columns, full_values, full_logical) = expand_to_full_row(target, &pairs);

    // 6. Mint the run id and build the lineage event UP FRONT, so the caller owns the
    //    run_id and hands it to the engine, which commits row + event atomically.
    //    inputs=[] (a create-from-params action has no upstream datasets). The old
    //    post-hoc snapshot_id payload is dropped: the event now commits WITH the
    //    snapshot, so their linkage is structural, not a best-effort breadcrumb.
    let run_id = RunId(Uuid::new_v4());
    let event = LineageEvent::completed_with_run(
        run_id,
        vec![DatasetRef::from(&step.target)],
        serde_json::json!({ "action": action_name }),
    );

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
    Ok((affected_object(target, columns, values), run_id))
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

/// Targeted single-object read backing the O(change) copy-on-write:
/// `SELECT "c1", "c2", ... FROM "schema"."table" WHERE "id" = ?` over all properties
/// (identifiers double-quoted, embedded quotes doubled; column order = property order).
/// Uses a `?` placeholder — NOT `$1`: the serving seam substitutes `?` via
/// `inline_params` (`serving::inline_params`), so a `$1` would never be bound. The
/// single param is the resolved identity value. The read runs over the identity-aware
/// merge view, so it returns the current merged version of exactly the targeted object.
fn select_object_sql(target: &ObjectType, id_column: &str) -> String {
    let q = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let cols = target
        .properties
        .iter()
        .map(|p| q(&p.name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {cols} FROM {}.{} WHERE {} = ?",
        q(&target.table.schema),
        q(&target.table.name),
        q(id_column),
    )
}

/// Max bounded retries of the inline-delta write after a per-identity CAS conflict
/// before the conflict surfaces to the caller (a genuinely-contended object).
const COW_MAX_RETRIES: u32 = 5;

/// Backoff before retrying a lost inline-delta CAS: capped exponential
/// (`min(CAP, BASE << attempt)`) plus a small deterministic jitter derived from hashing
/// `attempt` (no `rand` — a deterministic hash is enough to spread retries). Mirrors
/// `iceberg_writer::commit_backoff`'s scheme without the per-writer path; the
/// per-identity advisory lock already serializes same-identity writers, so genuine
/// contention (and thus a retry) is rare.
fn cow_backoff(attempt: u32) -> std::time::Duration {
    use std::hash::{Hash, Hasher};
    const BASE: std::time::Duration = std::time::Duration::from_millis(5);
    const CAP: std::time::Duration = std::time::Duration::from_millis(100);
    let exp = BASE.saturating_mul(1u32 << attempt.min(16)).min(CAP);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    attempt.hash(&mut hasher);
    // Jitter in [0, BASE): keep it bounded so it never dominates the delay.
    let jitter_ms = hasher.finish() % (BASE.as_millis() as u64).max(1);
    exp + std::time::Duration::from_millis(jitter_ms)
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

/// Locate the single live row whose `id_idx` cell equals `id_value` (the supplied,
/// already-typed identity). The identity is a primary key, so at most one live match:
/// no match is `NotFound` (the caller's 404); more than one is a corrupt invariant —
/// surfaced as a `Backend` fault (500), never a client error and never a silent
/// pick-one mutate. Returns the matched row's index and a clone of the row. Pure;
/// the unit-test seam for the mutate locate phase.
pub fn locate_unique_row(
    rows: &[Vec<SqlValue>],
    id_idx: usize,
    id_value: &SqlValue,
    idprop: &str,
) -> Result<(usize, Vec<SqlValue>), ActionError> {
    let mut matches = rows
        .iter()
        .enumerate()
        .filter(|(_, r)| r.get(id_idx) == Some(id_value));
    let (target_idx, existing) = match matches.next() {
        None => return Err(ActionError::NotFound),
        Some((i, r)) => (i, r.clone()),
    };
    if matches.next().is_some() {
        // >1 live row for a primary key is a corrupt invariant, not a client error.
        return Err(ActionError::ControlPlane(ControlPlaneError::Backend(
            format!("identity `{idprop}` matches more than one live row").into(),
        )));
    }
    Ok((target_idx, existing))
}

/// The three ordered fine-grained Write-policy legs of a mutate — a whole-row verdict,
/// deliberately NOT the INSERT gate (`write_filter::check_write_policy`), whose
/// deny-column-over-ALL-columns order is wrong for a PATCH. The order is
/// security-relevant and pinned by e2e + unit tests:
///   1. the EXISTING row must pass every policy `row_filter` (UPDATE and DELETE) —
///      a subject may not touch a row it cannot address, and learns nothing about
///      column policies when it cannot;
///   2. UPDATE only: no SET column may be policy-denied;
///   3. UPDATE only: the RESULTING row must pass every `row_filter` — a PATCH may
///      not move a row out of the subject's writable region.
///
/// Fail-closed (an UNKNOWN filter truth denies). Denials are logged server-side; the
/// returned `WriteDenied` reason is caller-scoped (column name only, never the
/// predicate). Pure; the unit-test seam for the mutate policy phase.
pub fn enforce_mutate_policy(
    policies: &[Policy],
    columns: &[String],
    existing: &[SqlValue],
    set_pairs: &[(String, SqlValue)],
    new_row: Option<&[SqlValue]>,
    action_name: &str,
) -> Result<(), ActionError> {
    // The existing row must pass every row-filter (both UPDATE and DELETE).
    if !row_filter_admits(policies, columns, existing) {
        tracing::info!(
            action = action_name,
            "mutate denied: existing row fails write policy filter"
        );
        return Err(ActionError::WriteDenied(WriteDenialReason::RowFilter));
    }
    if let Some(row) = new_row {
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
        if !row_filter_admits(policies, columns, row) {
            tracing::info!(
                action = action_name,
                "update denied: resulting row fails write policy filter"
            );
            return Err(ActionError::WriteDenied(WriteDenialReason::RowFilter));
        }
    }
    Ok(())
}

/// UPDATE/DELETE via O(change) inline-delta copy-on-write. Reads the single targeted
/// object by its declared identity through the identity-aware merge view (a privileged,
/// ACL-unfiltered read), applies the mutation to that one row (DELETE ⇒ a tombstone;
/// UPDATE ⇒ the named non-identity columns PATCHed), governs the affected row, then
/// commits ONE inline delta row + lineage — a mirror-only write that does NOT rewrite
/// the table's Parquet files. The write is guarded by a per-identity compare-and-swap
/// against the version token read BEFORE the row read; a lost race
/// (`ServingError::Conflict`) re-reads the fresh winner, re-governs, and re-writes, up
/// to [`COW_MAX_RETRIES`] times. Returns the affected object (UPDATE: the new version;
/// DELETE: the removed row) + run id. `NotFound` if no live row matches the identity; a
/// Backend fault if more than one live row does (a corrupt-PK invariant the merge view
/// cannot resolve — e.g. duplicate live file rows).
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
    let step = action
        .steps
        .first()
        .ok_or_else(|| ActionError::Misconfigured("action has no steps".into()))?;
    let policy_target = PolicyTarget::Type(step.target.clone());

    // The identity property + the supplied identity value (parsed/typed via the params).
    let idprop = target.identity.clone().ok_or_else(|| {
        ActionError::Misconfigured(format!("type `{}` has no declared identity", target.name.0))
    })?;
    let now = request_now();
    // Single-step mutate: no prior-step bindings (Task 5 populates the env across steps).
    let step_env = crate::params::StepEnv::new();
    let pairs = crate::params::resolve_action_row(action, target, body, now, &step_env)?;
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
    // The identity property's logical type — needed for the version-token read and, on
    // DELETE, for the one-cell tombstone id batch.
    let id_logical = target
        .properties
        .iter()
        .find(|p| p.name == idprop)
        .map(|p| p.ty.clone())
        .ok_or_else(|| {
            ActionError::Misconfigured(format!("identity `{idprop}` is not a property"))
        })?;

    // The PATCH columns the caller actually SET (non-identity, non-null). An omitted
    // optional param materializes as `SqlValue::Null`; PATCH semantics leave it untouched,
    // so it is excluded both from the column-denial check and from the written row.
    // Retry-invariant (derived from the request, not the read row).
    let set_pairs: Vec<(String, SqlValue)> = pairs
        .iter()
        .filter(|(c, v)| c != &idprop && !matches!(v, SqlValue::Null))
        .cloned()
        .collect();

    // Fine-grained Write policies for the subject on this type. Fetched once; the
    // per-row governance (`enforce_mutate_policy`) is re-run against the freshly-read
    // row on every retry, so a CAS re-read re-governs the current winner.
    let write_policies = deps
        .cp
        .acl()
        .policies_for(subject, Action::Write, &policy_target, PageReq::unbounded())
        .await?;

    // Lineage minted once; the run_id is stable across retries (reused on re-write, so
    // the caller's returned run_id names the same action regardless of contention).
    let run_id = RunId(Uuid::new_v4());
    let op = if is_update { "update" } else { "delete" };
    let event = LineageEvent {
        run_id,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![DatasetRef::from(&step.target)],
        outputs: vec![DatasetRef::from(&step.target)],
        payload: serde_json::json!({ "action": action_name, "op": op }),
    };

    // Bounded CAS retry loop: capture the per-identity version token, read the current
    // merged row, re-govern it, then commit ONE O(change) inline delta guarded by the
    // token. A lost race (`ServingError::Conflict`) re-reads the fresh winner and retries
    // up to COW_MAX_RETRIES times; past the cap the conflict surfaces to the caller.
    let mut attempt = 0u32;
    let returned = loop {
        // 1. Version token BEFORE the read — the provably-safe ordering: any mutation of
        //    this identity that lands between here and the write bumps its version, so the
        //    CAS in `write_delta` detects it and forces a retry (never a lost update).
        let v0 = deps
            .action_engine
            .current_inline_version(&target.table, &idprop, &id_value, &id_logical)
            .await?;

        // 2. Targeted merged read of the current live object (single `?` bound to the
        //    identity value; the serving seam substitutes it via `inline_params`).
        let live = deps
            .serving
            .fetch_rows(
                &select_object_sql(target, &idprop),
                std::slice::from_ref(&id_value),
            )
            .await?;
        // 0 live rows ⇒ the object does not exist (the caller's 404). >1 is a corrupt-PK
        // invariant the identity-dedup merge could not resolve (e.g. duplicate live file
        // rows) — surfaced as a Backend fault (the operator's 500), never a silent
        // pick-one mutate. Mirrors the old `locate_unique_row` guard.
        let existing = match live.rows.as_slice() {
            [] => return Err(ActionError::NotFound),
            [row] => row.clone(),
            _ => {
                return Err(ActionError::ControlPlane(ControlPlaneError::Backend(
                    format!("identity `{idprop}` matches more than one live row").into(),
                )));
            }
        };

        // 3. Compute the resulting row (UPDATE = existing with the SET columns
        //    overwritten; DELETE keeps `None`). Same PATCH logic as the whole-table path.
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

        // 4. Governance + constraints — unchanged from the whole-table path. The three
        //    ordered mutate legs run on the freshly-read `existing`/`new_row`, then the
        //    per-value constraint check on the SET values (403 before 422, mirroring
        //    INSERT). DELETE sets nothing (`set_pairs` empty), so it is unaffected.
        enforce_mutate_policy(
            &write_policies.items,
            &columns,
            &existing,
            &set_pairs,
            new_row.as_deref(),
            action_name,
        )?;
        let cviol = value_constraint_violations(target, &set_pairs)?;
        if !cviol.is_empty() {
            tracing::info!(
                action = action_name,
                count = cviol.len(),
                "update rejected: constraint violation"
            );
            return Err(ActionError::ConstraintViolation(cviol));
        }

        // 5. Commit ONE inline delta, guarded by the CAS on `v0`. UPDATE writes the full
        //    post-PATCH row; DELETE writes a tombstone carrying only the identity (the
        //    engine builds a one-cell id batch and NULLs the other columns).
        let res = match &new_row {
            Some(row) => {
                deps.action_engine
                    .write_delta(
                        &target.table,
                        &idprop,
                        false,
                        &columns,
                        row,
                        &logical,
                        event.clone(),
                        v0,
                    )
                    .await
            }
            None => {
                deps.action_engine
                    .write_delta(
                        &target.table,
                        &idprop,
                        true,
                        std::slice::from_ref(&idprop),
                        std::slice::from_ref(&id_value),
                        std::slice::from_ref(&id_logical),
                        event.clone(),
                        v0,
                    )
                    .await
            }
        };
        match res {
            // 6. Success: return the affected object (UPDATE: the new version; DELETE: the
            //    removed row's values).
            Ok(_) => break new_row.unwrap_or(existing),
            // Lost the per-identity CAS race — re-read the fresh winner and retry.
            Err(ServingError::Conflict(_)) if attempt < COW_MAX_RETRIES => {
                attempt += 1;
                tracing::debug!(
                    action = action_name,
                    attempt,
                    "cow: inline-delta CAS conflict, re-reading and retrying"
                );
                tokio::time::sleep(cow_backoff(attempt)).await;
                continue;
            }
            // A genuinely-contended object past the retry cap (or any other failure).
            Err(e) => return Err(ActionError::from(e)),
        }
    };

    // 7. Return the affected object + run id — unchanged shape.
    Ok((affected_object(target, columns, returned), run_id))
}
