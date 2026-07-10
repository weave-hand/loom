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
use crate::serving::{ActionEngine, ServingError, SqlValue, StepWrite, WriteMode};
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

    // Same-table multi-mutation guard: a multi-step Update/Delete stages an `Overwrite` built from
    // the table's CURRENT COMMITTED contents (nothing in the action has committed yet). So if ANY
    // other step also writes that same table, the Overwrite reads pre-action state — either missing
    // a sibling Insert's row (a spurious NotFound) or clobbering another step's post-image (a silent
    // lost update). Reject it at define time. Insert+Insert to one table stays allowed (those
    // coalesce as appends); only an Update/Delete step sharing a table with another step is rejected.
    for (i, (step, target)) in action.steps.iter().zip(targets).enumerate() {
        if !matches!(step.kind, ActionKind::Update | ActionKind::Delete) {
            continue;
        }
        let clashes = action
            .steps
            .iter()
            .zip(targets)
            .enumerate()
            .any(|(j, (_, t))| j != i && t.table == target.table);
        if clashes {
            return Err(ActionError::Misconfigured(format!(
                "action `{}` cannot Update/Delete table `{}`.`{}` that another step writes",
                action.name.0, target.table.schema, target.table.name
            )));
        }
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
/// copy-on-write). Returns the affected-object payload as an [`ActionOutcome`] (`Single` for
/// a lone bind-less step, `Multi` — one [`StepResult`] per step — otherwise) plus the `RunId`
/// so the caller can locate the action's lineage (committed atomically with the new snapshot).
pub async fn run_action(
    action_name: &str,
    body: &serde_json::Map<String, Value>,
    subject: &SubjectId,
    deps: &ActionDeps<'_>,
) -> Result<(ActionOutcome, RunId, ActionKind), ActionError> {
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

    // 2. Dispatch on step count. A lone bind-less step is EXACTLY today's single-object path
    //    (byte-compatible, inline tiering preserved). Anything else (≥2 steps, or a single bound
    //    step) is the multi-step orchestration, which resolves + governs EVERY step itself before
    //    one atomic `write_steps`. Only the single-step path needs a target resolved here — the
    //    multi-step path resolves each step's target inside `run_multi_step`.
    let single = (action.steps.len() == 1)
        .then(|| action.steps.first())
        .flatten()
        .filter(|s| s.bind.is_none());
    let Some(single_step) = single else {
        return run_multi_step(&action, body, subject, deps).await;
    };

    // Single-step: resolve ONLY this step's target (its table + property logical types). A missing
    // target is a broken ActionDef (internal inconsistency), not a client 404 — propagate as a
    // ControlPlane fault (-> 500).
    let target = deps.cp.ontology().get_type(&single_step.target).await?;

    // Govern: deny-by-default coarse Write on the target type, THEN conformance (so a
    // misconfigured ActionDef never leaks its definition-validity to an unauthorized caller),
    // then dispatch on kind. The fine-grained write policy + the actual write differ per kind.
    let policy_target = PolicyTarget::Type(single_step.target.clone());
    if deps
        .cp
        .acl()
        .check(subject, Action::Write, &policy_target)
        .await?
        == Decision::Deny
    {
        return Err(ActionError::Forbidden);
    }
    check_conformance(&action, &target)?;
    let kind = single_step.kind;
    let (rows, run_id) = match kind {
        ActionKind::Insert => run_insert(&action, &target, body, subject, deps).await?,
        ActionKind::Update => run_mutate(&action, &target, body, subject, deps, true).await?,
        ActionKind::Delete => run_mutate(&action, &target, body, subject, deps, false).await?,
    };
    Ok((ActionOutcome::Single(rows), run_id, kind))
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

/// One multi-step action step's affected object, labelled for the response
/// envelope. `bind` is the step's declared name (the semantic key, when it
/// declared one); `target` is the step's type (always present); array order
/// disambiguates unbound same-target steps.
#[derive(Debug)]
pub struct StepResult {
    pub bind: Option<String>,
    pub target: String,
    pub rows: ObjectRows,
}

/// The affected-object payload an action produces. `Single` is a lone
/// bind-less step (today's byte-compatible bare-object response); `Multi`
/// carries every step's result in declared order.
#[derive(Debug)]
pub enum ActionOutcome {
    Single(ObjectRows),
    Multi(Vec<StepResult>),
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
    let pairs = crate::params::resolve_action_row(step, target, body, now, &step_env)?;
    // Slice-4: resolve the action's downstream templates against the written row, so
    // the jobs ride the write's commit tx (atomic commit-or-neither).
    let downstream_jobs = crate::downstream::resolve_downstream(&action.downstream, &pairs);
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
    //    The resolved downstream jobs ride this same write tx (commit-or-neither).
    deps.action_engine
        .write_object(
            &target.table,
            &full_columns,
            &full_values,
            &full_logical,
            event,
            &downstream_jobs,
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
    select_object_sql_where(target, Some(id_column))
}

/// The full-table read backing the multi-step Update/Delete copy-on-write overwrite:
/// [`select_object_sql`] WITHOUT the identity `where` clause, so it returns EVERY live row of
/// `target` (over the identity-aware merge view) in property order. The caller locates the
/// targeted identity row, patches (Update) or drops (Delete) it, and writes the resulting row
/// set back as an `Overwrite` — every OTHER row preserved verbatim.
fn select_object_sql_all(target: &ObjectType) -> String {
    select_object_sql_where(target, None)
}

/// Shared builder for the targeted ([`select_object_sql`]) and full-table
/// ([`select_object_sql_all`]) reads: `SELECT <all props> FROM "schema"."table"` with an
/// optional `WHERE "<id>" = ?` predicate (identifiers double-quoted, embedded quotes doubled;
/// column order = property order). The `?` placeholder is substituted by the serving seam via
/// `inline_params` — a `$1` would never be bound.
fn select_object_sql_where(target: &ObjectType, id_column: Option<&str>) -> String {
    let q = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let cols = target
        .properties
        .iter()
        .map(|p| q(&p.name))
        .collect::<Vec<_>>()
        .join(", ");
    let predicate = match id_column {
        Some(id) => format!(" WHERE {} = ?", q(id)),
        None => String::new(),
    };
    format!(
        "SELECT {cols} FROM {}.{}{predicate}",
        q(&target.table.schema),
        q(&target.table.name),
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

/// Compute the mutate post-image and run its governance — the shared core of BOTH mutate
/// paths (the single-object inline-delta [`run_mutate`] and the multi-step file-tier
/// [`govern_and_build_mutate`]). UPDATE builds `new_row` by cloning `existing` and
/// overwriting each SET column in place; DELETE keeps `None`. It then runs the three ordered
/// mutate policy legs ([`enforce_mutate_policy`]) on the existing/new image and the per-value
/// constraint check on the SET values, returning the `ConstraintViolation` error on any
/// violation. Returns the computed `new_row` (`None` for DELETE). Pure over its inputs; the
/// single home for the PATCH + governance block both callers had verbatim.
fn mutate_governance(
    target: &ObjectType,
    columns: &[String],
    existing: &[SqlValue],
    set_pairs: &[(String, SqlValue)],
    write_policies: &[Policy],
    action_name: &str,
    is_update: bool,
) -> Result<Option<Vec<SqlValue>>, ActionError> {
    // UPDATE = existing with the SET columns overwritten; DELETE keeps `None`.
    let new_row: Option<Vec<SqlValue>> = is_update.then(|| {
        let mut row = existing.to_vec();
        for (col, val) in set_pairs {
            if let Some(ci) = columns.iter().position(|c| c == col)
                && let Some(slot) = row.get_mut(ci)
            {
                *slot = val.clone();
            }
        }
        row
    });

    // The three ordered mutate legs on the freshly-read existing/new image, then the
    // per-value constraint check on the SET values (403 before 422, mirroring INSERT).
    // DELETE sets nothing (`set_pairs` empty), so it is unaffected by the latter.
    enforce_mutate_policy(
        write_policies,
        columns,
        existing,
        set_pairs,
        new_row.as_deref(),
        action_name,
    )?;
    let cviol = value_constraint_violations(target, set_pairs)?;
    if !cviol.is_empty() {
        tracing::info!(
            action = action_name,
            count = cviol.len(),
            "update rejected: constraint violation"
        );
        return Err(ActionError::ConstraintViolation(cviol));
    }
    Ok(new_row)
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
    let pairs = crate::params::resolve_action_row(step, target, body, now, &step_env)?;
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

    // Slice-4 phase 2: resolve the action's downstream templates against the resolved
    // row (identity + any SET columns), so the jobs ride the write's commit tx — same
    // atomic commit-or-neither contract as the insert path.
    let jobs = crate::downstream::resolve_downstream(&action.downstream, &pairs);

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
                None,
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

        // 3/4. Compute the resulting row (UPDATE = existing PATCHed; DELETE keeps `None`)
        //      and run governance + constraints on the freshly-read image — shared verbatim
        //      with the whole-table path via `mutate_governance`.
        let new_row = mutate_governance(
            target,
            &columns,
            &existing,
            &set_pairs,
            &write_policies.items,
            action_name,
            is_update,
        )?;

        // 5. Commit ONE inline delta, guarded by the CAS on `v0`. UPDATE writes the full
        //    post-PATCH row; DELETE writes a tombstone carrying only the identity (the
        //    engine builds a one-cell id batch and NULLs the other columns).
        // The before-image (prior row) rides alongside the new row for the CDC emit
        // path (a later slice). It carries the full ordered property set + the merged
        // prior values, positionally aligned. Postgres accepts but ignores it here, so
        // the non-CDC update/delete write stays byte-identical.
        let before = Some(crate::serving::BeforeImage {
            columns: &columns,
            values: &existing,
            logical_types: &logical,
        });
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
                        before,
                        event.clone(),
                        v0,
                        &jobs,
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
                        before,
                        event.clone(),
                        v0,
                        &jobs,
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

/// Project the shared invocation `body` down to a single step's declared parameters — each step
/// sees ONLY its own param keys. The action-level unknown-key guard (in [`run_multi_step`]) has
/// already rejected any key belonging to no step, so a per-step `parse_params` over this
/// projection never trips on a sibling step's key. Absent keys (omitted optionals) are simply
/// not carried — `parse_params` re-materializes them as `SqlValue::Null`, exactly as the
/// single-step path does.
fn project_body(
    step: &ActionStep,
    body: &serde_json::Map<String, Value>,
) -> serde_json::Map<String, Value> {
    step.parameters
        .iter()
        .filter_map(|p| body.get(&p.name).map(|v| (p.name.clone(), v.clone())))
        .collect()
}

/// Log a fine-grained Write denial server-side (caller-scoped reason; the predicate/policy/role
/// stay here). Shared by the multi-step Insert gate and mirrors the single-step `run_insert` log.
fn log_write_denied(action_name: &str, reason: &WriteDenialReason) {
    match reason {
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
}

/// Multi-step orchestration: resolve + govern EVERY step (in declared order, threading the
/// cross-step binding env) BEFORE a single atomic `write_steps`. A denial on ANY step — coarse
/// Write gate, fine-grained ACL, or a constraint violation — returns before any write is issued,
/// so the whole action is all-or-nothing (nothing is ever partially written). Steps sharing a
/// target table coalesce into one multi-row `Append`; distinct targets stay distinct. One
/// `RunId`; the lineage event's `outputs` list every step's target dataset. Returns
/// `ActionOutcome::Multi`, one [`StepResult`] per declared step in order, captured before
/// `coalesce_appends` — the HTTP handler renders every step's affected object.
async fn run_multi_step(
    action: &ActionDef,
    body: &serde_json::Map<String, Value>,
    subject: &SubjectId,
    deps: &ActionDeps<'_>,
) -> Result<(ActionOutcome, RunId, ActionKind), ActionError> {
    let action_name = action.name.0.as_str();
    let now = request_now();
    let run_id = RunId(Uuid::new_v4());
    // The primary kind for HTTP status purposes: the first declared step's kind. `run_multi_step`
    // already requires at least one step (see the empty-steps guards below), so this is total via
    // `unwrap_or_default()` without an `expect`.
    let primary_kind = action.steps.first().map(|s| s.kind).unwrap_or_default();

    // Resolve EVERY step's target type (for their tables + property logical types, and so
    // cross-step conformance can see each step's target). A missing target is a broken ActionDef
    // (internal inconsistency), not a client 404 — propagate as a ControlPlane fault (-> 500).
    let mut targets: Vec<ObjectType> = Vec::with_capacity(action.steps.len());
    for step in &action.steps {
        targets.push(deps.cp.ontology().get_type(&step.target).await?);
    }
    let targets = targets.as_slice();

    // Coarse Write gate on the FIRST step BEFORE conformance, so a misconfigured ActionDef never
    // leaks its definition-validity to a caller unauthorized on the root target. The per-step loop
    // below re-gates every step (including this one — deliberately, so each target is checked).
    let first = action
        .steps
        .first()
        .ok_or_else(|| ActionError::Misconfigured("action has no steps".into()))?;
    if deps
        .cp
        .acl()
        .check(
            subject,
            Action::Write,
            &PolicyTarget::Type(first.target.clone()),
        )
        .await?
        == Decision::Deny
    {
        return Err(ActionError::Forbidden);
    }

    // Conformance across every step: each step's parameters/assignments mirror ITS target's
    // properties, every `StepRef` references a strictly-earlier bound step's real property, and no
    // Update/Delete step shares a table with another step. Surfaced as a clear error before any write.
    check_conformance_steps(action, targets)?;

    // Action-level unknown-key guard: the shared body is projected per step (each step sees only
    // ITS param keys), so a per-step `parse_params` never rejects a sibling step's key. Enforce
    // the "no unknown param" guarantee here instead, over the UNION of every step's params.
    let known: std::collections::HashSet<&str> = action
        .steps
        .iter()
        .flat_map(|s| s.parameters.iter().map(|p| p.name.as_str()))
        .collect();
    if let Some(k) = body.keys().find(|k| !known.contains(k.as_str())) {
        return Err(ActionError::BadParams(ParamError::Unknown(k.clone())));
    }

    let mut step_env = crate::params::StepEnv::new();
    let mut writes: Vec<StepWrite> = Vec::with_capacity(action.steps.len());
    let mut step_results: Vec<StepResult> = Vec::with_capacity(action.steps.len());
    // The first (primary) step's resolved row, captured below — `downstream` templates
    // resolve `@self.<prop>` refs against it, keyed by the action's primary identity
    // exactly as the single-step insert/update/delete paths key off their sole step.
    let mut primary_pairs: Vec<(String, SqlValue)> = Vec::new();
    let mut first = true;

    // Resolve + govern each step in declared order, building its staged write. NOTHING is written
    // in this loop — `write_steps` runs once, after the whole action clears governance.
    for (step, target) in action.steps.iter().zip(targets) {
        // a. Coarse Write gate on THIS step's target (deny-by-default).
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

        // b. Resolve the step's row (params → binds → assignments/exprs → StepRefs from the env),
        //    projecting the shared body to this step's params.
        let step_body = project_body(step, body);
        let pairs = crate::params::resolve_action_row(step, target, &step_body, now, &step_env)?;

        // Capture the FIRST step's resolved row (before it's consumed below) as the
        // `downstream` resolution key — a clone, since `pairs` stays in use for the rest
        // of this iteration. Gated on a separate `first` flag rather than
        // `primary_pairs.is_empty()`: a legitimately-empty row must not be reinterpreted
        // as "not yet captured".
        if first {
            primary_pairs = pairs.clone();
            first = false;
        }

        // c/d/e. Fine governance (identical to the single-object gates, per step) + build the
        //        step's staged write. Any denial/violation returns here — before any write.
        let write = govern_and_build_step(step, target, &pairs, subject, deps, action_name).await?;

        // Response: capture EVERY step's affected object, in declared order.
        let cols: Vec<String> = pairs.iter().map(|(c, _)| c.clone()).collect();
        let vals: Vec<SqlValue> = pairs.iter().map(|(_, v)| v.clone()).collect();
        step_results.push(StepResult {
            bind: step.bind.clone(),
            target: step.target.0.clone(),
            rows: affected_object(target, cols, vals),
        });

        // f. Expose this step's resolved row (INCLUDING its identity) under its `bind` name, so a
        //    later step's `StepRef` resolves against it.
        if let Some(b) = &step.bind {
            let row: BTreeMap<String, SqlValue> =
                pairs.iter().map(|(c, v)| (c.clone(), v.clone())).collect();
            step_env.insert(b.clone(), row);
        }

        writes.push(write);
    }

    // Coalesce Append writes that share a target table into one multi-row write; distinct targets
    // (and any Overwrite) stay distinct.
    let writes = coalesce_appends(writes);

    // ONE lineage event listing every step's target dataset (inputs stay [] for a from-params
    // create). The caller owns the run_id; the engine commits every step + this event atomically.
    let outputs: Vec<DatasetRef> = action
        .steps
        .iter()
        .map(|s| DatasetRef::from(&s.target))
        .collect();
    let event = LineageEvent::completed_with_run(
        run_id,
        outputs,
        serde_json::json!({ "action": action_name }),
    );

    // Resolve the action's downstream templates against the primary (first) step's
    // resolved row, so the jobs ride the multi-step write's commit tx — atomic
    // commit-or-neither, exactly as the single-step paths do.
    let jobs = crate::downstream::resolve_downstream(&action.downstream, &primary_pairs);

    deps.action_engine
        .write_steps(&writes, event, &jobs)
        .await?;

    if step_results.is_empty() {
        return Err(ActionError::Misconfigured("action has no steps".into()));
    }
    Ok((ActionOutcome::Multi(step_results), run_id, primary_kind))
}

/// Govern one step and build its staged [`StepWrite`], dispatching on the step's kind. Insert
/// gates the row and stages an `Append`; Update/Delete read the table's full contents, gate the
/// targeted row, and stage the full post-image as an `Overwrite`. Runs entirely before any write.
async fn govern_and_build_step(
    step: &ActionStep,
    target: &ObjectType,
    pairs: &[(String, SqlValue)],
    subject: &SubjectId,
    deps: &ActionDeps<'_>,
    action_name: &str,
) -> Result<StepWrite, ActionError> {
    match step.kind {
        ActionKind::Insert => {
            govern_and_build_insert(target, pairs, subject, deps, action_name).await
        }
        ActionKind::Update => {
            govern_and_build_mutate(target, pairs, subject, deps, action_name, true).await
        }
        ActionKind::Delete => {
            govern_and_build_mutate(target, pairs, subject, deps, action_name, false).await
        }
    }
}

/// INSERT step: gate the SET (non-null) columns/values through the fine-grained Write policy,
/// enforce per-value constraints, then stage the row (expanded to the target's full property
/// set) as an `Append`. Mirrors `run_insert`'s governance exactly.
async fn govern_and_build_insert(
    target: &ObjectType,
    pairs: &[(String, SqlValue)],
    subject: &SubjectId,
    deps: &ActionDeps<'_>,
    action_name: &str,
) -> Result<StepWrite, ActionError> {
    let policy_target = PolicyTarget::Type(target.name.clone());
    // Gate only the columns the caller actually SET (an omitted optional NULL is not "setting" it).
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
        log_write_denied(action_name, &reason);
        return Err(ActionError::WriteDenied(reason));
    }
    let cviol = value_constraint_violations(target, pairs)?;
    if !cviol.is_empty() {
        tracing::info!(
            action = action_name,
            count = cviol.len(),
            "insert rejected: constraint violation"
        );
        return Err(ActionError::ConstraintViolation(cviol));
    }
    let (full_columns, full_values, full_logical) = expand_to_full_row(target, pairs);
    Ok(StepWrite {
        table: target.table.clone(),
        columns: full_columns,
        rows: vec![full_values],
        logical_types: full_logical,
        mode: WriteMode::Append,
    })
}

/// UPDATE/DELETE step: read the table's FULL current contents (the unfiltered merge view),
/// locate the targeted identity row, govern it via the three ordered mutate legs + per-value
/// constraints, then stage the full post-image (targeted row PATCHed for Update / dropped for
/// Delete; every OTHER row preserved verbatim) as an `Overwrite`. `ensure_cow_supported` still
/// rejects vector-typed targets. This is the file-tier copy-on-write (O(table) per mutating
/// step), distinct from the single-object inline-delta path.
async fn govern_and_build_mutate(
    target: &ObjectType,
    pairs: &[(String, SqlValue)],
    subject: &SubjectId,
    deps: &ActionDeps<'_>,
    action_name: &str,
    is_update: bool,
) -> Result<StepWrite, ActionError> {
    ensure_cow_supported(target)?;
    let policy_target = PolicyTarget::Type(target.name.clone());
    let idprop = target.identity.clone().ok_or_else(|| {
        ActionError::Misconfigured(format!("type `{}` has no declared identity", target.name.0))
    })?;
    let id_value = pairs
        .iter()
        .find(|(c, _)| c == &idprop)
        .map(|(_, v)| v.clone())
        .ok_or_else(|| {
            ActionError::Misconfigured(format!("missing identity parameter `{idprop}`"))
        })?;

    let columns: Vec<String> = target.properties.iter().map(|p| p.name.clone()).collect();
    let logical: Vec<String> = target.properties.iter().map(|p| p.ty.clone()).collect();
    // The PATCH columns the caller actually SET (non-identity, non-null). DELETE sets nothing.
    let set_pairs: Vec<(String, SqlValue)> = pairs
        .iter()
        .filter(|(c, v)| c != &idprop && !matches!(v, SqlValue::Null))
        .cloned()
        .collect();

    let write_policies = deps
        .cp
        .acl()
        .policies_for(subject, Action::Write, &policy_target, PageReq::unbounded())
        .await?;

    // Full-table read of the current live contents (unfiltered merge view; no identity `where`).
    let live = deps
        .serving
        .fetch_rows(&select_object_sql_all(target), &[], None)
        .await?;
    let id_idx = columns.iter().position(|c| c == &idprop).ok_or_else(|| {
        ActionError::Misconfigured(format!("identity `{idprop}` is not a property"))
    })?;
    let (target_idx, existing) = locate_unique_row(&live.rows, id_idx, &id_value, &idprop)?;

    // The resulting row + governance/constraints on the freshly-read existing/new image —
    // shared verbatim with the single-object path via `mutate_governance`.
    let new_row = mutate_governance(
        target,
        &columns,
        &existing,
        &set_pairs,
        &write_policies.items,
        action_name,
        is_update,
    )?;

    // The full post-image: patch (UPDATE) or drop (DELETE) the targeted row, keep the rest verbatim.
    let mut rows = live.rows;
    match new_row {
        Some(r) => {
            if let Some(slot) = rows.get_mut(target_idx) {
                *slot = r;
            }
        }
        None => {
            if target_idx < rows.len() {
                rows.remove(target_idx);
            }
        }
    }

    Ok(StepWrite {
        table: target.table.clone(),
        columns,
        rows,
        logical_types: logical,
        mode: WriteMode::Overwrite,
    })
}

/// Coalesce staged writes that share a target table AND `Append` mode into one multi-row write
/// (`build_object_batches` handles N rows engine-side). Distinct targets — and any `Overwrite` —
/// stay distinct. Order-preserving: the first write to a table keeps its position; later same-table
/// Appends fold their rows into it.
fn coalesce_appends(writes: Vec<StepWrite>) -> Vec<StepWrite> {
    let mut out: Vec<StepWrite> = Vec::with_capacity(writes.len());
    for w in writes {
        if w.mode == WriteMode::Append
            && let Some(existing) = out
                .iter_mut()
                .find(|e| e.mode == WriteMode::Append && e.table == w.table)
        {
            existing.rows.extend(w.rows);
            continue;
        }
        out.push(w);
    }
    out
}
