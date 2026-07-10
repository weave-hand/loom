//! Define-time validation for action `downstream` job templates.
//!
//! See [`validate_action_downstream`] — the pure/static gate both adapters call from
//! `Ontology::define_action` so a misconfigured `downstream` (unknown job kind, or a
//! payload `@self.<prop>` ref to a column the primary step does not write) is rejected
//! loudly at define time rather than silently producing an undispatchable / unresolvable
//! job at runtime.

use crate::{ActionStep, ControlPlaneError, JobTemplate, KNOWN_JOB_KINDS};

/// Collect every `@<name>` / `@self.<name>` reference leaf in a JSON payload. A leaf is
/// any JSON string starting with `@`; the leading `@` and an optional `self.` prefix are
/// stripped to yield the referenced name. Recurses into objects and arrays; non-string
/// leaves are ignored. Pure — no expression evaluation (params are out of scope this
/// slice, so the only legal `@`-leaf is `@self.<prop>`).
fn ref_names(payload: &serde_json::Value) -> Vec<String> {
    let mut out = Vec::new();
    match payload {
        serde_json::Value::String(s) if s.starts_with('@') => {
            let name = s.trim_start_matches('@').trim_start_matches("self.");
            out.push(name.to_string());
        }
        serde_json::Value::Object(map) => {
            for v in map.values() {
                out.extend(ref_names(v));
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                out.extend(ref_names(v));
            }
        }
        _ => {}
    }
    out
}

/// Validate every downstream template of an action against its primary step's
/// **produced columns**:
/// - `kind` is a known worker kind ([`KNOWN_JOB_KINDS`]); and
/// - every payload `@self.<prop>` leaf names a column `primary_step` actually writes.
///
/// Produced columns = each parameter's [`crate::ParamDef::binds_property`] union each
/// assignment's `property` — exactly the row `resolve_action_row` (query-api) builds at
/// invoke time, so a template accepted here is guaranteed to resolve at runtime instead of
/// silently degrading to a literal `@self.<prop>` string. This is stricter than (and
/// replaces) validating against the target's full `ObjectType.properties`: a real property
/// the step neither binds via a param nor sets via an assignment is rejected, including
/// `@self.id` when no param/assignment produces the identity column. Pure/static — no I/O,
/// no expression evaluation.
pub fn validate_action_downstream(
    downstream: &[JobTemplate],
    primary_step: &ActionStep,
) -> Result<(), ControlPlaneError> {
    let produced: std::collections::HashSet<&str> = primary_step
        .parameters
        .iter()
        .map(crate::ParamDef::binds_property)
        .chain(primary_step.assignments.iter().map(|a| a.property.as_str()))
        .collect();
    for jt in downstream {
        if !KNOWN_JOB_KINDS.contains(&jt.kind.as_str()) {
            return Err(ControlPlaneError::Validation(format!(
                "downstream job kind `{}` is not a known worker kind",
                jt.kind
            )));
        }
        for name in ref_names(&jt.payload) {
            if !produced.contains(name.as_str()) {
                return Err(ControlPlaneError::Validation(format!(
                    "downstream payload references `@self.{name}`, which the action does not write (add it as a parameter or assignment)"
                )));
            }
        }
    }
    Ok(())
}
