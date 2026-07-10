//! Define-time validation for action `downstream` job templates.
//!
//! See [`validate_action_downstream`] — the pure/static gate both adapters call from
//! `Ontology::define_action` so a misconfigured `downstream` (unknown job kind, or a
//! payload `@self.<prop>` ref to a non-existent property) is rejected loudly at define
//! time rather than silently producing an undispatchable / unresolvable job at runtime.

use crate::{ControlPlaneError, JobTemplate, KNOWN_JOB_KINDS, ObjectType};

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

/// Validate every downstream template of an action against its primary target's
/// [`ObjectType`]:
/// - `kind` is a known worker kind ([`KNOWN_JOB_KINDS`]); and
/// - every payload `@self.<prop>` leaf names a real property of `primary_target`.
///
/// Property names only — params are out of scope this slice, keeping define-time and the
/// (later) runtime resolver consistent: any non-property `@`-leaf is rejected, including
/// `@self.id` when the target declares no identity (the `id` property exists iff identity
/// is declared, since identity must name one of `properties`). Pure/static — no I/O, no
/// expression evaluation.
pub fn validate_action_downstream(
    downstream: &[JobTemplate],
    primary_target: &ObjectType,
) -> Result<(), ControlPlaneError> {
    let prop_names: std::collections::HashSet<&str> = primary_target
        .properties
        .iter()
        .map(|p| p.name.as_str())
        .collect();
    for jt in downstream {
        if !KNOWN_JOB_KINDS.contains(&jt.kind.as_str()) {
            return Err(ControlPlaneError::Validation(format!(
                "downstream job kind `{}` is not a known worker kind",
                jt.kind
            )));
        }
        for name in ref_names(&jt.payload) {
            if !prop_names.contains(name.as_str()) {
                return Err(ControlPlaneError::Validation(format!(
                    "downstream payload references unknown `@self.{name}` (not a property of `{}`)",
                    primary_target.name.0
                )));
            }
        }
    }
    Ok(())
}
