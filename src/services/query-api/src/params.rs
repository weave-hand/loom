//! Parse a typed JSON action body into ordered (column, SqlValue) pairs, validated
//! against the action's parameters. The inverse of `render.rs`: Long arrives as a JSON
//! string, Double as a number, Boolean as a bool, String as a string, Date/Timestamp as
//! ISO strings. Pure logic, no I/O.

use std::collections::BTreeMap;

use control_plane_core::{ActionStep, JsonRepr, ObjectType, ParamDef, json_repr_of};
use serde_json::Value;

use crate::serving::SqlValue;

/// The cross-step binding environment at invocation: each earlier step's `bind` name mapped to
/// its resolved row (`property -> value`, including its identity). A `StepRef { bind, prop }`
/// assignment reads `env[bind][prop]`. The single-step path passes an empty env; Task 5 populates
/// it as it resolves steps in order.
pub type StepEnv = BTreeMap<String, BTreeMap<String, SqlValue>>;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ParamError {
    #[error("missing required parameter: {0}")]
    Missing(String),
    #[error("unknown parameter: {0}")]
    Unknown(String),
    #[error("parameter {0}: {1}")]
    BadValue(String, String),
}

/// Validate `body` against `params`; return values in `params` order. Required params must
/// be present and non-null; extra keys are rejected; each value is parsed per its logical type.
pub fn parse_params(
    params: &[ParamDef],
    body: &serde_json::Map<String, Value>,
) -> Result<Vec<(String, SqlValue)>, ParamError> {
    // Reject unknown keys up front.
    for k in body.keys() {
        if !params.iter().any(|p| &p.name == k) {
            return Err(ParamError::Unknown(k.clone()));
        }
    }
    let mut out = Vec::with_capacity(params.len());
    for p in params {
        match body.get(&p.name) {
            None | Some(Value::Null) => {
                if p.required {
                    return Err(ParamError::Missing(p.name.clone()));
                }
                out.push((p.name.clone(), SqlValue::Null));
            }
            Some(v) => out.push((p.name.clone(), parse_value(&p.name, &p.ty, v)?)),
        }
    }
    Ok(out)
}

/// Resolve ONE action step's invocation body into ordered `(property, SqlValue)` write pairs,
/// applying the step's param→property mapping (`binds`), its constant/expression assignments, and
/// any cross-step `StepRef`s (read from `step_env`). The body is keyed by PARAMETER name; the
/// returned pairs are keyed by the PROPERTY each param binds (or a constant/ref fills). Assumes
/// the step already passed conformance (so constants coerce and no property is double-written).
/// Constants reuse the same `parse_value` coercion — against the PROPERTY's logical type — that
/// parameters take, so a constant is a first-class equal of a param.
///
/// A single-step action passes its sole step (`action.steps.first()`); a multi-step action calls
/// this once per step in declared order, passing the step whose params/assignments to resolve and
/// the growing `step_env` so a later step's `StepRef` sees earlier steps' resolved rows.
pub fn resolve_action_row(
    step: &ActionStep,
    target: &ObjectType,
    body: &serde_json::Map<String, Value>,
    now: time::PrimitiveDateTime,
    step_env: &StepEnv,
) -> Result<Vec<(String, SqlValue)>, ParamError> {
    // Param leg: reuse parse_params (rejects unknown keys, enforces required, coerces by
    // param.ty), then remap each pair from param name → bound property. parse_params preserves
    // step.parameters order, so zipping the param refs onto its output is exact.
    let param_pairs = parse_params(&step.parameters, body)?;

    // param name -> value (for bare-identifier refs in expressions).
    let param_env: std::collections::HashMap<String, SqlValue> = step
        .parameters
        .iter()
        .zip(&param_pairs)
        .map(|(prm, (_, v))| (prm.name.clone(), v.clone()))
        .collect();

    let mut out: Vec<(String, SqlValue)> =
        Vec::with_capacity(param_pairs.len() + step.assignments.len());
    // property name -> value (params' bound properties, then earlier assignments), for @refs.
    let mut prop_env: std::collections::HashMap<String, SqlValue> =
        std::collections::HashMap::new();
    for (prm, (_, value)) in step.parameters.iter().zip(param_pairs) {
        prop_env.insert(prm.binds_property().to_string(), value.clone());
        out.push((prm.binds_property().to_string(), value));
    }

    // Assignment leg: constants coerce against the PROPERTY's logical type; expressions parse +
    // evaluate against the accumulating param/prop env, in declared order.
    for a in &step.assignments {
        let prop_ty = target
            .properties
            .iter()
            .find(|p| p.name == a.property)
            .map(|p| p.ty.as_str())
            .ok_or_else(|| {
                ParamError::BadValue(
                    a.property.clone(),
                    "assignment names an unknown property".into(),
                )
            })?;
        let value = match &a.source {
            control_plane_core::AssignmentSource::Const(v) => parse_value(&a.property, prop_ty, v)?,
            control_plane_core::AssignmentSource::Expr(src) => {
                let expr = crate::expr::parse_expr(src).map_err(|e| {
                    ParamError::BadValue(a.property.clone(), format!("expression parse: {e}"))
                })?;
                let env = RowEnv {
                    params: &param_env,
                    props: &prop_env,
                };
                crate::expr::eval(&expr, &env, now)
                    .map_err(|e| ParamError::BadValue(a.property.clone(), e.to_string()))?
            }
            control_plane_core::AssignmentSource::StepRef { bind, prop } => step_env
                .get(bind)
                .and_then(|row| row.get(prop))
                .cloned()
                .ok_or_else(|| {
                    ParamError::BadValue(
                        a.property.clone(),
                        format!("cross-step reference @{bind}.{prop} is unresolved"),
                    )
                })?,
        };
        prop_env.insert(a.property.clone(), value.clone());
        out.push((a.property.clone(), value));
    }
    Ok(out)
}

/// A `crate::expr::ValueEnv` over the resolved params + accumulated property values.
struct RowEnv<'a> {
    params: &'a std::collections::HashMap<String, SqlValue>,
    props: &'a std::collections::HashMap<String, SqlValue>,
}
impl crate::expr::ValueEnv for RowEnv<'_> {
    fn param(&self, name: &str) -> Option<SqlValue> {
        self.params.get(name).cloned()
    }
    fn prop(&self, name: &str) -> Option<SqlValue> {
        self.props.get(name).cloned()
    }
}

/// Define-time guard for a constant assignment: the JSON `value` must be a scalar (not
/// null/array/object) coercible to the property's logical type — the same acceptance the
/// write path applies via `parse_value`. Keeps the constant and the runtime coercion in
/// lockstep (a constant that conforms here cannot fail the write-path coercion later).
pub fn validate_const(property: &str, logical_ty: &str, value: &Value) -> Result<(), ParamError> {
    if value.is_null() || value.is_array() || value.is_object() {
        return Err(ParamError::BadValue(
            property.to_string(),
            "constant must be a scalar (string, number, or bool)".into(),
        ));
    }
    parse_value(property, logical_ty, value).map(|_| ())
}

fn parse_value(name: &str, logical_ty: &str, v: &Value) -> Result<SqlValue, ParamError> {
    let bad = |m: &str| ParamError::BadValue(name.to_string(), m.to_string());
    // Like `bad`, but folds the discarded source error into the message for diagnostics.
    let bad_src = |m: &str, e: &dyn std::fmt::Display| {
        ParamError::BadValue(name.to_string(), format!("{m}: {e}"))
    };
    let repr = json_repr_of(logical_ty).map_err(|e| bad_src("unknown logical type", &e.0))?;
    match repr {
        // Integer/Double both arrive as JSON numbers.
        JsonRepr::Number => {
            let n = v.as_f64().ok_or_else(|| bad("expected a number"))?;
            // Integer logical types still come through Number; keep them exact if whole.
            if n.fract() == 0.0 && n.abs() < 9.007e15 {
                Ok(SqlValue::Int(n as i64))
            } else {
                Ok(SqlValue::Double(n))
            }
        }
        // Long arrives as a JSON string (int64 precision).
        JsonRepr::NumericString => {
            let s = v.as_str().ok_or_else(|| bad("expected a numeric string"))?;
            s.parse::<i64>()
                .map(SqlValue::Int)
                .map_err(|e| bad_src("not an int64", &e))
        }
        JsonRepr::Bool => v
            .as_bool()
            .map(SqlValue::Bool)
            .ok_or_else(|| bad("expected a bool")),
        JsonRepr::PlainString => v
            .as_str()
            .map(|s| SqlValue::Text(s.to_string()))
            .ok_or_else(|| bad("expected a string")),
        JsonRepr::IsoDate => {
            let s = v
                .as_str()
                .ok_or_else(|| bad("expected an ISO date string"))?;
            let fmt = time::macros::format_description!("[year]-[month]-[day]");
            time::Date::parse(s, &fmt)
                .map(SqlValue::Date)
                .map_err(|e| bad_src("invalid ISO date", &e))
        }
        JsonRepr::IsoTimestamp => {
            let s = v
                .as_str()
                .ok_or_else(|| bad("expected an ISO timestamp string"))?;
            let fmt =
                time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
            time::PrimitiveDateTime::parse(s, &fmt)
                .map(SqlValue::Timestamp)
                .map_err(|e| bad_src("invalid ISO timestamp", &e))
        }
        // Vectors are stored data, not action/query inputs (road-vector-column-type).
        JsonRepr::FloatArray => Err(bad("vector parameters are not supported")),
    }
}
