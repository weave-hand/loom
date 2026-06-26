//! Parse a typed JSON action body into ordered (column, SqlValue) pairs, validated
//! against the action's parameters. The inverse of `render.rs`: Long arrives as a JSON
//! string, Double as a number, Boolean as a bool, String as a string, Date/Timestamp as
//! ISO strings. Pure logic, no I/O.

use control_plane_core::{JsonRepr, ParamDef, json_repr_of};
use serde_json::Value;

use crate::serving::SqlValue;

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

#[expect(
    clippy::map_err_ignore,
    reason = "error-handling debt — see docs/error-handling-debt.md"
)]
fn parse_value(name: &str, logical_ty: &str, v: &Value) -> Result<SqlValue, ParamError> {
    let bad = |m: &str| ParamError::BadValue(name.to_string(), m.to_string());
    let repr = json_repr_of(logical_ty).map_err(|_| bad("unknown logical type"))?;
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
                .map_err(|_| bad("not an int64"))
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
                .map_err(|_| bad("invalid ISO date"))
        }
        JsonRepr::IsoTimestamp => {
            let s = v
                .as_str()
                .ok_or_else(|| bad("expected an ISO timestamp string"))?;
            let fmt =
                time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
            time::PrimitiveDateTime::parse(s, &fmt)
                .map(SqlValue::Timestamp)
                .map_err(|_| bad("invalid ISO timestamp"))
        }
        // Vectors are stored data, not action/query inputs (road-vector-column-type).
        JsonRepr::FloatArray => Err(bad("vector parameters are not supported")),
    }
}
