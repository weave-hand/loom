//! Coerce a raw query-param filter value to its column's declared ontology logical type.
//! The string-input analog of `params::parse_value` (which coerces a JSON `Value` from an
//! action body). Reuses the `json_repr_of` -> `JsonRepr` taxonomy. Pure logic, no I/O.

use control_plane_core::{JsonRepr, json_repr_of};

use crate::serving::SqlValue;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum FilterError {
    #[error("filter {0}: {1}")]
    BadValue(String, String),
}

/// Coerce `raw` (a query-param string) to `logical_ty`'s `SqlValue`. Equality-only. The
/// `Number` repr (Integer/Double) resolves to `Int` when `raw` is a clean integer, else
/// `Double` — equality-correct under DuckDB numeric coercion.
pub fn coerce_filter(name: &str, logical_ty: &str, raw: &str) -> Result<SqlValue, FilterError> {
    let bad = |m: &str| FilterError::BadValue(name.to_string(), m.to_string());
    let repr = json_repr_of(logical_ty).map_err(|_| bad("unknown logical type"))?;
    match repr {
        JsonRepr::Number => {
            if let Ok(i) = raw.parse::<i64>() {
                Ok(SqlValue::Int(i))
            } else {
                raw.parse::<f64>()
                    .map(SqlValue::Double)
                    .map_err(|_| bad("expected a number"))
            }
        }
        JsonRepr::NumericString => raw
            .parse::<i64>()
            .map(SqlValue::Int)
            .map_err(|_| bad("not an int64")),
        JsonRepr::Bool => match raw {
            "true" => Ok(SqlValue::Bool(true)),
            "false" => Ok(SqlValue::Bool(false)),
            _ => Err(bad("expected true or false")),
        },
        JsonRepr::PlainString => Ok(SqlValue::Text(raw.to_string())),
        JsonRepr::IsoDate => {
            let fmt = time::macros::format_description!("[year]-[month]-[day]");
            time::Date::parse(raw, &fmt)
                .map(SqlValue::Date)
                .map_err(|_| bad("invalid ISO date"))
        }
        JsonRepr::IsoTimestamp => {
            let fmt =
                time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
            time::PrimitiveDateTime::parse(raw, &fmt)
                .map(SqlValue::Timestamp)
                .map_err(|_| bad("invalid ISO timestamp"))
        }
    }
}
