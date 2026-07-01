//! Coerce a raw query-param filter value to its column's declared ontology logical type.
//! The string-input analog of `params::parse_value` (which coerces a JSON `Value` from an
//! action body). Reuses the `json_repr_of` -> `JsonRepr` taxonomy. Pure logic, no I/O.

use control_plane_core::{JsonRepr, json_repr_of};

use crate::serving::SqlValue;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum FilterError {
    /// A value could not be coerced to its column's declared logical type. Carries the
    /// structured triple echoed to the caller in a `400` body — `column`, `expected` (the
    /// property's declared logical type name), `value` (the caller's own offending operand)
    /// — plus a human `detail` kept for the `Display`/logs. The value echo is safe: it is
    /// the caller's own input, and this variant is a *coercion* fault, never a permission
    /// signal (that is `handler::QueryError::BadFilter`).
    #[error("filter {column}: {detail}")]
    Coerce {
        column: String,
        expected: String,
        value: String,
        detail: String,
    },
    /// A malformed predicate grammar (bad operator arity, bad set-operand escape). Column +
    /// message only — there is no single offending value/type to echo.
    #[error("filter {0}: {1}")]
    BadValue(String, String),
}

/// A caller filter predicate: a column, a comparison operator, and its coerced operands.
/// Reuses `control_plane_core::CompareOp` but keeps operands on `SqlValue` (which carries
/// Double/Date/Timestamp — `ScalarValue` does not), so typed filtering is not regressed.
/// Operand arity: 0 (null ops), 1 (scalar ops), or N (set ops).
#[derive(Debug, Clone, PartialEq)]
pub struct CallerPredicate {
    pub column: String,
    pub op: control_plane_core::CompareOp,
    pub values: Vec<SqlValue>,
}

/// Coerce a single operand `raw` (a query-param string) to `logical_ty`'s `SqlValue`.
/// Operator-agnostic — the per-operand building block reused by `coerce_predicate`. The
/// `Number` repr (Integer/Double) resolves to `Int` when `raw` is a clean integer, else
/// `Double` — equality-correct under the engine's numeric coercion.
pub fn coerce_filter(name: &str, logical_ty: &str, raw: &str) -> Result<SqlValue, FilterError> {
    // Every `coerce_filter` failure is a value-coercion fault: carry the structured triple
    // {column, expected, value} for the HTTP body plus a `detail` for the Display/logs.
    let coerce = |detail: String| FilterError::Coerce {
        column: name.to_string(),
        expected: logical_ty.to_string(),
        value: raw.to_string(),
        detail,
    };
    let bad = |m: &str| coerce(m.to_string());
    // Like `bad`, but folds the discarded source error into the `detail` for diagnostics.
    let bad_src = |m: &str, e: &dyn std::fmt::Display| coerce(format!("{m}: {e}"));
    let repr = json_repr_of(logical_ty).map_err(|e| bad_src("unknown logical type", &e.0))?;
    match repr {
        JsonRepr::Number => {
            if let Ok(i) = raw.parse::<i64>() {
                Ok(SqlValue::Int(i))
            } else {
                raw.parse::<f64>()
                    .map(SqlValue::Double)
                    .map_err(|e| bad_src("expected a number", &e))
            }
        }
        JsonRepr::NumericString => raw
            .parse::<i64>()
            .map(SqlValue::Int)
            .map_err(|e| bad_src("not an int64", &e)),
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
                .map_err(|e| bad_src("invalid ISO date", &e))
        }
        JsonRepr::IsoTimestamp => {
            let fmt =
                time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
            time::PrimitiveDateTime::parse(raw, &fmt)
                .map(SqlValue::Timestamp)
                .map_err(|e| bad_src("invalid ISO timestamp", &e))
        }
        // Vector columns are not filterable (storage, not search — road-vector-column-type).
        JsonRepr::FloatArray => Err(bad("vector columns cannot be filtered")),
    }
}

/// Split a set-operator operand list on UNESCAPED commas, unescaping each operand.
/// Recognized escapes: `\,` -> `,` and `\\` -> `\`. Any other escape (`\x`) or a
/// dangling trailing `\` is a hard error - so every string is representable
/// (double a backslash, escape a comma) and ambiguity is rejected, not mangled.
/// Empty operands (an unescaped `,,` or a leading/trailing unescaped `,`) error,
/// preserving the "empty operand in set" contract.
fn split_set_operands(rest: &str) -> Result<Vec<String>, &'static str> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut chars = rest.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some(',') => cur.push(','),
                Some('\\') => cur.push('\\'),
                Some(_) => return Err("invalid escape in set operand (use \\, or \\\\)"),
                None => return Err("dangling escape in set operand"),
            },
            ',' => {
                if cur.is_empty() {
                    return Err("empty operand in set");
                }
                out.push(std::mem::take(&mut cur));
            }
            other => cur.push(other),
        }
    }
    if cur.is_empty() {
        return Err("empty operand in set");
    }
    out.push(cur);
    Ok(out)
}

/// Escape LIKE/ILIKE metacharacters in a text-pattern operand: each `\`, `%`, or
/// `_` is prefixed with the SQL escape char `\` so a literal metacharacter in the
/// caller's operand matches the character itself (not a wildcard). The caller wraps
/// the result with unescaped `%` sentinels for the chosen anchor. The escaped string
/// is bound as a parameter; the SQL is rendered with an explicit `ESCAPE '\'` clause.
fn escape_like(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 2);
    for c in raw.chars() {
        if matches!(c, '\\' | '%' | '_') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Parse a query-param value into a typed predicate. Grammar: split at the FIRST `:` into
/// `head`/`rest`; if `head` is a known op token it is that operator (null ops take no
/// operand; scalar ops take `rest` as one operand; set ops split `rest` on UNESCAPED `,`,
/// with `\,` and `\\` the only valid escapes — any other escape, or a trailing `\`, is
/// rejected, so a string operand can carry a literal comma or backslash); otherwise the
/// whole value is an `Eq` operand. Each operand is coerced via `coerce_filter`.
pub fn coerce_predicate(
    column: &str,
    logical_ty: &str,
    raw: &str,
) -> Result<CallerPredicate, FilterError> {
    use control_plane_core::CompareOp::*;
    let bad = |m: &str| FilterError::BadValue(column.to_string(), m.to_string());
    let mk = |op, values| CallerPredicate {
        column: column.to_string(),
        op,
        values,
    };

    let (head, rest) = match raw.split_once(':') {
        Some((h, r)) => (h, Some(r)),
        None => (raw, None),
    };
    let op = match head {
        "eq" => Some(Eq),
        "ne" => Some(Ne),
        "lt" => Some(Lt),
        "le" => Some(Le),
        "gt" => Some(Gt),
        "ge" => Some(Ge),
        "in" => Some(In),
        "nin" => Some(NotIn),
        "isnull" => Some(IsNull),
        "isnotnull" => Some(IsNotNull),
        "between" => Some(Between),
        "contains" => Some(Contains),
        "startswith" => Some(StartsWith),
        "endswith" => Some(EndsWith),
        _ => None,
    };

    match op {
        // Not a recognized op token: the whole value is an Eq operand.
        None => Ok(mk(Eq, vec![coerce_filter(column, logical_ty, raw)?])),
        Some(o @ (IsNull | IsNotNull)) => {
            if matches!(rest, Some(r) if !r.is_empty()) {
                return Err(bad("isnull/isnotnull take no operand"));
            }
            Ok(mk(o, vec![]))
        }
        Some(o @ (In | NotIn)) => {
            let r = rest.ok_or_else(|| bad("in/nin require operands"))?;
            if r.is_empty() {
                return Err(bad("in/nin require at least one operand"));
            }
            let parts = split_set_operands(r).map_err(bad)?;
            let mut values = Vec::with_capacity(parts.len());
            for part in parts {
                values.push(coerce_filter(column, logical_ty, &part)?);
            }
            Ok(mk(o, values))
        }
        Some(Between) => {
            let r = rest.ok_or_else(|| bad("between requires two operands"))?;
            let parts = split_set_operands(r).map_err(bad)?;
            if parts.len() != 2 {
                return Err(bad("between requires exactly two operands (lo,hi)"));
            }
            let mut values = Vec::with_capacity(2);
            for part in parts {
                values.push(coerce_filter(column, logical_ty, &part)?);
            }
            Ok(mk(Between, values))
        }
        Some(o @ (Contains | StartsWith | EndsWith)) => {
            let r = rest.ok_or_else(|| bad("text-pattern operator requires an operand"))?;
            // Carry the source error — `clippy::map_err_ignore` is enforced; a bare
            // `|_|` that drops `e` fails the lint gate (mirror `coerce_filter`'s style).
            let repr = json_repr_of(logical_ty).map_err(|e| {
                FilterError::BadValue(column.to_string(), format!("unknown logical type: {}", e.0))
            })?;
            if !matches!(repr, JsonRepr::PlainString) {
                return Err(bad("text-pattern operators apply to string properties only"));
            }
            let esc = escape_like(r);
            let pattern = match o {
                Contains => format!("%{esc}%"),
                StartsWith => format!("{esc}%"),
                EndsWith => format!("%{esc}"),
                // The outer pattern guarantees o ∈ {Contains,StartsWith,EndsWith};
                // keep the build total without a panic per the panic-safety lints.
                _ => format!("%{esc}%"),
            };
            Ok(mk(o, vec![SqlValue::Text(pattern)]))
        }
        // Scalar ops (eq/ne/lt/le/gt/ge): exactly one operand = `rest`.
        Some(o) => {
            let r = rest.ok_or_else(|| bad("operator requires an operand"))?;
            Ok(mk(o, vec![coerce_filter(column, logical_ty, r)?]))
        }
    }
}
