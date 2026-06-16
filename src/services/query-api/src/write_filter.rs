//! Pure, I/O-free evaluation of a stored `RowFilter` against a concrete row being
//! inserted by an action, plus the fine-grained Write-policy gate. The write-side
//! analog of the read path's `RowFilter`→SQL pushdown: here we have ONE known row
//! (the action's columns+values), so we evaluate the predicate in memory.
//!
//! Truth is three-valued (`Option<bool>`): `Some(true)`/`Some(false)` are known,
//! `None` is SQL UNKNOWN (a NULL cell under a value op, a type mismatch, or a failed
//! temporal parse). The gate is fail-closed — a row is allowed only if the filter is
//! `Some(true)`, mirroring "an UNKNOWN `WHERE` row is excluded from a read". Coercion
//! matches the read side exactly: numeric `Int`↔`Double`, and ISO-string operands
//! against `Date`/`Timestamp` cells.
//! (Caveat: a `Double` NaN yields UNKNOWN here via `partial_cmp`, where DuckDB
//! would treat NaN as orderable — write filters are not expected to carry NaN.)

use std::collections::BTreeMap;

use control_plane_core::{CompareOp, RowFilter, ScalarValue};

use crate::serving::SqlValue;

/// Compare one inserted cell against a `RowFilter` leaf operand, SQL-faithfully.
/// `Some(bool)` is a known truth value; `None` is UNKNOWN. `IsNull`/`IsNotNull`
/// inspect nullness; every other op on a `Null` cell is UNKNOWN.
pub fn compare_cell(cell: &SqlValue, op: CompareOp, operand: &ScalarValue) -> Option<bool> {
    use CompareOp::*;
    match op {
        IsNull => Some(matches!(cell, SqlValue::Null)),
        IsNotNull => Some(!matches!(cell, SqlValue::Null)),
        // All value ops are UNKNOWN on a NULL cell.
        _ if matches!(cell, SqlValue::Null) => None,
        In => in_list(cell, operand),
        NotIn => not3(in_list(cell, operand)),
        Eq => eq_cell(cell, operand),
        Ne => not3(eq_cell(cell, operand)),
        Lt | Le | Gt | Ge => order_cell(cell, op, operand),
    }
}

/// Three-valued NOT.
fn not3(v: Option<bool>) -> Option<bool> {
    v.map(|b| !b)
}

/// `cell IN (list)` as three-valued OR of `cell = elem` over the list. A non-list
/// operand is UNKNOWN.
fn in_list(cell: &SqlValue, operand: &ScalarValue) -> Option<bool> {
    let items = match operand {
        ScalarValue::List(xs) => xs,
        _ => return None,
    };
    let mut any_unknown = false;
    for elem in items {
        match eq_cell(cell, elem) {
            Some(true) => return Some(true),
            Some(false) => {}
            None => any_unknown = true,
        }
    }
    if any_unknown { None } else { Some(false) }
}

/// Equality of a cell against an operand, coercing read-faithfully. `None` on a
/// type mismatch or a failed temporal parse.
fn eq_cell(cell: &SqlValue, operand: &ScalarValue) -> Option<bool> {
    use std::cmp::Ordering;
    match (cell, operand) {
        (SqlValue::Text(a), ScalarValue::Text(b)) => Some(a == b),
        (SqlValue::Int(a), ScalarValue::Int(b)) => Some(a == b),
        // Avoid a direct float `==` (clippy::float_cmp): compare via partial_cmp.
        (SqlValue::Double(a), ScalarValue::Int(b)) => {
            Some(a.partial_cmp(&(*b as f64)) == Some(Ordering::Equal))
        }
        (SqlValue::Bool(a), ScalarValue::Bool(b)) => Some(a == b),
        (SqlValue::Date(a), ScalarValue::Text(b)) => parse_date(b).map(|d| *a == d),
        (SqlValue::Timestamp(a), ScalarValue::Text(b)) => parse_ts(b).map(|t| *a == t),
        _ => None,
    }
}

/// Ordering comparison (`Lt`/`Le`/`Gt`/`Ge`). Bool ordering and cross-type pairs are
/// undefined (`None`). `Double` coerces an `Int` operand to `f64`; `Date`/`Timestamp`
/// parse an ISO-`Text` operand.
fn order_cell(cell: &SqlValue, op: CompareOp, operand: &ScalarValue) -> Option<bool> {
    use std::cmp::Ordering;
    let ord: Ordering = match (cell, operand) {
        (SqlValue::Text(a), ScalarValue::Text(b)) => a.as_str().cmp(b.as_str()),
        (SqlValue::Int(a), ScalarValue::Int(b)) => a.cmp(b),
        (SqlValue::Double(a), ScalarValue::Int(b)) => a.partial_cmp(&(*b as f64))?,
        (SqlValue::Date(a), ScalarValue::Text(b)) => a.cmp(&parse_date(b)?),
        (SqlValue::Timestamp(a), ScalarValue::Text(b)) => a.cmp(&parse_ts(b)?),
        // Bool ordering and all cross-type pairs are undefined.
        _ => return None,
    };
    Some(match op {
        CompareOp::Lt => ord.is_lt(),
        CompareOp::Le => ord.is_le(),
        CompareOp::Gt => ord.is_gt(),
        CompareOp::Ge => ord.is_ge(),
        _ => return None,
    })
}

/// Evaluate a `RowFilter` against a concrete inserted row (`property name → cell`),
/// with SQL three-valued logic. `Some(true)` means the row satisfies the filter. An
/// absent property reads as a NULL (unset) cell.
pub fn eval(filter: &RowFilter, row: &BTreeMap<&str, &SqlValue>) -> Option<bool> {
    match filter {
        RowFilter::Compare {
            property,
            op,
            value,
        } => {
            let cell = row
                .get(property.as_str())
                .copied()
                .unwrap_or(&SqlValue::Null);
            compare_cell(cell, *op, value)
        }
        RowFilter::Not(x) => not3(eval(x, row)),
        RowFilter::And(xs) => and3(xs.iter().map(|x| eval(x, row))),
        RowFilter::Or(xs) => or3(xs.iter().map(|x| eval(x, row))),
    }
}

/// Three-valued AND: `Some(false)` if any child is false; else `None` if any unknown;
/// else `Some(true)` (empty ⇒ true).
fn and3(it: impl Iterator<Item = Option<bool>>) -> Option<bool> {
    let mut any_unknown = false;
    for v in it {
        match v {
            Some(false) => return Some(false),
            None => any_unknown = true,
            Some(true) => {}
        }
    }
    if any_unknown { None } else { Some(true) }
}

/// Three-valued OR: `Some(true)` if any child is true; else `None` if any unknown;
/// else `Some(false)` (empty ⇒ false).
fn or3(it: impl Iterator<Item = Option<bool>>) -> Option<bool> {
    let mut any_unknown = false;
    for v in it {
        match v {
            Some(true) => return Some(true),
            None => any_unknown = true,
            Some(false) => {}
        }
    }
    if any_unknown { None } else { Some(false) }
}

fn parse_date(s: &str) -> Option<time::Date> {
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    time::Date::parse(s, &fmt).ok()
}

fn parse_ts(s: &str) -> Option<time::PrimitiveDateTime> {
    let fmt = time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
    time::PrimitiveDateTime::parse(s, &fmt).ok()
}
