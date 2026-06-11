//! Compile an ACL RowFilter tree + a column projection + request equality filters
//! into a single read-only SELECT. Identifiers (table, columns) come ONLY from
//! trusted ontology/ACL metadata and are double-quoted; every caller VALUE is a
//! bound `?` parameter (never interpolated) — this is the injection boundary.

use control_plane_core::{CompareOp, RowFilter, ScalarValue, TableRef};

use crate::serving::SqlValue;

/// The value substituted for a masked column. A compile-time constant (never caller
/// data), so inlining it as a SQL literal is not an injection vector.
const MASK_MARKER: &str = "***";

fn quote_ident(id: &str) -> String {
    assert!(
        !id.contains('"'),
        "identifier must not contain a double quote: {id}"
    );
    format!("\"{id}\"")
}

fn scalar(v: &ScalarValue, out: &mut Vec<SqlValue>) {
    match v {
        ScalarValue::Text(s) => out.push(SqlValue::Text(s.clone())),
        ScalarValue::Int(i) => out.push(SqlValue::Int(*i)),
        ScalarValue::Bool(b) => out.push(SqlValue::Bool(*b)),
        ScalarValue::List(_) => unreachable!("lists handled by In/NotIn arm"),
    }
}

fn op_sql(op: CompareOp) -> &'static str {
    match op {
        CompareOp::Eq => "=",
        CompareOp::Ne => "<>",
        CompareOp::Lt => "<",
        CompareOp::Le => "<=",
        CompareOp::Gt => ">",
        CompareOp::Ge => ">=",
        _ => unreachable!("In/NotIn/IsNull/IsNotNull handled separately"),
    }
}

// NOTE: the `panic!`/`unreachable!` arms below encode a CompareOp<->ScalarValue
// invariant the type system does not enforce (e.g. `In` requires a `List` value, a
// scalar op requires a non-list value). For this slice that invariant holds by
// construction. Once row filters become real persisted policy data deserialized from
// the `acl` schema's jsonb, a malformed-but-type-valid filter could reach these arms;
// at that point `compile_select` should become fallible (a typed MalformedPolicy
// error) rather than panic inside a read request. Tracked for the deferred full-ACL
// spec — see the slice design doc's "What this slice is NOT".
fn filter_sql(f: &RowFilter, params: &mut Vec<SqlValue>) -> String {
    match f {
        RowFilter::Compare {
            property,
            op,
            value,
        } => match op {
            CompareOp::In | CompareOp::NotIn => {
                let items = match value {
                    ScalarValue::List(xs) => xs,
                    _ => panic!("In/NotIn requires a list value"),
                };
                let mut placeholders = Vec::with_capacity(items.len());
                for it in items {
                    scalar(it, params);
                    placeholders.push("?");
                }
                let kw = if matches!(op, CompareOp::In) {
                    "IN"
                } else {
                    "NOT IN"
                };
                format!(
                    "({} {} ({}))",
                    quote_ident(property),
                    kw,
                    placeholders.join(", ")
                )
            }
            CompareOp::IsNull => format!("({} IS NULL)", quote_ident(property)),
            CompareOp::IsNotNull => format!("({} IS NOT NULL)", quote_ident(property)),
            _ => {
                scalar(value, params);
                format!("({} {} ?)", quote_ident(property), op_sql(*op))
            }
        },
        RowFilter::And(xs) => format!(
            "({})",
            xs.iter()
                .map(|x| filter_sql(x, params))
                .collect::<Vec<_>>()
                .join(" AND ")
        ),
        RowFilter::Or(xs) => format!(
            "({})",
            xs.iter()
                .map(|x| filter_sql(x, params))
                .collect::<Vec<_>>()
                .join(" OR ")
        ),
        RowFilter::Not(x) => format!("(NOT {})", filter_sql(x, params)),
    }
}

/// `allowed_cols` must be non-empty (caller enforces). `row_filters` and `eq_filters`
/// are ANDed together as conjuncts.
pub fn compile_select(
    table: &TableRef,
    allowed_cols: &[String],
    mask_cols: &[String],
    row_filters: &[RowFilter],
    eq_filters: &[(String, SqlValue)],
    limit: u32,
) -> (String, Vec<SqlValue>) {
    let mut params = Vec::new();
    let cols = allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                // Masked: emit the constant marker, never the column's value.
                format!("'{MASK_MARKER}' AS {}", quote_ident(c))
            } else {
                quote_ident(c)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let from = format!(
        "{}.{}",
        quote_ident(&table.schema),
        quote_ident(&table.name)
    );

    let mut conjuncts: Vec<String> = Vec::new();
    for f in row_filters {
        conjuncts.push(filter_sql(f, &mut params));
    }
    for (col, val) in eq_filters {
        conjuncts.push(format!("({} = ?)", quote_ident(col)));
        params.push(val.clone());
    }

    let mut sql = format!("SELECT {cols} FROM {from}");
    if !conjuncts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conjuncts.join(" AND "));
    }
    sql.push_str(&format!(" LIMIT {limit}"));
    (sql, params)
}
