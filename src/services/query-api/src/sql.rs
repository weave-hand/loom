//! Compile an ACL RowFilter tree + a column projection + request equality filters
//! into a single read-only SELECT. Identifiers (table, columns) come ONLY from
//! trusted ontology/ACL metadata and are double-quoted; every caller VALUE is a
//! bound `?` parameter (never interpolated) — this is the injection boundary.

use control_plane_core::{
    CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef, validate_row_filter,
};

use crate::serving::SqlValue;

/// The value substituted for a masked column. A compile-time constant (never caller
/// data), so inlining it as a SQL literal is not an injection vector.
const MASK_MARKER: &str = "***";

/// A row filter that violated the CompareOp<->ScalarValue invariant (e.g. malformed
/// persisted policy data). Surfaced by the query API as an opaque 500, never a panic.
#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("malformed row filter: {0}")]
    MalformedFilter(String),
}

fn quote_ident(id: &str) -> String {
    assert!(
        !id.contains('"'),
        "identifier must not contain a double quote: {id}"
    );
    format!("\"{id}\"")
}

/// A column reference, optionally table-qualified. `alias` empty -> unqualified.
fn col_ref(alias: &str, id: &str) -> String {
    if alias.is_empty() {
        quote_ident(id)
    } else {
        format!("{alias}.{}", quote_ident(id))
    }
}

fn scalar(v: &ScalarValue, out: &mut Vec<SqlValue>) {
    match v {
        ScalarValue::Text(s) => out.push(SqlValue::Text(s.clone())),
        ScalarValue::Int(i) => out.push(SqlValue::Int(*i)),
        ScalarValue::Bool(b) => out.push(SqlValue::Bool(*b)),
        ScalarValue::List(_) => unreachable!("validated by validate_row_filter"),
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
        _ => unreachable!("validated by validate_row_filter"),
    }
}

/// PRECONDITION: `f` has passed `control_plane_core::validate_row_filter` (the sole
/// caller, `compile_select`, enforces this up front). The `unreachable!` arms below —
/// and those in `scalar`/`op_sql` — rely on that CompareOp<->ScalarValue invariant; a
/// caller that skips validation could turn them into a panic.
fn filter_sql(f: &RowFilter, alias: &str, params: &mut Vec<SqlValue>) -> String {
    match f {
        RowFilter::Compare {
            property,
            op,
            value,
        } => match op {
            CompareOp::In | CompareOp::NotIn => {
                let items = match value {
                    ScalarValue::List(xs) => xs,
                    _ => unreachable!("validated by validate_row_filter"),
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
                    col_ref(alias, property),
                    kw,
                    placeholders.join(", ")
                )
            }
            CompareOp::IsNull => format!("({} IS NULL)", col_ref(alias, property)),
            CompareOp::IsNotNull => format!("({} IS NOT NULL)", col_ref(alias, property)),
            _ => {
                scalar(value, params);
                format!("({} {} ?)", col_ref(alias, property), op_sql(*op))
            }
        },
        RowFilter::And(xs) => format!(
            "({})",
            xs.iter()
                .map(|x| filter_sql(x, alias, params))
                .collect::<Vec<_>>()
                .join(" AND ")
        ),
        RowFilter::Or(xs) => format!(
            "({})",
            xs.iter()
                .map(|x| filter_sql(x, alias, params))
                .collect::<Vec<_>>()
                .join(" OR ")
        ),
        RowFilter::Not(x) => format!("(NOT {})", filter_sql(x, alias, params)),
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
) -> Result<(String, Vec<SqlValue>), CompileError> {
    // Validate each ACL filter's shape up front; afterwards the SQL-building arms
    // below cannot hit a CompareOp<->ScalarValue mismatch.
    for f in row_filters {
        validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
    }
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
        conjuncts.push(filter_sql(f, "", &mut params));
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
    Ok((sql, params))
}

/// Compile a governed link traversal into a single read-only SELECT DISTINCT.
/// `f` aliases the source table, `t` the target; for a join-table backing, `j` is
/// the mapping table. Target columns are projected (masked ones emit the marker);
/// source eq-filters and both types' row filters are ANDed; every VALUE is bound.
///
/// PRECONDITION mirrors `compile_select`: row filters are validated up front so the
/// `filter_sql` invariant arms cannot panic.
#[allow(clippy::too_many_arguments)]
pub fn compile_traversal(
    from_table: &TableRef,
    to_table: &TableRef,
    backing: &LinkBacking,
    allowed_cols: &[String],
    mask_cols: &[String],
    source_filters: &[RowFilter],
    target_filters: &[RowFilter],
    source_eq_filters: &[(String, SqlValue)],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    for f in source_filters.iter().chain(target_filters) {
        validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
    }
    let mut params = Vec::new();

    let cols = allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                format!("'{MASK_MARKER}' AS {}", quote_ident(c))
            } else {
                format!("t.{}", quote_ident(c))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    let to_from = format!(
        "{}.{}",
        quote_ident(&to_table.schema),
        quote_ident(&to_table.name)
    );
    let from_from = format!(
        "{}.{}",
        quote_ident(&from_table.schema),
        quote_ident(&from_table.name)
    );
    let join = match backing {
        LinkBacking::ForeignKey {
            from_column,
            to_column,
        } => format!(
            "JOIN {from_from} f ON f.{} = t.{}",
            quote_ident(from_column),
            quote_ident(to_column),
        ),
        LinkBacking::JoinTable {
            table,
            from_key,
            from_column,
            to_column,
            to_key,
        } => {
            let jt = format!(
                "{}.{}",
                quote_ident(&table.schema),
                quote_ident(&table.name)
            );
            format!(
                "JOIN {jt} j ON j.{} = t.{} JOIN {from_from} f ON f.{} = j.{}",
                quote_ident(to_column),
                quote_ident(to_key),
                quote_ident(from_key),
                quote_ident(from_column),
            )
        }
    };

    let mut conjuncts: Vec<String> = Vec::new();
    for (col, val) in source_eq_filters {
        conjuncts.push(format!("(f.{} = ?)", quote_ident(col)));
        params.push(val.clone());
    }
    for f in source_filters {
        conjuncts.push(filter_sql(f, "f", &mut params));
    }
    for f in target_filters {
        conjuncts.push(filter_sql(f, "t", &mut params));
    }

    let mut sql = format!("SELECT DISTINCT {cols} FROM {to_from} t {join}");
    if !conjuncts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conjuncts.join(" AND "));
    }
    sql.push_str(&format!(" LIMIT {limit}"));
    Ok((sql, params))
}
