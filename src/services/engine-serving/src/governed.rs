//! The governed external-SQL path: translates loom's [`control_plane_core::RowFilter`]
//! ACL policy into DataFusion `Expr`s so the engine can apply row-level governance
//! directly in the query plan, matching `query-api::sql::filter_sql`'s SQL semantics.
//! See docs/superpowers/specs/2026-06-24-engine-serving-execution-wire-design.md.

use control_plane_core::{CompareOp, RowFilter, ScalarValue, validate_row_filter};
use datafusion::logical_expr::not;
use datafusion::prelude::{Expr, col, lit};
use datafusion::scalar::ScalarValue as DfScalar;

use crate::serving::EngineServingError;

/// A leaf `ScalarValue` (never a `List`) → a DataFusion literal `Expr`.
fn scalar_lit(v: &ScalarValue) -> Result<Expr, EngineServingError> {
    let s = match v {
        ScalarValue::Text(s) => DfScalar::Utf8(Some(s.clone())),
        ScalarValue::Int(i) => DfScalar::Int64(Some(*i)),
        ScalarValue::Bool(b) => DfScalar::Boolean(Some(*b)),
        ScalarValue::List(_) => {
            return Err(EngineServingError::Engine(
                "row filter: unexpected list scalar in leaf position".into(),
            ));
        }
    };
    Ok(lit(s))
}

/// Translate a `RowFilter` tree into a DataFusion `Expr`, matching
/// `query-api::sql::filter_sql` semantics. Fails closed: an invariant violation
/// (validated by `validate_row_filter`) returns an error, never a panic.
pub fn row_filter_to_expr(f: &RowFilter) -> Result<Expr, EngineServingError> {
    validate_row_filter(f, None).map_err(EngineServingError::Engine)?;
    build_expr(f)
}

fn build_expr(f: &RowFilter) -> Result<Expr, EngineServingError> {
    match f {
        RowFilter::Compare {
            property,
            op,
            value,
        } => {
            let c = col(property);
            match op {
                CompareOp::Eq => Ok(c.eq(scalar_lit(value)?)),
                CompareOp::Ne => Ok(c.not_eq(scalar_lit(value)?)),
                CompareOp::Lt => Ok(c.lt(scalar_lit(value)?)),
                CompareOp::Le => Ok(c.lt_eq(scalar_lit(value)?)),
                CompareOp::Gt => Ok(c.gt(scalar_lit(value)?)),
                CompareOp::Ge => Ok(c.gt_eq(scalar_lit(value)?)),
                CompareOp::In | CompareOp::NotIn => {
                    let ScalarValue::List(items) = value else {
                        return Err(EngineServingError::Engine(
                            "row filter: In/NotIn requires a list value".into(),
                        ));
                    };
                    let list = items
                        .iter()
                        .map(scalar_lit)
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(c.in_list(list, matches!(op, CompareOp::NotIn)))
                }
                CompareOp::IsNull => Ok(c.is_null()),
                CompareOp::IsNotNull => Ok(c.is_not_null()),
            }
        }
        RowFilter::And(xs) => fold_bool(xs, true),
        RowFilter::Or(xs) => fold_bool(xs, false),
        RowFilter::Not(x) => Ok(not(build_expr(x)?)),
    }
}

/// AND-fold (`and_identity=true`) or OR-fold (`false`) a slice of sub-filters.
/// An empty slice yields the identity literal (`true` for AND, `false` for OR).
fn fold_bool(xs: &[RowFilter], and: bool) -> Result<Expr, EngineServingError> {
    let mut acc: Option<Expr> = None;
    for x in xs {
        let e = build_expr(x)?;
        acc = Some(match acc {
            None => e,
            Some(a) if and => a.and(e),
            Some(a) => a.or(e),
        });
    }
    Ok(acc.unwrap_or_else(|| lit(and)))
}

/// AND-fold a slice of top-level row filters into one optional predicate.
#[expect(
    dead_code,
    reason = "consumed by the GovernedTableProvider filter pushdown landing in a later task of this feature"
)]
pub(crate) fn row_filters_conjunction(
    fs: &[RowFilter],
) -> Result<Option<Expr>, EngineServingError> {
    if fs.is_empty() {
        return Ok(None);
    }
    Ok(Some(fold_bool(fs, true)?))
}
