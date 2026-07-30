//! Per-column Parquet-footer statistics, merged across row groups and emitted as the
//! version-neutral `control_plane_core::snapshot::{ColumnStat, StatValue}`.
//!
//! This is the single place in the tree that matches on `parquet`'s `Statistics`
//! enum, so a future parquet major bump has exactly one file to touch. Both readers
//! that need it — the Iceberg mirror's landing/commit path
//! (`control_plane_postgres::iceberg_stats`) and the DataFusion write path
//! (`datafusion_io::write::file_stats_from_bytes`) — call in here.
//!
//! The entry point takes already-parsed `ParquetMetaData` rather than bytes: the two
//! callers have incompatible error types and would otherwise need a third, and the
//! datafusion-io caller needs the same metadata for its own row count, so a
//! bytes-taking helper would parse the footer twice.

use std::cmp::Ordering;

use control_plane_core::snapshot::{ColumnStat, StatValue};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;

/// Typed lower bound of a row-group column's `Statistics`, as a neutral `StatValue`.
/// Only the primitive types loom prunes on carry a bound; others -> `None`.
fn min_stat(stats: &Statistics) -> Option<StatValue> {
    match stats {
        Statistics::Boolean(s) => s.min_opt().map(|v| StatValue::Bool(*v)),
        Statistics::Int32(s) => s.min_opt().map(|v| StatValue::I32(*v)),
        Statistics::Int64(s) => s.min_opt().map(|v| StatValue::I64(*v)),
        Statistics::Float(s) => s.min_opt().map(|v| StatValue::F32(*v)),
        Statistics::Double(s) => s.min_opt().map(|v| StatValue::F64(*v)),
        Statistics::ByteArray(s) => s
            .min_opt()
            .and_then(|v| v.as_utf8().ok().map(|s| StatValue::Str(s.to_string()))),
        _ => None,
    }
}

/// Typed upper bound of a row-group column's `Statistics`, as a neutral `StatValue`.
fn max_stat(stats: &Statistics) -> Option<StatValue> {
    match stats {
        Statistics::Boolean(s) => s.max_opt().map(|v| StatValue::Bool(*v)),
        Statistics::Int32(s) => s.max_opt().map(|v| StatValue::I32(*v)),
        Statistics::Int64(s) => s.max_opt().map(|v| StatValue::I64(*v)),
        Statistics::Float(s) => s.max_opt().map(|v| StatValue::F32(*v)),
        Statistics::Double(s) => s.max_opt().map(|v| StatValue::F64(*v)),
        Statistics::ByteArray(s) => s
            .max_opt()
            .and_then(|v| v.as_utf8().ok().map(|s| StatValue::Str(s.to_string()))),
        _ => None,
    }
}

/// Partial order over same-typed `StatValue`s; used to fold per-row-group bounds
/// into a file-wide min/max. `None` for mismatched variants and float NaN, in which
/// case the fold keeps the bound it already has.
#[must_use]
pub fn stat_partial_cmp(a: &StatValue, b: &StatValue) -> Option<Ordering> {
    use StatValue::{Bool, F32, F64, I32, I64, Str};
    match (a, b) {
        (Bool(x), Bool(y)) => x.partial_cmp(y),
        (I32(x), I32(y)) => x.partial_cmp(y),
        (I64(x), I64(y)) => x.partial_cmp(y),
        (F32(x), F32(y)) => x.partial_cmp(y),
        (F64(x), F64(y)) => x.partial_cmp(y),
        (Str(x), Str(y)) => x.partial_cmp(y),
        _ => None,
    }
}

/// Fold a candidate bound into the running one. `replace_when` is the ordering of
/// `current` relative to `candidate` that means the candidate wins: `Ordering::Greater`
/// for a minimum fold (the running min is above the candidate), `Ordering::Less` for a
/// maximum fold. Incomparable pairs — mismatched variants, or a float NaN — compare as
/// `None` and therefore keep `current`, which is what the two hand-copied merges this
/// crate replaced did.
#[expect(
    clippy::unnecessary_wraps,
    reason = "kept Option<StatValue> to match the min/max accumulator type at both call sites"
)]
fn fold_bound(
    current: Option<StatValue>,
    candidate: StatValue,
    replace_when: Ordering,
) -> Option<StatValue> {
    match current {
        Some(cur) if stat_partial_cmp(&cur, &candidate) != Some(replace_when) => Some(cur),
        _ => Some(candidate),
    }
}

/// Merge typed min/max + null/size counts across ALL row groups for each column
/// index. `column_names[i]` is the name recorded for row-group column `i`, so the
/// slice both selects the columns (by position) and labels them.
///
/// # Panics
///
/// Panics when `column_names` is longer than the file's column count — index `i`
/// must address a real row-group column.
#[must_use]
pub fn column_stats(meta: &ParquetMetaData, column_names: &[String]) -> Vec<ColumnStat> {
    let mut out = Vec::with_capacity(column_names.len());
    for (i, name) in column_names.iter().enumerate() {
        let mut null_count = 0i64;
        let mut column_size_bytes = 0i64;
        let mut min: Option<StatValue> = None;
        let mut max: Option<StatValue> = None;
        for rg in meta.row_groups() {
            let col = rg.column(i);
            column_size_bytes += col.compressed_size();
            if let Some(stats) = col.statistics() {
                null_count += stats.null_count_opt().unwrap_or(0) as i64;
                if let Some(b) = min_stat(stats) {
                    min = fold_bound(min, b, Ordering::Greater);
                }
                if let Some(b) = max_stat(stats) {
                    max = fold_bound(max, b, Ordering::Less);
                }
            }
        }
        out.push(ColumnStat {
            column_name: name.clone(),
            null_count,
            column_size_bytes,
            min,
            max,
        });
    }
    out
}
