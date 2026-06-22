//! Per-column Parquet-footer stats for the Iceberg mirror, computed against
//! parquet57 (iceberg 0.9's parquet). This DUPLICATES the merge logic in
//! datafusion_io::write::file_stats_from_bytes by necessity: that crate is on
//! parquet 58 and the parquet `Statistics` enum is a distinct type per major, so
//! the function cannot be shared across the version boundary. Output is the
//! version-neutral core::snapshot::{ColumnStat, StatValue}.

use bytes::Bytes; // impl parquet57 ChunkReader; the type iceberg's InputFile::read() yields
use control_plane_core::snapshot::{ColumnStat, StatValue};
use control_plane_core::{ControlPlaneError, Result};
use parquet57::file::reader::{FileReader, SerializedFileReader};
use parquet57::file::statistics::Statistics;

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
/// into a file-wide min/max. `None` for mismatched/float-NaN cases (keeps current).
fn stat_partial_cmp(a: &StatValue, b: &StatValue) -> Option<std::cmp::Ordering> {
    use StatValue::*;
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

/// Merge typed min/max + null/size counts across ALL row groups for each column
/// index, mirroring `datafusion_io::write::file_stats_from_bytes` against parquet57.
/// `column_names[i]` is the name recorded for row-group column `i` (Iceberg writes
/// columns in schema order, so pass `columns_of(table)`-ordered names). Takes the
/// `Bytes` by value so it feeds `SerializedFileReader::new` directly.
pub fn column_stats_from_parquet(bytes: Bytes, column_names: &[String]) -> Result<Vec<ColumnStat>> {
    let reader =
        SerializedFileReader::new(bytes).map_err(|e| ControlPlaneError::Backend(Box::new(e)))?;
    let meta = reader.metadata();
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
                    min = match min {
                        Some(cur)
                            if stat_partial_cmp(&cur, &b) != Some(std::cmp::Ordering::Greater) =>
                        {
                            Some(cur)
                        }
                        _ => Some(b),
                    };
                }
                if let Some(b) = max_stat(stats) {
                    max = match max {
                        Some(cur)
                            if stat_partial_cmp(&cur, &b) != Some(std::cmp::Ordering::Less) =>
                        {
                            Some(cur)
                        }
                        _ => Some(b),
                    };
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
    Ok(out)
}

/// Render a stat bound as the text the `data_file_column_stat.min_value/max_value`
/// columns store. The read path (Task 3) re-types it via `stat_from_text`.
pub fn stat_to_text(v: &StatValue) -> String {
    match v {
        StatValue::Bool(b) => b.to_string(),
        StatValue::I32(x) => x.to_string(),
        StatValue::I64(x) => x.to_string(),
        StatValue::F32(x) => x.to_string(),
        StatValue::F64(x) => x.to_string(),
        StatValue::Str(s) => s.clone(),
    }
}

/// Re-type a stored bound by the column's iceberg type. Only the clean primitive
/// types carry bounds; date/timestamp/float/other -> None (never pruned on).
pub fn stat_from_text(text: &str, iceberg_type: &str) -> Option<StatValue> {
    match iceberg_type.trim().to_ascii_lowercase().as_str() {
        "int" => text.parse().ok().map(StatValue::I32),
        "long" => text.parse().ok().map(StatValue::I64),
        "double" => text.parse().ok().map(StatValue::F64),
        "boolean" => text.parse().ok().map(StatValue::Bool),
        "string" => Some(StatValue::Str(text.to_string())),
        _ => None,
    }
}
