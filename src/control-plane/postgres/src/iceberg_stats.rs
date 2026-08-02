//! Per-column Parquet-footer stats for the Iceberg mirror. The merge itself lives in
//! the shared `parquet_stats` crate — the tree's single reader of parquet's
//! `Statistics` enum, shared with `datafusion_io::write::file_stats_from_bytes` — so
//! this module is just the mirror's `Bytes`-in entry point plus the text codec the
//! `data_file_column_stat` columns are stored through. Output is the version-neutral
//! core::snapshot::{ColumnStat, StatValue}.

use bytes::Bytes; // impl parquet ChunkReader; the type iceberg's InputFile::read() yields
use control_plane_core::Result;
use control_plane_core::snapshot::{ColumnStat, StatValue};
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::backend;

/// Merge typed min/max + null/size counts across ALL row groups for each column
/// index. `column_names[i]` is the name recorded for row-group column `i` (Iceberg
/// writes columns in schema order, so pass `columns_of(table)`-ordered names). Takes
/// the `Bytes` by value so it feeds `SerializedFileReader::new` directly.
pub fn column_stats_from_parquet(bytes: Bytes, column_names: &[String]) -> Result<Vec<ColumnStat>> {
    let reader = SerializedFileReader::new(bytes).map_err(backend)?;
    Ok(parquet_stats::column_stats(reader.metadata(), column_names))
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
