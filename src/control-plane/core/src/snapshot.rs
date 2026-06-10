//! Inputs for the native DuckLake snapshot-commit primitive (register-only: the
//! caller writes the Parquet, loom writes the catalog rows). See
//! `docs/superpowers/specs/2026-06-09-ducklake-single-catalog-write-recipe.md`.

/// A column for `Tx::create_table`. `ty` is a DuckLake type string ("int64",
/// "varchar", …) — the dialect stored in `ducklake_column.column_type`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: String,
    pub ty: String,
    pub nullable: bool,
}

/// Per-column statistics for one data file (values as strings, matching DuckLake's
/// VARCHAR stat encoding). `min`/`max` are `None` when absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnStat {
    pub column_name: String,
    pub min: Option<String>,
    pub max: Option<String>,
    pub null_count: i64,
    /// Count of non-null values (DuckLake `value_count = num_values - null_count`).
    pub value_count: i64,
    pub column_size_bytes: i64,
}

/// A Parquet file the caller has already written to object storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataFile {
    pub path: String,
    pub path_is_relative: bool,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub footer_size: i64,
    pub column_stats: Vec<ColumnStat>,
}
