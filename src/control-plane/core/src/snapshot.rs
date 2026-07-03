//! Inputs for the native register-only snapshot-commit primitive (the caller writes
//! the data files; loom writes the catalog rows). Format-neutral: the active
//! table-format adapter (Iceberg) encodes these into its physical catalog.

/// A column for `TableTx::create_table`. `ty` is a loom LOGICAL type name (canonical:
/// "integer"/"long"/"double"/"boolean"/"string"/"date"/"timestamp", or a known
/// alias). The active adapter maps it to its physical type string.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ColumnSpec {
    pub name: String,
    pub ty: String,
    pub nullable: bool,
}

/// A typed scalar stat bound. Format-neutral: each adapter encodes it its own way
/// (Iceberg → typed binary lower/upper bound). Not `Eq` (carries floats).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum StatValue {
    Bool(bool),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Str(String),
}

/// Per-column statistics for one data file. `value_count` is NOT stored: it is
/// derivable (`record_count − null_count`) and each format counts differently, so the
/// adapter derives it (Iceberg's value count includes nulls).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ColumnStat {
    pub column_name: String,
    pub null_count: i64,
    pub column_size_bytes: i64,
    pub min: Option<StatValue>,
    pub max: Option<StatValue>,
}

/// The on-storage format of a registered data file. Explicit (not assumed Parquet)
/// because formats like Iceberg record it per file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FileFormat {
    Parquet,
}

/// A data file the caller has already written to object storage.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DataFile {
    pub path: String,
    pub path_is_relative: bool,
    pub file_format: FileFormat,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub column_stats: Vec<ColumnStat>,
    /// Parquet footer length — a physical-Parquet detail some formats persist
    /// (Iceberg ignores it). `Some` for Parquet files.
    pub parquet_footer_size: Option<i64>,
}
