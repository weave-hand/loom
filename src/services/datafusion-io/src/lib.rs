//! DuckLake <-> DataFusion IO: write Arrow batches as size-targeted Snappy Parquet
//! into object storage (with the per-file DuckLake stats the snapshot-commit
//! primitive needs), read a DuckLake table's Parquet back as a DataFusion table,
//! and infer loom logical column specs from an Arrow schema. Shared by `ingest` and
//! `transform`.

pub mod infer;
pub mod scan;
pub mod write;

pub use infer::{
    InferError, arrow_logical_type, infer_columns, logical_arrow_schema, logical_arrow_type,
};
pub use scan::{ScanError, register_empty_table, scan_table};
pub use write::{
    WriteConfig, WriteError, WrittenFile, estimate_partitions, file_stats_from_bytes, write_dataset,
};
