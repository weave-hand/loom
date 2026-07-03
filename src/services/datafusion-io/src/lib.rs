//! Table-format <-> DataFusion IO: write Arrow batches as size-targeted Snappy Parquet
//! into object storage (with the per-file `DataFile` stats the snapshot-commit
//! primitive needs), read a table's Parquet back as a DataFusion table,
//! and infer loom logical column specs from an Arrow schema. Also hosts the one
//! authoritative Arrow-IPC `decode_ipc` and the shared job-service `JobConfig`.
//! Shared across the `ingest`, `engine-serving`, `transform`, and `worker` services.

pub mod infer;
mod ipc;
mod job_config;
pub mod scan;
pub mod write;

pub use infer::{
    InferError, arrow_logical_type, infer_columns, logical_arrow_schema, logical_arrow_type,
};
pub use ipc::decode_ipc;
pub use job_config::JobConfig;
pub use scan::{ScanError, object_store_url_for, register_empty_table, scan_table};
pub use write::{
    WriteConfig, WriteError, WrittenFile, absolute_data_files, estimate_partitions,
    file_stats_from_bytes, write_dataset,
};
