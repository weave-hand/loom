//! loom engine-serving: the DataFusion execution tier for Iceberg reads. Builds a
//! SessionContext over the mirror's live tables and runs compiled, param-inlined
//! SQL, returning Arrow-58 IPC bytes. Hosted by the `engine` binary and reusable by
//! transform/compaction. See
//! docs/superpowers/specs/2026-06-24-engine-serving-execution-wire-design.md.

pub mod provider;
pub mod serving;

pub use provider::PgTableProvider;
pub use serving::{
    EngineServingError, IcebergMirrorTableProvider, execute_query, execute_query_to_ipc,
    prune_files, register_iceberg_table,
};
