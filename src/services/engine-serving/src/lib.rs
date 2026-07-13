//! loom engine-serving: the DataFusion execution tier for Iceberg reads. Builds a
//! SessionContext over the mirror's live tables and runs compiled, param-inlined
//! SQL, returning Arrow-58 IPC bytes. Hosted by the `engine` binary and reusable by
//! transform/compaction. See
//! docs/superpowers/specs/2026-06-24-engine-serving-execution-wire-design.md.

pub mod action_writer;
pub mod consolidate;
pub mod feed;
pub mod governed;
pub mod mv_delta;
pub mod mv_enrich;
pub mod not_null;
pub mod provider;
pub mod serving;
pub mod vector_search;

pub use action_writer::{IcebergActionWriter, StepWrite};
pub use consolidate::consolidate_table;
pub use feed::{FeedPins, changelog_feed_scan, changelog_feed_scan_at, log_feed_scan_at};
pub use governed::{
    GovernedTableProvider, TablePolicy, execute_governed_sql_stream, policy_for, row_filter_to_expr,
};
pub use mv_delta::mv_delta_scan;
pub use mv_enrich::mv_enrich_scan;
pub use provider::PgTableProvider;
pub use serving::{
    EngineServingError, IcebergMirrorTableProvider, build_serving_provider, execute_query,
    execute_query_stream, prune_files, register_iceberg_table,
};
pub use vector_search::{VectorQuery, merge_topk, vector_search};
