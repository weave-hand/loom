//! loom transform service: queue-driven SQL transforms. A worker reads input
//! DuckLake table(s) with DataFusion, runs a SQL query, and commits the result as a
//! new snapshot + lineage. See docs/superpowers/specs/.

pub mod backend;
pub mod compact;
pub mod conform;
pub mod handler;
pub mod run;
pub mod typed;

pub use backend::{TransformBackend, parse_transform_backend};
pub use compact::{CompactConfig, CompactError, compact_table, small_files};
pub use datafusion_io::WriteConfig;
pub use handler::{transform_handler, typed_transform_handler};
pub use run::{OutputMode, TransformError, TransformInput, TransformRequest, run_transform};
pub use typed::{TypedTransformError, run_typed_transform};
