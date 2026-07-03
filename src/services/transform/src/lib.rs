//! loom transform service: queue-driven SQL transforms. A worker reads input
//! table(s) with DataFusion, runs a SQL query, and commits the result as a
//! new snapshot + lineage. See docs/superpowers/specs/.

pub mod compact;
pub mod conform;
pub mod handler;
pub mod run;
pub mod typed;

pub use compact::{CompactConfig, CompactError, compact_table};
pub use datafusion_io::WriteConfig;
pub use handler::{transform_handler, typed_transform_handler};
pub use run::{OutputMode, TransformError, TransformInput, TransformRequest, run_transform};
pub use typed::{TypedTransformError, run_typed_transform};
