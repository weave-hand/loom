//! loom transform service: queue-driven SQL transforms. A worker reads input
//! DuckLake table(s) with DataFusion, runs a SQL query, and commits the result as a
//! new snapshot + lineage. See docs/superpowers/specs/.

pub mod handler;
pub mod run;

pub use handler::transform_handler;
pub use run::{TransformError, TransformRequest, run_transform};
