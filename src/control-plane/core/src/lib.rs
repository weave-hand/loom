//! loom control-plane core: traits, domain types, and the error model.
//! No I/O — adapters (memory, postgres) implement these against a backend.

mod error;

pub use error::{ControlPlaneError, Result};
