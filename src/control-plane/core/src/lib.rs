//! loom control-plane core: traits, domain types, and the error model.
//! No I/O — adapters (memory, postgres) implement these against a backend.

mod error;
mod transaction;

pub use error::{ControlPlaneError, Result};
pub use transaction::{ControlPlane, Tx};
