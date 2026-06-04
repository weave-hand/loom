//! loom control-plane core: traits, domain types, and the error model.
//! No I/O — adapters (memory, postgres) implement these against a backend.

mod error;
mod queue;
mod transaction;

pub use error::{ControlPlaneError, Result};
pub use queue::{Job, JobFailure, JobId, NewJob, Queue, RetryPolicy};
pub use transaction::{ControlPlane, Tx};
