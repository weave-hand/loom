//! loom control-plane core: traits, domain types, and the error model.
//! No I/O — adapters (memory, postgres) implement these against a backend.

mod acl;
mod catalog;
mod error;
mod lineage;
mod ontology;
mod queue;
mod transaction;

pub use acl::{
    Acl, Action, CompareOp, Decision, Policy, PolicyTarget, RoleId, RowFilter, ScalarValue,
    SubjectId,
};
pub use catalog::{Catalog, ColumnDef, FileRef, Snapshot, SnapshotId, TableRef, TableSchema};
pub use error::{ControlPlaneError, Result};
pub use lineage::{DatasetRef, EventType, Lineage, LineageEvent, RunId};
pub use ontology::{Cardinality, LinkDef, ObjectType, Ontology, PropertyDef, TypeName};
pub use queue::{Job, JobFailure, JobId, NewJob, Queue, RetryPolicy};
pub use transaction::{ControlPlane, Tx};
