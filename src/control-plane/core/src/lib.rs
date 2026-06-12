//! loom control-plane core: traits, domain types, and the error model.
//! No I/O — adapters (memory, postgres) implement these against a backend.

mod acl;
mod catalog;
mod error;
mod lineage;
mod logical_type;
mod ontology;
mod page;
mod queue;
mod snapshot;
mod transaction;

pub use acl::{
    Acl, Action, CompareOp, Decision, Effect, Policy, PolicyTarget, RoleId, RowFilter, ScalarValue,
    SubjectId, validate_row_filter,
};
pub use catalog::{Catalog, ColumnDef, FileRef, Snapshot, SnapshotId, TableRef, TableSchema};
pub use error::{ControlPlaneError, Result};
pub use lineage::{DatasetRef, EventType, Lineage, LineageEvent, RunId};
pub use logical_type::{
    BaseType, JsonRepr, UnknownLogicalType, json_repr_of, resolve_logical, satisfies,
};
pub use ontology::{Cardinality, LinkDef, ObjectType, Ontology, PropertyDef, TypeName};
pub use page::{Cursor, Page, PageReq};
pub use queue::{Job, JobFailure, JobId, NewJob, Queue, RetryPolicy};
pub use snapshot::{ColumnSpec, ColumnStat, DataFile};
pub use transaction::{ControlPlane, Tx};
