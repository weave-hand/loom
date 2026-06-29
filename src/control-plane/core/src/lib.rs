//! loom control-plane core: traits, domain types, and the error model.
//! No I/O — adapters (memory, postgres) implement these against a backend.

mod acl;
mod auth;
mod catalog;
mod compact_job;
mod error;
mod flush;
mod gc;
mod identity;
mod lineage;
mod logical_type;
mod ontology;
mod page;
mod queue;
pub mod snapshot;
mod transaction;
mod vector_index;
mod vector_index_job;

pub use acl::{
    Acl, Action, CompareOp, Decision, Effect, Policy, PolicyTarget, RoleId, RowFilter, ScalarValue,
    SubjectId, validate_row_filter,
};
pub use auth::{Auth, NewUser, PasswordCredential};
pub use catalog::{Catalog, ColumnDef, FileRef, Snapshot, SnapshotId, TableRef, TableSchema};
pub use compact_job::{COMPACT_JOB_KIND, CompactJob};
pub use error::{ControlPlaneError, Result};
pub use flush::{FLUSH_JOB_KIND, FlushJob};
pub use gc::{GC_JOB_KIND, GcJob};
pub use identity::{DatasetId, LOOM_DATASET_NAMESPACE, LOOM_TYPE_NAMESPACE, TypeId};
pub use lineage::{DatasetRef, EventType, Lineage, LineageEvent, RunId};
pub use logical_type::{
    BaseType, JsonRepr, UnknownLogicalType, json_repr_of, resolve_logical, satisfies,
};
pub use ontology::{
    ActionDef, ActionKind, ActionName, Aggregation, Cardinality, DerivedPropertyDef, LinkBacking,
    LinkDef, ObjectType, Ontology, ParamDef, PropertyDef, TypeName, VectorIndexDef,
};
pub use page::{Cursor, Page, PageReq};
pub use queue::{Job, JobFailure, JobId, NewJob, Queue, RetryPolicy};
pub use snapshot::{ColumnSpec, ColumnStat, DataFile, FileFormat, StatValue};
pub use transaction::{ControlPlane, Tx};
pub use vector_index::{
    FlatIndex, HnswIndex, IndexKind, IndexSpec, IvfFlatIndex, Metric, VectorIndex, VectorKey,
    decode, distance,
};
pub use vector_index_job::{BUILD_VECTOR_INDEX_JOB_KIND, BuildVectorIndexJob};
