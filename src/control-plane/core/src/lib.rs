//! loom control-plane core: traits, domain types, and the error model.
//! No I/O — adapters (memory, postgres) implement these against a backend.

mod acl;
mod auth;
mod catalog;
mod compact_job;
mod constraints;
mod error;
mod flush;
mod gc;
pub mod governed;
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
    ADMIN_ROLE, Acl, Action, CompareOp, Decision, Effect, Policy, PolicyTarget, RoleId, RowFilter,
    ScalarValue, SubjectId, check_grant_target, check_policy_write, validate_row_filter,
};
pub use auth::{
    Auth, NewServiceAccount, NewUser, PasswordCredential, ServiceAccount, ServiceToken, UserSummary,
};
pub use catalog::{Catalog, ColumnDef, FileRef, Snapshot, SnapshotId, TableRef, TableSchema};
pub use compact_job::{COMPACT_JOB_KIND, CompactJob};
pub use constraints::{
    ConstraintRule, ConstraintViolation, LengthConstraint, PropertyConstraints, PropertyValidator,
    RangeConstraint, validate_constraints,
};
pub use error::{ControlPlaneError, Result};
pub use flush::{FLUSH_JOB_KIND, FlushJob};
pub use gc::{GC_JOB_KIND, GcJob};
pub use governed::{GovernedCatalog, GovernedTable};
pub use identity::{
    DatasetId, LOOM_DATASET_NAMESPACE, LOOM_TYPE_NAMESPACE, TYPE_TABLE_BINDING_KIND, TypeId,
    type_table_binding_event,
};
pub use lineage::{
    DatasetRef, EventType, LINEAGE_MAX_DEPTH, Lineage, LineageEvent, RunId, check_depth,
    decode_dataset_cursor, decode_event_cursor, encode_dataset_cursor, encode_event_cursor,
};
pub use logical_type::{
    BaseType, JsonRepr, UnknownLogicalType, json_repr_of, resolve_logical, satisfies,
    vector_list_field,
};
pub use ontology::{
    ActionDef, ActionDefBuilder, ActionKind, ActionName, Aggregation, Assignment, AssignmentSource,
    Cardinality, DerivedPropertyDef, LinkBacking, LinkDef, ObjectType, ObjectTypeBuilder, Ontology,
    ParamDef, PropertyDef, ResultExpectation, TypeName, VectorIndexDef,
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
