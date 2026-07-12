//! loom control-plane core: traits, domain types, and the error model.
//! No I/O — adapters (memory, postgres) implement these against a backend.

mod acl;
mod action_downstream;
mod auth;
mod catalog;
mod compact_job;
mod conform;
mod constraints;
mod error;
mod flush;
mod gc;
pub mod governed;
mod identity;
mod job_schedule;
mod lineage;
mod logical_type;
mod ontology;
mod orphan_sweep;
mod page;
mod queue;
pub mod snapshot;
mod stream;
mod stream_consolidate_job;
mod stream_mv_job;
mod transaction;
mod transform_job;
mod transforms;
mod vector_index;
mod vector_index_job;

pub use acl::{
    ADMIN_ROLE, Acl, Action, CompareOp, Decision, Effect, Grant, Policy, PolicyTarget, RoleId,
    RolePolicy, RowFilter, ScalarValue, SubjectId, check_grant_target, check_policy_write,
    validate_row_filter,
};
pub use action_downstream::validate_action_downstream;
pub use auth::{
    Auth, LockoutPolicy, NewServiceAccount, NewUser, PasswordCredential, ServiceAccount,
    ServiceToken, UserSummary,
};
pub use catalog::{
    Catalog, ColumnDef, FileRef, Snapshot, SnapshotId, TableRef, TableSchema, small_files,
};
pub use compact_job::{COMPACT_JOB_KIND, CompactJob};
pub use conform::{Violation, check_conformance};
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
pub use job_schedule::{JobSchedule, SCHEDULABLE_JOB_KINDS, validate_job_schedule};
pub use lineage::{
    DatasetRef, EventType, LINEAGE_MAX_DEPTH, Lineage, LineageEvent, RunId, check_depth,
    decode_dataset_cursor, decode_event_cursor, encode_dataset_cursor, encode_event_cursor,
};
pub use logical_type::{
    BaseType, JsonRepr, UnknownLogicalType, json_repr_of, resolve_logical, satisfies,
    vector_list_field,
};
pub use ontology::{
    ActionDef, ActionDefBuilder, ActionKind, ActionName, ActionStep, Aggregation, Assignment,
    AssignmentSource, Cardinality, DerivedPropertyDef, JobTemplate, LinkBacking, LinkDef,
    ObjectType, ObjectTypeBuilder, Ontology, ParamDef, PropertyDef, ResultExpectation, TypeName,
    VectorIndexDef, validate_derived_columns,
};
pub use orphan_sweep::{ORPHAN_SWEEP_JOB_KIND, OrphanSweepJob};
pub use page::{Cursor, Page, PageReq};
pub use queue::{
    Job, JobFailure, JobId, JobScheduleStatus, KNOWN_JOB_KINDS, NewJob, Queue, RetryPolicy,
    ScheduleFired,
};
pub use snapshot::{ColumnSpec, ColumnStat, DataFile, FileFormat, StatValue};
pub use stream::{
    BucketOffsets, ChangeEvent, ChangeFeedPage, MergeEngine, MvWatermarks, StreamKind, StreamMeta,
    StreamTables, WatermarkAdvance, mv_key,
};
pub use stream_consolidate_job::{STREAM_CONSOLIDATE_JOB_KIND, StreamConsolidateJob};
pub use stream_mv_job::{LookupOn, MAX_LOOKUP_KEYS, STREAM_MV_JOB_KIND, StreamMvJob};
pub use transaction::{ControlPlane, TableControlPlane, TableTx, Tx};
pub use transform_job::{
    OutputMode, TRANSFORM_JOB_KIND, TYPED_TRANSFORM_JOB_KIND, TransformJob, TypedTransformJob,
};
pub use transforms::{
    RunOutcome, RunState, RunTrigger, TransformBody, TransformDef, TransformName, TransformRun,
    Transforms, TriggerNode, next_cron_occurrence, validate_cron,
    validate_no_multi_def_trigger_cycle, validate_no_trigger_cycle, validate_transform_def,
};
pub use vector_index::{
    FlatIndex, HnswIndex, IndexKind, IndexSpec, IvfFlatIndex, Metric, VectorIndex, VectorKey,
    decode, distance,
};
pub use vector_index_job::{BUILD_VECTOR_INDEX_JOB_KIND, BuildVectorIndexJob};
