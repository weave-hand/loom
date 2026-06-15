//! Queue handler: parse a transform job payload, run it, map the outcome to a
//! `JobFailure` (deterministic errors -> Abandon, transient -> Retry with backoff).

use std::sync::Arc;
use std::time::Duration;

use control_plane_core::{
    ControlPlane, DatasetRef, EventType, Job, JobFailure, LineageEvent, RetryPolicy, RunId,
    TableRef, TypeName,
};
use object_store::ObjectStore;
use serde::Deserialize;
use uuid::Uuid;

use crate::run::{TransformError, TransformInput, TransformRequest, run_transform};
use crate::typed::{TypedTransformError, run_typed_transform};

/// Wire form of a transform job payload. `{schema, name}` per table.
#[derive(Deserialize)]
struct TableSpec {
    schema: String,
    name: String,
}
impl From<&TableSpec> for TableRef {
    fn from(t: &TableSpec) -> Self {
        TableRef {
            schema: t.schema.clone(),
            name: t.name.clone(),
        }
    }
}

#[derive(Deserialize)]
struct TransformPayload {
    inputs: Vec<TableSpec>,
    output: TableSpec,
    sql: String,
}

/// Run one transform job. Takes the deps + the job, returns the worker outcome.
/// Used by the binary's handler closure and by tests.
pub async fn transform_handler(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    job: Job,
) -> Result<(), JobFailure> {
    let payload: TransformPayload = match serde_json::from_value(job.payload.clone()) {
        Ok(p) => p,
        Err(e) => {
            return Err(JobFailure {
                error: format!("malformed transform payload: {e}"),
                policy: RetryPolicy::Abandon,
            });
        }
    };
    let input_tables: Vec<TableRef> = payload.inputs.iter().map(TableRef::from).collect();
    let output = TableRef::from(&payload.output);
    let run_id = Uuid::new_v4().to_string();

    // Physical inputs register under their own table name.
    let inputs: Vec<TransformInput> = input_tables
        .iter()
        .map(|t| TransformInput {
            table: t,
            register_as: &t.name,
        })
        .collect();

    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: input_tables.iter().map(DatasetRef::from).collect(),
        outputs: vec![DatasetRef::from(&output)],
        payload: serde_json::json!({ "sql": payload.sql }),
    };

    let res = run_transform(
        cp,
        store,
        &run_id,
        TransformRequest {
            inputs: &inputs,
            output: &output,
            sql: &payload.sql,
            conform: None,
            lineage,
        },
    )
    .await;

    res.map(|_snapshot| ()).map_err(|e| JobFailure {
        error: e.to_string(),
        policy: retry_policy(&e, job.attempts),
    })
}

/// Wire form of a typed transform job (`"typed-transform"` kind): ontology type names.
#[derive(Deserialize)]
struct TypedTransformPayload {
    inputs: Vec<String>,
    output: String,
    sql: String,
}

/// Run one typed transform job. Inputs/output are ontology type names; the SQL
/// references inputs by type name; the result must conform to the output type.
pub async fn typed_transform_handler(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    job: Job,
) -> Result<(), JobFailure> {
    let payload: TypedTransformPayload = match serde_json::from_value(job.payload.clone()) {
        Ok(p) => p,
        Err(e) => {
            return Err(JobFailure {
                error: format!("malformed typed-transform payload: {e}"),
                policy: RetryPolicy::Abandon,
            });
        }
    };
    let inputs: Vec<TypeName> = payload.inputs.into_iter().map(TypeName).collect();
    let output = TypeName(payload.output);
    let run_id = Uuid::new_v4().to_string();

    let res = run_typed_transform(cp, store, &run_id, &inputs, &output, &payload.sql).await;

    res.map(|_snapshot| ()).map_err(|e| JobFailure {
        error: e.to_string(),
        policy: typed_retry_policy(&e, job.attempts),
    })
}

/// Typed-transform classification: an unknown type is deterministic; otherwise defer to
/// the physical mapping (which already classifies `DoesNotConform` as Abandon).
fn typed_retry_policy(err: &TypedTransformError, attempts: i32) -> RetryPolicy {
    match err {
        TypedTransformError::UnknownType(_) => RetryPolicy::Abandon,
        TypedTransformError::Transform(t) => retry_policy(t, attempts),
    }
}

/// Deterministic failures can't be retried; transient ones back off on attempts.
fn retry_policy(err: &TransformError, attempts: i32) -> RetryPolicy {
    match err {
        TransformError::UnknownInput(..)
        | TransformError::AmbiguousInput(_)
        | TransformError::DoesNotConform(_)
        | TransformError::DataFusion(_)
        | TransformError::Infer(_)
        | TransformError::NoSnapshot => RetryPolicy::Abandon,
        TransformError::ControlPlane(_) | TransformError::Scan(_) | TransformError::Write(_) => {
            RetryPolicy::Retry {
                delay: Duration::from_secs(2u64.saturating_pow(attempts.clamp(0, 6) as u32)),
            }
        }
    }
}
