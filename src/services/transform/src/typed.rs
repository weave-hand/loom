//! The typed transform primitive: resolve input/output ontology types to their physical
//! tables, run the SQL (written in type terms), validate the result conforms to the
//! output type, and commit — emitting first-class type-named lineage. Delegates the
//! scan/SQL/write/commit skeleton to `run_transform`.

use std::sync::Arc;

use control_plane_core::{
    ControlPlane, ControlPlaneError, DatasetRef, EventType, LineageEvent, RunId, SnapshotId,
    TypeName,
};
use object_store::ObjectStore;
use uuid::Uuid;

use crate::run::{TransformError, TransformInput, TransformRequest, run_transform};

#[derive(Debug, thiserror::Error)]
pub enum TypedTransformError {
    #[error("unknown ontology type {0}")]
    UnknownType(String),
    #[error(transparent)]
    Transform(#[from] TransformError),
}

/// Run one typed transform. `run_id` is a caller-unique output-file prefix (a UUID);
/// `root_url` is the warehouse root the output's paths are absolutized against.
#[allow(clippy::too_many_arguments, reason = "typed transform requires all schema, store, and run context args")]
pub async fn run_typed_transform(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    root_url: &str,
    run_id: &str,
    inputs: &[TypeName],
    output: &TypeName,
    sql: &str,
    output_mode: crate::run::OutputMode,
) -> Result<SnapshotId, TypedTransformError> {
    // 1. Resolve each input type to its backing table.
    let mut input_tables = Vec::with_capacity(inputs.len());
    for ty in inputs {
        let table = cp.ontology().resolve(ty).await.map_err(|e| match e {
            ControlPlaneError::NotFound(_) => TypedTransformError::UnknownType(ty.0.clone()),
            other => TypedTransformError::Transform(TransformError::ControlPlane(other)),
        })?;
        input_tables.push((ty.clone(), table));
    }

    // 2. Resolve the output type: its properties are the conformance contract; its table
    //    is the write target (which need not yet exist — create_table is idempotent).
    let out_type = cp.ontology().get_type(output).await.map_err(|e| match e {
        ControlPlaneError::NotFound(_) => TypedTransformError::UnknownType(output.0.clone()),
        other => TypedTransformError::Transform(TransformError::ControlPlane(other)),
    })?;

    // 3. Inputs register in DataFusion under their TYPE name (type-term SQL).
    let specs: Vec<TransformInput> = input_tables
        .iter()
        .map(|(ty, table)| TransformInput {
            table,
            register_as: &ty.0,
        })
        .collect();

    // 4. First-class type-named lineage; backing tables + SQL retained in the payload.
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: inputs.iter().map(DatasetRef::from).collect(),
        outputs: vec![DatasetRef::from(output)],
        payload: serde_json::json!({
            "sql": sql,
            "input_tables": input_tables
                .iter()
                .map(|(_, t)| format!("{}.{}", t.schema, t.name))
                .collect::<Vec<_>>(),
            "output_table": format!("{}.{}", out_type.table.schema, out_type.table.name),
        }),
    };

    run_transform(
        cp,
        store,
        root_url,
        run_id,
        TransformRequest {
            inputs: &specs,
            output: &out_type.table,
            sql,
            conform: Some(&out_type.properties),
            output_mode,
            lineage,
        },
    )
    .await
    .map_err(TypedTransformError::Transform)
}
