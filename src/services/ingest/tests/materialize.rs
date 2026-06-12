use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{DatasetRef, EventType, LineageEvent, RunId, TableRef};
use control_plane_memory::MemoryControlPlane;
use ingest::gate::{ColumnShape, ModelShape};
use ingest::{IngestError, MaterializeRequest, materialize};
use object_store::local::LocalFileSystem;
use time::OffsetDateTime;
use uuid::Uuid;

fn table() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "customer".into(),
    }
}

fn lineage(t: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef {
            namespace: "loom-ingest".into(),
            name: format!("{}.{}", t.schema, t.name),
        }],
        payload: serde_json::json!({}),
    }
}

fn batch() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
    ]));
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a@x"), Some("b@x")])),
        ],
    )
    .unwrap();
    (schema, b)
}

#[tokio::test]
async fn unmodeled_landing_returns_a_snapshot() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let (schema, b) = batch();
    let t = table();

    let snap = materialize(
        &cp,
        &store,
        MaterializeRequest {
            table: &t,
            schema,
            batches: &[b],
            file_name: "part-0.parquet",
            gate: None,
            lineage: lineage(&t),
        },
    )
    .await
    .unwrap();

    assert_eq!(snap.0, 1, "first snapshot in a fresh control plane");
    assert!(
        dir.path()
            .join("main")
            .join("customer")
            .join("part-0.parquet")
            .exists()
    );
}

#[tokio::test]
async fn modeled_landing_passes_gate_and_returns_a_snapshot() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let (schema, b) = batch();
    let t = table();

    // A model the batch satisfies (id int64 required, email varchar optional).
    let shape = ModelShape {
        columns: vec![
            ColumnShape {
                name: "id".into(),
                ty: "int64".into(),
                required: true,
            },
            ColumnShape {
                name: "email".into(),
                ty: "varchar".into(),
                required: false,
            },
        ],
    };

    let snap = materialize(
        &cp,
        &store,
        MaterializeRequest {
            table: &t,
            schema,
            batches: &[b],
            file_name: "modeled.parquet",
            gate: Some(&shape),
            lineage: lineage(&t),
        },
    )
    .await
    .unwrap();

    assert_eq!(snap.0, 1, "first snapshot in a fresh control plane");
    assert!(
        dir.path()
            .join("main")
            .join("customer")
            .join("modeled.parquet")
            .exists()
    );
}

#[tokio::test]
async fn gate_rejection_happens_before_any_write() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let (schema, b) = batch();
    let t = table();

    let shape = ModelShape {
        columns: vec![
            ColumnShape {
                name: "id".into(),
                ty: "int64".into(),
                required: true,
            },
            ColumnShape {
                name: "ssn".into(),
                ty: "varchar".into(),
                required: true,
            },
        ],
    };

    let err = materialize(
        &cp,
        &store,
        MaterializeRequest {
            table: &t,
            schema,
            batches: &[b],
            file_name: "part-0.parquet",
            gate: Some(&shape),
            lineage: lineage(&t),
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(err, IngestError::DoesNotConform(_)));
    assert!(
        !dir.path()
            .join("main")
            .join("customer")
            .join("part-0.parquet")
            .exists()
    );
}
