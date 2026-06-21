//! UnsupportedActionEngine rejects writes (the iceberg backend is read-only).
//! `rust_test` integration target (pure logic, no DB).

use control_plane_core::{DatasetRef, EventType, LineageEvent, RunId, TableRef, TypeName};
use query_api::serving::{ActionEngine, ServingError, SqlValue};
use query_api::serving_datafusion::UnsupportedActionEngine;
use uuid::Uuid;

#[tokio::test(flavor = "current_thread")]
async fn write_object_is_rejected() {
    let engine = UnsupportedActionEngine;
    let table = TableRef {
        schema: "sales".into(),
        name: "orders".into(),
    };
    let event = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(&TypeName("Order".into()))],
        payload: serde_json::json!({}),
    };
    let err = engine
        .write_object(&table, &["id".to_string()], &[SqlValue::Int(1)], &["Long".to_string()], event)
        .await
        .expect_err("must reject");
    match err {
        ServingError::Engine(m) => assert!(m.contains("iceberg"), "message names the backend: {m}"),
    }
}
