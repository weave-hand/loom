//! UnsupportedActionEngine rejects writes (the iceberg backend is read-only).
//! `rust_test` integration target (pure logic, no DB).

use control_plane_core::TableRef;
use query_api::serving::{ActionEngine, ServingError, SqlValue};
use query_api::serving_datafusion::UnsupportedActionEngine;

#[tokio::test(flavor = "current_thread")]
async fn insert_row_is_rejected() {
    let engine = UnsupportedActionEngine;
    let table = TableRef {
        schema: "sales".into(),
        name: "orders".into(),
    };
    let err = engine
        .insert_row(&table, &["id".to_string()], &[SqlValue::Int(1)])
        .await
        .expect_err("must reject");
    match err {
        ServingError::Engine(m) => assert!(m.contains("iceberg"), "message names the backend: {m}"),
    }
}
