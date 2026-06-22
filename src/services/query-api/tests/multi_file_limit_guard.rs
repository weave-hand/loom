//! Regression test: a governed read over a genuinely multi-file DuckLake table under
//! the standard LIMIT cap returns uncorrupted ids.
//!
//! Background: DuckDB/DuckLake has a bug (iss-multi-file-limit-misread) where reading
//! a multi-file Parquet table with a pushed-down LIMIT corrupts column values — the
//! LIMIT is pushed into each file's scan independently, causing values from one file's
//! first N rows to overwrite values from another file's rows after decompression. The
//! fix (Tasks 1-4) adds a stable `ORDER BY` barrier before the `LIMIT` on the DuckDB
//! serving path, which forces DuckDB to use its TopN operator rather than a per-file
//! LIMIT pushdown.
//!
//! Reproduction observation (Step 3): With `DuckDbDialect::limit_needs_order_barrier()`
//! temporarily returning `false`, the test FAILED with a corrupted id value of `65537`
//! (`0x10001`) outside the source range 1..=2000 — the corruption reproduces
//! deterministically in this environment. This confirms the guard does real work: the
//! ORDER BY barrier prevents DuckDB from pushing the LIMIT into each file's scan
//! independently, which is the root cause of the value corruption.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use axum::http::StatusCode;
use control_plane_core::{
    Catalog, DatasetRef, EventType, LineageEvent, ObjectType, Ontology, PageReq, RunId,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use e2e_support::{get, grant_read, ids_i64, prop, subject_with_role, tref};
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::serving::EmbeddedDuckDb;
use time::OffsetDateTime;
use uuid::Uuid;

/// Land a batch into the DuckLake-backed control plane under a caller-supplied
/// `file_prefix`. Distinct prefixes land into distinct directories, ensuring
/// each call produces a separate Parquet file registration in the catalog.
async fn land_prefixed(
    cp: &PgControlPlane,
    store: &Arc<dyn ObjectStore>,
    table: &control_plane_core::TableRef,
    schema: Arc<Schema>,
    batch: RecordBatch,
    file_prefix: &str,
) {
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(table)],
        payload: serde_json::json!({}),
    };
    materialize(
        cp,
        store.clone(),
        MaterializeRequest {
            table,
            schema,
            batches: &[batch],
            file_prefix,
            gate: None,
            lineage,
        },
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_file_limit_read_returns_uncorrupted_ids() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // widget(id): seed 2000 distinct ids across two separate Parquet files.
    // Using distinct file_prefix values ("run-1", "run-2") forces two separate
    // directory scans, yielding two registered Parquet files in the catalog.
    let widget = tref("main", "widget");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    // Batch 1: ids 1..=1000 under prefix "run-1"
    let ids_1: Vec<i64> = (1i64..=1000).collect();
    let batch_1 =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids_1))]).unwrap();
    land_prefixed(&cp, &store, &widget, schema.clone(), batch_1, "run-1").await;

    // Batch 2: ids 1001..=2000 under prefix "run-2"
    let ids_2: Vec<i64> = (1001i64..=2000).collect();
    let batch_2 =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids_2))]).unwrap();
    land_prefixed(&cp, &store, &widget, schema.clone(), batch_2, "run-2").await;

    // Assert the multi-file precondition via the catalog BEFORE reading.
    let snap = cp.current_snapshot(&widget).await.unwrap();
    let files = cp
        .files(&widget, snap.id, PageReq::unbounded())
        .await
        .unwrap();
    assert!(
        files.items.len() > 1,
        "test setup: expected a multi-file table, got {} file(s) — \
         the two distinct file_prefix appends must produce separate Parquet files",
        files.items.len()
    );

    // Define the widget type with identity `id`.
    cp.define_type(ObjectType {
        name: control_plane_core::TypeName("widget".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![],
        table: widget.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();

    // Build the serving engine.
    let eng = EmbeddedDuckDb::attach(
        &format!(
            "dbname={} host={} user=postgres",
            db,
            fx.socket_path().display()
        ),
        writer.data_path(),
    )
    .await
    .unwrap();
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    // Grant alice read on widget and issue the governed object read.
    let (_alice, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "widget").await;

    let (status, body) = get(cp.clone(), eng.clone(), "/objects/widget", "alice").await;
    assert_eq!(status, StatusCode::OK);

    // ids_i64 returns sorted ids from the {objects:[...]} body.
    let got = ids_i64(&body);

    // The read caps at DEFAULT_LIMIT = 1000: expect exactly 1000 rows.
    assert_eq!(
        got.len(),
        1000,
        "read should return exactly DEFAULT_LIMIT (1000) rows, got {}",
        got.len()
    );

    // Every returned id must be within the known source range 1..=2000.
    // A corrupted id (the bug's signature) would jump outside this range.
    for id in &got {
        assert!(
            (1..=2000).contains(id),
            "corrupted id outside source range 1..=2000: {id}"
        );
    }

    // No duplicates — corruption can collide values from different files.
    let mut dedup = got.clone();
    dedup.dedup();
    assert_eq!(
        dedup.len(),
        got.len(),
        "duplicate/corrupted ids in result: {got:?}"
    );
}
