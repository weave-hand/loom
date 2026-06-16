//! Rollback atomicity for the snapshot-commit unit through the real Postgres
//! adapter. The atomic-unit conformance (`snapshot_conformance`) proves all four
//! legs commit together; this proves the inverse at the row level: when the tx
//! is rolled back, loom's native catalog writer leaks ZERO `ducklake_*` rows —
//! no half-written snapshot, table, or data file — and no lineage event or job.
//!
//! Where the testkit rollback leg checks absence through the read API, this
//! checks the raw catalog tables directly (via `count_rows`), so a partial
//! INSERT that the read API happened to hide would still be caught.

use control_plane_core::{
    Catalog, ColumnSpec, ControlPlane, DataFile, DatasetRef, EventType, FileFormat, Lineage,
    LineageEvent, NewJob, PageReq, Queue, RunId, TableRef,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use time::OffsetDateTime;

#[tokio::test]
async fn rollback_leaks_no_catalog_rows() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    // Bare ATTACH: 27 ducklake_* tables + snapshot 0 + `main` schema, no table.
    writer.bootstrap().await;

    // Baseline catalog row counts (snapshot 0 from bootstrap is the only row).
    let snaps_before = writer.count_rows("ducklake_snapshot").await;
    let tables_before = writer.count_rows("ducklake_table").await;
    let columns_before = writer.count_rows("ducklake_column").await;
    let files_before = writer.count_rows("ducklake_data_file").await;
    let changes_before = writer.count_rows("ducklake_snapshot_changes").await;

    // Stage the full atomic unit — create_table + append_files + emit + enqueue —
    // then ROLL BACK instead of committing.
    let t = TableRef {
        schema: "main".into(),
        name: "rolled".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(
        &t,
        &[ColumnSpec {
            name: "id".into(),
            ty: "int64".into(),
            nullable: false,
        }],
    )
    .await
    .unwrap();
    tx.append_files(
        &t,
        &[DataFile {
            path: "a.parquet".into(),
            path_is_relative: true,
            file_format: FileFormat::Parquet,
            record_count: 3,
            file_size_bytes: 48,
            column_stats: vec![],
            parquet_footer_size: Some(10),
        }],
    )
    .await
    .unwrap();
    tx.emit(LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        inputs: vec![],
        outputs: vec![DatasetRef {
            namespace: "ducklake".into(),
            name: "main.rolled".into(),
        }],
        payload: serde_json::json!({"eventType": "COMPLETE"}),
    })
    .await
    .unwrap();
    tx.enqueue(NewJob {
        kind: "downstream".into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    })
    .await
    .unwrap();
    tx.rollback().await.unwrap();

    // Every catalog table is back to its baseline — no half-written snapshot rows.
    assert_eq!(
        writer.count_rows("ducklake_snapshot").await,
        snaps_before,
        "rolled-back commit must add no ducklake_snapshot row"
    );
    assert_eq!(
        writer.count_rows("ducklake_table").await,
        tables_before,
        "rolled-back commit must add no ducklake_table row"
    );
    assert_eq!(
        writer.count_rows("ducklake_column").await,
        columns_before,
        "rolled-back commit must add no ducklake_column row"
    );
    assert_eq!(
        writer.count_rows("ducklake_data_file").await,
        files_before,
        "rolled-back commit must add no ducklake_data_file row"
    );
    assert_eq!(
        writer.count_rows("ducklake_snapshot_changes").await,
        changes_before,
        "rolled-back commit must add no ducklake_snapshot_changes row"
    );

    // The table is invisible through the read API, and the other two legs left
    // nothing either: no lineage event for the run, no dequeue-able job.
    assert!(
        cp.current_snapshot(&t).await.is_err(),
        "rolled-back table absent from read API"
    );
    assert!(
        cp.events_for(&run, PageReq::unbounded())
            .await
            .unwrap()
            .is_empty(),
        "rolled-back lineage event must not be visible"
    );
    assert!(
        cp.dequeue(&["downstream".to_string()], "w1")
            .await
            .unwrap()
            .is_none(),
        "rolled-back job must not be dequeue-able"
    );
}
