//! Symmetric log-vs-CDC declaration guard (iss-stream-log-vs-cdc-declare),
//! exercised at the reconcile_stream_mode seam both HTTP surfaces share.
//! loom_fixture_test (Postgres).
//!   - mode=stream (log) with a MATCHING count against a CDC table -> Err(Validation),
//!     registry still kind='cdc'  (the defect: silently accepted before the fix)
//!   - mode=stream with a MISMATCHED count against a CDC table -> Err(Conflict)
//!     (count precedence preserved — pins existing behavior)
//!   - control: a second same-count log declare on a LOG table stays Ok
//!     (the byte-identical log redeclare the slice-2 constraint pins)
//!
//! Mirrors `stream_merge_declare.rs`'s PgFixture / local_sql_catalog / land_cdc
//! harness and its columns / batch / lineage / always_inline helpers.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ControlPlaneError, MergeEngine, ObjectType, Ontology, StreamKind, StreamTables, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{CdcDecl, InlineLimits, land, land_cdc};
use control_plane_postgres::iceberg_mirror::live_table_id;
use loom_test_seed::local_sql_catalog;

fn columns() -> Vec<control_plane_core::ColumnSpec> {
    vec![
        control_plane_core::ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        control_plane_core::ColumnSpec {
            name: "name".into(),
            ty: "string".into(),
            nullable: true,
        },
    ]
}

fn batch() -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("a")])),
        ],
    )
    .expect("batch");
    (schema, vec![b])
}

fn lineage() -> control_plane_core::LineageEvent {
    control_plane_core::LineageEvent::completed(vec![], serde_json::json!({ "source": "test" }))
}

fn always_inline() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: usize::MAX,
        flush_byte_threshold: i64::MAX,
    }
}

/// mode=stream (log) with a MATCHING bucket count against an already-declared
/// CDC table must be rejected with Validation, leaving the registry kind='cdc'.
/// Before the fix this is silently accepted (the defect).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_declare_matching_count_against_cdc_table_is_validation_error() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let table = TableRef {
        schema: "s".into(),
        name: "w_cdc".into(),
    };
    cp.define_type(
        ObjectType::build("WCdc", ("s", "w_cdc"))
            .prop_req("id", "Long")
            .prop("name", "String")
            .identity("id")
            .done(),
    )
    .await
    .expect("define type");

    // Declare the table as CDC (buckets=2), as POST /models/{type}?mode=cdc does.
    let (schema, batches) = batch();
    land_cdc(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        None,
        Some(CdcDecl {
            buckets: 2,
            bucket_key: "id".into(),
            merge_engine: MergeEngine::LastRow,
        }),
        &[],
    )
    .await
    .expect("cdc declare lands");

    // The defect repro: a log declare with the MATCHING count (2) against the
    // CDC table, as POST /datasets/s/w_cdc?mode=stream&buckets=2 does.
    let (schema, batches) = batch();
    let res = land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        Some(2),
    )
    .await;
    // Assert on the message too, not just the variant: the batch→stream-conversion
    // guard also returns `Validation`, so `matches!` alone would not pin the KIND
    // guard as the source of the rejection.
    assert!(
        matches!(&res, Err(ControlPlaneError::Validation(msg)) if msg.contains("different stream kind")),
        "log declare with a matching count against a cdc table must be a kind-mismatch Validation, got {res:?}"
    );

    // The registry row is untouched: still kind='cdc' with its bucket_key.
    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("live_table_id")
        .expect("cdc table has a live mirror row");
    drop(conn);
    let meta = cp
        .stream_meta(tid)
        .await
        .expect("stream_meta")
        .expect("declared stream table");
    assert_eq!(meta.kind, StreamKind::Cdc, "registry stays kind='cdc'");
    assert_eq!(meta.bucket_count, 2, "bucket count unchanged");
    assert_eq!(
        meta.bucket_key.as_deref(),
        Some("id"),
        "bucket_key unchanged"
    );

    // Precedence preserved: a MISMATCHED count against the same cdc table still
    // reports the bucket-count Conflict (the n != m arm fires first, exactly as
    // it does for the cdc-against-log direction today).
    let (schema, batches) = batch();
    let res = land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        Some(3),
    )
    .await;
    assert!(
        matches!(res, Err(ControlPlaneError::Conflict(_))),
        "log declare with a mismatched count against a cdc table stays Conflict, got {res:?}"
    );

    drop(wh);
    drop(catalog);
}

/// THE MIRROR DIRECTION: mode=cdc with a MATCHING bucket count against an
/// already-declared LOG table must also be rejected with Validation, leaving the
/// registry kind='log'. Confirms the kind-change guard is symmetric — a redeclare
/// can transition neither Log->Cdc nor Cdc->Log — which is what makes it safe for
/// `reconcile_stream_mode`'s steady-state `(Some, Some)` arm to skip the
/// CDC-over-MV guard entirely: an already-declared table's kind can never change
/// underneath it, so no steady-state append can turn a table CDC.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_declare_matching_count_against_log_table_is_validation_error() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let table = TableRef {
        schema: "s".into(),
        name: "w_log2".into(),
    };

    // Declare the table as a LOG stream (buckets=2), as POST /datasets/{schema}/{table}?mode=stream does.
    let (schema, batches) = batch();
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        Some(2),
    )
    .await
    .expect("log declare lands");

    // A cdc declare with the MATCHING count (2) against the LOG table, as
    // POST /models/{type}?mode=cdc does.
    let (schema, batches) = batch();
    let res = land_cdc(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        None,
        Some(CdcDecl {
            buckets: 2,
            bucket_key: "id".into(),
            merge_engine: MergeEngine::LastRow,
        }),
        &[],
    )
    .await;
    assert!(
        matches!(&res, Err(ControlPlaneError::Validation(msg)) if msg.contains("different stream kind")),
        "cdc declare with a matching count against a log table must be a kind-mismatch Validation, got {res:?}"
    );

    // The registry row is untouched: still kind='log'.
    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("live_table_id")
        .expect("log table has a live mirror row");
    drop(conn);
    let meta = cp
        .stream_meta(tid)
        .await
        .expect("stream_meta")
        .expect("declared stream table");
    assert_eq!(meta.kind, StreamKind::Log, "registry stays kind='log'");
    assert_eq!(meta.bucket_count, 2, "bucket count unchanged");

    drop(wh);
    drop(catalog);
}

/// Control (non-regression): a second same-count log declare against a LOG
/// table is still accepted — the pure log redeclare path the slice-2
/// byte-identical constraint pins.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_redeclare_matching_count_against_log_table_stays_ok() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let table = TableRef {
        schema: "s".into(),
        name: "w_log".into(),
    };

    let (schema, batches) = batch();
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        Some(2),
    )
    .await
    .expect("first log declare lands");

    let (schema, batches) = batch();
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        Some(2),
    )
    .await
    .expect("same-count log redeclare stays accepted (byte-identical log path)");

    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("live_table_id")
        .expect("log table has a live mirror row");
    drop(conn);
    let meta = cp
        .stream_meta(tid)
        .await
        .expect("stream_meta")
        .expect("declared stream table");
    assert_eq!(meta.kind, StreamKind::Log, "registry reads kind='log'");
    assert_eq!(meta.bucket_count, 2);

    drop(wh);
    drop(catalog);
}
