//! `inline_live_batch_shadow` + `clear_has_shadow_if_quiescent`: the Snapshot-fold
//! read (every live inline row — appends, versions, tombstones — with
//! `begin_snapshot`/`loom_tombstone` framing) and the conditional flag clear that
//! only re-arms the byte-trigger flush once no live shadow delta remains.
//! loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::{BooleanArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline;

fn table() -> TableRef {
    TableRef {
        schema: "sales".to_string(),
        name: "orders".to_string(),
    }
}

fn id_spec() -> ColumnSpec {
    ColumnSpec {
        name: "id".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

/// A one-cell batch holding just the id column (`long`).
fn id_batch(v: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).expect("id batch")
}

fn lin() -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "test" }),
    }
}

/// Resolve the internal inline table id the same way the other inline tests do.
async fn tid_of(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar(
        "select table_id from iceberg_mirror.table \
         where table_namespace='sales' and table_name='orders' and end_snapshot is null",
    )
    .fetch_one(pool)
    .await
    .expect("table_id")
}

/// Seed: append id=1, a version delta id=2, a tombstone delta id=3. Returns the
/// three snapshots (append, version, tombstone) in that order.
async fn seed_three_rows(
    pool: &sqlx::PgPool,
    table: &TableRef,
) -> (
    control_plane_core::SnapshotId,
    control_plane_core::SnapshotId,
    control_plane_core::SnapshotId,
) {
    let cols = vec![id_spec()];

    let s1 = iceberg_inline::inline_append(pool, table, &cols, &id_batch(1), lin(), None, None)
        .await
        .expect("seed append id=1");

    let v0_id2 =
        iceberg_inline::current_inline_version(pool, table, &[id_spec()], "id", &id_batch(2))
            .await
            .expect("current version id=2");
    let s2 = iceberg_inline::write_inline_delta(
        pool,
        table,
        &cols,
        "id",
        false,
        &id_batch(2),
        None,
        lin(),
        v0_id2,
        None,
        &[],
    )
    .await
    .expect("version delta id=2");

    let v0_id3 =
        iceberg_inline::current_inline_version(pool, table, &[id_spec()], "id", &id_batch(3))
            .await
            .expect("current version id=3");
    let s3 = iceberg_inline::write_inline_delta(
        pool,
        table,
        &[id_spec()],
        "id",
        true,
        &id_batch(3),
        None,
        lin(),
        v0_id3,
        None,
        &[],
    )
    .await
    .expect("tombstone delta id=3");

    (s1, s2, s3)
}

/// Case 1: projection + row ids. After one append, one version, one tombstone,
/// `inline_live_batch_shadow` returns all 3 rows; the schema ends with
/// `[.., begin_snapshot: Int64 non-null, loom_tombstone: Boolean non-null]`; the
/// tombstone row (id=3) has `loom_tombstone=true`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shadow_read_projects_all_live_rows() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let table = table();

    let (_s1, _s2, s3) = seed_three_rows(&pool, &table).await;

    let catalog = IcebergCatalog::new(pool.clone());
    let (_tid, row_ids, batch) = catalog
        .inline_live_batch_shadow(&table, s3)
        .await
        .expect("inline_live_batch_shadow")
        .expect("3 live rows");

    assert_eq!(row_ids.len(), 3, "one row per identity (1, 2, 3)");
    assert_eq!(batch.num_rows(), 3);

    let schema = batch.schema();
    let n = schema.fields().len();
    assert!(n >= 2, "at least the two framing columns");
    let begin_field = schema.field(n - 2);
    let tomb_field = schema.field(n - 1);
    assert_eq!(begin_field.name(), "begin_snapshot");
    assert_eq!(begin_field.data_type(), &DataType::Int64);
    assert!(!begin_field.is_nullable(), "begin_snapshot is non-null");
    assert_eq!(tomb_field.name(), "loom_tombstone");
    assert_eq!(tomb_field.data_type(), &DataType::Boolean);
    assert!(!tomb_field.is_nullable(), "loom_tombstone is non-null");

    // The user column `id` precedes the framing columns.
    assert_eq!(schema.field(0).name(), "id");

    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id is Int64Array");
    let tomb_col = batch
        .column(n - 1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("loom_tombstone is BooleanArray");

    // Rows are ordered by loom_row_id, which matches insertion order here: id=1
    // (append), id=2 (version), id=3 (tombstone).
    assert_eq!(id_col.value(0), 1);
    assert_eq!(id_col.value(1), 2);
    assert_eq!(id_col.value(2), 3);
    assert!(!tomb_col.value(0), "append row is not a tombstone");
    assert!(!tomb_col.value(1), "version row is not a tombstone");
    assert!(tomb_col.value(2), "the id=3 row is the live tombstone");
}

/// Case 2: MVCC bound. At a snapshot before the tombstone's `begin_snapshot`, only
/// the earlier rows (append + version) return.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shadow_read_respects_mvcc_bound() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let table = table();

    let (_s1, s2, _s3) = seed_three_rows(&pool, &table).await;

    let catalog = IcebergCatalog::new(pool.clone());
    let (_tid, row_ids, batch) = catalog
        .inline_live_batch_shadow(&table, s2)
        .await
        .expect("inline_live_batch_shadow at s2")
        .expect("2 live rows before the tombstone");

    assert_eq!(row_ids.len(), 2, "only the append + version are live at s2");
    assert_eq!(batch.num_rows(), 2);
}

/// Case 3: quiescent clear. With the version + tombstone live,
/// `clear_has_shadow_if_quiescent` returns `false` and `has_shadow` stays `true`;
/// after end-capping the two delta rows (the append stays live — `+I` rows are not
/// shadow deltas), it returns `true` and `has_shadow` reads `false`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quiescent_clear_waits_for_live_deltas_to_drain() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let table = table();

    seed_three_rows(&pool, &table).await;
    let tid = tid_of(&pool).await;

    let mut conn = pool.acquire().await.expect("acquire");
    assert!(
        iceberg_inline::has_shadow(&mut conn, tid)
            .await
            .expect("has_shadow"),
        "seeding a version/tombstone flags the table as shadowed"
    );

    // Live deltas remain (version id=2, tombstone id=3): the clear is a no-op.
    let cleared = iceberg_inline::clear_has_shadow_if_quiescent(&mut conn, tid)
        .await
        .expect("clear_has_shadow_if_quiescent (deltas still live)");
    assert!(!cleared, "must not clear while a shadow delta is live");
    assert!(
        iceberg_inline::has_shadow(&mut conn, tid)
            .await
            .expect("has_shadow after no-op clear"),
        "flag stays set while a shadow delta is live"
    );

    // End-cap the two delta rows (version id=2, tombstone id=3) directly — standing
    // in for the consolidation write this task's read feeds (Task 3/plan). The
    // append (id=1, `+I`) is deliberately left live: it is not a shadow delta.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "update iceberg_mirror.inline_{tid} set end_snapshot = 999999999 \
         where end_snapshot is null and (loom_tombstone or loom_change_kind in ('+U','-D'))"
    )))
    .execute(&mut *conn)
    .await
    .expect("end-cap the two delta rows");

    // Now quiescent: the clear takes effect.
    let cleared = iceberg_inline::clear_has_shadow_if_quiescent(&mut conn, tid)
        .await
        .expect("clear_has_shadow_if_quiescent (quiescent)");
    assert!(cleared, "must clear once no live shadow delta remains");
    assert!(
        !iceberg_inline::has_shadow(&mut conn, tid)
            .await
            .expect("has_shadow after real clear"),
        "flag is cleared once quiescent"
    );

    // Case 4: idempotent — a second quiescent clear returns `false` (nothing to
    // delete) without error.
    let cleared_again = iceberg_inline::clear_has_shadow_if_quiescent(&mut conn, tid)
        .await
        .expect("clear_has_shadow_if_quiescent is idempotent");
    assert!(!cleared_again, "nothing left to clear the second time");
}
