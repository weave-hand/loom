//! write_inline_delta + current_inline_version: per-identity compare-and-swap over
//! the inline delta tier (row-versions and tombstones). loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, ControlPlaneError, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
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

fn qty_spec() -> ColumnSpec {
    ColumnSpec {
        name: "qty".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

/// A one-cell batch holding just the id column (`long`).
fn id_batch(name: &str, v: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).expect("id batch")
}

/// A full one-row {id, qty} batch (the post-PATCH row a version delta carries).
fn full_row_batch(id: i64, qty: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("qty", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![qty])),
        ],
    )
    .expect("full row batch")
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_write_and_cas_conflict() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec(), qty_spec()];

    // Seed the inline table via one append {id:1, qty:1}.
    iceberg_inline::inline_append(&pool, &table, &cols, &full_row_batch(1, 1), lin(), None)
        .await
        .expect("seed append");

    // Capture the current live version for id=1.
    let v0 = iceberg_inline::current_inline_version(
        &pool,
        &table,
        &[id_spec()],
        "id",
        &id_batch("id", 1),
    )
    .await
    .expect("current version id=1");
    assert!(v0 > 0, "seeded id has a positive version, got {v0}");

    // Write a VERSION delta {id:1, qty:9} with the correct expected_version -> Ok, newer.
    let v1 = iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(1, 9),
        lin(),
        v0,
    )
    .await
    .expect("version delta with correct expected_version");
    assert!(v1.0 > v0, "new version {} must exceed old {v0}", v1.0);

    // The current version for id=1 now reflects the new write.
    let cur = iceberg_inline::current_inline_version(
        &pool,
        &table,
        &[id_spec()],
        "id",
        &id_batch("id", 1),
    )
    .await
    .expect("current version after write");
    assert_eq!(cur, v1.0, "current version advanced to the new delta");

    // Write again with the STALE expected_version=v0 -> Conflict (CAS lost).
    let stale = iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(1, 7),
        lin(),
        v0,
    )
    .await;
    assert!(
        matches!(stale, Err(ControlPlaneError::Conflict(_))),
        "stale expected_version must Conflict, got {stale:?}"
    );

    // A DIFFERENT identity id=2 hashes to a different advisory key -> no contention.
    let v0_id2 = iceberg_inline::current_inline_version(
        &pool,
        &table,
        &[id_spec()],
        "id",
        &id_batch("id", 2),
    )
    .await
    .expect("current version id=2");
    assert_eq!(v0_id2, 0, "unwritten id has version 0");
    let r2 = iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(2, 5),
        lin(),
        v0_id2,
    )
    .await;
    assert!(r2.is_ok(), "a different identity must not conflict: {r2:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tombstone_delta_marks_deleted() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec()];

    // Seed one row {id:1}.
    iceberg_inline::inline_append(&pool, &table, &cols, &id_batch("id", 1), lin(), None)
        .await
        .expect("seed append");

    let v0 = iceberg_inline::current_inline_version(
        &pool,
        &table,
        &[id_spec()],
        "id",
        &id_batch("id", 1),
    )
    .await
    .expect("current version id=1");

    // Write a TOMBSTONE delta carrying just the id.
    iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &[id_spec()],
        "id",
        true,
        &id_batch("id", 1),
        lin(),
        v0,
    )
    .await
    .expect("tombstone delta");

    let tid = tid_of(&pool).await;

    // A live inline row for id=1 with loom_tombstone=true and the id populated exists.
    let exists: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select exists(select 1 from iceberg_mirror.inline_{tid} \
         where loom_tombstone = true and \"id\" = 1 and end_snapshot is null)"
    )))
    .fetch_one(&pool)
    .await
    .expect("tombstone existence check");
    assert!(exists, "a live tombstone row for id=1 must exist");

    // The mutation flagged the table as carrying inline shadow deltas.
    let mut conn = pool.acquire().await.expect("acquire");
    let flagged = iceberg_inline::has_shadow(&mut conn, tid)
        .await
        .expect("has_shadow");
    assert!(
        flagged,
        "a mutated table must be flagged as having a shadow"
    );
}
