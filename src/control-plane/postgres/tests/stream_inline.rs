//! Verifies the universal log-framing columns (`loom_change_kind`, `loom_bucket`,
//! `loom_offset`) on the inline tier: an `inline_append` row defaults to `'+I'`
//! with NULL bucket/offset, a `write_inline_delta` tombstone stamps `'-D'`, and a
//! version delta stamps `'+U'`. loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, EventType, LineageEvent, RunId, TableRef};
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

/// Read back `(loom_change_kind, loom_bucket, loom_offset)` for the highest-
/// version live row matching `id_column = id_value` in `inline_<tid>`. Delta rows
/// are never end-capped (the merge-on-read lets the highest `begin_snapshot`
/// win), so an identity can have more than one live row after a delta write —
/// `order by begin_snapshot desc limit 1` picks the winner deterministically.
async fn framing_cols(
    pool: &sqlx::PgPool,
    tid: i64,
    id_column: &str,
    id_value: i64,
) -> (String, Option<i32>, Option<i64>) {
    let row: (String, Option<i32>, Option<i64>) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "select loom_change_kind, loom_bucket, loom_offset from iceberg_mirror.inline_{tid} \
         where \"{id_column}\" = {id_value} and end_snapshot is null \
         order by begin_snapshot desc limit 1"
    )))
    .fetch_one(pool)
    .await
    .expect("framing columns readback");
    row
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_row_is_plus_i_with_null_bucket_offset() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec()];

    iceberg_inline::inline_append(&pool, &table, &cols, &id_batch("id", 1), lin(), None)
        .await
        .expect("inline append succeeds");

    let tid = tid_of(&pool).await;
    let (kind, bucket, offset) = framing_cols(&pool, tid, "id", 1).await;
    assert_eq!(kind, "+I", "a plain append defaults loom_change_kind to +I");
    assert!(
        bucket.is_none() && offset.is_none(),
        "a batch-table append has no bucket/offset yet (Task 3 stamps those): got bucket={bucket:?} offset={offset:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tombstone_delta_is_minus_d() {
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
    let (kind, _bucket, _offset) = framing_cols(&pool, tid, "id", 1).await;
    assert_eq!(kind, "-D", "a tombstone delta stamps loom_change_kind = -D");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn version_delta_is_plus_u() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec(), qty_spec()];

    // Seed the inline table via one append {id:1, qty:1}.
    iceberg_inline::inline_append(&pool, &table, &cols, &full_row_batch(1, 1), lin(), None)
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

    // Write a VERSION delta {id:1, qty:9}.
    iceberg_inline::write_inline_delta(
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
    .expect("version delta");

    let tid = tid_of(&pool).await;
    let (kind, _bucket, _offset) = framing_cols(&pool, tid, "id", 1).await;
    assert_eq!(kind, "+U", "a version delta stamps loom_change_kind = +U");
}
