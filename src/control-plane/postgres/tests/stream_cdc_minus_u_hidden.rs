//! A CDC table's `-U` (update-before) rows are audit-only before-images destined
//! for the changelog flush (Task 5) — they must never compete as the identity's
//! live/current-state row, and must never move the CAS version token
//! (`current_inline_version`/`read_max_version`). This proves both current-state
//! reads exclude a manually-inserted `-U` row for an identity that already has a
//! live `+I` row: `inline_live_batch` returns exactly one row (the `+I` image,
//! never the `-U` image), and `current_inline_version` is unaffected by the `-U`
//! row even though it carries a HIGHER `begin_snapshot`. loom_fixture_test
//! (Postgres).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline;
use control_plane_postgres::iceberg_mirror::next_snapshot;

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

fn val_spec() -> ColumnSpec {
    ColumnSpec {
        name: "val".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

/// A one-cell batch holding just the id column (`long`).
fn id_batch(name: &str, v: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).expect("id batch")
}

/// A full one-row `{id, val}` batch.
fn full_row_batch(id: i64, val: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![val])),
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
async fn minus_u_row_is_excluded_from_current_state_reads() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec(), val_spec()];

    // Seed ONE live row for id=1 (val=100) via a plain append -- loom_change_kind
    // defaults to '+I'.
    iceberg_inline::inline_append(&pool, &table, &cols, &full_row_batch(1, 100), lin(), None, None)
        .await
        .expect("seed append");

    let tid = tid_of(&pool).await;

    let v_before = iceberg_inline::current_inline_version(
        &pool,
        &table,
        &[id_spec()],
        "id",
        &id_batch("id", 1),
    )
    .await
    .expect("current version after seed +I");

    // Manually insert a '-U' before-image row for the SAME identity (id=1), val=999
    // (a different value so we can tell which row a read returns), with a HIGHER
    // begin_snapshot than the +I row -- so an unfixed `read_max_version` would report
    // this row's (wrong) version, and an unfixed `inline_live_batch` would surface it
    // as a second live row.
    let mut conn = pool.acquire().await.expect("acquire");
    let minus_u_snapshot = next_snapshot(&mut conn, None).await.expect("next_snapshot");
    drop(conn);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "insert into {} (begin_snapshot, loom_tombstone, loom_change_kind, \"id\", \"val\") \
         values ($1, false, '-U', 1, 999)",
        iceberg_inline::inline_table_name(tid),
    )))
    .bind(minus_u_snapshot.0)
    .execute(&pool)
    .await
    .expect("manual -U before-image insert");

    // read_max_version (via current_inline_version) must be UNCHANGED by the -U row,
    // even though it carries a higher begin_snapshot.
    let v_after = iceberg_inline::current_inline_version(
        &pool,
        &table,
        &[id_spec()],
        "id",
        &id_batch("id", 1),
    )
    .await
    .expect("current version after manual -U insert");
    assert_eq!(
        v_after, v_before,
        "a -U before-image row must never advance the CAS version token: before={v_before} after={v_after}"
    );

    // inline_live_batch must return exactly ONE row for id=1 -- the +I row -- and
    // never the -U before-image, even though the -U row is "live" under the raw MVCC
    // predicate (begin_snapshot <= at, end_snapshot is null).
    let catalog = IcebergCatalog::new(pool.clone());
    let (_batch_tid, row_ids, batch) = catalog
        .inline_live_batch(&table, minus_u_snapshot)
        .await
        .expect("inline_live_batch must not error")
        .expect("the live +I row must be present");
    assert_eq!(
        row_ids.len(),
        1,
        "the -U row must not compete as an extra live row: {row_ids:?}"
    );

    let val_idx = batch.schema().index_of("val").expect("val column");
    let val_col = batch
        .column(val_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("val is Int64");
    assert_eq!(
        val_col.value(0),
        100,
        "the returned row must be the +I image (val=100), never the -U image (val=999)"
    );
}
