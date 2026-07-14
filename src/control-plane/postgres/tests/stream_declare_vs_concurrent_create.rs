//! A stream-declaring `land` must not convert a table a CONCURRENT writer created
//! as a batch table (root cause behind iss-stream-first-declare-race-kind's
//! reachability).
//!
//! `land_parquet` probes `pre_existing` on a separate pooled connection BEFORE its
//! transaction; `ensure_table` then silently resolves a lost unique-index race to
//! the winner's table_id. The witness therefore says "brand new" about a table
//! another transaction just created, and the batch->stream conversion guard
//! (`reconcile_stream_mode`'s "cannot convert existing batch table") never fires.
//!
//! Deterministic, no sleeps — the same `pg_stat_activity` barrier as
//! `stream_first_declare_race.rs`: connection A holds an uncommitted
//! `iceberg_mirror.table` insert for `s.t`; the landing writer's `ensure_table`
//! blocks on it; once it is observably blocked we commit A, so the writer takes
//! exactly the lost-race path.
//! loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, ControlPlaneError, LineageEvent, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_mirror::next_snapshot;
use loom_test_seed::local_sql_catalog;
use sqlx::PgPool;

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

fn batch() -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let b = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
        .expect("batch");
    (schema, vec![b])
}

fn lineage() -> LineageEvent {
    LineageEvent::completed(vec![], serde_json::json!({ "source": "test" }))
}

/// Direct-write (Parquet) limits: never inline, so the write takes `land_parquet`
/// — the path whose `pre_existing` probe runs on a separate connection before the
/// transaction. `land` routes inline iff `bytes <= inline_byte_limit`
/// (`iceberg_landing.rs:181`), so a limit of 0 forces Parquet — but ONLY because
/// `batch()` is non-empty (an empty batch has `bytes == 0`, which would flip it
/// back to the inline path). Keep the batch non-empty.
fn never_inline() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: 0,
        flush_byte_threshold: 0,
    }
}

/// Barrier: block until some backend is waiting on another transaction's lock
/// inside an `iceberg_mirror.table` insert — i.e. the lander's `ensure_table` is
/// blocked on our uncommitted row. Panics rather than hanging.
///
/// The bound is deliberately generous (30 s, not the 6 s the registry-level race
/// tests use): before it reaches `ensure_table` the lander must create the Iceberg
/// table and load its metadata through the object store, which is slow on a loaded
/// CI box. Reads another backend's `query` column — superuser only; the fixture
/// connects as `postgres`.
async fn await_ensure_table_blocked(pool: &PgPool) {
    for _ in 0..3000 {
        let blocked: i64 = sqlx::query_scalar(
            "select count(*) from pg_stat_activity \
             where datname = current_database() \
               and wait_event_type = 'Lock' \
               and query like 'insert into iceberg_mirror.table%'",
        )
        .fetch_one(pool)
        .await
        .expect("pg_stat_activity barrier probe");
        if blocked >= 1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the lander's ensure_table never blocked on the concurrent creator");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_declare_losing_the_create_race_cannot_convert_the_winners_table() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    // `SqlCatalog` is NOT `Clone` (iceberg_sql_catalog/catalog.rs:219), so share it
    // with the spawned lander through an `Arc` — `&Arc<SqlCatalog>` derefs to the
    // `&SqlCatalog` that `land` wants.
    let catalog =
        Arc::new(local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await);

    let table = TableRef {
        schema: "s".into(),
        name: "t".into(),
    };

    // A: a concurrent BATCH writer that has created the mirror row for s.t but has
    // not committed. Held open.
    let mut winner = pool.begin().await.expect("begin winner tx");
    let at = next_snapshot(&mut winner, None)
        .await
        .expect("winner snapshot");
    sqlx::query(
        "insert into iceberg_mirror.table (table_namespace, table_name, begin_snapshot) \
         values ($1, $2, $3)",
    )
    .bind(&table.schema)
    .bind(&table.name)
    .bind(at.0)
    .execute(&mut *winner)
    .await
    .expect("winner creates the mirror row");

    // B: a stream-declaring land. Its pre-transaction probe sees NO table (A is
    // uncommitted) => pre_existing = false. Its `ensure_table` then blocks on A.
    let (schema, batches) = batch();
    let pool_b = pool.clone();
    let cat_b = Arc::clone(&catalog);
    let table_b = table.clone();
    let lander = tokio::spawn(async move {
        land(
            &pool_b,
            &cat_b,
            &table_b,
            &columns(),
            schema,
            batches,
            never_inline(),
            lineage(),
            Some(2),
        )
        .await
    });

    await_ensure_table_blocked(&pool).await;
    winner.commit().await.expect("commit winner");

    let res = lander.await.expect("join lander");
    assert!(
        matches!(&res, Err(ControlPlaneError::Validation(msg))
                 if msg.contains("cannot convert existing batch table")),
        "a stream declare that LOSES the create race must see the winner's table as \
         pre-existing and refuse the batch->stream conversion, got {res:?}"
    );

    drop(wh);
    drop(catalog);
}
