#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "shared fixture-test support library, not a production path"
)]
//! Shared fixture-test support for the direct-write (Parquet) stream-vs-batch RACE
//! test family — `tests/stream_declare_vs_concurrent_create.rs` (the conversion guard
//! under a lost mirror-row create race) and `tests/stream_create_race_schema.rs`
//! (#623: the Iceberg physical schema must agree with the committed mirror mode).
//!
//! Both tests arrange the *same* race: connection A holds an uncommitted
//! `iceberg_mirror.table` insert for `s.t`, a stream-declaring `land` blocks on it,
//! and A commits once the lander is observably blocked — so the lander takes exactly
//! the lost-race path. Only the assertions differ. That arrangement plus the tiny
//! column/batch/lineage/limits builders is what lived twice; it lives here now.
//!
//! Extracted when the second test landed rather than left to a follow-up, for the
//! reason `gc_test_support.rs` records: the copy-paste-then-extract round trip is
//! more expensive than extracting at the moment the second copy appears.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, ControlPlaneError, LineageEvent, SnapshotId, TableRef};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_mirror::next_snapshot;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use loom_test_seed::local_sql_catalog;
use sqlx::PgPool;

/// The single `long` user column every test in this family lands.
pub fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// One non-empty row. Non-empty MATTERS: `never_inline` forces the Parquet path by
/// setting `inline_byte_limit = 0`, and `land` routes inline iff
/// `bytes <= inline_byte_limit` — an empty batch has `bytes == 0` and would flip
/// back to the inline path, silently testing the wrong code path.
pub fn batch() -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let b = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
        .expect("batch");
    (schema, vec![b])
}

pub fn lineage() -> LineageEvent {
    LineageEvent::completed(vec![], serde_json::json!({ "source": "test" }))
}

/// Direct-write (Parquet) limits: never inline, so the write takes `land_parquet` —
/// the path whose stream/batch mode decision this test family is about. See
/// [`batch`] for why the batch must stay non-empty.
pub fn never_inline() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: 0,
        flush_byte_threshold: 0,
    }
}

/// The three reserved framing column names, as `framing_column_specs` fixes them.
pub const FRAMING: [&str; 3] = ["loom_change_kind", "loom_bucket", "loom_offset"];

/// Boot a fresh hermetic-fixture DB, a temp warehouse, the vendored `SqlCatalog`
/// over it, and a direct-query pool. Keep the returned `TempDir` alive in the
/// caller's scope: dropping it removes the warehouse directory the catalog's
/// `file://` URLs point at.
///
/// The catalog comes back in an `Arc` because `SqlCatalog` is NOT `Clone`
/// (`iceberg_sql_catalog/catalog.rs`) and these tests hand it to a spawned lander;
/// `&Arc<SqlCatalog>` derefs to the `&SqlCatalog` that `land` wants.
pub async fn harness(
    fx: &PgFixture,
) -> (
    PgControlPlane,
    String,
    tempfile::TempDir,
    Arc<SqlCatalog>,
    PgPool,
) {
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog =
        Arc::new(local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await);
    let pool = fx.pool_for(&db).await;
    (cp, db, wh, catalog, pool)
}

/// Barrier: block until some backend is waiting on another transaction's lock inside
/// an `iceberg_mirror.table` insert — i.e. the lander's mirror-row ensure is blocked
/// on our uncommitted row. Panics rather than hanging.
///
/// The bound is deliberately generous (30 s): on a loaded CI box the lander has real
/// work to do before it reaches the insert. Reads another backend's `query` column,
/// which is superuser-only — the fixture connects as `postgres`.
pub async fn await_ensure_table_blocked(pool: &PgPool) {
    for _ in 0..3000 {
        let blocked: i64 = sqlx::query_scalar(
            "select count(*) from pg_stat_activity \
             where datname = current_database() \
               and wait_event_type = 'Lock' \
               and wait_event = 'transactionid' \
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
    panic!("the lander's mirror-row ensure never blocked on the concurrent creator");
}

/// Drive a stream-declaring `land` on `table` that LOSES the mirror-row create race
/// to a concurrent batch writer, and return the lander's result.
///
/// Deterministic, no sleeps: this opens a transaction that inserts the live
/// `iceberg_mirror.table` row for `table` and holds it uncommitted, spawns the
/// declaring lander, waits on [`await_ensure_table_blocked`] until the lander is
/// observably blocked on that row, then commits — so the lander is guaranteed to take
/// the lost-race path (`ensure_table_witnessed` resolves it to the winner's
/// `table_id` with `created == false`) rather than racing nondeterministically.
///
/// The winning writer here is a raw mirror-row insert, not a full `land` — the race
/// this family cares about is decided entirely by that row, and a raw insert is what
/// makes the interleaving deterministic.
pub async fn losing_stream_declare(
    pool: &PgPool,
    catalog: &Arc<SqlCatalog>,
    table: &TableRef,
    buckets: i32,
) -> Result<SnapshotId, ControlPlaneError> {
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

    let (schema, batches) = batch();
    let pool_b = pool.clone();
    let cat_b = Arc::clone(catalog);
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
            Some(buckets),
        )
        .await
    });

    await_ensure_table_blocked(pool).await;
    winner.commit().await.expect("commit winner");
    lander.await.expect("join lander")
}

/// Field names of the Iceberg table's current physical schema, or `None` if the
/// Iceberg table does not exist at all. `table_exists` is a plain catalog SELECT and
/// does not require the namespace to exist, so "never created" comes back as `None`
/// rather than panicking.
pub async fn iceberg_field_names(catalog: &SqlCatalog, table: &TableRef) -> Option<Vec<String>> {
    let ident = TableIdent::new(
        NamespaceIdent::new(table.schema.clone()),
        table.name.clone(),
    );
    if !catalog.table_exists(&ident).await.expect("table_exists") {
        return None;
    }
    let loaded = catalog.load_table(&ident).await.expect("load_table");
    Some(
        loaded
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect(),
    )
}
