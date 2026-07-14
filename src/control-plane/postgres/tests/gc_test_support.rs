#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "shared fixture-test support library, not a production path"
)]
//! Shared fixture-test support for the GC / retention-guard test family
//! (`tests/iceberg_gc.rs`, `tests/timetravel_intact.rs`) —
//! `iss-timetravel-quiet-table-overconservative` Task 3.
//!
//! `iceberg_gc.rs` alone repeated the hermetic `PgFixture` harness preamble
//! (fresh DB + temp warehouse + vendored `SqlCatalog` + direct-query pool) 10
//! times, plus a handful of small `land`/age/lineage helpers. Extracted here
//! UP FRONT — before `timetravel_intact.rs` needed its own copy — rather than
//! doing the copy-paste-then-extract round trip: see the controller note on
//! Task 3 of `iss-timetravel-quiet-table-overconservative`.
//!
//! Deliberately NOT included: anything specific to one test's topology (e.g.
//! `reclaim_watermark_is_monotone`'s two-bucket MV setup stays local to
//! `iceberg_gc.rs` — it is a one-off arrangement, not a repeated shape).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use control_plane_core::{
    ColumnSpec, ControlPlane, LineageEvent, MvWatermarks, RunId, TableRef, TransformBody,
    TransformDef, TransformName, WatermarkAdvance,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use loom_test_seed::local_sql_catalog;

/// A GC-reclaim horizon comfortably past any realistic retention window — the
/// `gc_retention` every fixture test in this family calls `gc_table` with.
pub const SEVEN_DAYS: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 3600);

/// Boot a fresh hermetic-fixture DB, a temp warehouse, the vendored
/// `SqlCatalog` over it, and a direct-query pool — THE preamble every
/// fixture test in this family starts with. Keep the returned `TempDir`
/// alive in the caller's scope: dropping it removes the warehouse directory
/// the catalog's `file://` URLs point at.
pub async fn harness(
    fx: &PgFixture,
) -> (
    PgControlPlane,
    String,
    tempfile::TempDir,
    SqlCatalog,
    sqlx::PgPool,
) {
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    (cp, db, wh, catalog, pool)
}

/// A single `id: long` (non-null) column spec, for `land`/`overwrite_parquet_snapshot`.
pub fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// A schema + batch of `rows` rows (`id: long` = `0..rows`), for `land`. `land`
/// takes pre-decoded batches directly, so this reuses `batch` rather than
/// round-tripping through an Arrow IPC encode/decode.
pub fn ipc_body(rows: i64) -> (Arc<Schema>, Vec<RecordBatch>) {
    let b = batch(rows);
    (b.schema(), vec![b])
}

/// A bare `id: long` record batch of ids `0..rows`.
pub fn batch(rows: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .expect("batch")
}

/// A minimal Complete lineage event targeting `schema.name`, payload
/// `{"source": "test"}` — a thin `TableRef`-building wrapper over
/// `loom_test_seed::test_lineage`.
pub fn lineage(run: RunId, schema: &str, name: &str) -> LineageEvent {
    loom_test_seed::test_lineage(
        run,
        &TableRef {
            schema: schema.into(),
            name: name.into(),
        },
    )
}

/// Strip a `file://` URL to a local filesystem path.
pub fn local_path(file_url: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(file_url.strip_prefix("file://").unwrap_or(file_url))
}

/// Backdate snapshot `snap_id`'s `snapshot_time` so it looks aged out of the window.
pub async fn age_snapshot(pool: &sqlx::PgPool, snap_id: i64) {
    let old = time::OffsetDateTime::now_utc() - time::Duration::days(365);
    sqlx::query("update iceberg_mirror.snapshot set snapshot_time = $1 where snapshot_id = $2")
        .bind(old)
        .bind(snap_id)
        .execute(pool)
        .await
        .expect("age snapshot");
}

/// Backdate EVERY snapshot so the whole history looks aged out (H = max snapshot id).
pub async fn age_all_snapshots(pool: &sqlx::PgPool) {
    let old = time::OffsetDateTime::now_utc() - time::Duration::days(365);
    sqlx::query("update iceberg_mirror.snapshot set snapshot_time = $1")
        .bind(old)
        .execute(pool)
        .await
        .expect("age all snapshots");
}

/// The currently-live `table_id` for `(ns, name)` (fixture-side, before a drop).
/// `iceberg_mirror.table` is spelled unquoted to match the crate's own SQL (the
/// keyword parses fine after the schema qualifier — the committed `.sqlx` cache
/// proves real Postgres accepts it).
pub async fn live_tid(pool: &sqlx::PgPool, ns: &str, name: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "select table_id from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
    )
    .bind(ns)
    .bind(name)
    .fetch_one(pool)
    .await
    .expect("live tid")
}

/// Count of `table`/`column`/`data_file` mirror rows for a specific `table_id`
/// (used to assert a dropped incarnation's metadata is fully gone). Returns
/// `(table_rows, column_rows, data_file_rows)`.
pub async fn mirror_row_counts(pool: &sqlx::PgPool, tid: i64) -> (i64, i64, i64) {
    let t: i64 =
        sqlx::query_scalar("select count(*) from iceberg_mirror.table where table_id = $1")
            .bind(tid)
            .fetch_one(pool)
            .await
            .expect("t count");
    let c: i64 =
        sqlx::query_scalar("select count(*) from iceberg_mirror.column where table_id = $1")
            .bind(tid)
            .fetch_one(pool)
            .await
            .expect("c count");
    let d: i64 =
        sqlx::query_scalar("select count(*) from iceberg_mirror.data_file where table_id = $1")
            .bind(tid)
            .fetch_one(pool)
            .await
            .expect("d count");
    (t, c, d)
}

/// Count of `iceberg_mirror.inline_trigger` rows for a specific `table_id`.
pub async fn inline_trigger_count(pool: &sqlx::PgPool, tid: i64) -> i64 {
    sqlx::query_scalar("select count(*) from iceberg_mirror.inline_trigger where table_id = $1")
        .bind(tid)
        .fetch_one(pool)
        .await
        .expect("inline_trigger count")
}

/// True if the physical `iceberg_mirror.inline_<tid>` table still exists.
pub async fn inline_table_exists(pool: &sqlx::PgPool, tid: i64) -> bool {
    let name = format!("iceberg_mirror.inline_{tid}");
    let reg: Option<String> = sqlx::query_scalar("select to_regclass($1)::text")
        .bind(&name)
        .fetch_one(pool)
        .await
        .expect("to_regclass");
    reg.is_some()
}

/// Read the live incarnation's reclaim watermark straight from the mirror.
pub async fn reclaimed_through(pool: &sqlx::PgPool, tid: i64) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "select reclaimed_through from iceberg_mirror.table where table_id = $1",
    )
    .bind(tid)
    .fetch_one(pool)
    .await
    .expect("reclaimed_through")
}

/// Register a micro-batch MV over `source` -> `s.<output>` WITHOUT a data trigger
/// (`on_input_commit: false`), so landing into the source never auto-fires a run —
/// the caller drives the watermark by hand via [`advance`]. Mirrors
/// `tests/mv_floor.rs`'s `register_mv`.
pub async fn register_mv(cp: &PgControlPlane, name: &str, source: &TableRef, output: &TableRef) {
    cp.transforms()
        .define_transform(TransformDef {
            name: TransformName(name.into()),
            body: TransformBody::MicroBatch {
                source: source.clone(),
                output: output.clone(),
                buckets: 1,
                sql: "select id from events".into(),
            },
            schedule: None,
            on_input_commit: false,
        })
        .await
        .expect("register mv");
}

/// CAS-advance `mv`'s watermark for `(source_tid, bucket)` from `from` to `to`.
pub async fn advance(
    cp: &PgControlPlane,
    mv: &str,
    source_tid: i64,
    bucket: i32,
    from: i64,
    to: i64,
) {
    cp.advance_mv_watermark(mv, source_tid, &[WatermarkAdvance { bucket, from, to }])
        .await
        .expect("advance watermark");
}
