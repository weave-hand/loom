//! Fixture tests for the MV read-position floor (`mv_floor`) and the
//! watermark-aware GC guard it drives (road-mv-watermark-aware-gc).
//!
//! The floor is the per-bucket `min(next_offset)` across every micro-batch MV
//! reading a source table — where an MV with no watermark row for a bucket
//! (a registered-but-never-run MV, or one that has never touched that bucket)
//! floors it at 0, exactly as `mv_delta_scan` reads it.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlane, DatasetId, EventType, LineageEvent, MvWatermarks, RunId,
    TableRef, TransformBody, TransformDef, TransformName, WatermarkAdvance, mv_key,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_gc::gc_table;
use control_plane_postgres::iceberg_inline::inline_table_name;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_mirror::live_table_id;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use control_plane_postgres::mv_floor::mv_floor;
use iceberg::{Catalog as _, NamespaceIdent, TableIdent};
use loom_test_seed::local_sql_catalog;
use sqlx::AssertSqlSafe;
use time::OffsetDateTime;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// An `(id: long)` schema + batch of ids `0..rows`.
fn batch(rows: i64) -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .expect("batch");
    (schema, vec![b])
}

fn lineage(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "mv-floor-test" }),
    }
}

/// Register a micro-batch MV over `source` -> `s.<output>` WITHOUT a data trigger
/// (`on_input_commit: false`), so landing into the source never auto-fires a run:
/// these tests drive the watermark by hand. Registration alone is what the floor
/// keys off — an MV that never runs must pin its source at 0.
async fn register_mv(cp: &PgControlPlane, name: &str, source: &TableRef, output: &TableRef) {
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

/// CAS-advance `mv`'s watermark for `(source_tid, bucket)` from `from` to `to` —
/// what a completed micro-batch commit does (`pg_advance_mv_watermark`).
async fn advance(cp: &PgControlPlane, mv: &str, source_tid: i64, bucket: i32, from: i64, to: i64) {
    cp.advance_mv_watermark(mv, source_tid, &[WatermarkAdvance { bucket, from, to }])
        .await
        .expect("advance watermark");
}

/// The seeded world every test in this file starts from.
struct Seeded {
    pool: sqlx::PgPool,
    catalog: SqlCatalog,
    src: TableRef,
    tid: i64,
}

/// Seed `s.events` with `rows` events: a declared log stream table of
/// `buckets` buckets (`None` = a plain, non-stream table), landed INLINE when
/// `inline` is true and straight into Parquet FILES when false, with one MV
/// registered per `(transform name, output name)` in `mvs`.
#[expect(
    clippy::too_many_arguments,
    reason = "this is the ONE shared seed shape six tests across Tasks 1-3 reuse; \
              splitting it would fragment that shared shape, which is exactly the \
              duplication this helper exists to avoid"
)]
async fn seed_source(
    fx: &PgFixture,
    cp: &PgControlPlane,
    db: &str,
    wh: &str,
    rows: i64,
    buckets: Option<i32>,
    inline: bool,
    mvs: &[(&str, &str)],
) -> Seeded {
    let pool = fx.pool_for(db).await;
    let catalog = local_sql_catalog(fx.pg_dsn(db), wh).await;
    let src = tref("s", "events");
    let (schema, batches) = batch(rows);
    land(
        &pool,
        &catalog,
        &src,
        &columns(),
        schema,
        batches,
        InlineLimits {
            // A 0 byte limit forces the write straight to Parquet; a large one
            // keeps every row inline until an explicit flush.
            inline_byte_limit: if inline { 1 << 20 } else { 0 },
            flush_byte_threshold: i64::MAX,
        },
        lineage(&src),
        buckets,
    )
    .await
    .expect("land source");
    for (name, output) in mvs {
        register_mv(cp, name, &src, &tref("s", output)).await;
    }
    let mut conn = pool.acquire().await.expect("conn");
    let tid = live_table_id(&mut conn, &src.schema, &src.name)
        .await
        .expect("tid")
        .expect("live tid");
    drop(conn);
    Seeded {
        pool,
        catalog,
        src,
        tid,
    }
}

// ---- floor semantics -------------------------------------------------------

/// A plain (non-stream) table has no floor at all: GC keeps its fast path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_table_has_no_floor() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        3,
        None,
        true,
        &[],
    )
    .await;

    assert_eq!(
        mv_floor(&s.pool, &s.src, s.tid).await.expect("floor"),
        None,
        "a non-stream table has no MV floor"
    );
}

/// A declared stream table nothing reads: still no floor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_table_without_readers_has_no_floor() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        4,
        Some(2),
        true,
        &[],
    )
    .await;

    assert_eq!(
        mv_floor(&s.pool, &s.src, s.tid).await.expect("floor"),
        None,
        "no MV reads this stream table -> no floor"
    );
}

/// A registered MV that has never run pins EVERY bucket at 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_but_unrun_mv_floors_at_zero() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        4,
        Some(2),
        true,
        &[("mv_a", "out_a")],
    )
    .await;

    let floor = mv_floor(&s.pool, &s.src, s.tid)
        .await
        .expect("floor")
        .expect("a registered MV reads this source");
    assert_eq!(
        floor.per_bucket,
        [(0, 0), (1, 0)].into_iter().collect(),
        "an unrun MV floors every bucket at 0"
    );
    assert_eq!(floor.min_offset(), 0, "the file guard holds everything");
}

/// Two MVs at different progress: the SLOWER one sets each bucket's floor, and an
/// MV with no row for a bucket floors THAT bucket at 0 even while ahead elsewhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slowest_mv_sets_the_floor_per_bucket() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        8,
        Some(2),
        true,
        &[("mv_a", "out_a"), ("mv_b", "out_b")],
    )
    .await;
    let a = mv_key(&tref("s", "out_a"));
    let b = mv_key(&tref("s", "out_b"));

    // mv_a consumed bucket 0 to 5 and bucket 1 to 3; mv_b consumed only bucket 0,
    // to 2, and has never touched bucket 1.
    advance(&cp, &a, s.tid, 0, 0, 5).await;
    advance(&cp, &a, s.tid, 1, 0, 3).await;
    advance(&cp, &b, s.tid, 0, 0, 2).await;

    let floor = mv_floor(&s.pool, &s.src, s.tid)
        .await
        .expect("floor")
        .expect("two MVs read this source");
    assert_eq!(
        floor.per_bucket,
        [(0, 2), (1, 0)].into_iter().collect(),
        "bucket 0: min(5, 2) = 2; bucket 1: mv_b has no row there -> 0"
    );
    assert_eq!(
        floor.slowest.get(&0).map(String::as_str),
        Some(b.as_str()),
        "bucket 0's laggard is mv_b"
    );
    assert_eq!(
        floor.min_offset(),
        0,
        "the file guard takes the cross-bucket minimum"
    );
}

/// A single caught-up MV floors at its watermark (the release case Task 2 leans on).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caught_up_mv_floors_at_its_watermark() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        4,
        Some(1),
        true,
        &[("mv_a", "out_a")],
    )
    .await;
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 4).await;

    let floor = mv_floor(&s.pool, &s.src, s.tid)
        .await
        .expect("floor")
        .expect("registered MV");
    assert_eq!(floor.per_bucket, [(0, 4)].into_iter().collect());
    assert_eq!(floor.min_offset(), 4);
}

const SEVEN_DAYS: Duration = Duration::from_secs(7 * 24 * 3600);

/// Backdate EVERY snapshot so the whole history is aged out (H = max snapshot id).
async fn age_all_snapshots(pool: &sqlx::PgPool) {
    let old = OffsetDateTime::now_utc() - time::Duration::days(365);
    sqlx::query("update iceberg_mirror.snapshot set snapshot_time = $1")
        .bind(old)
        .execute(pool)
        .await
        .expect("age all snapshots");
}

/// End-cap every live data file of `tid` at snapshot `snap`. This is the mirror
/// state EVERY file-retiring path leaves behind (compaction, a future small-file
/// merge, stream retention) — the class the floor exists to guard. It is done in
/// SQL because the real writers of that state live ABOVE this crate (they must
/// write replacement Parquet with DataFusion first).
async fn end_cap_data_files(pool: &sqlx::PgPool, tid: i64, snap: i64) {
    sqlx::query(
        "update iceberg_mirror.data_file set end_snapshot = $1 \
         where table_id = $2 and end_snapshot is null",
    )
    .bind(snap)
    .bind(tid)
    .execute(pool)
    .await
    .expect("end-cap data files");
}

/// End-capped (GC-candidate) rows still physically present in `inline_<tid>`.
/// The physical name comes from `inline_table_name` (schema-qualified), and the
/// formatted SQL needs `AssertSqlSafe` — sqlx 0.9 only accepts a literal otherwise.
async fn end_capped_inline_count(pool: &sqlx::PgPool, tid: i64) -> i64 {
    let inline = inline_table_name(tid);
    sqlx::query_scalar(AssertSqlSafe(format!(
        "select count(*) from {inline} where end_snapshot is not null"
    )))
    .fetch_one(pool)
    .await
    .expect("count end-capped inline rows")
}

/// `iceberg_mirror.data_file` rows for `tid` (any `end_snapshot`).
async fn data_file_count(pool: &sqlx::PgPool, tid: i64) -> i64 {
    sqlx::query_scalar("select count(*) from iceberg_mirror.data_file where table_id = $1")
        .bind(tid)
        .fetch_one(pool)
        .await
        .expect("count data files")
}

/// The table's current snapshot id. NOTE `Snapshot::id` is the `SnapshotId(i64)`
/// newtype (`core/src/catalog.rs:16`) — unwrap it with `.0`.
async fn current_snapshot_id(pool: &sqlx::PgPool, table: &TableRef) -> i64 {
    IcebergCatalog::new(pool.clone())
        .current_snapshot(table)
        .await
        .expect("current snapshot")
        .id
        .0
}

// ---- GC under the floor ----------------------------------------------------

/// A lagging MV holds its source's unread tail: end-capped inline rows AT OR ABOVE
/// the MV's watermark survive GC, the ones below it are reclaimed, and
/// `held_by_mv_floor` counts the difference.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lagging_mv_holds_the_unread_tail() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // 6 inline events (offsets 0..6) in one bucket, one MV registered.
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        true,
        &[("mv_a", "out_a")],
    )
    .await;

    // The MV has consumed offsets 0,1,2 (next_offset = 3): 3,4,5 are unread.
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 3).await;

    // Flush: the 6 rows land in a live Parquet file and their inline copies are
    // end-capped — the age-eligible candidates GC would otherwise take.
    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    age_all_snapshots(&s.pool).await;
    assert_eq!(
        end_capped_inline_count(&s.pool, s.tid).await,
        6,
        "all 6 inline rows are end-capped candidates"
    );

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    assert_eq!(
        summary.inline_rows, 3,
        "only offsets 0,1,2 — strictly below the floor — are reclaimed"
    );
    assert_eq!(
        summary.held_by_mv_floor, 3,
        "offsets 3,4,5 are held for the lagging MV"
    );
    assert_eq!(
        end_capped_inline_count(&s.pool, s.tid).await,
        3,
        "the unread tail is physically still there"
    );
}

/// Once the MV catches up, the next GC reclaims what was held: the floor releases.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caught_up_mv_releases_the_tail() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        true,
        &[("mv_a", "out_a")],
    )
    .await;
    let mv = mv_key(&tref("s", "out_a"));
    advance(&cp, &mv, s.tid, 0, 0, 3).await;
    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    age_all_snapshots(&s.pool).await;
    let first = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("first gc");
    assert_eq!(first.held_by_mv_floor, 3, "the tail is held while lagging");

    // The MV consumes the rest (3 -> 6).
    advance(&cp, &mv, s.tid, 0, 3, 6).await;

    let second = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("second gc");
    assert_eq!(second.inline_rows, 3, "the held tail is now reclaimed");
    assert_eq!(second.held_by_mv_floor, 0, "nothing is held any more");
    assert_eq!(
        end_capped_inline_count(&s.pool, s.tid).await,
        0,
        "no end-capped inline rows remain"
    );
}

/// A registered-but-never-run MV pins EVERYTHING — including end-capped FILES and
/// their Parquet, which is the tier a future compaction/retention path would eat.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrun_mv_pins_every_end_capped_row_and_file() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // Land straight to FILES; register an MV and never run it.
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        false,
        &[("mv_a", "out_a")],
    )
    .await;

    let files_before = data_file_count(&s.pool, s.tid).await;
    assert!(files_before > 0, "the source landed at least one file");
    let snap = current_snapshot_id(&s.pool, &s.src).await;
    end_cap_data_files(&s.pool, s.tid, snap).await;
    age_all_snapshots(&s.pool).await;

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    assert_eq!(
        summary.data_file_rows, 0,
        "every file carries offsets the unrun MV has not read (floor 0): none reclaimed"
    );
    assert_eq!(summary.objects_deleted, 0, "no Parquet object deleted");
    assert_eq!(
        summary.held_by_mv_floor,
        u64::try_from(files_before).expect("count fits u64"),
        "every end-capped file is held by the floor"
    );
    assert_eq!(
        data_file_count(&s.pool, s.tid).await,
        files_before,
        "the mirror rows survive"
    );
}

/// Two MVs at different offsets: the SLOWER one bounds the reclaim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slowest_of_two_mvs_bounds_the_reclaim() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        true,
        &[("mv_a", "out_a"), ("mv_b", "out_b")],
    )
    .await;
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 5).await;
    advance(&cp, &mv_key(&tref("s", "out_b")), s.tid, 0, 0, 2).await;

    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    age_all_snapshots(&s.pool).await;

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    assert_eq!(
        summary.inline_rows, 2,
        "offsets 0,1 only — mv_b, at 2, is the laggard"
    );
    assert_eq!(summary.held_by_mv_floor, 4, "offsets 2..6 are held");
}

/// A table no MV reads GCs exactly as before the floor existed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_source_table_gcs_unchanged() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        None,
        true,
        &[],
    )
    .await;

    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    age_all_snapshots(&s.pool).await;

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    assert_eq!(
        summary.inline_rows, 6,
        "every end-capped inline row reclaimed"
    );
    assert_eq!(summary.held_by_mv_floor, 0, "no floor, nothing held");
    assert_eq!(end_capped_inline_count(&s.pool, s.tid).await, 0);
}

/// Dropping the source BYPASSES the floor: the dropped incarnation is fully
/// reclaimed even with a lagging MV registered (drop-GC must converge; the run
/// logs a warning naming the stranded MVs).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_source_bypasses_the_floor() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        false,
        &[("mv_a", "out_a")],
    )
    .await;
    // The MV is barely started: offset 1 of 6 — a floor that would hold everything
    // above it if this were a live table.
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 1).await;
    let files_before = data_file_count(&s.pool, s.tid).await;
    assert!(files_before > 0, "the source landed at least one file");

    // Drop the source through the iceberg Catalog trait — the same call the
    // dropped-incarnation tests in `tests/iceberg_gc.rs:535` make.
    let ident = TableIdent::new(
        NamespaceIdent::new(s.src.schema.clone()),
        s.src.name.clone(),
    );
    s.catalog.drop_table(&ident).await.expect("drop source");
    // Age the whole history so the drop snapshot itself is past the horizon (full
    // reclaim of the dropped incarnation).
    age_all_snapshots(&s.pool).await;

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    assert!(
        summary.data_file_rows > 0,
        "the dropped incarnation's files are reclaimed — the floor is bypassed"
    );
    assert_eq!(
        summary.held_by_mv_floor, 0,
        "held counts the LIVE incarnation only; there is none"
    );
    assert_eq!(
        data_file_count(&s.pool, s.tid).await,
        0,
        "no data_file row of the dropped incarnation survives"
    );
}
