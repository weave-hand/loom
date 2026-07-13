//! Fixture tests for the MV read-position floor (`mv_floor`) and the
//! watermark-aware GC guard it drives (road-mv-watermark-aware-gc).
//!
//! The floor is the per-bucket `min(next_offset)` across every micro-batch MV
//! reading a source table — where an MV with no watermark row for a bucket
//! (a registered-but-never-run MV, or one that has never touched that bucket)
//! floors it at 0, exactly as `mv_delta_scan` reads it.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetId, EventType, LineageEvent, MvWatermarks, RunId, TableRef,
    TransformBody, TransformDef, TransformName, WatermarkAdvance, mv_key,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_mirror::live_table_id;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use control_plane_postgres::mv_floor::mv_floor;
use loom_test_seed::local_sql_catalog;
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
    #[expect(
        dead_code,
        reason = "unused by Task 1's floor tests; Task 2 appends the dropped-source \
                  GC test to this file, which drops through this Catalog handle"
    )]
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
