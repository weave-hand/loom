#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::let_underscore_must_use,
    clippy::unused_result_ok,
    clippy::map_err_ignore,
    clippy::unreachable,
    reason = "test/fixture harness code, not a production path"
)]
//! Shared seed/assert helpers for the end-cap-seam fixture tests
//! (`iss-end-cap-ignores-mv-floor`) and for `tests/mv_floor.rs`.
//!
//! Every helper here was either lifted from `tests/mv_floor.rs` (its previous, and
//! only, home) or is a seed shape more than one of the new tests needs. Per-test
//! topologies stay LOCAL to their test file — this library carries what is genuinely
//! shared, not everything.

use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlane, DatasetId, EventType, LineageEvent, MergeEngine,
    MvWatermarks, RunId, StreamTables, TableRef, TransformBody, TransformDef, TransformName,
    WatermarkAdvance, mv_key,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline::{
    current_inline_version, inline_append, inline_table_name, write_inline_delta,
};
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_mirror::{ensure_table, live_table_id, next_snapshot};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use control_plane_postgres::mv_floor::mv_floor;
use loom_test_seed::local_sql_catalog;
use sqlx::AssertSqlSafe;
use time::OffsetDateTime;

// ---- generic seed shapes ---------------------------------------------------

/// Convenience constructor for a `TableRef`.
pub fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

/// `(id: long, label: string)`. The STRING column is load-bearing, not decoration:
/// it puts a non-numeric `max_value` into `iceberg_mirror.data_file_column_stat`,
/// so the floor's file guard really runs against a stats table holding TEXT bounds.
/// The guard's `::bigint` cast lives OUTSIDE its scalar subquery precisely because
/// Postgres may reorder quals inside one `WHERE`; with only a `long` column, an
/// inlined cast would pass, and the hazard would go untested. Any refactor that
/// moves the cast back in now fails these tests instead of hard-erroring GC in prod.
///
/// `label` is **nullable** — a superset of `tests/mv_floor.rs`'s original
/// non-nullable copy — because a tombstone write NULLs every non-id column.
pub fn columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "label".into(),
            ty: "string".into(),
            nullable: true,
        },
    ]
}

/// An `(id: long, label: string)` schema + batch of ids `0..rows`.
pub fn batch(rows: i64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..rows).map(|i| format!("row-{i}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch");
    (schema, vec![b])
}

/// A minimal `LineageEvent` naming `table` as its sole output.
pub fn lineage(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "end-cap-seed-test" }),
    }
}

/// Register a micro-batch MV over `source` -> `s.<output>` WITHOUT a data trigger
/// (`on_input_commit: false`), so landing into the source never auto-fires a run:
/// tests drive the watermark by hand. Registration alone is what the floor keys
/// off — an MV that never runs must pin its source at 0.
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

/// CAS-advance `mv`'s watermark for `(source_tid, bucket)` from `from` to `to` —
/// what a completed micro-batch commit does (`pg_advance_mv_watermark`).
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

/// The seeded world every `mv_floor` test starts from.
pub struct Seeded {
    pub pool: sqlx::PgPool,
    pub catalog: SqlCatalog,
    pub src: TableRef,
    pub tid: i64,
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
pub async fn seed_source(
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

// ---- CDC seed shapes (Tasks 2b/6) ------------------------------------------

/// `(id: long, val: long)` — the CDC table shape `seed_floored_cdc` declares.
pub fn cdc_specs() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "val".into(),
            ty: "long".into(),
            nullable: false,
        },
    ]
}

/// A one-row `(id, val)` batch.
pub fn row_batch(id: i64, val: i64) -> RecordBatch {
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
    .expect("row batch")
}

/// A one-row, id-only batch — the shape `current_inline_version` reads.
pub fn id_only_batch(id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id]))]).expect("id batch")
}

/// A framed CDC batch for the `(id, val)` table: user columns first, then the
/// three reserved framing columns, in the order `augment_with_framing` builds
/// them (`loom_change_kind`, `loom_bucket`, `loom_offset`). This is what the CDC
/// consolidate fold produces and what `overwrite_stream_base` demands.
pub fn framed_cdc_batch(id: i64, val: i64, bucket: i32, offset: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
        // NOT nullable — `framing_column_specs` (`iceberg_landing.rs:662-665`) declares it
        // `nullable: false`, as do every provider-schema builder (`serving.rs`, `feed.rs`)
        // and the sibling fixture `tests/stream_overwrite_framing.rs:43`. `loom_bucket` /
        // `loom_offset` below ARE nullable. Getting this wrong does not panic
        // `RecordBatch::try_new` (it only checks arrays, not schema nullability), but any
        // consumer asserting schema equality against the production framing shape fails.
        Field::new("loom_change_kind", DataType::Utf8, false),
        Field::new("loom_bucket", DataType::Int32, true),
        Field::new("loom_offset", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![val])),
            Arc::new(StringArray::from(vec!["+I"])),
            Arc::new(Int32Array::from(vec![bucket])),
            Arc::new(Int64Array::from(vec![offset])),
        ],
    )
    .expect("framed batch")
}

/// A declared CDC table with an append + an update, carrying a watermark row
/// against it — so `mv_floor` is `Some` (a ghost key: `advance_mv_watermark` is
/// a bare CAS that never checks a def exists). Declared CDC BEFORE any write,
/// because the physical schema must carry framing from the start.
pub struct FlooredCdc {
    pub pool: sqlx::PgPool,
    pub catalog: SqlCatalog,
    pub table: TableRef,
    pub tid: i64,
    #[expect(
        clippy::pub_underscore_fields,
        reason = "the leading underscore is intentional: the field's only job is to keep \
                  the warehouse tempdir alive for the struct's lifetime, never to be read"
    )]
    pub _wh: tempfile::TempDir,
}

/// Seed a `main.widget` CDC table (append id=1 val=100, update to val=200) and
/// plant a ghost watermark row against it, so its `mv_floor` is `Some`. The
/// `consolidate_threshold: Some(1000)` on the update delta is load-bearing:
/// large enough that deltas accrue without enqueuing a job, while still making
/// `bump_consolidate_trigger` run — with `None` no `consolidate_trigger` row
/// exists at all, and a later assertion on it fails with `RowNotFound`.
pub async fn seed_floored_cdc(fx: &PgFixture, cp: &PgControlPlane, db: &str) -> FlooredCdc {
    let pool = fx.pool_for(db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let table = tref("main", "widget");
    let cols = cdc_specs();

    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 1, "id", MergeEngine::LastRow)
        .await
        .expect("declare_cdc");

    inline_append(
        &pool,
        &table,
        &cols,
        &row_batch(1, 100),
        lineage(&table),
        None,
        None,
    )
    .await
    .expect("seed append");
    let v0 = current_inline_version(&pool, &table, &[cols[0].clone()], "id", &id_only_batch(1))
        .await
        .expect("version");
    write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &row_batch(1, 200),
        Some((&cols, &row_batch(1, 100))),
        lineage(&table),
        v0,
        Some(1000),
        &[],
    )
    .await
    .expect("cdc update delta");

    // Plant the floor: a watermark row against this CDC source.
    cp.advance_mv_watermark(
        &mv_key(&tref("main", "out_a")),
        tid,
        &[WatermarkAdvance {
            bucket: 0,
            from: 0,
            to: 1,
        }],
    )
    .await
    .expect("advance watermark");
    let mut conn = pool.acquire().await.expect("conn");
    assert!(
        mv_floor(&mut conn, &table, tid)
            .await
            .expect("mv_floor")
            .is_some(),
        "sanity: the watermark row must produce a floor, or these tests prove nothing"
    );
    drop(conn);

    FlooredCdc {
        pool,
        catalog,
        table,
        tid,
        _wh: wh,
    }
}

// ---- assertions -------------------------------------------------------------

/// Backdate EVERY snapshot so the whole history is aged out (H = max snapshot id).
pub async fn age_all_snapshots(pool: &sqlx::PgPool) {
    let old = OffsetDateTime::now_utc() - time::Duration::days(365);
    sqlx::query("update iceberg_mirror.snapshot set snapshot_time = $1")
        .bind(old)
        .execute(pool)
        .await
        .expect("age all snapshots");
}

/// End-capped (GC-candidate) rows still physically present in `inline_<tid>`.
/// The physical name comes from `inline_table_name` (schema-qualified), and the
/// formatted SQL needs `AssertSqlSafe` — sqlx 0.9 only accepts a literal otherwise.
pub async fn end_capped_inline_count(pool: &sqlx::PgPool, tid: i64) -> i64 {
    let inline = inline_table_name(tid);
    sqlx::query_scalar(AssertSqlSafe(format!(
        "select count(*) from {inline} where end_snapshot is not null"
    )))
    .fetch_one(pool)
    .await
    .expect("count end-capped inline rows")
}

/// `iceberg_mirror.data_file` rows for `tid` (any `end_snapshot`).
pub async fn data_file_count(pool: &sqlx::PgPool, tid: i64) -> i64 {
    sqlx::query_scalar("select count(*) from iceberg_mirror.data_file where table_id = $1")
        .bind(tid)
        .fetch_one(pool)
        .await
        .expect("count data files")
}

/// `iceberg_mirror.data_file` rows for `tid` still LIVE (`end_snapshot is null`) —
/// the subset an end-cap or GC pass would touch next, as opposed to
/// [`data_file_count`]'s "any `end_snapshot`" superset.
pub async fn live_file_count(pool: &sqlx::PgPool, tid: i64) -> i64 {
    sqlx::query_scalar(
        "select count(*) from iceberg_mirror.data_file where table_id = $1 and end_snapshot is null",
    )
    .bind(tid)
    .fetch_one(pool)
    .await
    .expect("count live data files")
}

/// The table's current snapshot id. NOTE `Snapshot::id` is the `SnapshotId(i64)`
/// newtype (`core/src/catalog.rs:16`) — unwrap it with `.0`.
pub async fn current_snapshot_id(pool: &sqlx::PgPool, table: &TableRef) -> i64 {
    IcebergCatalog::new(pool.clone())
        .current_snapshot(table)
        .await
        .expect("current snapshot")
        .id
        .0
}
