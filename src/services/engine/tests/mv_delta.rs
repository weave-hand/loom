//! MvDeltaTicket over the wire: seed a 2-bucket log stream table with inline +
//! flushed rows, fetch the framed delta from watermark zero, advance the
//! watermark, fetch the tail only. A non-stream source is a deterministic error.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, EventType, LineageEvent, MvWatermarks, RunId, TableRef,
    TransformBody, TransformDef, TransformName, WatermarkAdvance,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_inline::inline_append;
use engine_wire::flight::FlightTableClient;
use loom_test_flight::spawn_flight_uds;
use loom_test_seed::local_sql_catalog;

// ---- helpers ---------------------------------------------------------------

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

fn id_spec() -> ColumnSpec {
    ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }
}

fn id_batch(ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids.to_vec()))]).expect("batch")
}

fn lin() -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "mv-delta-test" }),
    }
}

/// Resolve the internal mirror `table_id`, the same way the other inline tests do
/// (`stream_flush_persist.rs`'s `tid_of`).
async fn tid_of(pool: &sqlx::PgPool, schema: &str, name: &str) -> i64 {
    sqlx::query_scalar(
        "select table_id from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
    )
    .bind(schema)
    .bind(name)
    .fetch_one(pool)
    .await
    .expect("table_id")
}

/// Every batch must carry the three reserved framing columns.
fn assert_carries_framing(batches: &[RecordBatch]) {
    assert!(!batches.is_empty(), "expected at least one batch");
    for b in batches {
        for c in ["loom_change_kind", "loom_bucket", "loom_offset"] {
            assert!(
                b.schema().index_of(c).is_ok(),
                "batch missing framing column {c}: {:?}",
                b.schema()
            );
        }
    }
}

/// Flatten every batch's `(id, loom_bucket, loom_offset)` rows, in the batches'
/// arrival (= claimed sort) order.
fn framed_rows(batches: &[RecordBatch]) -> Vec<(i64, i32, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let id_idx = b.schema().index_of("id").expect("id col");
        let bucket_idx = b.schema().index_of("loom_bucket").expect("loom_bucket col");
        let offset_idx = b.schema().index_of("loom_offset").expect("loom_offset col");
        let ids = b
            .column(id_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let buckets = b
            .column(bucket_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("loom_bucket is Int32");
        let offsets = b
            .column(offset_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("loom_offset is Int64");
        for i in 0..b.num_rows() {
            out.push((ids.value(i), buckets.value(i), offsets.value(i)));
        }
    }
    out
}

fn sorted(mut ids: Vec<i64>) -> Vec<i64> {
    ids.sort_unstable();
    ids
}

// ---- tests -----------------------------------------------------------------

/// Legs 1-3 in one test (a shared seed): full delta (files ∪ inline, framed,
/// (bucket, offset)-ordered, gapless per bucket), the watermarked tail after
/// advancing to the post-flush high-water per bucket, and an independent `mv`
/// consumer unaffected by the first's watermark (watermarks are per-mv).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mv_delta_full_watermarked_tail_and_independent_consumer() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let wh_str = wh.path().display().to_string();
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh_str).await;
    let run = RunId(uuid::Uuid::new_v4());

    let src = tref("s", "events");
    let cols = vec![id_spec()];

    // 4 rows inline (declares a 2-bucket log stream), then flush to Parquet.
    inline_append(
        &pool,
        &src,
        &cols,
        &id_batch(&[1, 2, 3, 4]),
        lin(),
        None,
        Some(2),
    )
    .await
    .expect("append 1..4");

    // #627: `advance_mv_watermark("s.out", ...)` in leg 2 below now requires a live
    // micro-batch def naming the key. Register it HERE — right after `s.events` is
    // declared a 2-bucket stream table (so `bootstrap_mv_watermarks` can resolve its
    // bucket count) but BEFORE the flush. Ordering is load-bearing:
    // `define_transform` -> `reconcile_mv_watermarks` -> `bootstrap_mv_watermarks`
    // seeds `s.out`'s watermark to the source's earliest surviving offset, which is 0
    // at this point (nothing reclaimed yet). That keeps leg 2's `from: 0` advance
    // matching the seeded row; registering after the flush would risk seeding a
    // non-zero row and changing the CAS semantics under test.
    cp.transforms()
        .define_transform(TransformDef {
            name: TransformName("def_s_out".into()),
            body: TransformBody::MicroBatch {
                source: src.clone(),
                output: tref("s", "out"),
                buckets: 2,
                sql: "select * from mv_delta".into(),
            },
            schedule: None,
            on_input_commit: false,
        })
        .await
        .expect("register s.out mv def");

    flush_table(&catalog, &pool, &src, run)
        .await
        .expect("flush")
        .expect("flushed something");

    // 2 more rows, an un-flushed inline tail (no redeclare needed — already a
    // stream table; `combine_stream_decl`/`inline_append`'s `StreamDecl::None`
    // path leaves the existing declaration alone).
    inline_append(&pool, &src, &cols, &id_batch(&[5, 6]), lin(), None, None)
        .await
        .expect("append 5..6");

    let eng = spawn_flight_uds(fx, &db, &wh_str).await;
    let client = FlightTableClient::connect(&eng.sock)
        .await
        .expect("connect");

    // Leg 1: full delta from watermark zero — all 6 rows, files ∪ inline, framed.
    let batches = client
        .fetch_mv_delta("s.out".into(), "s".into(), "events".into())
        .await
        .expect("fetch_mv_delta");
    assert_carries_framing(&batches);
    let rows = framed_rows(&batches);
    assert_eq!(
        sorted(rows.iter().map(|(id, _, _)| *id).collect()),
        vec![1, 2, 3, 4, 5, 6],
        "all 6 rows present, files union inline"
    );

    // (bucket, offset)-ordered...
    let mut prev: Option<(i32, i64)> = None;
    for (_, b, o) in &rows {
        if let Some(p) = prev {
            assert!(
                p <= (*b, *o),
                "rows must be (bucket, offset)-ordered: {rows:?}"
            );
        }
        prev = Some((*b, *o));
    }
    // ...and per-bucket offsets are gapless from 0.
    let mut by_bucket: HashMap<i32, Vec<i64>> = HashMap::new();
    for (_, b, o) in &rows {
        by_bucket.entry(*b).or_default().push(*o);
    }
    for (bucket, mut offsets) in by_bucket {
        offsets.sort_unstable();
        let want: Vec<i64> = (0..offsets.len() as i64).collect();
        assert_eq!(
            offsets, want,
            "bucket {bucket} offsets gapless: {offsets:?}"
        );
    }

    // Leg 2: watermarked tail. Compute the post-flush high-water offset per
    // bucket from leg 1's framing (one past the greatest offset among the
    // FLUSHED ids, 1..4), advance `s.out`'s watermark to it, and re-fetch —
    // expect exactly the tail (ids 5, 6).
    let flushed_ids: HashSet<i64> = [1_i64, 2, 3, 4].into_iter().collect();
    let mut high_water: HashMap<i32, i64> = HashMap::new();
    for (id, b, o) in &rows {
        if flushed_ids.contains(id) {
            let entry = high_water.entry(*b).or_insert(0);
            *entry = (*entry).max(*o + 1);
        }
    }
    let src_tid = tid_of(&pool, "s", "events").await;
    let advances: Vec<WatermarkAdvance> = high_water
        .iter()
        .map(|(&bucket, &to)| WatermarkAdvance {
            bucket,
            from: 0,
            to,
        })
        .collect();
    cp.advance_mv_watermark("s.out", src_tid, &advances)
        .await
        .expect("advance watermark");

    let tail_batches = client
        .fetch_mv_delta("s.out".into(), "s".into(), "events".into())
        .await
        .expect("fetch tail");
    let tail_rows = framed_rows(&tail_batches);
    assert_eq!(
        sorted(tail_rows.iter().map(|(id, _, _)| *id).collect()),
        vec![5, 6],
        "watermarked re-fetch returns exactly the un-flushed tail"
    );

    // Leg 3: an independent consumer (a different `mv` key) still sees all 6 —
    // watermarks are per-mv, not per-source-table.
    let other_batches = client
        .fetch_mv_delta("s.other".into(), "s".into(), "events".into())
        .await
        .expect("fetch other consumer");
    assert_eq!(
        sorted(
            framed_rows(&other_batches)
                .iter()
                .map(|(id, _, _)| *id)
                .collect()
        ),
        vec![1, 2, 3, 4, 5, 6],
        "an independent mv consumer is unaffected by s.out's watermark"
    );
}

/// Leg 4: a plain (non-stream) source table is a deterministic error naming the
/// table — the worker (a later task) maps this to abandoning the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mv_delta_non_stream_source_is_a_deterministic_error() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let wh_str = wh.path().display().to_string();

    let plain = tref("s", "plain");
    inline_append(
        &pool,
        &plain,
        &[id_spec()],
        &id_batch(&[1, 2]),
        lin(),
        None,
        None,
    )
    .await
    .expect("land plain");

    let eng = spawn_flight_uds(fx, &db, &wh_str).await;
    let client = FlightTableClient::connect(&eng.sock)
        .await
        .expect("connect");

    let err = client
        .fetch_mv_delta("s.out".into(), "s".into(), "plain".into())
        .await
        .expect_err("a non-stream source must fail");
    assert!(
        err.to_string().contains("s.plain"),
        "error names the offending table, got: {err}"
    );
}
