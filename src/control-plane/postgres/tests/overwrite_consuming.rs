//! Fixture tests: `overwrite_parquet_snapshot_consuming` — the consuming variant of
//! the overwrite/replace commit primitive. A commit carrying a targeted
//! `InlineEndCap { table_id, row_ids }` must retire EXACTLY those inline rows and
//! leave every other live inline row alone, so a mutation landing mid-consolidation
//! survives and keeps shadowing the new base. A plain `overwrite_parquet_snapshot`
//! (no cap) stays byte-identical — it still blanket-caps every live inline row.

use loom_test_seed::local_sql_catalog;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline::inline_append;
use control_plane_postgres::iceberg_landing::{
    InlineLimits, land, overwrite_parquet_snapshot, overwrite_parquet_snapshot_consuming,
};
use control_plane_postgres::iceberg_sql_catalog::InlineEndCap;

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

fn inline_batch(ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids.to_vec()))]).expect("batch")
}

fn lineage() -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({}),
    }
}

/// A schema + batch of `rows` rows, single `id: long` column (ids `0..rows`) — the
/// seed payload for `land` with `inline_byte_limit: 0` (forces real Parquet), so
/// the consuming overwrite has a live data file to end-cap alongside the inline
/// rows.
fn ipc_body(rows: i64) -> (Arc<Schema>, Vec<RecordBatch>) {
    let b = inline_batch(&(0..rows).collect::<Vec<_>>());
    (b.schema(), vec![b])
}

/// `end_snapshot` of the single physical inline row `loom_row_id = row_id` in
/// `iceberg_mirror.inline_<tid>` — `None` means still live.
async fn inline_end_snapshot(pool: &sqlx::PgPool, tid: i64, row_id: i64) -> Option<i64> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select end_snapshot from iceberg_mirror.inline_{tid} where loom_row_id = {row_id}"
    )))
    .fetch_one(pool)
    .await
    .expect("end_snapshot")
}

/// Count inline rows live at a given snapshot for the given table id.
async fn live_inline_count(
    pool: &sqlx::PgPool,
    tid: i64,
    at: control_plane_core::SnapshotId,
) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select count(*) from iceberg_mirror.inline_{tid} \
         where begin_snapshot <= {s} and (end_snapshot is null or end_snapshot > {s})",
        s = at.0,
    )))
    .fetch_one(pool)
    .await
    .expect("live inline count")
}

/// Case 1 + 2 (brief): a targeted cap retires exactly the named row and leaves the
/// other live inline row alone; the survivor still serves via `inline_live_batch`.
/// Also checks the file tier: the seed Parquet file is end-capped, the replacement
/// is the sole live file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consuming_overwrite_retires_only_named_rows() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());

    let t = TableRef {
        schema: "wh".into(),
        name: "consuming".into(),
    };

    // Seed a live data file via a real Parquet land (limit 0 forces Parquet, not inline).
    let (schema, batches) = ipc_body(1);
    let s0 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(),
        None,
    )
    .await
    .expect("seed file row");

    // Two live inline rows on top of the seeded file.
    let _s1 = inline_append(
        &pool,
        &t,
        &columns(),
        &inline_batch(&[10]),
        lineage(),
        None,
        None,
    )
    .await
    .expect("inline append id=10");
    let s2 = inline_append(
        &pool,
        &t,
        &columns(),
        &inline_batch(&[20]),
        lineage(),
        None,
        None,
    )
    .await
    .expect("inline append id=20");

    let (tid, row_ids, _batch) = ice
        .inline_live_batch(&t, s2)
        .await
        .expect("inline_live_batch at s2")
        .expect("two live inline rows before the consuming overwrite");
    assert_eq!(
        row_ids.len(),
        2,
        "two live inline rows before consuming overwrite"
    );
    let (first_id, second_id) = (row_ids[0], row_ids[1]);

    // Fold: a consuming overwrite that consumed only `first_id`, replacing the file
    // tier with one folded row.
    let cap = InlineEndCap {
        table_id: tid,
        row_ids: &[first_id],
    };
    let s3 = overwrite_parquet_snapshot_consuming(
        &pool,
        &catalog,
        &t,
        &columns(),
        vec![inline_batch(&[99])],
        None,
        cap,
    )
    .await
    .expect("consuming overwrite");
    assert!(s3.0 > s2.0, "consuming overwrite advances the snapshot");

    // The named row is retired at the new snapshot; the other survives untouched.
    assert_eq!(
        inline_end_snapshot(&pool, tid, first_id).await,
        Some(s3.0),
        "named row end-capped at the consuming overwrite's snapshot"
    );
    assert_eq!(
        inline_end_snapshot(&pool, tid, second_id).await,
        None,
        "un-named row must survive a targeted consuming overwrite"
    );

    // File tier: the seed file was live just before the overwrite, and is end-capped
    // by it; the replacement is the sole live file afterward.
    let files_before_overwrite = ice.files_with_stats(&t, s2).await.expect("files at s2");
    assert_eq!(
        files_before_overwrite.len(),
        1,
        "seed file live before the overwrite"
    );
    let files_now = ice.files_with_stats(&t, s3).await.expect("files at s3");
    assert_eq!(files_now.len(), 1, "replacement file is the sole live file");
    assert_ne!(
        files_now[0].path, files_before_overwrite[0].path,
        "replacement is a distinct data file from the seed"
    );
    // Time travel to s0 still resolves the original seed file.
    let files_at_s0 = ice.files_with_stats(&t, s0).await.expect("files at s0");
    assert_eq!(files_at_s0.len(), 1, "seed file still time-travels to s0");

    // Case 2: the survivor still serves — `inline_live_batch` at the new snapshot
    // returns only the surviving row.
    let (tid2, live_ids, live_batch) = ice
        .inline_live_batch(&t, s3)
        .await
        .expect("inline_live_batch at s3")
        .expect("survivor still live at s3");
    assert_eq!(tid2, tid);
    assert_eq!(
        live_ids,
        vec![second_id],
        "only the survivor's loom_row_id is live"
    );
    let ids = live_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column");
    assert_eq!(ids.value(0), 20, "surviving row is id=20");
}

/// Case 3 (brief): the zero-file (truncate) branch of the consuming overwrite
/// retires exactly the named inline rows (both, here) and end-caps all live data
/// files, asserted by id rather than by a blanket "count went to zero" check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consuming_overwrite_truncate_retires_named_rows() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());

    let t = TableRef {
        schema: "wh".into(),
        name: "consuming_trunc".into(),
    };

    let (schema, batches) = ipc_body(1);
    let _s0 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(),
        None,
    )
    .await
    .expect("seed file row");

    let _s1 = inline_append(
        &pool,
        &t,
        &columns(),
        &inline_batch(&[10]),
        lineage(),
        None,
        None,
    )
    .await
    .expect("inline append id=10");
    let s2 = inline_append(
        &pool,
        &t,
        &columns(),
        &inline_batch(&[20]),
        lineage(),
        None,
        None,
    )
    .await
    .expect("inline append id=20");

    let (tid, row_ids, _batch) = ice
        .inline_live_batch(&t, s2)
        .await
        .expect("inline_live_batch at s2")
        .expect("two live inline rows before the truncating consuming overwrite");
    assert_eq!(row_ids.len(), 2, "two live inline rows before truncate");

    // Seed file is live right before the truncate.
    let files_before = ice.files_with_stats(&t, s2).await.expect("files at s2");
    assert_eq!(files_before.len(), 1, "seed file live before truncate");

    let cap = InlineEndCap {
        table_id: tid,
        row_ids: &row_ids,
    };
    let s3 =
        overwrite_parquet_snapshot_consuming(&pool, &catalog, &t, &columns(), vec![], None, cap)
            .await
            .expect("consuming truncate overwrite");
    assert!(s3.0 > s2.0, "truncate advances the snapshot");

    // Both named rows are retired at the truncate snapshot — assert by id, not count.
    for row_id in &row_ids {
        assert_eq!(
            inline_end_snapshot(&pool, tid, *row_id).await,
            Some(s3.0),
            "row {row_id} end-capped at the truncate snapshot"
        );
    }
    assert_eq!(
        live_inline_count(&pool, tid, s3).await,
        0,
        "no inline rows live after the truncating consuming overwrite"
    );

    // All files end-capped: nothing live at s3.
    let files_now = ice.files_with_stats(&t, s3).await.expect("files at s3");
    assert!(files_now.is_empty(), "truncate leaves no live data files");
}

/// Case 4 (brief): a plain `overwrite_parquet_snapshot` (no cap) is byte-identical
/// to before — it still blanket-caps every live inline row. Contrast case for the
/// targeted-cap behavior above.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_overwrite_still_blanket_caps() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let t = TableRef {
        schema: "wh".into(),
        name: "consuming_plain".into(),
    };

    let s1 = inline_append(
        &pool,
        &t,
        &columns(),
        &inline_batch(&[1]),
        lineage(),
        None,
        None,
    )
    .await
    .expect("inline append id=1");

    let tid: i64 = sqlx::query_scalar(
        "select table_id from iceberg_mirror.table \
         where table_namespace='wh' and table_name='consuming_plain' and end_snapshot is null",
    )
    .fetch_one(&pool)
    .await
    .expect("tid");

    assert_eq!(
        live_inline_count(&pool, tid, s1).await,
        1,
        "one inline row live at s1"
    );

    let s2 = overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &t,
        &columns(),
        vec![inline_batch(&[2])],
        None,
        &[],
    )
    .await
    .expect("plain overwrite");
    assert!(s2.0 > s1.0, "overwrite advances the snapshot");

    assert_eq!(
        live_inline_count(&pool, tid, s2).await,
        0,
        "plain overwrite still blanket-caps every live inline row"
    );
}
