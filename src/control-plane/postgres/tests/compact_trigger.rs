//! Event-driven compaction auto-trigger (`iceberg_compact::maybe_enqueue_compact`,
//! wired at the tail of `SqlCatalog::write_mirror`): landing small files past a
//! configured threshold enqueues exactly one deduped `compact_table` job, with no
//! operator POST involved. Mirrors `tests/inline_flush_trigger.rs`'s trigger shape
//! and `worker/tests/compact_e2e.rs`'s land-small-Parquet-files setup.
//! loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    COMPACT_JOB_KIND, CompactJob, DataFile, DatasetId, EventType, FileFormat, LineageEvent, RunId,
    StreamTables, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_compact::{CompactTriggerCfg, compact_table};
use control_plane_postgres::iceberg_inline::set_has_shadow;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_mirror::live_table_id;
use loom_test_seed::local_sql_catalog;

fn columns() -> Vec<control_plane_core::ColumnSpec> {
    vec![control_plane_core::ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// A schema + single-row batch (`id: long`) — `land` now takes pre-decoded
/// batches, so build these directly rather than round-tripping through an
/// Arrow IPC encode/decode. One land call per file with `inline_byte_limit: 0`
/// forces the Parquet branch, so each land emits one small file.
fn ipc_body(id: i64) -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![id]))])
        .expect("batch");
    (schema, vec![batch])
}

fn lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "compact-trigger-test" }),
    }
}

fn small_limits() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: 0,           // always write real Parquet
        flush_byte_threshold: i64::MAX, // no flush auto-enqueue
    }
}

async fn available_jobs(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "select count(*) from queue.jobs where kind = $1 and state = 'available'",
    )
    .bind(COMPACT_JOB_KIND)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Land `n` one-row Parquet files against `table` through `catalog`.
async fn land_n_small(
    pool: &sqlx::PgPool,
    catalog: &control_plane_postgres::iceberg_sql_catalog::SqlCatalog,
    table: &TableRef,
    n: i64,
) {
    for id in 0..n {
        let (schema, batches) = ipc_body(id);
        land(
            pool,
            catalog,
            table,
            &columns(),
            schema,
            batches,
            small_limits(),
            lineage(RunId(uuid::Uuid::new_v4()), table),
            None,
        )
        .await
        .expect("land");
    }
}

fn trigger_cfg() -> CompactTriggerCfg {
    CompactTriggerCfg {
        small_file_bytes: 1 << 20, // 1 MiB — the test's tiny files all qualify
        min_small_files: 3,        // N = 3 keeps the test fast
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn below_threshold_enqueues_nothing() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string())
        .await
        .with_compact_trigger(trigger_cfg());
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    land_n_small(&pool, &catalog, &table, 2).await;
    assert_eq!(available_jobs(&pool).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crossing_enqueues_exactly_one() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string())
        .await
        .with_compact_trigger(trigger_cfg());
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    land_n_small(&pool, &catalog, &table, 3).await;
    assert_eq!(available_jobs(&pool).await, 1);

    let payload: serde_json::Value = sqlx::query_scalar(
        "select payload from queue.jobs where kind = $1 and state = 'available'",
    )
    .bind(COMPACT_JOB_KIND)
    .fetch_one(&pool)
    .await
    .unwrap();
    let want = serde_json::to_value(CompactJob {
        schema: table.schema.clone(),
        name: table.name.clone(),
    })
    .unwrap();
    assert_eq!(
        payload, want,
        "dedup-coherent with the operator endpoint's payload shape"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_holds_past_threshold() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string())
        .await
        .with_compact_trigger(trigger_cfg());
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    land_n_small(&pool, &catalog, &table, 5).await;
    assert_eq!(available_jobs(&pool).await, 1, "debounced");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_files_do_not_count() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string())
        .await
        .with_compact_trigger(CompactTriggerCfg {
            small_file_bytes: 1, // nothing qualifies
            min_small_files: 3,
        });
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    land_n_small(&pool, &catalog, &table, 5).await;
    assert_eq!(available_jobs(&pool).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_commit_does_not_retrigger() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string())
        .await
        .with_compact_trigger(trigger_cfg());
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    land_n_small(&pool, &catalog, &table, 3).await;
    assert_eq!(
        available_jobs(&pool).await,
        1,
        "job present before compaction"
    );

    // Expire the smalls via a real compaction commit, registering one coalesced
    // (large) replacement file — mirrors compact_e2e's DataFile construction.
    let small_paths: Vec<String> = sqlx::query_scalar(
        "select path from iceberg_mirror.data_file d \
         join iceberg_mirror.\"table\" t on t.table_id = d.table_id \
         where t.table_namespace = $1 and t.table_name = $2 \
           and d.end_snapshot is null",
    )
    .bind(&table.schema)
    .bind(&table.name)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        small_paths.len(),
        3,
        "three live small files before compaction"
    );

    let coalesced = vec![DataFile {
        path: format!("{}/wh/t/compact-1/part-0.parquet", wh.path().display()),
        path_is_relative: false,
        file_format: FileFormat::Parquet,
        record_count: 3,
        file_size_bytes: 3, // deliberately small — proves it's the CODE PATH, not size, that guards
        column_stats: vec![],
        parquet_footer_size: None,
    }];
    compact_table(&pool, &table, &small_paths, &coalesced)
        .await
        .expect("compact")
        .expect("snapshot");

    assert_eq!(
        available_jobs(&pool).await,
        1,
        "compaction's own commit does not re-trigger"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_catalog_enqueues_nothing() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    // No with_compact_trigger call: the default-off regression pin.
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    land_n_small(&pool, &catalog, &table, 5).await;
    assert_eq!(available_jobs(&pool).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_table_skips() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string())
        .await
        .with_compact_trigger(trigger_cfg());
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    // Land one file first so the table has a live mirror row, declare it a
    // stream table, then cross the threshold.
    land_n_small(&pool, &catalog, &table, 1).await;
    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("live_table_id")
        .expect("table has a live row");
    drop(conn);
    cp.declare_stream(tid, 4).await.expect("declare_stream");

    land_n_small(&pool, &catalog, &table, 2).await;
    assert_eq!(
        available_jobs(&pool).await,
        0,
        "declared stream tables skip"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shadow_flagged_table_skips() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string())
        .await
        .with_compact_trigger(trigger_cfg());
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    land_n_small(&pool, &catalog, &table, 1).await;
    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("live_table_id")
        .expect("table has a live row");
    set_has_shadow(&mut conn, tid)
        .await
        .expect("set_has_shadow");
    drop(conn);

    land_n_small(&pool, &catalog, &table, 2).await;
    assert_eq!(available_jobs(&pool).await, 0, "shadow-flagged tables skip");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changelog_table_skips() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string())
        .await
        .with_compact_trigger(trigger_cfg());
    let pool = fx.pool_for(&db).await;
    let base = TableRef {
        schema: "wh".into(),
        name: "base".into(),
    };
    let changelog = TableRef {
        schema: "wh".into(),
        name: "base__changelog".into(),
    };

    // Two independent tables: `changelog` accrues the small files; `base`'s
    // stream_table row points its changelog_table_id at `changelog`'s tid.
    land_n_small(&pool, &catalog, &changelog, 1).await;
    land_n_small(&pool, &catalog, &base, 1).await;
    let mut conn = pool.acquire().await.expect("acquire");
    let changelog_tid = live_table_id(&mut conn, &changelog.schema, &changelog.name)
        .await
        .expect("live_table_id")
        .expect("changelog has a live row");
    let base_tid = live_table_id(&mut conn, &base.schema, &base.name)
        .await
        .expect("live_table_id")
        .expect("base has a live row");
    drop(conn);
    // `set_changelog_table_id` UPDATEs an existing stream.stream_table row, so
    // `base` must first be declared CDC (mirrors stream_cdc_dual_flush.rs) —
    // otherwise the update is a silent no-op against a non-existent row.
    cp.declare_cdc(base_tid, 1, "id", control_plane_core::MergeEngine::LastRow)
        .await
        .expect("declare_cdc");
    cp.set_changelog_table_id(base_tid, changelog_tid)
        .await
        .expect("set_changelog_table_id");

    land_n_small(&pool, &catalog, &changelog, 2).await;
    assert_eq!(
        available_jobs(&pool).await,
        0,
        "a changelog_table_id target skips"
    );
}
