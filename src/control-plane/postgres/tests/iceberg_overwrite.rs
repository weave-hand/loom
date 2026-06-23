//! Fixture tests for `overwrite_parquet_snapshot` — the Iceberg twin of DuckLake's
//! `Tx::replace_files`. The replacement files become the table's sole live set while
//! prior files stay reachable by time travel (mirror end-cap at the new snapshot).
//! Mirrors the DuckLake contract test `snapshot_replace.rs`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc57::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::Catalog;
use control_plane_core::{
    ColumnSpec, DatasetId, EventType, Lineage, LineageEvent, PageReq, RunId, StatValue, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{land, overwrite_parquet_snapshot};
use control_plane_postgres::iceberg_mirror::{
    end_cap_live_data_files, ensure_table, next_snapshot,
};
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// An Arrow-57 IPC body of `rows` rows, single `id: long` column (ids `0..rows`) —
/// used to seed the initial append via `land` (limit 0 forces real Parquet).
fn ipc_body(rows: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

/// A bare `id: long` record batch of ids `0..rows` — the replacement payload passed
/// to `overwrite_parquet_snapshot` (re-wrapped under the table's field-id schema by
/// the landing Parquet path, so only column order/type matter, not field metadata).
fn batch(rows: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .expect("batch")
}

fn lineage(run: RunId, schema: &str, name: &str) -> LineageEvent {
    let out = TableRef {
        schema: schema.into(),
        name: name.into(),
    };
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&out).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{warehouse}"),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

/// Append `a` (10 rows) at `s1`, overwrite with `b` (4 rows) at `s2`: at the current
/// snapshot only `b` is live; the prior snapshot still time-travels to `a`. The
/// Iceberg twin of `snapshot_replace.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_expires_old_and_preserves_time_travel() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());

    let t = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    // append a.parquet (10 rows) -> s1 (limit 0 forces real Parquet).
    let s1 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        &ipc_body(10),
        0,
        i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
    )
    .await
    .expect("append");

    // overwrite with b.parquet (4 rows) -> s2.
    let s2 = overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &t,
        &columns(),
        vec![batch(4)],
        Some(&lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")),
    )
    .await
    .expect("overwrite");
    assert!(s2.0 > s1.0, "overwrite advances the snapshot");

    // current snapshot: only the replacement, 4 rows.
    assert_eq!(ice.current_snapshot(&t).await.expect("current").id, s2);
    let now = ice.files_with_stats(&t, s2).await.expect("files at s2");
    assert_eq!(now.len(), 1, "only the replacement is live");
    assert_eq!(now[0].record_count, 4);

    // prior snapshot: the original file, 10 rows (time travel via end_snapshot).
    let before = ice.files_with_stats(&t, s1).await.expect("files at s1");
    assert_eq!(before.len(), 1, "prior snapshot retains the original file");
    assert_eq!(before[0].record_count, 10);
    assert_ne!(now[0].path, before[0].path, "distinct data files");
}

/// The replaced file carries its per-column footer stats, so the serving pruner
/// operates on replaced data immediately (no backfill).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replaced_files_carry_per_column_stats() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());

    let t = TableRef {
        schema: "wh".into(),
        name: "stats".into(),
    };

    land(
        &pool,
        &catalog,
        &t,
        &columns(),
        &ipc_body(10),
        0,
        i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "stats"),
    )
    .await
    .expect("append");

    // ids 0..4 -> min 0, max 3.
    let s2 = overwrite_parquet_snapshot(&pool, &catalog, &t, &columns(), vec![batch(4)], None)
        .await
        .expect("overwrite");

    let now = ice.files_with_stats(&t, s2).await.expect("files at s2");
    assert_eq!(now.len(), 1);
    let id_stat = now[0]
        .column_stats
        .iter()
        .find(|s| s.column_name == "id")
        .expect("id column stat present on replaced file");
    assert_eq!(id_stat.min, Some(StatValue::I64(0)));
    assert_eq!(id_stat.max, Some(StatValue::I64(3)));
}

/// Truncation: overwrite with zero files end-caps all live files; the current
/// snapshot lists none; the prior snapshot still time-travels to the original.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncate_overwrite_with_zero_files() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());

    let t = TableRef {
        schema: "wh".into(),
        name: "trunc".into(),
    };

    let s1 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        &ipc_body(10),
        0,
        i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "trunc"),
    )
    .await
    .expect("append");

    // zero new files -> truncation.
    let s2 = overwrite_parquet_snapshot(&pool, &catalog, &t, &columns(), vec![], None)
        .await
        .expect("truncate");
    assert!(s2.0 > s1.0, "truncate advances the snapshot");

    assert_eq!(ice.current_snapshot(&t).await.expect("current").id, s2);
    let now = ice.files_with_stats(&t, s2).await.expect("files at s2");
    assert!(now.is_empty(), "truncate leaves no live files at current");

    let before = ice.files_with_stats(&t, s1).await.expect("files at s1");
    assert_eq!(before.len(), 1, "prior snapshot still time-travels");
    assert_eq!(before[0].record_count, 10);
}

/// The overwrite emits the caller-provided lineage event atomically, readable via
/// the lineage read (same shape the append path emits).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_emits_lineage() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let t = TableRef {
        schema: "wh".into(),
        name: "lin".into(),
    };

    land(
        &pool,
        &catalog,
        &t,
        &columns(),
        &ipc_body(10),
        0,
        i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "lin"),
    )
    .await
    .expect("append");

    let run = RunId(uuid::Uuid::new_v4());
    overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &t,
        &columns(),
        vec![batch(4)],
        Some(&lineage(run, "wh", "lin")),
    )
    .await
    .expect("overwrite");

    let page = cp
        .events_for(&run, PageReq::unbounded())
        .await
        .expect("events");
    assert_eq!(
        page.items.len(),
        1,
        "one lineage event for the overwrite run"
    );
    assert_eq!(page.items[0].outputs[0].name, "wh.lin");
}

/// Atomicity: the overwrite's mirror mutations (allocate snapshot + end-cap live
/// files) are one Postgres transaction. A failure before commit must leave the prior
/// live set intact — no orphaned end-cap, no advanced snapshot. There is no way to
/// inject a post-end-cap failure through the typed `overwrite_parquet_snapshot` API
/// (a valid `LineageEvent` never violates a constraint, and `lineage.event_type` is
/// plain `text`), so this exercises the same mirror sequence `write_mirror` runs
/// (`next_snapshot` -> `ensure_table` -> `end_cap_live_data_files`) inside a tx that is
/// rolled back, proving the end-cap is transactional.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_atomicity_leaves_prior_set_intact() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());

    let t = TableRef {
        schema: "wh".into(),
        name: "atomic".into(),
    };

    let s1 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        &ipc_body(10),
        0,
        i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "atomic"),
    )
    .await
    .expect("append");

    // Replicate the overwrite's mirror mutations, then roll back without committing.
    {
        let mut tx = pool.begin().await.expect("begin");
        let conn = &mut *tx;
        let at = next_snapshot(conn, None).await.expect("snapshot");
        let tid = ensure_table(conn, "wh", "atomic", at).await.expect("table");
        end_cap_live_data_files(conn, tid, at)
            .await
            .expect("end-cap");
        tx.rollback().await.expect("rollback");
    }

    // The original live set is untouched: file still live at the unchanged current.
    assert_eq!(
        ice.current_snapshot(&t).await.expect("current").id,
        s1,
        "rolled-back snapshot is not visible"
    );
    let live = ice.files_with_stats(&t, s1).await.expect("files at s1");
    assert_eq!(live.len(), 1, "the original file is still live");
    assert_eq!(live[0].record_count, 10);
}
