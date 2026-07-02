//! Iceberg subset-expire compaction commit (`iceberg_compact::compact_table`): expire
//! a subset of live files + register coalesced files at one snapshot, preserving time
//! travel; conflict on a non-live expire path; NotFound -> None.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlaneError, DataFile, DatasetId, EventType, FileFormat,
    LineageEvent, RunId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_compact::compact_table;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
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

/// An Arrow IPC body of `rows` rows, single `id: long` column (ids `0..rows`) —
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

fn syn_file(path: &str, rows: i64) -> DataFile {
    DataFile {
        path: path.into(),
        path_is_relative: false,
        file_format: FileFormat::Parquet,
        record_count: rows,
        file_size_bytes: rows * 16,
        column_stats: vec![],
        parquet_footer_size: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_expires_subset_and_preserves_time_travel() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    // Land three real files (a:10, b:5, c:2) so the mirror has three live rows.
    let _s1 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        &ipc_body(10),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
    )
    .await
    .expect("a");
    land(
        &pool,
        &catalog,
        &t,
        &columns(),
        &ipc_body(5),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
    )
    .await
    .expect("b");
    let before = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        &ipc_body(2),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
    )
    .await
    .expect("c");

    let live = ice.files_with_stats(&t, before).await.expect("live");
    assert_eq!(live.len(), 3);
    // Expire the two smallest (b,c by record_count); coalesce into one synthetic file.
    let mut by_rows = live.clone();
    by_rows.sort_by_key(|f| f.record_count);
    let expire: Vec<String> = by_rows[..2].iter().map(|f| f.path.clone()).collect();
    let coalesced_rows: i64 = by_rows[..2].iter().map(|f| f.record_count).sum();
    let new = vec![syn_file(
        &format!("{}/wh/t/compact-1/part-0.parquet", wh.path().display()),
        coalesced_rows,
    )];

    let snap = compact_table(&pool, &t, &expire, &new)
        .await
        .expect("compact")
        .expect("snapshot");
    assert!(snap.0 > before.0, "compaction advances the snapshot");

    // Current: the untouched large file + the coalesced file; row total preserved.
    let now = ice.files_with_stats(&t, snap).await.expect("now");
    assert_eq!(now.len(), 2, "one untouched + one coalesced");
    let total: i64 = now.iter().map(|f| f.record_count).sum();
    assert_eq!(total, 17, "10 + (5+2) preserved");

    // Time travel: the prior snapshot still lists all three originals.
    let back = ice.files_with_stats(&t, before).await.expect("back");
    assert_eq!(back.len(), 3, "prior snapshot retains the three originals");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_conflicts_on_non_live_expire_path() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };
    land(
        &pool,
        &catalog,
        &t,
        &columns(),
        &ipc_body(3),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
    )
    .await
    .expect("a");

    let err = compact_table(
        &pool,
        &t,
        &["does/not/exist.parquet".into()],
        &[syn_file(&format!("{}/x.parquet", wh.path().display()), 3)],
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, ControlPlaneError::Conflict(_)),
        "non-live expire path conflicts, got {err:?}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_missing_table_is_none() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef {
        schema: "wh".into(),
        name: "ghost".into(),
    };
    let out = compact_table(&pool, &t, &[], &[]).await.expect("ok");
    assert!(
        out.is_none(),
        "no snapshot for a table that was never written"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_compactions_race_exactly_one_commits() {
    // The spec's real conflict case: two compactions target the SAME live small-file
    // set; exactly one commits, the other gets Conflict (then a re-run converges).
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    land(
        &pool,
        &catalog,
        &t,
        &columns(),
        &ipc_body(3),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
    )
    .await
    .expect("a");
    let before = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        &ipc_body(2),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
    )
    .await
    .expect("b");
    let live = ice.files_with_stats(&t, before).await.expect("live");
    let expire: Vec<String> = live.iter().map(|f| f.path.clone()).collect();
    let total: i64 = live.iter().map(|f| f.record_count).sum();

    // Two compactions of the SAME expire set, each adding a distinct coalesced file.
    let mk = |p: &str| {
        vec![syn_file(
            &format!("{}/wh/t/{p}/part-0.parquet", wh.path().display()),
            total,
        )]
    };
    let (p1, p2) = (pool.clone(), pool.clone());
    let (e1, e2) = (expire.clone(), expire.clone());
    let (t1, t2) = (t.clone(), t.clone());
    let (f1, f2) = (mk("c1"), mk("c2"));
    let (r1, r2) = tokio::join!(
        async move { compact_table(&p1, &t1, &e1, &f1).await },
        async move { compact_table(&p2, &t2, &e2, &f2).await },
    );
    let oks = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, Ok(Some(_))))
        .count();
    let conflicts = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, Err(ControlPlaneError::Conflict(_))))
        .count();
    assert_eq!(oks, 1, "exactly one compaction commits: {r1:?} / {r2:?}");
    assert_eq!(conflicts, 1, "the loser gets Conflict: {r1:?} / {r2:?}");

    // The winner's coalesced file is the sole live file; row set preserved.
    let head = ice.current_snapshot(&t).await.expect("head").id;
    let now = ice.files_with_stats(&t, head).await.expect("now");
    assert_eq!(
        now.len(),
        1,
        "exactly one coalesced file is live after the race"
    );
    assert_eq!(now.iter().map(|f| f.record_count).sum::<i64>(), total);

    // A re-run of the loser now converges to a clean no-op (only one live file left,
    // and its path is not in the stale expire set -> nothing to expire -> Conflict-free
    // path is moot; the worker would compute a fresh small set and no-op). Assert a
    // fresh compaction with the CURRENT live set is a no-op-equivalent single commit.
    // (The worker's <2-small-files no-op is covered in the worker e2e; here we only
    // assert the race left the table consistent.)
}
