//! Fixture tests for the orphaned-object sweep (`orphan_sweep::sweep_orphans`):
//! planted orphans older than grace are deleted; every mirror-referenced object
//! (live, historical-in-window, dropped-in-window, puffin) survives; Iceberg
//! metadata is out of scope and untouchable; young orphans are held by grace; a
//! second immediate sweep is a clean no-op.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, DatasetId, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{InlineLimits, land, overwrite_parquet_snapshot};
use control_plane_postgres::orphan_sweep::{SweepSummary, sweep_orphans};
use control_plane_postgres::vector_index::{VectorIndexRow, insert_vector_index};
use iceberg::{Catalog as _, NamespaceIdent, TableIdent};
use loom_test_seed::local_sql_catalog;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use time::OffsetDateTime;

const ZERO_GRACE: Duration = Duration::ZERO;

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

fn batch(rows: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .expect("batch")
}

fn ipc_body(rows: i64) -> (Arc<Schema>, Vec<RecordBatch>) {
    let b = batch(rows);
    (b.schema(), vec![b])
}

fn lineage(run: RunId, schema: &str, name: &str) -> LineageEvent {
    let out = TableRef {
        schema: schema.into(),
        name: name.into(),
    };
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&out).dataset_ref()],
        payload: serde_json::json!({ "source": "orphan-sweep-test" }),
    }
}

fn small_limits() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: 0,
        flush_byte_threshold: i64::MAX,
    }
}

/// A writable local object store rooted at `wh`, plus the matching `root_url`
/// prefix — the same shape `store_config::build_write_store` produces for a
/// `file://` warehouse, but without pulling the services-layer crate into a
/// control-plane test.
fn store_for(wh: &std::path::Path) -> (Arc<dyn ObjectStore>, String) {
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wh).expect("local store"));
    (store, format!("file://{}", wh.display()))
}

/// Strip a `file://` URL to a local filesystem path.
fn local_path(file_url: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(file_url.strip_prefix("file://").unwrap_or(file_url))
}

/// Planted orphans older than grace are deleted; the referenced data file survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_deletes_orphans_keeps_referenced() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef {
        schema: "wh".into(),
        name: "kept".into(),
    };

    let (schema, batches) = ipc_body(4);
    let s1 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        small_limits(),
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "kept"),
        None,
    )
    .await
    .expect("land");
    let kept = local_path(&ice.files_with_stats(&t, s1).await.expect("files")[0].path);
    assert!(kept.exists(), "referenced file present before sweep");

    // Two planted orphans (a stray parquet + a stray puffin), referenced by no row.
    let orphan_parquet = wh.path().join("orphan-abc.parquet");
    let orphan_puffin = wh.path().join("orphan-abc.puffin");
    std::fs::write(&orphan_parquet, b"orphan-parquet").expect("write orphan parquet");
    std::fs::write(&orphan_puffin, b"orphan-puffin").expect("write orphan puffin");

    let (store, root_url) = store_for(wh.path());
    let summary = sweep_orphans(&store, &root_url, &pool, ZERO_GRACE)
        .await
        .expect("sweep");

    assert!(!orphan_parquet.exists(), "orphan parquet deleted");
    assert!(!orphan_puffin.exists(), "orphan puffin deleted");
    assert!(kept.exists(), "referenced data file survived");
    assert_eq!(
        summary.objects_deleted, 2,
        "both orphans deleted: {summary:?}"
    );
    assert_eq!(summary.candidates_skipped_grace, 0);
}

/// A young orphan (freshly written) is HELD when grace exceeds its age.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_holds_young_orphan_under_grace() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let _catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let orphan = wh.path().join("young.parquet");
    std::fs::write(&orphan, b"young").expect("write");

    let (store, root_url) = store_for(wh.path());
    let summary = sweep_orphans(&store, &root_url, &pool, Duration::from_secs(3600))
        .await
        .expect("sweep");

    assert!(orphan.exists(), "young orphan held by the 1h grace window");
    assert_eq!(summary.objects_deleted, 0);
    assert_eq!(
        summary.candidates_skipped_grace, 1,
        "held young orphan counted: {summary:?}"
    );
}

/// Iceberg metadata/manifest objects are out of scope: never candidates, even
/// when unreferenced. A control orphan parquet in the same dir IS deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_never_touches_metadata() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let _catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let meta_json = wh.path().join("v3.metadata.json");
    let manifest = wh.path().join("snap-42.avro");
    let control = wh.path().join("stray.parquet");
    std::fs::write(&meta_json, b"{}").expect("meta");
    std::fs::write(&manifest, b"avro").expect("manifest");
    std::fs::write(&control, b"parquet").expect("control");

    let (store, root_url) = store_for(wh.path());
    let summary = sweep_orphans(&store, &root_url, &pool, ZERO_GRACE)
        .await
        .expect("sweep");

    assert!(meta_json.exists(), "metadata JSON out of scope, untouched");
    assert!(manifest.exists(), "manifest .avro out of scope, untouched");
    assert!(!control.exists(), "the in-scope orphan parquet was deleted");
    assert_eq!(summary.objects_deleted, 1, "only the parquet: {summary:?}");
}

/// A historical, end-capped-but-in-window data file (its `data_file` row still
/// present with `end_snapshot` set — not yet GC'd) is still referenced, so it
/// survives the sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_keeps_historical_in_window_file() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef {
        schema: "wh".into(),
        name: "hist".into(),
    };

    let (schema, batches) = ipc_body(4);
    let s1 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        small_limits(),
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "hist"),
        None,
    )
    .await
    .expect("land");
    let a_path = local_path(&ice.files_with_stats(&t, s1).await.expect("files@s1")[0].path);

    // Overwrite end-caps file A@s2, but A's data_file row (end_snapshot set) remains.
    overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &t,
        &columns(),
        vec![batch(2)],
        Some(&lineage(RunId(uuid::Uuid::new_v4()), "wh", "hist")),
        &[],
    )
    .await
    .expect("overwrite");
    assert!(a_path.exists(), "end-capped file A on disk before sweep");

    let (store, root_url) = store_for(wh.path());
    let summary = sweep_orphans(&store, &root_url, &pool, ZERO_GRACE)
        .await
        .expect("sweep");

    assert!(
        a_path.exists(),
        "historical in-window file survives (still referenced)"
    );
    assert_eq!(
        summary.objects_deleted, 0,
        "nothing unreferenced: {summary:?}"
    );
}

/// A dropped-but-in-window table's data file is still referenced (its `data_file`
/// rows survive until `gc_table` ages out the drop snapshot), so the sweep keeps it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_keeps_dropped_incarnation_file() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef {
        schema: "wh".into(),
        name: "dropped".into(),
    };

    let (schema, batches) = ipc_body(4);
    let s1 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        small_limits(),
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "dropped"),
        None,
    )
    .await
    .expect("land");
    let path = local_path(&ice.files_with_stats(&t, s1).await.expect("files")[0].path);

    let ident = TableIdent::new(NamespaceIdent::new("wh".into()), "dropped".into());
    catalog.drop_table(&ident).await.expect("drop");
    assert!(path.exists(), "dropped table's file on disk before sweep");

    let (store, root_url) = store_for(wh.path());
    let summary = sweep_orphans(&store, &root_url, &pool, ZERO_GRACE)
        .await
        .expect("sweep");

    assert!(
        path.exists(),
        "dropped-in-window file survives (data_file row still present)"
    );
    assert_eq!(
        summary.objects_deleted, 0,
        "nothing unreferenced: {summary:?}"
    );
}

/// A puffin sidecar referenced by a `vector_index` row survives; a stray puffin dies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_keeps_referenced_puffin() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef {
        schema: "wh".into(),
        name: "indexed".into(),
    };

    let (schema, batches) = ipc_body(4);
    let s1 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        small_limits(),
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "indexed"),
        None,
    )
    .await
    .expect("land");
    let tid: i64 = sqlx::query_scalar::<_, i64>(
        "select table_id from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
    )
    .bind("wh")
    .bind("indexed")
    .fetch_one(&pool)
    .await
    .expect("tid");

    let referenced = wh.path().join("indexed.idx.puffin");
    std::fs::write(&referenced, b"puffin").expect("write referenced puffin");
    let mut conn = pool.acquire().await.expect("conn");
    insert_vector_index(
        &mut conn,
        &VectorIndexRow {
            table_id: tid,
            column: "id".into(),
            index_name: "idx".into(),
            covered_snapshot: s1.0,
            metric: "l2".into(),
            index_kind: "flat".into(),
            dim: 4,
            row_count: 4,
            puffin_path: format!("file://{}", referenced.display()),
        },
    )
    .await
    .expect("insert vector_index");
    drop(conn);

    let stray = wh.path().join("stray.puffin");
    std::fs::write(&stray, b"stray").expect("write stray");

    let (store, root_url) = store_for(wh.path());
    let summary = sweep_orphans(&store, &root_url, &pool, ZERO_GRACE)
        .await
        .expect("sweep");

    assert!(referenced.exists(), "referenced puffin survives");
    assert!(!stray.exists(), "stray puffin deleted");
    assert_eq!(
        summary.objects_deleted, 1,
        "only the stray puffin: {summary:?}"
    );
}

/// A second immediate sweep deletes nothing and errors nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_is_idempotent() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let _catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let orphan = wh.path().join("once.parquet");
    std::fs::write(&orphan, b"once").expect("write");

    let (store, root_url) = store_for(wh.path());
    let first = sweep_orphans(&store, &root_url, &pool, ZERO_GRACE)
        .await
        .expect("first");
    assert_eq!(first.objects_deleted, 1);

    let second = sweep_orphans(&store, &root_url, &pool, ZERO_GRACE)
        .await
        .expect("second");
    assert_eq!(
        second,
        SweepSummary::default(),
        "second sweep is a clean no-op: {second:?}"
    );
}
