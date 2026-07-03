//! Contract tests for `IcebergControlPlane`/`IcebergTx` — the Iceberg-backed `Tx`.
//! `register_files` is mirror-only and never reads the Parquet bytes (it projects the
//! `DataFile`'s carried stats), and `IcebergCatalog` reads resolve through the mirror,
//! so these use synthetic `DataFile`s with precomputed stats.

use loom_test_seed::local_sql_catalog;

use control_plane_core::{
    ColumnSpec, ColumnStat, ControlPlane, DataFile, FileFormat, PageReq, StatValue,
    TableControlPlane, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;

fn cols() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

fn data_file(path: &str, rows: i64) -> DataFile {
    DataFile {
        path: path.into(),
        path_is_relative: true,
        file_format: FileFormat::Parquet,
        record_count: rows,
        file_size_bytes: rows * 16,
        column_stats: vec![ColumnStat {
            column_name: "id".into(),
            null_count: 0,
            column_size_bytes: rows * 8,
            min: Some(StatValue::I64(0)),
            max: Some(StatValue::I64(rows - 1)),
        }],
        parquet_footer_size: Some(120),
    }
}

async fn iceberg_cp(fx: &PgFixture) -> (IcebergControlPlane, tempfile::TempDir) {
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    (IcebergControlPlane::new(pg, catalog), wh)
}

fn t() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "t".into(),
    }
}

/// Append through `IcebergTx`, then overwrite: the same transform op set (`create_table`
/// → append/replace → commit) commits to Iceberg, the replacement becomes the sole live
/// set, and a prior snapshot still time-travels — proving `Tx::replace_files` drives the
/// overwrite primitive through the polymorphic seam.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_then_overwrite_preserves_time_travel() {
    let fx = PgFixture::shared();
    let (cp, _wh) = iceberg_cp(fx).await;
    let t = t();

    // append a.parquet (10 rows).
    let mut tx = cp.begin_table().await.unwrap();
    tx.create_table(&t, &cols()).await.unwrap();
    tx.append_files(&t, &[data_file("a.parquet", 10)])
        .await
        .unwrap();
    let s1 = tx.commit().await.unwrap().expect("append snapshot");

    assert_eq!(cp.catalog().current_snapshot(&t).await.unwrap().id, s1);
    let f1 = cp
        .catalog()
        .files(&t, s1, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(f1.items.len(), 1);
    assert_eq!(f1.items[0].record_count, 10);

    // overwrite with b.parquet (4 rows).
    let mut tx = cp.begin_table().await.unwrap();
    tx.create_table(&t, &cols()).await.unwrap();
    tx.replace_files(&t, &[data_file("b.parquet", 4)])
        .await
        .unwrap();
    let s2 = tx.commit().await.unwrap().expect("overwrite snapshot");
    assert!(s2.0 > s1.0);

    // current: only the replacement.
    let now = cp
        .catalog()
        .files(&t, s2, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(now.items.len(), 1, "only the replacement is live");
    assert_eq!(now.items[0].record_count, 4);

    // prior snapshot: the original file (time travel).
    let before = cp
        .catalog()
        .files(&t, s1, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(before.items.len(), 1, "prior snapshot retains the original");
    assert_eq!(before.items[0].record_count, 10);
}

/// `compact_files` stages the compaction and `commit` applies it via the subset-expire
/// machinery. An empty expire + empty write set commits a no-op snapshot (tables that
/// have no prior mirror rows return None from commit).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_files_stages_without_error() {
    let fx = PgFixture::shared();
    let (cp, _wh) = iceberg_cp(fx).await;
    let mut tx = cp.begin_table().await.unwrap();
    // Staging never fails — even with empty slices.
    tx.compact_files(&t(), &[], &[]).await.unwrap();
}

/// Atomicity: a commit that fails mid-apply (files staged without a matching
/// `create_table`) rolls back the held tx — the allocated snapshot does not persist,
/// so the table has no live state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_commit_leaves_no_snapshot() {
    let fx = PgFixture::shared();
    let (cp, _wh) = iceberg_cp(fx).await;
    let t = t();

    let mut tx = cp.begin_table().await.unwrap();
    // Stage files WITHOUT create_table -> commit allocates a snapshot then errors on
    // the missing columns, and the held tx rolls back.
    tx.append_files(&t, &[data_file("a.parquet", 10)])
        .await
        .unwrap();
    assert!(tx.commit().await.is_err(), "commit must fail");

    // No snapshot/mirror state persisted.
    assert!(
        cp.catalog().current_snapshot(&t).await.is_err(),
        "a rolled-back commit leaves the table with no live snapshot"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_order_contract() {
    let fx = PgFixture::shared();
    let (cp, _wh) = iceberg_cp(fx).await;
    control_plane_testkit::snapshot_write_order_contract(&cp).await;
}
