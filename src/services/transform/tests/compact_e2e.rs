//! Compaction e2e: land three small files into one table, compact_table coalesces them
//! into a single file (serving read-back sees the same rows), the pre-compaction snapshot
//! still time-travels to the three originals, and a second compaction is a no-op (one
//! file left -> fewer than two small files -> None). On the Iceberg control plane; row
//! content reads through `engine_serving`, file counts through the catalog. NO DuckDB.

mod transform_e2e_support;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{Catalog, ControlPlane, PageReq};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use transform::{CompactConfig, WriteConfig, compact_table};

use transform_e2e_support::{
    col_csv, cols, make_catalog, scalar_i64, seed_table, seed_table_absolute, tref,
};

#[tokio::test(flavor = "multi_thread")]
async fn compact_coalesces_small_files_and_preserves_time_travel() {
    let fx = PgFixture::start();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let warehouse = wh.path().display().to_string();
    let root_url = format!("file://{warehouse}");
    let catalog = make_catalog(fx.pg_dsn(&db), &warehouse).await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wh.path()).expect("store"));
    let cp = IcebergControlPlane::new(pg, catalog);

    let cspec = cols(&[("id", "long", false), ("label", "string", true)]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]));

    let acc = tref("main", "acc");
    let row = |id: i64, label: &str| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![Some(label.to_string())])),
            ],
        )
        .unwrap()
    };

    // Three appends (distinct prefixes) -> three small data files.
    seed_table(
        &cp,
        &store,
        &acc,
        &cspec,
        schema.clone(),
        row(1, "a"),
        "run-1",
    )
    .await;
    seed_table(
        &cp,
        &store,
        &acc,
        &cspec,
        schema.clone(),
        row(2, "b"),
        "run-2",
    )
    .await;
    seed_table(
        &cp,
        &store,
        &acc,
        &cspec,
        schema.clone(),
        row(3, "c"),
        "run-3",
    )
    .await;

    let before = cp.catalog().current_snapshot(&acc).await.unwrap().id;
    let files_before = cp
        .catalog()
        .files(&acc, before, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(files_before.len(), 3, "three small files before compaction");

    // Compact: 10 MiB threshold (all three qualify), default 128 MiB output target -> 1 file.
    let cfg = CompactConfig {
        small_file_threshold_bytes: 10 * 1024 * 1024,
        write: WriteConfig::default(),
    };
    let snap = compact_table(&cp, store.clone(), &root_url, "compact-1", &acc, &cfg)
        .await
        .unwrap()
        .expect("compaction produced a snapshot");

    let files_after = cp
        .catalog()
        .files(&acc, snap, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(files_after.len(), 1, "three small files coalesced into one");

    // Serving read-back: the row set is unchanged.
    let serving = IcebergCatalog::new(fx.pool_for(&db).await);
    let count =
        engine_serving::execute_query(&serving, "SELECT count(*) FROM \"main\".\"acc\"", None)
            .await
            .expect("count");
    assert_eq!(scalar_i64(&count), 3, "compaction preserves the row set");
    let labels = engine_serving::execute_query(
        &serving,
        "SELECT \"id\", \"label\" FROM \"main\".\"acc\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("labels");
    assert_eq!(col_csv(&labels), "a,b,c", "values intact after rewrite");

    // Time travel: the pre-compaction snapshot still lists the three originals.
    let files_then = cp
        .catalog()
        .files(&acc, before, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(
        files_then.len(),
        3,
        "prior snapshot retains the original three files (time travel)"
    );

    // No-op: one coalesced file remains (< 2 small files) -> None, no new snapshot.
    let again = compact_table(&cp, store.clone(), &root_url, "compact-2", &acc, &cfg)
        .await
        .unwrap();
    assert!(again.is_none(), "fewer than two small files -> no-op");
    let head = cp.catalog().current_snapshot(&acc).await.unwrap().id;
    assert_eq!(head, snap, "no-op created no new snapshot");
}

#[tokio::test(flavor = "multi_thread")]
async fn compact_leaves_large_files_untouched() {
    let fx = PgFixture::start();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let warehouse = wh.path().display().to_string();
    let root_url = format!("file://{warehouse}");
    let catalog = make_catalog(fx.pg_dsn(&db), &warehouse).await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wh.path()).expect("store"));
    let cp = IcebergControlPlane::new(pg, catalog);

    let cspec = cols(&[("id", "long", false), ("label", "string", true)]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]));

    let mixed = tref("main", "mixed");
    let row = |id: i64, label: &str| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![Some(label.to_string())])),
            ],
        )
        .unwrap()
    };

    // Three 1-row (small) files plus one 200-row (large) file.
    seed_table(
        &cp,
        &store,
        &mixed,
        &cspec,
        schema.clone(),
        row(1, "a"),
        "s1",
    )
    .await;
    seed_table(
        &cp,
        &store,
        &mixed,
        &cspec,
        schema.clone(),
        row(2, "b"),
        "s2",
    )
    .await;
    seed_table(
        &cp,
        &store,
        &mixed,
        &cspec,
        schema.clone(),
        row(3, "c"),
        "s3",
    )
    .await;
    let big = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from((0..200).collect::<Vec<i64>>())),
            Arc::new(StringArray::from(
                (0..200).map(|i| Some(format!("x{i}"))).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    // The large file is left untouched by compaction (never scanned), but IS read back
    // through serving for the final count, so it is seeded with an ABSOLUTE mirror path.
    seed_table_absolute(
        &cp,
        &store,
        &root_url,
        &mixed,
        &cspec,
        schema.clone(),
        big,
        "big",
    )
    .await;

    // Set the threshold to the largest file's exact size so the 200-row file is NOT a
    // candidate (`file_size_bytes < threshold` is false at equality) while the three
    // 1-row files are. Reading sizes from the catalog keeps the test robust to parquet
    // size variance rather than hard-coding byte counts.
    let before = cp.catalog().current_snapshot(&mixed).await.unwrap().id;
    let live = cp
        .catalog()
        .files(&mixed, before, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(live.len(), 4, "four files before compaction");
    let large = live
        .items
        .iter()
        .max_by_key(|f| f.file_size_bytes)
        .unwrap()
        .clone();

    let cfg = CompactConfig {
        small_file_threshold_bytes: large.file_size_bytes,
        write: WriteConfig::default(),
    };
    let snap = compact_table(&cp, store.clone(), &root_url, "mix-1", &mixed, &cfg)
        .await
        .unwrap()
        .expect("the three small files were compacted");

    // After: the large file is still live (path unchanged) plus exactly one coalesced file.
    let after = cp
        .catalog()
        .files(&mixed, snap, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(after.len(), 2, "large file untouched + one coalesced file");
    assert!(
        after.items.iter().any(|f| f.path == large.path),
        "the large file is left live with its path unchanged"
    );

    // Row set preserved: 3 small + 200 large = 203.
    let serving = IcebergCatalog::new(fx.pool_for(&db).await);
    let count =
        engine_serving::execute_query(&serving, "SELECT count(*) FROM \"main\".\"mixed\"", None)
            .await
            .expect("count");
    assert_eq!(
        scalar_i64(&count),
        203,
        "compaction over a mixed set preserves all rows"
    );
}
