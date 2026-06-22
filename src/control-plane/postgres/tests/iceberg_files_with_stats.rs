//! Fixture: the Iceberg read channel (`IcebergCatalog::files_with_stats`) joins
//! the live data files to their persisted per-column stats and re-types the stored
//! text bounds via each column's iceberg type. Seeds an explicit TWO-file table via
//! two `seed_arrays` appends (each appends one Parquet file) so min/max/null-counts
//! per file are known, then asserts each file's `column_stats` carries the re-typed
//! `StatValue` bounds and null-counts against the seed values. `loom_fixture_test`,
//! not an inline module.

use control_plane_core::snapshot::StatValue;
use control_plane_core::{Catalog, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn files_with_stats_retypes_bounds_per_file() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];

    // File A: id long = [10, 4, 7] (min 4, max 10); name string = ["m","a","z"] (min "a", max "z").
    writer
        .seed_arrays(
            "sales",
            "orders",
            &cols,
            &[
                SeedCol::Long(vec![10, 4, 7]),
                SeedCol::Str(vec!["m", "a", "z"]),
            ],
        )
        .await;

    // File B: id long = [100, 200] (min 100, max 200); name string = ["q","b"] (min "b", max "q").
    writer
        .seed_arrays(
            "sales",
            "orders",
            &cols,
            &[SeedCol::Long(vec![100, 200]), SeedCol::Str(vec!["q", "b"])],
        )
        .await;

    let catalog = IcebergCatalog::new(pool.clone());
    let table = TableRef {
        schema: "sales".to_string(),
        name: "orders".to_string(),
    };
    let snap = catalog.current_snapshot(&table).await.unwrap();
    let files = catalog.files_with_stats(&table, snap.id).await.unwrap();

    assert_eq!(files.len(), 2, "two appends -> two live data files");

    // Files come back ordered by data_file_id, so files[0] is file A, files[1] is file B.
    let a = &files[0];
    assert_eq!(a.record_count, 3);
    let a_id = a
        .column_stats
        .iter()
        .find(|s| s.column_name == "id")
        .expect("id stat for file A");
    assert_eq!(a_id.null_count, 0);
    assert_eq!(a_id.min, Some(StatValue::I64(4)));
    assert_eq!(a_id.max, Some(StatValue::I64(10)));
    let a_name = a
        .column_stats
        .iter()
        .find(|s| s.column_name == "name")
        .expect("name stat for file A");
    assert_eq!(a_name.min, Some(StatValue::Str("a".to_string())));
    assert_eq!(a_name.max, Some(StatValue::Str("z".to_string())));

    let b = &files[1];
    assert_eq!(b.record_count, 2);
    let b_id = b
        .column_stats
        .iter()
        .find(|s| s.column_name == "id")
        .expect("id stat for file B");
    assert_eq!(b_id.null_count, 0);
    assert_eq!(b_id.min, Some(StatValue::I64(100)));
    assert_eq!(b_id.max, Some(StatValue::I64(200)));
    let b_name = b
        .column_stats
        .iter()
        .find(|s| s.column_name == "name")
        .expect("name stat for file B");
    assert_eq!(b_name.min, Some(StatValue::Str("b".to_string())));
    assert_eq!(b_name.max, Some(StatValue::Str("q".to_string())));
}
