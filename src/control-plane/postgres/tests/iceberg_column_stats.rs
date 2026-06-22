//! Fixture: the Iceberg write path persists per-file column stats in the same
//! transaction as the snapshot commit. Seeds a table with explicit values via
//! `IcebergWriter::seed_arrays` so min/max/null-counts are known, then reads the
//! `iceberg_mirror.data_file_column_stat` rows directly and asserts the bounds.
//! `loom_fixture_test`, not an inline module.

use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use sqlx::Row;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_path_persists_per_column_stats() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    // One file: id long = [10, 4, 7] (min 4, max 10), name string = ["m","a","z"]
    // (min "a", max "z"). No nulls.
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

    // Read the persisted stats directly. Runtime query (tests don't go through the
    // compile-time cache). Exactly one data file -> two stat rows.
    let rows = sqlx::query(
        "select column_name, null_count, min_value, max_value \
         from iceberg_mirror.data_file_column_stat order by column_name",
    )
    .fetch_all(&pool)
    .await
    .expect("query stats");

    assert_eq!(rows.len(), 2, "one stat row per column for the single file");

    // Ordered by column_name: "id" then "name".
    let id = &rows[0];
    assert_eq!(id.get::<String, _>("column_name"), "id");
    assert_eq!(id.get::<i64, _>("null_count"), 0);
    assert_eq!(
        id.get::<Option<String>, _>("min_value"),
        Some("4".to_string())
    );
    assert_eq!(
        id.get::<Option<String>, _>("max_value"),
        Some("10".to_string())
    );

    let name = &rows[1];
    assert_eq!(name.get::<String, _>("column_name"), "name");
    assert_eq!(name.get::<i64, _>("null_count"), 0);
    assert_eq!(
        name.get::<Option<String>, _>("min_value"),
        Some("a".to_string())
    );
    assert_eq!(
        name.get::<Option<String>, _>("max_value"),
        Some("z".to_string())
    );
}
