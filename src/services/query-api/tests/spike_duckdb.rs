//! GO/NO-GO spike: can an embedded duckdb-rs at our pinned version ATTACH loom's
//! DuckLake catalog (with the vendored 1.5.3 ducklake extension) and read it?
//! Mirrors the ATTACH preamble proven in postgres/tests/ducklake_interop.rs.

use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};

#[tokio::test(flavor = "multi_thread")]
async fn embedded_duckdb_attaches_loom_catalog() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await; // creates the 27 ducklake_* tables + `main` schema

    let ext_dir = std::env::var("DUCKDB_EXTENSION_DIR").expect("DUCKDB_EXTENSION_DIR");
    // Reuse the writer's DATA_PATH: DuckLake records it in the catalog on bootstrap
    // and rejects a mismatched one on re-ATTACH.
    let attach = format!(
        "SET extension_directory='{}';\nLOAD ducklake;\nLOAD postgres_scanner;\n\
         ATTACH 'ducklake:postgres:dbname={} host={} user=postgres' AS lake \
         (DATA_PATH '{}/', DATA_INLINING_ROW_LIMIT 0);",
        ext_dir,
        db,
        fx.socket_path().display(),
        writer.data_path().display(),
    );

    // Run on a blocking thread: duckdb-rs is synchronous.
    tokio::task::spawn_blocking(move || {
        let conn = duckdb::Connection::open_in_memory().expect("open duckdb");
        conn.execute_batch(&attach).expect("ATTACH loom catalog");
        // duckdb_tables() lists tables across attached catalogs; reading it back
        // proves the attached DuckLake catalog is queryable (0 rows for a
        // bootstrap-only catalog — the point is the read succeeds).
        let n: i64 = conn
            .query_row(
                "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake'",
                [],
                |r| r.get(0),
            )
            .expect("read catalog");
        assert!(n >= 0, "catalog readable");
    })
    .await
    .unwrap();
}
