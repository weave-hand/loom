//! Decision-E spike: prove DuckLake inline writes. A single-row INSERT with
//! DATA_INLINING_ROW_LIMIT > 0 must (1) write NO Parquet data file, (2) be readable
//! through the read-side ATTACH (DATA_INLINING_ROW_LIMIT 0), and (3) produce a new
//! catalog snapshot visible via Catalog::current_snapshot. Gates the rest of Actions.

use control_plane_core::{ControlPlane, TableRef};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};

fn preamble(
    ext_dir: &str,
    pg_conn: &str,
    data_path: &std::path::Path,
    inline_limit: u32,
) -> String {
    format!(
        "SET extension_directory='{ext_dir}';\nLOAD ducklake;\nLOAD postgres_scanner;\n\
         ATTACH 'ducklake:postgres:{pg_conn}' AS lake \
         (DATA_PATH '{}/', DATA_INLINING_ROW_LIMIT {inline_limit});\nUSE lake;",
        data_path.display(),
    )
}

fn parquet_count(dir: &std::path::Path) -> usize {
    fn walk(dir: &std::path::Path, n: &mut usize) {
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, n);
                } else if p.extension().is_some_and(|x| x == "parquet") {
                    *n += 1;
                }
            }
        }
    }
    let mut n = 0;
    walk(dir, &mut n);
    n
}

#[tokio::test(flavor = "multi_thread")]
async fn inline_insert_writes_no_parquet_and_reads_back() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;

    // Create an empty table (0 batches => no Parquet written by seed).
    writer
        .seed(
            "main",
            "widget",
            &[
                ("id".into(), "BIGINT".into(), false),
                ("name".into(), "VARCHAR".into(), true),
            ],
            &[],
        )
        .await;
    let data_path = writer.data_path().to_path_buf();
    assert_eq!(
        parquet_count(&data_path),
        0,
        "table created, no Parquet yet"
    );

    let ext_dir = std::env::var("DUCKDB_EXTENSION_DIR").expect("DUCKDB_EXTENSION_DIR");
    let pg_conn = format!(
        "dbname={db} host={} user=postgres",
        fx.socket_path().display()
    );

    // INLINE write: ATTACH with inlining ON, INSERT one row.
    let attach_w = preamble(&ext_dir, &pg_conn, &data_path, 100);
    tokio::task::spawn_blocking(move || {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&attach_w).unwrap();
        conn.execute(
            "INSERT INTO main.widget (id, name) VALUES (?, ?)",
            duckdb::params![1_i64, "a"],
        )
        .unwrap();
    })
    .await
    .unwrap();

    // (1) No Parquet file was written — the row is inline.
    assert_eq!(
        parquet_count(&data_path),
        0,
        "inline insert wrote no Parquet file"
    );

    // (2) Read back through the READ-side config (inlining disabled) — must see the row.
    let attach_r = preamble(&ext_dir, &pg_conn, &data_path, 0);
    let count = tokio::task::spawn_blocking(move || {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&attach_r).unwrap();
        conn.query_row("SELECT count(*) FROM main.widget", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(count, 1, "read-side ATTACH reconciles the inlined row");

    // (3) The new snapshot is visible via the catalog (the handler reads this for lineage).
    let snap = cp
        .catalog()
        .current_snapshot(&TableRef {
            schema: "main".into(),
            name: "widget".into(),
        })
        .await
        .expect("current_snapshot after inline insert");
    assert!(snap.id.0 > 0, "inline insert advanced the catalog snapshot");
}
