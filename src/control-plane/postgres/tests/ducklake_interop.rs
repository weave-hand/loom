//! Interop guardrail: prove the pinned DuckDB engine can READ a catalog that
//! loom's NATIVE writer produced, and BUILD ON IT (append its own snapshot on
//! top of loom's `ducklake_*` rows). If loom's catalog rows diverge from what
//! DuckDB expects, DuckDB will reject them here — this test is the canary.
//!
//! Direction matters: the conformance test proves loom reads its own writes;
//! THIS proves DuckDB (the foreign engine) reads loom's writes.

use control_plane_core::{
    Catalog, ColumnSpec, ColumnStat, ControlPlane, DataFile, PageReq, TableRef, Tx,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};

// REQUIRED guardrail: loom creates a table (DDL only), then DuckDB inserts into
// it and reads back, and the snapshot DuckDB allocates is loom's + 1 (counter
// continuity). This proves DuckDB parsed loom's ducklake_table / ducklake_column
// / ducklake_schema_versions / ducklake_snapshot rows and advanced loom's
// snapshot/file/row-id counters.
#[tokio::test]
async fn duckdb_reads_loom_catalog_and_appends() {
    let fixture = PgFixture::start();
    let (cp, db) = fixture.fresh_db().await;
    let writer = DuckLakeWriter::new(fixture.socket_path(), &db);

    // Bare ATTACH: 27 ducklake_* tables, snapshot 0, `main` schema, no table.
    writer.bootstrap().await;

    // loom natively creates main.t (id int64, name varchar) — DDL only, no file.
    let t = TableRef {
        schema: "main".into(),
        name: "t".into(),
    };
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(
        &t,
        &[
            ColumnSpec {
                name: "id".into(),
                ty: "int64".into(),
                nullable: false,
            },
            ColumnSpec {
                name: "name".into(),
                ty: "varchar".into(),
                nullable: true,
            },
        ],
    )
    .await
    .unwrap();
    let loom_snap = tx
        .commit()
        .await
        .unwrap()
        .expect("create_table commit yields a snapshot id")
        .0;

    // THE GUARDRAIL: DuckDB inserts into loom's table, then reads it back.
    // If DuckDB could not parse loom's catalog rows this would error.
    writer
        .exec("INSERT INTO lake.main.t VALUES (1, 'a');")
        .await;
    let count = writer
        .query_scalar("SELECT count(*) FROM lake.main.t;")
        .await;
    assert_eq!(count, "1", "DuckDB must read its own row from loom's table");
    let id = writer.query_scalar("SELECT id FROM lake.main.t;").await;
    assert_eq!(id, "1");

    // Counter continuity: the snapshot DuckDB created sits directly atop loom's.
    let max_snap = writer.max_snapshot_id().await;
    assert_eq!(
        max_snap,
        loom_snap + 1,
        "DuckDB's INSERT snapshot must be loom's + 1 (continuity), loom_snap={loom_snap}",
    );
}

// STRETCH guardrail: DuckDB SCANS a data file that loom's `append_files`
// registered. We can't make loom write Parquet yet (no ingest service), so we
// use DuckDB to COPY a real Parquet to the path loom will register, read the
// file's true size + footer length off disk, then loom registers it via
// append_files + commit. DuckDB then scans loom's row purely from loom's
// catalog rows + the on-disk file.
#[tokio::test]
async fn duckdb_scans_loom_appended_file() {
    let fixture = PgFixture::start();
    let (cp, db) = fixture.fresh_db().await;
    let writer = DuckLakeWriter::new(fixture.socket_path(), &db);
    writer.bootstrap().await;

    let t = TableRef {
        schema: "main".into(),
        name: "t".into(),
    };
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(
        &t,
        &[
            ColumnSpec {
                name: "id".into(),
                ty: "int64".into(),
                nullable: false,
            },
            ColumnSpec {
                name: "name".into(),
                ty: "varchar".into(),
                nullable: true,
            },
        ],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap().expect("create snapshot");

    // DuckLake resolves a relative file path as DATA_PATH + schema.path +
    // table.path + file.path. With path_is_relative=true and file "loom.parquet"
    // for table main.t, the file must live at <data_path>/main/t/loom.parquet.
    let file_dir = writer.data_path().join("main").join("t");
    std::fs::create_dir_all(&file_dir).expect("create file dir");
    let abs = file_dir.join("loom.parquet");

    // Use DuckDB itself to write the Parquet so the file matches DuckDB's reader.
    writer
        .exec(&format!(
            "COPY (SELECT CAST(1 AS BIGINT) AS id, 'a' AS name) TO '{}' (FORMAT parquet);",
            abs.display()
        ))
        .await;

    let bytes = std::fs::read(&abs).expect("read written parquet");
    let file_size_bytes = bytes.len() as i64;
    let footer_size = parquet_footer_size(&bytes);

    // loom natively registers the file DuckDB wrote.
    let mut tx = cp.begin().await.unwrap();
    tx.append_files(
        &t,
        &[DataFile {
            path: "loom.parquet".into(),
            path_is_relative: true,
            record_count: 1,
            file_size_bytes,
            footer_size,
            column_stats: vec![
                ColumnStat {
                    column_name: "id".into(),
                    min: Some("1".into()),
                    max: Some("1".into()),
                    null_count: 0,
                    value_count: 1,
                    column_size_bytes: 8,
                },
                ColumnStat {
                    column_name: "name".into(),
                    min: Some("a".into()),
                    max: Some("a".into()),
                    null_count: 0,
                    value_count: 1,
                    column_size_bytes: 8,
                },
            ],
        }],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap().expect("append snapshot");

    // DuckDB scans loom's registered file from loom's catalog rows.
    let count = writer
        .query_scalar("SELECT count(*) FROM lake.main.t;")
        .await;
    assert_eq!(count, "1", "DuckDB must scan loom's appended file");
    let id = writer.query_scalar("SELECT id FROM lake.main.t;").await;
    assert_eq!(id, "1");
}

/// Read the Parquet footer length: the 8 bytes before the trailing `PAR1` magic
/// are a 4-byte little-endian footer length followed by the magic. DuckLake's
/// `footer_size` records that length (an I/O hint for the metadata read).
fn parquet_footer_size(bytes: &[u8]) -> i64 {
    assert!(bytes.len() >= 8, "not a parquet file");
    assert_eq!(&bytes[bytes.len() - 4..], b"PAR1", "missing parquet magic");
    let len_bytes = &bytes[bytes.len() - 8..bytes.len() - 4];
    u32::from_le_bytes(len_bytes.try_into().unwrap()) as i64
}
