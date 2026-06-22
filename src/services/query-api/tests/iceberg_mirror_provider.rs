//! De-risk: IcebergMirrorTableProvider prunes whole files a predicate can't match,
//! always keeps no-stats files, and returns correct rows over the survivors.
//! Pure-logic test (local Parquet, no Postgres) — plain rust_test.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::snapshot::{ColumnStat, StatValue};
use control_plane_postgres::iceberg_catalog::FileWithStats;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::SessionContext;
use object_store::local::LocalFileSystem;
use parquet::arrow::ArrowWriter;
use query_api::serving::SqlValue;
use query_api::serving_datafusion::{IcebergMirrorTableProvider, batches_to_rows, prune_files};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// Write a one-file Parquet at `path` with the given ids/names.
fn write_parquet(path: &std::path::Path, ids: Vec<i64>, names: Vec<&str>) {
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(file, schema(), None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn stats(path: &str, size: i64, min: i64, max: i64) -> FileWithStats {
    FileWithStats {
        path: path.to_string(),
        record_count: 1,
        file_size_bytes: size,
        column_stats: vec![ColumnStat {
            column_name: "id".into(),
            null_count: 0,
            column_size_bytes: 0,
            min: Some(StatValue::I64(min)),
            max: Some(StatValue::I64(max)),
        }],
    }
}

#[test]
fn prune_drops_nonmatching_keeps_nostats() {
    let s = schema();
    let file_a = stats("/w/a.parquet", 10, 1, 5); // id in [1,5]
    let file_b = stats("/w/b.parquet", 10, 100, 200); // id in [100,200]
    let file_c = FileWithStats {
        path: "/w/c.parquet".into(),
        record_count: 1,
        file_size_bytes: 10,
        column_stats: vec![], // no stats -> always kept
    };
    let files = vec![file_a, file_b, file_c];
    // WHERE id = 3 -> only file_a can match; file_c kept (no stats); file_b dropped.
    let kept = prune_files(&s, &[col("id").eq(lit(3i64))], &files);
    let kept_paths: Vec<&str> = kept.iter().map(|f| f.path.as_str()).collect();
    assert!(kept_paths.contains(&"/w/a.parquet"), "matching file kept");
    assert!(
        kept_paths.contains(&"/w/c.parquet"),
        "no-stats file always kept"
    );
    assert!(
        !kept_paths.contains(&"/w/b.parquet"),
        "non-matching file dropped"
    );
}

#[tokio::test]
async fn scan_over_survivors_returns_correct_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path_a = dir.path().join("a.parquet");
    let path_b = dir.path().join("b.parquet");
    write_parquet(&path_a, vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    write_parquet(&path_b, vec![100, 200], vec!["x", "y"]);

    let size_a = std::fs::metadata(&path_a).unwrap().len() as i64;
    let size_b = std::fs::metadata(&path_b).unwrap().len() as i64;
    let files = vec![
        stats(path_a.to_str().unwrap(), size_a, 1, 5),
        stats(path_b.to_str().unwrap(), size_b, 100, 200),
    ];

    let ctx = SessionContext::new();
    ctx.register_object_store(
        ObjectStoreUrl::local_filesystem().as_ref(),
        Arc::new(LocalFileSystem::new()),
    );
    let provider = IcebergMirrorTableProvider::try_new(&ctx, files)
        .await
        .unwrap();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    // id = 3 lives only in file A; file B is pruned. Result must be exactly [3].
    let df = ctx.sql("SELECT id FROM t WHERE id = 3").await.unwrap();
    let batches = df.collect().await.unwrap();
    let rows = batches_to_rows(batches);
    assert_eq!(rows.columns, vec!["id".to_string()]);
    assert_eq!(rows.rows.len(), 1, "exactly one matching row");
    assert_eq!(rows.rows[0][0], SqlValue::Int(3));
}
