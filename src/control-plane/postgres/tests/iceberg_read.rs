//! Fixture test for `read_files_as_batches` — land a known batch, resolve its
//! file paths from the mirror, read them back, and assert exact row count + schema.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::Catalog;
use control_plane_core::{ColumnSpec, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
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

/// An Arrow IPC body of `rows` rows, single `id: long` column (ids `0..rows`).
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
        outputs: vec![control_plane_core::DatasetId::from(&out).dataset_ref()],
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

/// Land 3 rows, resolve the file paths from the mirror, call `read_files_as_batches`,
/// and assert exact row count + schema field names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_landed_file_back_to_exact_rows() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let table = TableRef {
        schema: "wh".into(),
        name: "read_test".into(),
    };

    // Land 3 rows (limit 0 forces real Parquet).
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(3),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "read_test"),
    )
    .await
    .expect("land");

    // Resolve the live file paths at the current snapshot via the mirror.
    let ice = IcebergCatalog::new(pool.clone());
    let snap = ice.current_snapshot(&table).await.expect("snapshot");
    let files: Vec<String> = ice
        .files_with_stats(&table, snap.id)
        .await
        .expect("files")
        .into_iter()
        .map(|f| f.path)
        .collect();
    assert!(!files.is_empty());

    let (schema, batches) = control_plane_postgres::read_files_as_batches(&catalog, &table, &files)
        .await
        .expect("read");

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "expected 3 rows, got {total}");
    assert!(
        schema.fields().iter().any(|f| f.name() == "id"),
        "schema should contain 'id' field"
    );
}
