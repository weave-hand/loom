//! End-to-end: `run_transform` driven by an `IcebergControlPlane` writes its output to
//! Iceberg (mirror-projected). The SAME transform code (`run.rs`, unchanged) commits
//! through the polymorphic `Tx` — append adds the result, overwrite replaces the live
//! contents while a prior snapshot still time-travels. Proves acceptance #1/#2/#3:
//! `LOOM_TRANSFORM_BACKEND=iceberg` output is readable through the Iceberg catalog.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlane, DataFile, DatasetRef, EventType, FileFormat, LineageEvent,
    PageReq, RunId, SnapshotId, TableRef, Tx,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use datafusion_io::{WriteConfig, write_dataset};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use time::OffsetDateTime;
use transform::{OutputMode, TransformInput, TransformRequest, run_transform};
use uuid::Uuid;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

fn id_cols() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

fn lineage(out: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(out)],
        payload: serde_json::json!({}),
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

/// Sum the record counts of the files live at `snap` for `table`.
async fn live_rows(cp: &IcebergControlPlane, table: &TableRef, snap: SnapshotId) -> i64 {
    cp.catalog()
        .files(table, snap, PageReq::unbounded())
        .await
        .unwrap()
        .items
        .iter()
        .map(|f| f.record_count)
        .sum()
}

/// A transform whose output goes to Iceberg appends, then overwrites — replacing the
/// live contents while the prior snapshot still time-travels.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transform_writes_output_to_iceberg() {
    let fx = PgFixture::start();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let warehouse = wh.path().display().to_string();
    let catalog = make_catalog(fx.pg_dsn(&db), &warehouse).await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wh.path()).expect("store"));
    let cp = IcebergControlPlane::new(pg, catalog);

    let src = tref("main", "src");
    let out = tref("main", "out");

    // Seed input `main.src` with 3 rows: write real Parquet via write_dataset, then
    // register it into Iceberg through the same IcebergTx the transform output uses.
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![0i64, 1, 2]))],
    )
    .unwrap();
    let written = write_dataset(
        store.clone(),
        "main/src/seed",
        schema.clone(),
        &[batch],
        &WriteConfig::default(),
    )
    .await
    .unwrap();
    let src_files: Vec<DataFile> = written
        .into_iter()
        .map(|f| DataFile {
            path: f.path,
            path_is_relative: true,
            file_format: FileFormat::Parquet,
            record_count: f.record_count,
            file_size_bytes: f.file_size_bytes,
            column_stats: f.column_stats,
            parquet_footer_size: Some(f.footer_size),
        })
        .collect();
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(&src, &id_cols()).await.unwrap();
    tx.append_files(&src, &src_files).await.unwrap();
    tx.commit().await.unwrap().expect("seed snapshot");

    // Append transform: out := SELECT id FROM src  (3 rows).
    let inputs = [TransformInput {
        table: &src,
        register_as: "src",
    }];
    run_transform(
        &cp,
        store.clone(),
        "run-append",
        TransformRequest {
            inputs: &inputs,
            output: &out,
            sql: "SELECT id FROM src",
            conform: None,
            output_mode: OutputMode::Append,
            lineage: lineage(&out),
        },
    )
    .await
    .expect("append transform");

    let snap_append = cp.catalog().current_snapshot(&out).await.unwrap().id;
    assert_eq!(
        live_rows(&cp, &out, snap_append).await,
        3,
        "append wrote all 3 rows to Iceberg"
    );

    // Overwrite transform: out := SELECT id FROM src WHERE id = 0  (1 row).
    run_transform(
        &cp,
        store.clone(),
        "run-overwrite",
        TransformRequest {
            inputs: &inputs,
            output: &out,
            sql: "SELECT id FROM src WHERE id = 0",
            conform: None,
            output_mode: OutputMode::Overwrite,
            lineage: lineage(&out),
        },
    )
    .await
    .expect("overwrite transform");

    let snap_overwrite = cp.catalog().current_snapshot(&out).await.unwrap().id;
    assert!(snap_overwrite.0 > snap_append.0);
    assert_eq!(
        live_rows(&cp, &out, snap_overwrite).await,
        1,
        "overwrite REPLACED the live contents -> 1 row"
    );
    // Time travel: the pre-overwrite snapshot still sees all 3 rows.
    assert_eq!(
        live_rows(&cp, &out, snap_append).await,
        3,
        "prior snapshot still time-travels to the appended 3 rows"
    );
}
