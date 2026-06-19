//! Fixture tests for the Iceberg landing entrypoint: byte-size routing between an
//! inline (mirror-only) write and a real Parquet write, both emitting lineage
//! atomically and returning the loom mirror snapshot id.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc57::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, DatasetId, EventType, Lineage, LineageEvent, PageReq, RunId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

/// Build an Arrow-57 IPC stream body of `rows` rows with a single `id: long` column.
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

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
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
        outputs: vec![DatasetId::from(&out).dataset_ref()],
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

/// A small request (under the byte limit) inlines: mirror-only rows + lineage, no
/// object-storage Parquet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_request_inlines_and_emits_lineage() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let run = RunId(uuid::Uuid::new_v4());
    let table = TableRef {
        schema: "wh".into(),
        name: "small".into(),
    };
    // usize::MAX limit -> always inline.
    let snap = land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(3),
        usize::MAX,
        lineage(run, "wh", "small"),
    )
    .await
    .expect("land inline");
    assert!(snap.0 > 0, "a real snapshot id");

    // The inline rows are live in the mirror at that snapshot.
    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice.current_snapshot(&table).await.expect("current");
    assert_eq!(cur.id, snap, "returned id is the mirror current snapshot");

    let page = cp
        .events_for(&run, PageReq::unbounded())
        .await
        .expect("events");
    assert_eq!(page.items.len(), 1, "one lineage event for the run");
    assert_eq!(page.items[0].outputs[0].name, "wh.small");
}

/// A large request (over the byte limit) writes real Parquet: create-if-absent
/// namespace + table, append, atomic lineage, returns the mirror snapshot id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_request_writes_parquet_and_emits_lineage() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let run = RunId(uuid::Uuid::new_v4());
    let table = TableRef {
        schema: "wh".into(),
        name: "big".into(),
    };
    // limit 0 -> always Parquet.
    let snap = land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(5),
        0,
        lineage(run, "wh", "big"),
    )
    .await
    .expect("land parquet");

    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice.current_snapshot(&table).await.expect("current");
    assert_eq!(cur.id, snap, "returned id is the mirror current snapshot");

    let page = cp
        .events_for(&run, PageReq::unbounded())
        .await
        .expect("events");
    assert_eq!(page.items.len(), 1, "one lineage event for the run");
    assert_eq!(page.items[0].outputs[0].name, "wh.big");
}
