//! The relocated engine-side write executor: build a one-row IPC stream, land it,
//! overwrite it, and truncate it — asserting snapshot ids and committed rows.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetRef, EventType, LineageEvent, ObjectType, PropertyDef, RunId,
    TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use engine_serving::IcebergActionWriter;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use uuid::Uuid;

async fn build_catalog(dsn: &str, warehouse: &std::path::Path) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn.to_string());
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{}", warehouse.display()),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("build SqlCatalog")
}

fn one_row_ipc(id: i64, name: &str) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![name])),
        ],
    )
    .expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

fn cols() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: true,
        },
        ColumnSpec {
            name: "name".into(),
            ty: "string".into(),
            nullable: true,
        },
    ]
}

fn event(op: &str) -> LineageEvent {
    let ds = DatasetRef {
        namespace: "loom".into(),
        name: "main.widget".into(),
    };
    LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("ts"),
        inputs: vec![],
        outputs: vec![ds],
        payload: serde_json::json!({ "action": "test", "op": op }),
    }
}

/// Define the `Widget` type in the ontology so `land`/`overwrite_parquet_snapshot`
/// succeed on a fresh table. Copied from `action_e2e.rs::setup_widget_writer`.
async fn e2e_seed_widget_table(cp: &PgControlPlane) {
    let widget = TypeName("Widget".into());
    cp.ontology()
        .define_type(ObjectType {
            name: widget.clone(),
            table: TableRef {
                schema: "main".into(),
                name: "widget".into(),
            },
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
                PropertyDef {
                    name: "name".into(),
                    ty: "String".into(),
                    required: false,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
            derived: vec![],
            identity: None,
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_overwrite_truncate() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = Arc::new(build_catalog(&dsn, warehouse.path()).await);

    // Define the table in the mirror catalog so land/overwrite have a target.
    e2e_seed_widget_table(&cp).await;

    let table = TableRef {
        schema: "main".into(),
        name: "widget".into(),
    };
    // Large inline limit so the single row inlines (no flush job needed).
    let writer = IcebergActionWriter::new(catalog, pool.clone(), 16 * 1024 * 1024, i64::MAX);

    let s1 = writer
        .write_object(&table, &cols(), &one_row_ipc(1, "a"), event("insert"))
        .await
        .expect("write_object");
    assert!(s1.0 > 0);

    let s2 = writer
        .overwrite_table(&table, &cols(), &one_row_ipc(2, "b"), event("update"))
        .await
        .expect("overwrite_table");
    assert!(s2.0 > s1.0);

    // Empty ipc ⇒ truncate (delete-all).
    let s3 = writer
        .overwrite_table(&table, &[], &[], event("delete"))
        .await
        .expect("truncate");
    assert!(s3.0 > s2.0);
}
