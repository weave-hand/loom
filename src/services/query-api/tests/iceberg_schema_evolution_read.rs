//! End-to-end: after an additive land, a current-snapshot read returns the superset
//! schema with the landed value for post-evolution rows and NULL for pre-evolution rows.
//! Lands through `control_plane_postgres::iceberg_landing::land` (inline limit 0 forces
//! Parquet), reads through `register_iceberg_table` + the embedded engine. Setup mirrors
//! tests/iceberg_pruning_e2e.rs + tests/iceberg_action_e2e.rs.
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc57::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use datafusion::prelude::SessionContext;
use engine_serving::register_iceberg_table;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use query_api::serving::SqlValue;
use query_api::serving_datafusion::batches_to_rows;

fn col(name: &str, ty: &str, nullable: bool) -> ColumnSpec {
    ColumnSpec {
        name: name.into(),
        ty: ty.into(),
        nullable,
    }
}
fn lineage() -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "read-evolution-test" }),
    }
}
fn encode(schema: &Arc<Schema>, batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
    }
    buf
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_after_additive_land_returns_superset_with_nulls() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().unwrap();
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef {
        schema: "s".into(),
        name: "t".into(),
    };

    // Base land (a long, b string) — ids 0,1.
    let ab = vec![col("a", "long", false), col("b", "string", true)];
    let ab_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
    ]));
    let ab_batch = RecordBatch::try_new(
        ab_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![0i64, 1])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )
    .unwrap();
    land(
        &pool,
        &catalog,
        &t,
        &ab,
        &encode(&ab_schema, &ab_batch),
        0,
        i64::MAX,
        lineage(),
    )
    .await
    .expect("base land");

    // Additive land (a long, b string, c long-nullable) — ids 2,3 with c set.
    let abc = vec![
        col("a", "long", false),
        col("b", "string", true),
        col("c", "long", true),
    ];
    let abc_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
        Field::new("c", DataType::Int64, true),
    ]));
    let abc_batch = RecordBatch::try_new(
        abc_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![2i64, 3])),
            Arc::new(StringArray::from(vec!["p", "q"])),
            Arc::new(Int64Array::from(vec![200i64, 300])),
        ],
    )
    .unwrap();
    land(
        &pool,
        &catalog,
        &t,
        &abc,
        &encode(&abc_schema, &abc_batch),
        0,
        i64::MAX,
        lineage(),
    )
    .await
    .expect("additive land");

    // Read at the current snapshot through the serving path.
    let cat = IcebergCatalog::new(pool.clone());
    let ctx = SessionContext::new();
    register_iceberg_table(&ctx, &cat, &t)
        .await
        .expect("register");
    let df = ctx
        .sql("SELECT a, c FROM \"s\".\"t\" ORDER BY a")
        .await
        .expect("sql");
    let rows = batches_to_rows(df.collect().await.expect("collect"));

    // Column c is present (superset schema); c is NULL for the pre-evolution rows
    // (a in {0,1}) and the landed value for post-evolution rows (a=2 -> 200, a=3 -> 300).
    assert_eq!(rows.columns, vec!["a".to_string(), "c".to_string()]);
    assert_eq!(
        rows.rows,
        vec![
            vec![SqlValue::Int(0), SqlValue::Null],
            vec![SqlValue::Int(1), SqlValue::Null],
            vec![SqlValue::Int(2), SqlValue::Int(200)],
            vec![SqlValue::Int(3), SqlValue::Int(300)],
        ]
    );

    // Keep the warehouse TempDir alive until here (its files back the read).
    drop(wh);
}
