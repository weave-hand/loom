//! Cold-path k-NN: build an index over flushed (Parquet) vector rows, then
//! vector_search returns the exact top-k. Cosine and L2 both verified. Plus the
//! no-index deterministic error. (Hot inline delta is empty this slice — vectors
//! can't inline; see FUTURE fut-inline-vector-hot-delta. Do NOT land inline
//! vector rows here — it errors.)

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Float32Array, Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, DatasetId, EventType, LineageEvent, Metric, ObjectType, Ontology,
    PropertyDef, RunId, TableRef, TypeName,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use control_plane_postgres::vector_index::build_vector_index;
use engine_serving::EngineServingError;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

fn columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "embedding".into(),
            ty: "vector(4)".into(),
            nullable: false,
        },
    ]
}

/// Build an Arrow IPC body with `id: long` + `embedding: list<float32>` (4 elements).
fn ipc_body(rows: &[(i64, [f32; 4])]) -> Vec<u8> {
    let element = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(element.clone());
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    for (_, emb) in rows {
        lb.values().append_slice(emb);
        lb.append(true);
    }
    let id_array = arrow_array::Int64Array::from(ids);
    let emb_array = lb.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", DataType::List(element), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(emb_array)],
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

fn lineage_evt(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
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

/// `ids` downcasts the identity column (Int64Array) → Vec<i64> in row order.
fn ids(batch: &RecordBatch) -> Vec<i64> {
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("identity column is Int64");
    (0..col.len()).map(|i| col.value(i)).collect()
}

/// `distances` downcasts column 1 (Float32Array) → Vec<f32> in row order.
fn distances(batch: &RecordBatch) -> Vec<f32> {
    let col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("_distance column is Float32");
    (0..col.len()).map(|i| col.value(i)).collect()
}

/// Seed the wh.docs table: register type, land rows 1..=4 forced to Parquet
/// (inline_byte_limit 0), and build a vector index with the given metric.
/// Returns the `SqlCatalog`, `PgPool`, control plane, and the `TempDir` guard
/// (caller must keep it alive across all `vector_search` calls).
async fn seed_and_build(
    fx: &PgFixture,
    db: &str,
    metric: Metric,
) -> (
    SqlCatalog,
    sqlx::PgPool,
    control_plane_postgres::PgControlPlane,
    tempfile::TempDir,
) {
    use control_plane_postgres::PgControlPlane;
    use std::time::Duration;

    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(db).await;
    let catalog = make_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_secs(5));

    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    // Register the object type with the ontology (identity = "id").
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Docs".into()),
            table: table.clone(),
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                },
                PropertyDef {
                    name: "embedding".into(),
                    ty: "Vector".into(),
                    required: true,
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .expect("define_type");

    let run = RunId(uuid::Uuid::new_v4());

    // Land rows 1–2 (forced to Parquet: inline_byte_limit = 0).
    let rows_1_2: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(rows_1_2),
        0,
        i64::MAX,
        lineage_evt(run, &table),
    )
    .await
    .expect("land rows 1-2");

    // Land rows 3–4 (also Parquet).
    let rows_3_4: &[(i64, [f32; 4])] = &[(3, [0.0, 0.0, 1.0, 0.0]), (4, [0.0, 0.0, 0.0, 1.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(rows_3_4),
        0,
        i64::MAX,
        lineage_evt(run, &table),
    )
    .await
    .expect("land rows 3-4");

    // Build the vector index.
    let build_run = RunId(uuid::Uuid::new_v4());
    build_vector_index(&catalog, &pool, &table, "embedding", metric, build_run)
        .await
        .expect("build_vector_index");

    (catalog, pool, cp, wh)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knn_cold_exact_cosine() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;

    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    let (catalog, pool, _cp, _wh) = seed_and_build(&fx, &db, Metric::Cosine).await;

    // Query: nearest to id=1's embedding [1,0,0,0] with cosine, k=2.
    let batch = engine_serving::vector_search(
        &catalog,
        &pool,
        &table,
        "embedding",
        &[1.0_f32, 0.0, 0.0, 0.0],
        2,
    )
    .await
    .expect("vector_search cosine");

    assert_eq!(batch.num_rows(), 2, "k=2 rows returned");
    let id_vec = ids(&batch);
    // id=1 is exact match (distance ~ 0); must be first.
    assert_eq!(id_vec[0], 1, "nearest is id=1 (cosine)");
    // Distances ascending.
    let dists = distances(&batch);
    assert!(dists[0] <= dists[1], "distances ascending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knn_cold_exact_l2() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;

    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    let (catalog, pool, _cp, _wh) = seed_and_build(&fx, &db, Metric::L2).await;

    // Query: nearest to id=2's embedding [0,1,0,0] with L2, k=2.
    let batch = engine_serving::vector_search(
        &catalog,
        &pool,
        &table,
        "embedding",
        &[0.0_f32, 1.0, 0.0, 0.0],
        2,
    )
    .await
    .expect("vector_search l2");

    assert_eq!(batch.num_rows(), 2, "k=2 rows returned");
    let id_vec = ids(&batch);
    // id=2 is exact match (L2 distance = 0); must be first.
    assert_eq!(id_vec[0], 2, "nearest is id=2 (L2)");
    // Distances ascending.
    let dists = distances(&batch);
    assert!(dists[0] <= dists[1], "distances ascending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_bound_index_is_deterministic_error() {
    // Seed a table with a vector column but DO NOT build an index; assert
    // vector_search returns Err(EngineServingError::NoIndex(_)), never panics.
    use control_plane_postgres::PgControlPlane;
    use std::time::Duration;

    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;

    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(&db).await;
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_secs(5));

    let table = TableRef {
        schema: "wh".into(),
        name: "nodocs".into(),
    };

    // Register type.
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("NoDocs".into()),
            table: table.clone(),
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                },
                PropertyDef {
                    name: "embedding".into(),
                    ty: "Vector".into(),
                    required: true,
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .expect("define_type");

    // Land one row (Parquet), but skip build_vector_index.
    let run = RunId(uuid::Uuid::new_v4());
    let rows: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(rows),
        0,
        i64::MAX,
        lineage_evt(run, &table),
    )
    .await
    .expect("land row");

    // Call vector_search — must get NoIndex, not a panic.
    let err = engine_serving::vector_search(
        &catalog,
        &pool,
        &table,
        "embedding",
        &[1.0_f32, 0.0, 0.0, 0.0],
        1,
    )
    .await
    .expect_err("should be NoIndex error");

    assert!(
        matches!(err, EngineServingError::NoIndex(_)),
        "expected NoIndex, got: {err:?}"
    );
}
