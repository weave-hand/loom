//! k-NN over the cold Puffin index merged with the hot inline delta. Cold-only
//! (knn_cold_exact_*), no-index error, AND the cold∪hot merge (knn_cold_hot_merge_*):
//! a vector row landed inline AFTER the index's covered snapshot S is found in the
//! hot delta and merged exactly once. Cosine and L2 both verified.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Float32Array, Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetId, EventType, IndexSpec, LineageEvent, Metric, ObjectType,
    PropertyDef, RunId, TableRef, TypeName, VectorIndexDef,
};
use control_plane_postgres::fixture::PgFixture;
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
                    ty: "vector(4)".into(),
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

    // Declare the named flat index (metric supplied by the declaration), then build it.
    cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: "by_flat".into(),
            type_name: TypeName("Docs".into()),
            property: "embedding".into(),
            metric,
            spec: IndexSpec::Flat,
        })
        .await
        .expect("define_vector_index");
    let build_run = RunId(uuid::Uuid::new_v4());
    build_vector_index(&catalog, &pool, &table, "by_flat", build_run)
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
        "by_flat",
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
        "by_flat",
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
                    ty: "vector(4)".into(),
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
        "by_flat",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knn_cold_hot_merge_cosine() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    // Cold rows 1-4 + index built at S (covered_snapshot = S).
    let (catalog, pool, _cp, _wh) = seed_and_build(&fx, &db, Metric::Cosine).await;

    // Land row 5 INLINE (born after S): the unique nearest to the query, living
    // only in the hot delta. inline_byte_limit = usize::MAX forces the inline path.
    let run = RunId(uuid::Uuid::new_v4());
    let inline: &[(i64, [f32; 4])] = &[(5, [0.95, 0.05, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(inline),
        usize::MAX,
        i64::MAX,
        lineage_evt(run, &table),
    )
    .await
    .expect("land inline row 5");

    // Query close to [1,0,0,0]; row 5 is strictly nearer than the cold row 1.
    let batch = engine_serving::vector_search(
        &catalog,
        &pool,
        &table,
        "by_flat",
        &[0.9_f32, 0.1, 0.0, 0.0],
        2,
    )
    .await
    .expect("vector_search cold+hot cosine");

    assert_eq!(batch.num_rows(), 2, "k=2");
    let id_vec = ids(&batch);
    assert_eq!(id_vec[0], 5, "hot inline row is the nearest (no miss)");
    assert_eq!(
        id_vec[1], 1,
        "cold row 1 is second (merge spans both tiers)"
    );
    assert_eq!(
        id_vec.iter().filter(|&&x| x == 5).count(),
        1,
        "inline row counted once"
    );
    let dists = distances(&batch);
    assert!(dists[0] <= dists[1], "distances ascending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knn_cold_hot_merge_l2() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    let (catalog, pool, _cp, _wh) = seed_and_build(&fx, &db, Metric::L2).await;

    let run = RunId(uuid::Uuid::new_v4());
    let inline: &[(i64, [f32; 4])] = &[(5, [0.95, 0.05, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(inline),
        usize::MAX,
        i64::MAX,
        lineage_evt(run, &table),
    )
    .await
    .expect("land inline row 5");

    // L2 nearest to [0.9,0.1,0,0]: row 5 (||·||²=0.005) beats cold row 1 (0.02).
    let batch = engine_serving::vector_search(
        &catalog,
        &pool,
        &table,
        "by_flat",
        &[0.9_f32, 0.1, 0.0, 0.0],
        2,
    )
    .await
    .expect("vector_search cold+hot l2");

    assert_eq!(batch.num_rows(), 2, "k=2");
    let id_vec = ids(&batch);
    assert_eq!(id_vec[0], 5, "hot inline row is the nearest (no miss)");
    assert_eq!(id_vec[1], 1, "cold row 1 is second");
    assert_eq!(
        id_vec.iter().filter(|&&x| x == 5).count(),
        1,
        "inline row counted once"
    );
    let dists = distances(&batch);
    assert!(dists[0] <= dists[1], "distances ascending");
}

/// Like `seed_and_build` but builds an IVF index (nlist=2 over the 4 cold rows).
async fn seed_and_build_ivf(
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
                    ty: "vector(4)".into(),
                    required: true,
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .expect("define_type");

    let run = RunId(uuid::Uuid::new_v4());
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

    cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: "by_ivf".into(),
            type_name: TypeName("Docs".into()),
            property: "embedding".into(),
            metric,
            spec: IndexSpec::IvfFlat { nlist: Some(2) },
        })
        .await
        .expect("define_vector_index");
    let build_run = RunId(uuid::Uuid::new_v4());
    build_vector_index(&catalog, &pool, &table, "by_ivf", build_run)
        .await
        .expect("build ivf");

    (catalog, pool, cp, wh)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivf_cold_search_returns_exact_match() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let (catalog, pool, _cp, _wh) = seed_and_build_ivf(&fx, &db, Metric::Cosine).await;

    // Query id=1's own embedding: its centroid is always probed (nearest), so the
    // exact match is found even though the index is approximate.
    let batch = engine_serving::vector_search(
        &catalog,
        &pool,
        &table,
        "by_ivf",
        &[1.0_f32, 0.0, 0.0, 0.0],
        1,
    )
    .await
    .expect("ivf cold search");
    assert_eq!(ids(&batch)[0], 1, "exact match found via IVF cold index");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivf_hot_delta_row_is_never_pruned_cosine() {
    // The freshness invariant: a row landed inline after S is scored EXACTLY and
    // wins, regardless of IVF cluster pruning on the cold side.
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let (catalog, pool, _cp, _wh) = seed_and_build_ivf(&fx, &db, Metric::Cosine).await;

    // Row 5 inline (born after S): the unique nearest to the query.
    let run = RunId(uuid::Uuid::new_v4());
    let inline: &[(i64, [f32; 4])] = &[(5, [0.95, 0.05, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(inline),
        usize::MAX,
        i64::MAX,
        lineage_evt(run, &table),
    )
    .await
    .expect("land inline row 5");

    let batch = engine_serving::vector_search(
        &catalog,
        &pool,
        &table,
        "by_ivf",
        &[0.9_f32, 0.1, 0.0, 0.0],
        2,
    )
    .await
    .expect("ivf cold+hot search");
    let id_vec = ids(&batch);
    assert_eq!(
        id_vec[0], 5,
        "hot inline row is nearest — never pruned by IVF"
    );
    assert_eq!(
        id_vec.iter().filter(|&&x| x == 5).count(),
        1,
        "counted once"
    );
    let dists = distances(&batch);
    assert!(dists[0] <= dists[1], "ascending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivf_hot_delta_row_is_never_pruned_l2() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let (catalog, pool, _cp, _wh) = seed_and_build_ivf(&fx, &db, Metric::L2).await;

    let run = RunId(uuid::Uuid::new_v4());
    let inline: &[(i64, [f32; 4])] = &[(5, [0.95, 0.05, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(inline),
        usize::MAX,
        i64::MAX,
        lineage_evt(run, &table),
    )
    .await
    .expect("land inline row 5");

    let batch = engine_serving::vector_search(
        &catalog,
        &pool,
        &table,
        "by_ivf",
        &[0.9_f32, 0.1, 0.0, 0.0],
        2,
    )
    .await
    .expect("ivf cold+hot l2");
    let id_vec = ids(&batch);
    assert_eq!(
        id_vec[0], 5,
        "hot inline row is nearest (L2) — never pruned"
    );
    assert_eq!(
        id_vec.iter().filter(|&&x| x == 5).count(),
        1,
        "counted once"
    );
}

/// Like `seed_and_build` but builds an HNSW index (m=None, ef_construction=None → defaults).
async fn seed_and_build_hnsw(
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
                    ty: "vector(4)".into(),
                    required: true,
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .expect("define_type");

    let run = RunId(uuid::Uuid::new_v4());
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

    cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: "by_hnsw".into(),
            type_name: TypeName("Docs".into()),
            property: "embedding".into(),
            metric,
            spec: IndexSpec::Hnsw {
                m: None,
                ef_construction: None,
            },
        })
        .await
        .expect("define_vector_index");
    let build_run = RunId(uuid::Uuid::new_v4());
    build_vector_index(&catalog, &pool, &table, "by_hnsw", build_run)
        .await
        .expect("build hnsw");

    (catalog, pool, cp, wh)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hnsw_cold_hot_merge_counts_fresh_row_once_cosine() {
    // The freshness invariant: a row landed inline after the HNSW cold index's covered
    // snapshot S is scored EXACTLY via the hot path and merged, never dropped by graph
    // approximation.
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let (catalog, pool, _cp, _wh) = seed_and_build_hnsw(&fx, &db, Metric::Cosine).await;

    // Row 5 inline (born after S): the unique nearest to the query, living only in the
    // hot delta — it is NOT in the cold HNSW graph.
    let run = RunId(uuid::Uuid::new_v4());
    let inline: &[(i64, [f32; 4])] = &[(5, [0.95, 0.05, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(inline),
        usize::MAX,
        i64::MAX,
        lineage_evt(run, &table),
    )
    .await
    .expect("land inline row 5");

    // Query close to [1,0,0,0]: row 5 (cosine dist ≈ 0.003) beats cold row 1 (dist ≈ 0.016).
    let batch = engine_serving::vector_search(
        &catalog,
        &pool,
        &table,
        "by_hnsw",
        &[0.9_f32, 0.1, 0.0, 0.0],
        2,
    )
    .await
    .expect("hnsw cold+hot cosine");

    let id_vec = ids(&batch);
    assert_eq!(
        id_vec[0], 5,
        "hot inline row is nearest — never pruned by HNSW graph"
    );
    assert_eq!(
        id_vec.iter().filter(|&&x| x == 5).count(),
        1,
        "fresh row counted exactly once (cold∪hot dedup holds)"
    );
    let dists = distances(&batch);
    assert!(dists[0] <= dists[1], "distances ascending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hnsw_cold_hot_merge_counts_fresh_row_once_l2() {
    // L2 variant of the HNSW freshness invariant.
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let (catalog, pool, _cp, _wh) = seed_and_build_hnsw(&fx, &db, Metric::L2).await;

    let run = RunId(uuid::Uuid::new_v4());
    let inline: &[(i64, [f32; 4])] = &[(5, [0.95, 0.05, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(inline),
        usize::MAX,
        i64::MAX,
        lineage_evt(run, &table),
    )
    .await
    .expect("land inline row 5");

    // L2 nearest to [0.9,0.1,0,0]: row 5 (||·||²=0.005) beats cold row 1 (0.02).
    let batch = engine_serving::vector_search(
        &catalog,
        &pool,
        &table,
        "by_hnsw",
        &[0.9_f32, 0.1, 0.0, 0.0],
        2,
    )
    .await
    .expect("hnsw cold+hot l2");

    let id_vec = ids(&batch);
    assert_eq!(
        id_vec[0], 5,
        "hot inline row is nearest (L2) — never pruned by HNSW graph"
    );
    assert_eq!(
        id_vec.iter().filter(|&&x| x == 5).count(),
        1,
        "fresh row counted exactly once (cold∪hot dedup holds)"
    );
    let dists = distances(&batch);
    assert!(dists[0] <= dists[1], "distances ascending");
}
