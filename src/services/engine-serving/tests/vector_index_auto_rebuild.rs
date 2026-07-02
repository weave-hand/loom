//! End-to-end freshness test: a just-flushed vector goes MISSING from k-NN at the
//! post-flush snapshot, then becomes visible again once the auto-enqueued rebuild runs.
//! Proves the slice's user-visible guarantee across the real k-NN read path.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::Int64Array;
use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetId, EventType, IndexSpec, LineageEvent, Metric, ObjectType,
    PropertyDef, RunId, TableRef, TypeName, VectorIndexDef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use control_plane_postgres::vector_index::build_vector_index;
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
    let batch = arrow_array::RecordBatch::try_new(
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
fn ids(batch: &arrow_array::RecordBatch) -> Vec<i64> {
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("identity column is Int64");
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
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
                PropertyDef {
                    name: "embedding".into(),
                    ty: "vector(4)".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
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
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
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
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
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
async fn flushed_vector_is_missing_then_restored_by_auto_rebuild() {
    use control_plane_core::{Metric, RunId, TableRef};
    use control_plane_postgres::iceberg_flush::flush_table;
    use control_plane_postgres::vector_index::build_vector_index;

    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    // 1. Cold rows 1-4 + flat index built at covered_snapshot S.
    let (catalog, pool, _cp, _wh) = seed_and_build(fx, &db, Metric::Cosine).await;

    // 2. Land row 5 INLINE (born after S) — the strictly-nearest vector to the query,
    //    living only in the hot delta. (Mirrors knn_cold_hot_merge_cosine.)
    let run = RunId(uuid::Uuid::new_v4());
    let inline: &[(i64, [f32; 4])] = &[(5, [0.95, 0.05, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(inline),
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage_evt(run, &table),
    )
    .await
    .expect("land inline row 5");

    let q = &[0.9_f32, 0.1, 0.0, 0.0];

    // Sanity (hot-delta merge): row 5 is the nearest and visible BEFORE the flush.
    let hot = engine_serving::vector_search(&catalog, &pool, &table, "by_flat", q, 2, None, None)
        .await
        .expect("knn pre-flush");
    assert_eq!(ids(&hot)[0], 5, "inline row is nearest before flush");

    // 3. Flush: drains row 5 to cold Parquet, end-caps it, AND auto-enqueues a rebuild.
    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");

    // 4. GAP (the bug this slice fixes): row 5 left the hot delta (end-capped) and is not
    //    in the cold index (built at the older S) -> missing from k-NN.
    let gap = engine_serving::vector_search(&catalog, &pool, &table, "by_flat", q, 2, None, None)
        .await
        .expect("knn post-flush");
    assert!(
        !ids(&gap).contains(&5),
        "just-flushed row is in the visibility gap"
    );

    // 5. Drain the auto-enqueued rebuild (simulate the worker): read the enqueued job's
    //    index_name and run the build. The fetch_one FAILS without this slice (no job).
    let index_name: String =
        sqlx::query_scalar("select payload->>'index_name' from queue.jobs where kind = $1")
            .bind(control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND)
            .fetch_one(&pool)
            .await
            .expect("a rebuild job was auto-enqueued by the flush");
    assert_eq!(index_name, "by_flat");
    build_vector_index(
        &catalog,
        &pool,
        &table,
        &index_name,
        RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("rebuild");

    // 6. Fresh again: the rebuilt cold index (covered_snapshot advanced past the flush)
    //    once more makes row 5 the nearest.
    let fresh = engine_serving::vector_search(&catalog, &pool, &table, "by_flat", q, 2, None, None)
        .await
        .expect("knn post-rebuild");
    assert_eq!(ids(&fresh)[0], 5, "auto-rebuild restored the flushed row");
}
