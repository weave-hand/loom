//! k-NN over the cold Puffin index merged with the hot inline delta. Cold-only
//! (knn_cold_exact_*), no-index error, AND the cold∪hot merge (knn_cold_hot_merge_*):
//! a vector row landed inline AFTER the index's covered snapshot S is found in the
//! hot delta and merged exactly once. Cosine and L2 both verified.

use control_plane_core::{
    ControlPlane, IndexSpec, Metric, ObjectType, PropertyDef, RunId, TableRef, TypeName,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::land;
use engine_serving::{EngineServingError, VectorQuery};
use loom_test_seed::{
    cold_limits, distances_f32, hot_limits, ids_i64, land_vec4, local_sql_catalog,
    seed_docs_vector, test_lineage, vec4_batches, vec4_columns,
};

/// Terse `VectorQuery` builder for the call sites in this file.
fn vq<'a>(
    table: &'a TableRef,
    index_name: &'a str,
    query: &'a [f32],
    k: usize,
    nprobe: Option<u32>,
    ef_search: Option<u32>,
) -> VectorQuery<'a> {
    VectorQuery {
        table,
        index_name,
        query,
        k,
        nprobe,
        ef_search,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knn_cold_exact_cosine() {
    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;

    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    let s = seed_docs_vector(fx, &db, "by_flat", Metric::Cosine, IndexSpec::Flat, true).await;

    // Query: nearest to id=1's embedding [1,0,0,0] with cosine, k=2.
    let batch = engine_serving::vector_search(
        &s.catalog,
        &s.pool,
        vq(&table, "by_flat", &[1.0_f32, 0.0, 0.0, 0.0], 2, None, None),
    )
    .await
    .expect("vector_search cosine");

    assert_eq!(batch.num_rows(), 2, "k=2 rows returned");
    let id_vec = ids_i64(&batch);
    // id=1 is exact match (distance ~ 0); must be first.
    assert_eq!(id_vec[0], 1, "nearest is id=1 (cosine)");
    // Distances ascending.
    let dists = distances_f32(&batch);
    assert!(dists[0] <= dists[1], "distances ascending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knn_cold_exact_l2() {
    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;

    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    let s = seed_docs_vector(fx, &db, "by_flat", Metric::L2, IndexSpec::Flat, true).await;

    // Query: nearest to id=2's embedding [0,1,0,0] with L2, k=2.
    let batch = engine_serving::vector_search(
        &s.catalog,
        &s.pool,
        vq(&table, "by_flat", &[0.0_f32, 1.0, 0.0, 0.0], 2, None, None),
    )
    .await
    .expect("vector_search l2");

    assert_eq!(batch.num_rows(), 2, "k=2 rows returned");
    let id_vec = ids_i64(&batch);
    // id=2 is exact match (L2 distance = 0); must be first.
    assert_eq!(id_vec[0], 2, "nearest is id=2 (L2)");
    // Distances ascending.
    let dists = distances_f32(&batch);
    assert!(dists[0] <= dists[1], "distances ascending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_bound_index_is_deterministic_error() {
    // Seed a table with a vector column but DO NOT build an index; assert
    // vector_search returns Err(EngineServingError::NoIndex(_)), never panics.
    use control_plane_postgres::PgControlPlane;
    use std::time::Duration;

    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;

    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(&db).await;
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
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
            version: None,
        })
        .await
        .expect("define_type");

    // Land one row (Parquet), but skip build_vector_index.
    let run = RunId(uuid::Uuid::new_v4());
    let rows: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0])];
    let (schema, batches) = vec4_batches(rows);
    land(
        &pool,
        &catalog,
        &table,
        &vec4_columns(),
        schema,
        batches,
        cold_limits(),
        test_lineage(run, &table),
        None,
    )
    .await
    .expect("land row");

    // Call vector_search — must get NoIndex, not a panic.
    let err = engine_serving::vector_search(
        &catalog,
        &pool,
        vq(&table, "by_flat", &[1.0_f32, 0.0, 0.0, 0.0], 1, None, None),
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
    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    // Cold rows 1-4 + index built at S (covered_snapshot = S).
    let s = seed_docs_vector(fx, &db, "by_flat", Metric::Cosine, IndexSpec::Flat, true).await;

    // Land row 5 INLINE (born after S): the unique nearest to the query, living
    // only in the hot delta. inline_byte_limit = usize::MAX forces the inline path.
    land_vec4(&s, &[(5, [0.95, 0.05, 0.0, 0.0])], hot_limits()).await;

    // Query close to [1,0,0,0]; row 5 is strictly nearer than the cold row 1.
    let batch = engine_serving::vector_search(
        &s.catalog,
        &s.pool,
        vq(&table, "by_flat", &[0.9_f32, 0.1, 0.0, 0.0], 2, None, None),
    )
    .await
    .expect("vector_search cold+hot cosine");

    assert_eq!(batch.num_rows(), 2, "k=2");
    let id_vec = ids_i64(&batch);
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
    let dists = distances_f32(&batch);
    assert!(dists[0] <= dists[1], "distances ascending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knn_cold_hot_merge_l2() {
    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    let s = seed_docs_vector(fx, &db, "by_flat", Metric::L2, IndexSpec::Flat, true).await;

    land_vec4(&s, &[(5, [0.95, 0.05, 0.0, 0.0])], hot_limits()).await;

    // L2 nearest to [0.9,0.1,0,0]: row 5 (||·||²=0.005) beats cold row 1 (0.02).
    let batch = engine_serving::vector_search(
        &s.catalog,
        &s.pool,
        vq(&table, "by_flat", &[0.9_f32, 0.1, 0.0, 0.0], 2, None, None),
    )
    .await
    .expect("vector_search cold+hot l2");

    assert_eq!(batch.num_rows(), 2, "k=2");
    let id_vec = ids_i64(&batch);
    assert_eq!(id_vec[0], 5, "hot inline row is the nearest (no miss)");
    assert_eq!(id_vec[1], 1, "cold row 1 is second");
    assert_eq!(
        id_vec.iter().filter(|&&x| x == 5).count(),
        1,
        "inline row counted once"
    );
    let dists = distances_f32(&batch);
    assert!(dists[0] <= dists[1], "distances ascending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivf_cold_search_returns_exact_match() {
    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let s = seed_docs_vector(
        fx,
        &db,
        "by_ivf",
        Metric::Cosine,
        IndexSpec::IvfFlat { nlist: Some(2) },
        true,
    )
    .await;

    // Query id=1's own embedding: its centroid is always probed (nearest), so the
    // exact match is found even though the index is approximate.
    let batch = engine_serving::vector_search(
        &s.catalog,
        &s.pool,
        vq(&table, "by_ivf", &[1.0_f32, 0.0, 0.0, 0.0], 1, None, None),
    )
    .await
    .expect("ivf cold search");
    assert_eq!(
        ids_i64(&batch)[0],
        1,
        "exact match found via IVF cold index"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivf_hot_delta_row_is_never_pruned_cosine() {
    // The freshness invariant: a row landed inline after S is scored EXACTLY and
    // wins, regardless of IVF cluster pruning on the cold side.
    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let s = seed_docs_vector(
        fx,
        &db,
        "by_ivf",
        Metric::Cosine,
        IndexSpec::IvfFlat { nlist: Some(2) },
        true,
    )
    .await;

    // Row 5 inline (born after S): the unique nearest to the query.
    land_vec4(&s, &[(5, [0.95, 0.05, 0.0, 0.0])], hot_limits()).await;

    let batch = engine_serving::vector_search(
        &s.catalog,
        &s.pool,
        vq(&table, "by_ivf", &[0.9_f32, 0.1, 0.0, 0.0], 2, None, None),
    )
    .await
    .expect("ivf cold+hot search");
    let id_vec = ids_i64(&batch);
    assert_eq!(
        id_vec[0], 5,
        "hot inline row is nearest — never pruned by IVF"
    );
    assert_eq!(
        id_vec.iter().filter(|&&x| x == 5).count(),
        1,
        "counted once"
    );
    let dists = distances_f32(&batch);
    assert!(dists[0] <= dists[1], "ascending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivf_hot_delta_row_is_never_pruned_l2() {
    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let s = seed_docs_vector(
        fx,
        &db,
        "by_ivf",
        Metric::L2,
        IndexSpec::IvfFlat { nlist: Some(2) },
        true,
    )
    .await;

    land_vec4(&s, &[(5, [0.95, 0.05, 0.0, 0.0])], hot_limits()).await;

    let batch = engine_serving::vector_search(
        &s.catalog,
        &s.pool,
        vq(&table, "by_ivf", &[0.9_f32, 0.1, 0.0, 0.0], 2, None, None),
    )
    .await
    .expect("ivf cold+hot l2");
    let id_vec = ids_i64(&batch);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hnsw_cold_hot_merge_counts_fresh_row_once_cosine() {
    // The freshness invariant: a row landed inline after the HNSW cold index's covered
    // snapshot S is scored EXACTLY via the hot path and merged, never dropped by graph
    // approximation.
    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let s = seed_docs_vector(
        fx,
        &db,
        "by_hnsw",
        Metric::Cosine,
        IndexSpec::Hnsw {
            m: None,
            ef_construction: None,
        },
        true,
    )
    .await;

    // Row 5 inline (born after S): the unique nearest to the query, living only in the
    // hot delta — it is NOT in the cold HNSW graph.
    land_vec4(&s, &[(5, [0.95, 0.05, 0.0, 0.0])], hot_limits()).await;

    // Query close to [1,0,0,0]: row 5 (cosine dist ≈ 0.003) beats cold row 1 (dist ≈ 0.016).
    let batch = engine_serving::vector_search(
        &s.catalog,
        &s.pool,
        vq(&table, "by_hnsw", &[0.9_f32, 0.1, 0.0, 0.0], 2, None, None),
    )
    .await
    .expect("hnsw cold+hot cosine");

    let id_vec = ids_i64(&batch);
    assert_eq!(
        id_vec[0], 5,
        "hot inline row is nearest — never pruned by HNSW graph"
    );
    assert_eq!(
        id_vec.iter().filter(|&&x| x == 5).count(),
        1,
        "fresh row counted exactly once (cold∪hot dedup holds)"
    );
    let dists = distances_f32(&batch);
    assert!(dists[0] <= dists[1], "distances ascending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hnsw_cold_hot_merge_counts_fresh_row_once_l2() {
    // L2 variant of the HNSW freshness invariant.
    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let s = seed_docs_vector(
        fx,
        &db,
        "by_hnsw",
        Metric::L2,
        IndexSpec::Hnsw {
            m: None,
            ef_construction: None,
        },
        true,
    )
    .await;

    land_vec4(&s, &[(5, [0.95, 0.05, 0.0, 0.0])], hot_limits()).await;

    // L2 nearest to [0.9,0.1,0,0]: row 5 (||·||²=0.005) beats cold row 1 (0.02).
    let batch = engine_serving::vector_search(
        &s.catalog,
        &s.pool,
        vq(&table, "by_hnsw", &[0.9_f32, 0.1, 0.0, 0.0], 2, None, None),
    )
    .await
    .expect("hnsw cold+hot l2");

    let id_vec = ids_i64(&batch);
    assert_eq!(
        id_vec[0], 5,
        "hot inline row is nearest (L2) — never pruned by HNSW graph"
    );
    assert_eq!(
        id_vec.iter().filter(|&&x| x == 5).count(),
        1,
        "fresh row counted exactly once (cold∪hot dedup holds)"
    );
    let dists = distances_f32(&batch);
    assert!(dists[0] <= dists[1], "distances ascending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivf_nprobe_full_reproduces_exact_match() {
    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let s = seed_docs_vector(
        fx,
        &db,
        "by_ivf",
        Metric::Cosine,
        IndexSpec::IvfFlat { nlist: Some(2) },
        true,
    )
    .await;

    // nprobe = nlist (2) probes every cluster → the exact nearest is always found.
    let batch = engine_serving::vector_search(
        &s.catalog,
        &s.pool,
        vq(
            &table,
            "by_ivf",
            &[1.0_f32, 0.0, 0.0, 0.0],
            1,
            Some(2),
            None,
        ),
    )
    .await
    .expect("ivf nprobe=nlist");
    assert_eq!(
        ids_i64(&batch)[0],
        1,
        "nprobe=nlist reproduces the exact match"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flat_ignores_both_knobs() {
    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let s = seed_docs_vector(fx, &db, "by_flat", Metric::Cosine, IndexSpec::Flat, true).await;

    // Flat: nprobe/ef_search must not change results.
    let plain = engine_serving::vector_search(
        &s.catalog,
        &s.pool,
        vq(&table, "by_flat", &[1.0_f32, 0.0, 0.0, 0.0], 2, None, None),
    )
    .await
    .expect("flat plain");
    let knobbed = engine_serving::vector_search(
        &s.catalog,
        &s.pool,
        vq(
            &table,
            "by_flat",
            &[1.0_f32, 0.0, 0.0, 0.0],
            2,
            Some(4),
            Some(64),
        ),
    )
    .await
    .expect("flat knobbed");
    assert_eq!(ids_i64(&plain), ids_i64(&knobbed), "Flat ignores knobs");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_dim_mismatch_is_error() {
    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let s = seed_docs_vector(fx, &db, "by_flat", Metric::Cosine, IndexSpec::Flat, true).await;

    // Index dim is 4; a length-3 query must be a deterministic DimMismatch, never a panic.
    let err = engine_serving::vector_search(
        &s.catalog,
        &s.pool,
        vq(&table, "by_flat", &[1.0_f32, 0.0, 0.0], 2, None, None),
    )
    .await
    .expect_err("dim mismatch");
    assert!(
        matches!(err, EngineServingError::DimMismatch(_)),
        "expected DimMismatch, got {err:?}"
    );
}
