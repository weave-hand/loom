//! Engine-level Flight k-NN wire test: boot a `FlightDataService` over a UDS,
//! seed a vector table (Parquet-flushed rows), build a vector index via the
//! `build_vector_index` primitive, then call `FlightTableClient::vector_search`
//! and assert the returned batch's identities are the exact top-k.
//!
//! Also asserts a no-index ticket yields a `not_found`-mapped error over the wire.
//! (CI-only fixture test — boots Postgres; cannot run under `buck2 test //src/...`
//! from a fresh environment without Postgres binaries.)

use loom_test_flight::spawn_flight_uds;
use loom_test_seed::{
    distances_f32, ids_i64, local_sql_catalog, test_lineage, vec4_columns, vec4_ipc,
};
use std::time::Duration;

use control_plane_core::{
    ControlPlane, IndexSpec, Metric, ObjectType, PropertyDef, RunId, TableRef, TypeName,
    VectorIndexDef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::vector_index::build_vector_index;
use engine_wire::flight::{FlightTableClient, VectorSearchTicket};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// k-NN Flight round-trip: seed 4 vectors, build a cosine index, query the
/// engine via `FlightTableClient::vector_search`, assert top-2 identities.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vector_search_flight_top_k() {
    use control_plane_postgres::PgControlPlane;

    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(&db).await;
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_secs(5));

    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    // Register the object type (identity = "id").
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

    // Declare a named cosine flat index on the embedding property.
    cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: "by_flat".into(),
            type_name: TypeName("Docs".into()),
            property: "embedding".into(),
            metric: Metric::Cosine,
            spec: IndexSpec::Flat,
        })
        .await
        .expect("define_vector_index");

    let run = RunId(uuid::Uuid::new_v4());

    // Land rows 1–2 (forced to Parquet: inline_byte_limit = 0).
    let rows_1_2: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &vec4_columns(),
        &vec4_ipc(rows_1_2),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        test_lineage(run, &table),
    )
    .await
    .expect("land rows 1-2");

    // Land rows 3–4.
    let rows_3_4: &[(i64, [f32; 4])] = &[(3, [0.0, 0.0, 1.0, 0.0]), (4, [0.0, 0.0, 0.0, 1.0])];
    land(
        &pool,
        &catalog,
        &table,
        &vec4_columns(),
        &vec4_ipc(rows_3_4),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        test_lineage(run, &table),
    )
    .await
    .expect("land rows 3-4");

    // Build the cosine vector index.
    let build_run = RunId(uuid::Uuid::new_v4());
    build_vector_index(&catalog, &pool, &table, "by_flat", build_run)
        .await
        .expect("build_vector_index");

    // Spawn the Flight server.
    let eng = spawn_flight_uds(fx, &db, &wh.path().display().to_string()).await;
    let client = FlightTableClient::connect(&eng.sock)
        .await
        .expect("connect");

    // Query: k=2, nearest to id=1's embedding [1,0,0,0].
    let batches = client
        .vector_search(VectorSearchTicket {
            schema: "wh".into(),
            name: "docs".into(),
            index_name: "by_flat".into(),
            query: vec![1.0, 0.0, 0.0, 0.0],
            k: 2,
            nprobe: None,
            ef_search: None,
        })
        .await
        .expect("vector_search");

    assert_eq!(batches.len(), 1, "one batch returned");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2, "k=2 rows returned");

    let id_vec = ids_i64(batch);
    assert_eq!(id_vec[0], 1, "nearest is id=1 (cosine, exact match)");

    let dists = distances_f32(batch);
    assert!(dists[0] <= dists[1], "distances are ascending");
}

/// A VectorSearchTicket for a table with no built index must surface as
/// `not_found` over the wire (the engine maps `EngineServingError::NoIndex`
/// to `Status::not_found`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vector_search_no_index_is_not_found() {
    use control_plane_postgres::PgControlPlane;

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
                    ty: "Vector".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .expect("define_type");

    // Land one row but skip build_vector_index.
    let run = RunId(uuid::Uuid::new_v4());
    let rows: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &vec4_columns(),
        &vec4_ipc(rows),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        test_lineage(run, &table),
    )
    .await
    .expect("land row");

    // Spawn the Flight server.
    let eng = spawn_flight_uds(fx, &db, &wh.path().display().to_string()).await;
    let client = FlightTableClient::connect(&eng.sock)
        .await
        .expect("connect");

    // Must get an error (not_found mapped from NoIndex).
    let err = client
        .vector_search(VectorSearchTicket {
            schema: "wh".into(),
            name: "nodocs".into(),
            index_name: "by_flat".into(),
            query: vec![1.0, 0.0, 0.0, 0.0],
            k: 1,
            nprobe: None,
            ef_search: None,
        })
        .await
        .expect_err("missing index");
    assert!(
        matches!(err, engine_wire::flight::VectorSearchError::NoIndex(_)),
        "missing index classifies as NoIndex, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// WIRE PIN — a wrong-dimension query must surface as `invalid_argument`
// (client-side: `VectorSearchError::DimMismatch`). Previously pinned only via
// the in-process engine (query-api's vector_search_e2e), never over the wire.
// Seed prologue cloned from vector_search_flight_top_k (harness item collapses
// these later).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vector_search_dim_mismatch_is_invalid_argument() {
    use control_plane_postgres::PgControlPlane;

    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(&db).await;
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_secs(5));

    let table = TableRef {
        schema: "wh".into(),
        name: "dimdocs".into(),
    };
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("DimDocs".into()),
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
    cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: "by_flat".into(),
            type_name: TypeName("DimDocs".into()),
            property: "embedding".into(),
            metric: Metric::Cosine,
            spec: IndexSpec::Flat,
        })
        .await
        .expect("define_vector_index");

    let run = RunId(uuid::Uuid::new_v4());
    let rows: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &vec4_columns(),
        &vec4_ipc(rows),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        test_lineage(run, &table),
    )
    .await
    .expect("land rows");
    build_vector_index(
        &catalog,
        &pool,
        &table,
        "by_flat",
        RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("build_vector_index");

    let eng = spawn_flight_uds(fx, &db, &wh.path().display().to_string()).await;
    let client = FlightTableClient::connect(&eng.sock)
        .await
        .expect("connect");

    // Index dim is 4; query has length 2 -> DimMismatch (invalid_argument on the
    // wire), NOT NoIndex and NOT an opaque Engine error.
    let err = client
        .vector_search(VectorSearchTicket {
            schema: "wh".into(),
            name: "dimdocs".into(),
            index_name: "by_flat".into(),
            query: vec![1.0, 0.0],
            k: 1,
            nprobe: None,
            ef_search: None,
        })
        .await
        .expect_err("wrong-dim query must be rejected");
    assert!(
        matches!(&err, engine_wire::flight::VectorSearchError::DimMismatch(_)),
        "expected DimMismatch off the invalid_argument status, got: {err:?}"
    );
}
