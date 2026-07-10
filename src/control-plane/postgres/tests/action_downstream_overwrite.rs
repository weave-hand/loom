//! Fixture test: `overwrite_parquet_snapshot` MERGES an action's declared
//! `downstream` jobs with the vector-index `rebuild_jobs` it already enqueues
//! (`overwrite_vector_rebuild.rs` pins the rebuild-only contract) — the merge
//! must never clobber one set with the other. Exercised directly at the
//! postgres-fixture level since no action currently routes through
//! `overwrite_table` (mutate goes through `write_delta`); this defensively
//! proves the wiring for a future overwrite-based caller.

use loom_test_seed::{local_sql_catalog, test_lineage, vec4_batches, vec4_columns};

use control_plane_core::{
    BUILD_VECTOR_INDEX_JOB_KIND, ControlPlane, IndexSpec, Metric, NewJob, ObjectType, PropertyDef,
    RunId, TRANSFORM_JOB_KIND, TableRef, TypeName, VectorIndexDef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land, overwrite_parquet_snapshot};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use serde_json::json;
use sqlx::PgPool;
use tempfile::TempDir;

/// Boot a fresh db, define the `Docs` object type (id+embedding), declare one
/// vector index over `embedding` (Flat/Cosine), and return the catalog + pool
/// + table ref + temp dir. Mirrors `overwrite_vector_rebuild.rs`'s `setup`.
async fn setup(fx: &PgFixture) -> (PgControlPlane, SqlCatalog, PgPool, TableRef, TempDir) {
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
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

    (cp, catalog, pool, table, wh)
}

/// Seed the table with inline rows exactly as `overwrite_vector_rebuild.rs` does
/// (usize::MAX forces the inline path for any size).
async fn seed(pool: &PgPool, catalog: &SqlCatalog, table: &TableRef) {
    let run = RunId(uuid::Uuid::new_v4());
    let rows: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    let (schema, batches) = vec4_batches(rows);
    land(
        pool,
        catalog,
        table,
        &vec4_columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        test_lineage(run, table),
        None,
    )
    .await
    .expect("land");
}

/// Count `queue.jobs` rows with the given kind.
async fn job_count(pool: &PgPool, kind: &str) -> i64 {
    sqlx::query_scalar::<_, i64>("select count(*)::bigint from queue.jobs where kind = $1")
        .bind(kind)
        .fetch_one(pool)
        .await
        .expect("job_count")
}

/// An overwrite carrying an action's `downstream` job MUST enqueue BOTH that
/// job AND the table's declared vector-index rebuild job — the merge added by
/// this task, proven against the clobber it defends against (a naive
/// `jobs: &rebuild_jobs` would silently drop the action's downstream job).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_merges_downstream_job_with_vector_rebuild() {
    let fx = PgFixture::shared();
    let (_cp, catalog, pool, table, _wh) = setup(fx).await;
    seed(&pool, &catalog, &table).await;
    assert_eq!(
        job_count(&pool, BUILD_VECTOR_INDEX_JOB_KIND).await,
        0,
        "seed must not enqueue"
    );
    assert_eq!(
        job_count(&pool, TRANSFORM_JOB_KIND).await,
        0,
        "seed must not enqueue a transform job either"
    );

    let downstream_job = NewJob {
        kind: TRANSFORM_JOB_KIND.into(),
        payload: json!({ "note": "action downstream" }),
        run_at: None,
        priority: 0,
    };

    let (_schema, batches) = vec4_batches(&[(1, [0.1, 0.2, 0.3, 0.4])]);
    let ev = test_lineage(RunId(uuid::Uuid::new_v4()), &table);
    overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &table,
        &vec4_columns(),
        batches,
        Some(&ev),
        &[downstream_job],
    )
    .await
    .expect("overwrite");

    assert_eq!(
        job_count(&pool, BUILD_VECTOR_INDEX_JOB_KIND).await,
        1,
        "the declared index's rebuild job must still enqueue — merge, not clobber"
    );
    assert_eq!(
        job_count(&pool, TRANSFORM_JOB_KIND).await,
        1,
        "the action's downstream job must enqueue alongside the rebuild job"
    );
}
