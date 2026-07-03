//! Fixture tests: the overwrite/replace commit (`overwrite_parquet_snapshot`)
//! auto-enqueues `build_vector_index` rebuild jobs atomically when the
//! overwritten table has declared vector indexes, deduped against any pending
//! (state='available') job for the same kind+payload — the same contract the
//! flush path pins in `flush_vector_rebuild.rs`, via the shared
//! `rebuild_jobs_for` helper.

use loom_test_seed::{local_sql_catalog, test_lineage, vec4_batches, vec4_columns};

use control_plane_core::{
    BUILD_VECTOR_INDEX_JOB_KIND, ControlPlane, IndexSpec, Metric, ObjectType, PropertyDef, RunId,
    TableRef, TypeName, VectorIndexDef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land, overwrite_parquet_snapshot};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use sqlx::PgPool;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Setup (adapted from flush_vector_rebuild.rs — declares TWO vector indexes)
// ---------------------------------------------------------------------------

/// Boot a fresh db, define the `Docs` object type (id+embedding), optionally declare
/// `by_flat` + `by_flat2` vector indexes (both Flat/Cosine over `embedding`), and
/// return the catalog + pool + table ref + temp dir.
async fn setup(
    fx: &PgFixture,
    with_indexes: bool,
) -> (PgControlPlane, SqlCatalog, PgPool, TableRef, TempDir) {
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
        })
        .await
        .expect("define_type");

    if with_indexes {
        for name in ["by_flat", "by_flat2"] {
            cp.ontology()
                .define_vector_index(VectorIndexDef {
                    name: name.into(),
                    type_name: TypeName("Docs".into()),
                    property: "embedding".into(),
                    metric: Metric::Cosine,
                    spec: IndexSpec::Flat,
                })
                .await
                .expect("define_vector_index");
        }
    }

    (cp, catalog, pool, table, wh)
}

/// Count `queue.jobs` rows with the given kind.
async fn job_count(pool: &PgPool, kind: &str) -> i64 {
    sqlx::query_scalar::<_, i64>("select count(*)::bigint from queue.jobs where kind = $1")
        .bind(kind)
        .fetch_one(pool)
        .await
        .expect("job_count")
}

/// Count `queue.jobs` rows with the given kind and state.
async fn job_count_by_state(pool: &PgPool, kind: &str, state: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "select count(*)::bigint from queue.jobs where kind = $1 and state = $2",
    )
    .bind(kind)
    .bind(state)
    .fetch_one(pool)
    .await
    .expect("job_count_by_state")
}

/// Seed the table with inline rows exactly as flush_vector_rebuild.rs does
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
    )
    .await
    .expect("land");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A non-empty overwrite enqueues exactly one `build_vector_index` job per
/// declared index, atomically with the replace commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_enqueues_one_rebuild_per_declared_index() {
    let fx = PgFixture::shared();
    let (_cp, catalog, pool, table, _wh) = setup(fx, true).await;
    seed(&pool, &catalog, &table).await;
    assert_eq!(
        job_count(&pool, BUILD_VECTOR_INDEX_JOB_KIND).await,
        0,
        "seed must not enqueue"
    );

    let (_schema, batches) = vec4_batches(&[(1, [0.1, 0.2, 0.3, 0.4])]);
    let ev = test_lineage(RunId(uuid::Uuid::new_v4()), &table);
    overwrite_parquet_snapshot(&pool, &catalog, &table, &vec4_columns(), batches, Some(&ev))
        .await
        .expect("overwrite");

    assert_eq!(
        job_count_by_state(&pool, BUILD_VECTOR_INDEX_JOB_KIND, "available").await,
        2,
        "one available rebuild job per declared index"
    );
    // payloads name each declared index exactly once
    let names: Vec<String> = sqlx::query_scalar(
        "select payload->>'index_name' from queue.jobs where kind = $1 order by 1",
    )
    .bind(BUILD_VECTOR_INDEX_JOB_KIND)
    .fetch_all(&pool)
    .await
    .expect("names");
    assert_eq!(names, vec!["by_flat", "by_flat2"]);
}
