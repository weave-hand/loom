//! Fixture tests: flush auto-enqueues `build_vector_index` rebuild jobs atomically
//! when the flushed table has declared vector indexes, deduped against any pending
//! (state='available') job for the same kind+payload.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetId, EventType, IndexSpec, LineageEvent, Metric, ObjectType,
    PropertyDef, RunId, TableRef, TypeName, VectorIndexDef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use sqlx::PgPool;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers (copied from vector_index_build.rs)
// ---------------------------------------------------------------------------

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

fn ipc_body(rows: &[(i64, [f32; 4])]) -> Vec<u8> {
    let element = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(element.clone());
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    for (_, emb) in rows {
        lb.values().append_slice(emb);
        lb.append(true);
    }
    let id_array = Int64Array::from(ids);
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

fn lineage(run: RunId, table: &TableRef) -> LineageEvent {
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

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

/// Boot a fresh db, define the `Docs` object type (id+embedding), optionally declare
/// a `by_flat` vector index, and return the catalog + pool + table ref + temp dir.
async fn setup(
    fx: &PgFixture,
    with_index: bool,
) -> (PgControlPlane, SqlCatalog, PgPool, TableRef, TempDir) {
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
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

    if with_index {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Flush auto-enqueues exactly one `build_vector_index` job per declared index.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_enqueues_one_build_job_per_declared_index() {
    let fx = PgFixture::shared();
    let (_cp, catalog, pool, table, _wh) = setup(fx, true).await;

    let run = RunId(uuid::Uuid::new_v4());
    let rows: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];

    // Land inline (usize::MAX forces the inline path for any size).
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(rows),
        usize::MAX,
        i64::MAX,
        lineage(run, &table),
    )
    .await
    .expect("land");

    // No job yet — flush hasn't run.
    assert_eq!(
        job_count(&pool, control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND).await,
        0,
        "no job before flush"
    );

    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");

    assert_eq!(
        job_count(&pool, control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND).await,
        1,
        "one rebuild job enqueued after flush"
    );
}

/// Flush on a table with no declared vector index enqueues nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_without_declared_index_enqueues_nothing() {
    let fx = PgFixture::shared();
    let (_cp, catalog, pool, table, _wh) = setup(fx, false).await;

    let run = RunId(uuid::Uuid::new_v4());
    let rows: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0])];

    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(rows),
        usize::MAX,
        i64::MAX,
        lineage(run, &table),
    )
    .await
    .expect("land");

    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");

    assert_eq!(
        job_count(&pool, control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND).await,
        0,
        "no job when no index declared"
    );
}

/// No-op flush (no live inline rows) enqueues nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noop_flush_enqueues_nothing() {
    let fx = PgFixture::shared();
    // Create a CP with the timeout for this test
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
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

    // Flush with no inline rows → returns None, no job.
    let result = flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    assert!(result.is_none(), "noop flush returns None");

    assert_eq!(
        job_count(&pool, control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND).await,
        0,
        "no job on noop flush"
    );
}

/// Two flushes while the build job is still pending (state='available') deduplicate
/// to a single enqueued job.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_flushes_with_pending_build_enqueue_one() {
    let fx = PgFixture::shared();
    let (_cp, catalog, pool, table, _wh) = setup(fx, true).await;
    let run = RunId(uuid::Uuid::new_v4());

    // Land + flush → 1 available job.
    let rows1: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(rows1),
        usize::MAX,
        i64::MAX,
        lineage(run, &table),
    )
    .await
    .expect("land 1");
    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush 1");
    assert_eq!(
        job_count(&pool, control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND).await,
        1,
        "one job after first flush"
    );

    // Land more rows + second flush while job is still available → dedup, still 1 job.
    let rows2: &[(i64, [f32; 4])] = &[(2, [0.0, 1.0, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(rows2),
        usize::MAX,
        i64::MAX,
        lineage(run, &table),
    )
    .await
    .expect("land 2");
    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush 2");

    assert_eq!(
        job_count(&pool, control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND).await,
        1,
        "dedup: still one job after second flush with pending job"
    );
}

/// Flush while the build job is running (not available) enqueues a fresh pending job.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_while_build_running_enqueues_a_fresh_pending() {
    let fx = PgFixture::shared();
    let (_cp, catalog, pool, table, _wh) = setup(fx, true).await;
    let run = RunId(uuid::Uuid::new_v4());

    // Land + flush → 1 available job.
    let rows1: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(rows1),
        usize::MAX,
        i64::MAX,
        lineage(run, &table),
    )
    .await
    .expect("land 1");
    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush 1");

    // Simulate the worker picking up the job (move to 'running').
    sqlx::query("update queue.jobs set state='running', locked_by='test-worker', locked_at=now(), attempts=1, updated_at=now() where kind=$1")
        .bind(control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND)
        .execute(&pool)
        .await
        .expect("move job to running");

    // Land more rows + flush again → job is running, not available → new job enqueued.
    let rows2: &[(i64, [f32; 4])] = &[(2, [0.0, 1.0, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(rows2),
        usize::MAX,
        i64::MAX,
        lineage(run, &table),
    )
    .await
    .expect("land 2");
    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush 2");

    let available = job_count_by_state(
        &pool,
        control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND,
        "available",
    )
    .await;
    let running = job_count_by_state(
        &pool,
        control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND,
        "running",
    )
    .await;
    assert_eq!(running, 1, "one running job");
    assert_eq!(available, 1, "one fresh available job after second flush");
}
