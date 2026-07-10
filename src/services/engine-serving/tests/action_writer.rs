//! The relocated engine-side write executor: build a one-row IPC stream, land it,
//! overwrite it, and truncate it — asserting snapshot ids and committed rows.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    BUILD_VECTOR_INDEX_JOB_KIND, ColumnSpec, ControlPlane, DatasetRef, EventType, IndexSpec,
    LineageEvent, Metric, ObjectType, PropertyDef, RunId, STREAM_CONSOLIDATE_JOB_KIND,
    StreamTables, TableRef, TypeName, VectorIndexDef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
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

/// A one-row, id-only IPC stream — the shape `current_inline_version` expects
/// for its `id_ipc` argument.
fn id_only_ipc(id: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![id]))])
        .expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

fn id_only_cols() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: true,
    }]
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
                // Vector property so tests can declare indexes; landing derives
                // the physical schema from the caller's `columns`, so tests that
                // never write it are unaffected.
                PropertyDef {
                    name: "embedding".into(),
                    ty: "vector(4)".into(),
                    required: false,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
            derived: vec![],
            identity: None,
            version: None,
        })
        .await
        .unwrap();
}

/// Count `queue.jobs` rows with the given kind. Copied from
/// `flush_vector_rebuild.rs`.
async fn job_count(pool: &sqlx::PgPool, kind: &str) -> i64 {
    sqlx::query_scalar::<_, i64>("select count(*)::bigint from queue.jobs where kind = $1")
        .bind(kind)
        .fetch_one(pool)
        .await
        .expect("job_count")
}

/// Count `queue.jobs` rows with the given kind and state. Copied from
/// `flush_vector_rebuild.rs`.
async fn job_count_by_state(pool: &sqlx::PgPool, kind: &str, state: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "select count(*)::bigint from queue.jobs where kind = $1 and state = $2",
    )
    .bind(kind)
    .bind(state)
    .fetch_one(pool)
    .await
    .expect("job_count_by_state")
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
        .write_object(&table, &cols(), &one_row_ipc(1, "a"), event("insert"), &[])
        .await
        .expect("write_object");
    assert!(s1.0 > 0);

    let s2 = writer
        .overwrite_table(&table, &cols(), &one_row_ipc(2, "b"), event("update"), &[])
        .await
        .expect("overwrite_table");
    assert!(s2.0 > s1.0);

    // Empty ipc ⇒ truncate (delete-all).
    let s3 = writer
        .overwrite_table(&table, &[], &[], event("delete"), &[])
        .await
        .expect("truncate");
    assert!(s3.0 > s2.0);
}

/// `overwrite_table` at the engine-serving writer seam enqueues one deduped
/// `build_vector_index` job per index declared on the table's ontology type —
/// pins the wiring proven at the postgres layer
/// (`overwrite_vector_rebuild.rs`) through the writer's delegation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_table_enqueues_declared_index_rebuilds() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = Arc::new(build_catalog(&dsn, warehouse.path()).await);

    e2e_seed_widget_table(&cp).await;
    // Declare TWO vector indexes over the seeded `embedding` property.
    for name in ["by_flat", "by_flat2"] {
        cp.ontology()
            .define_vector_index(VectorIndexDef {
                name: name.into(),
                type_name: TypeName("Widget".into()),
                property: "embedding".into(),
                metric: Metric::Cosine,
                spec: IndexSpec::Flat,
            })
            .await
            .expect("define_vector_index");
    }

    let table = TableRef {
        schema: "main".into(),
        name: "widget".into(),
    };
    // Large inline limit so the single row inlines (no flush job needed).
    let writer = IcebergActionWriter::new(catalog, pool.clone(), 16 * 1024 * 1024, i64::MAX);

    writer
        .write_object(&table, &cols(), &one_row_ipc(1, "a"), event("insert"), &[])
        .await
        .expect("write_object");
    assert_eq!(
        job_count(&pool, BUILD_VECTOR_INDEX_JOB_KIND).await,
        0,
        "inline write must not enqueue rebuilds"
    );

    writer
        .overwrite_table(&table, &cols(), &one_row_ipc(2, "b"), event("update"), &[])
        .await
        .expect("overwrite_table");
    assert_eq!(
        job_count_by_state(&pool, BUILD_VECTOR_INDEX_JOB_KIND, "available").await,
        2,
        "one available rebuild job per declared index"
    );
}

/// The production write seam (`IcebergActionWriter::write_delta`, the exact
/// method `engine::service::EngineControlService::write_delta` — the sole gRPC
/// handler backing query-api's governed CDC mutations — calls) now threads a
/// real `consolidate_delta_threshold` all the way to
/// `iceberg_inline::write_inline_delta`: was hardcoded to `None` (dead
/// machinery), now `Some(self.consolidate_delta_threshold)`. Configure the
/// writer with a small threshold via `with_consolidate_delta_threshold` (the
/// same builder `engine::run` calls with `EngineTuning::consolidate_delta_threshold`)
/// and prove an UPDATE delta (a `(-U, +U)` pair == 2 delta rows) crossing
/// threshold=2 enqueues exactly one `stream_consolidate` job.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_delta_threads_consolidate_threshold_to_production_path() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = Arc::new(build_catalog(&dsn, warehouse.path()).await);

    e2e_seed_widget_table(&cp).await;

    let table = TableRef {
        schema: "main".into(),
        name: "widget".into(),
    };

    // Declare the table CDC (keyed on `id`) BEFORE any write, exactly as
    // `stream_cdc_consolidate_trigger.rs` does.
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, "main", "widget", at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 2, "id", control_plane_core::MergeEngine::LastRow)
        .await
        .expect("declare_cdc");

    let threshold = 2;
    let writer = IcebergActionWriter::new(catalog, pool.clone(), 16 * 1024 * 1024, i64::MAX)
        .with_consolidate_delta_threshold(threshold);

    // Seed id=1 (a plain +I append — not a delta, never touches the trigger).
    writer
        .write_object(&table, &cols(), &one_row_ipc(1, "a"), event("insert"), &[])
        .await
        .expect("seed insert");
    assert_eq!(
        job_count(&pool, STREAM_CONSOLIDATE_JOB_KIND).await,
        0,
        "a plain insert must not enqueue a consolidate"
    );

    let v0 = writer
        .current_inline_version(&table, &id_only_cols(), "id", &id_only_ipc(1))
        .await
        .expect("version after seed");

    // UPDATE id=1 through the SAME production writer/method the engine's
    // `write_delta` RPC handler calls: a (-U, +U) delta pair == 2 rows,
    // crossing threshold=2.
    writer
        .write_delta(
            &table,
            &cols(),
            "id",
            false,
            &one_row_ipc(1, "b"),
            &one_row_ipc(1, "a"),
            &serde_json::to_string(&cols()).expect("before_columns_json"),
            event("update"),
            v0,
            &[],
        )
        .await
        .expect("update delta crossing the threshold");

    assert_eq!(
        job_count(&pool, STREAM_CONSOLIDATE_JOB_KIND).await,
        1,
        "the production write_delta path enqueues stream_consolidate on crossing \
         the threshold now that the writer threads Some(threshold) through"
    );
}
