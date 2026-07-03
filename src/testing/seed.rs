#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::let_underscore_must_use,
    clippy::unused_result_ok,
    clippy::map_err_ignore,
    clippy::unreachable,
    reason = "cross-crate test/fixture harness code, not a production path"
)]
//! Cross-crate vector/wire test seed fixtures (`road-test-wire-harness`).
//!
//! THE shared home for the seed prologue previously hand-rolled per test
//! file across control-plane/postgres, engine-serving, engine, worker, and
//! query-api: the vec4 docs table (`vec4_columns`/`vec4_batches`), the local-fs
//! `SqlCatalog` (`local_sql_catalog` — 25+ byte-identical `make_catalog`
//! copies collapsed), the seed/build composite (`seed_docs_table`/
//! `seed_docs_vector`, built on the #304 `ObjectType::build` DSL), and the
//! k-NN result extractors/asserts (`ids_i64`/`distances_f32`/`assert_knn`).
//!
//! Deliberately divergent seeds (id-kind parametrized, dim-8, width-swept)
//! stay local to their test files — divergence is intent, not duplication.
//! The engine-UDS spawn harness lives in the sibling `loom_test_flight`
//! crate so this one never pulls service deps into control-plane tests.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{BooleanArray, Float32Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetId, EventType, IndexSpec, LineageEvent, Metric, ObjectType,
    RunId, TableRef, TypeName, VectorIndexDef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use control_plane_postgres::vector_index::build_vector_index;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

/// The canonical vec4 landing columns: `id: long` + `embedding: vector(4)`.
pub fn vec4_columns() -> Vec<ColumnSpec> {
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

/// Build a schema + one batch with `id: long` + `embedding: list<float32>`
/// (4 elements). `land` takes pre-decoded batches, so build these directly
/// rather than round-tripping through an Arrow IPC encode/decode.
pub fn vec4_batches(rows: &[(i64, [f32; 4])]) -> (SchemaRef, Vec<RecordBatch>) {
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
    (schema, vec![batch])
}

/// A small, human-legible demo object table: 8 rows of scalar-only columns
/// (`id: long` identity, `name`/`department: string`, `salary: double`,
/// `active: boolean`) whose Arrow types are all in `arrow_logical_type`'s
/// supported set, so ingest's `land_model` infers a clean object type from
/// the schema. Backs the `tools/dev-up.sh` local seed (so a freshly booted
/// stack has something for the object-explorer to render) and any e2e that
/// wants a governed non-vector object to read back.
pub fn employees_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, false),
        Field::new("salary", DataType::Float64, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    let id = Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
    let name = StringArray::from(vec![
        "Ada Lovelace",
        "Alan Turing",
        "Grace Hopper",
        "Katherine Johnson",
        "Dennis Ritchie",
        "Barbara Liskov",
        "Edsger Dijkstra",
        "Radia Perlman",
    ]);
    let department = StringArray::from(vec![
        "Research",
        "Research",
        "Engineering",
        "Mathematics",
        "Engineering",
        "Research",
        "Research",
        "Networking",
    ]);
    let salary = Float64Array::from(vec![
        185_000.0, 192_000.0, 178_000.0, 171_000.0, 180_000.0, 196_000.0, 199_000.0, 175_000.0,
    ]);
    let active = BooleanArray::from(vec![true, true, true, false, true, true, false, true]);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id),
            Arc::new(name),
            Arc::new(department),
            Arc::new(salary),
            Arc::new(active),
        ],
    )
    .expect("employees batch")
}

/// [`employees_batch`] encoded as an Arrow IPC **stream** (what
/// `datafusion_io::decode_ipc`/`land_model` expect on the ingest wire). The
/// `tools/dev-up.sh` emitter writes these bytes to a file and `curl`s them
/// into `POST /models/employees`.
pub fn employees_ipc() -> Vec<u8> {
    let batch = employees_batch();
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).expect("ipc writer");
        writer.write(&batch).expect("ipc write batch");
        writer.finish().expect("ipc finish");
    }
    buf
}

/// A minimal Complete lineage event targeting `table` (payload
/// `{"source": "test"}` — the canonical test payload).
pub fn test_lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

/// Build a local-filesystem `SqlCatalog` over `dsn` with `warehouse` as the
/// `file://` warehouse root — the single home for the tree's 25+
/// byte-identical `make_catalog` copies.
pub async fn local_sql_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
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

/// Downcast the identity column (column 0, `Int64Array`) → `Vec<i64>` in row order.
pub fn ids_i64(batch: &RecordBatch) -> Vec<i64> {
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("identity column is Int64");
    (0..col.len()).map(|i| col.value(i)).collect()
}

/// Downcast column 1 (`Float32Array`, the `_distance` column) → `Vec<f32>` in row order.
pub fn distances_f32(batch: &RecordBatch) -> Vec<f32> {
    let col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("_distance column is Float32");
    (0..col.len()).map(|i| col.value(i)).collect()
}

/// The four shared k-NN result predicates, exactly as the per-file assert
/// blocks stated them: `k` rows returned, `nearest` is first, `nearest`
/// appears exactly once (cold∪hot dedup), distances ascending. Call sites
/// keep any FURTHER local assertions verbatim after this call.
pub fn assert_knn(batch: &RecordBatch, nearest: i64, k: usize) {
    assert_eq!(batch.num_rows(), k, "k rows returned");
    let ids = ids_i64(batch);
    assert_eq!(ids[0], nearest, "nearest id first");
    assert_eq!(
        ids.iter().filter(|&&x| x == nearest).count(),
        1,
        "nearest counted exactly once"
    );
    let dists = distances_f32(batch);
    for w in dists.windows(2) {
        assert!(w[0] <= w[1], "distances ascending");
    }
}

/// `InlineLimits` forcing every landed row to Parquet (the "cold" seed shape).
pub fn cold_limits() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: 0,
        flush_byte_threshold: i64::MAX,
    }
}

/// `InlineLimits` forcing every landed row inline (the "hot delta" shape).
pub fn hot_limits() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: usize::MAX,
        flush_byte_threshold: i64::MAX,
    }
}

/// Everything a seeded vector test needs, by name (the old per-file
/// composites returned an anonymous 4-tuple). `wh` is the warehouse TempDir
/// guard — hold the whole struct alive across search calls.
pub struct VectorSeed {
    pub catalog: SqlCatalog,
    pub pool: sqlx::PgPool,
    pub cp: PgControlPlane,
    pub table: TableRef,
    pub wh: tempfile::TempDir,
}

/// Seed the canonical `wh.docs` world: register the `Docs` type (identity
/// `id`, `embedding vector(4)`, via the #304 seed DSL) and land cold rows
/// 1..=4 (forced to Parquet) in the canonical two batches sharing one run id.
pub async fn seed_docs_table(fx: &PgFixture, db: &str) -> VectorSeed {
    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(db).await;
    let catalog = local_sql_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_secs(5));
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    cp.ontology()
        .define_type(
            ObjectType::build("Docs", ("wh", "docs"))
                .prop_req("id", "Long")
                .prop_req("embedding", "vector(4)")
                .identity("id")
                .done(),
        )
        .await
        .expect("define_type");

    let run = RunId(uuid::Uuid::new_v4());
    let rows_1_2: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    let rows_3_4: &[(i64, [f32; 4])] = &[(3, [0.0, 0.0, 1.0, 0.0]), (4, [0.0, 0.0, 0.0, 1.0])];
    for rows in [rows_1_2, rows_3_4] {
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
        )
        .await
        .expect("land cold rows");
    }

    VectorSeed {
        catalog,
        pool,
        cp,
        table,
        wh,
    }
}

/// `seed_docs_table` + declare the named vector index over `embedding`
/// (metric/spec supplied) and, when `build`, run `build_vector_index` —
/// the full canonical `seed_and_build`/`_ivf`/`_hnsw` composite, index
/// shape now a parameter.
pub async fn seed_docs_vector(
    fx: &PgFixture,
    db: &str,
    index: &str,
    metric: Metric,
    spec: IndexSpec,
    build: bool,
) -> VectorSeed {
    let s = seed_docs_table(fx, db).await;
    s.cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: index.into(),
            type_name: TypeName("Docs".into()),
            property: "embedding".into(),
            metric,
            spec,
        })
        .await
        .expect("define_vector_index");
    if build {
        let build_run = RunId(uuid::Uuid::new_v4());
        build_vector_index(&s.catalog, &s.pool, &s.table, index, build_run)
            .await
            .expect("build_vector_index");
    }
    s
}

/// Land more vec4 rows into a seeded world (fresh run id, canonical lineage) —
/// `hot_limits()` for the classic "row 5 inline after the covered snapshot".
pub async fn land_vec4(s: &VectorSeed, rows: &[(i64, [f32; 4])], limits: InlineLimits) {
    let (schema, batches) = vec4_batches(rows);
    land(
        &s.pool,
        &s.catalog,
        &s.table,
        &vec4_columns(),
        schema,
        batches,
        limits,
        test_lineage(RunId(uuid::Uuid::new_v4()), &s.table),
    )
    .await
    .expect("land vec4 rows");
}
