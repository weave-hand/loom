//! Build primitive: land a typed object with an `embedding: vector(4)` column
//! (all flushed to Parquet since the inline path does not support vector columns),
//! run the build as of S, assert a Puffin sidecar exists and a vector_index mirror
//! row + lineage event were written in one tx, covering all rows live at S.

use loom_test_seed::{local_sql_catalog, test_lineage, vec4_columns, vec4_batches};

use control_plane_core::{
    Catalog, ControlPlane, IndexSpec, Metric, ObjectType, PageReq, PropertyDef, RunId, TableRef,
    TypeName, VectorIndex, VectorIndexDef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::puffin::read_flat_index;
use control_plane_postgres::vector_index::{build_vector_index, lookup_vector_index};
use iceberg::io::FileIO;

/// Build covers all rows live at S, writes a Puffin sidecar, inserts a
/// vector_index mirror row, and emits exactly one lineage event with output
/// namespace "loom-vector-index".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_covers_all_rows_live_at_s() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    // 1. Register the object type with the ontology (identity = "id").
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

    // 1b. Declare a named flat index on the embedding property.
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

    // 2. Land rows 1..=4.  Vector columns can't inline, so we force Parquet
    //    with limit 0 for all batches.
    let run = RunId(uuid::Uuid::new_v4());
    let rows_1_2: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    let rows_3_4: &[(i64, [f32; 4])] = &[(3, [0.0, 0.0, 1.0, 0.0]), (4, [0.0, 0.0, 0.0, 1.0])];
    // First batch: rows 1-2 (limit 0 forces Parquet).
    let (schema_1_2, batches_1_2) = vec4_batches(rows_1_2);
    land(
        &pool,
        &catalog,
        &table,
        &vec4_columns(),
        schema_1_2,
        batches_1_2,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        test_lineage(run, &table),
    )
    .await
    .expect("land rows 1-2");

    // Second batch: rows 3-4 (also Parquet).
    let (schema_3_4, batches_3_4) = vec4_batches(rows_3_4);
    land(
        &pool,
        &catalog,
        &table,
        &vec4_columns(),
        schema_3_4,
        batches_3_4,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        test_lineage(run, &table),
    )
    .await
    .expect("land rows 3-4");

    // 3. Resolve table_id for lookup assertions.
    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice
        .current_snapshot(&table)
        .await
        .expect("current snapshot");
    let files = ice
        .files_with_stats(&table, cur.id)
        .await
        .expect("files_with_stats");
    // We should have at least 2 files (one per land call).
    assert!(!files.is_empty(), "at least one Parquet file exists");

    // 4. Run build_vector_index.
    let build_run = RunId(uuid::Uuid::new_v4());
    let built = build_vector_index(&catalog, &pool, &table, "by_flat", build_run)
        .await
        .expect("build_vector_index");

    // 5. Assert row_count covers all 4 rows.
    assert_eq!(built.row_count, 4, "all 4 rows covered");
    assert!(built.covered_snapshot > 0, "covered snapshot is set");

    // 6. lookup_vector_index returns the row.
    // Resolve table_id from mirror.
    let mut conn = pool.acquire().await.expect("acquire");
    let table_id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
        "select table_id from iceberg_mirror.\"table\" \
         where table_namespace = 'wh' and table_name = 'docs' and end_snapshot is null",
    ))
    .fetch_one(&mut *conn)
    .await
    .expect("table_id");

    let found = lookup_vector_index(&pool, table_id, "by_flat", built.covered_snapshot)
        .await
        .expect("lookup")
        .expect("should be Some");
    assert_eq!(found.row_count, 4);
    assert_eq!(found.metric, "cosine");
    assert_eq!(found.dim, 4);
    assert_eq!(found.puffin_path, built.puffin_path);

    // 7. read_flat_index searches correctly.
    let file_io = FileIO::new_with_fs();
    // The puffin_path starts with "file://" for local FS. FileIO::new_with_fs
    // expects a path without the scheme for the flat index roundtrip test.
    // Use the path directly.
    let index = read_flat_index(&file_io, &built.puffin_path)
        .await
        .expect("read_flat_index");
    assert_eq!(index.dim(), 4);
    assert_eq!(index.row_count(), 4);
    // Query with [1,0,0,0] -> nearest should be id=1.
    let results = index.search(&[1.0, 0.0, 0.0, 0.0], 1);
    assert!(!results.is_empty(), "search returned a result");
    assert_eq!(
        results[0].0,
        control_plane_core::VectorKey::Int(1),
        "nearest to [1,0,0,0] is id=1"
    );

    // 8. Exactly one lineage event with output namespace "loom-vector-index".
    let events = cp
        .lineage()
        .events_for(&build_run, PageReq::unbounded())
        .await
        .expect("events_for");
    assert_eq!(events.items.len(), 1, "exactly one lineage event");
    assert_eq!(
        events.items[0].outputs[0].namespace, "loom-vector-index",
        "output namespace is loom-vector-index"
    );
    assert_eq!(
        events.items[0].outputs[0].name, built.puffin_path,
        "output name is the puffin path"
    );

    // The input node must be the CANONICAL loom dataset ref (same node the
    // landing/flush paths emit), not an ad-hoc {schema, name} pair — otherwise
    // the index-build edge is disconnected from the table's lineage graph.
    // Guards iss-vector-build-lineage-ref.
    assert_eq!(
        events.items[0].inputs,
        vec![control_plane_core::DatasetRef::from(&table)],
        "input is the canonical loom dataset ref for the source table"
    );
}
