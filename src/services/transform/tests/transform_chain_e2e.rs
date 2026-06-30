//! Transform-chain e2e: a transform that reads ANOTHER transform's output, whose
//! mirror files carry ABSOLUTE `file://` paths (promoted by `absolute_data_files`).
//! This is the one path that failed before scan_table became scheme-aware
//! (iss-iceberg-transform-chain-path): scan_table would prepend `loom://data/...`
//! to an already-absolute path and register no store under `file://`.

mod transform_e2e_support;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use datafusion_io::WriteConfig;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;

use transform_e2e_support::{cols, lineage, make_catalog, scalar_i64, seed_table, tref};

#[tokio::test(flavor = "multi_thread")]
async fn transform_reads_transform_output_with_absolute_paths() {
    let fx = PgFixture::start();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let warehouse = wh.path().display().to_string();
    let root_url = format!("file://{warehouse}");
    let catalog = make_catalog(fx.pg_dsn(&db), &warehouse).await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wh.path()).expect("store"));
    let cp = IcebergControlPlane::new(pg, catalog);

    // 1. Seed a base input table with RELATIVE paths (the shape a landed table has).
    let base = tref("src", "base");
    let base_cols = cols(&[("id", "long", false), ("name", "string", true)]);
    let base_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    seed_table(
        &cp,
        &store,
        &base,
        &base_cols,
        base_schema.clone(),
        RecordBatch::try_new(
            base_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .unwrap(),
        "seed-base",
    )
    .await;

    // 2. Transform A: src.base -> stage.mid. run_transform commits stage.mid's files
    //    with ABSOLUTE file:// paths (via absolute_data_files in run.rs).
    let mid = tref("stage", "mid");
    transform::run_transform(
        &cp,
        store.clone(),
        &root_url,
        &WriteConfig::default(),
        "run-a",
        transform::TransformRequest {
            inputs: &[transform::TransformInput {
                table: &base,
                register_as: "base",
            }],
            output: &mid,
            sql: "SELECT id, name FROM base",
            conform: None,
            output_mode: transform::OutputMode::Append,
            lineage: lineage(&mid),
        },
    )
    .await
    .expect("transform A lands stage.mid with absolute file:// paths");

    // 3. Transform B: reads stage.mid (ABSOLUTE paths!) -> dst.out. This is the call
    //    that errored before scan_table became scheme-aware.
    let out = tref("dst", "out");
    transform::run_transform(
        &cp,
        store.clone(),
        &root_url,
        &WriteConfig::default(),
        "run-b",
        transform::TransformRequest {
            inputs: &[transform::TransformInput {
                table: &mid,
                register_as: "mid",
            }],
            output: &out,
            sql: "SELECT count(*) AS n FROM mid",
            conform: None,
            output_mode: transform::OutputMode::Append,
            lineage: lineage(&out),
        },
    )
    .await
    .expect("transform B resolves transform A's absolute-path files");

    // 4. Read dst.out back through the serving engine; B counted A's 2 rows.
    let serving = IcebergCatalog::new(fx.pool_for(&db).await);
    let n = engine_serving::execute_query(&serving, "SELECT \"n\" FROM \"dst\".\"out\"", None)
        .await
        .expect("serving read");
    assert_eq!(scalar_i64(&n), 2, "transform B read transform A's 2 rows");
}
