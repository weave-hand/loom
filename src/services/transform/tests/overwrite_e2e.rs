//! Overwrite output mode e2e: a transform with output_mode=overwrite replaces the output
//! table's contents (serving read-back sees only the new result), a second overwrite
//! replaces again, and a read at the pre-overwrite snapshot still time-travels to the old
//! rows. Drives the real queue -> worker -> transform_handler path on the Iceberg control
//! plane; reads output back through `engine_serving`.

mod transform_e2e_support;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{ControlPlane, NewJob, PageReq, Queue};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use control_plane_worker::Worker;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use tokio_util::sync::CancellationToken;
use transform::transform_handler;

use datafusion_io::WriteConfig;
use transform_e2e_support::{col_csv, cols, make_catalog, scalar_i64, seed_table, tref};

/// Run the worker once over the `transform` queue until the job drains. The queue
/// dequeues through `pg`; the handler commits through a fresh Iceberg control plane.
async fn drain_transforms(
    pg: &PgControlPlane,
    fx: &PgFixture,
    db: &str,
    warehouse: &str,
    store: &Arc<dyn ObjectStore>,
    root_url: &str,
) {
    let cp_h: Arc<dyn ControlPlane> = Arc::new(IcebergControlPlane::new(
        pg.clone(),
        make_catalog(fx.pg_dsn(db), warehouse).await,
    ));
    let store_h = store.clone();
    let root_h = root_url.to_string();
    let token = CancellationToken::new();
    let t = token.clone();
    let worker = Worker::new(pg.clone(), "overwrite-test", Duration::from_millis(300))
        .with_poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(async move {
        worker
            .run(&["transform".to_string()], t, move |job| {
                let cp = cp_h.clone();
                let store = store_h.clone();
                let root_url = root_h.clone();
                async move {
                    transform_handler(cp.as_ref(), store, &root_url, &WriteConfig::default(), job)
                        .await
                }
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(800)).await;
    token.cancel();
    handle.await.unwrap().unwrap();
}

/// Enqueue an overwrite transform that selects all rows of `src` into `out`.
async fn enqueue_overwrite(pg: &PgControlPlane, src: &str, out: &str) {
    pg.enqueue(NewJob {
        kind: "transform".into(),
        payload: serde_json::json!({
            "inputs": [{ "schema": "main", "name": src }],
            "output": { "schema": "main", "name": out },
            "sql": format!("SELECT id, label FROM {src}"),
            "output_mode": "overwrite"
        }),
        run_at: None,
        priority: 0,
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_replaces_contents_and_preserves_time_travel() {
    let fx = PgFixture::shared();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let warehouse = wh.path().display().to_string();
    let root_url = format!("file://{warehouse}");
    let catalog = make_catalog(fx.pg_dsn(&db), &warehouse).await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wh.path()).expect("store"));
    let cp = IcebergControlPlane::new(pg.clone(), catalog);

    let cspec = cols(&[("id", "long", false), ("label", "string", true)]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]));

    // Two source tables: src_a (2 rows), src_b (1 row, different labels).
    let src_a = tref("main", "src_a");
    seed_table(
        &cp,
        &store,
        &src_a,
        &cspec,
        schema.clone(),
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a1"), Some("a2")])),
            ],
        )
        .unwrap(),
        "seed-a",
    )
    .await;
    let src_b = tref("main", "src_b");
    seed_table(
        &cp,
        &store,
        &src_b,
        &cspec,
        schema.clone(),
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![9])),
                Arc::new(StringArray::from(vec![Some("b9")])),
            ],
        )
        .unwrap(),
        "seed-b",
    )
    .await;

    // First overwrite: out := src_a (2 rows). (Output table is new -> create + replace.)
    enqueue_overwrite(&pg, "src_a", "out").await;
    drain_transforms(&pg, fx, &db, &warehouse, &store, &root_url).await;
    let out = tref("main", "out");
    let snap_after_a = cp.catalog().current_snapshot(&out).await.unwrap().id;

    let serving = IcebergCatalog::new(fx.pool_for(&db).await);
    let count_a =
        engine_serving::execute_query(&serving, "SELECT count(*) FROM \"main\".\"out\"", None)
            .await
            .expect("count a");
    assert_eq!(
        scalar_i64(&count_a),
        2,
        "first overwrite wrote src_a's rows"
    );
    let labels_a = engine_serving::execute_query(
        &serving,
        "SELECT \"id\", \"label\" FROM \"main\".\"out\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("labels a");
    assert_eq!(col_csv(&labels_a), "a1,a2");

    // Second overwrite: out := src_b (1 row). Replaces, not appends.
    enqueue_overwrite(&pg, "src_b", "out").await;
    drain_transforms(&pg, fx, &db, &warehouse, &store, &root_url).await;
    let serving = IcebergCatalog::new(fx.pool_for(&db).await);
    let count_b =
        engine_serving::execute_query(&serving, "SELECT count(*) FROM \"main\".\"out\"", None)
            .await
            .expect("count b");
    assert_eq!(
        scalar_i64(&count_b),
        1,
        "second overwrite REPLACED (not appended) -> 1 row"
    );
    let labels_b = engine_serving::execute_query(
        &serving,
        "SELECT \"id\", \"label\" FROM \"main\".\"out\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("labels b");
    assert_eq!(col_csv(&labels_b), "b9", "only src_b's row remains live");

    // Time travel: the snapshot after the first overwrite still has src_a's 2 rows. The
    // catalog lists exactly the files live at that snapshot (the b-file is excluded).
    let files_then = cp
        .catalog()
        .files(&out, snap_after_a, PageReq::unbounded())
        .await
        .unwrap();
    let rows_then: i64 = files_then.items.iter().map(|f| f.record_count).sum();
    assert_eq!(
        rows_then, 2,
        "prior snapshot still sees src_a's 2 rows (time travel)"
    );
}
