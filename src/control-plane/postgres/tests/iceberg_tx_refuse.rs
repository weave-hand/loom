//! Site 2: `IcebergTx::commit` refuses when a staged-files target is a declared
//! stream table, rolling back (no snapshot, no files). A batch target commits as
//! before.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlaneError, DataFile, EventType, LineageEvent, PageReq, RunId,
    StreamTables, TableControlPlane, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use iceberg::Catalog as IceCatalogTrait;
use iceberg::spec::{NestedField, PrimitiveType, Schema as IceSchema, Type};
use iceberg::{NamespaceIdent, TableCreation};
use loom_test_seed::local_sql_catalog;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}
fn cols() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iceberg_tx_commit_refuses_stream_target() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog =
        Arc::new(local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await);
    let pool = fx.pool_for(&db).await;

    let streamt = tref("s", "tx_stream_out");

    // Declare the output a log stream BEFORE the transform commit.
    let mut tx = pool.begin().await.expect("begin");
    let at = next_snapshot(&mut tx, None).await.expect("snap");
    let tid = ensure_table(&mut tx, &streamt.schema, &streamt.name, at)
        .await
        .expect("ensure");
    tx.commit().await.expect("commit");
    cp.declare_stream(tid, 4).await.expect("declare_stream");

    // Create the real Iceberg physical table: `write_object_data_files` requires it
    // to already exist (normally done by `ensure_iceberg_table`, which stays
    // `pub(crate)`), so build it directly via the raw iceberg `Catalog` trait,
    // matching the field-id schema `ice_schema(cols())` would produce.
    let ns = NamespaceIdent::new(streamt.schema.clone());
    catalog
        .create_namespace(&ns, HashMap::new())
        .await
        .expect("ns");
    let ice_schema = IceSchema::builder()
        .with_fields([Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .expect("schema");
    let creation = TableCreation::builder()
        .name(streamt.name.clone())
        .schema(ice_schema)
        .build();
    catalog
        .create_table(&ns, creation)
        .await
        .expect("create table");

    // Build a DataFile the staging seam can register. Write one real Parquet file
    // to the warehouse via the same helper the landing path uses.
    let batch = {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).expect("b")
    };
    let files: Vec<DataFile> = control_plane_postgres::iceberg_landing::write_object_data_files(
        &catalog,
        &streamt,
        &cols(),
        vec![batch],
    )
    .await
    .expect("write files");

    // Stage create + append against the stream target, then commit → refused.
    // `PgControlPlane` is `Clone`; the engine builds this exactly as below (service.rs:301).
    let icp = IcebergControlPlane::new(cp.clone(), catalog.clone());
    let mut txn = icp.begin_table().await.expect("begin_table");
    txn.create_table(&streamt, &cols()).await.expect("create");
    txn.append_files(&streamt, &files)
        .await
        .expect("stage append");
    let lineage = LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "test" }),
    };
    txn.emit(lineage).await.expect("emit");
    let err = txn
        .commit()
        .await
        .expect_err("stream target must be refused");
    assert!(
        matches!(err, ControlPlaneError::Validation(_)),
        "got {err:?}"
    );
    assert!(
        err.to_string().contains("stream-table target refused:"),
        "msg: {err}"
    );

    // Rolled back: the stream table HAS a live snapshot (from declare_stream) but no
    // data files were registered.
    let ice = IcebergCatalog::new(pool.clone());
    let snap = ice.current_snapshot(&streamt).await.expect("snap");
    let live = ice
        .files(&streamt, snap.id, PageReq::unbounded())
        .await
        .expect("files");
    assert!(
        live.items.is_empty(),
        "no files after refused commit, got {}",
        live.items.len()
    );
}
