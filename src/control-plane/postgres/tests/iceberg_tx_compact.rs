//! IcebergTx::compact_files over the polymorphic ControlPlane seam: stage + commit
//! expires a subset and adds coalesced files at one snapshot, time travel preserved.

use loom_test_seed::local_sql_catalog;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    DataFile, DatasetId, EventType, FileFormat, LineageEvent, RunId, TableControlPlane, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};

fn columns() -> Vec<control_plane_core::ColumnSpec> {
    vec![control_plane_core::ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// `land` now takes pre-decoded batches; build the schema + batch directly
/// rather than round-tripping through an Arrow IPC encode/decode.
fn ipc_body(rows: i64) -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .expect("batch");
    (schema, vec![batch])
}

fn lineage(run: RunId, schema: &str, name: &str) -> LineageEvent {
    let out = TableRef {
        schema: schema.into(),
        name: name.into(),
    };
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&out).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iceberg_tx_compact_files_swaps_subset() {
    let fx = PgFixture::shared();
    let (pgcp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice_cat = IcebergCatalog::new(pool.clone());
    let t = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    let (schema_a, batches_a) = ipc_body(10);
    land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema_a,
        batches_a,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
        None,
    )
    .await
    .expect("a");
    let (schema_b, batches_b) = ipc_body(2);
    let before = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema_b,
        batches_b,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
        None,
    )
    .await
    .expect("b");
    let live = ice_cat.files_with_stats(&t, before).await.expect("live");
    let small = live.iter().min_by_key(|f| f.record_count).unwrap();
    let expire = vec![small.path.clone()];
    let new = vec![DataFile {
        path: format!("{}/wh/t/c/part-0.parquet", wh.path().display()),
        path_is_relative: false,
        file_format: FileFormat::Parquet,
        record_count: small.record_count,
        file_size_bytes: 99,
        column_stats: vec![],
        parquet_footer_size: None,
    }];

    let cp = IcebergControlPlane::new(pgcp, catalog);
    let mut tx = cp.begin_table().await.expect("begin");
    tx.compact_files(&t, &expire, &new).await.expect("stage");
    let snap = tx.commit().await.expect("commit").expect("snapshot");

    let now = ice_cat.files_with_stats(&t, snap).await.expect("now");
    assert_eq!(now.len(), 2, "untouched + coalesced");
    let back = ice_cat.files_with_stats(&t, before).await.expect("back");
    assert_eq!(back.len(), 2, "prior snapshot intact");
}
