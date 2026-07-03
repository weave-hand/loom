//! End-to-end (road-vector-column-type): land a dataset with a `vector(4)` column
//! through the real `land` entrypoint over the Iceberg backend, then confirm the mirror
//! decoded the column type as `vector(4)` and the embedding reads back value-exact via
//! the columnar Arrow path (`read_files_as_batches`) — the read path the consumer uses
//! to hydrate an external index. No vector search, no per-object JSON serving.

use loom_test_seed::{local_sql_catalog, vec4_columns};
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Float32Array, Int64Array, ListArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{Catalog, DatasetId, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::read_files_as_batches;

/// Schema + batch: `id: long` + `embedding: list<float>` (non-null element), two
/// rows each holding `width` floats (`width = 4` matches the declared `vector(4)`).
/// `land` now takes pre-decoded batches, so build these directly rather than
/// round-tripping through an Arrow IPC encode/decode.
fn ipc_body(width: usize) -> (Arc<Schema>, Vec<RecordBatch>) {
    let element = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(element.clone());
    let row0: Vec<f32> = (0..width).map(|i| 0.1 * (i + 1) as f32).collect();
    let row1: Vec<f32> = (0..width).map(|i| 0.5 + 0.1 * i as f32).collect();
    lb.values().append_slice(&row0);
    lb.append(true);
    lb.values().append_slice(&row1);
    lb.append(true);
    let embedding = lb.finish();
    let id = Int64Array::from(vec![1i64, 2]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", DataType::List(element), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(id), Arc::new(embedding)])
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
async fn lands_and_reads_back_a_vector_column() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let t = TableRef {
        schema: "wh".into(),
        name: "chunks".into(),
    };
    // limit 0 -> force the real Parquet write path (the inline path is scalar-only).
    let (schema, batches) = ipc_body(4);
    let snap = land(
        &pool,
        &catalog,
        &t,
        &vec4_columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "chunks"),
    )
    .await
    .expect("land vector");

    let ice = IcebergCatalog::new(pool.clone());

    // The mirror decoded the column type back to `vector(4)`.
    let schema = ice.schema(&t, snap).await.expect("schema");
    let emb = schema
        .columns
        .iter()
        .find(|c| c.name == "embedding")
        .expect("embedding column");
    assert_eq!(
        emb.ty, "vector(4)",
        "vector dimension recovered from the mirror"
    );

    // The embedding reads back value-exact via the columnar Arrow path.
    let files = ice.files_with_stats(&t, snap).await.expect("files");
    let paths: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
    let (_s, batches) = read_files_as_batches(&catalog, &t, &paths)
        .await
        .expect("read back");
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2);
    let col = batches[0]
        .column_by_name("embedding")
        .expect("embedding col")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("list array");
    let row0 = col.value(0);
    let f0 = row0
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("float32");
    assert_eq!(
        f0.values(),
        &[0.1f32, 0.2, 0.3, 0.4],
        "embedding floats are value-exact after landing + read-back"
    );
}

/// A vector whose per-row element count ≠ the declared `N` is a deterministic
/// bad-input rejection, not a silent store (acceptance #4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn width_mismatch_is_rejected() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let t = TableRef {
        schema: "wh".into(),
        name: "bad".into(),
    };
    // vec4_columns() declares vector(4) but the data carries 3-element rows.
    let (schema, batches) = ipc_body(3);
    let r = land(
        &pool,
        &catalog,
        &t,
        &vec4_columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "bad"),
    )
    .await;
    assert!(r.is_err(), "a width mismatch must be rejected, not stored");
}
