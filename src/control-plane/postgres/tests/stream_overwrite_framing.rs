//! `overwrite_parquet_snapshot` must preserve framing when overwriting a declared
//! stream table: today it hardcodes `include_framing=false`, so an overwrite of a
//! CDC/stream base silently drops `loom_change_kind`/`loom_bucket`/`loom_offset`
//! from the physical schema even though the table stays declared as a stream in
//! the registry — a schema divergence future flushes/reads would trip over. A
//! batch (non-stream) table's overwrite must stay byte-identical (no framing).
//! loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, EventType, LineageEvent, RunId, StreamTables, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_inline::inline_append;
use control_plane_postgres::iceberg_landing::overwrite_parquet_snapshot;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use loom_test_seed::local_sql_catalog;

fn id_spec() -> ColumnSpec {
    ColumnSpec {
        name: "id".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

fn id_batch(ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids.to_vec()))]).expect("batch")
}

/// A replacement batch shaped like the physical layout of a declared stream
/// table: user column `id` followed by the three reserved framing columns, in
/// the fixed order `augment_with_framing` builds (change_kind, bucket, offset).
fn framed_batch(ids: &[i64], bucket: i32, start_offset: i64) -> RecordBatch {
    let n = ids.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("loom_change_kind", DataType::Utf8, false),
        Field::new("loom_bucket", DataType::Int32, true),
        Field::new("loom_offset", DataType::Int64, true),
    ]));
    let offsets: Vec<i64> = (0..n as i64).map(|i| start_offset + i).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(vec!["+I"; n])),
            Arc::new(Int32Array::from(vec![bucket; n])),
            Arc::new(Int64Array::from(offsets)),
        ],
    )
    .expect("framed batch")
}

fn lin() -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "test" }),
    }
}

async fn tid_of(pool: &sqlx::PgPool, schema: &str, name: &str) -> i64 {
    sqlx::query_scalar(
        "select table_id from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
    )
    .bind(schema)
    .bind(name)
    .fetch_one(pool)
    .await
    .expect("table_id")
}

/// Overwriting a declared CDC/stream base must PRESERVE the physical framing
/// columns — the bug this task fixes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_overwrite_preserves_framing() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let table = TableRef {
        schema: "s".to_string(),
        name: "base".to_string(),
    };
    let cols = vec![id_spec()];

    // Declare the table CDC (bucket_count=1) BEFORE any write, mirroring
    // stream_cdc_emission.rs: ensure_table for the tid, then declare_cdc.
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 1, "id").await.expect("declare_cdc");

    // Insert + flush so the base holds framed file rows.
    inline_append(
        &pool,
        &table,
        &cols,
        &id_batch(&[1, 2, 3]),
        lin(),
        None,
        None,
    )
    .await
    .expect("seed append");
    let run = RunId(uuid::Uuid::new_v4());
    flush_table(&catalog, &pool, &table, run)
        .await
        .expect("flush")
        .expect("flushed something");

    let ice = IcebergCatalog::new(pool.clone());

    // Overwrite the base with a fresh framed batch (user col + framing cols).
    let at_after = overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &table,
        &cols,
        vec![framed_batch(&[4, 5], 0, 0)],
        Some(&lin()),
    )
    .await
    .expect("overwrite");

    let phys: Vec<_> = ice
        .physical_columns(tid, at_after)
        .await
        .expect("physical_columns")
        .into_iter()
        .map(|c| c.name)
        .collect();
    for c in ["loom_change_kind", "loom_bucket", "loom_offset"] {
        assert!(
            phys.contains(&c.to_string()),
            "overwrite of a declared stream table must preserve framing: {phys:?}"
        );
    }
}

/// A NON-stream (batch) table's overwrite has NO framing columns — unchanged
/// behaviour (byte-identical to before this task).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_overwrite_has_no_framing() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let table = TableRef {
        schema: "s".to_string(),
        name: "plain".to_string(),
    };
    let cols = vec![id_spec()];

    inline_append(
        &pool,
        &table,
        &cols,
        &id_batch(&[1, 2, 3]),
        lin(),
        None,
        None,
    )
    .await
    .expect("seed append");
    let run = RunId(uuid::Uuid::new_v4());
    flush_table(&catalog, &pool, &table, run)
        .await
        .expect("flush")
        .expect("flushed something");

    let ice = IcebergCatalog::new(pool.clone());
    let tid = tid_of(&pool, &table.schema, &table.name).await;

    let at_after = overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &table,
        &cols,
        vec![id_batch(&[4, 5])],
        Some(&lin()),
    )
    .await
    .expect("overwrite");

    let phys: Vec<_> = ice
        .physical_columns(tid, at_after)
        .await
        .expect("physical_columns")
        .into_iter()
        .map(|c| c.name)
        .collect();
    for c in ["loom_change_kind", "loom_bucket", "loom_offset"] {
        assert!(
            !phys.contains(&c.to_string()),
            "batch-table overwrite must have NO framing: {phys:?}"
        );
    }
}
