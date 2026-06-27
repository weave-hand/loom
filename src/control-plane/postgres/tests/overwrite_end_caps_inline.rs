//! Fixture test: `overwrite_parquet_snapshot` supersedes the inline tier.
//!
//! An inline append followed by `overwrite_parquet_snapshot` must end-cap the
//! stale inline rows so a live read at the new snapshot returns only the replacement
//! data. Time travel to the pre-overwrite snapshot must still return the original
//! inline row.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, EventType, LineageEvent, RunId, SnapshotId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline::inline_append;
use control_plane_postgres::iceberg_landing::overwrite_parquet_snapshot;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

fn inline_batch(ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids.to_vec()))]).expect("batch")
}

fn lineage() -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({}),
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

/// Count inline rows live at a given snapshot for the given table id.
async fn live_inline_count(pool: &sqlx::PgPool, tid: i64, at: SnapshotId) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select count(*) from iceberg_mirror.inline_{tid} \
         where begin_snapshot <= {s} and (end_snapshot is null or end_snapshot > {s})",
        s = at.0,
    )))
    .fetch_one(pool)
    .await
    .expect("live inline count")
}

/// land 1 inline row {id:1} -> S1
/// overwrite with 1 row {id:2} -> S2
/// read live (S2): inline row {id:1} must be end-capped (count = 0)
/// read as-of S1: inline row {id:1} must still be visible (time travel, count = 1)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_end_caps_stale_inline_row() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());

    let t = TableRef {
        schema: "wh".into(),
        name: "things".into(),
    };

    // Step 1: inline-append {id:1} -> S1 (mirror-only, no Parquet).
    let s1 = inline_append(
        &pool,
        &t,
        &columns(),
        &inline_batch(&[1]),
        lineage(),
        None,
    )
    .await
    .expect("inline append id=1");

    // Resolve the mirror table_id for direct inline-table queries.
    let tid: i64 = sqlx::query_scalar(
        "select table_id from iceberg_mirror.table \
         where table_namespace='wh' and table_name='things' and end_snapshot is null",
    )
    .fetch_one(&pool)
    .await
    .expect("tid");

    // Sanity: one inline row live at S1.
    assert_eq!(
        live_inline_count(&pool, tid, s1).await,
        1,
        "one inline row live at S1"
    );

    // Step 2: overwrite with Parquet {id:2} -> S2.
    // This creates the Iceberg table in the catalog (first commit) AND must end-cap
    // the inline row from step 1.
    let s2 = overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &t,
        &columns(),
        vec![inline_batch(&[2])],
        None,
    )
    .await
    .expect("overwrite id=2");
    assert!(s2.0 > s1.0, "overwrite advances snapshot");

    // Live read at S2: the inline row {id:1} must be end-capped.
    assert_eq!(
        live_inline_count(&pool, tid, s2).await,
        0,
        "stale inline row must be end-capped by overwrite (this is the bug we fix)"
    );

    // Time travel to S1: the original inline row {id:1} must still be readable.
    assert_eq!(
        live_inline_count(&pool, tid, s1).await,
        1,
        "original inline row must still be visible at S1 (time travel)"
    );

    // Cross-check via inline_live_batch: no inline rows at S2.
    let inline_at_s2 = ice
        .inline_live_batch(&t, s2)
        .await
        .expect("inline_live_batch at S2");
    assert!(
        inline_at_s2.is_none(),
        "inline_live_batch must return None at S2 (no live inline rows)"
    );

    // Cross-check via inline_live_batch: original row still at S1.
    let inline_at_s1 = ice
        .inline_live_batch(&t, s1)
        .await
        .expect("inline_live_batch at S1");
    let (_tid2, _row_ids, batch_s1) = inline_at_s1
        .expect("inline_live_batch must return Some at S1 (time travel)");
    let ids = batch_s1
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column");
    assert_eq!(ids.value(0), 1, "time travel returns original row id=1");
}
