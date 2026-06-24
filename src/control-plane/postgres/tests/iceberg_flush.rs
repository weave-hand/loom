//! Fixture tests for `flush_table`: drains a table's live inline rows into a real
//! Iceberg Parquet snapshot, retires the inline rows at the same snapshot, and emits
//! a compaction lineage event — atomically, exactly-once, time-travel-correct.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, DatasetId, EventType, Lineage, LineageEvent, PageReq, RunId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_inline::inline_append;
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

fn inline_lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
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

/// After flushing an inline-only table: there must be at least one real Parquet file
/// at the current snapshot, and there must be no live inline rows (i.e. the inline
/// rows were end-capped). That pair proves exactly-once delivery at current.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_inline_only_makes_rows_file_backed_exactly_once() {
    let fx = PgFixture::start();
    let (_, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let run = RunId(uuid::Uuid::new_v4());
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    // Land three rows as inline.
    inline_append(
        &pool,
        &table,
        &columns(),
        &inline_batch(&[1, 2, 3]),
        inline_lineage(run, &table),
        None,
    )
    .await
    .expect("inline_append");

    // Flush.
    let snap = flush_table(&catalog, &pool, &table, run)
        .await
        .expect("flush")
        .expect("should have flushed something");

    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice.current_snapshot(&table).await.expect("current");
    assert_eq!(cur.id, snap, "flush returns the new current snapshot");

    // A real Parquet file now exists.
    let files = ice
        .files(&table, cur.id, PageReq::unbounded())
        .await
        .expect("files");
    assert!(
        !files.items.is_empty(),
        "at least one Parquet file at current"
    );
    // The flushed file(s) carry exactly the 3 inline rows — none lost, none duplicated.
    let flushed_rows: i64 = files.items.iter().map(|f| f.record_count).sum();
    assert_eq!(
        flushed_rows, 3,
        "flushed Parquet holds exactly the inline rows"
    );

    // No live inline rows remain — they were end-capped at the flush snapshot.
    let inline = ice
        .inline_live_batch(&table, cur.id)
        .await
        .expect("inline_live_batch");
    assert!(inline.is_none(), "inline rows retired at current snapshot");
}

/// Flushing a table with no live inline rows is a no-op: returns Ok(None), no new
/// snapshot or data file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_with_no_live_rows_is_a_noop() {
    let fx = PgFixture::start();
    let (_, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let run = RunId(uuid::Uuid::new_v4());
    let table = TableRef {
        schema: "wh".into(),
        name: "empty".into(),
    };

    // No inline_append — no inline rows ever. Flush must be a no-op.
    let result = flush_table(&catalog, &pool, &table, run)
        .await
        .expect("flush noop");
    assert!(result.is_none(), "no inline rows -> Ok(None)");
}

/// After flushing, `events_for(run)` must include exactly one compaction lineage
/// event where inputs[0].name == outputs[0].name == "wh.t" (the table's dataset ref).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_emits_compaction_lineage() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let inline_run = RunId(uuid::Uuid::new_v4());
    let flush_run = RunId(uuid::Uuid::new_v4());
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    inline_append(
        &pool,
        &table,
        &columns(),
        &inline_batch(&[10]),
        inline_lineage(inline_run, &table),
        None,
    )
    .await
    .expect("inline_append");

    flush_table(&catalog, &pool, &table, flush_run)
        .await
        .expect("flush")
        .expect("flushed");

    // The flush run emits exactly one compaction event.
    let events = cp
        .events_for(&flush_run, PageReq::unbounded())
        .await
        .expect("events");
    assert_eq!(events.items.len(), 1, "one compaction event");

    let ev = &events.items[0];
    assert_eq!(ev.inputs.len(), 1);
    assert_eq!(ev.outputs.len(), 1);
    // loom dataset ref for "wh"."t" is "wh.t"
    assert_eq!(ev.inputs[0].name, "wh.t");
    assert_eq!(ev.outputs[0].name, "wh.t");
}

/// Time-travel correctness: the snapshot before the flush still sees the rows as
/// live inline, while the flush snapshot sees them as a Parquet file and no inline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_preserves_time_travel() {
    let fx = PgFixture::start();
    let (_, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let run = RunId(uuid::Uuid::new_v4());
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    // Append inline to get snap0.
    let snap0 = inline_append(
        &pool,
        &table,
        &columns(),
        &inline_batch(&[1, 2]),
        inline_lineage(run, &table),
        None,
    )
    .await
    .expect("inline_append");

    // Flush: snap1 > snap0.
    let snap1 = flush_table(&catalog, &pool, &table, run)
        .await
        .expect("flush")
        .expect("flushed");
    assert!(snap1.0 > snap0.0, "flush snapshot is newer");

    let ice = IcebergCatalog::new(pool.clone());

    // At snap0: inline rows are still live (the end-cap is at snap1, not snap0).
    let inline_at_snap0 = ice
        .inline_live_batch(&table, snap0)
        .await
        .expect("inline @ snap0");
    assert!(
        inline_at_snap0.is_some(),
        "rows still live inline at pre-flush snapshot"
    );

    // At snap0: the flushed data file did NOT exist yet (begin_snapshot == snap1 > snap0).
    let files_at_snap0 = ice
        .files(&table, snap0, PageReq::unbounded())
        .await
        .expect("files @ snap0");
    assert!(
        files_at_snap0.items.is_empty(),
        "no Parquet file at pre-flush snapshot"
    );

    // At snap1: the Parquet file is visible.
    let files_at_snap1 = ice
        .files(&table, snap1, PageReq::unbounded())
        .await
        .expect("files @ snap1");
    assert!(
        !files_at_snap1.items.is_empty(),
        "Parquet file visible at flush snapshot"
    );

    // At snap1: no live inline rows (they were end-capped at snap1).
    let inline_at_snap1 = ice
        .inline_live_batch(&table, snap1)
        .await
        .expect("inline @ snap1");
    assert!(
        inline_at_snap1.is_none(),
        "inline rows retired at flush snapshot"
    );
}

/// Concurrent inline writes: rows appended AFTER the flush remain live inline.
/// A second flush drains those too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_leaves_later_inline_rows_live() {
    let fx = PgFixture::start();
    let (_, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let run = RunId(uuid::Uuid::new_v4());
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    // Append row A.
    inline_append(
        &pool,
        &table,
        &columns(),
        &inline_batch(&[1]),
        inline_lineage(run, &table),
        None,
    )
    .await
    .expect("inline A");

    // Flush A.
    flush_table(&catalog, &pool, &table, run)
        .await
        .expect("flush 1")
        .expect("flushed 1");

    // Append row B (after the flush).
    inline_append(
        &pool,
        &table,
        &columns(),
        &inline_batch(&[2]),
        inline_lineage(run, &table),
        None,
    )
    .await
    .expect("inline B");

    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice.current_snapshot(&table).await.expect("current after B");

    // B is still live inline.
    let inline_after_b = ice
        .inline_live_batch(&table, cur.id)
        .await
        .expect("inline after B");
    assert!(
        inline_after_b.is_some(),
        "B is still live inline after first flush"
    );

    // Second flush drains B.
    flush_table(&catalog, &pool, &table, run)
        .await
        .expect("flush 2")
        .expect("flushed 2");

    let cur2 = ice
        .current_snapshot(&table)
        .await
        .expect("current after flush 2");
    let inline_after_flush2 = ice
        .inline_live_batch(&table, cur2.id)
        .await
        .expect("inline after second flush");
    assert!(
        inline_after_flush2.is_none(),
        "no live inline rows after second flush"
    );
}
