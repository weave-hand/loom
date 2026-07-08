//! Flush suppression for shadow-bearing tables: once a table carries inline shadow
//! deltas (a `write_inline_delta` mutation has landed), the byte-trigger flush must
//! never drain it — flushing a row-version/tombstone into Parquet would duplicate or
//! resurrect a file row. loom_fixture_test (Postgres).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, DatasetId, EventType, LineageEvent, PageReq, RunId, TableRef,
};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_inline::{self, inline_append};
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

fn table() -> TableRef {
    TableRef {
        schema: "wh".to_string(),
        name: "t".to_string(),
    }
}

fn id_spec() -> ColumnSpec {
    ColumnSpec {
        name: "id".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

fn qty_spec() -> ColumnSpec {
    ColumnSpec {
        name: "qty".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

/// A one-cell batch holding just the id column (`long`).
fn id_batch(v: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).expect("id batch")
}

/// A full one-row {id, qty} batch (the post-PATCH row a version delta carries).
fn full_row_batch(id: i64, qty: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("qty", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![qty])),
        ],
    )
    .expect("full row batch")
}

fn lineage(run: RunId, table: &TableRef) -> LineageEvent {
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

/// Resolve the internal inline table id the same way the other inline tests do.
async fn tid_of(pool: &sqlx::PgPool, table: &TableRef) -> i64 {
    sqlx::query_scalar(
        "select table_id from iceberg_mirror.\"table\" \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
    )
    .bind(&table.schema)
    .bind(&table.name)
    .fetch_one(pool)
    .await
    .expect("table_id")
}

/// Once a mutation (`write_inline_delta`) has landed for a table, the byte-trigger
/// flush must be suppressed: flushing the raw (never end-capped) inline delta rows
/// into Parquet would duplicate or resurrect a file row. Seeds a FILE-only object
/// (the primary copy-on-write target — mutations target an existing object), then
/// mutates it via a version delta. `flush_table` must return `Ok(None)`: no new
/// snapshot, no new Parquet file, and the mutated row must still read back correctly
/// (no duplicate, no resurrection to the pre-mutation value).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_is_suppressed_after_a_mutation() {
    let fx = PgFixture::shared();
    let (_, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let run = RunId(uuid::Uuid::new_v4());
    let table = table();

    // Land a real Parquet file {id:1, qty:1} — a file-only object (no inline storage
    // yet), which is the primary copy-on-write target.
    let seed_cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("qty".to_string(), "long".to_string(), false),
    ];
    let writer = IcebergWriter::new(pool.clone(), dsn);
    writer
        .seed_arrays(
            &table.schema,
            &table.name,
            &seed_cols,
            &[SeedCol::Long(vec![1]), SeedCol::Long(vec![1])],
        )
        .await;

    let cols = vec![id_spec(), qty_spec()];
    let v0 =
        iceberg_inline::current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
            .await
            .expect("current version");
    assert_eq!(
        v0, 0,
        "a freshly-seeded file-only object has inline version 0"
    );

    // Mutate: write a VERSION delta {id:1, qty:9}. This flags has_shadow.
    iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(1, 9),
        None,
        lineage(run, &table),
        v0,
        None,
    )
    .await
    .expect("version delta");

    let tid = tid_of(&pool, &table).await;
    let mut conn = pool.acquire().await.expect("acquire");
    assert!(
        iceberg_inline::has_shadow(&mut conn, tid)
            .await
            .expect("has_shadow"),
        "a mutated table must be flagged as carrying a shadow"
    );
    drop(conn);

    // Use the SAME catalog/warehouse the seed wrote into, so a (would-be) flush
    // writes/reads the real physical location.
    let catalog = writer.sql_catalog().await;
    let ice = IcebergCatalog::new(pool.clone());

    let before = ice
        .current_snapshot(&table)
        .await
        .expect("current before flush");
    let files_before = ice
        .files(&table, before.id, PageReq::unbounded())
        .await
        .expect("files before flush");
    let (_, row_ids_before, batch_before) = ice
        .inline_live_batch(&table, before.id)
        .await
        .expect("inline_live_batch before flush")
        .expect("the mutation's inline row is live before the flush attempt");
    assert_eq!(
        row_ids_before.len(),
        1,
        "one live inline row: the version delta"
    );

    // The byte-trigger flush must be suppressed: Ok(None), no corruption.
    let result = flush_table(&catalog, &pool, &table, run)
        .await
        .expect("suppressed flush must not error");
    assert!(
        result.is_none(),
        "flush must be suppressed while the table is shadowed"
    );

    // No new snapshot or Parquet file was produced by the suppressed flush.
    let after = ice
        .current_snapshot(&table)
        .await
        .expect("current after flush");
    assert_eq!(
        after.id, before.id,
        "a suppressed flush allocates no new snapshot"
    );
    let files_after = ice
        .files(&table, after.id, PageReq::unbounded())
        .await
        .expect("files after flush");
    assert_eq!(
        files_after.items.len(),
        files_before.items.len(),
        "a suppressed flush must not write any new Parquet file"
    );

    // Read-back: the live inline row is untouched — nothing end-capped (no
    // duplicate written to Parquet), and it still carries the mutated value.
    let (_, row_ids_after, batch_after) = ice
        .inline_live_batch(&table, after.id)
        .await
        .expect("inline_live_batch after flush")
        .expect("the inline row is still live (nothing was flushed)");
    assert_eq!(
        row_ids_after, row_ids_before,
        "no inline rows were end-capped by the suppressed flush"
    );

    for batch in [&batch_before, &batch_after] {
        let qty_col = batch.schema().index_of("qty").expect("qty column");
        let qty = batch
            .column(qty_col)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("qty is Int64Array");
        assert_eq!(
            qty.value(0),
            9,
            "reads the mutated value, not resurrected to the pre-mutation 1"
        );
    }
}

/// A table that has taken NO mutation (`has_shadow == false`) must flush normally
/// via the byte-trigger path — the suppression guard must not over-fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_only_table_still_flushes() {
    let fx = PgFixture::shared();
    let (_, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let run = RunId(uuid::Uuid::new_v4());
    let table = table();
    let cols = vec![id_spec(), qty_spec()];

    inline_append(
        &pool,
        &table,
        &cols,
        &full_row_batch(1, 1),
        lineage(run, &table),
        None,
        None,
    )
    .await
    .expect("seed append");

    let tid = tid_of(&pool, &table).await;
    let mut conn = pool.acquire().await.expect("acquire");
    assert!(
        !iceberg_inline::has_shadow(&mut conn, tid)
            .await
            .expect("has_shadow"),
        "an append-only table must not be flagged as shadowed"
    );
    drop(conn);

    let result = flush_table(&catalog, &pool, &table, run)
        .await
        .expect("flush");
    assert!(
        result.is_some(),
        "an unshadowed table must drain normally via flush_table"
    );

    // No live inline rows remain — the flush actually ran, not a suppressed no-op.
    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice.current_snapshot(&table).await.expect("current");
    let inline = ice
        .inline_live_batch(&table, cur.id)
        .await
        .expect("inline_live_batch");
    assert!(
        inline.is_none(),
        "inline rows were end-capped by the real flush"
    );
}
