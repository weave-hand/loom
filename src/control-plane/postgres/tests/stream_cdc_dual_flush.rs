//! `flush_locked`'s CDC dual-write branch: a `kind='cdc'` table's flush advances
//! TWO Iceberg mirror snapshots — the base table (the `+I/+U/-D` subset, never
//! `-U`) and the changelog table (`{name}__changelog`, every emitted event
//! including `-U`) — writing both Parquet files and committing both mirror
//! snapshots on ONE Postgres transaction (`append_batches_on_tx` called twice,
//! then one `tx.commit()`). loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, EventType, Lineage, LineageEvent, PageReq, RunId, StreamTables, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_inline::{
    current_inline_version, inline_append, write_inline_delta,
};
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use control_plane_postgres::read_files_as_batches;
use loom_test_seed::local_sql_catalog;

fn id_spec() -> ColumnSpec {
    ColumnSpec {
        name: "id".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

fn val_spec() -> ColumnSpec {
    ColumnSpec {
        name: "val".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

/// A full one-row `{id, val}` batch (used as the live write and as a before/
/// after image for the CDC update).
fn full_row_batch(id: i64, val: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![val])),
        ],
    )
    .expect("full row batch")
}

/// A one-cell batch holding just the id column (`long`), for
/// `current_inline_version`'s CAS-token lookup.
fn id_batch(v: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).expect("id batch")
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

/// `(loom_change_kind, val, loom_bucket, loom_offset)` for every row across
/// `batches` (a table's flushed Parquet, decoded via `read_files_as_batches`) —
/// the framing + payload columns the assertions below key on.
fn rows(batches: &[RecordBatch]) -> Vec<(String, i64, i32, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let kind_idx = b.schema().index_of("loom_change_kind").expect("kind col");
        let val_idx = b.schema().index_of("val").expect("val col");
        let bucket_idx = b.schema().index_of("loom_bucket").expect("bucket col");
        let offset_idx = b.schema().index_of("loom_offset").expect("offset col");
        let kinds = b
            .column(kind_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("loom_change_kind is a string column");
        let vals = b
            .column(val_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("val is Int64");
        let buckets = b
            .column(bucket_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("loom_bucket is Int32");
        let offsets = b
            .column(offset_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("loom_offset is Int64");
        for i in 0..b.num_rows() {
            out.push((
                kinds.value(i).to_string(),
                vals.value(i),
                buckets.value(i),
                offsets.value(i),
            ));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_flush_dual_writes_base_and_changelog_on_one_tx() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let run = RunId(uuid::Uuid::new_v4());

    let table = TableRef {
        schema: "sales".to_string(),
        name: "orders_dual".to_string(),
    };
    let cols = vec![id_spec(), val_spec()];
    let bucket_count = 2;

    // Declare the table CDC (keyed on `id`) BEFORE any write, exactly as
    // `stream_cdc_emission.rs` does: ensure_table (needs an explicit tx, since
    // its savepoint retry requires one), commit, then declare_cdc.
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(
        tid,
        bucket_count,
        "id",
        control_plane_core::MergeEngine::LastRow,
    )
    .await
    .expect("declare_cdc");

    // Seed id=1, val=100 (a plain +I append) — small enough to inline.
    inline_append(
        &pool,
        &table,
        &cols,
        &full_row_batch(1, 100),
        lin(),
        None,
        None,
    )
    .await
    .expect("seed append");

    // UPDATE id=1: val 100 -> 200. Emits the CDC (-U before-image, +U
    // after-image) pair, same as `stream_cdc_emission.rs`.
    let v0 = current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
        .await
        .expect("version after seed");
    write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(1, 200),
        Some((&cols, &full_row_batch(1, 100))),
        lin(),
        v0,
        None,
    )
    .await
    .expect("cdc update delta");

    // Flush: this is the primitive under test. `flush_locked`'s CDC branch must
    // dual-write the base (+I/+U subset, no -U) and the changelog (all three
    // rows) on ONE Postgres transaction.
    let base_snap = flush_table(&catalog, &pool, &table, run)
        .await
        .expect("flush")
        .expect("flushed something");

    // --- Base table: +I/+U only, -U EXCLUDED ---
    let ice = IcebergCatalog::new(pool.clone());
    let base_files = ice
        .files(&table, base_snap, PageReq::unbounded())
        .await
        .expect("base files");
    let base_paths: Vec<String> = base_files.items.into_iter().map(|f| f.path).collect();
    let (_schema, base_batches) = read_files_as_batches(&catalog, &table, &base_paths)
        .await
        .expect("read base batches");
    let base_rows = rows(&base_batches);

    assert_eq!(
        base_rows.len(),
        2,
        "base holds +I and +U only: {base_rows:?}"
    );
    let base_kinds: Vec<&str> = base_rows.iter().map(|(k, ..)| k.as_str()).collect();
    assert!(
        !base_kinds.contains(&"-U"),
        "base must EXCLUDE -U before-images: {base_rows:?}"
    );
    assert!(
        base_kinds.contains(&"+I"),
        "base keeps the seed +I: {base_rows:?}"
    );
    assert!(
        base_kinds.contains(&"+U"),
        "base keeps the +U after-image: {base_rows:?}"
    );
    let base_plus_i = base_rows.iter().find(|(k, ..)| k == "+I").expect("+I row");
    let base_plus_u = base_rows.iter().find(|(k, ..)| k == "+U").expect("+U row");
    assert_eq!(base_plus_i.1, 100, "base +I keeps the seed value");
    assert_eq!(base_plus_u.1, 200, "base +U keeps the after-image value");

    // --- Changelog table: ALL events, INCLUDING -U ---
    let clog = TableRef {
        schema: table.schema.clone(),
        name: format!("{}__changelog", table.name),
    };
    let clog_snap = ice
        .current_snapshot(&clog)
        .await
        .expect("changelog table advanced a snapshot too")
        .id;
    let clog_files = ice
        .files(&clog, clog_snap, PageReq::unbounded())
        .await
        .expect("changelog files");
    let clog_paths: Vec<String> = clog_files.items.into_iter().map(|f| f.path).collect();
    let (_schema, clog_batches) = read_files_as_batches(&catalog, &clog, &clog_paths)
        .await
        .expect("read changelog batches");
    let clog_rows = rows(&clog_batches);

    assert_eq!(
        clog_rows.len(),
        3,
        "changelog holds the full change sequence (+I, -U, +U): {clog_rows:?}"
    );
    let clog_minus_u = clog_rows
        .iter()
        .find(|(k, ..)| k == "-U")
        .expect("changelog must carry the -U before-image");
    assert_eq!(
        clog_minus_u.1, 100,
        "changelog -U carries the before-image value (100): {clog_rows:?}"
    );
    let clog_plus_u = clog_rows.iter().find(|(k, ..)| k == "+U").expect("+U row");
    assert_eq!(
        clog_plus_u.1, 200,
        "changelog +U carries the after-image value"
    );
    assert!(
        clog_rows.iter().any(|(k, ..)| k == "+I"),
        "changelog also keeps the seed +I: {clog_rows:?}"
    );

    // Both mirrors advanced (both queries above succeeded reading a live
    // snapshot's files — a missing/behind snapshot would have failed or read
    // zero files); offsets stay gapless per bucket across the flush boundary,
    // proven on the changelog's complete sequence (all three rows share id=1's
    // bucket, offsets 0,1,2).
    let mut offsets: Vec<i64> = clog_rows.iter().map(|(.., o)| *o).collect();
    offsets.sort_unstable();
    assert_eq!(
        offsets,
        vec![0, 1, 2],
        "gapless per-bucket offsets: {clog_rows:?}"
    );
    let buckets: std::collections::HashSet<i32> = clog_rows.iter().map(|(_, _, b, _)| *b).collect();
    assert_eq!(
        buckets.len(),
        1,
        "id=1's whole history shares one bucket: {clog_rows:?}"
    );

    // Atomicity is STRUCTURAL (both appends run `append_batches_on_tx` on the
    // same caller-provided `tx`, committed once — see `flush_locked_cdc`), not
    // re-provable by fault injection in this harness. As a same-commit witness,
    // both appends emit the flush's lineage event (same `run_id`) — the base and
    // changelog append both ran under this one `flush_table` call.
    let events = cp
        .events_for(&run, PageReq::unbounded())
        .await
        .expect("lineage events for the flush's run_id");
    assert_eq!(
        events.items.len(),
        2,
        "one lineage emit per append (base + changelog), same run_id: {:?}",
        events.items
    );
}
