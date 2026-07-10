//! The CDC emission heart: on a `kind='cdc'` table, `write_inline_delta` emits the
//! full change sequence rather than a single row. An UPDATE writes an adjacent
//! `(-U before-image, +U after-image)` pair at consecutive per-bucket offsets (`-U`
//! first); a DELETE writes a single `-D` carrying the FULL prior image with
//! `loom_tombstone=true` (not the id-only NULL tombstone the non-CDC path writes).
//! Every emitted row is stamped `loom_bucket = hash(identity) % bucket_count` and a
//! gapless per-bucket offset, and shares the mutation's single allocated snapshot.
//! A current-state merged read reflects the after-image (then, post-delete, no row)
//! and never surfaces the `-U` before-image. loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, EventType, LineageEvent, RunId, SnapshotId, StreamTables, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_inline;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};

fn table() -> TableRef {
    TableRef {
        schema: "sales".to_string(),
        name: "orders".to_string(),
    }
}

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

/// A one-cell batch holding just the id column (`long`).
fn id_batch(v: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).expect("id batch")
}

/// A full one-row `{id, val}` batch (the full property set — used as before/after
/// images).
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

/// `(loom_change_kind, loom_tombstone, val, loom_bucket, loom_offset)` for every
/// live inline row, ordered by offset. `val` is nullable so an id-only tombstone
/// (the non-CDC path) reads back as `None` — which is exactly what the CDC `-D`
/// must NOT do (it carries the full prior image, so `val` is populated).
async fn rows_by_offset(
    pool: &sqlx::PgPool,
    tid: i64,
) -> Vec<(String, bool, Option<i64>, i32, i64)> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "select loom_change_kind, loom_tombstone, \"val\", loom_bucket, loom_offset \
         from iceberg_mirror.inline_{tid} where end_snapshot is null order by loom_offset"
    )))
    .fetch_all(pool)
    .await
    .expect("framing rows readback")
}

/// The single `val` a current-state merged read surfaces for id=1, or `None` if no
/// live row (post-delete).
///
/// `IcebergCatalog::inline_live_batch` is deliberately NOT identity-deduped or
/// tombstone-filtered — it is the raw feed the flush path and vector-index "hot"
/// read consume; the REAL per-identity merge-on-read (greatest-precedence row per
/// identity, tombstoned winner hidden) lives only in
/// `engine-serving::build_merge_view`'s DataFusion `ROW_NUMBER()` window, which
/// this postgres-only crate has no access to. So this helper emulates that same
/// dedup locally, scoped to id=1: among the mvcc-live, non-`-U` rows, `loom_offset`
/// is monotonically issued per mutation (precedence-equivalent to `begin_snapshot`
/// ordering here), so the MAX-offset row is the winner; if it is a tombstone there
/// is no live value.
async fn merged_val(pool: &sqlx::PgPool, tid: i64, at: SnapshotId) -> Option<i64> {
    let row: Option<(i64, bool)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "select \"val\", loom_tombstone from iceberg_mirror.inline_{tid} \
         where \"id\" = 1 and begin_snapshot <= {at} \
           and (end_snapshot is null or end_snapshot > {at}) \
           and (loom_change_kind is null or loom_change_kind <> '-U') \
         order by loom_offset desc limit 1",
        at = at.0,
    )))
    .fetch_optional(pool)
    .await
    .expect("merge-emulation readback");
    let (val, tombstone) = row?;
    if tombstone { None } else { Some(val) }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_update_then_delete_emits_full_change_sequence() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec(), val_spec()];
    let bucket_count = 2;

    // Declare the table CDC (keyed on `id`) BEFORE any write, mirroring
    // stream_cdc_bucket.rs: ensure_table to get the tid, declare_cdc, then seed.
    // `ensure_table`'s savepoint retry requires an explicit transaction
    // (SAVEPOINT is illegal outside one), so begin a tx and commit before
    // declaring.
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

    // Seed the initial live row id=1, val=100 (a plain +I append).
    iceberg_inline::inline_append(
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

    // The +I row records id=1's bucket; every emitted row must share it.
    let seed_rows = rows_by_offset(&pool, tid).await;
    assert_eq!(seed_rows.len(), 1, "one seed row: {seed_rows:?}");
    let (_, _, _, id1_bucket, seed_off) = seed_rows[0];
    assert_eq!(seed_off, 0, "seed +I is the first offset in the bucket");

    let v0 =
        iceberg_inline::current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
            .await
            .expect("version after seed");

    // UPDATE id=1: val 100 -> 200. The before-image is the FULL prior row
    // (id=1,val=100) with its OWN cols+batch; the after-image is the caller's
    // (cols, {id=1,val=200}).
    let before_update = full_row_batch(1, 100);
    let after_update = full_row_batch(1, 200);
    let at_update = iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &after_update,
        Some((&cols, &before_update)),
        lin(),
        v0,
        None,
        &[],
    )
    .await
    .expect("cdc update delta");

    // After the update: exactly a -U(val=100) then +U(val=200) at consecutive
    // offsets in id=1's bucket, -U first, sharing the bucket.
    let rows = rows_by_offset(&pool, tid).await;
    let minus_u = rows
        .iter()
        .find(|(k, ..)| k == "-U")
        .expect("a -U before-image row must exist after a CDC update");
    let plus_u = rows
        .iter()
        .find(|(k, ..)| k == "+U")
        .expect("a +U after-image row must exist after a CDC update");
    assert_eq!(minus_u.2, Some(100), "-U carries the before-image val=100");
    assert_eq!(plus_u.2, Some(200), "+U carries the after-image val=200");
    assert_eq!(minus_u.3, id1_bucket, "-U shares id=1's bucket");
    assert_eq!(plus_u.3, id1_bucket, "+U shares id=1's bucket");
    assert_eq!(
        minus_u.4 + 1,
        plus_u.4,
        "-U/+U at consecutive offsets, -U first: {rows:?}"
    );
    assert!(!minus_u.1, "-U is not a tombstone");
    assert!(!plus_u.1, "+U is not a tombstone");

    // A current-state merged read returns the after-image (val=200), never the -U
    // before-image (val=100).
    assert_eq!(
        merged_val(&pool, tid, at_update).await,
        Some(200),
        "merged read reflects the +U after-image, never the -U"
    );

    // DELETE id=1. Non-CDC delete passes columns=[id]/id_batch; the CDC branch
    // takes the before-image (the full prior row id=1,val=200) for the -D.
    let v1 =
        iceberg_inline::current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
            .await
            .expect("version after update");
    assert_eq!(v1, at_update.0, "CAS token is the +U begin_snapshot");
    let before_delete = full_row_batch(1, 200);
    let at_delete = iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &[id_spec()],
        "id",
        true,
        &id_batch(1),
        Some((&cols, &before_delete)),
        lin(),
        v1,
        None,
        &[],
    )
    .await
    .expect("cdc delete delta");

    // After the delete: a -D carrying the FULL prior image (val=200, NOT NULL) with
    // loom_tombstone=true, at the next offset in the bucket.
    let rows = rows_by_offset(&pool, tid).await;
    let minus_d = rows
        .iter()
        .find(|(k, ..)| k == "-D")
        .expect("a -D row must exist after a CDC delete");
    assert!(minus_d.1, "-D is a tombstone (hides the base row)");
    assert_eq!(
        minus_d.2,
        Some(200),
        "-D carries the FULL prior image (val=200), not the id-only NULL tombstone: {rows:?}"
    );
    assert_eq!(minus_d.3, id1_bucket, "-D shares id=1's bucket");
    assert_eq!(
        minus_d.4,
        plus_u.4 + 1,
        "-D is at the next offset after +U: {rows:?}"
    );

    // Per-bucket offsets are gapless: seed +I (0), -U (1), +U (2), -D (3).
    let mut offs: Vec<i64> = rows
        .iter()
        .filter(|(.., b, _)| *b == id1_bucket)
        .map(|(.., o)| *o)
        .collect();
    offs.sort_unstable();
    assert_eq!(
        offs,
        vec![0, 1, 2, 3],
        "gapless per-bucket offsets: {rows:?}"
    );

    // Post-delete, a current-state merged read returns no live row.
    assert_eq!(
        merged_val(&pool, tid, at_delete).await,
        None,
        "the -D tombstone hides id=1 from current-state reads"
    );
}
