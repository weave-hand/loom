//! Verifies the universal log-framing columns (`loom_change_kind`, `loom_bucket`,
//! `loom_offset`) on the inline tier: an `inline_append` row defaults to `'+I'`
//! with NULL bucket/offset, a `write_inline_delta` tombstone stamps `'-D'`, and a
//! version delta stamps `'+U'`. loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, ControlPlaneError, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_inline;

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

fn qty_spec() -> ColumnSpec {
    ColumnSpec {
        name: "qty".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

/// A one-cell batch holding just the id column (`long`).
fn id_batch(name: &str, v: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).expect("id batch")
}

/// A multi-row batch holding just the id column (`long`), one row per value.
fn id_batch_n(name: &str, vs: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vs.to_vec()))]).expect("id batch")
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

/// Resolve the internal inline table id the same way the other inline tests do.
async fn tid_of(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar(
        "select table_id from iceberg_mirror.table \
         where table_namespace='sales' and table_name='orders' and end_snapshot is null",
    )
    .fetch_one(pool)
    .await
    .expect("table_id")
}

/// Read back `(loom_change_kind, loom_bucket, loom_offset)` for the highest-
/// version live row matching `id_column = id_value` in `inline_<tid>`. Delta rows
/// are never end-capped (the merge-on-read lets the highest `begin_snapshot`
/// win), so an identity can have more than one live row after a delta write —
/// `order by begin_snapshot desc limit 1` picks the winner deterministically.
async fn framing_cols(
    pool: &sqlx::PgPool,
    tid: i64,
    id_column: &str,
    id_value: i64,
) -> (String, Option<i32>, Option<i64>) {
    let row: (String, Option<i32>, Option<i64>) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "select loom_change_kind, loom_bucket, loom_offset from iceberg_mirror.inline_{tid} \
         where \"{id_column}\" = {id_value} and end_snapshot is null \
         order by begin_snapshot desc limit 1"
    )))
    .fetch_one(pool)
    .await
    .expect("framing columns readback");
    row
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_row_is_plus_i_with_null_bucket_offset() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec()];

    iceberg_inline::inline_append(&pool, &table, &cols, &id_batch("id", 1), lin(), None, None)
        .await
        .expect("inline append succeeds");

    let tid = tid_of(&pool).await;
    let (kind, bucket, offset) = framing_cols(&pool, tid, "id", 1).await;
    assert_eq!(kind, "+I", "a plain append defaults loom_change_kind to +I");
    assert!(
        bucket.is_none() && offset.is_none(),
        "a batch-table append has no bucket/offset yet (Task 3 stamps those): got bucket={bucket:?} offset={offset:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tombstone_delta_is_minus_d() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec()];

    // Seed one row {id:1}.
    iceberg_inline::inline_append(&pool, &table, &cols, &id_batch("id", 1), lin(), None, None)
        .await
        .expect("seed append");

    let v0 = iceberg_inline::current_inline_version(
        &pool,
        &table,
        &[id_spec()],
        "id",
        &id_batch("id", 1),
    )
    .await
    .expect("current version id=1");

    iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &[id_spec()],
        "id",
        true,
        &id_batch("id", 1),
        lin(),
        v0,
    )
    .await
    .expect("tombstone delta");

    let tid = tid_of(&pool).await;
    let (kind, bucket, offset) = framing_cols(&pool, tid, "id", 1).await;
    assert_eq!(kind, "-D", "a tombstone delta stamps loom_change_kind = -D");
    assert!(
        bucket.is_none() && offset.is_none(),
        "a delta row must never stamp bucket/offset: got bucket={bucket:?} offset={offset:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn version_delta_is_plus_u() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec(), qty_spec()];

    // Seed the inline table via one append {id:1, qty:1}.
    iceberg_inline::inline_append(
        &pool,
        &table,
        &cols,
        &full_row_batch(1, 1),
        lin(),
        None,
        None,
    )
    .await
    .expect("seed append");

    let v0 = iceberg_inline::current_inline_version(
        &pool,
        &table,
        &[id_spec()],
        "id",
        &id_batch("id", 1),
    )
    .await
    .expect("current version id=1");

    // Write a VERSION delta {id:1, qty:9}.
    iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(1, 9),
        lin(),
        v0,
    )
    .await
    .expect("version delta");

    let tid = tid_of(&pool).await;
    let (kind, bucket, offset) = framing_cols(&pool, tid, "id", 1).await;
    assert_eq!(kind, "+U", "a version delta stamps loom_change_kind = +U");
    assert!(
        bucket.is_none() && offset.is_none(),
        "a delta row must never stamp bucket/offset: got bucket={bucket:?} offset={offset:?}"
    );
}

/// Read back every live row's `(loom_bucket, loom_offset)` for `tid`, ordered by
/// `loom_row_id` (i.e. insertion order) — the append-order view Task 3's bucket
/// assignment (`row_index % bucket_count`) is checked against.
async fn bucket_offset_rows(pool: &sqlx::PgPool, tid: i64) -> Vec<(Option<i32>, Option<i64>)> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "select loom_bucket, loom_offset from iceberg_mirror.inline_{tid} order by loom_row_id"
    )))
    .fetch_all(pool)
    .await
    .expect("bucket/offset readback")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_append_stamps_gapless_per_bucket_offsets() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec()];

    // First append declares the table as a stream with 2 buckets.
    let batch4 = id_batch_n("id", &[1, 2, 3, 4]);
    iceberg_inline::inline_append(&pool, &table, &cols, &batch4, lin(), None, Some(2))
        .await
        .expect("append");

    let tid = tid_of(&pool).await;
    let rows = bucket_offset_rows(&pool, tid).await;
    let buckets: Vec<Option<i32>> = rows.iter().map(|(b, _)| *b).collect();
    let offsets: Vec<Option<i64>> = rows.iter().map(|(_, o)| *o).collect();
    assert_eq!(
        buckets,
        vec![Some(0), Some(1), Some(0), Some(1)],
        "rows 0,2 -> bucket 0; rows 1,3 -> bucket 1"
    );
    assert_eq!(
        offsets,
        vec![Some(0), Some(0), Some(1), Some(1)],
        "each bucket's offsets start at 0 and are gapless"
    );

    // Second append of 2 more rows continues each bucket's offsets: `None`
    // omits the stream declaration, so it appends using the recorded mode.
    let batch2 = id_batch_n("id", &[5, 6]);
    iceberg_inline::inline_append(&pool, &table, &cols, &batch2, lin(), None, None)
        .await
        .expect("append 2");

    let rows = bucket_offset_rows(&pool, tid).await;
    let bucket0_offsets: Vec<i64> = rows
        .iter()
        .filter(|(b, _)| *b == Some(0))
        .filter_map(|(_, o)| *o)
        .collect();
    let bucket1_offsets: Vec<i64> = rows
        .iter()
        .filter(|(b, _)| *b == Some(1))
        .filter_map(|(_, o)| *o)
        .collect();
    assert_eq!(
        bucket0_offsets,
        vec![0, 1, 2],
        "bucket 0's offsets are gapless across appends"
    );
    assert_eq!(
        bucket1_offsets,
        vec![0, 1, 2],
        "bucket 1's offsets are gapless across appends"
    );

    // Conflicting bucket count is rejected.
    let batch1 = id_batch_n("id", &[7]);
    let err =
        iceberg_inline::inline_append(&pool, &table, &cols, &batch1, lin(), None, Some(3)).await;
    assert!(
        matches!(err, Err(ControlPlaneError::Conflict(_))),
        "bucket count mismatch is a Conflict, got {err:?}"
    );

    // A non-stream table (no `Some`) stamps NULL bucket/offset.
    let other = TableRef {
        schema: "sales".to_string(),
        name: "line_items".to_string(),
    };
    iceberg_inline::inline_append(&pool, &other, &cols, &id_batch("id", 1), lin(), None, None)
        .await
        .expect("non-stream append");
    let other_tid: i64 = sqlx::query_scalar(
        "select table_id from iceberg_mirror.table \
         where table_namespace='sales' and table_name='line_items' and end_snapshot is null",
    )
    .fetch_one(&pool)
    .await
    .expect("other table_id");
    let other_rows = bucket_offset_rows(&pool, other_tid).await;
    assert_eq!(
        other_rows,
        vec![(None, None)],
        "a non-stream table's append leaves bucket/offset NULL"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_append_rejects_non_positive_bucket_count() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec()];
    let batch1 = id_batch_n("id", &[1]);

    let err =
        iceberg_inline::inline_append(&pool, &table, &cols, &batch1, lin(), None, Some(0)).await;
    assert!(
        matches!(err, Err(ControlPlaneError::Validation(_))),
        "bucket_count 0 must be rejected as Validation, not panic: got {err:?}"
    );
}

/// Concurrent first-declare of a brand-new table with DIFFERING bucket counts must
/// not desync: exactly one writer's count wins and gets recorded in
/// `stream.stream_table`, and the OTHER writer must fail rather than silently
/// stamping rows against a bucket count the recorded table doesn't admit.
///
/// Both writers observe `stream_bucket_count == None` (the table is brand new), so
/// both call `pg_declare_stream` — an `INSERT ... ON CONFLICT (table_id) DO NOTHING`.
/// One of them wins the insert; the other's insert is a no-op. Without re-reading
/// the stored count after the declare, the loser would proceed with its OWN
/// requested count and stamp `loom_bucket = row % <its count>`, producing bucket
/// values the committed `stream_table.bucket_count` (the winner's count) does not
/// admit — the desync this test guards against. The fix re-reads the recorded
/// count post-declare and returns `Conflict` if it disagrees with the request.
///
/// Under this specific brand-new-table race, the two writers ALSO race
/// `ensure_table`'s creation of the `iceberg_mirror.table` row itself — but that
/// race is now resolved by a savepoint retry (the loser absorbs the winner's
/// `table_id` instead of surfacing a raw `23505`), so both writers ALWAYS reach
/// the declare with the SAME resolved `table_id`. The loser is therefore
/// deterministically rejected at the stream-declare stage fixed here
/// (`Conflict`). Pin down the invariant that actually matters: no stamped
/// `loom_bucket` ever exceeds the recorded `stream_table.bucket_count`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_declare_with_differing_counts_does_not_desync() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec()];

    let pool_a = pool.clone();
    let table_a = table.clone();
    let cols_a = cols.clone();
    let batch_a = id_batch_n("id", &[1, 2]);
    let handle_a = tokio::spawn(async move {
        iceberg_inline::inline_append(&pool_a, &table_a, &cols_a, &batch_a, lin(), None, Some(2))
            .await
    });

    let pool_b = pool.clone();
    let table_b = table.clone();
    let cols_b = cols.clone();
    let batch_b = id_batch_n("id", &[3, 4, 5]);
    let handle_b = tokio::spawn(async move {
        iceberg_inline::inline_append(&pool_b, &table_b, &cols_b, &batch_b, lin(), None, Some(3))
            .await
    });

    let result_a = handle_a.await.expect("task a join");
    let result_b = handle_b.await.expect("task b join");

    // Exactly one of the two concurrent first-declares succeeds; the other must be
    // rejected as the stream-declare Conflict this fix adds — never silently
    // proceeding with a mismatched count.
    let outcomes = [&result_a, &result_b];
    let ok_count = outcomes.iter().filter(|r| r.is_ok()).count();
    let clean_rejection_count = outcomes
        .iter()
        .filter(|r| matches!(r, Err(ControlPlaneError::Conflict(_))))
        .count();
    assert_eq!(
        ok_count, 1,
        "exactly one concurrent first-declare must succeed: a={result_a:?} b={result_b:?}"
    );
    assert_eq!(
        clean_rejection_count, 1,
        "the losing first-declare must fail cleanly with Conflict, not desync: \
         a={result_a:?} b={result_b:?}"
    );

    // Whichever count won, every stamped row must be `< bucket_count` — i.e. no row
    // was stamped for a bucket the recorded count doesn't admit. Don't assume which
    // writer won: read both the recorded count and the actual stamped rows back.
    let tid = tid_of(&pool).await;
    let bucket_count: i32 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select bucket_count from stream.stream_table where table_id = {tid}"
    )))
    .fetch_one(&pool)
    .await
    .expect("recorded bucket_count");

    let rows = bucket_offset_rows(&pool, tid).await;
    assert!(
        !rows.is_empty(),
        "the winning writer's rows must have been committed"
    );
    for (bucket, _offset) in &rows {
        let b = bucket.expect("a stream-table row must have a stamped bucket");
        assert!(
            b < bucket_count,
            "stamped bucket {b} must be < recorded bucket_count {bucket_count} (desync)"
        );
    }
}

/// Two concurrent FIRST writes to the same brand-new `(ns, name)` both resolve to
/// the single live `iceberg_mirror.table` row: the loser of the unique-index race
/// absorbs the winner's `table_id` via the savepoint retry, instead of surfacing a
/// raw `23505`. Exactly one live table row exists afterward.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_ensure_table_resolves_to_one_id() {
    use control_plane_postgres::iceberg_mirror;

    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    async fn first_write(pool: sqlx::PgPool) -> control_plane_core::Result<i64> {
        let mut tx = pool.begin().await.expect("begin");
        let snap = iceberg_mirror::next_snapshot(&mut tx, None)
            .await
            .expect("allocate snapshot");
        let tid = iceberg_mirror::ensure_table(&mut tx, "ns", "brand_new", snap).await?;
        tx.commit().await.expect("commit");
        Ok(tid)
    }

    let handle_a = tokio::spawn(first_write(pool.clone()));
    let handle_b = tokio::spawn(first_write(pool.clone()));
    let tid_a = handle_a
        .await
        .expect("join a")
        .expect("writer a resolves ensure_table");
    let tid_b = handle_b
        .await
        .expect("join b")
        .expect("writer b resolves ensure_table");

    assert_eq!(tid_a, tid_b, "both first-writers resolve to one table_id");

    let live: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
        "select count(*) from iceberg_mirror.\"table\" where end_snapshot is null",
    ))
    .fetch_one(&pool)
    .await
    .expect("live row count");
    assert_eq!(
        live, 1,
        "exactly one live table row after concurrent first writes"
    );
}
