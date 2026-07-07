//! CDC tables bucket rows by `hash(identity) % bucket_count`, not the log
//! table's `row_index % bucket_count`: a key's whole change history (+I/-U/
//! +U/-D) must stay in one bucket, since LastRow merge and per-key ordering
//! depend on it. This proves it end-to-end through `inline_append`: two
//! separate appends carrying the SAME identity land in the SAME bucket, with
//! gapless per-bucket offsets. loom_fixture_test (Postgres).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, EventType, LineageEvent, RunId, StreamTables, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_inline::inline_append;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};

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

/// Read back `(id, loom_bucket, loom_offset)` for every live row in
/// `inline_<tid>`, ordered by offset -- mirrors `stream_inline.rs`'s
/// `framing_cols` helper but over the whole table, not one identity.
async fn framing_rows(pool: &sqlx::PgPool, tid: i64) -> Vec<(i64, i32, i64)> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "select \"id\", loom_bucket, loom_offset from iceberg_mirror.inline_{tid} \
         order by loom_offset"
    )))
    .fetch_all(pool)
    .await
    .expect("framing rows readback")
}

/// Each bucket's offsets, sorted, must be exactly `0..n` (gapless, no dups) --
/// mirrors `stream_flush_persist.rs`'s `assert_gapless_per_bucket`.
fn assert_gapless_per_bucket(rows: &[(i64, i32, i64)]) {
    let mut by_bucket: HashMap<i32, Vec<i64>> = HashMap::new();
    for (_, b, o) in rows {
        by_bucket.entry(*b).or_default().push(*o);
    }
    for (bucket, mut os) in by_bucket {
        os.sort_unstable();
        let want: Vec<i64> = (0..os.len() as i64).collect();
        assert_eq!(os, want, "bucket {bucket} offsets must be gapless: {os:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_identity_stays_in_one_bucket_across_appends() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "s".to_string(),
        name: "cdc".to_string(),
    };
    let cols = vec![id_spec()];
    let bucket_count = 4;

    // Predeclare the table as CDC (keyed on `id`) BEFORE any inline_append --
    // inline_append has no CDC-declare parameter of its own; a real caller
    // declares the table via dataset->model binding, then appends. Mirrors
    // `stream_reserved_schema.rs`'s direct ensure_table/next_snapshot use.
    let mut conn = pool.acquire().await.expect("acquire");
    let at0 = next_snapshot(&mut conn, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut conn, &table.schema, &table.name, at0)
        .await
        .expect("ensure_table");
    drop(conn);
    cp.declare_cdc(tid, bucket_count, "id")
        .await
        .expect("declare_cdc");

    // Append A: id=2, id=1 (id=1 at ROW INDEX 1). Append B: id=1, id=3 (id=1 at
    // ROW INDEX 0). Row indexing restarts at 0 for every append, so under the
    // OLD `row_index % bucket_count` scheme id=1 would land in bucket 1 in
    // append A and bucket 0 in append B -- deliberately DIFFERENT row
    // positions, so this test actually distinguishes hash-on-identity from
    // row-index bucketing (same-position rows would land in the same bucket
    // under either scheme and this test would pass vacuously).
    inline_append(&pool, &table, &cols, &id_batch(&[2, 1]), lin(), None, None)
        .await
        .expect("append A");
    inline_append(&pool, &table, &cols, &id_batch(&[1, 3]), lin(), None, None)
        .await
        .expect("append B");

    let rows = framing_rows(&pool, tid).await;
    assert_eq!(rows.len(), 4, "4 rows landed: {rows:?}");

    let id1_buckets: Vec<i32> = rows
        .iter()
        .filter(|(id, _, _)| *id == 1)
        .map(|(_, b, _)| *b)
        .collect();
    assert_eq!(
        id1_buckets.len(),
        2,
        "id=1 appears in both appends: {rows:?}"
    );
    assert_eq!(
        id1_buckets[0], id1_buckets[1],
        "id=1's two rows (from separate appends) must share the SAME bucket \
         (hash-on-identity), not differ by row_index % bucket_count: {rows:?}"
    );

    for (_, b, _) in &rows {
        assert!(
            (0..bucket_count).contains(b),
            "bucket {b} out of range 0..{bucket_count}: {rows:?}"
        );
    }

    assert_gapless_per_bucket(&rows);
}
