//! write_inline_delta + current_inline_version: per-identity compare-and-swap over
//! the inline delta tier (row-versions and tombstones). loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, ControlPlaneError, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_write_and_cas_conflict() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec(), qty_spec()];

    // Seed the inline table via one append {id:1, qty:1}.
    iceberg_inline::inline_append(&pool, &table, &cols, &full_row_batch(1, 1), lin(), None)
        .await
        .expect("seed append");

    // Capture the current live version for id=1.
    let v0 = iceberg_inline::current_inline_version(
        &pool,
        &table,
        &[id_spec()],
        "id",
        &id_batch("id", 1),
    )
    .await
    .expect("current version id=1");
    assert!(v0 > 0, "seeded id has a positive version, got {v0}");

    // Write a VERSION delta {id:1, qty:9} with the correct expected_version -> Ok, newer.
    let v1 = iceberg_inline::write_inline_delta(
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
    .expect("version delta with correct expected_version");
    assert!(v1.0 > v0, "new version {} must exceed old {v0}", v1.0);

    // The current version for id=1 now reflects the new write.
    let cur = iceberg_inline::current_inline_version(
        &pool,
        &table,
        &[id_spec()],
        "id",
        &id_batch("id", 1),
    )
    .await
    .expect("current version after write");
    assert_eq!(cur, v1.0, "current version advanced to the new delta");

    // Write again with the STALE expected_version=v0 -> Conflict (CAS lost).
    let stale = iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(1, 7),
        lin(),
        v0,
    )
    .await;
    assert!(
        matches!(stale, Err(ControlPlaneError::Conflict(_))),
        "stale expected_version must Conflict, got {stale:?}"
    );

    // A DIFFERENT identity id=2 hashes to a different advisory key -> no contention.
    let v0_id2 = iceberg_inline::current_inline_version(
        &pool,
        &table,
        &[id_spec()],
        "id",
        &id_batch("id", 2),
    )
    .await
    .expect("current version id=2");
    assert_eq!(v0_id2, 0, "unwritten id has version 0");
    let r2 = iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(2, 5),
        lin(),
        v0_id2,
    )
    .await;
    assert!(r2.is_ok(), "a different identity must not conflict: {r2:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tombstone_delta_marks_deleted() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec()];

    // Seed one row {id:1}.
    iceberg_inline::inline_append(&pool, &table, &cols, &id_batch("id", 1), lin(), None)
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

    // Write a TOMBSTONE delta carrying just the id.
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

    // A live inline row for id=1 with loom_tombstone=true and the id populated exists.
    let exists: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select exists(select 1 from iceberg_mirror.inline_{tid} \
         where loom_tombstone = true and \"id\" = 1 and end_snapshot is null)"
    )))
    .fetch_one(&pool)
    .await
    .expect("tombstone existence check");
    assert!(exists, "a live tombstone row for id=1 must exist");

    // The mutation flagged the table as carrying inline shadow deltas.
    let mut conn = pool.acquire().await.expect("acquire");
    let flagged = iceberg_inline::has_shadow(&mut conn, tid)
        .await
        .expect("has_shadow");
    assert!(
        flagged,
        "a mutated table must be flagged as having a shadow"
    );
}

/// A FILE-ONLY object — landed as a real Parquet data_file, so it has a live mirror
/// `table` + `column` rows but NO `inline_<tid>` table yet — is the PRIMARY
/// copy-on-write target. This exercises the two file-only lifecycle bugs:
///   - Bug #1: `current_inline_version` must return `Ok(0)` (not error) when the
///     inline relation is absent.
///   - Bug #2: a tombstone-FIRST `write_inline_delta` (columns = `[id spec]`) must
///     provision `inline_<tid>` with the table's FULL live column set, so a later
///     full-column read (`inline_live_batch`) does not fail on a missing column.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_only_object_tombstone_first_lifecycle() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let table = table();

    // Land a real Parquet file {id:1, qty:5} via the writer chain. This projects the
    // mirror table + both column rows (id, qty) + a data_file row, but creates NO
    // inline table — the file-only starting state. `qty` is NULLABLE, modelling a
    // realistic identity type (only the identity is required; other properties are
    // optional): a tombstone nulls the non-id columns, so the full-column read must
    // reconstruct `qty` as a nullable field.
    let seed_cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("qty".to_string(), "long".to_string(), true),
    ];
    let writer = IcebergWriter::new(pool.clone(), dsn);
    writer
        .seed_arrays(
            &table.schema,
            &table.name,
            &seed_cols,
            &[SeedCol::Long(vec![1]), SeedCol::Long(vec![5])],
        )
        .await;

    // Bug #1: no inline table exists, so the identity's inline version is 0 — the
    // read must NOT error with `relation ... does not exist`.
    let v0 = iceberg_inline::current_inline_version(
        &pool,
        &table,
        &[id_spec()],
        "id",
        &id_batch("id", 1),
    )
    .await
    .expect("current_inline_version on a file-only object must be Ok(0), not an error");
    assert_eq!(v0, 0, "a file-only object has inline version 0");

    // Bug #2: a tombstone is the FIRST inline op. It passes columns = [id spec], but
    // write_inline_delta must provision inline_<tid> with the FULL live column set.
    let at = iceberg_inline::write_inline_delta(
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
    .expect("tombstone-first delta on a file-only object must succeed");

    // The full-column read must NOT error: `inline_live_batch` selects the full
    // logical column list (id + qty). Under-provisioning (id column only) would fail
    // here with `column "qty" does not exist`.
    let catalog = IcebergCatalog::new(pool.clone());
    let (tid, row_ids, batch) = catalog
        .inline_live_batch(&table, at)
        .await
        .expect("inline_live_batch must not error after a tombstone-first provision")
        .expect("the live tombstone row must be present");
    assert_eq!(
        batch.num_columns(),
        2,
        "inline_<tid> was provisioned with the FULL column set (id, qty)"
    );
    assert_eq!(
        row_ids.len(),
        1,
        "exactly one live inline row (the tombstone)"
    );

    // The delete is recorded: a live tombstone row for id=1 exists in inline_<tid>.
    let tomb: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select exists(select 1 from iceberg_mirror.inline_{tid} \
         where loom_tombstone = true and \"id\" = 1 and end_snapshot is null)"
    )))
    .fetch_one(&pool)
    .await
    .expect("tombstone existence check");
    assert!(tomb, "a live tombstone row for id=1 must exist");
}

/// Concurrent first-time provisioning of the inline tier must not race.
///
/// A file-only object has no `inline_<tid>` yet, so the FIRST mutation of any
/// identity provisions it lazily via `CREATE TABLE IF NOT EXISTS`. Postgres's
/// `IF NOT EXISTS` is NOT concurrency-safe: two sessions that both find the table
/// absent then both create it — one aborts with `relation "inline_<tid>" already
/// exists` / a `pg_type` unique violation, poisoning its whole transaction. Several
/// concurrent first mutations of DISTINCT identities (distinct advisory keys → no
/// per-identity serialization) all hit that shared first-create window. The
/// savepoint-guarded idempotent DDL (`run_idempotent_ddl`) absorbs the lost race, so
/// every writer commits. Without the guard, all-but-one would error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_provisioning_of_distinct_identities() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let table = table();

    // Land a real Parquet file: a live mirror table exists, but NO inline storage yet
    // (the file-only starting state, the primary copy-on-write target). `qty` nullable.
    let seed_cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("qty".to_string(), "long".to_string(), true),
    ];
    let writer = IcebergWriter::new(pool.clone(), dsn);
    writer
        .seed_arrays(
            &table.schema,
            &table.name,
            &seed_cols,
            &[SeedCol::Long(vec![0]), SeedCol::Long(vec![0])],
        )
        .await;

    // Fire N concurrent version deltas, each for a DISTINCT identity so they do NOT
    // serialize on the per-identity advisory lock — they race the shared first CREATE.
    const N: i64 = 8;
    let cols = vec![id_spec(), qty_spec()];
    let mut handles = Vec::new();
    for id in 1..=N {
        let pool = pool.clone();
        let table = table.clone();
        let cols = cols.clone();
        handles.push(tokio::spawn(async move {
            iceberg_inline::write_inline_delta(
                &pool,
                &table,
                &cols,
                "id",
                false,
                &full_row_batch(id, id),
                lin(),
                0,
            )
            .await
        }));
    }
    for (i, h) in handles.into_iter().enumerate() {
        h.await.expect("task join").unwrap_or_else(|e| {
            panic!("concurrent first-provisioning writer {i} must succeed: {e:?}")
        });
    }

    // Every distinct-identity delta committed exactly one live inline row (the table
    // was provisioned once, and no writer aborted on the concurrent-create race).
    let tid = tid_of(&pool).await;
    let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select count(*) from iceberg_mirror.inline_{tid} where end_snapshot is null"
    )))
    .fetch_one(&pool)
    .await
    .expect("count live inline rows");
    assert_eq!(
        n, N,
        "all {N} concurrent distinct-identity deltas committed a live row"
    );
}
