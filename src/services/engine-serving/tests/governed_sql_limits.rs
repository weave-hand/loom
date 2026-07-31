//! Resource bounds on the arbitrary-SQL governed path (#664): the memory pool and
//! the wall-clock deadline each trip with the RESOURCE-EXHAUSTED class (not an
//! opaque engine fault), the same SQL succeeds unbounded, and a normal query under
//! generous bounds is unaffected.

use control_plane_core::{GovernedCatalog, GovernedTable, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use engine_serving::EngineServingError;
use engine_serving::governed::execute_governed_sql_stream;
use engine_serving::sql_limits::{GovernedSqlLimits, governed_stream_error};
use futures::TryStreamExt;
use std::time::Duration;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

/// Seed `s.big` with `rows` rows (`id`, `name`) and return a catalog plus a
/// fully-visible (empty-policy) governed catalog over it.
///
/// The `IcebergWriter` is RETURNED, not dropped here: it owns the `TempDir`
/// warehouse root and deletes it on drop (`postgres/src/fixture.rs:484-486`), so
/// dropping it inside this helper would wipe the Parquet before the query runs and
/// every test would fail on a missing file instead of on its budget.
async fn setup(rows: usize) -> (IcebergWriter, IcebergCatalog, GovernedCatalog) {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    writer.seed("s", "big", &cols, &[rows]).await;
    let cat = GovernedCatalog {
        tables: vec![GovernedTable {
            table: tref("s", "big"),
            row_filters: vec![],
            denied: vec![],
            masked: vec![],
        }],
    };
    (writer, IcebergCatalog::new(pool), cat)
}

/// Drive the governed path to completion, returning the FIRST error classified
/// through the shared `governed_stream_error` seam (the exact class the Flight
/// wire will map), or `Ok(row_count)`.
async fn drive(
    catalog: &IcebergCatalog,
    cat: &GovernedCatalog,
    sql: &str,
    limits: &GovernedSqlLimits,
) -> Result<usize, EngineServingError> {
    let stream = execute_governed_sql_stream(catalog, sql, cat, None, limits).await?;
    let batches: Vec<_> = stream
        .try_collect()
        .await
        .map_err(|e| governed_stream_error(&e))?;
    Ok(batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum())
}

const SORT_SQL: &str =
    "SELECT \"id\", \"name\" FROM \"s\".\"big\" ORDER BY \"name\" DESC, \"id\" DESC";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_bound_trips_with_the_resource_exhausted_class() {
    let (_writer, catalog, cat) = setup(500).await;
    let limits = GovernedSqlLimits {
        memory_bytes: Some(1), // one byte: any tracked reservation breaches it
        deadline: None,
    };
    let err = drive(&catalog, &cat, SORT_SQL, &limits)
        .await
        .expect_err("a 1-byte pool must not sort 500 rows");
    // Asserted on the VARIANT, not `is_err()`: the whole risk is that a pool
    // breach is misclassified as `Engine` (500) by the class-erasing `to_serving`.
    assert!(
        matches!(err, EngineServingError::ResourceExhausted(_)),
        "expected ResourceExhausted, got {err:?}"
    );
    // And it must name LOOM's knob, not DataFusion's own config keys.
    assert!(
        err.to_string().contains("LOOM_SQL_MEMORY_LIMIT_BYTES"),
        "message must name the budget: {err}"
    );
}

/// A realistically-sized pool (not the degenerate 1 byte above) still trips with the
/// RESOURCE-EXHAUSTED class over a larger table — the bound is not an artefact of a
/// pathological limit.
///
/// This deliberately does NOT pin the no-spill property, despite the temptation: with
/// any pool small enough to be interesting, `SortExec` fails its 10 MiB
/// `sort_spill_reservation_bytes` pre-reservation before it ever reaches a spill
/// decision, so it errors identically whether or not the disk manager is disabled
/// (verified by deleting the setting and watching this file stay green). No-spill is
/// pinned directly in `tests/deadline_stream.rs::the_bounded_session_can_never_spill_to_disk`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_realistic_pool_still_trips_on_a_larger_table() {
    let (_writer, catalog, cat) = setup(5_000).await;
    let limits = GovernedSqlLimits {
        memory_bytes: Some(64 * 1024),
        deadline: None,
    };
    let err = drive(&catalog, &cat, SORT_SQL, &limits)
        .await
        .expect_err("must fail hard, not spill");
    assert!(
        matches!(err, EngineServingError::ResourceExhausted(_)),
        "expected ResourceExhausted, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_same_sql_succeeds_unbounded() {
    let (_writer, catalog, cat) = setup(500).await;
    // Without this, the memory tests above would pass even if the SQL were invalid.
    let n = drive(&catalog, &cat, SORT_SQL, &GovernedSqlLimits::unbounded())
        .await
        .expect("unbounded run must succeed");
    assert_eq!(n, 500);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_deadline_trips_during_planning() {
    let (_writer, catalog, cat) = setup(10).await;
    let limits = GovernedSqlLimits {
        memory_bytes: None,
        deadline: Some(Duration::ZERO),
    };
    let err = drive(&catalog, &cat, SORT_SQL, &limits)
        .await
        .expect_err("a zero budget must trip");
    assert!(
        matches!(err, EngineServingError::ResourceExhausted(_)),
        "expected ResourceExhausted, got {err:?}"
    );
    // A zero budget expires while `plan_governed_sql` is still doing catalog IO, so
    // this is the WRAPPER's `timeout_at` arm, not `DeadlineStream`. Asserted on the
    // message so the two deadline paths stay distinguishable — the stream adapter's
    // own timer is covered deterministically by `tests/deadline_stream.rs`.
    assert!(
        err.to_string().contains("while planning"),
        "expected the planning-phase deadline, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generous_bounds_are_not_a_false_positive() {
    let (_writer, catalog, cat) = setup(10).await;
    let limits = GovernedSqlLimits {
        memory_bytes: Some(1024 * 1024 * 1024),
        deadline: Some(Duration::from_secs(300)),
    };
    let n = drive(&catalog, &cat, SORT_SQL, &limits)
        .await
        .expect("a normal query under generous bounds must succeed");
    assert_eq!(n, 10);
}
