//! Direct unit coverage for `pg_refuse_stream_target`: a declared log table and a
//! declared CDC table (both the base row and its changelog table) are refused; a
//! plain batch table and a table with no live mirror row pass.

use control_plane_core::{ControlPlaneError, MergeEngine, StreamTables, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use control_plane_postgres::stream::pg_refuse_stream_target;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

/// Create a live mirror row for `table` and return its `table_id`.
async fn ensure(pool: &sqlx::PgPool, table: &TableRef) -> i64 {
    let mut tx = pool.begin().await.expect("begin");
    let at = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    tid
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_declared_streams_passes_batch_and_absent() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // (a) absent: no live mirror row at all → passes.
    let absent = tref("s", "absent");
    let mut c = pool.acquire().await.expect("acquire");
    pg_refuse_stream_target(&mut c, &absent)
        .await
        .expect("absent passes");

    // (b) batch: has a live mirror row, not stream-declared → passes.
    let batch = tref("s", "batch");
    ensure(&pool, &batch).await;
    pg_refuse_stream_target(&mut c, &batch)
        .await
        .expect("batch passes");

    // (c) log stream: declared log table → refused.
    let logt = tref("s", "logt");
    let log_tid = ensure(&pool, &logt).await;
    cp.declare_stream(log_tid, 4).await.expect("declare_stream");
    let e = pg_refuse_stream_target(&mut c, &logt)
        .await
        .expect_err("log refused");
    assert!(matches!(e, ControlPlaneError::Validation(_)), "got {e:?}");
    assert!(
        e.to_string().contains("stream-table target refused:"),
        "message prefix, got: {e}"
    );

    // (d) CDC base: declared CDC table → refused (matched on `table_id`).
    let cdc = tref("s", "cdc");
    let cdc_tid = ensure(&pool, &cdc).await;
    cp.declare_cdc(cdc_tid, 1, "id", MergeEngine::LastRow)
        .await
        .expect("declare_cdc");
    let e = pg_refuse_stream_target(&mut c, &cdc)
        .await
        .expect_err("cdc refused");
    assert!(matches!(e, ControlPlaneError::Validation(_)), "got {e:?}");

    // (e) CDC changelog: a separate table pointed at by the base row's
    // `changelog_table_id` → refused even though it has no `stream_table` row of its own.
    let clog = tref("s", "cdc__changelog");
    let clog_tid = ensure(&pool, &clog).await;
    // Wire the base CDC row to point at the changelog's mirror id, via the public
    // StreamTables trait method (no need to widen any crate-private helper).
    cp.set_changelog_table_id(cdc_tid, clog_tid)
        .await
        .expect("set changelog id");
    let e = pg_refuse_stream_target(&mut c, &clog)
        .await
        .expect_err("changelog refused");
    assert!(matches!(e, ControlPlaneError::Validation(_)), "got {e:?}");
}
