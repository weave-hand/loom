//! `consolidate_stream` takes the SAME per-table advisory lock `flush_table`/
//! `gc_table` use (`iceberg_flush::lock_table`, keyed by `iceberg_flush::lock_key`),
//! so it can no longer race a concurrent flush between its file-read and its
//! overwrite-commit — see the design note on `consolidate_stream` itself. A full
//! flush-vs-consolidate race is hard to fixture deterministically (both sides
//! would need to land at the exact file-read/overwrite boundary); this proves
//! the directly-testable, narrower claim the lock gives us: while ANOTHER
//! session holds the table's advisory lock (standing in for an in-flight
//! flush/GC), `consolidate_stream` cannot proceed past the gate, and once that
//! lock releases, it completes normally.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use control_plane_core::{StreamTables, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::lock_table;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use engine_serving::consolidate_stream;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

async fn build_catalog(dsn: &str, warehouse: &std::path::Path) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn.to_string());
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{}", warehouse.display()),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("build SqlCatalog")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consolidate_stream_defers_to_a_held_table_lock() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = Arc::new(build_catalog(&dsn, warehouse.path()).await);

    let table = TableRef {
        schema: "main".into(),
        name: "widget".into(),
    };

    // Declare the table CDC (keyed on `id`) — enough shape for `consolidate_stream`
    // to pass its early no-op checks and reach the locked window.
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, "main", "widget", at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 2, "id", control_plane_core::MergeEngine::LastRow)
        .await
        .expect("declare_cdc");

    // Hold the SAME per-table advisory lock `consolidate_stream` must take, from
    // a separate transaction — standing in for an in-flight flush/GC on this table.
    let held = lock_table(&pool, &table).await.expect("lock_table");

    let cp2 = cp.clone();
    let catalog2 = catalog.clone();
    let pool2 = pool.clone();
    let table2 = table.clone();
    let consolidate_task =
        tokio::spawn(async move { consolidate_stream(&cp2, &catalog2, &pool2, &table2).await });

    // Give the spawned task a moment to actually reach and block on the lock,
    // then assert it has NOT proceeded past the gate while the lock is held.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !consolidate_task.is_finished(),
        "consolidate_stream must defer while a concurrent holder has the table's advisory lock"
    );

    // Release the held lock — consolidate_stream must now proceed past the
    // gate. This fixture only seeds the postgres mirror rows (not a real
    // physical Iceberg table in the warehouse), so the call can legitimately
    // error trying to read a table that was never actually created there;
    // what this asserts is narrower and load-bearing for Fix 3: it no longer
    // hangs waiting on the lock once the lock is free, i.e. the lock really
    // was the thing gating it above.
    held.release().await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), consolidate_task)
        .await
        .expect("consolidate_stream should proceed once the table lock is released")
        .expect("consolidate_stream task should not panic");
    drop(outcome);
}
