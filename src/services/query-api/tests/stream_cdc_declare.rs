//! CDC declaration slice 2b (Task 2): a `mode=cdc` land eagerly creates the
//! table's durable changelog Iceberg table + mirror row + registry pointer, in
//! two phases — the changelog's Iceberg metadata BEFORE any commit tx
//! (`land_cdc`), and its `iceberg_mirror.table` row + `changelog_table_id`
//! pointer INSIDE the write tx (`reconcile_stream_mode`'s CDC first-declare
//! arm). Drives `land_cdc` directly (the postgres-crate landing entrypoint,
//! same primitive `stream_cdc_e2e.rs`'s HTTP path bottoms out on) rather than
//! going through the ingest HTTP router or ontology bind, since this is a
//! control-plane-level contract, not an HTTP/ACL one.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, EventType, LineageEvent, RunId, StreamKind, StreamTables, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{CdcDecl, InlineLimits, land_cdc};
use control_plane_postgres::iceberg_mirror::live_table_id;
use iceberg::{Catalog as IceCatalog, NamespaceIdent, TableIdent};
use loom_test_seed::local_sql_catalog;

/// A 2-column table: `id` (long, required — the CDC bucket key) and `qty` (long,
/// nullable).
fn columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "qty".into(),
            ty: "long".into(),
            nullable: true,
        },
    ]
}

fn batch(ids: &[i64], qtys: &[i64]) -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("qty", DataType::Int64, true),
    ]));
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(qtys.to_vec())),
        ],
    )
    .expect("batch");
    (schema, vec![b])
}

fn lineage(run: RunId) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "test" }),
    }
}

fn always_inline() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: usize::MAX,
        flush_byte_threshold: i64::MAX,
    }
}

/// A `mode=cdc` land creates the changelog table's Iceberg metadata (Phase A,
/// `land_cdc`) and, in the same write tx, its `iceberg_mirror.table` row and the
/// `stream_table.changelog_table_id` pointer (Phase B, `reconcile_stream_mode`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_declaration_creates_changelog_table_mirror_row_and_pointer() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let table = TableRef {
        schema: "s".into(),
        name: "widget".into(),
    };
    let (schema, batches) = batch(&[1, 2], &[10, 20]);
    let run = RunId(uuid::Uuid::new_v4());

    land_cdc(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(run),
        None,
        Some(CdcDecl {
            buckets: 2,
            bucket_key: "id".into(),
        }),
        &[],
    )
    .await
    .expect("land cdc");

    // The base table's stream metadata now carries a changelog pointer.
    let mut conn = pool.acquire().await.expect("acquire");
    let base_tid = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("live_table_id")
        .expect("base table has a live mirror row");
    let meta = cp
        .stream_meta(base_tid)
        .await
        .expect("stream_meta")
        .expect("base table is a declared stream table");
    assert_eq!(meta.kind, StreamKind::Cdc, "declared as a cdc table");
    let clog_tid = meta
        .changelog_table_id
        .expect("changelog_table_id is set on cdc first-declare");
    assert_ne!(
        clog_tid, base_tid,
        "the changelog table is a DIFFERENT mirror table from the base"
    );

    // The changelog's own mirror row exists and matches the pointer.
    let clog_ref = TableRef {
        schema: table.schema.clone(),
        name: format!("{}__changelog", table.name),
    };
    let clog_live_tid = live_table_id(&mut conn, &clog_ref.schema, &clog_ref.name)
        .await
        .expect("live_table_id for changelog")
        .expect("the changelog mirror row exists");
    assert_eq!(
        clog_live_tid, clog_tid,
        "the registry pointer targets the changelog's own mirror row"
    );

    // The changelog's Iceberg physical schema (created eagerly by `land_cdc`,
    // before this write's commit tx) carries the user columns plus the three
    // reserved framing columns. Its `iceberg_mirror.column` rows are NOT yet
    // projected (no data has ever been appended to it in this slice), so this
    // checks the raw Iceberg metadata rather than `IcebergCatalog::physical_columns`.
    let ident = TableIdent::new(NamespaceIdent::new(clog_ref.schema.clone()), clog_ref.name);
    let ice_table = catalog
        .load_table(&ident)
        .await
        .expect("load changelog table");
    let field_names: Vec<String> = ice_table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.clone())
        .collect();
    for want in [
        "id",
        "qty",
        "loom_change_kind",
        "loom_bucket",
        "loom_offset",
    ] {
        assert!(
            field_names.iter().any(|n| n == want),
            "changelog schema {field_names:?} missing {want:?}"
        );
    }

    drop(wh);
}

/// A plain batch land (no stream declaration) creates no `__changelog` table and
/// leaves no `stream_table` row at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_cdc_batch_land_creates_no_changelog_table() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let table = TableRef {
        schema: "s".into(),
        name: "gizmo".into(),
    };
    let (schema, batches) = batch(&[1, 2], &[10, 20]);
    let run = RunId(uuid::Uuid::new_v4());

    land_cdc(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(run),
        None,
        None,
        &[],
    )
    .await
    .expect("land batch");

    let mut conn = pool.acquire().await.expect("acquire");
    let base_tid = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("live_table_id")
        .expect("base table has a live mirror row");
    assert_eq!(
        cp.stream_meta(base_tid).await.expect("stream_meta"),
        None,
        "a plain batch land declares no stream mode"
    );

    let clog_name = format!("{}__changelog", table.name);
    assert_eq!(
        live_table_id(&mut conn, &table.schema, &clog_name)
            .await
            .expect("live_table_id for changelog"),
        None,
        "no changelog mirror table is created for a batch land"
    );

    drop(wh);
    drop(catalog);
}

/// A `Log` stream declaration (not CDC) likewise creates no `__changelog`
/// table — only a CDC first-declare owns a durable changelog.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_declare_creates_no_changelog_table() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let table = TableRef {
        schema: "s".into(),
        name: "log_widget".into(),
    };
    let (schema, batches) = batch(&[1, 2], &[10, 20]);
    let run = RunId(uuid::Uuid::new_v4());

    land_cdc(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(run),
        Some(2),
        None,
        &[],
    )
    .await
    .expect("land log stream");

    let mut conn = pool.acquire().await.expect("acquire");
    let base_tid = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("live_table_id")
        .expect("base table has a live mirror row");
    let meta = cp
        .stream_meta(base_tid)
        .await
        .expect("stream_meta")
        .expect("base table is a declared log stream table");
    assert_eq!(meta.kind, StreamKind::Log, "declared as a log table");
    assert_eq!(
        meta.changelog_table_id, None,
        "a log table never gets a changelog pointer"
    );

    let clog_name = format!("{}__changelog", table.name);
    assert_eq!(
        live_table_id(&mut conn, &table.schema, &clog_name)
            .await
            .expect("live_table_id for changelog"),
        None,
        "no changelog mirror table is created for a log declare"
    );

    drop(wh);
    drop(catalog);
}
