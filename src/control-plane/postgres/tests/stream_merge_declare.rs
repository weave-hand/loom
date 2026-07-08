//! merge_engine declaration validation through land_cdc -> reconcile_stream_mode
//! (the seam every declarer passes through). loom_fixture_test (Postgres).
//!   - versioned against a type with NO version property      -> Err(Validation)
//!   - versioned against a type whose version col is non-orderable -> Err(Validation)
//!   - versioned against a type with an orderable version col -> Ok, meta reports Versioned
//!   - first_row / last_row (default) accepted with no version property
//!   - redeclare an existing CDC table with a different engine -> Err(Conflict)
//!
//! Mirrors `stream_cdc_declare.rs`'s PgFixture / local_sql_catalog / land_cdc harness
//! and its columns / batch / lineage / always_inline helpers. Each case first defines
//! an ontology type bound to the target TableRef (so `version_for_table` resolves)
//! via `cp.ontology().define_type(...)`, then calls `land_cdc` — the primitive the
//! HTTP `/models/{type}?mode=cdc&merge_engine=…` path bottoms out on — so the
//! validation is exercised at the `reconcile_stream_mode` seam that enforces it for
//! every declarer (HTTP and direct).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ControlPlaneError, MergeEngine, ObjectType, Ontology, StreamTables, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{CdcDecl, InlineLimits, land_cdc};
use control_plane_postgres::iceberg_mirror::live_table_id;
use loom_test_seed::local_sql_catalog;

/// `id`/`seq`/`qty` (long) + `name` (string) — superset of every case's column needs
/// so the single `batch` helper lands a valid row regardless of which columns the
/// case's type actually declares.
fn columns() -> Vec<control_plane_core::ColumnSpec> {
    vec![
        control_plane_core::ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        control_plane_core::ColumnSpec {
            name: "seq".into(),
            ty: "long".into(),
            nullable: true,
        },
        control_plane_core::ColumnSpec {
            name: "qty".into(),
            ty: "long".into(),
            nullable: true,
        },
        control_plane_core::ColumnSpec {
            name: "name".into(),
            ty: "string".into(),
            nullable: true,
        },
    ]
}

fn batch() -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("seq", DataType::Int64, true),
        Field::new("qty", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![100])),
            Arc::new(StringArray::from(vec![Some("a")])),
        ],
    )
    .expect("batch");
    (schema, vec![b])
}

fn lineage() -> control_plane_core::LineageEvent {
    control_plane_core::LineageEvent::completed(vec![], serde_json::json!({ "source": "test" }))
}

fn always_inline() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: usize::MAX,
        flush_byte_threshold: i64::MAX,
    }
}

/// Versioned, no version property ⇒ `Err(Validation)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn versioned_no_version_property_is_validation_error() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let table = TableRef {
        schema: "s".into(),
        name: "w_noversion".into(),
    };
    cp.define_type(
        ObjectType::build("WNoVersion", ("s", "w_noversion"))
            .prop_req("id", "Long")
            .identity("id")
            .done(),
    )
    .await
    .expect("define type");

    let (schema, batches) = batch();
    let res = land_cdc(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        None,
        Some(CdcDecl {
            buckets: 2,
            bucket_key: "id".into(),
            merge_engine: MergeEngine::Versioned,
        }),
    )
    .await;
    assert!(
        matches!(res, Err(ControlPlaneError::Validation(_))),
        "versioned against a type with no version property must be Validation, got {res:?}"
    );

    drop(wh);
    drop(catalog);
}

/// Versioned, non-orderable version property ⇒ `Err(Validation)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn versioned_non_orderable_version_property_is_validation_error() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let table = TableRef {
        schema: "s".into(),
        name: "w_strversion".into(),
    };
    cp.define_type(
        ObjectType::build("WStrVersion", ("s", "w_strversion"))
            .prop_req("id", "Long")
            .prop("name", "String")
            .identity("id")
            .version("name")
            .done(),
    )
    .await
    .expect("define type");

    let (schema, batches) = batch();
    let res = land_cdc(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        None,
        Some(CdcDecl {
            buckets: 2,
            bucket_key: "id".into(),
            merge_engine: MergeEngine::Versioned,
        }),
    )
    .await;
    assert!(
        matches!(res, Err(ControlPlaneError::Validation(_))),
        "versioned against a type whose version col is non-orderable must be Validation, got {res:?}"
    );

    drop(wh);
    drop(catalog);
}

/// Versioned, orderable version property ⇒ `Ok`, meta reports Versioned; then a
/// redeclare on the SAME table with a different engine ⇒ `Err(Conflict)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn versioned_orderable_ok_then_redeclare_different_engine_is_conflict() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let table = TableRef {
        schema: "s".into(),
        name: "w_versioned".into(),
    };
    cp.define_type(
        ObjectType::build("WVersioned", ("s", "w_versioned"))
            .prop_req("id", "Long")
            .prop_req("seq", "Long")
            .identity("id")
            .version("seq")
            .done(),
    )
    .await
    .expect("define type");

    // Case 3: Versioned, orderable version property ⇒ Ok, meta reports Versioned.
    let (schema, batches) = batch();
    land_cdc(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        None,
        Some(CdcDecl {
            buckets: 2,
            bucket_key: "id".into(),
            merge_engine: MergeEngine::Versioned,
        }),
    )
    .await
    .expect("versioned land_cdc against orderable version property");

    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("live_table_id")
        .expect("base table has a live mirror row");
    let meta = cp
        .stream_meta(tid)
        .await
        .expect("stream_meta")
        .expect("declared stream table");
    assert_eq!(
        meta.merge_engine,
        MergeEngine::Versioned,
        "stream_meta reports Versioned"
    );
    drop(conn);

    // Case 5: Redeclare on the SAME table with a different engine ⇒ Err(Conflict).
    let (schema, batches) = batch();
    let res = land_cdc(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        None,
        Some(CdcDecl {
            buckets: 2,
            bucket_key: "id".into(),
            merge_engine: MergeEngine::LastRow,
        }),
    )
    .await;
    assert!(
        matches!(res, Err(ControlPlaneError::Conflict(_))),
        "redeclare with a different engine must be Conflict, got {res:?}"
    );

    drop(wh);
    drop(catalog);
}

/// FirstRow / LastRow (default) ⇒ `Ok` with no version property required.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_row_last_row_accepted_with_no_version_property() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let table = TableRef {
        schema: "s".into(),
        name: "w_firstrow".into(),
    };
    cp.define_type(
        ObjectType::build("WFirstRow", ("s", "w_firstrow"))
            .prop_req("id", "Long")
            .identity("id")
            .done(),
    )
    .await
    .expect("define type");

    let (schema, batches) = batch();
    land_cdc(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        None,
        Some(CdcDecl {
            buckets: 2,
            bucket_key: "id".into(),
            merge_engine: MergeEngine::FirstRow,
        }),
    )
    .await
    .expect("first_row land_cdc");

    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("live_table_id")
        .expect("first_row table has a live mirror row");
    let meta = cp
        .stream_meta(tid)
        .await
        .expect("stream_meta")
        .expect("declared stream table");
    assert_eq!(
        meta.merge_engine,
        MergeEngine::FirstRow,
        "stream_meta reports FirstRow"
    );
    drop(conn);

    // LastRow on a fresh table with no version property.
    let table_last = TableRef {
        schema: "s".into(),
        name: "w_lastrow".into(),
    };
    cp.define_type(
        ObjectType::build("WLastRow", ("s", "w_lastrow"))
            .prop_req("id", "Long")
            .identity("id")
            .done(),
    )
    .await
    .expect("define type");

    let (schema, batches) = batch();
    land_cdc(
        &pool,
        &catalog,
        &table_last,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        None,
        Some(CdcDecl {
            buckets: 2,
            bucket_key: "id".into(),
            merge_engine: MergeEngine::LastRow,
        }),
    )
    .await
    .expect("last_row land_cdc");

    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &table_last.schema, &table_last.name)
        .await
        .expect("live_table_id")
        .expect("last_row table has a live mirror row");
    let meta = cp
        .stream_meta(tid)
        .await
        .expect("stream_meta")
        .expect("declared stream table");
    assert_eq!(
        meta.merge_engine,
        MergeEngine::LastRow,
        "stream_meta reports LastRow"
    );

    drop(wh);
    drop(catalog);
}
