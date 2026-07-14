//! A typed UPDATE/DELETE against a CDC table whose FIRST land went straight to Parquet
//! (`inline_byte_limit` exceeded — the production `/models/{t}?mode=cdc` path for any body
//! over `LOOM_INLINE_BYTE_LIMIT`, 16 MiB by default).
//!
//! Such a table has framing columns (`loom_change_kind`/`loom_bucket`/`loom_offset`) in its
//! live `iceberg_mirror.column` set — `land_parquet_stream` registers them — but NO physical
//! `inline_<tid>` relation, because the Parquet route never provisions inline storage. The
//! first typed mutation therefore CREATEs `inline_<tid>` from `full_live_column_specs`, whose
//! list already carries the framing columns that `inline_ddl` hardcodes — a duplicate-column
//! CREATE TABLE. The LOG variant of this shape is refused outright
//! (`pg_stream_meta_for_typed_write`); the CDC variant is a SUPPORTED mutation, so it must
//! work, not fail.
//!
//! loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, MergeEngine, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_inline::{
    current_inline_version, inline_table_name, write_inline_delta,
};
use control_plane_postgres::iceberg_landing::{CdcDecl, InlineLimits, land_cdc};
use control_plane_postgres::iceberg_mirror::live_table_id;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use end_cap_seed::{batch, columns, id_only_batch, lineage, tref};
use loom_test_seed::local_sql_catalog;
use sqlx::AssertSqlSafe;

/// The one-cell `(id)` spec a typed DELETE lowers to.
fn id_specs() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// A one-row `(id, label)` batch — the image shape `columns()` describes.
fn row(id: i64, label: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![label])),
        ],
    )
    .expect("row batch")
}

/// A CDC `main.widget` declared by a land that is forced STRAIGHT TO PARQUET
/// (`inline_byte_limit: 0`), so its mirror carries framing and it has no inline storage.
async fn seed_parquet_cdc(fx: &PgFixture, db: &str, wh: &str) -> (sqlx::PgPool, TableRef, i64) {
    let pool = fx.pool_for(db).await;
    let catalog: SqlCatalog = local_sql_catalog(fx.pg_dsn(db), wh).await;
    let table = tref("main", "widget");
    let (schema, batches) = batch(2); // ids 0..2, labels row-0/row-1
    land_cdc(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        InlineLimits {
            // 0 => every write is over the limit, so the land takes the direct-write
            // Parquet stream path and NEVER provisions `inline_<tid>`.
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(&table),
        None,
        Some(CdcDecl {
            buckets: 1,
            bucket_key: "id".into(),
            merge_engine: MergeEngine::LastRow,
        }),
        &[],
    )
    .await
    .expect("land cdc to parquet");

    let mut conn = pool.acquire().await.expect("conn");
    let tid = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("tid")
        .expect("live tid");
    // The premise: framing IS in the mirror, and inline storage is NOT provisioned.
    let framing: i64 = sqlx::query_scalar(
        "select count(*) from iceberg_mirror.column where table_id = $1 \
         and end_snapshot is null \
         and column_name in ('loom_change_kind', 'loom_bucket', 'loom_offset')",
    )
    .bind(tid)
    .fetch_one(&mut *conn)
    .await
    .expect("count framing columns");
    assert_eq!(framing, 3, "a parquet-landed CDC table must carry framing");
    let inline: Option<String> = sqlx::query_scalar("select to_regclass($1)::text")
        .bind(inline_table_name(tid))
        .fetch_one(&mut *conn)
        .await
        .expect("to_regclass");
    assert!(
        inline.is_none(),
        "the parquet land must NOT have provisioned inline storage"
    );
    drop(conn);
    (pool, table, tid)
}

/// Rows of `inline_<tid>` with the given change kind, and whether they are framed.
async fn kind_rows(pool: &sqlx::PgPool, tid: i64, kind: &str) -> Vec<(bool, bool)> {
    sqlx::query_as(AssertSqlSafe(format!(
        "select loom_bucket is not null, loom_offset is not null from {} \
         where loom_change_kind = $1 order by loom_row_id",
        inline_table_name(tid),
    )))
    .bind(kind)
    .fetch_all(pool)
    .await
    .expect("read inline rows")
}

/// A typed DELETE against a parquet-landed CDC table must write its `-D` changelog row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_delete_on_a_parquet_landed_cdc_table_succeeds() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let (pool, table, tid) = seed_parquet_cdc(fx, &db, &wh.path().display().to_string()).await;

    let v = current_inline_version(&pool, &table, &id_specs(), "id", &id_only_batch(1))
        .await
        .expect("version");
    write_inline_delta(
        &pool,
        &table,
        &id_specs(),
        "id",
        true, // tombstone: a typed DELETE
        &id_only_batch(1),
        Some((&columns(), &row(1, "row-1"))), // the CDC before-image
        lineage(&table),
        v,
        Some(1000),
        &[],
    )
    .await
    .expect("a typed DELETE on a CDC table must land its -D changelog row");

    let d = kind_rows(&pool, tid, "-D").await;
    assert_eq!(d.len(), 1, "exactly one -D row");
    assert_eq!(d.first(), Some(&(true, true)), "the -D row must be framed");
}

/// A typed UPDATE against the same shape must write its adjacent `-U`/`+U` pair.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_update_on_a_parquet_landed_cdc_table_succeeds() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let (pool, table, tid) = seed_parquet_cdc(fx, &db, &wh.path().display().to_string()).await;

    let v = current_inline_version(&pool, &table, &id_specs(), "id", &id_only_batch(1))
        .await
        .expect("version");
    write_inline_delta(
        &pool,
        &table,
        &columns(),
        "id",
        false, // a typed UPDATE
        &row(1, "patched"),
        Some((&columns(), &row(1, "row-1"))),
        lineage(&table),
        v,
        Some(1000),
        &[],
    )
    .await
    .expect("a typed UPDATE on a CDC table must land its -U/+U pair");

    assert_eq!(kind_rows(&pool, tid, "-U").await.len(), 1, "one -U row");
    let plus = kind_rows(&pool, tid, "+U").await;
    assert_eq!(plus.len(), 1, "one +U row");
    assert_eq!(
        plus.first(),
        Some(&(true, true)),
        "the +U row must be framed"
    );
}
