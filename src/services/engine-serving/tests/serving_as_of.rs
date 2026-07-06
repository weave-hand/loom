//! Time-travel reads: `build_serving_provider(.., at: Some(snapshot))` pins the
//! served rows to a historical snapshot, while `at: None` keeps serving the
//! live (current) snapshot. Harness modeled on `tests/inline_vector_sql.rs`
//! (fixture boot, `local_sql_catalog`, `land`), swapped to a plain long/string
//! schema (no vectors needed here) and driven straight through
//! `build_serving_provider` + a `SessionContext` scan instead of SQL text.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use control_plane_core::{ColumnSpec, RunId, SnapshotId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use engine_serving::build_serving_provider;
use loom_test_seed::{cold_limits, local_sql_catalog, test_lineage};
use sqlx::PgPool;

/// The `(id: long, name: string)` schema shared by both landed snapshots.
fn cols() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "name".into(),
            ty: "string".into(),
            nullable: false,
        },
    ]
}

/// Build a schema + one batch of `(id, name)` rows, forced to Parquet
/// (`cold_limits`) so each `land` call produces a real new snapshot over a
/// real data file.
fn batch(ids: &[i64]) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let names: Vec<String> = ids.iter().map(|i| format!("row{i}")).collect();
    let id_array = Int64Array::from(ids.to_vec());
    let name_array = StringArray::from(names);
    let rb = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(name_array)],
    )
    .expect("batch");
    (schema, vec![rb])
}

/// Land `ids` as a new Parquet-backed snapshot over `table`, returning the
/// new snapshot id.
async fn land_rows(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    ids: &[i64],
) -> SnapshotId {
    let (schema, batches) = batch(ids);
    land(
        pool,
        catalog,
        table,
        &cols(),
        schema,
        batches,
        cold_limits(),
        test_lineage(RunId(uuid::Uuid::new_v4()), table),
    )
    .await
    .expect("land")
}

/// Scan `provider` through `ctx` and return the total row count across all
/// result batches.
async fn count_rows(ctx: &SessionContext, provider: Arc<dyn TableProvider>) -> usize {
    let df = ctx.read_table(provider).expect("read_table");
    let batches = df.collect().await.expect("collect");
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn as_of_snapshot_sees_only_rows_landed_by_then() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let sql_catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let table = TableRef {
        schema: "s".into(),
        name: "t".into(),
    };

    let s1 = land_rows(&pool, &sql_catalog, &table, &[1, 2, 3]).await; // 3 rows at S1
    let _s2 = land_rows(&pool, &sql_catalog, &table, &[4, 5, 6, 7, 8]).await; // +5 rows (8 total) at S2

    let catalog = IcebergCatalog::new(pool);
    let ctx = SessionContext::new();

    let at_s1 = build_serving_provider(&ctx, &catalog, &table, None, Some(s1))
        .await
        .unwrap()
        .expect("provider at S1");
    assert_eq!(
        count_rows(&ctx, at_s1).await,
        3,
        "as-of S1 sees only the first write"
    );

    let live = build_serving_provider(&ctx, &catalog, &table, None, None)
        .await
        .unwrap()
        .expect("live provider");
    assert_eq!(count_rows(&ctx, live).await, 8, "live sees both writes");
}
