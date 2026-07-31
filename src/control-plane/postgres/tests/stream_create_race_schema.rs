//! #623: a brand-new table's Iceberg `create_table` must not encode a stream/batch
//! decision that has not been committed.
//!
//! A stream-declaring land creates the Iceberg table WITH the three reserved framing
//! columns as its first physical act. If it then LOSES the mirror-row create race to a
//! concurrent batch writer, `reconcile_stream_mode` rejects the batch->stream
//! conversion — but the framing-schema'd Iceberg table it already created survives,
//! under a mirror row that says batch. Physical schema and mirror mode diverge.
//!
//! This test drives that exact race deterministically (the same `pg_stat_activity`
//! barrier as `stream_declare_vs_concurrent_create.rs`: connection A holds an
//! uncommitted `iceberg_mirror.table` insert for `s.t`, the declaring lander blocks on
//! it, and we commit A once it is observably blocked) and then asserts what that test
//! does not: framing columns are present in the Iceberg physical schema **iff** the
//! mirror says the table is a stream table.
//! loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, ControlPlaneError, LineageEvent, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_mirror::next_snapshot;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use loom_test_seed::local_sql_catalog;
use sqlx::PgPool;

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

fn batch() -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let b = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
        .expect("batch");
    (schema, vec![b])
}

fn lineage() -> LineageEvent {
    LineageEvent::completed(vec![], serde_json::json!({ "source": "test" }))
}

/// Direct-write (Parquet) limits: never inline, so the write takes `land_parquet` —
/// the path whose mode decision this test is about. `land` routes inline iff
/// `bytes <= inline_byte_limit`, so a limit of 0 forces Parquet, but ONLY because
/// `batch()` is non-empty. Keep the batch non-empty.
fn never_inline() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: 0,
        flush_byte_threshold: 0,
    }
}

/// Barrier: block until some backend is waiting on another transaction's lock inside
/// an `iceberg_mirror.table` insert — i.e. the lander's mirror-row ensure is blocked
/// on our uncommitted row. Panics rather than hanging. The bound is generous (30 s)
/// because a loaded CI box is slow. Reads another backend's `query` column —
/// superuser only; the fixture connects as `postgres`.
async fn await_ensure_table_blocked(pool: &PgPool) {
    for _ in 0..3000 {
        let blocked: i64 = sqlx::query_scalar(
            "select count(*) from pg_stat_activity \
             where datname = current_database() \
               and wait_event_type = 'Lock' \
               and wait_event = 'transactionid' \
               and query like 'insert into iceberg_mirror.table%'",
        )
        .fetch_one(pool)
        .await
        .expect("pg_stat_activity barrier probe");
        if blocked >= 1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the lander's mirror-row ensure never blocked on the concurrent creator");
}

/// The three reserved framing column names, as `framing_column_specs` fixes them.
const FRAMING: [&str; 3] = ["loom_change_kind", "loom_bucket", "loom_offset"];

/// Field names of the Iceberg table's current physical schema, or `None` if the
/// Iceberg table does not exist at all.
async fn iceberg_field_names(
    catalog: &control_plane_postgres::iceberg_sql_catalog::SqlCatalog,
    table: &TableRef,
) -> Option<Vec<String>> {
    let ident = TableIdent::new(
        NamespaceIdent::new(table.schema.clone()),
        table.name.clone(),
    );
    if !catalog.table_exists(&ident).await.expect("table_exists") {
        return None;
    }
    let loaded = catalog.load_table(&ident).await.expect("load_table");
    Some(
        loaded
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_losing_stream_declare_leaves_no_framing_schema_behind() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    // `SqlCatalog` is NOT `Clone`, so share it with the spawned lander through an
    // `Arc` — `&Arc<SqlCatalog>` derefs to the `&SqlCatalog` that `land` wants.
    let catalog =
        Arc::new(local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await);

    let table = TableRef {
        schema: "s".into(),
        name: "t".into(),
    };

    // A: a concurrent BATCH writer that has created the mirror row for s.t but has
    // not committed. Held open.
    let mut winner = pool.begin().await.expect("begin winner tx");
    let at = next_snapshot(&mut winner, None)
        .await
        .expect("winner snapshot");
    sqlx::query(
        "insert into iceberg_mirror.table (table_namespace, table_name, begin_snapshot) \
         values ($1, $2, $3)",
    )
    .bind(&table.schema)
    .bind(&table.name)
    .bind(at.0)
    .execute(&mut *winner)
    .await
    .expect("winner creates the mirror row");

    // B: a stream-declaring land that will lose the mirror-row race to A.
    let (schema, batches) = batch();
    let pool_b = pool.clone();
    let cat_b = Arc::clone(&catalog);
    let table_b = table.clone();
    let lander = tokio::spawn(async move {
        land(
            &pool_b,
            &cat_b,
            &table_b,
            &columns(),
            schema,
            batches,
            never_inline(),
            lineage(),
            Some(2),
        )
        .await
    });

    await_ensure_table_blocked(&pool).await;
    winner.commit().await.expect("commit winner");

    let res = lander.await.expect("join lander");
    assert!(
        matches!(&res, Err(ControlPlaneError::Validation(msg))
                 if msg.contains("cannot convert existing batch table")),
        "the losing stream declare must be refused by the conversion guard, got {res:?}"
    );

    // THE ASSERTION THIS TEST EXISTS FOR (#623): the refused declaration must not
    // have left a framing-schema'd Iceberg table behind. The mirror says batch, so
    // the physical schema must carry no framing columns — either because the Iceberg
    // table was never created, or because it was created without them.
    let names = iceberg_field_names(&catalog, &table).await;
    if let Some(names) = &names {
        for f in FRAMING {
            assert!(
                !names.iter().any(|n| n == f),
                "mirror mode is batch but the Iceberg physical schema carries the \
                 framing column {f}; schema and mirror have diverged. fields = {names:?}"
            );
        }
    }

    // And the table stays usable as the batch table the mirror says it is: a plain
    // batch land must succeed and produce exactly the user columns.
    let (schema2, batches2) = batch();
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema2,
        batches2,
        never_inline(),
        lineage(),
        None,
    )
    .await
    .expect("a plain batch land into the batch table must succeed");

    let after = iceberg_field_names(&catalog, &table)
        .await
        .expect("the batch land created the Iceberg table");
    assert_eq!(
        after,
        vec!["id".to_owned()],
        "a batch table's Iceberg physical schema must be exactly its user columns"
    );
}

/// The other half of the spec's "framing present **iff** the mirror says stream".
/// The test above pins the batch direction under a lost race; this pins the stream
/// direction, so neither can be satisfied by a fix that just never writes framing.
///
/// No barrier needed: the declaration commits first, so a following plain batch land
/// must observe the committed stream mode and agree with it. (This direction already
/// holds on the current tree — it is a characterization test guarding the
/// restructure, not a regression gate. The gate is the test above.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_land_after_a_declaration_agrees_with_the_committed_stream_mode() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog =
        Arc::new(local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await);

    let table = TableRef {
        schema: "s".into(),
        name: "t".into(),
    };

    // A stream-declaring land on a brand-new table, uncontended: it wins by default.
    let (schema, batches) = batch();
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        never_inline(),
        lineage(),
        Some(2),
    )
    .await
    .expect("the stream-declaring land must succeed on a brand-new table");

    // A plain batch land (decl = None) into the now-declared stream table.
    let (schema2, batches2) = batch();
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema2,
        batches2,
        never_inline(),
        lineage(),
        None,
    )
    .await
    .expect("a plain batch land into a declared stream table must succeed");

    // The mirror says stream ...
    let bucket_count: Option<i32> = sqlx::query_scalar(
        "select st.bucket_count from stream.stream_table st \
         join iceberg_mirror.table t on t.table_id = st.table_id \
         where t.table_namespace = $1 and t.table_name = $2 and t.end_snapshot is null",
    )
    .bind(&table.schema)
    .bind(&table.name)
    .fetch_optional(&pool)
    .await
    .expect("read the stream registry");
    assert_eq!(
        bucket_count,
        Some(2),
        "the mirror must record this as a declared stream table"
    );

    // ... so the Iceberg physical schema must carry the framing columns.
    let names = iceberg_field_names(&catalog, &table)
        .await
        .expect("the Iceberg table exists");
    for f in FRAMING {
        assert!(
            names.iter().any(|n| n == f),
            "mirror mode is stream but the Iceberg physical schema is missing the \
             framing column {f}; schema and mirror have diverged. fields = {names:?}"
        );
    }
}
