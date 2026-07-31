//! #623: a brand-new table's Iceberg `create_table` must not encode a stream/batch
//! decision that has not been committed.
//!
//! A stream-declaring land creates the Iceberg table WITH the three reserved framing
//! columns as its first physical act. If it then LOSES the mirror-row create race to a
//! concurrent batch writer, `reconcile_stream_mode` rejects the batch->stream
//! conversion — but the framing-schema'd Iceberg table it already created survives,
//! under a mirror row that says batch. Physical schema and mirror mode diverge.
//!
//! These tests pin both directions of the property the fix establishes: framing
//! columns are present in the Iceberg physical schema **iff** the mirror says the
//! table is a stream table. The race arrangement itself lives in
//! `stream_race_support` (shared with `stream_declare_vs_concurrent_create.rs`).
//! loom_fixture_test (Postgres).

use control_plane_core::{ControlPlaneError, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::land;
use stream_race_support::{
    FRAMING, batch, columns, harness, iceberg_field_names, lineage, losing_stream_declare,
    never_inline,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_losing_stream_declare_leaves_no_framing_schema_behind() {
    let fx = PgFixture::shared();
    let (_cp, _db, _wh, catalog, pool) = harness(fx).await;
    let table = TableRef {
        schema: "s".into(),
        name: "t".into(),
    };

    let res = losing_stream_declare(&pool, &catalog, &table, 2).await;
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

/// The other half of "framing present **iff** the mirror says stream". The test above
/// pins the batch direction under a lost race; this pins the stream direction, so
/// neither can be satisfied by a fix that simply never writes framing.
///
/// No barrier needed: the declaration commits first, so a following plain batch land
/// must observe the committed stream mode and agree with it. (This direction already
/// held before the fix — it is a characterization test guarding the restructure, not
/// the regression gate. The gate is the test above.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_land_after_a_declaration_agrees_with_the_committed_stream_mode() {
    let fx = PgFixture::shared();
    let (_cp, _db, _wh, catalog, pool) = harness(fx).await;
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

    // The mirror says stream. `stream.stream_table` is keyed by `table_id`, so it is
    // joined through the LIVE mirror row rather than queried by name; `bucket_count`
    // is `int not null`, so an absent row (not a null) is what "batch" looks like.
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
