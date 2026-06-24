//! End-to-end regression for the inline PG TableProvider: file+inline UNION,
//! time-travel MVCC via the base predicate, and WHERE/LIMIT pushdown.

use control_plane_core::{SnapshotId, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use query_api::serving::SqlValue;
use query_api::serving_datafusion::batches_to_rows;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unions_file_and_inline_rows() {
    // Seed >=1 Parquet file row AND >=1 live inline row in the same table, then
    // SELECT * and assert the row count == file_rows + inline_rows, and that an
    // inline-only id is present.
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    // 3 file rows: id 0, 1, 2
    writer.seed("sales", "orders", &cols, &[3]).await;
    // 1 inline row: id 200
    writer
        .inline(
            "sales",
            "orders",
            &cols,
            &[(200, "inline_row")],
            uuid::Uuid::new_v4(),
        )
        .await;

    // was: DataFusionServingEngine::new(IcebergCatalog::new(pool)).fetch_rows(sql, &[])
    let catalog = IcebergCatalog::new(pool);
    let sql = "SELECT \"id\", \"name\" FROM \"sales\".\"orders\" ORDER BY \"id\"";
    let inlined = query_api::serving::inline_params(sql, &[]);
    let rows = batches_to_rows(
        engine_serving::execute_query(&catalog, &inlined, None)
            .await
            .expect("execute_query"),
    );

    // 3 file rows (0,1,2) + 1 inline row (200), unioned.
    let ids: Vec<i64> = rows
        .rows
        .iter()
        .map(|r| match &r[0] {
            SqlValue::Int(n) => *n,
            other => panic!("id not int: {other:?}"),
        })
        .collect();
    assert_eq!(
        ids,
        vec![0, 1, 2, 200],
        "file rows and inline row must all appear in UNION result"
    );
    assert!(
        ids.contains(&200),
        "inline-only id 200 must be present in result"
    );
    assert_eq!(
        rows.rows.last().unwrap()[1],
        SqlValue::Text("inline_row".into()),
        "the inline row's name reads back correctly"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn time_travel_excludes_rows_added_after_snapshot() {
    // Assert MVCC at the catalog layer (option a from the brief):
    // inline_live_batch uses the same base predicate the PG TableProvider uses.
    // 1. Inline row A -> snap_a (snapshot id returned by inline()).
    // 2. Inline row B -> snap_b (> snap_a).
    // 3. inline_live_batch at snap_a -> 1 row (only A).
    // 4. inline_live_batch at snap_b -> 2 rows (A and B).
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];

    let snap_a = writer
        .inline(
            "events",
            "log",
            &cols,
            &[(1, "row_a")],
            uuid::Uuid::new_v4(),
        )
        .await;

    let snap_b = writer
        .inline(
            "events",
            "log",
            &cols,
            &[(2, "row_b")],
            uuid::Uuid::new_v4(),
        )
        .await;

    assert!(
        snap_b > snap_a,
        "snap_b ({snap_b}) must be > snap_a ({snap_a})"
    );

    let catalog = IcebergCatalog::new(pool);
    let table = TableRef {
        schema: "events".into(),
        name: "log".into(),
    };

    // At snap_a: only row A is live (row B's begin_snapshot > snap_a).
    let result_a = catalog
        .inline_live_batch(&table, SnapshotId(snap_a))
        .await
        .expect("inline_live_batch at snap_a");
    let (_tid_a, _row_ids_a, batch_a) =
        result_a.expect("batch must exist at snap_a (row A was written)");
    assert_eq!(
        batch_a.num_rows(),
        1,
        "at snap_a, exactly 1 row (row A) should be live"
    );

    // At snap_b: both A and B are live.
    let result_b = catalog
        .inline_live_batch(&table, SnapshotId(snap_b))
        .await
        .expect("inline_live_batch at snap_b");
    let (_tid_b, _row_ids_b, batch_b) =
        result_b.expect("batch must exist at snap_b (rows A and B were written)");
    assert_eq!(
        batch_b.num_rows(),
        2,
        "at snap_b, exactly 2 rows (A and B) should be live"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pushdown_where_and_limit_return_correct_rows() {
    // Seed inline rows ids 1..=5, then verify WHERE and LIMIT filtering is correct.
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    // Inline all 5 rows in one call.
    writer
        .inline(
            "push",
            "data",
            &cols,
            &[
                (1, "row1"),
                (2, "row2"),
                (3, "row3"),
                (4, "row4"),
                (5, "row5"),
            ],
            uuid::Uuid::new_v4(),
        )
        .await;

    // was: DataFusionServingEngine::new(IcebergCatalog::new(pool)).fetch_rows(sql, &[])
    let catalog = IcebergCatalog::new(pool);

    // WHERE id > 3 ORDER BY id -> {4, 5}
    let sql_where = "SELECT \"id\" FROM \"push\".\"data\" WHERE \"id\" > 3 ORDER BY \"id\"";
    let inlined_where = query_api::serving::inline_params(sql_where, &[]);
    let rows_where = batches_to_rows(
        engine_serving::execute_query(&catalog, &inlined_where, None)
            .await
            .expect("fetch_rows WHERE"),
    );
    let ids_where: Vec<i64> = rows_where
        .rows
        .iter()
        .map(|r| match &r[0] {
            SqlValue::Int(n) => *n,
            other => panic!("id not int: {other:?}"),
        })
        .collect();
    assert_eq!(
        ids_where,
        vec![4, 5],
        "WHERE id > 3 must return exactly {{4, 5}}"
    );

    // WHERE id > 3 LIMIT 1 -> exactly one row
    let sql_limit = "SELECT \"id\" FROM \"push\".\"data\" WHERE \"id\" > 3 ORDER BY \"id\" LIMIT 1";
    let inlined_limit = query_api::serving::inline_params(sql_limit, &[]);
    let rows_limit = batches_to_rows(
        engine_serving::execute_query(&catalog, &inlined_limit, None)
            .await
            .expect("fetch_rows LIMIT"),
    );
    assert_eq!(
        rows_limit.rows.len(),
        1,
        "LIMIT 1 must return exactly one row"
    );
    assert_eq!(
        rows_limit.rows[0][0],
        SqlValue::Int(4),
        "LIMIT 1 with ORDER BY id must return id=4 (smallest id > 3)"
    );
}
