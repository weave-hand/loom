//! Additive happy-path (write side) + non-additive rejection, driven through the real
//! landing entrypoint `land` (inline limit 0 forces real Parquet). Asserts mirror state,
//! not reads (the read path is Task 6). Setup mirrors tests/iceberg_overwrite.rs.
use loom_test_seed::local_sql_catalog;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::Catalog;
use control_plane_core::{ColumnSpec, EventType, LineageEvent, RunId, SnapshotId, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};

fn col(name: &str, ty: &str, nullable: bool) -> ColumnSpec {
    ColumnSpec {
        name: name.into(),
        ty: ty.into(),
        nullable,
    }
}

fn lineage(run: RunId) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "schema-evolution-test" }),
    }
}

/// IPC body for (a long, b string) of `rows` rows.
fn ipc_ab(rows: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..rows).map(|i| format!("b{i}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    encode(&schema, &batch)
}

/// IPC body for (a long, b string, c long-nullable) of `rows` rows.
fn ipc_abc(rows: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
        Field::new("c", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..rows).map(|i| format!("b{i}")).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..rows).map(|i| i + 1000).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    encode(&schema, &batch)
}

/// IPC body for (a long) only — used to test a dropped column.
fn ipc_a(rows: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .unwrap();
    encode(&schema, &batch)
}

/// IPC body for (id long, name string, extra long-nullable) of `rows` rows — the
/// additive superset over the inline `(id, name)` schema.
fn ipc_id_name_extra(rows: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("extra", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..rows).map(|i| format!("name{i}")).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..rows).map(|i| i + 1000).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    encode(&schema, &batch)
}

fn encode(schema: &Arc<Schema>, batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn additive_land_evolves_mirror_and_bumps_schema_version() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().unwrap();
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef {
        schema: "s".into(),
        name: "t".into(),
    };
    let ab = vec![col("a", "long", false), col("b", "string", true)];
    let abc = vec![
        col("a", "long", false),
        col("b", "string", true),
        col("c", "long", true),
    ];

    let s1 = land(
        &pool,
        &catalog,
        &t,
        &ab,
        &ipc_ab(3),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4())),
    )
    .await
    .expect("base land");
    let s2 = land(
        &pool,
        &catalog,
        &t,
        &abc,
        &ipc_abc(2),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4())),
    )
    .await
    .expect("additive land");
    assert!(s2.0 > s1.0);

    // tid for the live table.
    let tid: i64 = sqlx::query_scalar(
        "select table_id from iceberg_mirror.table where table_namespace='s' and table_name='t' and end_snapshot is null",
    ).fetch_one(&pool).await.unwrap();

    // Three live columns at s2; c.begin_snapshot == s2.
    let live: Vec<(String, i64)> = sqlx::query_as(
        "select column_name, begin_snapshot from iceberg_mirror.column \
         where table_id=$1 and begin_snapshot <= $2 and (end_snapshot is null or end_snapshot > $2) \
         order by column_order",
    )
    .bind(tid)
    .bind(s2.0)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        live.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>(),
        ["a", "b", "c"]
    );
    let c_begin = live.iter().find(|(n, _)| n == "c").unwrap().1;
    assert_eq!(c_begin, s2.0, "c appears at the second snapshot");

    // schema_version bumped: s1 -> 1, s2 -> 2.
    let v1: i64 = sqlx::query_scalar(
        "select schema_version from iceberg_mirror.snapshot where snapshot_id=$1",
    )
    .bind(s1.0)
    .fetch_one(&pool)
    .await
    .unwrap();
    let v2: i64 = sqlx::query_scalar(
        "select schema_version from iceberg_mirror.snapshot where snapshot_id=$1",
    )
    .bind(s2.0)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((v1, v2), (1, 2));

    // The new file is live at s2 and readable (record_count from the additive batch).
    let ice = IcebergCatalog::new(pool.clone());
    let files = ice.files_with_stats(&t, s2).await.unwrap();
    assert_eq!(
        files.len(),
        2,
        "both the base and additive files are live at s2"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_additive_land_is_rejected_and_mirror_unchanged() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().unwrap();
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef {
        schema: "s".into(),
        name: "t".into(),
    };
    let ab = vec![col("a", "long", false), col("b", "string", true)];

    let s1 = land(
        &pool,
        &catalog,
        &t,
        &ab,
        &ipc_ab(3),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4())),
    )
    .await
    .expect("base land");

    // Drop b: land only (a long).
    let only_a = vec![col("a", "long", false)];
    let err = land(
        &pool,
        &catalog,
        &t,
        &only_a,
        &ipc_a(2),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4())),
    )
    .await
    .expect_err("dropping b must be rejected");
    assert!(
        err.to_string().contains("schema evolution unsupported"),
        "got: {err}"
    );

    // Add a required column: (a long, b string, d long NOT NULL).
    let abd_req = vec![
        col("a", "long", false),
        col("b", "string", true),
        col("d", "long", false),
    ];
    let body = {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("d", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0i64])),
                Arc::new(StringArray::from(vec!["b0"])),
                Arc::new(Int64Array::from(vec![7i64])),
            ],
        )
        .unwrap();
        encode(&schema, &batch)
    };
    let err2 = land(
        &pool,
        &catalog,
        &t,
        &abd_req,
        &body,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4())),
    )
    .await
    .expect_err("required new column must be rejected");
    assert!(
        err2.to_string().contains("schema evolution unsupported"),
        "got: {err2}"
    );

    // Mirror unchanged: still exactly the 2 columns at the still-current snapshot s1.
    let ice = IcebergCatalog::new(pool.clone());
    assert_eq!(
        ice.current_snapshot(&t).await.unwrap().id,
        s1,
        "rejected lands did not advance the snapshot"
    );
    let n: i64 = sqlx::query_scalar(
        "select count(*) from iceberg_mirror.column c \
         join iceberg_mirror.table tb on c.table_id = tb.table_id \
         where tb.table_namespace='s' and tb.table_name='t' and tb.end_snapshot is null and c.end_snapshot is null",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(n, 2, "still exactly columns a, b");
}

/// An additive Parquet land is rejected while un-flushed live inline rows exist: such a
/// land would project a new column into the mirror that the physical `inline_<tid>`
/// table lacks, so inline reconstruction (read AND flush) would later fail. The caller
/// must let the flush job run first. Guards the cross-task seam find-1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn additive_land_rejected_while_live_inline_rows_exist() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().unwrap();
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef {
        schema: "s".into(),
        name: "t".into(),
    };

    // Seed live inline rows for (id long, name string) — these accumulate in the physical
    // `inline_<tid>` table, which has DDL columns id,name only.
    let writer = IcebergWriter::new(pool.clone(), fx.pg_dsn(&db));
    let id_name = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    let snap = writer
        .inline(
            "s",
            "t",
            &id_name,
            &[(1, "one"), (2, "two")],
            uuid::Uuid::new_v4(),
        )
        .await;
    let s1 = SnapshotId(snap);

    // Attempt an ADDITIVE (id, name, extra) Parquet land while those inline rows are live.
    let abc = vec![
        col("id", "long", false),
        col("name", "string", false),
        col("extra", "long", true),
    ];
    let err = land(
        &pool,
        &catalog,
        &t,
        &abc,
        &ipc_id_name_extra(2),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4())),
    )
    .await
    .expect_err("additive land while live inline rows exist must be rejected");
    assert!(
        err.to_string().contains("schema evolution unsupported"),
        "got: {err}"
    );

    // Mirror unchanged: current snapshot did not advance and the inline rows are still live.
    let ice = IcebergCatalog::new(pool.clone());
    assert_eq!(
        ice.current_snapshot(&t).await.unwrap().id,
        s1,
        "rejected additive land did not advance the snapshot"
    );
    let tid: i64 = sqlx::query_scalar(
        "select table_id from iceberg_mirror.table where table_namespace='s' and table_name='t' and end_snapshot is null",
    ).fetch_one(&pool).await.unwrap();
    let live_inline: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select count(*) from iceberg_mirror.inline_{tid} where end_snapshot is null"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(live_inline, 2, "inline rows still live after the rejection");
    // The physical inline table still has only id,name (no `extra` column projected).
    let n_cols: i64 = sqlx::query_scalar(
        "select count(*) from iceberg_mirror.column c \
         join iceberg_mirror.table tb on c.table_id = tb.table_id \
         where tb.table_namespace='s' and tb.table_name='t' and tb.end_snapshot is null and c.end_snapshot is null",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(n_cols, 2, "still exactly columns id, name");
}
