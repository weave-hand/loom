//! Inline (hot-tier) round-trip for a vector(N) column: write via inline_append,
//! read back via inline_live_batch, assert bit-exact f32 reconstruction.
//! loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Array, Float32Array, Int64Array, ListArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, DatasetId, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline::inline_append;

fn columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "embedding".into(),
            ty: "vector(4)".into(),
            nullable: false,
        },
    ]
}

fn batch(rows: &[(i64, [f32; 4])]) -> RecordBatch {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(item.clone());
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    for (_, e) in rows {
        lb.values().append_slice(e);
        lb.append(true);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", DataType::List(item), false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(ids)), Arc::new(lb.finish())],
    )
    .expect("batch")
}

fn lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

/// Columns with a nullable embedding for the null-vector test.
fn nullable_columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "embedding".into(),
            ty: "vector(4)".into(),
            nullable: true,
        },
    ]
}

/// Batch with two rows: row 0 has a real vector; row 1 has a NULL vector.
fn nullable_batch() -> RecordBatch {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(item.clone());
    // row 0: non-null vector
    lb.values().append_slice(&[1.0_f32, 2.0, 3.0, 4.0]);
    lb.append(true);
    // row 1: null vector — append no values, then append(false) for a null list
    lb.append(false);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", DataType::List(item), true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![10_i64, 11])),
            Arc::new(lb.finish()),
        ],
    )
    .expect("nullable_batch")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_vector_null_row_round_trips() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = TableRef {
        schema: "wh".into(),
        name: "docs_nullable".into(),
    };

    let snap = inline_append(
        &pool,
        &table,
        &nullable_columns(),
        &nullable_batch(),
        lineage(RunId(uuid::Uuid::new_v4()), &table),
        None,
    )
    .await
    .expect("inline_append nullable vector rows");

    let cat = IcebergCatalog::new(pool.clone());
    let (_tid, ids, out) = cat
        .inline_live_batch(&table, snap)
        .await
        .expect("inline_live_batch")
        .expect("live rows exist");

    assert_eq!(ids.len(), 2, "two live inline rows");

    let id_col = out
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id Int64");
    assert_eq!(id_col.value(0), 10);
    assert_eq!(id_col.value(1), 11);

    let emb_col = out
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("embedding List");

    // Row 0: non-null, bit-exact reconstruction.
    assert!(!emb_col.is_null(0), "row 0 must be non-null");
    let row0 = emb_col.value(0);
    let f0 = row0
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("child Float32");
    assert_eq!(f0.values(), &[1.0_f32, 2.0, 3.0, 4.0], "row 0 bit-exact");

    // Row 1: null list.
    assert!(emb_col.is_null(1), "row 1 must be null");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_vector_round_trips_bit_exact() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let rows: &[(i64, [f32; 4])] = &[(1, [0.1, 0.2, 0.3, 0.4]), (2, [-1.5, 0.0, 3.25, 9.0])];

    let snap = inline_append(
        &pool,
        &table,
        &columns(),
        &batch(rows),
        lineage(RunId(uuid::Uuid::new_v4()), &table),
        None,
    )
    .await
    .expect("inline_append vector rows");

    let cat = IcebergCatalog::new(pool.clone());
    let (_tid, ids, out) = cat
        .inline_live_batch(&table, snap)
        .await
        .expect("inline_live_batch")
        .expect("live rows exist");

    assert_eq!(ids.len(), 2, "two live inline rows");

    let id_col = out
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id Int64");
    assert_eq!(
        (0..2).map(|i| id_col.value(i)).collect::<Vec<_>>(),
        vec![1, 2]
    );

    let v = out
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("embedding List");
    for (i, (_, expect)) in rows.iter().enumerate() {
        let row = v.value(i);
        let f = row
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("child Float32");
        assert_eq!(f.values(), expect, "row {i} vector bit-exact");
    }
}
