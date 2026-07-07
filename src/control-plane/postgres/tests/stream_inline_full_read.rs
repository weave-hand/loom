//! `inline_live_batch_full` is the changelog-flush counterpart to
//! `inline_live_batch`: it reads the same live inline rows but does NOT exclude
//! `-U` before-images, so a flush can carry every framing row (including `-U`)
//! into the changelog. This proves the contrast directly: after an insert + an
//! update on a CDC table (which emits a `-U`/`+U` pair), `inline_live_batch`
//! returns rows whose `loom_change_kind` set contains no `-U`, while
//! `inline_live_batch_full` returns a superset that does contain a `-U` row.
//! loom_fixture_test (Postgres).

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, EventType, LineageEvent, RunId, StreamTables, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};

fn table() -> TableRef {
    TableRef {
        schema: "sales".to_string(),
        name: "orders".to_string(),
    }
}

fn id_spec() -> ColumnSpec {
    ColumnSpec {
        name: "id".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

fn val_spec() -> ColumnSpec {
    ColumnSpec {
        name: "val".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

/// A one-cell batch holding just the id column (`long`).
fn id_batch(v: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).expect("id batch")
}

/// A full one-row `{id, val}` batch (used as before/after images).
fn full_row_batch(id: i64, val: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![val])),
        ],
    )
    .expect("full row batch")
}

fn lin() -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "test" }),
    }
}

/// `loom_change_kind` for a set of `loom_row_id`s, read back from `inline_<tid>`
/// -- mirrors `stream_cdc_emission.rs::rows_by_offset`'s framing readback, but
/// keyed by row id (what `inline_live_batch`/`inline_live_batch_full` return)
/// rather than offset.
async fn change_kinds_for(pool: &sqlx::PgPool, tid: i64, row_ids: &[i64]) -> HashSet<String> {
    let ids = row_ids
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let rows: Vec<(Option<String>,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "select loom_change_kind from iceberg_mirror.inline_{tid} where loom_row_id in ({ids})"
    )))
    .fetch_all(pool)
    .await
    .expect("change-kind readback");
    rows.into_iter()
        .map(|(k,)| k.unwrap_or_else(|| "+I".to_string()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_read_includes_minus_u_that_live_batch_excludes() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec(), val_spec()];
    let bucket_count = 1;

    // Declare the table CDC (keyed on `id`) BEFORE any write, mirroring
    // stream_cdc_emission.rs.
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, bucket_count, "id")
        .await
        .expect("declare_cdc");

    // Seed the initial live row id=1, val=100 (a plain +I append).
    iceberg_inline::inline_append(
        &pool,
        &table,
        &cols,
        &full_row_batch(1, 100),
        lin(),
        None,
        None,
    )
    .await
    .expect("seed append");

    let v0 =
        iceberg_inline::current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
            .await
            .expect("version after seed");

    // UPDATE id=1: val 100 -> 200 -- emits a (-U before-image, +U after-image) pair.
    let before_update = full_row_batch(1, 100);
    let after_update = full_row_batch(1, 200);
    let at_update = iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &after_update,
        Some((&cols, &before_update)),
        lin(),
        v0,
    )
    .await
    .expect("cdc update delta");

    let catalog = IcebergCatalog::new(pool.clone());

    // `inline_live_batch` excludes -U: its row set's change-kinds contain no "-U".
    let (live_tid, live_row_ids, _live_batch) = catalog
        .inline_live_batch(&table, at_update)
        .await
        .expect("inline_live_batch must not error")
        .expect("a live row must be present");
    assert_eq!(live_tid, tid, "inline_live_batch resolves the same table id");
    let live_kinds = change_kinds_for(&pool, tid, &live_row_ids).await;
    assert!(
        !live_kinds.contains("-U"),
        "inline_live_batch must never surface a -U row: {live_kinds:?}"
    );

    // `inline_live_batch_full` is a superset that DOES contain a -U row.
    let (full_tid, full_row_ids, _full_batch) = catalog
        .inline_live_batch_full(&table, at_update)
        .await
        .expect("inline_live_batch_full must not error")
        .expect("live rows must be present");
    assert_eq!(
        full_tid, tid,
        "inline_live_batch_full resolves the same table id"
    );
    let full_kinds = change_kinds_for(&pool, tid, &full_row_ids).await;
    assert!(
        full_kinds.contains("-U"),
        "inline_live_batch_full must surface the -U before-image: {full_kinds:?}"
    );

    assert!(
        full_row_ids.len() > live_row_ids.len(),
        "the full read is a strict superset of the live read: full={full_row_ids:?} live={live_row_ids:?}"
    );
    let live_set: HashSet<i64> = live_row_ids.iter().copied().collect();
    assert!(
        live_row_ids.iter().all(|id| full_row_ids.contains(id)),
        "every live row id must also appear in the full read"
    );
    assert!(
        full_row_ids.iter().any(|id| !live_set.contains(id)),
        "the full read must contain at least one row id absent from the live read"
    );
}
