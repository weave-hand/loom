//! Full-column governed read over the merged view with a live tombstone inline
//! row (iss-search-vector-merge-view-nullable, Defect B). A non-CDC DELETE's
//! inline row carries only the identity — every other data column is physically
//! NULL — and the merge fold NEEDS that row (identity + `loom_tombstone` hide the
//! file row). The inline tier's DECLARED schema must admit those NULLs, or the
//! scan batch fails arrow's non-nullable validation inside
//! `PgTableProvider::fetch_batch` and the read 500s before the fold ever drops the
//! tombstone. `/search` never trips this (its survivor post-filter projects only
//! the identity, so the vector column is pruned from the inline scan); any read
//! that materializes a REQUIRED non-identity column does.
//!
//! The merged view's SERVED schema still declares `embedding` non-nullable (the
//! final projection restores it above the tombstone filter) — that half is pinned
//! in `engine-serving/tests/merge_view_schema.rs`.
//!
//! Writes the inline deltas directly via `iceberg_inline` (the proven pattern from
//! `vector_search_cold_suppression_e2e.rs`). loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, DatasetId, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_inline;
use e2e_support::{get, grant_read, ids_i64, seed_vector_type, subject_with_role};

use axum::http::StatusCode;

/// The `wh.docs` table `seed_vector_type` lands into.
fn docs_table() -> TableRef {
    TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    }
}

/// The `Docs(id long, embedding vector(4))` inline column set — both REQUIRED.
fn docs_columns() -> Vec<ColumnSpec> {
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

/// A one-row `{id, embedding}` batch (Int64 `id` + `List<Float32>` `embedding`).
fn vec_batch(id: i64, e: [f32; 4]) -> RecordBatch {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(item.clone());
    lb.values().append_slice(&e);
    lb.append(true);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", DataType::List(item), false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![id])), Arc::new(lb.finish())],
    )
    .expect("vec_batch")
}

/// A one-cell batch holding just the `id` column — the CAS lookup key, and a
/// tombstone's carried batch (its `embedding` is NULL in `inline_<tid>`).
fn id_batch(id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id]))]).expect("id_batch")
}

fn lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "e2e" }),
    }
}

/// GET /objects/Docs with a live inline UPDATE (id=1) and a live inline tombstone
/// (id=2): 200, survivors [1, 3, 4], and id=1 serves the UPDATED embedding — the
/// merge winner's values flow through the widened inline tier and out through the
/// mirror-nullability-restoring final projection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn objects_read_with_live_tombstone_serves_survivors() {
    let fx = PgFixture::shared();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(fx, &db).await;
    let (_subj, role) = subject_with_role(&cp, "reader").await;
    grant_read(&cp, &role, "Docs").await;
    let cp = Arc::new(cp);

    let pool = fx.pool_for(&db).await;
    let table = docs_table();
    let columns = docs_columns();

    // Inline UPDATE on id=1: a full-row shadow with a NEW embedding.
    let v1 = iceberg_inline::current_inline_version(&pool, &table, &columns, "id", &id_batch(1))
        .await
        .unwrap();
    iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &columns,
        "id",
        false,
        &vec_batch(1, [0.9, 0.1, 0.0, 0.0]),
        None,
        lineage(RunId(uuid::Uuid::new_v4()), &table),
        v1,
        None,
        &[], // jobs — the 11th param
    )
    .await
    .unwrap();

    // Inline DELETE on id=2: an id-only tombstone (embedding NULL in inline_<tid>).
    let v2 = iceberg_inline::current_inline_version(&pool, &table, &columns, "id", &id_batch(2))
        .await
        .unwrap();
    iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &columns,
        "id",
        true,
        &id_batch(2),
        None,
        lineage(RunId(uuid::Uuid::new_v4()), &table),
        v2,
        None,
        &[], // jobs — the 11th param
    )
    .await
    .unwrap();

    // Full-column read: the projection materializes `embedding` from BOTH tiers,
    // including the tombstone row's NULL. Pre-fix: 500.
    let (status, body) = get(cp.clone(), serving.clone(), "/objects/Docs", "reader").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "full-column merged read succeeds with a live tombstone: {body}"
    );
    assert_eq!(
        ids_i64(&body),
        vec![1, 3, 4],
        "tombstoned id=2 hidden; survivors served: {body}"
    );

    // The merge winner's VALUES are intact. `id` is a Long -> NumericString, and a
    // vector(4) has no SqlValue variant -> ArrayFormatter -> SqlValue::Text, i.e. a
    // JSON *string*. (arrow_to_sqlvalue, serving_datafusion.rs.)
    let objects = body["objects"].as_array().expect("objects array");
    let one = objects
        .iter()
        .find(|o| o["id"] == serde_json::json!("1"))
        .unwrap_or_else(|| panic!("id=1 present: {body}"));
    let emb = one["embedding"]
        .as_str()
        .unwrap_or_else(|| panic!("a vector(4) renders as a JSON string (ArrayFormatter): {body}"));
    assert!(
        emb.starts_with("[0.9") && emb.contains("0.1"),
        "id=1 serves the UPDATED embedding [0.9, 0.1, 0.0, 0.0], got {emb}: {body}"
    );
}
