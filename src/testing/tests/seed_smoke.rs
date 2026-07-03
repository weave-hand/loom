//! Smoke for `loom_test_seed`: the shared vector seed prologue produces the
//! same wh.docs world the per-file copies did — type defined, 4 cold rows
//! landed, flat index built — and the batch/extractor helpers round-trip.

use control_plane_core::{IndexSpec, Metric};
use control_plane_postgres::fixture::PgFixture;
use loom_test_seed::{
    assert_knn, hot_limits, ids_i64, land_vec4, seed_docs_vector, vec4_batches, vec4_columns,
};

#[test]
fn vec4_batches_round_trips_ids() {
    let (schema, batches) = vec4_batches(&[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])]);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].schema(), schema);
    assert_eq!(ids_i64(&batches[0]), vec![1, 2]);
    assert_eq!(vec4_columns().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seed_docs_vector_seeds_and_builds() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    // build = true exercises define_type + 2 cold lands + define_vector_index
    // + build_vector_index end-to-end (each step expects internally).
    let s = seed_docs_vector(fx, &db, "by_flat", Metric::Cosine, IndexSpec::Flat, true).await;
    assert_eq!(s.table.schema, "wh");
    assert_eq!(s.table.name, "docs");
    // Hot landing through the same seed works too.
    land_vec4(&s, &[(5, [0.9, 0.1, 0.0, 0.0])], hot_limits()).await;
    // The pool is live and points at the seeded db.
    let one: i64 = sqlx::query_scalar("select 1::bigint")
        .fetch_one(&s.pool)
        .await
        .expect("select 1");
    assert_eq!(one, 1);
}

#[test]
fn assert_knn_checks_the_four_shared_predicates() {
    use std::sync::Arc;
    let batch = arrow_array::RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("_distance", arrow_schema::DataType::Float32, false),
        ])),
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![5, 1])),
            Arc::new(arrow_array::Float32Array::from(vec![0.1_f32, 0.2])),
        ],
    )
    .expect("batch");
    assert_knn(&batch, 5, 2);
}
