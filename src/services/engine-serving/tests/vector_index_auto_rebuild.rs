//! End-to-end freshness test: a just-flushed vector goes MISSING from k-NN at the
//! post-flush snapshot, then becomes visible again once the auto-enqueued rebuild runs.
//! Proves the slice's user-visible guarantee across the real k-NN read path.

use control_plane_core::IndexSpec;
use control_plane_postgres::fixture::PgFixture;
use engine_serving::VectorQuery;
use loom_test_seed::{hot_limits, ids_i64, land_vec4, seed_docs_vector};

/// Terse `VectorQuery` builder for the call sites in this file.
fn vq<'a>(
    table: &'a TableRef,
    index_name: &'a str,
    query: &'a [f32],
    k: usize,
    nprobe: Option<u32>,
    ef_search: Option<u32>,
) -> VectorQuery<'a> {
    VectorQuery {
        table,
        index_name,
        query,
        k,
        nprobe,
        ef_search,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flushed_vector_is_missing_then_restored_by_auto_rebuild() {
    use control_plane_core::{Metric, RunId, TableRef};
    use control_plane_postgres::iceberg_flush::flush_table;
    use control_plane_postgres::vector_index::build_vector_index;

    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    // 1. Cold rows 1-4 + flat index built at covered_snapshot S.
    let s = seed_docs_vector(fx, &db, "by_flat", Metric::Cosine, IndexSpec::Flat, true).await;

    // 2. Land row 5 INLINE (born after S) — the strictly-nearest vector to the query,
    //    living only in the hot delta. (Mirrors knn_cold_hot_merge_cosine.)
    land_vec4(&s, &[(5, [0.95, 0.05, 0.0, 0.0])], hot_limits()).await;

    let q = &[0.9_f32, 0.1, 0.0, 0.0];

    // Sanity (hot-delta merge): row 5 is the nearest and visible BEFORE the flush.
    let hot =
        engine_serving::vector_search(&s.catalog, &s.pool, vq(&table, "by_flat", q, 2, None, None))
            .await
            .expect("knn pre-flush");
    assert_eq!(ids_i64(&hot)[0], 5, "inline row is nearest before flush");

    // 3. Flush: drains row 5 to cold Parquet, end-caps it, AND auto-enqueues a rebuild.
    flush_table(&s.catalog, &s.pool, &table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");

    // 4. GAP (the bug this slice fixes): row 5 left the hot delta (end-capped) and is not
    //    in the cold index (built at the older S) -> missing from k-NN.
    let gap =
        engine_serving::vector_search(&s.catalog, &s.pool, vq(&table, "by_flat", q, 2, None, None))
            .await
            .expect("knn post-flush");
    assert!(
        !ids_i64(&gap).contains(&5),
        "just-flushed row is in the visibility gap"
    );

    // 5. Drain the auto-enqueued rebuild (simulate the worker): read the enqueued job's
    //    index_name and run the build. The fetch_one FAILS without this slice (no job).
    let index_name: String =
        sqlx::query_scalar("select payload->>'index_name' from queue.jobs where kind = $1")
            .bind(control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND)
            .fetch_one(&s.pool)
            .await
            .expect("a rebuild job was auto-enqueued by the flush");
    assert_eq!(index_name, "by_flat");
    build_vector_index(
        &s.catalog,
        &s.pool,
        &table,
        &index_name,
        RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("rebuild");

    // 6. Fresh again: the rebuilt cold index (covered_snapshot advanced past the flush)
    //    once more makes row 5 the nearest.
    let fresh =
        engine_serving::vector_search(&s.catalog, &s.pool, vq(&table, "by_flat", q, 2, None, None))
            .await
            .expect("knn post-rebuild");
    assert_eq!(
        ids_i64(&fresh)[0],
        5,
        "auto-rebuild restored the flushed row"
    );
}
