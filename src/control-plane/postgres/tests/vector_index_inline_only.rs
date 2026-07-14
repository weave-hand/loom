//! `build_vector_index` over a table that has ONLY ever been inline-appended —
//! no Parquet file, and therefore no row in the vendored `iceberg_tables`
//! catalog (that row is minted by the first Parquet write, i.e. at flush).
//!
//! `collect_vectors` explicitly supports inline-only data — it unions the hot
//! inline tier into the cold file tier — but the build hit `catalog.load_table`
//! on TWO paths that an un-flushed table cannot satisfy: `read_files_as_batches`
//! loads the table BEFORE it iterates the (possibly empty) path list, and
//! `write_sidecar` loads it for the Puffin path's metadata location + `FileIO`.
//! Same shape as `mv_delta_locked` (`iss-mv-delta-inline-source-unflushed`, #436)
//! and `consolidate_locked`'s CDC arm (`iss-consolidate-inline-only-base`).
//!
//! Reachable via the engine's `BuildVectorIndex` RPC, and note `overwrite_truncate`
//! enqueues rebuild jobs *without* creating an Iceberg table.
//!
//! Seeded off `loom_test_seed`'s canonical `wh.docs` world, but with
//! `seed_docs_world` (the type only, NO landing) + `hot_limits` — `seed_docs_table`
//! forces its rows to Parquet, which is the one state these tests must avoid.

use control_plane_core::{IndexSpec, Metric, RunId};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::vector_index::build_vector_index;
use loom_test_seed::{
    VectorSeed, define_docs_index, hot_limits, land_vec4, land_vec4_declaring, seed_docs_world,
};

/// The three vectors every case indexes, landed INLINE ONLY.
const ROWS: &[(i64, [f32; 4])] = &[
    (1, [1.0, 0.0, 0.0, 0.0]),
    (2, [0.0, 1.0, 0.0, 0.0]),
    (3, [0.0, 0.0, 1.0, 0.0]),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_over_an_inline_only_table_reads_the_hot_tier() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let s: VectorSeed = seed_docs_world(fx, &db).await;
    define_docs_index(&s, "by_flat", Metric::Cosine, IndexSpec::Flat).await;
    land_vec4(&s, ROWS, hot_limits()).await;

    // Pre-fix this dies in one of the two `catalog.load_table` calls ("No such
    // table: wh.docs"), even though the file list is empty and every row is
    // sitting in the hot inline tier.
    let built = build_vector_index(
        &s.catalog,
        &s.pool,
        &s.table,
        "by_flat",
        RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("an inline-only table must index off its hot tier, with no flush");

    assert_eq!(
        built.row_count, 3,
        "all three inline rows were indexed — the hot tier is the whole table"
    );
    assert!(
        !built.puffin_path.is_empty(),
        "the build wrote a Puffin index blob for an inline-only table"
    );
}

/// The build materializes the Iceberg table so it can mint the sidecar path — so it
/// is the BUILD, not the flush, that fixes the created Iceberg schema. For a declared
/// stream table that schema must carry the three reserved framing columns, because a
/// later `flush_table` writes FRAMED Parquet and its own `ensure_iceberg_table` no-ops
/// on the already-created table. A build that created an unframed schema here would
/// leave the flush appending framed files against a framing-free Iceberg schema
/// (forcing `include_framing = false` fails this test with `schema evolution
/// unsupported: column "loom_change_kind" was dropped`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declared_stream_table_stays_flushable_after_an_inline_only_build() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let s: VectorSeed = seed_docs_world(fx, &db).await;
    define_docs_index(&s, "by_flat", Metric::Cosine, IndexSpec::Flat).await;
    // Declare the stream AS PART OF the first write — the only legal moment
    // (`reconcile_stream_mode` refuses to convert an already-landed batch table).
    // Its physical schema now carries the log framing every Parquet write emits.
    land_vec4_declaring(&s, ROWS, hot_limits(), Some(4)).await;

    let built = build_vector_index(
        &s.catalog,
        &s.pool,
        &s.table,
        "by_flat",
        RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("build over the inline-only stream table");
    assert_eq!(built.row_count, 3, "the three inline rows were indexed");

    // The Iceberg table the build created must accept the flush's framed Parquet.
    let snap = flush_table(&s.catalog, &s.pool, &s.table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush after an inline-only build")
        .expect("the three live inline rows flushed into a Parquet snapshot");
    assert!(snap.0 > 0, "the flush advanced the mirror snapshot");
}
