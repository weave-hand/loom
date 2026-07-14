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
//! Seeding mirrors `vector_index_hnsw.rs`, except `InlineLimits` forces the inline
//! branch (`inline_byte_limit: usize::MAX`) and disarms the byte-flush
//! (`flush_byte_threshold: i64::MAX`), so no Parquet file is ever written.

use loom_test_seed::{local_sql_catalog, test_lineage, vec4_batches, vec4_columns};

use control_plane_core::{
    ControlPlane, IndexSpec, Metric, ObjectType, PropertyDef, RunId, TableRef, TypeName,
    VectorIndexDef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use control_plane_postgres::vector_index::build_vector_index;

fn table() -> TableRef {
    TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    }
}

/// Define the `Docs` type over `wh.docs` plus a `Flat` vector index on `embedding`.
async fn define_docs(cp: &impl ControlPlane) {
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Docs".into()),
            table: table(),
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
                PropertyDef {
                    name: "embedding".into(),
                    ty: "vector(4)".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
            version: None,
        })
        .await
        .expect("define_type");

    cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: "by_flat".into(),
            type_name: TypeName("Docs".into()),
            property: "embedding".into(),
            metric: Metric::Cosine,
            spec: IndexSpec::Flat,
        })
        .await
        .expect("define_vector_index");
}

/// Land three vectors INLINE ONLY: no Parquet file, and therefore no
/// `iceberg_tables` row for the table. `stream_buckets` declares the table a
/// stream table on this, its first write — the only legal moment (a landed batch
/// table can never become one: `reconcile_stream_mode`).
async fn land_inline_only(pool: &sqlx::PgPool, catalog: &SqlCatalog, stream_buckets: Option<i32>) {
    let run = RunId(uuid::Uuid::new_v4());
    let rows: &[(i64, [f32; 4])] = &[
        (1, [1.0, 0.0, 0.0, 0.0]),
        (2, [0.0, 1.0, 0.0, 0.0]),
        (3, [0.0, 0.0, 1.0, 0.0]),
    ];
    let (schema, batches) = vec4_batches(rows);
    land(
        pool,
        catalog,
        &table(),
        &vec4_columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        test_lineage(run, &table()),
        stream_buckets,
    )
    .await
    .expect("inline land (no flush, no Parquet)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_over_an_inline_only_table_reads_the_hot_tier() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    define_docs(&cp).await;
    land_inline_only(&pool, &catalog, None).await;

    // Pre-fix this dies in one of the two `catalog.load_table` calls ("No such
    // table: wh.docs"), even though the file list is empty and every row is
    // sitting in the hot inline tier.
    let built = build_vector_index(
        &catalog,
        &pool,
        &table(),
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
/// is the build, not the flush, that fixes the created Iceberg SCHEMA. For a declared
/// stream table that schema must carry the three reserved framing columns, because a
/// later `flush_table` writes FRAMED Parquet and its own `ensure_iceberg_table` no-ops
/// on the already-created table. A build that created an unframed schema here would
/// leave the flush appending framed files against a framing-free Iceberg schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declared_stream_table_stays_flushable_after_an_inline_only_build() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    define_docs(&cp).await;
    // Declared a stream table by its first (inline-only) write: its physical schema
    // carries the log framing, which every subsequent Parquet write emits.
    land_inline_only(&pool, &catalog, Some(4)).await;

    let built = build_vector_index(
        &catalog,
        &pool,
        &table(),
        "by_flat",
        RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("build over the inline-only stream table");
    assert_eq!(built.row_count, 3, "the three inline rows were indexed");

    // The Iceberg table the build created must accept the flush's framed Parquet.
    let snap = flush_table(&catalog, &pool, &table(), RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush after an inline-only build")
        .expect("the three live inline rows flushed into a Parquet snapshot");
    assert!(snap.0 > 0, "the flush advanced the mirror snapshot");
}
