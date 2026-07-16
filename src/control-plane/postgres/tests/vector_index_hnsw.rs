//! HNSW build: select IndexSpec::Hnsw, assert the Puffin blob decodes to an hnsw
//! index, the mirror row records index_kind = "hnsw", and a search over the
//! decoded index returns the nearest match.

use loom_test_seed::{local_sql_catalog, test_lineage, vec4_batches, vec4_columns};

use control_plane_core::{
    ControlPlane, IndexSpec, Metric, ObjectType, RunId, TableRef, VectorIndexDef, VectorKey,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::puffin::read_vector_index;
use control_plane_postgres::vector_index::{build_vector_index, lookup_vector_index};
use iceberg::io::FileIO;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hnsw_build_writes_decodable_blob_and_mirror_kind() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    cp.ontology()
        .define_type(
            ObjectType::build("Docs", ("wh", "docs"))
                .prop_req("id", "Long")
                .prop_req("embedding", "vector(4)")
                .identity("id")
                .done(),
        )
        .await
        .expect("define_type");

    cp.ontology()
        .define_vector_index(VectorIndexDef::new(
            "by_hnsw",
            "Docs",
            "embedding",
            Metric::Cosine,
            IndexSpec::Hnsw {
                m: None,
                ef_construction: None,
            },
        ))
        .await
        .expect("define_vector_index");

    let run = RunId(uuid::Uuid::new_v4());
    let rows: &[(i64, [f32; 4])] = &[
        (1, [1.0, 0.0, 0.0, 0.0]),
        (2, [0.0, 1.0, 0.0, 0.0]),
        (3, [0.0, 0.0, 1.0, 0.0]),
        (4, [0.0, 0.0, 0.0, 1.0]),
    ];
    let (schema, batches) = vec4_batches(rows);
    land(
        &pool,
        &catalog,
        &table,
        &vec4_columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        test_lineage(run, &table),
        None,
    )
    .await
    .expect("land rows");

    // Build with HNSW (defaults: m=16, ef_construction=200).
    let build_run = RunId(uuid::Uuid::new_v4());
    let built = build_vector_index(&catalog, &pool, &table, "by_hnsw", build_run)
        .await
        .expect("build hnsw");
    assert_eq!(built.row_count, 4);

    // Mirror row records the HNSW kind.
    let mut conn = pool.acquire().await.expect("acquire");
    let table_id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
        "select table_id from iceberg_mirror.\"table\" \
         where table_namespace = 'wh' and table_name = 'docs' and end_snapshot is null",
    ))
    .fetch_one(&mut *conn)
    .await
    .expect("table_id");
    let found = lookup_vector_index(&pool, table_id, "by_hnsw", built.covered_snapshot)
        .await
        .expect("lookup")
        .expect("Some");
    assert_eq!(found.index_kind, "hnsw");

    // The blob decodes polymorphically to an hnsw index that searches.
    let file_io = FileIO::new_with_fs();
    let idx = read_vector_index(&file_io, &built.puffin_path)
        .await
        .expect("read");
    assert_eq!(idx.index_kind(), control_plane_core::IndexKind::Hnsw);
    assert_eq!(idx.dim(), 4);
    assert_eq!(idx.row_count(), 4);
    let res = idx.search(&[1.0, 0.0, 0.0, 0.0], 1);
    assert_eq!(res[0].0, VectorKey::Int(1));
}
