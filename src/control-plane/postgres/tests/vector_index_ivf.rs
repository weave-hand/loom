//! IVF build: select IndexSpec::IvfFlat, assert the Puffin blob decodes to an
//! ivf_flat index, the mirror row records index_kind = "ivf_flat", and a search
//! over the decoded index returns the exact match when every cluster is probed.

use loom_test_seed::{local_sql_catalog, test_lineage, vec4_columns, vec4_ipc};

use control_plane_core::{
    ControlPlane, IndexSpec, Metric, ObjectType, PropertyDef, RunId, TableRef, TypeName,
    VectorIndexDef, VectorKey,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::puffin::read_vector_index;
use control_plane_postgres::vector_index::{build_vector_index, lookup_vector_index};
use iceberg::io::FileIO;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivf_build_writes_decodable_blob_and_mirror_kind() {
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
        .define_type(ObjectType {
            name: TypeName("Docs".into()),
            table: table.clone(),
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
        })
        .await
        .expect("define_type");

    cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: "by_ivf".into(),
            type_name: TypeName("Docs".into()),
            property: "embedding".into(),
            metric: Metric::Cosine,
            spec: IndexSpec::IvfFlat { nlist: Some(2) },
        })
        .await
        .expect("define_vector_index");

    let run = RunId(uuid::Uuid::new_v4());
    let rows: &[(i64, [f32; 4])] = &[
        (1, [1.0, 0.0, 0.0, 0.0]),
        (2, [0.0, 1.0, 0.0, 0.0]),
        (3, [0.0, 0.0, 1.0, 0.0]),
        (4, [0.0, 0.0, 0.0, 1.0]),
    ];
    land(
        &pool,
        &catalog,
        &table,
        &vec4_columns(),
        &vec4_ipc(rows),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        test_lineage(run, &table),
    )
    .await
    .expect("land rows");

    // Build with IVF (nlist=2 over 4 rows).
    let build_run = RunId(uuid::Uuid::new_v4());
    let built = build_vector_index(&catalog, &pool, &table, "by_ivf", build_run)
        .await
        .expect("build ivf");
    assert_eq!(built.row_count, 4);

    // Mirror row records the IVF kind.
    let mut conn = pool.acquire().await.expect("acquire");
    let table_id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
        "select table_id from iceberg_mirror.\"table\" \
         where table_namespace = 'wh' and table_name = 'docs' and end_snapshot is null",
    ))
    .fetch_one(&mut *conn)
    .await
    .expect("table_id");
    let found = lookup_vector_index(&pool, table_id, "by_ivf", built.covered_snapshot)
        .await
        .expect("lookup")
        .expect("Some");
    assert_eq!(found.index_kind, "ivf_flat");

    // The blob decodes polymorphically to an ivf_flat index that searches.
    let file_io = FileIO::new_with_fs();
    let idx = read_vector_index(&file_io, &built.puffin_path)
        .await
        .expect("read");
    assert_eq!(idx.index_kind(), control_plane_core::IndexKind::IvfFlat);
    assert_eq!(idx.dim(), 4);
    assert_eq!(idx.row_count(), 4);
    // With 2 clusters, probing default nprobe may miss; but id=1 is its own
    // cluster's nearest, and nprobe>=1 always probes the query's own centroid.
    let res = idx.search(&[1.0, 0.0, 0.0, 0.0], 1);
    assert_eq!(res[0].0, VectorKey::Int(1));
}
