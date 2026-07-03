//! Headline acceptance: two named indexes (HNSW/Cosine and IVF-Flat/L2) on the
//! same vector(8) property build independently into distinct Puffin blobs and
//! each decodes + searches correctly by name.

use loom_test_seed::{local_sql_catalog, test_lineage};
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, IndexSpec, Metric, ObjectType, PropertyDef, RunId, TableRef,
    TypeName, VectorIndexDef, VectorKey,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::puffin::read_vector_index;
use control_plane_postgres::vector_index::{build_vector_index, lookup_vector_index};
use iceberg::io::FileIO;

fn columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "embedding".into(),
            ty: "vector(8)".into(),
            nullable: false,
        },
    ]
}

fn ipc_body(rows: &[(i64, [f32; 8])]) -> Vec<u8> {
    let element = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(element.clone());
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    for (_, emb) in rows {
        lb.values().append_slice(emb);
        lb.append(true);
    }
    let id_array = Int64Array::from(ids);
    let emb_array = lb.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", DataType::List(element), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(emb_array)],
    )
    .expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_named_indexes_on_one_property_build_and_search_independently() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "document".into(),
    };

    // Define the Document type with id + embedding(8) properties.
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Document".into()),
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
                    ty: "vector(8)".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .expect("define_type");

    // Declare two named indexes on the same property with different kinds and metrics.
    cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: "by_sim".into(),
            type_name: TypeName("Document".into()),
            property: "embedding".into(),
            metric: Metric::Cosine,
            spec: IndexSpec::Hnsw {
                m: Some(16),
                ef_construction: Some(200),
            },
        })
        .await
        .expect("define by_sim");

    cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: "by_cluster".into(),
            type_name: TypeName("Document".into()),
            property: "embedding".into(),
            metric: Metric::L2,
            spec: IndexSpec::IvfFlat { nlist: Some(4) },
        })
        .await
        .expect("define by_cluster");

    // Seed: 8 orthogonal unit vectors in R^8 (standard basis).
    // Cosine query e_1 = [1,0,...,0] → id=1 is nearest (similarity=1, all others=0).
    // L2 query e_8 = [0,...,0,1]     → id=8 is nearest (distance=0, all others=√2).
    let run = RunId(uuid::Uuid::new_v4());
    let rows: &[(i64, [f32; 8])] = &[
        (1, [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        (2, [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        (3, [0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        (4, [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0]),
        (5, [0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0]),
        (6, [0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0]),
        (7, [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0]),
        (8, [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0]),
    ];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(rows),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        test_lineage(run, &table),
    )
    .await
    .expect("land rows");

    // Build both named indexes independently.
    build_vector_index(
        &catalog,
        &pool,
        &table,
        "by_sim",
        RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("build by_sim");
    build_vector_index(
        &catalog,
        &pool,
        &table,
        "by_cluster",
        RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("build by_cluster");

    // Resolve the live mirror table_id.
    let mut conn = pool.acquire().await.expect("acquire");
    let table_id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
        "select table_id from iceberg_mirror.\"table\" \
         where table_namespace = 'wh' and table_name = 'document' and end_snapshot is null",
    ))
    .fetch_one(&mut *conn)
    .await
    .expect("table_id");
    drop(conn);

    // Each name resolves to a distinct mirror row with the correct kind and metric.
    let sim = lookup_vector_index(&pool, table_id, "by_sim", i64::MAX)
        .await
        .expect("lookup by_sim")
        .expect("Some");
    assert_eq!(sim.index_kind, "hnsw");
    assert_eq!(sim.metric, "cosine");

    let clus = lookup_vector_index(&pool, table_id, "by_cluster", i64::MAX)
        .await
        .expect("lookup by_cluster")
        .expect("Some");
    assert_eq!(clus.index_kind, "ivf_flat");
    assert_eq!(clus.metric, "l2");

    assert_ne!(
        sim.puffin_path, clus.puffin_path,
        "each named index has its own blob"
    );

    // Each blob decodes + searches independently.
    let file_io = FileIO::new_with_fs();

    // HNSW/Cosine: query = e_1 → nearest is id=1 (cosine similarity = 1.0).
    let idx_sim = read_vector_index(&file_io, &sim.puffin_path)
        .await
        .expect("read by_sim");
    assert_eq!(idx_sim.index_kind(), control_plane_core::IndexKind::Hnsw);
    assert_eq!(idx_sim.dim(), 8);
    assert_eq!(idx_sim.row_count(), 8);
    let res_sim = idx_sim.search(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 1);
    assert_eq!(res_sim[0].0, VectorKey::Int(1));

    // IVF-Flat/L2: query = e_8 → nearest is id=8 (L2 distance = 0).
    let idx_clus = read_vector_index(&file_io, &clus.puffin_path)
        .await
        .expect("read by_cluster");
    assert_eq!(
        idx_clus.index_kind(),
        control_plane_core::IndexKind::IvfFlat
    );
    assert_eq!(idx_clus.dim(), 8);
    assert_eq!(idx_clus.row_count(), 8);
    let res_clus = idx_clus.search(&[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0], 1);
    assert_eq!(res_clus[0].0, VectorKey::Int(8));
}
