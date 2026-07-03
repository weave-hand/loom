//! Identity-kind coverage for the cold+hot k-NN path. The ontology permits
//! String and Integer identity columns; `extract_rows` (build) supports them
//! on the cold tier, but the hot-delta leg hardcoded Int64
//! (iss-inline-delta-string-identity). The cold pin here is GREEN pre-fix;
//! Task 2 appends the *_cold_hot_merge red tests.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetId, EventType, IndexSpec, LineageEvent, Metric, ObjectType,
    PropertyDef, RunId, TableRef, TypeName, VectorIndexDef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use control_plane_postgres::vector_index::build_vector_index;
use engine_serving::VectorQuery;
use loom_test_seed::local_sql_catalog;

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

fn columns(id_ty: &str) -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: id_ty.into(),
            nullable: false,
        },
        ColumnSpec {
            name: "embedding".into(),
            ty: "vector(4)".into(),
            nullable: false,
        },
    ]
}

fn ipc_from(id_field: Field, id_array: Arc<dyn Array>, embs: &[[f32; 4]]) -> Vec<u8> {
    let element = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(element.clone());
    for emb in embs {
        lb.values().append_slice(emb);
        lb.append(true);
    }
    let schema = Arc::new(Schema::new(vec![
        id_field,
        Field::new("embedding", DataType::List(element), false),
    ]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![id_array, Arc::new(lb.finish())]).expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

fn ipc_str(rows: &[(&str, [f32; 4])]) -> Vec<u8> {
    let ids: Vec<&str> = rows.iter().map(|(id, _)| *id).collect();
    let embs: Vec<[f32; 4]> = rows.iter().map(|(_, e)| *e).collect();
    ipc_from(
        Field::new("id", DataType::Utf8, false),
        Arc::new(StringArray::from(ids)),
        &embs,
    )
}

fn object_type(name: &str, table: &TableRef, id_ty: &str) -> ObjectType {
    ObjectType {
        name: TypeName(name.into()),
        table: table.clone(),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: id_ty.into(),
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
    }
}

fn lineage_evt(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

/// Define the type + a flat cosine index, land `cold_ipc` as Parquet, build.
/// The warehouse TempDir is returned — the cold search reads its Parquet.
async fn seed_and_build(
    fx: &PgFixture,
    db: &str,
    table: &TableRef,
    type_name: &str,
    id_ty_logical: &str,
    id_ty_property: &str,
    cold_ipc: Vec<u8>,
) -> (SqlCatalog, sqlx::PgPool, tempfile::TempDir) {
    let pool = fx.pool_for(db).await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_secs(5));
    cp.ontology()
        .define_type(object_type(type_name, table, id_ty_property))
        .await
        .expect("define_type");
    cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: "by_flat".into(),
            type_name: TypeName(type_name.into()),
            property: "embedding".into(),
            metric: Metric::Cosine,
            spec: IndexSpec::Flat,
        })
        .await
        .expect("define_vector_index");
    land(
        &pool,
        &catalog,
        table,
        &columns(id_ty_logical),
        &cold_ipc,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage_evt(table),
    )
    .await
    .expect("land cold");
    build_vector_index(
        &catalog,
        &pool,
        table,
        "by_flat",
        RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("build_vector_index");
    (catalog, pool, wh)
}

/// Identity column of the result batch as strings (Utf8 output).
fn ids_str(batch: &RecordBatch) -> Vec<String> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("identity column is Utf8")
        .iter()
        .map(|v| v.expect("non-null id").to_string())
        .collect()
}

const COLD_STR: &[(&str, [f32; 4])] = &[
    ("a", [1.0, 0.0, 0.0, 0.0]),
    ("b", [0.0, 1.0, 0.0, 0.0]),
    ("c", [0.0, 0.0, 1.0, 0.0]),
    ("d", [0.0, 0.0, 0.0, 1.0]),
];

/// GREEN pre-fix: the COLD path already supports String identities end to end
/// (extract_rows Utf8 arm -> VectorKey::Str -> Utf8 result column). The defect
/// is hot-only; this pin proves the fix does not regress the cold tier.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn string_identity_cold_search() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "sdocs".into(),
    };
    let (catalog, pool, _wh) = seed_and_build(
        fx,
        &db,
        &table,
        "SDocs",
        "string",
        "String",
        ipc_str(COLD_STR),
    )
    .await;

    let batch = engine_serving::vector_search(
        &catalog,
        &pool,
        vq(&table, "by_flat", &[1.0_f32, 0.0, 0.0, 0.0], 1, None, None),
    )
    .await
    .expect("cold search over string identity");
    assert_eq!(ids_str(&batch), vec!["a".to_string()], "nearest is 'a'");
}

fn ipc_int(rows: &[(i32, [f32; 4])]) -> Vec<u8> {
    let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
    let embs: Vec<[f32; 4]> = rows.iter().map(|(_, e)| *e).collect();
    ipc_from(
        Field::new("id", DataType::Int32, false),
        Arc::new(Int32Array::from(ids)),
        &embs,
    )
}

/// Identity column of the result batch as i64 (Int64 output — Integer
/// identities widen through VectorKey::Int).
fn ids_i64(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("identity column is Int64")
        .iter()
        .map(|v| v.expect("non-null id"))
        .collect()
}

/// RED pre-fix: with a String identity, an inline row born after the covered
/// snapshot makes the WHOLE search error (the hot leg's Int64 hardcode).
/// Desired: the hot row merges in first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn string_identity_cold_hot_merge() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "sdocs".into(),
    };
    let (catalog, pool, _wh) = seed_and_build(
        fx,
        &db,
        &table,
        "SDocs",
        "string",
        "String",
        ipc_str(COLD_STR),
    )
    .await;

    let hot: &[(&str, [f32; 4])] = &[("hot", [0.95, 0.05, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns("string"),
        &ipc_str(hot),
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage_evt(&table),
    )
    .await
    .expect("land inline hot row");

    let batch = engine_serving::vector_search(
        &catalog,
        &pool,
        vq(&table, "by_flat", &[0.9_f32, 0.1, 0.0, 0.0], 2, None, None),
    )
    .await
    .expect("cold+hot search over string identity");
    assert_eq!(
        ids_str(&batch),
        vec!["hot".to_string(), "a".to_string()],
        "hot inline row merges in first, cold 'a' second"
    );
}

const COLD_INT: &[(i32, [f32; 4])] = &[
    (1, [1.0, 0.0, 0.0, 0.0]),
    (2, [0.0, 1.0, 0.0, 0.0]),
    (3, [0.0, 0.0, 1.0, 0.0]),
    (4, [0.0, 0.0, 0.0, 1.0]),
];

/// RED pre-fix: same defect class for Integer identities (int4 never decodes
/// as i64). Desired: hot row merges; result ids widen to Int64 exactly as the
/// cold tier's VectorKey::Int does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integer_identity_cold_hot_merge() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "idocs".into(),
    };
    let (catalog, pool, _wh) = seed_and_build(
        fx,
        &db,
        &table,
        "IDocs",
        "integer",
        "Integer",
        ipc_int(COLD_INT),
    )
    .await;

    let hot: &[(i32, [f32; 4])] = &[(5, [0.95, 0.05, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns("integer"),
        &ipc_int(hot),
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage_evt(&table),
    )
    .await
    .expect("land inline hot row");

    let batch = engine_serving::vector_search(
        &catalog,
        &pool,
        vq(&table, "by_flat", &[0.9_f32, 0.1, 0.0, 0.0], 2, None, None),
    )
    .await
    .expect("cold+hot search over integer identity");
    assert_eq!(
        ids_i64(&batch),
        vec![5, 1],
        "hot row first, cold row 1 second"
    );
}
