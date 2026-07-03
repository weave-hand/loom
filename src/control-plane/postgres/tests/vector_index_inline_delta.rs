//! Seam-level pins for `inline_delta_batch` — the hot-delta read the k-NN
//! search path merges over. The engine-serving merge e2es exercise it only
//! indirectly (and only with a Long identity); these tests pin the batch
//! CONTRACT at the seam (field names, arrow types, nullability, MVCC window)
//! so the road-vector-build-decomposition fix can prove the Long path
//! byte-identical. `int_identity_delta_batch_shape` is GREEN pre-fix.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Array, Float32Array, Int64Array, ListArray, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetId, EventType, LineageEvent, ObjectType, PropertyDef, RunId,
    TableRef, TypeName,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use control_plane_postgres::vector_index::inline_delta_batch;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

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

/// IPC body from a prebuilt identity array + embeddings (shared by the
/// per-identity-kind wrappers below and Task 2's appends).
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

fn ipc_long(rows: &[(i64, [f32; 4])]) -> Vec<u8> {
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let embs: Vec<[f32; 4]> = rows.iter().map(|(_, e)| *e).collect();
    ipc_from(
        Field::new("id", DataType::Int64, false),
        Arc::new(Int64Array::from(ids)),
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

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{warehouse}"),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

/// Land `cold_ipc` as Parquet (limit 0), then `hot_ipc` INLINE (limit
/// usize::MAX) — passed together as `(cold_ipc, hot_ipc)`. Returns
/// (pool, s_cold, s_hot): the delta window is (s_cold, s_hot].
async fn seed(
    fx: &PgFixture,
    db: &str,
    table: &TableRef,
    type_name: &str,
    id_ty_logical: &str,
    id_ty_property: &str,
    (cold_ipc, hot_ipc): (Vec<u8>, Vec<u8>),
) -> (sqlx::PgPool, i64, i64) {
    let pool = fx.pool_for(db).await;
    let cp = control_plane_postgres::PgControlPlane::new(
        pool.clone(),
        std::time::Duration::from_secs(5),
    );
    cp.ontology()
        .define_type(object_type(type_name, table, id_ty_property))
        .await
        .expect("define_type");
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let s_cold = land(
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
    let s_hot = land(
        &pool,
        &catalog,
        table,
        &columns(id_ty_logical),
        &hot_ipc,
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage_evt(table),
    )
    .await
    .expect("land hot");
    (pool, s_cold.0, s_hot.0)
}

/// GREEN pre-fix: the Long-identity delta batch contract the fix must keep
/// byte-identical — identity field named after the ontology identity column,
/// Int64, non-nullable; vector field List<Float32> with the canonical "item"
/// child; only rows born in (born_after, at]; None outside the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn int_identity_delta_batch_shape() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "ldocs".into(),
    };
    let cold: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    let hot: &[(i64, [f32; 4])] = &[(5, [0.9, 0.1, 0.0, 0.0])];
    let (pool, s_cold, s_hot) = seed(
        fx,
        &db,
        &table,
        "LDocs",
        "long",
        "Long",
        (ipc_long(cold), ipc_long(hot)),
    )
    .await;

    let batch = inline_delta_batch(&pool, &table, s_cold, s_hot)
        .await
        .expect("delta")
        .expect("Some: one row born after s_cold");
    let schema = batch.schema();
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert!(!schema.field(0).is_nullable());
    assert_eq!(schema.field(1).name(), "embedding");
    let DataType::List(child) = schema.field(1).data_type() else {
        panic!("vector column is a List");
    };
    assert_eq!(child.name(), "item");
    assert_eq!(child.data_type(), &DataType::Float32);
    assert!(!schema.field(1).is_nullable());

    assert_eq!(batch.num_rows(), 1);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 ids");
    assert_eq!(ids.value(0), 5);
    let vecs = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("list");
    let elems = vecs.value(0);
    let f = elems
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("f32 child");
    assert_eq!(f.values(), &[0.9, 0.1, 0.0, 0.0]);

    // MVCC window: nothing born after s_hot -> None.
    assert!(
        inline_delta_batch(&pool, &table, s_hot, s_hot)
            .await
            .expect("delta at s_hot")
            .is_none()
    );
}

fn ipc_str(rows: &[(&str, [f32; 4])]) -> Vec<u8> {
    let ids: Vec<&str> = rows.iter().map(|(id, _)| *id).collect();
    let embs: Vec<[f32; 4]> = rows.iter().map(|(_, e)| *e).collect();
    ipc_from(
        Field::new("id", DataType::Utf8, false),
        Arc::new(arrow_array::StringArray::from(ids)),
        &embs,
    )
}

fn ipc_int(rows: &[(i32, [f32; 4])]) -> Vec<u8> {
    let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
    let embs: Vec<[f32; 4]> = rows.iter().map(|(_, e)| *e).collect();
    ipc_from(
        Field::new("id", DataType::Int32, false),
        Arc::new(arrow_array::Int32Array::from(ids)),
        &embs,
    )
}

/// RED pre-fix: a String identity makes `inline_delta_batch` fail with a sqlx
/// mismatched-types Backend error (`try_get::<i64>` on a PG `text` column).
/// Desired: a Utf8 identity column, mirroring the cold path's Utf8 support
/// (iss-inline-delta-string-identity).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn string_identity_delta_is_utf8() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "sdocs".into(),
    };
    let cold: &[(&str, [f32; 4])] = &[("a", [1.0, 0.0, 0.0, 0.0])];
    let hot: &[(&str, [f32; 4])] = &[("hot", [0.9, 0.1, 0.0, 0.0])];
    let (pool, s_cold, s_hot) = seed(
        fx,
        &db,
        &table,
        "SDocs",
        "string",
        "String",
        (ipc_str(cold), ipc_str(hot)),
    )
    .await;

    let batch = inline_delta_batch(&pool, &table, s_cold, s_hot)
        .await
        .expect("delta over string identity")
        .expect("Some: one row born after s_cold");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("Utf8 ids");
    assert_eq!(ids.value(0), "hot");
}

/// RED pre-fix: an Integer identity fails the same way (PG `integer` never
/// decodes as i64 under sqlx's strict typing). Desired: an Int32 identity
/// column, mirroring extract_rows' Int32 arm.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integer_identity_delta_is_int32() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "idocs".into(),
    };
    let cold: &[(i32, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0])];
    let hot: &[(i32, [f32; 4])] = &[(5, [0.9, 0.1, 0.0, 0.0])];
    let (pool, s_cold, s_hot) = seed(
        fx,
        &db,
        &table,
        "IDocs",
        "integer",
        "Integer",
        (ipc_int(cold), ipc_int(hot)),
    )
    .await;

    let batch = inline_delta_batch(&pool, &table, s_cold, s_hot)
        .await
        .expect("delta over integer identity")
        .expect("Some: one row born after s_cold");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Int32);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .expect("Int32 ids");
    assert_eq!(ids.value(0), 5);
}
