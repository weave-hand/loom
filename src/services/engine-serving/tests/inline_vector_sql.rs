//! Plain SQL reads projecting a `vector(N)` column through `execute_query`, across
//! all three storage arms: Parquet files (pins the file path across the
//! element→item serving-schema rename), live inline PG rows (the
//! iss-pg-provider-vector-drift defect — RED until PgTableProvider shares the
//! adapter's column_array), and the files∪inline UNION.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Float32Array, Int64Array, ListArray, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, DatasetId, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

fn columns() -> Vec<ColumnSpec> {
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

/// Arrow IPC body with `id: long` + `embedding: list<float32>` (4 elements).
fn ipc_body(rows: &[(i64, [f32; 4])]) -> Vec<u8> {
    let element = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(element.clone());
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    for (_, emb) in rows {
        lb.values().append_slice(emb);
        lb.append(true);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", DataType::List(element), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids)), Arc::new(lb.finish())],
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

/// Land `file_rows` as Parquet (inline_byte_limit 0) and `inline_rows` as live
/// inline PG rows (inline_byte_limit usize::MAX), returning the pool + the
/// warehouse TempDir guard (keep alive across the query).
async fn seed(
    fx: &PgFixture,
    db: &str,
    file_rows: &[(i64, [f32; 4])],
    inline_rows: &[(i64, [f32; 4])],
) -> (sqlx::PgPool, tempfile::TempDir) {
    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(db).await;
    let catalog = make_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    if !file_rows.is_empty() {
        land(
            &pool,
            &catalog,
            &table,
            &columns(),
            &ipc_body(file_rows),
            0,
            i64::MAX,
            lineage_evt(&table),
        )
        .await
        .expect("land file rows");
    }
    if !inline_rows.is_empty() {
        land(
            &pool,
            &catalog,
            &table,
            &columns(),
            &ipc_body(inline_rows),
            usize::MAX,
            i64::MAX,
            lineage_evt(&table),
        )
        .await
        .expect("land inline rows");
    }
    (pool, wh)
}

/// Run the projecting SQL and flatten (ids, embeddings) in row order.
async fn read_vectors(pool: sqlx::PgPool) -> (Vec<i64>, Vec<Vec<f32>>) {
    let catalog = IcebergCatalog::new(pool);
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT \"id\", \"embedding\" FROM \"wh\".\"docs\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("execute_query projecting a vector column");
    let mut ids = Vec::new();
    let mut embs = Vec::new();
    for b in &batches {
        let id = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        ids.extend((0..id.len()).map(|i| id.value(i)));
        let list = b
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("embedding is List");
        for row in list.iter() {
            let v = row.expect("non-null embedding");
            let f = v
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("Float32 child");
            embs.push(f.values().to_vec());
        }
    }
    (ids, embs)
}

const E1: [f32; 4] = [1.0, 0.0, 0.0, 0.5];
const E2: [f32; 4] = [0.0, 1.0, 0.0, 0.25];
const E3: [f32; 4] = [0.0, 0.0, 1.0, 0.125];

/// Files-only: pins the Parquet arm (on-disk child stays Iceberg's "element";
/// the serving schema's child name must keep adapting on read).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_projects_vector_from_parquet_files() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let (pool, _wh) = seed(fx, &db, &[(1, E1), (2, E2)], &[]).await;
    let (ids, embs) = read_vectors(pool).await;
    assert_eq!(ids, vec![1, 2]);
    assert_eq!(
        embs,
        vec![E1.to_vec(), E2.to_vec()],
        "file vectors bit-exact"
    );
}

/// Inline-only: THE iss-pg-provider-vector-drift defect — RED today
/// ("inline PG provider: unsupported logical type \"vector(4)\"").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_projects_vector_from_inline_rows() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let (pool, _wh) = seed(fx, &db, &[], &[(1, E1), (2, E2)]).await;
    let (ids, embs) = read_vectors(pool).await;
    assert_eq!(ids, vec![1, 2]);
    assert_eq!(
        embs,
        vec![E1.to_vec(), E2.to_vec()],
        "inline vectors bit-exact"
    );
}

/// Files ∪ inline: both providers must present the SAME vector schema
/// (list child "item") or the union/planning rejects it. RED today.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_projects_vector_from_union_of_files_and_inline() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let (pool, _wh) = seed(fx, &db, &[(1, E1), (2, E2)], &[(3, E3)]).await;
    let (ids, embs) = read_vectors(pool).await;
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(
        embs,
        vec![E1.to_vec(), E2.to_vec(), E3.to_vec()],
        "unioned vectors bit-exact"
    );
}
