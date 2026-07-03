//! Plain SQL reads projecting a `vector(N)` column through `execute_query`, across
//! all three storage arms: Parquet files (pins the file path across the
//! element→item serving-schema rename), live inline PG rows (the
//! iss-pg-provider-vector-drift defect — RED until PgTableProvider shares the
//! adapter's column_array), and the files∪inline UNION.

use arrow_array::{Float32Array, Int64Array, ListArray};
use control_plane_core::{LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use loom_test_seed::{local_sql_catalog, test_lineage, vec4_columns, vec4_ipc};

fn lineage_evt(table: &TableRef) -> LineageEvent {
    test_lineage(RunId(uuid::Uuid::new_v4()), table)
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
    let catalog = local_sql_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    if !file_rows.is_empty() {
        land(
            &pool,
            &catalog,
            &table,
            &vec4_columns(),
            &vec4_ipc(file_rows),
            InlineLimits {
                inline_byte_limit: 0,
                flush_byte_threshold: i64::MAX,
            },
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
            &vec4_columns(),
            &vec4_ipc(inline_rows),
            InlineLimits {
                inline_byte_limit: usize::MAX,
                flush_byte_threshold: i64::MAX,
            },
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
        // Pin the wire contract explicitly: the list child is the canonical
        // "item" field from core's map, on every storage arm.
        if let arrow_schema::DataType::List(child) = b.schema().field(1).data_type() {
            assert_eq!(child.name(), "item", "canonical list-child field name");
        } else {
            panic!("embedding column is not a List type");
        }
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
