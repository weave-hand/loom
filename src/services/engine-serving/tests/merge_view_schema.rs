//! The merged view's SERVED schema is byte-identical to the mirror's — names,
//! types, order AND nullability — even with a live inline tombstone
//! (iss-search-vector-merge-view-nullable).
//!
//! This is not cosmetic. The worker infers a transform/MV's output columns from
//! the served Arrow schema (`worker::transform` / `worker::stream_mv` ->
//! `datafusion_io::infer_columns`, which copies `f.is_nullable()`); a widened flag
//! makes `check_conformance` reject a REQUIRED property
//! (`core::conform` -> `NullabilityViolation`) and `classify_schema_change` reject
//! an MV re-run (`ColumnNullabilityChanged`). The inline TIER must declare its
//! non-identity columns nullable (a non-CDC tombstone row is id-only, so they
//! really are NULL there) — but the merged OUTPUT, which sits above the
//! `_loom_tomb = false` filter that drops exactly those rows, must not.
//!
//! loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, DatasetId, EventType, LineageEvent, ObjectType, Ontology, RunId, TableRef,
};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline;
use datafusion::prelude::SessionContext;
use engine_serving::serving::{arrow_schema_from_mirror, build_serving_provider};
use uuid::Uuid;

/// `(id long NOT NULL, name string NOT NULL)` — the mirror column order. `name` is
/// the REQUIRED non-identity column the tombstone row leaves NULL.
fn seed_cols() -> Vec<(String, String, bool)> {
    vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ]
}

/// The same shape as [`seed_cols`] in the form `write_inline_delta` takes.
fn inline_cols() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "name".into(),
            ty: "string".into(),
            nullable: false,
        },
    ]
}

/// A full `{id, name}` row — a non-CDC inline VERSION (an UPDATE shadow).
fn row_batch(id: i64, name: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![name])),
        ],
    )
    .expect("row_batch")
}

/// An id-only batch — the CAS lookup key, and a non-CDC tombstone's carried batch
/// (its `name` column is physically NULL in `inline_<tid>`).
fn id_batch(id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id]))]).expect("id_batch")
}

fn lineage(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

/// An identity type whose non-identity property is REQUIRED — the shape the
/// register's defect needed (`Docs.embedding`), minus the vector type.
async fn define_identity_type(cp: &control_plane_postgres::PgControlPlane, table: &TableRef) {
    cp.define_type(
        ObjectType::build("Thing", (table.schema.clone(), table.name.clone()))
            .prop_req("id", "Long")
            .prop_req("name", "String")
            .identity("id")
            .done(),
    )
    .await
    .expect("define_type");
}

/// The pin: with an identity type carrying a REQUIRED non-identity column, a live
/// inline UPDATE shadow AND a live inline tombstone, the serving provider's schema
/// still equals the mirror's — field for field, nullability included — and the
/// full-column read over it succeeds (pre-fix it 500s inside
/// `PgTableProvider::fetch_batch`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merged_view_schema_equals_mirror_with_live_tombstone() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let table = TableRef {
        schema: "s".into(),
        name: "things".into(),
    };

    // File tier: three rows, all columns present.
    writer
        .seed_arrays(
            &table.schema,
            &table.name,
            &seed_cols(),
            &[
                SeedCol::Long(vec![1, 2, 3]),
                SeedCol::Str(vec!["a", "b", "c"]),
            ],
        )
        .await;
    define_identity_type(&cp, &table).await;

    let cols = inline_cols();
    // Inline UPDATE shadow on id=1 — a FULL row (every data column present).
    let v1 = iceberg_inline::current_inline_version(&pool, &table, &cols, "id", &id_batch(1))
        .await
        .expect("version id=1");
    iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false, // row-version
        &row_batch(1, "shadow"),
        None,
        lineage(&table),
        v1,
        None,
        &[],
    )
    .await
    .expect("inline update");
    // Inline TOMBSTONE on id=2 — id-only, so `name` is physically NULL.
    let v2 = iceberg_inline::current_inline_version(&pool, &table, &cols, "id", &id_batch(2))
        .await
        .expect("version id=2");
    iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        true, // tombstone
        &id_batch(2),
        None,
        lineage(&table),
        v2,
        None,
        &[],
    )
    .await
    .expect("inline tombstone");

    let catalog = IcebergCatalog::new(pool);
    let snap = catalog
        .current_snapshot(&table)
        .await
        .expect("current snapshot")
        .id;
    let mirror = arrow_schema_from_mirror(
        &catalog
            .schema(&table, snap)
            .await
            .expect("mirror schema")
            .columns,
    )
    .expect("arrow schema");

    // THE CONTRACT: the served schema is byte-identical to the mirror's. `assert_eq`
    // on `&Fields` compares name + data_type + nullability + metadata.
    let ctx = SessionContext::new();
    let provider = build_serving_provider(&ctx, &catalog, &table, None, None)
        .await
        .expect("build_serving_provider")
        .expect("live table");
    assert_eq!(
        provider.schema().fields(),
        mirror.fields(),
        "merged view serves the mirror schema EXACTLY (nullability included)"
    );

    // ... and the full-column read over that schema still works: the tombstoned id
    // is gone and the UPDATE shadow's value wins.
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT \"id\", \"name\" FROM \"s\".\"things\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("full-column read over the merged view");
    let mut got: Vec<(i64, String)> = Vec::new();
    for b in &batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 ids");
        let names = b
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 names");
        for i in 0..b.num_rows() {
            got.push((ids.value(i), names.value(i).to_string()));
        }
    }
    assert_eq!(
        got,
        vec![(1, "shadow".to_string()), (3, "c".to_string())],
        "tombstoned id=2 hidden; the UPDATE shadow wins for id=1"
    );
}
