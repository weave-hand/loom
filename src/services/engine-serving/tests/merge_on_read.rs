//! Identity-aware merge-on-read (scalable copy-on-write, Task 3).
//!
//! Seeds a table's two tiers — a Parquet-backed file row (via the landing/writer
//! chain) and inline delta rows (a later-snapshot inline VERSION or a TOMBSTONE) —
//! then reads through `engine_serving::execute_query` and asserts the merged result.
//!
//! For an identity-bearing type the inline delta with the greatest precedence
//! (`begin_snapshot`) wins per identity, and a tombstone winner hides the id; the
//! file tier synthesizes precedence 0, so any inline delta shadows a file row.
//! An identity-less type keeps the additive union (no dedup).

use arrow::array::{Int64Array, RecordBatch, StringArray};
use control_plane_core::{ObjectType, Ontology, PropertyDef, TableRef, TypeName};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_mirror::live_table_id;
use sqlx::AssertSqlSafe;
use uuid::Uuid;

/// The `(id, name)` logical schema shared by every case, in mirror column order.
fn cols() -> Vec<(String, String, bool)> {
    vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ]
}

/// Define `(schema.name)` as an ontology type with the given identity column, so
/// `identity_for_table` resolves (or not) during the merge-on-read decision.
async fn define_type(
    cp: &control_plane_postgres::PgControlPlane,
    schema: &str,
    name: &str,
    identity: Option<&str>,
) {
    cp.define_type(ObjectType {
        name: TypeName(format!("Type_{schema}_{name}")),
        table: TableRef {
            schema: schema.into(),
            name: name.into(),
        },
        identity: identity.map(str::to_string),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "name".into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
    })
    .await
    .expect("define_type");
}

/// Flatten result batches into `(id, name)` pairs (both columns non-null in these tests).
fn rows(batches: &[RecordBatch]) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        let names = b
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column is Utf8");
        for i in 0..b.num_rows() {
            out.push((ids.value(i), names.value(i).to_string()));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_version_shadows_file_row() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    // File-resident row {id:1, name:"file"} (real Parquet via the writer chain).
    writer
        .seed_arrays(
            "s",
            "t",
            &cols(),
            &[SeedCol::Long(vec![1]), SeedCol::Str(vec!["file"])],
        )
        .await;
    // Inline VERSION {id:1, name:"inline"} at a LATER snapshot (begin_snapshot > file).
    writer
        .inline("s", "t", &cols(), &[(1, "inline")], Uuid::new_v4())
        .await;
    // Identity on `id` so the merge dedups by it.
    define_type(&cp, "s", "t", Some("id")).await;

    let catalog = IcebergCatalog::new(pool);
    // `SELECT *` expands to the merge view's schema: it MUST equal the mirror data
    // schema exactly (no `_loom_prec` / `_loom_tomb` helper columns leaked), which the
    // governed layer and all callers rely on.
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT * FROM \"s\".\"t\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("execute_query");

    let sch = batches.first().expect("at least one batch").schema();
    assert_eq!(
        sch.fields().len(),
        2,
        "merge view exposes exactly the mirror data columns (no helper columns leaked)"
    );
    assert_eq!(sch.field(0).name(), "id");
    assert_eq!(sch.field(1).name(), "name");
    assert_eq!(sch.field(0).data_type(), &arrow::datatypes::DataType::Int64);
    assert_eq!(sch.field(1).data_type(), &arrow::datatypes::DataType::Utf8);
    assert!(!sch.field(0).is_nullable(), "id keeps mirror nullability");
    assert!(!sch.field(1).is_nullable(), "name keeps mirror nullability");

    // The inline version shadows the file row: exactly one row for id=1, name="inline".
    assert_eq!(rows(&batches), vec![(1, "inline".to_string())]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tombstone_hides_file_row() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    // Two file rows {1:"file1", 2:"file2"}.
    writer
        .seed_arrays(
            "s2",
            "t2",
            &cols(),
            &[
                SeedCol::Long(vec![1, 2]),
                SeedCol::Str(vec!["file1", "file2"]),
            ],
        )
        .await;
    // Inline row for id=1 (creates the inline tier), then mark it a TOMBSTONE.
    writer
        .inline("s2", "t2", &cols(), &[(1, "x")], Uuid::new_v4())
        .await;
    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, "s2", "t2")
        .await
        .expect("live_table_id")
        .expect("tid present");
    drop(conn);
    sqlx::query(AssertSqlSafe(format!(
        "update iceberg_mirror.inline_{tid} set loom_tombstone = true where \"id\" = 1"
    )))
    .execute(&pool)
    .await
    .expect("set tombstone");

    define_type(&cp, "s2", "t2", Some("id")).await;

    let catalog = IcebergCatalog::new(pool);
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT \"id\", \"name\" FROM \"s2\".\"t2\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("execute_query");

    // id=1 is tombstoned (hidden); the untouched file row id=2 survives.
    assert_eq!(rows(&batches), vec![(2, "file2".to_string())]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identity_less_type_unions_additively() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    // File row and inline row that COLLIDE on id=1.
    writer
        .seed_arrays(
            "s3",
            "t3",
            &cols(),
            &[SeedCol::Long(vec![1]), SeedCol::Str(vec!["file"])],
        )
        .await;
    writer
        .inline("s3", "t3", &cols(), &[(1, "inline")], Uuid::new_v4())
        .await;
    // No identity → additive union, no dedup.
    define_type(&cp, "s3", "t3", None).await;

    let catalog = IcebergCatalog::new(pool);
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT \"id\", \"name\" FROM \"s3\".\"t3\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("execute_query");

    // Both rows survive (identity-less: no shadowing).
    assert_eq!(
        rows(&batches),
        vec![(1, "file".to_string()), (1, "inline".to_string())]
    );
}
