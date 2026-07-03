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

/// Like [`cols`] but the non-id column is CAMELCASE (`unitPrice`). Exercises the
/// merge view's case handling: DataFusion's `col()` lowercases unquoted identifiers,
/// so a mixed-case mirror column must be referenced case-preservingly or it fails to
/// resolve (regression for main's `action-computed-e2e`, which uses camelCase columns).
fn camel_cols() -> Vec<(String, String, bool)> {
    vec![
        ("id".to_string(), "long".to_string(), false),
        ("unitPrice".to_string(), "string".to_string(), false),
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

/// Define `(schema.name)` as an identity type whose non-id property is the camelCase
/// `unitPrice`, so the merge-on-read view for it must preserve mixed-case column names.
async fn define_camel_type(cp: &control_plane_postgres::PgControlPlane, schema: &str, name: &str) {
    cp.define_type(ObjectType {
        name: TypeName(format!("Type_{schema}_{name}")),
        table: TableRef {
            schema: schema.into(),
            name: name.into(),
        },
        identity: Some("id".to_string()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "unitPrice".into(),
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
    let batches =
        engine_serving::execute_query(&catalog, "SELECT * FROM \"s\".\"t\" ORDER BY \"id\"", None)
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
async fn inline_version_shadows_file_row_camelcase_column() {
    // Regression: an identity type whose non-id column is CAMELCASE (`unitPrice`).
    // The merge view referenced columns via `col(name)`, which lowercases unquoted
    // identifiers, so it looked up a nonexistent `unitprice` and the read failed with
    // `No field named unitprice` (main's `action-computed-e2e`). The view must instead
    // preserve the mixed-case name AND leak no `begin_snapshot`/`loom_tombstone` helper.
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    // File-resident row {id:1, unitPrice:"file"} (real Parquet via the writer chain).
    writer
        .seed_arrays(
            "s6",
            "t6",
            &camel_cols(),
            &[SeedCol::Long(vec![1]), SeedCol::Str(vec!["file"])],
        )
        .await;
    // Inline VERSION {id:1, unitPrice:"shadow"} at a LATER snapshot (shadows the file row).
    writer
        .inline("s6", "t6", &camel_cols(), &[(1, "shadow")], Uuid::new_v4())
        .await;
    define_camel_type(&cp, "s6", "t6").await;

    let catalog = IcebergCatalog::new(pool);
    // `SELECT *` expands to the merge view's schema: it MUST equal the mirror data
    // schema exactly — the camelCase name preserved, no helper columns leaked.
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT * FROM \"s6\".\"t6\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("execute_query");

    let sch = batches.first().expect("at least one batch").schema();
    let names: Vec<&str> = sch.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec!["id", "unitPrice"],
        "merge view preserves the mixed-case column name and leaks no helper columns"
    );
    assert!(
        !names.contains(&"begin_snapshot") && !names.contains(&"loom_tombstone"),
        "no merge helper column leaks into the view schema: {names:?}"
    );

    // The inline version shadows the file row: exactly one row for id=1, value "shadow".
    assert_eq!(rows(&batches), vec![(1, "shadow".to_string())]);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lone_inline_tombstone_hides_id_with_no_file_tier() {
    // No file tier at all: exercises the `(None, Some(inline))` branch of the
    // identity-dedup merge (build_serving_provider), which neither existing identity
    // test reaches (both seed a file row).
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    // A single inline row for id=1 (creates the inline tier, no file rows seeded),
    // then mark it a TOMBSTONE — same raw-SQL technique as `tombstone_hides_file_row`.
    writer
        .inline("s4", "t4", &cols(), &[(1, "x")], Uuid::new_v4())
        .await;
    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, "s4", "t4")
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

    define_type(&cp, "s4", "t4", Some("id")).await;

    let catalog = IcebergCatalog::new(pool);
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT \"id\", \"name\" FROM \"s4\".\"t4\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("execute_query");

    // No file tier and the lone inline row is tombstoned: id=1 is absent entirely.
    assert!(
        rows(&batches).is_empty(),
        "expected no rows, got {:?}",
        rows(&batches)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_only_dedup_keeps_latest_begin_snapshot_version() {
    // No file tier: two inline VERSIONs of id=1 at different begin_snapshots (each
    // `.inline()` call allocates the next mirror snapshot id, so the second call's
    // rows carry a strictly greater begin_snapshot). Exercises intra-inline-tier
    // dedup within the `(None, Some(inline))` branch: the greatest-precedence
    // version must win even with no file tier present.
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    let snap_low = writer
        .inline("s5", "t5", &cols(), &[(1, "v1")], Uuid::new_v4())
        .await;
    let snap_high = writer
        .inline("s5", "t5", &cols(), &[(1, "v9")], Uuid::new_v4())
        .await;
    assert!(
        snap_high > snap_low,
        "second inline call must allocate a later snapshot ({snap_low} vs {snap_high})"
    );

    define_type(&cp, "s5", "t5", Some("id")).await;

    let catalog = IcebergCatalog::new(pool);
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT \"id\", \"name\" FROM \"s5\".\"t5\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("execute_query");

    // Exactly one row for id=1: the higher-begin_snapshot version ("v9") wins.
    assert_eq!(rows(&batches), vec![(1, "v9".to_string())]);
}
