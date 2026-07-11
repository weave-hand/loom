//! mv_enrich_scan: the slice-5 enrich read — folded current state (merge
//! engine applied), optional typed IN key predicate, user columns only.
//! loom_fixture_test (Postgres + local warehouse).
//!
//! Mirrors `merge_on_read.rs`'s ACTUAL seeding harness: `PgFixture::shared()`,
//! `IcebergCatalog::new(pool)`, `IcebergWriter::{seed_arrays, inline}`, and
//! `define_type` — never `land_cdc`/`flush_table`/`local_sql_catalog`, which
//! are prod-only or worker-test helpers.

use arrow::array::{Int64Array, RecordBatch};
use control_plane_core::{ObjectType, Ontology, PropertyDef, TableRef, TypeName};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_mirror::live_table_id;
use engine_serving::mv_enrich::mv_enrich_scan;
use sqlx::AssertSqlSafe;
use uuid::Uuid;

/// Convenience `TableRef` constructor (matches the per-test-file convention
/// used across the engine-serving/postgres test suites — no shared helper).
fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

/// The `(id, name)` logical schema shared by every case.
fn cols() -> Vec<(String, String, bool)> {
    vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ]
}

/// Define `(schema.name)` as an ontology type with the given identity column
/// (matches `merge_on_read.rs::define_type`).
async fn define_type(
    cp: &control_plane_postgres::PgControlPlane,
    schema: &str,
    name: &str,
    identity: Option<&str>,
) {
    cp.define_type(ObjectType {
        name: TypeName(format!("Type_{schema}_{name}")),
        table: tref(schema, name),
        identity: identity.map(str::to_string),
        version: None,
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

/// Flatten the `id` column (column 0) out of a batch set, sorted.
fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        for i in 0..b.num_rows() {
            out.push(arr.value(i));
        }
    }
    out.sort_unstable();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enrich_scan_folds_filters_and_hides_framing() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    // File tier: +I id=1 "ada", +I id=2 "bob".
    writer
        .seed_arrays(
            "s",
            "customers",
            &cols(),
            &[SeedCol::Long(vec![1, 2]), SeedCol::Str(vec!["ada", "bob"])],
        )
        .await;
    // Inline tier (later snapshot, shadows the file rows by identity):
    // id=1 updated to "ada2", id=2 gets an inline row we'll tombstone below,
    // id=3 is a fresh inline-only insert.
    writer
        .inline(
            "s",
            "customers",
            &cols(),
            &[(1, "ada2"), (2, "x"), (3, "cyd")],
            Uuid::new_v4(),
        )
        .await;
    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, "s", "customers")
        .await
        .expect("live_table_id")
        .expect("tid present");
    drop(conn);
    // Delete id=2 via the same raw-SQL tombstone technique `merge_on_read.rs`
    // uses (`tombstone_hides_file_row`): only id=2's inline row is affected.
    sqlx::query(AssertSqlSafe(format!(
        "update iceberg_mirror.inline_{tid} set loom_tombstone = true where \"id\" = 2"
    )))
    .execute(&pool)
    .await
    .expect("set tombstone");

    // Identity on `id` so the merge dedups by it (update shadows, delete hides).
    define_type(&cp, "s", "customers", Some("id")).await;

    let catalog = IcebergCatalog::new(pool.clone());
    let table = tref("s", "customers");

    // 1. Unkeyed: the full folded current state (LastRow-equivalent via the
    //    non-CDC MVCC precedence): {1:"ada2", 3:"cyd"}. id=2 tombstoned away.
    let (schema, batches) = mv_enrich_scan(&catalog, &table, None, None)
        .await
        .expect("unkeyed scan");
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        names.iter().all(|n| !n.starts_with("loom_")),
        "logical read: no framing columns, got {names:?}"
    );
    assert_eq!(
        ids(&batches),
        vec![1, 3],
        "folded: update applied, delete dropped"
    );

    // 2. Keyed: only the requested keys' folded rows.
    let keys = [serde_json::json!(1), serde_json::json!(99)];
    let (_, keyed) = mv_enrich_scan(&catalog, &table, Some(("id", &keys)), None)
        .await
        .expect("keyed scan");
    assert_eq!(ids(&keyed), vec![1], "key 99 has no row; key 1 folded");

    // 3. Live-but-empty table: an inline append of ZERO rows still commits a
    //    live iceberg_mirror table/column/snapshot row set (schema known),
    //    with no files and no live inline rows — exactly the case
    //    `build_serving_provider` returns `Ok(None)` for.
    writer
        .inline("s", "empty", &cols(), &[], Uuid::new_v4())
        .await;
    let empty_table = tref("s", "empty");
    let (empty_schema, empty) = mv_enrich_scan(&catalog, &empty_table, None, None)
        .await
        .expect("empty scan");
    assert!(
        empty.iter().all(|b| b.num_rows() == 0),
        "live-but-empty table serves zero rows"
    );
    assert!(
        !empty_schema.fields().is_empty(),
        "live-but-empty table still serves its declared logical schema"
    );

    // 4. Unknown table (never declared, never landed): deterministic error.
    let err = mv_enrich_scan(&catalog, &tref("s", "nope"), None, None)
        .await
        .expect_err("unknown table must error");
    assert!(
        err.to_string().contains("mv enrich:"),
        "unknown-table error must carry the stable prefix: {err}"
    );

    // 5. Non-coercible key value (not a scalar): deterministic error, never a
    //    silent empty result.
    let bad = [serde_json::json!({"not": "a scalar"})];
    let err = mv_enrich_scan(&catalog, &table, Some(("id", &bad)), None)
        .await
        .expect_err("non-coercible key must error");
    assert!(
        err.to_string().contains("mv enrich:"),
        "bad-key error must carry the stable prefix: {err}"
    );
}
