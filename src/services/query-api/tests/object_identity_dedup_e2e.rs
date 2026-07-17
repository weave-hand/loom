//! Object-identity dedup e2e (road-object-identity-dedup): a many-to-many traversal
//! Customer --orders--> Order --people--> Person, where Person declares `identity = "ssn"`.
//! Two DISTINCT people share every non-identity column (`Kim`/`Ames`, different `ssn`) — the
//! collapse trap. A third person is reached via TWO order paths (the genuine-duplicate case).
//!
//! Proves the traversal dedups on the RAW identity, below the ACL masking layer:
//!   - identity visible  => 3 rows (two Kim/Ames NOT merged; the double-pathed person deduped),
//!   - identity MASKED    => still 3 rows (each Kim/Ames rendered `'***'`) — under the old
//!     `SELECT DISTINCT`-over-visible dedup this returned 2 (the bug this slice fixes),
//!   - identity DENIED    => still 3 rows (ssn column absent from the projection),
//!   - the double-pathed person collapses to a single row in every case (no over-splitting).

use control_plane_core::{Cardinality, ControlPlane, LinkDef, ObjectType, Ontology, PropertyDef};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{InProcessServingEngine, grant_read, grant_read_columns, subject_with_role};
use query_api::handler::{ChainQuery, ObjectRows, QueryDeps, Subject, read_linked_chain};
use query_api::serving::SqlValue;
use std::sync::Arc;

fn pdef(name: &str, ty: &str, required: bool) -> PropertyDef {
    let p = PropertyDef::new(name, ty);
    if required { p.required() } else { p }
}

/// The values of a single projected column, in row order.
fn col_values<'a>(rows: &'a ObjectRows, col: &str) -> Vec<&'a SqlValue> {
    let idx = rows
        .columns
        .iter()
        .position(|c| c == col)
        .unwrap_or_else(|| panic!("column {col} not projected: {:?}", rows.columns));
    rows.rows.iter().map(|r| &r[idx]).collect()
}

/// Seed the collapse-trap graph and serve it via the in-process Iceberg/DataFusion engine.
/// The returned `IcebergWriter` MUST be kept alive — its `TempDir` holds the Parquet warehouse.
async fn setup(
    fx: &PgFixture,
) -> (
    PgControlPlane,
    Arc<dyn query_api::serving::ServingEngine>,
    IcebergWriter,
) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    // customer(id): a single source object (1).
    writer
        .seed_arrays(
            "main",
            "customer",
            &[("id".to_string(), "long".to_string(), false)],
            &[SeedCol::Long(vec![1])],
        )
        .await;

    // person(ssn, name, city): 111 & 222 are DISTINCT objects sharing name/city (collapse trap);
    // 333 is a third, distinct object reached via two order paths (genuine-duplicate case).
    let person_cols = vec![
        ("ssn".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), true),
        ("city".to_string(), "string".to_string(), true),
    ];
    writer
        .seed_arrays(
            "main",
            "person",
            &person_cols,
            &[
                SeedCol::Long(vec![111, 222, 333]),
                SeedCol::Str(vec!["Kim", "Kim", "Bob"]),
                SeedCol::Str(vec!["Ames", "Ames", "Lux"]),
            ],
        )
        .await;

    // orders(id, customer_id, person_id): all belong to customer 1; orders 12 & 13 both point at
    // person 333 (two paths to one target).
    let order_cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("customer_id".to_string(), "long".to_string(), false),
        ("person_id".to_string(), "long".to_string(), false),
    ];
    writer
        .seed_arrays(
            "main",
            "orders",
            &order_cols,
            &[
                SeedCol::Long(vec![10, 11, 12, 13]),
                SeedCol::Long(vec![1, 1, 1, 1]),
                SeedCol::Long(vec![111, 222, 333, 333]),
            ],
        )
        .await;

    cp.define_type(
        ObjectType::build("Customer", ("main", "customer"))
            .add_prop(pdef("id", "Long", true))
            .done(),
    )
    .await
    .unwrap();
    cp.define_type(
        ObjectType::build("Order", ("main", "orders"))
            .add_prop(pdef("id", "Long", true))
            .add_prop(pdef("customer_id", "Long", true))
            .add_prop(pdef("person_id", "Long", true))
            .done(),
    )
    .await
    .unwrap();
    // Person's identity is "ssn" — the declared PK the dedup keys on.
    cp.define_type(
        ObjectType::build("Person", ("main", "person"))
            .add_prop(pdef("ssn", "Long", true))
            .add_prop(pdef("name", "String", false))
            .add_prop(pdef("city", "String", false))
            .identity("ssn")
            .done(),
    )
    .await
    .unwrap();

    cp.define_link(LinkDef::fk(
        "orders",
        "Customer",
        "Order",
        Cardinality::Many,
        "id",
        "customer_id",
    ))
    .await
    .unwrap();
    cp.define_link(LinkDef::fk(
        "people",
        "Order",
        "Person",
        Cardinality::Many,
        "person_id",
        "ssn",
    ))
    .await
    .unwrap();

    let sql_catalog = writer.sql_catalog().await;
    let catalog = IcebergCatalog::new(pool.clone());
    let eng: Arc<dyn query_api::serving::ServingEngine> = Arc::new(
        InProcessServingEngine::new_with_search(catalog, pool.clone(), sql_catalog),
    );
    (cp, eng, writer)
}

fn chain() -> ChainQuery {
    ChainQuery {
        from_type: "Customer".into(),
        path: vec!["orders".into(), "people".into()],
        filters: vec![],
        ids: vec![],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn masked_or_denied_identity_preserves_object_cardinality() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &*eng,
        default_limit: 1000,
        gc_retention: e2e_support::TEST_GC_RETENTION,
    };

    // ---- Baseline: identity visible => three distinct people (111, 222, 333). ----
    // Anchors the fixture: the two Kim/Ames rows are NOT merged, and the double-pathed 333
    // collapses to exactly one row.
    let (alice, alice_role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &alice_role, "Customer").await;
    grant_read(&cp, &alice_role, "Order").await;
    grant_read(&cp, &alice_role, "Person").await;
    let rows = read_linked_chain(&chain(), &Subject(alice), &deps)
        .await
        .unwrap();
    assert_eq!(
        rows.rows.len(),
        3,
        "identity visible: two distinct Kim/Ames + one deduped double-path person; got {:?}",
        rows.rows
    );

    // ---- Bug case: identity MASKED => still three rows. ----
    // Under the old SELECT DISTINCT-over-visible dedup, 111 and 222 (name='Kim', city='Ames',
    // ssn='***') collapsed to one row => 2 total. Keying on the raw ssn keeps all three.
    let (bob, bob_role) = subject_with_role(&cp, "bob").await;
    grant_read(&cp, &bob_role, "Customer").await;
    grant_read(&cp, &bob_role, "Order").await;
    grant_read_columns(&cp, &bob_role, "Person", vec![], vec!["ssn".into()]).await;
    let masked = read_linked_chain(&chain(), &Subject(bob), &deps)
        .await
        .unwrap();
    assert_eq!(
        masked.rows.len(),
        3,
        "masked identity must not collapse distinct objects; got {:?}",
        masked.rows
    );
    // The identity is genuinely suppressed in the output — every ssn renders as the mask marker.
    for v in col_values(&masked, "ssn") {
        assert_eq!(
            v,
            &SqlValue::Text("***".into()),
            "masked ssn must render '***', not the raw value: {v:?}"
        );
    }

    // ---- Identity DENIED (dropped from the projection) => still three rows. ----
    let (carol, carol_role) = subject_with_role(&cp, "carol").await;
    grant_read(&cp, &carol_role, "Customer").await;
    grant_read(&cp, &carol_role, "Order").await;
    grant_read_columns(&cp, &carol_role, "Person", vec!["ssn".into()], vec![]).await;
    let denied = read_linked_chain(&chain(), &Subject(carol), &deps)
        .await
        .unwrap();
    assert_eq!(
        denied.rows.len(),
        3,
        "denied identity must not collapse distinct objects; got {:?}",
        denied.rows
    );
    // The identity column is absent from the projection (denied), yet cardinality is preserved.
    assert!(
        !denied.columns.contains(&"ssn".to_string()),
        "denied ssn must be absent from projection: {:?}",
        denied.columns
    );
}
