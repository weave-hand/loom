//! View-aware governed READ e2e: the acceptance criteria for catalog views on the
//! real fixture engine (Postgres + in-process Iceberg/DataFusion). A type bound to a
//! catalog VIEW reads the view's predicate/projection-narrowed base rows; ACL grants
//! on the view and on its base are exact-match decoupled; a view's projected schema
//! bounds what a bind may declare; and the base→view lineage edge is governed.
//!
//! Complements `view_write_e2e` (the write path). Reads ride engine view-expansion +
//! catalog delegation (built in earlier tasks); this suite is the governed-read proof.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).
//! Spec: docs/superpowers/specs/2026-07-13-catalog-views-design.md

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{
    Acl, Action, Cardinality, Catalog, CompareOp, ControlPlane, Effect, LinkDef, ObjectType,
    PolicyTarget, RoleId, RowFilter, ScalarValue, TableRef, ViewDef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{
    InProcessServingEngine, NoServing, get, grant_read, grant_read_columns, grant_read_filtered,
    ids_i64, subject_with_role, tref,
};
use ingest::bind;
use ingest::bind::BindError;

/// Seed the base `main.customers(id long, region string, amount long)` with a mixed
/// EU/US population: (1,EU,5), (2,US,20), (3,EU,50), (4,US,8). Returns the cp, db name,
/// pool, and the writer whose `TempDir` must be kept alive (it holds the Parquet
/// warehouse the serving engine scans).
async fn seed_customers(fx: &PgFixture) -> (PgControlPlane, String, sqlx::PgPool, IcebergWriter) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let writer = IcebergWriter::new(pool.clone(), fx.pg_dsn(&db));
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("region".to_string(), "string".to_string(), true),
        ("amount".to_string(), "long".to_string(), true),
    ];
    writer
        .seed_arrays(
            "main",
            "customers",
            &cols,
            &[
                SeedCol::Long(vec![1, 2, 3, 4]),
                SeedCol::Str(vec!["EU", "US", "EU", "US"]),
                SeedCol::Long(vec![5, 20, 50, 8]),
            ],
        )
        .await;
    (cp, db, pool, writer)
}

/// Define a region-predicate view over `main.customers` with the given projection.
async fn define_region_view(
    pool: &sqlx::PgPool,
    view: TableRef,
    region: &str,
    projection: Option<Vec<String>>,
) {
    IcebergCatalog::new(pool.clone())
        .define_view(ViewDef {
            view,
            base: tref("main", "customers"),
            predicate: Some(RowFilter::Compare {
                property: "region".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text(region.into()),
            }),
            columns: projection,
        })
        .await
        .expect("define_view");
}

/// A router-ready in-process serving engine over the fixture warehouse (SQL reads
/// only — the read suite needs no vector search).
fn mk_eng(pool: &sqlx::PgPool) -> Arc<dyn query_api::serving::ServingEngine> {
    Arc::new(InProcessServingEngine::new(IcebergCatalog::new(
        pool.clone(),
    )))
}

/// A Table-target Read grant (the coarse `grant_read` in `e2e_support` is Type-only).
async fn grant_read_table(cp: &PgControlPlane, role: &RoleId, table: TableRef) {
    cp.grant(
        role,
        Action::Read,
        PolicyTarget::Table(table),
        Effect::Allow,
    )
    .await
    .unwrap();
}

/// Acceptance criterion: two roles, each granted Read on a type bound to a DIFFERENT
/// view over the same base, read exactly-disjoint row slices; the sibling type is 403;
/// and the physical base is invisible through `/datasets` to either (no existence
/// oracle — same 404 posture the `datasets_routes` suite asserts).
#[tokio::test(flavor = "multi_thread")]
async fn disjoint_view_grants_read_disjoint_slices() {
    let fx = PgFixture::shared();
    let (cp, _db, pool, _writer) = seed_customers(fx).await;
    let cp = Arc::new(cp);

    define_region_view(&pool, tref("gov", "customers_eu"), "EU", None).await;
    define_region_view(&pool, tref("gov", "customers_us"), "US", None).await;

    cp.ontology()
        .define_type(
            ObjectType::build("CustomerEu", ("gov", "customers_eu"))
                .prop_req("id", "Long")
                .prop("region", "String")
                .prop("amount", "Long")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_type(
            ObjectType::build("CustomerUs", ("gov", "customers_us"))
                .prop_req("id", "Long")
                .prop("region", "String")
                .prop("amount", "Long")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();

    let (_e, eu_role) = subject_with_role(&cp, "eu_reader").await;
    grant_read(&cp, &eu_role, "CustomerEu").await;
    let (_u, us_role) = subject_with_role(&cp, "us_reader").await;
    grant_read(&cp, &us_role, "CustomerUs").await;

    let eng = mk_eng(&pool);

    // Each role sees only its own region's rows.
    let (s, body) = get(cp.clone(), eng.clone(), "/objects/CustomerEu", "eu_reader").await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(ids_i64(&body), vec![1, 3], "EU slice only: {body}");

    let (s, body) = get(cp.clone(), eng.clone(), "/objects/CustomerUs", "us_reader").await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(ids_i64(&body), vec![2, 4], "US slice only: {body}");

    // The sibling type is forbidden (403), not merely empty.
    let (s, _) = get(cp.clone(), eng.clone(), "/objects/CustomerUs", "eu_reader").await;
    assert_eq!(
        s,
        StatusCode::FORBIDDEN,
        "eu_reader may not read CustomerUs"
    );
    let (s, _) = get(cp.clone(), eng.clone(), "/objects/CustomerEu", "us_reader").await;
    assert_eq!(
        s,
        StatusCode::FORBIDDEN,
        "us_reader may not read CustomerEu"
    );

    // The physical base is invisible through /datasets to either (404, non-oracle).
    for who in ["eu_reader", "us_reader"] {
        let (s, _) = get(cp.clone(), eng.clone(), "/datasets/main/customers", who).await;
        assert_eq!(s, StatusCode::NOT_FOUND, "base is invisible to {who}");
    }
}

/// Exact-match ACL decoupling: a Table grant on the base does NOT reveal a view over
/// it, and a Type grant on a view-bound type does NOT reveal the base. Each grant sees
/// only its own dataset.
#[tokio::test(flavor = "multi_thread")]
async fn base_grant_does_not_leak_views_and_vice_versa() {
    let fx = PgFixture::shared();
    let (cp, _db, pool, _writer) = seed_customers(fx).await;
    let cp = Arc::new(cp);

    define_region_view(&pool, tref("gov", "customers_eu"), "EU", None).await;
    cp.ontology()
        .define_type(
            ObjectType::build("CustomerEu", ("gov", "customers_eu"))
                .prop_req("id", "Long")
                .prop("region", "String")
                .prop("amount", "Long")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();

    let (_b, base_role) = subject_with_role(&cp, "base_reader").await;
    grant_read_table(&cp, &base_role, tref("main", "customers")).await;
    let (_v, view_role) = subject_with_role(&cp, "view_reader").await;
    grant_read(&cp, &view_role, "CustomerEu").await;

    let eng = mk_eng(&pool);

    // Base grant: the base is visible, the view is NOT.
    let (s, _) = get(
        cp.clone(),
        eng.clone(),
        "/datasets/main/customers",
        "base_reader",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "base_reader sees the base");
    let (s, _) = get(
        cp.clone(),
        eng.clone(),
        "/datasets/gov/customers_eu",
        "base_reader",
    )
    .await;
    assert_eq!(
        s,
        StatusCode::NOT_FOUND,
        "a base Table grant must not leak the view"
    );

    // View (type) grant: the view is visible, the base is NOT.
    let (s, _) = get(
        cp.clone(),
        eng.clone(),
        "/datasets/gov/customers_eu",
        "view_reader",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "view_reader sees the view");
    let (s, _) = get(
        cp.clone(),
        eng.clone(),
        "/datasets/main/customers",
        "view_reader",
    )
    .await;
    assert_eq!(
        s,
        StatusCode::NOT_FOUND,
        "a view type grant must not leak the base"
    );
}

/// Preview through a view is predicate- AND projection-narrowed: only in-view rows and
/// only projected columns; the base preview (base-granted) returns every row/column.
#[tokio::test(flavor = "multi_thread")]
async fn view_preview_is_predicate_and_projection_narrowed() {
    let fx = PgFixture::shared();
    let (cp, _db, pool, _writer) = seed_customers(fx).await;
    let cp = Arc::new(cp);

    // Projection drops `amount`.
    define_region_view(
        &pool,
        tref("gov", "customers_eu"),
        "EU",
        Some(vec!["id".into(), "region".into()]),
    )
    .await;
    cp.ontology()
        .define_type(
            ObjectType::build("CustomerEu", ("gov", "customers_eu"))
                .prop_req("id", "Long")
                .prop("region", "String")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();

    let (_v, view_role) = subject_with_role(&cp, "view_reader").await;
    grant_read(&cp, &view_role, "CustomerEu").await;
    let (_b, base_role) = subject_with_role(&cp, "base_reader").await;
    grant_read_table(&cp, &base_role, tref("main", "customers")).await;

    let eng = mk_eng(&pool);

    // View preview: only EU rows (2 of 4), only the projected columns (id, region).
    let (s, body) = get(
        cp.clone(),
        eng.clone(),
        "/datasets/gov/customers_eu/preview",
        "view_reader",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(
        body["columns"],
        serde_json::json!(["id", "region"]),
        "projected columns only: {body}"
    );
    assert_eq!(
        body["rows"].as_array().unwrap().len(),
        2,
        "EU rows only: {body}"
    );

    // Base preview (base-granted): every row and column.
    let (s, body) = get(
        cp.clone(),
        eng.clone(),
        "/datasets/main/customers/preview",
        "base_reader",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(
        body["columns"],
        serde_json::json!(["id", "region", "amount"]),
        "{body}"
    );
    assert_eq!(
        body["rows"].as_array().unwrap().len(),
        4,
        "all base rows: {body}"
    );
}

/// Link traversal to a view-bound endpoint type stays inside the view: an FK link from
/// a base-bound `Order` type to a view-bound `CustomerEu` returns only in-view targets;
/// an order referencing an out-of-view customer yields no target.
#[tokio::test(flavor = "multi_thread")]
async fn link_traversal_through_view_bound_type_stays_in_view() {
    let fx = PgFixture::shared();
    let (cp, _db, pool, writer) = seed_customers(fx).await;

    // Base orders referencing customers: order 10->cust1(EU), 11->cust2(US), 12->cust3(EU).
    let ord_cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("customer_id".to_string(), "long".to_string(), false),
    ];
    writer
        .seed_arrays(
            "main",
            "orders",
            &ord_cols,
            &[
                SeedCol::Long(vec![10, 11, 12]),
                SeedCol::Long(vec![1, 2, 3]),
            ],
        )
        .await;

    let cp = Arc::new(cp);

    // The FK endpoint column `id` is inside the view projection.
    define_region_view(
        &pool,
        tref("gov", "customers_eu"),
        "EU",
        Some(vec!["id".into(), "region".into()]),
    )
    .await;

    cp.ontology()
        .define_type(
            ObjectType::build("Order", ("main", "orders"))
                .prop_req("id", "Long")
                .prop_req("customer_id", "Long")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_type(
            ObjectType::build("CustomerEu", ("gov", "customers_eu"))
                .prop_req("id", "Long")
                .prop("region", "String")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_link(LinkDef::fk(
            "customer",
            "Order",
            "CustomerEu",
            Cardinality::Many,
            "customer_id",
            "id",
        ))
        .await
        .unwrap();

    let (_r, role) = subject_with_role(&cp, "reader").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "CustomerEu").await;

    let eng = mk_eng(&pool);

    // Order -> customer traversal: only in-view (EU) targets; cust2 (US) never appears
    // even though order 11 references it.
    let (s, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Order/links/customer",
        "reader",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(
        ids_i64(&body),
        vec![1, 3],
        "traversal stays inside the view (no US target): {body}"
    );

    drop(writer);
}

/// A view's PROJECTED schema bounds what a bind may declare: a type whose properties
/// are within the projection conforms; a type naming a column outside it does not.
#[tokio::test(flavor = "multi_thread")]
async fn bind_through_view_validates_against_projected_schema() {
    let fx = PgFixture::shared();
    let (cp, _db, pool, _writer) = seed_customers(fx).await;

    // Projection drops `amount`.
    define_region_view(
        &pool,
        tref("gov", "customers_eu"),
        "EU",
        Some(vec!["id".into(), "region".into()]),
    )
    .await;
    let catalog = IcebergCatalog::new(pool.clone());

    // Properties within the projection conform.
    bind(
        &catalog,
        &cp,
        ObjectType::build("CustomerEuOk", ("gov", "customers_eu"))
            .prop_req("id", "Long")
            .prop("region", "String")
            .identity("id")
            .done(),
    )
    .await
    .expect("a type within the projection conforms");

    // A property outside the projection (`amount`) does NOT conform.
    let err = bind(
        &catalog,
        &cp,
        ObjectType::build("CustomerEuBad", ("gov", "customers_eu"))
            .prop_req("id", "Long")
            .prop("amount", "Long")
            .identity("id")
            .done(),
    )
    .await
    .expect_err("a property outside the projection must not conform");
    assert!(
        matches!(err, BindError::DoesNotConform(_)),
        "expected DoesNotConform against the projected schema, got {err:?}"
    );
}

/// An ACL row-filter / column-mask policy on a view-bound type COMPOSES with the view
/// predicate: `region='EU'` (view) ∧ `amount>10` (policy) = the single row; a mask on a
/// projected column redacts it in the results.
#[tokio::test(flavor = "multi_thread")]
async fn acl_policy_on_view_bound_type_composes_with_view_predicate() {
    let fx = PgFixture::shared();
    let (cp, _db, pool, _writer) = seed_customers(fx).await;
    let cp = Arc::new(cp);

    define_region_view(&pool, tref("gov", "customers_eu"), "EU", None).await;
    cp.ontology()
        .define_type(
            ObjectType::build("CustomerEu", ("gov", "customers_eu"))
                .prop_req("id", "Long")
                .prop("region", "String")
                .prop("amount", "Long")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();

    // Row filter amount>10 composes with region='EU': EU rows are {1(amt5), 3(amt50)};
    // the filter keeps only id 3.
    let (_f, filt_role) = subject_with_role(&cp, "filt_reader").await;
    grant_read_filtered(
        &cp,
        &filt_role,
        "CustomerEu",
        RowFilter::Compare {
            property: "amount".into(),
            op: CompareOp::Gt,
            value: ScalarValue::Int(10),
        },
    )
    .await;

    // Column mask on the projected `region`.
    let (_m, mask_role) = subject_with_role(&cp, "mask_reader").await;
    grant_read_columns(&cp, &mask_role, "CustomerEu", vec![], vec!["region".into()]).await;

    let eng = mk_eng(&pool);

    let (s, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/CustomerEu",
        "filt_reader",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(
        ids_i64(&body),
        vec![3],
        "view predicate AND row-filter policy compose: {body}"
    );

    let (s, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/CustomerEu",
        "mask_reader",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let objs = body["objects"].as_array().unwrap();
    assert_eq!(
        objs.len(),
        2,
        "both EU rows visible; only the column is masked: {body}"
    );
    for o in objs {
        assert_eq!(o["region"], "***", "region column is masked: {body}");
    }
}

/// The base→view lineage edge (emitted at `define_view`) is governed: a subject who can
/// read both endpoints sees the view node downstream of the base; a subject who can read
/// only the view has the base (its query seed) cut — the closure's seed-gated ACL posture.
#[tokio::test(flavor = "multi_thread")]
async fn lineage_shows_base_to_view_edge_under_view_grant() {
    let fx = PgFixture::shared();
    let (cp, _db, pool, _writer) = seed_customers(fx).await;
    let cp = Arc::new(cp);

    // Defining the view emits the base→view lineage edge (base=input, view=output).
    define_region_view(
        &pool,
        tref("gov", "customers_eu"),
        "EU",
        Some(vec!["id".into(), "region".into()]),
    )
    .await;
    // A view-bound type makes the view node readable via the Table→Type fallback.
    cp.ontology()
        .define_type(
            ObjectType::build("CustomerEu", ("gov", "customers_eu"))
                .prop_req("id", "Long")
                .prop("region", "String")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();

    // "both": Table grant on the base + Type grant on the view-bound type.
    let (_b, both_role) = subject_with_role(&cp, "both").await;
    grant_read_table(&cp, &both_role, tref("main", "customers")).await;
    grant_read(&cp, &both_role, "CustomerEu").await;
    // "viewonly": only the view-bound type grant (no base grant).
    let (_v, view_role) = subject_with_role(&cp, "viewonly").await;
    grant_read(&cp, &view_role, "CustomerEu").await;

    // downstream(base, depth=1): the view is a direct descendant of the base.
    let (s, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/loom/main.customers/downstream?depth=1",
        "both",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let names: Vec<String> = body["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect();
    assert!(
        names.contains(&"gov.customers_eu".to_string()),
        "the base→view edge is visible to a both-grantee: {body}"
    );

    // viewonly cannot read the base (the query seed) -> seed-gated empty (base cut).
    let (s, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/loom/main.customers/downstream?depth=1",
        "viewonly",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(
        body["datasets"].as_array().unwrap().len(),
        0,
        "the base is cut for a view-only subject (denied seed): {body}"
    );

    // Strong case: seed the walk at the ontology TYPE bound to the view (not the
    // denied base), and walk upstream. This exercises two view-specific mechanisms
    // the seed-gated assertion above cannot: (a) a view-only subject can see their
    // own view node in a lineage walk (the walk is a normal non-empty page, not the
    // seed-gated empty-like-unknown shape); (b) the base is cut MID-WALK by node
    // ACL — a different mechanism from seed rejection, since here the seed
    // (CustomerEu) is readable to both subjects and only the ancestor differs.
    //
    // Chain: main.customers --[view-definition edge]--> gov.customers_eu
    //        --[type-table-binding edge]--> CustomerEu (loom:type).
    // upstream(CustomerEu, depth=2): depth 1 = gov.customers_eu (the view's own
    // backing table, visible via the Table->Type ACL fallback); depth 2 =
    // main.customers (the base, two hops up).
    let (s, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/loom:type/CustomerEu/upstream?depth=2",
        "both",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let names: Vec<String> = body["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect();
    assert!(
        names.contains(&"gov.customers_eu".to_string())
            && names.contains(&"main.customers".to_string()),
        "both-grantee walks upstream(type) all the way to the base: {body}"
    );

    let (s, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/loom:type/CustomerEu/upstream?depth=2",
        "viewonly",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let names: Vec<String> = body["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect();
    assert!(
        names.contains(&"gov.customers_eu".to_string()),
        "view-only subject sees their own view node in the walk (not seed-gated): {body}"
    );
    assert!(
        !names.contains(&"main.customers".to_string()),
        "the base is ACL-cut mid-walk, not because the seed was denied: {body}"
    );
}
