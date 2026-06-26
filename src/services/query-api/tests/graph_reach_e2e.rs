//! Graph reachability e2e: GET /objects/:type/graph/:link over the real HTTP router
//! backed by an in-process Iceberg/DataFusion serving engine. Proves the bounded
//! recursive reachability shape {objects:[...]} over a self-link:
//!   - depth bounds (reachable-within-1 vs -2 vs -3 differ),
//!   - a cycle (1->2->3->1) terminates and the node set is deduped,
//!   - a Read row-filter (active=true) prunes reachability THROUGH a blocked node,
//!   - a non-self link -> 400 (NotCyclicPath),
//!   - ?depth=0 and ?depth=99 -> 400 (out-of-range depth).
//!
//! The graph is a single `Person` table with a `knows(a, b)` join-table SELF-link and a
//! boolean `active` column. Edge set: 1->2, 2->3, 3->1 (a 3-cycle), 3->4. Person declares
//! identity `id`. A separate `employer` FK link Person -> Company drives the non-self case.

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{
    Acl, Action, Cardinality, CompareOp, LinkBacking, LinkDef, ObjectType, Ontology, Policy,
    PolicyTarget, RowFilter, ScalarValue, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{
    InProcessServingEngine, get, grant_read, ids_i64 as ids, prop, subject_with_role, tref,
};

/// Seed a single `Person` table with a `knows(a, b)` join-table self-link forming a graph
/// `1->2, 2->3, 3->1 (cycle), 3->4`, plus a boolean `active` column (node 2 inactive). A
/// `company` table + `employer` FK link Person -> Company drives the non-self-link case.
/// Person declares identity `id`. Returns the wired control plane + serving engine; the
/// caller MUST keep the `IcebergWriter` alive (its TempDir holds the Parquet read).
async fn setup(fx: &PgFixture) -> (PgControlPlane, InProcessServingEngine, IcebergWriter) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);

    // person(id, name, active, company_id): node 2 is INACTIVE (governance test).
    let person = tref("main", "person");
    writer
        .seed_arrays(
            "main",
            "person",
            &[
                ("id".to_string(), "long".to_string(), false),
                ("name".to_string(), "string".to_string(), true),
                ("active".to_string(), "boolean".to_string(), false),
                ("company_id".to_string(), "long".to_string(), true),
            ],
            &[
                SeedCol::Long(vec![1, 2, 3, 4]),
                SeedCol::Str(vec!["ann", "bob", "cal", "dee"]),
                // node 2 (bob) inactive; the rest active.
                SeedCol::Bool(vec![true, false, true, true]),
                SeedCol::NullableLong(vec![Some(7), Some(7), Some(8), Some(8)]),
            ],
        )
        .await;

    // knows(a, b): the self-link edge set 1->2, 2->3, 3->1 (cycle), 3->4.
    let knows = tref("main", "knows");
    writer
        .seed_arrays(
            "main",
            "knows",
            &[
                ("a".to_string(), "long".to_string(), false),
                ("b".to_string(), "long".to_string(), false),
            ],
            &[
                SeedCol::Long(vec![1, 2, 3, 3]),
                SeedCol::Long(vec![2, 3, 1, 4]),
            ],
        )
        .await;

    // company(id, name): targets for the non-self employer link.
    let company = tref("main", "company");
    writer
        .seed_arrays(
            "main",
            "company",
            &[
                ("id".to_string(), "long".to_string(), false),
                ("name".to_string(), "string".to_string(), true),
            ],
            &[
                SeedCol::Long(vec![7, 8]),
                SeedCol::Str(vec!["acme", "globex"]),
            ],
        )
        .await;

    // Person declares identity `id`.
    cp.define_type(ObjectType {
        name: TypeName("Person".into()),
        properties: vec![
            prop("id", "Long", true),
            prop("name", "String", false),
            prop("active", "Boolean", true),
            prop("company_id", "Long", false),
        ],
        derived: vec![],
        table: person.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("Company".into()),
        properties: vec![prop("id", "Long", true), prop("name", "String", false)],
        derived: vec![],
        table: company.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();

    // `knows`: the join-table SELF-link Person -> Person.
    cp.define_link(LinkDef {
        name: "knows".into(),
        from: TypeName("Person".into()),
        to: TypeName("Person".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::JoinTable {
            table: knows.clone(),
            from_key: "id".into(),
            from_column: "a".into(),
            to_column: "b".into(),
            to_key: "id".into(),
        },
    })
    .await
    .unwrap();
    // `employer`: a non-self FK link Person -> Company (drives NotCyclicPath -> 400).
    cp.define_link(LinkDef {
        name: "employer".into(),
        from: TypeName("Person".into()),
        to: TypeName("Company".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: "company_id".into(),
            to_column: "id".into(),
        },
    })
    .await
    .unwrap();

    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    (cp, eng, writer)
}

#[tokio::test(flavor = "multi_thread")]
async fn depth_bounds_and_cycle_termination() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // depth=1 from {1}: one hop 1->2 => {2}.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=1&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ids(&body), vec![2], "depth 1: {body}");

    // depth=2 from {1}: 1->2->3 => {2, 3}.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=2&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ids(&body), vec![2, 3], "depth 2: {body}");

    // depth=3 from {1}: 1->2->3->{1 (cycle), 4}. Terminates despite the 1->2->3->1 cycle;
    // the node set is deduped (1 reappears exactly once via the cycle) => {1, 2, 3, 4}.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![1, 2, 3, 4],
        "depth 3 over the cycle terminates + dedups: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn row_filter_prunes_reachability_through_blocked_node() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    // A Read row-filter active=true on Person makes node 2 (inactive) unreachable; since 2
    // is the only first hop out of 1, ALL reachability THROUGH 2 (3, 4, the cycle back to 1)
    // is cut. From {1}, depth 3 now reaches nothing.
    let (_c, role) = subject_with_role(&cp, "carol").await;
    grant_read(&cp, &role, "Person").await;
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Person".into())),
            row_filter: Some(RowFilter::Compare {
                property: "active".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Bool(true),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=3&_ids=1",
        "carol",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        Vec::<i64>::new(),
        "reachability through the inactive node 2 is cut: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn non_self_link_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Company").await;

    // employer is Person -> Company (not a self-link) -> 400 NotCyclicPath.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/employer?depth=2&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "recursing a non-self link -> 400"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn out_of_range_depth_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // depth=0 (below 1) -> 400.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=0&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "depth=0 -> 400");

    // depth=99 (above the cap 10) -> 400.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=99&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "depth=99 -> 400");
}
