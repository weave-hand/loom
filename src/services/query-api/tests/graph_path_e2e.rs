//! Graph path-cycle e2e: GET /objects/:type/graph?path=l1,l2 over the real HTTP router
//! backed by an in-process Iceberg/DataFusion serving engine. Proves the bounded recursive
//! reachability shape {objects:[...]} over a MULTI-LINK cyclic path
//! `Person --memberOf--> Team --hasMember--> Person` (shared-team membership):
//!   - depth bounds (a bridge person makes depth 2 reach further than depth 1),
//!   - a cycle (the pattern inherently revisits the seed) terminates and the set dedups,
//!   - a Read row-filter on the INTERMEDIATE `Team` (active=true) prunes the Persons
//!     reachable only through the inactive team (intermediate governance in the recursion),
//!   - a non-cyclic `?path=worksAt` (ends at Company) -> 400 (NotCyclicPath),
//!   - an absent/empty `?path=` -> 400.
//!
//! Graph: a `person` table, a `team` table, and a `membership(person_id, team_id)` join
//! table backing BOTH directions (memberOf: Person -> Team; hasMember: Team -> Person).
//! Teams: T1{1,2}, T2{3,4,5}, T3{5,6}. Person 5 bridges T2 and T3, so from person 3 the
//! shared-membership reach grows with depth: depth 1 -> {3,4,5} (T2), depth 2 -> {3,4,5,6}
//! (via 5 -> T3 -> 6). Team T3 is INACTIVE, so an active=true Team filter prunes 6. A
//! `company` table + a `worksAt` FK link Person -> Company drives the non-cyclic case.

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

/// Seed the shared-membership graph: person, team, membership(person_id, team_id), company.
/// Teams T1{1,2}, T2{3,4,5}, T3{5,6}; T3 inactive. `memberOf` Person->Team and `hasMember`
/// Team->Person both back onto `membership`, forming a Person->Team->Person cycle. A
/// `worksAt` FK Person->Company drives the non-cyclic case. Person & Team declare identity
/// `id`. Caller MUST keep the `IcebergWriter` alive (its TempDir holds the Parquet read).
async fn setup(fx: &PgFixture) -> (PgControlPlane, InProcessServingEngine, IcebergWriter) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);

    // person(id, name, company_id): 6 persons; company drives the worksAt link.
    let person = tref("main", "person");
    writer
        .seed_arrays(
            "main",
            "person",
            &[
                ("id".to_string(), "long".to_string(), false),
                ("name".to_string(), "string".to_string(), true),
                ("company_id".to_string(), "long".to_string(), true),
            ],
            &[
                SeedCol::Long(vec![1, 2, 3, 4, 5, 6]),
                SeedCol::Str(vec!["ann", "bob", "cal", "dee", "eve", "fin"]),
                SeedCol::NullableLong(vec![Some(7), Some(7), Some(8), Some(8), Some(8), Some(8)]),
            ],
        )
        .await;

    // team(id, name, active): T3 (id=3) is INACTIVE (intermediate governance test).
    let team = tref("main", "team");
    writer
        .seed_arrays(
            "main",
            "team",
            &[
                ("id".to_string(), "long".to_string(), false),
                ("name".to_string(), "string".to_string(), true),
                ("active".to_string(), "boolean".to_string(), false),
            ],
            &[
                SeedCol::Long(vec![1, 2, 3]),
                SeedCol::Str(vec!["red", "green", "blue"]),
                SeedCol::Bool(vec![true, true, false]),
            ],
        )
        .await;

    // membership(person_id, team_id): T1{1,2}, T2{3,4,5}, T3{5,6}. Person 5 bridges T2/T3.
    let membership = tref("main", "membership");
    writer
        .seed_arrays(
            "main",
            "membership",
            &[
                ("person_id".to_string(), "long".to_string(), false),
                ("team_id".to_string(), "long".to_string(), false),
            ],
            &[
                SeedCol::Long(vec![1, 2, 3, 4, 5, 5, 6]),
                SeedCol::Long(vec![1, 1, 2, 2, 2, 3, 3]),
            ],
        )
        .await;

    // company(id, name): targets for the non-cyclic worksAt link.
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
            prop("company_id", "Long", false),
        ],
        derived: vec![],
        table: person.clone(),
        identity: Some("id".into()),
        version: None,
    })
    .await
    .unwrap();
    // Team declares identity `id`.
    cp.define_type(ObjectType {
        name: TypeName("Team".into()),
        properties: vec![
            prop("id", "Long", true),
            prop("name", "String", false),
            prop("active", "Boolean", true),
        ],
        derived: vec![],
        table: team.clone(),
        identity: Some("id".into()),
        version: None,
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("Company".into()),
        properties: vec![prop("id", "Long", true), prop("name", "String", false)],
        derived: vec![],
        table: company.clone(),
        identity: Some("id".into()),
        version: None,
    })
    .await
    .unwrap();

    // `memberOf`: Person -> Team via the membership join-table.
    cp.define_link(LinkDef {
        name: "memberOf".into(),
        from: TypeName("Person".into()),
        to: TypeName("Team".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::JoinTable {
            table: membership.clone(),
            from_key: "id".into(),
            from_column: "person_id".into(),
            to_column: "team_id".into(),
            to_key: "id".into(),
        },
    })
    .await
    .unwrap();
    // `hasMember`: Team -> Person via the same membership join-table (the inverse edge).
    cp.define_link(LinkDef {
        name: "hasMember".into(),
        from: TypeName("Team".into()),
        to: TypeName("Person".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::JoinTable {
            table: membership.clone(),
            from_key: "id".into(),
            from_column: "team_id".into(),
            to_column: "person_id".into(),
            to_key: "id".into(),
        },
    })
    .await
    .unwrap();
    // `worksAt`: a non-cyclic FK link Person -> Company (drives NotCyclicPath -> 400).
    cp.define_link(LinkDef {
        name: "worksAt".into(),
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
async fn shared_membership_reach_depth_bounds_and_cycle() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Team").await;

    // depth=1 from {1}: memberOf T1, hasMember -> {1, 2} (the cycle revisits 1; deduped).
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=memberOf,hasMember&depth=1&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![1, 2],
        "depth 1 from 1 (T1 cluster): {body}"
    );

    // depth=1 from {3}: T2 -> {3, 4, 5}. Person 5 bridges to T3 but only at the next hop.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=memberOf,hasMember&depth=1&_ids=3",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![3, 4, 5],
        "depth 1 from 3 (T2 cluster): {body}"
    );

    // depth=2 from {3}: via bridge person 5 (also on T3), T3 -> {5, 6}, so 6 joins. The
    // pattern inherently revisits {3,4,5}; it terminates and the set dedups to {3,4,5,6}.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=memberOf,hasMember&depth=2&_ids=3",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![3, 4, 5, 6],
        "depth 2 from 3 reaches further via the T3 bridge + dedups the cycle: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn intermediate_team_filter_prunes_reach_through_inactive_team() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    // A Read row-filter active=true on the INTERMEDIATE `Team` makes T3 (inactive)
    // un-traversable inside the recursion, so person 6 — reachable only THROUGH T3 — is
    // pruned. From {3}, depth 2 now stays at {3, 4, 5} (T2 only).
    let (_c, role) = subject_with_role(&cp, "carol").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Team").await;
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Team".into())),
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
        "/objects/Person/graph?path=memberOf,hasMember&depth=2&_ids=3",
        "carol",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![3, 4, 5],
        "the inactive Team T3 is pruned, so person 6 (reachable only through it) disappears: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn non_cyclic_path_is_400() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Team").await;
    // Read on Company so the path RESOLVES to the cyclic check: read_graph_reach Read-gates
    // every landed type BEFORE the cyclic check, so without Read on Company the worksAt
    // landing returns 403 (Forbidden), masking the 400 we want to assert.
    grant_read(&cp, &role, "Company").await;

    // worksAt is Person -> Company; following it forward lands on Company, NOT back at Person,
    // so the path is not a cycle -> 400 NotCyclicPath. (memberOf,worksAt would instead 404 as
    // worksAt is not a Team outbound link; a single non-cyclic link isolates NotCyclicPath.)
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=worksAt&depth=2&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a non-cyclic path -> 400 NotCyclicPath"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn absent_or_empty_path_is_400() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Team").await;

    // Absent ?path= -> 400.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?depth=2&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "absent path -> 400");

    // Empty ?path= -> 400.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=&depth=2&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "empty path -> 400");
}
