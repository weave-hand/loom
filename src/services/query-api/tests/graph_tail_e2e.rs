//! Graph recursive-core + relational-tail e2e: GET /objects/:type/graph?path=knows*,worksAt over
//! the real HTTP router backed by an in-process Iceberg/DataFusion serving engine. Proves:
//!   - knows*,worksAt from {1} returns companies of the depth>=1 reachable people {2,3}, and
//!     EXCLUDES company 10 (person 1's own employer — the seed is not in its own reach set),
//!   - a multi-hop tail knows*,worksAt,locatedIn projects the final City type and DEDUPS (two
//!     companies sharing one city collapse to a single row — City declares identity "id", so the
//!     dedup keys on the raw identity via a windowed ROW_NUMBER),
//!   - a Read row-filter (active=true) on Person prunes the recursive core (inactive person 3 is
//!     cut, dropping its company 12),
//!   - worksAt,knows* (the `*` is not the path prefix) -> 400,
//!   - knows* alone (empty relational tail) -> 400,
//!   - *,worksAt (bare `*` core with no link name) -> 400.
//!
//! Graph: person(id, name, active, knows_id, worksat_id) with a `knows` FK self-link (1->2, 2->3)
//! and a `worksAt` FK link to company(id, cname, city_id), which has a `locatedIn` FK link to
//! city(id, cname). worksAt: 1->10, 2->11, 3->12. locatedIn: 10->22, 11->20, 12->20 (companies 11
//! and 12 share city 20). Person 3 is inactive (for the row-filter test). All three types declare
//! identity `id`.

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{
    Acl, Action, Cardinality, CompareOp, LinkDef, ObjectType, Ontology, Policy, PolicyTarget,
    RowFilter, ScalarValue, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{
    InProcessServingEngine, get, grant_read, ids_i64 as ids, prop, subject_with_role, tref,
};

/// Seed person/company/city. knows: 1->2, 2->3 (FK knows_id). worksAt: 1->10, 2->11, 3->12 (FK
/// worksat_id). locatedIn: 10->22, 11->20, 12->20 (FK city_id; companies 11 & 12 share city 20).
/// Person 3 is INACTIVE. The caller MUST keep the returned `IcebergWriter` alive.
async fn setup(fx: &PgFixture) -> (PgControlPlane, InProcessServingEngine, IcebergWriter) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);

    // person(id, name, active, knows_id, worksat_id).
    let person = tref("main", "person");
    writer
        .seed_arrays(
            "main",
            "person",
            &[
                ("id".to_string(), "long".to_string(), false),
                ("name".to_string(), "string".to_string(), true),
                ("active".to_string(), "boolean".to_string(), false),
                ("knows_id".to_string(), "long".to_string(), true),
                ("worksat_id".to_string(), "long".to_string(), true),
            ],
            &[
                SeedCol::Long(vec![1, 2, 3]),
                SeedCol::Str(vec!["ann", "bob", "cal"]),
                // person 3 inactive (row-filter test); 1 and 2 active.
                SeedCol::Bool(vec![true, true, false]),
                // knows: 1->2, 2->3 (3 has no outbound knows edge).
                SeedCol::NullableLong(vec![Some(2), Some(3), None]),
                // worksAt: 1->10, 2->11, 3->12.
                SeedCol::NullableLong(vec![Some(10), Some(11), Some(12)]),
            ],
        )
        .await;

    // company(id, cname, city_id). locatedIn: 10->22, 11->20, 12->20.
    let company = tref("main", "company");
    writer
        .seed_arrays(
            "main",
            "company",
            &[
                ("id".to_string(), "long".to_string(), false),
                ("cname".to_string(), "string".to_string(), true),
                ("city_id".to_string(), "long".to_string(), true),
            ],
            &[
                SeedCol::Long(vec![10, 11, 12]),
                SeedCol::Str(vec!["x", "acme", "beta"]),
                SeedCol::NullableLong(vec![Some(22), Some(20), Some(20)]),
            ],
        )
        .await;

    // city(id, cname).
    let city = tref("main", "city");
    writer
        .seed_arrays(
            "main",
            "city",
            &[
                ("id".to_string(), "long".to_string(), false),
                ("cname".to_string(), "string".to_string(), true),
            ],
            &[SeedCol::Long(vec![20, 22]), SeedCol::Str(vec!["hq", "z"])],
        )
        .await;

    cp.define_type(
        ObjectType::build("Person", (person.schema.as_str(), person.name.as_str()))
            .add_prop(prop("id", "Long", true))
            .add_prop(prop("name", "String", false))
            .add_prop(prop("active", "Boolean", true))
            .add_prop(prop("knows_id", "Long", false))
            .add_prop(prop("worksat_id", "Long", false))
            .identity("id")
            .done(),
    )
    .await
    .unwrap();
    cp.define_type(
        ObjectType::build("Company", (company.schema.as_str(), company.name.as_str()))
            .add_prop(prop("id", "Long", true))
            .add_prop(prop("cname", "String", false))
            .add_prop(prop("city_id", "Long", false))
            .identity("id")
            .done(),
    )
    .await
    .unwrap();
    cp.define_type(
        ObjectType::build("City", (city.schema.as_str(), city.name.as_str()))
            .add_prop(prop("id", "Long", true))
            .add_prop(prop("cname", "String", false))
            .identity("id")
            .done(),
    )
    .await
    .unwrap();

    // knows: FK self-link Person -> Person via knows_id.
    cp.define_link(LinkDef::fk(
        "knows",
        "Person",
        "Person",
        Cardinality::Many,
        "knows_id",
        "id",
    ))
    .await
    .unwrap();
    // worksAt: FK link Person -> Company via worksat_id.
    cp.define_link(LinkDef::fk(
        "worksAt",
        "Person",
        "Company",
        Cardinality::One,
        "worksat_id",
        "id",
    ))
    .await
    .unwrap();
    // locatedIn: FK link Company -> City via city_id.
    cp.define_link(LinkDef::fk(
        "locatedIn",
        "Company",
        "City",
        Cardinality::One,
        "city_id",
        "id",
    ))
    .await
    .unwrap();

    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    (cp, eng, writer)
}

#[tokio::test(flavor = "multi_thread")]
async fn core_then_tail_projects_companies_of_reachable_people() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Company").await;

    // knows* from {1} depth 3 => reachable people {2,3} (1 excluded: depth>=1). worksAt of {2,3}
    // = companies {11,12}. Company 10 (person 1's employer) is ABSENT — 1 is not in its own reach.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=knows*,worksAt&depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![11, 12],
        "companies of reachable {{2,3}}, excluding seed's company 10: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_hop_tail_projects_cities_and_dedups() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Company").await;
    grant_read(&cp, &role, "City").await;

    // knows*,worksAt,locatedIn from {1}: reach {2,3} -> companies {11,12} -> cities {20,20}.
    // The identity-keyed window dedup collapses the shared city 20 to a single row. City 22
    // (company 10's city) is absent. => {20}.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=knows*,worksAt,locatedIn&depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![20],
        "two companies share city 20, deduped to one row; city 22 absent: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn row_filter_prunes_the_recursive_core() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    // active=true on Person cuts inactive person 3 from the recursive core. From {1}: 1->2 (active,
    // d1), 2->3 (3 inactive, cut). reach = {2}. worksAt of {2} = {11}. (Unfiltered it is {11,12}.)
    let (_c, role) = subject_with_role(&cp, "carol").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Company").await;
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
        "/objects/Person/graph?path=knows*,worksAt&depth=3&_ids=1",
        "carol",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![11],
        "inactive person 3 cut from the core drops company 12: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn star_not_on_first_segment_is_400() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Company").await;

    // `*` on the second segment -> the recursive core is not the path prefix -> 400.
    // 400 reason: "first path segment"
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=worksAt,knows*&depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "`*` not on the first segment -> 400"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_tail_is_400() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // `knows*` alone (no tail) -> 400.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=knows*&depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "empty relational tail -> 400"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bare_star_core_is_400() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // `*,worksAt` has an empty recursive core (bare `*` with no link name) -> 400.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=*,worksAt&depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "bare * core is empty -> 400"
    );
}
