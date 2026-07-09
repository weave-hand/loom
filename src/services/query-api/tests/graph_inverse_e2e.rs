//! Inverse-in-path e2e: GET /objects/:type/graph?path=memberOf,~memberOf over the real HTTP
//! router + in-process Iceberg/DataFusion serving, declaring ONLY `memberOf` (Person -> Team
//! over the `membership` join table) -- NO `hasMember` link is defined here. Proves the
//! inverse hop needs no declared return link: `memberOf.reversed()` is byte-identical to
//! `hasMember`'s backing over the same join table (both land Team -> Person via
//! `membership(team_id, person_id)`), so the mixed cycle `memberOf,~memberOf` reproduces
//! part-2's (`graph_path_e2e.rs`) `memberOf,hasMember` reachability exactly:
//!   - depth bounds + equivalence: from person 1 (T1 = {1,2}), depth=1 -> {1,2}; from person 3
//!     (T2 = {3,4,5}), depth=1 -> {3,4,5}; depth=2 reaches further via bridge person 5's T3
//!     membership -> {3,4,5,6} -- the two depths differ, matching part-2's asserted sets.
//!   - cycle terminates + dedups (load-bearing): the pattern inherently revisits the seed;
//!     at depth=3 the result set stays {3,4,5,6} -- finite, and the id list carries no
//!     duplicates (SQL `DISTINCT` dedup holds end-to-end over the reversed join too).
//!   - intermediate governance: a Read row-filter (active=true) on the INTERMEDIATE `Team`
//!     prunes reach through the inactive T3, so person 6 (reachable only through it) drops
//!     out: depth=2 from 3 -> {3,4,5}.
//!   - error arms:
//!     - `?path=~memberOf` queried on `Team` is non-cyclic (the single inverse hop lands on
//!       Person, not back at Team) -> 400 NotCyclicPath.
//!     - `?path=~nope` (no link named `nope` inbound to Person) -> 404 UnknownLink.
//!     - the `~`+`*` HTTP-gating regression this suite exists to protect: `?path=~memberOf*`
//!       must NOT be routed to the recursive-tail branch, because the `starred` filter in
//!       `get_graph_path` requires `direction == Direction::Forward`; a `~`-prefixed starred
//!       element stays a path-cycle inverse hop literally named `memberOf*`, resolved via
//!       `links_to` -> no link named `memberOf*` -> 404 UnknownLink. Without the
//!       `Forward`-only guard this would silently reroute into the recursive-tail path
//!       instead of erroring.
//!
//! Graph: the identical `person`/`team`/`membership(person_id, team_id)` seed as
//! `graph_path_e2e.rs` -- Teams T1{1,2}, T2{3,4,5}, T3{5,6}; T3 INACTIVE; person 5 bridges
//! T2 and T3 -- but declaring ONLY the `memberOf` (Person -> Team) link; `hasMember` is
//! deliberately NOT defined, so every `~memberOf` hop below is resolved purely from the
//! inverse of `memberOf`.

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

/// Seed the shared-membership graph: person, team, membership(person_id, team_id). Teams
/// T1{1,2}, T2{3,4,5}, T3{5,6}; T3 inactive; person 5 bridges T2/T3. Declares ONLY `memberOf`
/// (Person -> Team over `membership`) -- `hasMember` is deliberately NOT defined. Person &
/// Team declare identity `id`. Caller MUST keep the `IcebergWriter` alive (its TempDir holds
/// the Parquet read).
async fn setup(fx: &PgFixture) -> (PgControlPlane, InProcessServingEngine, IcebergWriter) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);

    // person(id, name): 6 persons.
    let person = tref("main", "person");
    writer
        .seed_arrays(
            "main",
            "person",
            &[
                ("id".to_string(), "long".to_string(), false),
                ("name".to_string(), "string".to_string(), true),
            ],
            &[
                SeedCol::Long(vec![1, 2, 3, 4, 5, 6]),
                SeedCol::Str(vec!["ann", "bob", "cal", "dee", "eve", "fin"]),
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

    // Person declares identity `id`.
    cp.define_type(ObjectType {
        name: TypeName("Person".into()),
        properties: vec![prop("id", "Long", true), prop("name", "String", false)],
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

    // `memberOf`: Person -> Team via the membership join-table. This is the ONLY link
    // declared -- `~memberOf` (its inverse) is resolved purely from this definition.
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

    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    (cp, eng, writer)
}

#[tokio::test(flavor = "multi_thread")]
async fn mixed_cycle_matches_part_two_reach_and_depths_differ() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Team").await;

    // depth=1 from {1}: memberOf -> T1, ~memberOf -> {1, 2} (the cycle revisits 1; deduped).
    // Matches graph_path_e2e's `memberOf,hasMember&depth=1&_ids=1` result exactly.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=memberOf,~memberOf&depth=1&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let depth1_from_1 = ids(&body);
    assert_eq!(
        depth1_from_1,
        vec![1, 2],
        "depth 1 from 1 (T1 cluster): {body}"
    );

    // depth=1 from {3}: T2 -> {3, 4, 5}. Person 5 bridges to T3 but only at the next hop.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=memberOf,~memberOf&depth=1&_ids=3",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let depth1_from_3 = ids(&body);
    assert_eq!(
        depth1_from_3,
        vec![3, 4, 5],
        "depth 1 from 3 (T2 cluster): {body}"
    );

    // depth=2 from {3}: via bridge person 5 (also on T3), T3 -> {5, 6}, so 6 joins. Matches
    // graph_path_e2e's `memberOf,hasMember&depth=2&_ids=3` result exactly.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=memberOf,~memberOf&depth=2&_ids=3",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let depth2_from_3 = ids(&body);
    assert_eq!(
        depth2_from_3,
        vec![3, 4, 5, 6],
        "depth 2 from 3 reaches further via the T3 bridge: {body}"
    );
    assert_ne!(
        depth1_from_3, depth2_from_3,
        "distinct depths must produce distinct reach: {depth1_from_3:?} vs {depth2_from_3:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cycle_terminates_and_dedups_at_depth_three() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Team").await;

    // The mixed cycle inherently revisits the seed (memberOf then its own inverse lands
    // back on Person). At depth=3 the reach from {3} must still be the finite, deduped
    // {3,4,5,6} -- neither growing further nor containing duplicate ids.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=memberOf,~memberOf&depth=3&_ids=3",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let got = ids(&body);
    assert_eq!(
        got,
        vec![3, 4, 5, 6],
        "the cycle terminates at the same shared-team set as depth=2: {body}"
    );
    let mut deduped = got.clone();
    deduped.dedup();
    assert_eq!(
        got.len(),
        deduped.len(),
        "result set must not contain duplicate ids: {got:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn intermediate_team_filter_prunes_reach_through_inactive_team() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    // A Read row-filter active=true on the INTERMEDIATE `Team` makes T3 (inactive)
    // un-traversable inside the recursion, so person 6 -- reachable only THROUGH T3 -- is
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
        "/objects/Person/graph?path=memberOf,~memberOf&depth=2&_ids=3",
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
async fn single_inverse_hop_on_team_is_non_cyclic_400() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Team").await;

    // `~memberOf` from Team: the single inverse hop resolves memberOf's `to` (Team) ->
    // `from` (Person), landing on Person, NOT back at Team -> not a cycle -> 400
    // NotCyclicPath.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Team/graph?path=~memberOf&depth=2&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a non-cyclic inverse path -> 400 NotCyclicPath"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_inverse_link_is_404() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Team").await;

    // No link named `nope` is inbound to Person -> UnknownLink -> 404.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=~nope&depth=2&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "an unknown inverse link -> 404 UnknownLink"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn starred_inverse_hop_is_404_not_recursive_tail() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Team").await;

    // Regression: `~memberOf*` must NOT be routed to the recursive-tail (`*`) branch. The
    // `starred` filter in `get_graph_path` requires `direction == Direction::Forward`, so a
    // `~`-prefixed starred element stays a path-cycle inverse hop literally named
    // `memberOf*`. `links_to(Person)` has no link by that name -> 404 UnknownLink. Without
    // the Forward-only guard on `starred`, this would instead be silently accepted as a
    // recursive-core + tail path.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=~memberOf*&depth=2&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "~memberOf* stays an inverse hop named `memberOf*` -> 404 UnknownLink, not a recursive tail"
    );
}
