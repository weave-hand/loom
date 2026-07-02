//! Shortest-path-tree e2e: GET /objects/:type/graph/:link?tree=true and
//! /objects/:type/graph?path=...&tree=true over the real HTTP router backed by an in-process
//! Iceberg/DataFusion serving engine. Proves the happy path (a {roots, nodes} tree with parent
//! pointers), backward compatibility (no flag -> flat set), the reject cases (tree over
//! union/recursive-core routes, and a non-boolean tree token), and the tree's core properties:
//! deterministic tie-break, valid reconstruction, cycle termination, ACL pruning, and forests.
//!
//! Graph: a single `Person` table with a `knows(a, b)` join-table SELF-link. Person declares
//! identity `id`.

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
    InProcessServingEngine, get, grant_read, prop, subject_with_role, tree_nodes, tree_roots, tref,
};

/// Seed `person(id, name)` with a `knows(a, b)` join-table self-link. `edges` is the (a, b)
/// edge set. Person declares identity `id`. Caller MUST keep the returned IcebergWriter alive.
async fn setup(
    fx: &PgFixture,
    ids: Vec<i64>,
    names: Vec<&str>,
    edges: &[(i64, i64)],
) -> (PgControlPlane, InProcessServingEngine, IcebergWriter) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    let person = tref("main", "person");
    writer
        .seed_arrays(
            "main",
            "person",
            &[
                ("id".to_string(), "long".to_string(), false),
                ("name".to_string(), "string".to_string(), true),
            ],
            &[SeedCol::Long(ids), SeedCol::Str(names)],
        )
        .await;

    let (a, b): (Vec<i64>, Vec<i64>) = edges.iter().copied().unzip();
    let knows = tref("main", "knows");
    writer
        .seed_arrays(
            "main",
            "knows",
            &[
                ("a".to_string(), "long".to_string(), false),
                ("b".to_string(), "long".to_string(), false),
            ],
            &[SeedCol::Long(a), SeedCol::Long(b)],
        )
        .await;

    cp.define_type(ObjectType {
        name: TypeName("Person".into()),
        properties: vec![prop("id", "Long", true), prop("name", "String", false)],
        derived: vec![],
        table: person.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();
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

    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    (cp, eng, writer)
}

/// Seed person(id, name, active) + knows(a,b) self-link with the given edges + per-id active
/// flags. Person declares identity `id`. Caller keeps the IcebergWriter alive.
async fn setup_active(
    fx: &PgFixture,
    ids: Vec<i64>,
    names: Vec<&str>,
    active: Vec<bool>,
    edges: &[(i64, i64)],
) -> (PgControlPlane, InProcessServingEngine, IcebergWriter) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    let person = tref("main", "person");
    writer
        .seed_arrays(
            "main",
            "person",
            &[
                ("id".to_string(), "long".to_string(), false),
                ("name".to_string(), "string".to_string(), true),
                ("active".to_string(), "boolean".to_string(), false),
            ],
            &[
                SeedCol::Long(ids),
                SeedCol::Str(names),
                SeedCol::Bool(active),
            ],
        )
        .await;

    let (a, b): (Vec<i64>, Vec<i64>) = edges.iter().copied().unzip();
    let knows = tref("main", "knows");
    writer
        .seed_arrays(
            "main",
            "knows",
            &[
                ("a".to_string(), "long".to_string(), false),
                ("b".to_string(), "long".to_string(), false),
            ],
            &[SeedCol::Long(a), SeedCol::Long(b)],
        )
        .await;

    cp.define_type(ObjectType {
        name: TypeName("Person".into()),
        properties: vec![
            prop("id", "Long", true),
            prop("name", "String", false),
            prop("active", "Boolean", true),
        ],
        derived: vec![],
        table: person.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();
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

    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    (cp, eng, writer)
}

#[tokio::test(flavor = "multi_thread")]
async fn linear_chain_tree_has_root_and_parent_pointers() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(
        fx,
        vec![1, 2, 3],
        vec!["ann", "bob", "cal"],
        &[(1, 2), (2, 3)],
    )
    .await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // depth=3 from {1}: tree 1(root) -> 2 -> 3.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=3&_ids=1&tree=true",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(tree_roots(&body), vec![1], "root is the seed: {body}");
    let nodes = tree_nodes(&body);
    assert_eq!(
        nodes,
        vec![(1, 0, None), (2, 1, Some(1)), (3, 2, Some(2))],
        "root + parent pointers along the chain: {body}"
    );
    // the object projection is present per node
    assert_eq!(
        body["nodes"][0]["object"]["name"],
        serde_json::json!("ann"),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn without_flag_returns_the_flat_set() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(
        fx,
        vec![1, 2, 3],
        vec!["ann", "bob", "cal"],
        &[(1, 2), (2, 3)],
    )
    .await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // No ?tree= -> the reachable SET shape {objects:[...]}, unchanged.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.get("objects").is_some(),
        "flat set shape without the flag: {body}"
    );
    assert!(
        body.get("roots").is_none(),
        "no tree shape without the flag: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tree_with_links_union_is_400() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx, vec![1, 2], vec!["ann", "bob"], &[(1, 2)]).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // ?links= (union) + tree -> 400 (tree over union is a deferred follow-on).
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?links=knows&depth=2&_ids=1&tree=true",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "tree + links -> 400");
}

#[tokio::test(flavor = "multi_thread")]
async fn tree_with_starred_path_is_400() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx, vec![1, 2], vec!["ann", "bob"], &[(1, 2)]).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // ?path=knows* (recursive-core + tail) + tree -> 400 (deferred follow-on).
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=knows*&depth=2&_ids=1&tree=true",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "tree + starred path -> 400"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tree_with_invalid_value_is_400() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx, vec![1, 2], vec!["ann", "bob"], &[(1, 2)]).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // ?tree=maybe is neither true nor false -> 400.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=2&_ids=1&tree=maybe",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "tree=maybe -> 400");
}

#[tokio::test(flavor = "multi_thread")]
async fn tie_break_picks_smallest_parent_deterministically() {
    let fx = PgFixture::shared();
    // Two shortest paths to 4: 1->2->4 and 1->3->4. Tie-break => parent(4) == 2 (smaller id).
    let (cp, eng, _writer) = setup(
        fx,
        vec![1, 2, 3, 4],
        vec!["a", "b", "c", "d"],
        &[(1, 2), (1, 3), (2, 4), (3, 4)],
    )
    .await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // Run several times; the settled tie-break must be scan/join-order independent.
    for run in 0..5 {
        let (status, body) = get(
            cp.clone(),
            eng.clone(),
            "/objects/Person/graph/knows?depth=3&_ids=1&tree=true",
            "alice",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "run {run}: {body}");
        let nodes = tree_nodes(&body);
        let four = nodes
            .iter()
            .find(|(id, _, _)| *id == 4)
            .copied()
            .expect("node 4 present");
        assert_eq!(
            four,
            (4, 2, Some(2)),
            "run {run}: 4 settles to depth 2, parent 2: {body}"
        );
        assert_eq!(tree_roots(&body), vec![1], "run {run}: root is 1: {body}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn parent_pointers_form_a_valid_rooted_tree() {
    let fx = PgFixture::shared();
    // Linear plus a branch: 1->2->3, 2->4. Every non-root has a parent that is itself in the
    // tree, and reconstructed depth == reported depth (no dangling parent).
    let (cp, eng, _writer) = setup(
        fx,
        vec![1, 2, 3, 4],
        vec!["a", "b", "c", "d"],
        &[(1, 2), (2, 3), (2, 4)],
    )
    .await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=3&_ids=1&tree=true",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let nodes = tree_nodes(&body);
    let depth_of: std::collections::HashMap<i64, i64> =
        nodes.iter().map(|(id, d, _)| (*id, *d)).collect();
    for (id, depth, parent) in &nodes {
        match parent {
            None => assert_eq!(*depth, 0, "a root has depth 0 (id {id}): {body}"),
            Some(p) => {
                let pd = depth_of.get(p).copied().expect("parent present in tree");
                assert_eq!(
                    *depth,
                    pd + 1,
                    "child depth = parent depth + 1 (id {id}): {body}"
                );
            }
        }
    }
    assert_eq!(nodes.len(), 4, "all four nodes present: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn cycle_terminates_and_seed_stays_a_root() {
    let fx = PgFixture::shared();
    // 1->2->3->1 cycle. From {1}, depth 3 terminates; each node settles to its min depth; the
    // seed 1, re-reached by the cycle, stays a depth-0 parentless root.
    let (cp, eng, _writer) = setup(
        fx,
        vec![1, 2, 3],
        vec!["a", "b", "c"],
        &[(1, 2), (2, 3), (3, 1)],
    )
    .await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=3&_ids=1&tree=true",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut nodes = tree_nodes(&body);
    nodes.sort_by_key(|(id, _, _)| *id);
    assert_eq!(
        nodes,
        vec![(1, 0, None), (2, 1, Some(1)), (3, 2, Some(2))],
        "cycle terminates; 1 stays a depth-0 root; 2,3 settle to min depth: {body}"
    );
    assert_eq!(tree_roots(&body), vec![1], "{body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn row_filter_prunes_and_no_node_reports_a_blocked_parent() {
    let fx = PgFixture::shared();
    // Branch graph 1->2, 1->3, 3->4; node 2 is INACTIVE. A Read row-filter active=true removes
    // 2 from the permitted subgraph. The 1->3->4 branch stays permitted, so 3 and 4 SURVIVE
    // (pruning-with-survivors, not a degenerate root-only result), while 2 is absent entirely
    // and no surviving node reports 2 as its parent.
    let (cp, eng, _writer) = setup_active(
        fx,
        vec![1, 2, 3, 4],
        vec!["a", "b", "c", "d"],
        vec![true, false, true, true],
        &[(1, 2), (1, 3), (3, 4)],
    )
    .await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

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
        "/objects/Person/graph/knows?depth=3&_ids=1&tree=true",
        "carol",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut nodes = tree_nodes(&body);
    nodes.sort_by_key(|(id, _, _)| *id);
    // 1 (root), 3 (via the permitted branch), 4 (below 3) survive; the inactive 2 is pruned.
    assert_eq!(
        nodes,
        vec![(1, 0, None), (3, 1, Some(1)), (4, 2, Some(3))],
        "permitted branch survives; inactive 2 is cut: {body}"
    );
    // The denied node 2 is absent AND never reported as a parent (path edges don't leak it).
    assert!(
        nodes
            .iter()
            .all(|(id, _, parent)| *id != 2 && *parent != Some(2)),
        "denied intermediate 2 is absent as node and as parent: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn forest_two_roots_and_shared_node_settles_to_min_depth() {
    let fx = PgFixture::shared();
    // Two seeds {1, 10} whose components OVERLAP at node 3: 1->2->3 (3 is depth 2 from seed 1)
    // and 10->3 (3 is depth 1 from seed 10). Node 3 is reachable from both seeds; it must
    // settle to the MIN depth (1) => parent 10, deterministically — the spec's forest property.
    let (cp, eng, _writer) = setup(
        fx,
        vec![1, 2, 3, 10],
        vec!["a", "b", "c", "x"],
        &[(1, 2), (2, 3), (10, 3)],
    )
    .await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=3&_ids=1,10&tree=true",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(tree_roots(&body), vec![1, 10], "two roots (forest): {body}");
    let mut nodes = tree_nodes(&body);
    nodes.sort_by_key(|(id, _, _)| *id);
    assert_eq!(
        nodes,
        vec![
            (1, 0, None),
            (2, 1, Some(1)),
            (3, 1, Some(10)),
            (10, 0, None)
        ],
        "shared node 3 settles to the shorter path (depth 1 via 10), not depth 2 via 2: {body}"
    );
}
