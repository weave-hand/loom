//! Graph multi-edge union e2e: GET /objects/:type/graph?links=… over the real HTTP router
//! backed by DuckDB-over-DuckLake. Proves the union reachability shape {objects:[...]} where
//! each step follows ANY ONE of a set of self-links:
//!   - ?links=knows,colleagues unions both edge sets (reaches more than either alone),
//!   - ?links=knows alone is a strict subset (a colleagues-only node is absent),
//!   - a cycle formed ACROSS the union (5--knows-->6, 6--colleagues-->5) terminates + dedups,
//!   - a Read row-filter (active=true) prunes reachability through a blocked node,
//!   - ?path=…&links=… together -> 400, empty ?links= -> 400.
//!
//! Graph: one `Person` table with a `knows` FK self-link via knows_id and a `colleagues(a, b)`
//! join-table self-link, plus a boolean `active` column. Component A (nodes 1..4): knows edges
//! 1->2, 2->3 (acyclic, so knows alone reaches {2,3} from 1); colleagues edge 1->4 (so the union
//! adds the colleagues-only node 4); no edge returns to 1, keeping the {1}-seeded sets free of the
//! seed. Component B (nodes 5,6): 5--knows-->6 and 6--colleagues-->5 form a 2-cycle that exists
//! ONLY across the union of both backings — exercising termination + dedup over the union
//! edge-relation CTE (a distinct shape from the path compiler) on real DuckDB. Person declares
//! identity `id`.

use std::sync::Arc;

use arrow::array::{BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use axum::http::StatusCode;
use control_plane_core::{
    Acl, Action, Cardinality, CompareOp, LinkBacking, LinkDef, ObjectType, Ontology, Policy,
    PolicyTarget, RowFilter, ScalarValue, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use e2e_support::{get, grant_read, ids_i64 as ids, land, prop, subject_with_role, tref};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::serving::EmbeddedDuckDb;

/// Seed a `Person` table with a `knows_id` FK self-link (edges 1->2, 2->3) and a
/// `colleagues(a, b)` join-table self-link (edge 1->4), plus a boolean `active` column
/// (node 2 inactive). Person declares identity `id`. The caller MUST keep the returned
/// `DuckLakeWriter` alive (its TempDir holds the Parquet read).
async fn setup(fx: &PgFixture) -> (PgControlPlane, EmbeddedDuckDb, DuckLakeWriter) {
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // person(id, name, active, knows_id): the FK self-link edges 1->2, 2->3 live in knows_id.
    // node 2 (bob) is INACTIVE (governance test). node 4 has no knows_id (colleagues-only).
    // Nodes 5,6 are a separate component used only by the union-cycle test: 5 --knows--> 6 and
    // 6 --colleagues--> 5 form a 2-cycle that exists ONLY across the union of both backings
    // (neither link alone closes it). They are disconnected from 1..4, so the {1}-seeded
    // assertions are unaffected.
    let person = tref("main", "person");
    let person_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, false),
        Field::new("knows_id", DataType::Int64, true),
    ]));
    let person_batch = RecordBatch::try_new(
        person_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(StringArray::from(vec![
                Some("ann"),
                Some("bob"),
                Some("cal"),
                Some("dee"),
                Some("eve"),
                Some("fin"),
            ])),
            Arc::new(BooleanArray::from(vec![
                true, false, true, true, true, true,
            ])),
            // 1->2, 2->3 (3,4 have no outbound knows edge); 5->6 (6 has none).
            Arc::new(Int64Array::from(vec![
                Some(2),
                Some(3),
                None,
                None,
                Some(6),
                None,
            ])),
        ],
    )
    .unwrap();
    land(&cp, &store, &person, person_schema, person_batch).await;

    // colleagues(a, b): join-table self-link edges 1->4 and 6->5. knows never touches 4, so the
    // union adds 4 over knows alone (no back-edge to 1 -> the {1} seed stays out of its own
    // result). 6->5 closes the 5<->6 union-cycle (5->6 is the knows edge).
    let colleagues = tref("main", "colleagues");
    let colleagues_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let colleagues_batch = RecordBatch::try_new(
        colleagues_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 6])),
            Arc::new(Int64Array::from(vec![4, 5])),
        ],
    )
    .unwrap();
    land(
        &cp,
        &store,
        &colleagues,
        colleagues_schema,
        colleagues_batch,
    )
    .await;

    cp.define_type(ObjectType {
        name: TypeName("Person".into()),
        properties: vec![
            prop("id", "Long", true),
            prop("name", "String", false),
            prop("active", "Boolean", true),
            prop("knows_id", "Long", false),
        ],
        derived: vec![],
        table: person.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();

    // `knows`: FK self-link Person -> Person via knows_id.
    cp.define_link(LinkDef {
        name: "knows".into(),
        from: TypeName("Person".into()),
        to: TypeName("Person".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "knows_id".into(),
            to_column: "id".into(),
        },
    })
    .await
    .unwrap();
    // `colleagues`: join-table self-link Person -> Person.
    cp.define_link(LinkDef {
        name: "colleagues".into(),
        from: TypeName("Person".into()),
        to: TypeName("Person".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::JoinTable {
            table: colleagues.clone(),
            from_key: "id".into(),
            from_column: "a".into(),
            to_column: "b".into(),
            to_key: "id".into(),
        },
    })
    .await
    .unwrap();

    let eng = EmbeddedDuckDb::attach(
        &format!(
            "dbname={} host={} user=postgres",
            db,
            fx.socket_path().display()
        ),
        writer.data_path(),
    )
    .await
    .unwrap();
    (cp, eng, writer)
}

#[tokio::test(flavor = "multi_thread")]
async fn union_reaches_more_than_either_link_alone() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // knows alone from {1}, depth 3: 1->2->3 => {2, 3} (4 is colleagues-only, absent).
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?links=knows&depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ids(&body), vec![2, 3], "knows alone: {body}");

    // knows UNION colleagues from {1}, depth 3: adds the colleagues edge 1->4 => {2, 3, 4}.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?links=knows,colleagues&depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![2, 3, 4],
        "union adds the colleagues-only node 4: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn union_cycle_terminates_and_dedups() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // Nodes 5,6 form a 2-cycle that exists ONLY across the union: 5 --knows--> 6 (FK) and
    // 6 --colleagues--> 5 (join table). From {5} with both links at depth 5: 5->6 (d1),
    // 6->5 (d2), 5->6 (d3)... The recursive depth bound terminates the traversal and the
    // CTE-level UNION dedups, so each node appears once => {5, 6}. Neither link alone closes
    // this cycle (knows only goes 5->6; colleagues only goes 6->5).
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?links=knows,colleagues&depth=5&_ids=5",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![5, 6],
        "union-cycle terminates and dedups to {{5,6}}: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn row_filter_prunes_union_reachability_through_blocked_node() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    // A Read row-filter active=true on Person makes node 2 (inactive) unreachable. From {1},
    // knows goes 1->2 (cut) so 3 (only reachable via 2) is also cut; colleagues still reaches 4.
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
        "/objects/Person/graph?links=knows,colleagues&depth=3&_ids=1",
        "carol",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![4],
        "node 2 (inactive) cut prunes knows-reachability; colleagues still reaches 4: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn path_and_links_together_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=knows&links=colleagues&depth=2&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "path and links together -> 400"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_links_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // links= present but empty (all entries dropped) and no path => 400.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?links=&depth=2&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "empty links -> 400");
}
