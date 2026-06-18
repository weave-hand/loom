//! Graph path-cycle e2e: GET /objects/:type/graph?path=l1,l2 over the real HTTP router
//! backed by a DuckDB serving engine reading a DuckLake-on-Postgres catalog. Proves the
//! bounded recursive reachability shape {objects:[...]} over a MULTI-LINK cyclic path
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

use arrow::array::{BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, Cardinality, CompareOp, ControlPlane, DatasetRef, Effect, EventType, LineageEvent,
    LinkBacking, LinkDef, ObjectType, Ontology, Policy, PolicyTarget, PropertyDef, RoleId,
    RowFilter, RunId, ScalarValue, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use http_body_util::BodyExt;
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, EmbeddedDuckDb, ServingError, SqlValue};
use time::OffsetDateTime;
use tower::ServiceExt;
use uuid::Uuid;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}

/// No-op write engine: the read-only graph route never touches it, but `AppState`
/// requires one.
struct StubAction;

#[async_trait]
impl ActionEngine for StubAction {
    async fn insert_row(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
    ) -> std::result::Result<(), ServingError> {
        Ok(())
    }
}

async fn land(
    cp: &PgControlPlane,
    store: &Arc<dyn ObjectStore>,
    table: &TableRef,
    schema: Arc<Schema>,
    batch: RecordBatch,
) {
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(table)],
        payload: serde_json::json!({}),
    };
    materialize(
        cp,
        store.clone(),
        MaterializeRequest {
            table,
            schema,
            batches: &[batch],
            file_prefix: "run-1",
            gate: None,
            lineage,
        },
    )
    .await
    .unwrap();
}

/// Seed the shared-membership graph: person, team, membership(person_id, team_id), company.
/// Teams T1{1,2}, T2{3,4,5}, T3{5,6}; T3 inactive. `memberOf` Person->Team and `hasMember`
/// Team->Person both back onto `membership`, forming a Person->Team->Person cycle. A
/// `worksAt` FK Person->Company drives the non-cyclic case. Person & Team declare identity
/// `id`. Caller MUST keep the `DuckLakeWriter` alive (its TempDir holds the Parquet read).
async fn setup(fx: &PgFixture) -> (PgControlPlane, EmbeddedDuckDb, DuckLakeWriter) {
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // person(id, name, company_id): 6 persons; company drives the worksAt link.
    let person = tref("main", "person");
    let person_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("company_id", DataType::Int64, true),
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
            Arc::new(Int64Array::from(vec![
                Some(7),
                Some(7),
                Some(8),
                Some(8),
                Some(8),
                Some(8),
            ])),
        ],
    )
    .unwrap();
    land(&cp, &store, &person, person_schema, person_batch).await;

    // team(id, name, active): T3 (id=3) is INACTIVE (intermediate governance test).
    let team = tref("main", "team");
    let team_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, false),
    ]));
    let team_batch = RecordBatch::try_new(
        team_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("red"),
                Some("green"),
                Some("blue"),
            ])),
            Arc::new(BooleanArray::from(vec![true, true, false])),
        ],
    )
    .unwrap();
    land(&cp, &store, &team, team_schema, team_batch).await;

    // membership(person_id, team_id): T1{1,2}, T2{3,4,5}, T3{5,6}. Person 5 bridges T2/T3.
    let membership = tref("main", "membership");
    let membership_schema = Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int64, false),
        Field::new("team_id", DataType::Int64, false),
    ]));
    let membership_batch = RecordBatch::try_new(
        membership_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 5, 6])),
            Arc::new(Int64Array::from(vec![1, 1, 2, 2, 2, 3, 3])),
        ],
    )
    .unwrap();
    land(
        &cp,
        &store,
        &membership,
        membership_schema,
        membership_batch,
    )
    .await;

    // company(id, name): targets for the non-cyclic worksAt link.
    let company = tref("main", "company");
    let company_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let company_batch = RecordBatch::try_new(
        company_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![7, 8])),
            Arc::new(StringArray::from(vec![Some("acme"), Some("globex")])),
        ],
    )
    .unwrap();
    land(&cp, &store, &company, company_schema, company_batch).await;

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

async fn subject_with_role(cp: &PgControlPlane, name: &str) -> (SubjectId, RoleId) {
    let subj = SubjectId(name.into());
    let role = RoleId(format!("{name}-role"));
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    (subj, role)
}

async fn grant_read(cp: &PgControlPlane, role: &RoleId, type_name: &str) {
    cp.grant(
        role,
        Action::Read,
        PolicyTarget::Type(TypeName(type_name.into())),
        Effect::Allow,
    )
    .await
    .unwrap();
}

/// Drive the HTTP router and return (status, parsed JSON body).
async fn get(
    cp: Arc<PgControlPlane>,
    eng: Arc<EmbeddedDuckDb>,
    uri: &str,
    subject: &str,
) -> (StatusCode, serde_json::Value) {
    let app = router(AppState {
        cp: cp as Arc<dyn ControlPlane>,
        serving: eng,
        action_engine: Arc::new(StubAction),
    });
    let res = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Loom-Subject", subject)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

/// Collect the sorted set of `id`s from an {"objects":[...]} reachability body. `id` is a
/// `Long`, rendered as a numeric STRING (int64 exceeds JSON's safe-integer range), so parse.
fn ids(body: &serde_json::Value) -> Vec<i64> {
    let mut out: Vec<i64> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().parse::<i64>().unwrap())
        .collect();
    out.sort_unstable();
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_membership_reach_depth_bounds_and_cycle() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
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
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
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
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
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
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
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
