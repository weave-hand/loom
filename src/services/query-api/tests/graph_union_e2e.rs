//! Graph multi-edge union e2e: GET /objects/:type/graph?links=… over the real HTTP router
//! backed by DuckDB-over-DuckLake. Proves the union reachability shape {objects:[...]} where
//! each step follows ANY ONE of a set of self-links:
//!   - ?links=knows,colleagues unions both edge sets (reaches more than either alone),
//!   - ?links=knows alone is a strict subset (a colleagues-only node is absent),
//!   - a cycle terminates and the node set is deduped,
//!   - a Read row-filter (active=true) prunes reachability through a blocked node,
//!   - ?path=…&links=… together -> 400, empty ?links= -> 400.
//!
//! Graph: one `Person` table with a `knows` FK self-link via knows_id and a `colleagues(a, b)`
//! join-table self-link, plus a boolean `active` column. knows edges: 1->2, 2->3 (acyclic, so
//! knows alone reaches {2,3} from 1). colleagues edge: 1->4 (so the union adds the
//! colleagues-only node 4). No edge returns to 1, keeping the reachable sets free of the seed.
//! Cycle termination over the recursive CTE is covered by graph_reach_e2e (shared machinery);
//! this suite isolates the union axis. Person declares identity `id`.

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
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                Some("ann"),
                Some("bob"),
                Some("cal"),
                Some("dee"),
            ])),
            Arc::new(BooleanArray::from(vec![true, false, true, true])),
            // 1->2, 2->3, 3 and 4 have no outbound knows edge.
            Arc::new(Int64Array::from(vec![Some(2), Some(3), None, None])),
        ],
    )
    .unwrap();
    land(&cp, &store, &person, person_schema, person_batch).await;

    // colleagues(a, b): join-table self-link edge 1->4. knows never touches 4, so the union
    // adds 4 over knows alone. No back-edge to 1 -> the seed stays out of the reachable set.
    let colleagues = tref("main", "colleagues");
    let colleagues_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let colleagues_batch = RecordBatch::try_new(
        colleagues_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![4])),
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

/// Collect the sorted set of `id`s from an {"objects":[...]} body. `id` is a `Long`, rendered
/// as a numeric STRING (int64 exceeds JSON's safe-integer range), so parse.
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
