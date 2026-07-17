//! read_graph_reach_with_tail on an in-memory control plane + a stub serving engine. The stub
//! returns canned object rows matching the Company projection [id, name]; the test asserts the
//! governance short-circuits (BadGraphPath for a non-self core link, UnknownLink for an unknown
//! core/tail link, BadGraphPath for an empty tail, NoIdentity, Forbidden when a tail type is not
//! Read-granted) and the happy path returning the stub's projected rows. Real recursion + the
//! windowed identity-dedup tail is the graph tail e2e.

use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, Cardinality, Effect, LinkDef, ObjectType, Ontology, PolicyTarget, RoleId,
    SubjectId, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::handler::{
    GraphTailQuery, QueryDeps, QueryError, Subject, read_graph_reach_with_tail,
};
use query_api::serving::{Rows, ServingEngine, ServingError, SqlValue};

/// A serving stub returning canned object rows in the projected order [id, name].
struct GraphServing {
    rows: Vec<Vec<SqlValue>>,
}

#[async_trait]
impl ServingEngine for GraphServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
        _at: Option<control_plane_core::SnapshotId>,
    ) -> Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec!["id".into(), "name".into()],
            rows: self.rows.clone(),
        })
    }
}

fn person_type(identity: Option<String>) -> ObjectType {
    let b = ObjectType::build("Person", ("main", "person"))
        .prop_req("id", "Long")
        .prop("name", "Text");
    match identity {
        Some(id) => b.identity(id).done(),
        None => b.done(),
    }
}

fn company_type() -> ObjectType {
    ObjectType::build("Company", ("main", "company"))
        .prop_req("id", "Long")
        .prop("name", "Text")
        .identity("id")
        .done()
}

/// Seed: Person with a `knows` FK self-link, plus a `worksAt` FK link Person -> Company (the tail
/// target). `grant_company` toggles whether the subject is granted Read on Company.
async fn seeded(person: ObjectType, grant_company: bool) -> (MemoryControlPlane, SubjectId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(person).await.unwrap();
    cp.define_type(company_type()).await.unwrap();
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

    let analyst = SubjectId("analyst".into());
    let reader = RoleId("reader".into());
    cp.define_subject(&analyst).await.unwrap();
    cp.define_role(&reader).await.unwrap();
    cp.assign_role(&analyst, &reader).await.unwrap();
    cp.grant(
        &reader,
        Action::Read,
        PolicyTarget::Type(TypeName("Person".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    if grant_company {
        cp.grant(
            &reader,
            Action::Read,
            PolicyTarget::Type(TypeName("Company".into())),
            Effect::Allow,
        )
        .await
        .unwrap();
    }
    (cp, analyst)
}

fn tail_query(core: &str, tail: &[&str]) -> GraphTailQuery {
    GraphTailQuery {
        type_name: "Person".into(),
        core_link: core.into(),
        tail_links: tail.iter().map(|s| s.to_string()).collect(),
        depth: 3,
        filters: vec![],
        ids: vec![],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_non_self_core_link() {
    let (cp, subj) = seeded(person_type(Some("id".into())), true).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: &cp,
        serving: &serving,
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
    };
    // `worksAt` lands on Company, not back on Person -> not a self-link core.
    let err = read_graph_reach_with_tail(&tail_query("worksAt", &["knows"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::BadGraphPath(_)),
        "expected BadGraphPath for a non-self core, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_an_unknown_core_link() {
    let (cp, subj) = seeded(person_type(Some("id".into())), true).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: &cp,
        serving: &serving,
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
    };
    let err = read_graph_reach_with_tail(&tail_query("nope", &["worksAt"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::UnknownLink(l) if l == "nope"),
        "expected UnknownLink(nope), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_an_unknown_tail_link() {
    let (cp, subj) = seeded(person_type(Some("id".into())), true).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: &cp,
        serving: &serving,
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
    };
    let err = read_graph_reach_with_tail(&tail_query("knows", &["nope"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::UnknownLink(l) if l == "nope"),
        "expected UnknownLink(nope), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_an_empty_tail() {
    let (cp, subj) = seeded(person_type(Some("id".into())), true).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: &cp,
        serving: &serving,
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
    };
    let err = read_graph_reach_with_tail(&tail_query("knows", &[]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::BadGraphPath(_)),
        "expected BadGraphPath for an empty tail, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_type_without_identity() {
    let (cp, subj) = seeded(person_type(None), true).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: &cp,
        serving: &serving,
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
    };
    let err = read_graph_reach_with_tail(&tail_query("knows", &["worksAt"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::NoIdentity(t) if t == "Person"),
        "expected NoIdentity(Person), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn forbids_when_tail_type_not_granted() {
    // Read on Person but NOT Company -> the tail's Read gate denies.
    let (cp, subj) = seeded(person_type(Some("id".into())), false).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: &cp,
        serving: &serving,
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
    };
    let err = read_graph_reach_with_tail(&tail_query("knows", &["worksAt"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::Forbidden),
        "expected Forbidden (Company not Read-granted), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn returns_projected_tail_objects() {
    let (cp, subj) = seeded(person_type(Some("id".into())), true).await;
    let serving = GraphServing {
        rows: vec![
            vec![SqlValue::Int(11), SqlValue::Text("Acme".into())],
            vec![SqlValue::Int(12), SqlValue::Text("Beta".into())],
        ],
    };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: &cp,
        serving: &serving,
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
    };
    let rows =
        read_graph_reach_with_tail(&tail_query("knows", &["worksAt"]), &Subject(subj), &deps)
            .await
            .unwrap();
    // Projection is the FINAL tail type (Company): columns [id, name], logical [Long, Text].
    assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        rows.logical_types,
        vec!["Long".to_string(), "Text".to_string()]
    );
    assert_eq!(
        rows.rows,
        vec![
            vec![SqlValue::Int(11), SqlValue::Text("Acme".into())],
            vec![SqlValue::Int(12), SqlValue::Text("Beta".into())],
        ]
    );
}
