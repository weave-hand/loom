//! read_graph_reach_union on an in-memory control plane + a stub serving engine. The stub
//! returns canned object rows matching the Person projection [id, name]; the test asserts the
//! governance short-circuits (NotCyclicPath for a non-self link, UnknownLink, empty link set,
//! NoIdentity), the happy path returning the stub's reachable objects, and that duplicate link
//! names collapse to one arm. Real recursive reachability over DuckDB is the graph union e2e.

use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, Cardinality, Effect, LinkBacking, LinkDef, ObjectType, Ontology, PolicyTarget,
    PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::handler::{GraphUnionQuery, QueryDeps, QueryError, Subject, read_graph_reach_union};
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
    ) -> std::result::Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec!["id".into(), "name".into()],
            rows: self.rows.clone(),
        })
    }
}

fn person_type(identity: Option<String>) -> ObjectType {
    ObjectType {
        name: TypeName("Person".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "name".into(),
                ty: "Text".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "person".into(),
        },
        identity,
    }
}

fn company_type() -> ObjectType {
    ObjectType {
        name: TypeName("Company".into()),
        properties: vec![PropertyDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
        }],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "company".into(),
        },
        identity: Some("id".into()),
    }
}

/// Seed: Person with a `knows` FK self-link and a `colleagues` join-table self-link (both
/// Person -> Person), plus an `employer` FK link Person -> Company (a non-self link). An
/// analyst granted Read on Person and Company.
async fn seeded(person: ObjectType) -> (MemoryControlPlane, SubjectId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(person).await.unwrap();
    cp.define_type(company_type()).await.unwrap();
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
    cp.define_link(LinkDef {
        name: "colleagues".into(),
        from: TypeName("Person".into()),
        to: TypeName("Person".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::JoinTable {
            table: TableRef {
                schema: "main".into(),
                name: "colleagues".into(),
            },
            from_key: "id".into(),
            from_column: "a".into(),
            to_column: "b".into(),
            to_key: "id".into(),
        },
    })
    .await
    .unwrap();
    cp.define_link(LinkDef {
        name: "employer".into(),
        from: TypeName("Person".into()),
        to: TypeName("Company".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: "employer_id".into(),
            to_column: "id".into(),
        },
    })
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
    cp.grant(
        &reader,
        Action::Read,
        PolicyTarget::Type(TypeName("Company".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    (cp, analyst)
}

fn union_query(links: &[&str]) -> GraphUnionQuery {
    GraphUnionQuery {
        type_name: "Person".into(),
        links: links.iter().map(|s| s.to_string()).collect(),
        depth: 3,
        filters: vec![],
        ids: vec![],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_non_self_link() {
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
        default_limit: 1000,
    };
    // `employer` lands on Company, not back on Person -> not a self-link.
    let err = read_graph_reach_union(&union_query(&["knows", "employer"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::NotCyclicPath(l) if l == "employer"),
        "expected NotCyclicPath(employer), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_an_unknown_link() {
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
        default_limit: 1000,
    };
    let err = read_graph_reach_union(&union_query(&["knows", "nope"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::UnknownLink(l) if l == "nope"),
        "expected UnknownLink(nope), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_an_empty_link_set() {
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
        default_limit: 1000,
    };
    let err = read_graph_reach_union(&union_query(&[]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::NotCyclicPath(_)),
        "expected NotCyclicPath, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_type_without_identity() {
    let (cp, subj) = seeded(person_type(None)).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
        default_limit: 1000,
    };
    let err = read_graph_reach_union(&union_query(&["knows"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::NoIdentity(t) if t == "Person"),
        "expected NoIdentity(Person), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn returns_reachable_objects_for_a_self_link_union() {
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing {
        rows: vec![
            vec![SqlValue::Int(2), SqlValue::Text("Bob".into())],
            vec![SqlValue::Int(3), SqlValue::Text("Cara".into())],
        ],
    };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
        default_limit: 1000,
    };
    let rows = read_graph_reach_union(
        &union_query(&["knows", "colleagues"]),
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        rows.logical_types,
        vec!["Long".to_string(), "Text".to_string()]
    );
    assert_eq!(
        rows.rows,
        vec![
            vec![SqlValue::Int(2), SqlValue::Text("Bob".into())],
            vec![SqlValue::Int(3), SqlValue::Text("Cara".into())],
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_link_names_collapse() {
    // A link listed twice resolves to one arm (no error); the happy path still returns rows.
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing {
        rows: vec![vec![SqlValue::Int(2), SqlValue::Text("Bob".into())]],
    };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
        default_limit: 1000,
    };
    let rows = read_graph_reach_union(&union_query(&["knows", "knows"]), &Subject(subj), &deps)
        .await
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Int(2), SqlValue::Text("Bob".into())]]
    );
}
