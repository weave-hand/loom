//! read_graph_reach on an in-memory control plane + a stub serving engine. The stub
//! returns canned object rows (no real DuckDB) whose columns match the Person projection;
//! the test asserts the governance short-circuits (`NotCyclicPath`, `NoIdentity`), the
//! happy path that returns the stub's reachable objects (single self-link and a 2-link
//! cyclic path), and that a non-cyclic path is rejected. Real recursive reachability over
//! DuckDB is covered by the graph e2e (Task 3).

use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, Cardinality, Effect, LinkBacking, LinkDef, ObjectType, Ontology, PolicyTarget,
    PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::handler::{GraphQuery, QueryDeps, QueryError, Subject, read_graph_reach};
use query_api::serving::{Rows, ServingEngine, ServingError, SqlValue};

/// A serving stub that returns canned object rows in the order compile_graph_reach
/// projects the visible (allowed) Person columns: [id, name].
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

fn team_type() -> ObjectType {
    ObjectType {
        name: TypeName("Team".into()),
        properties: vec![PropertyDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
        }],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "team".into(),
        },
        identity: Some("id".into()),
    }
}

/// Seed a control plane: Person with a `knows` FK self-link (Person -> Person), an
/// `employer` FK link (Person -> Company), a `memberOf` link (Person -> Team) and a
/// `hasMember` link (Team -> Person) forming a Person->Team->Person cycle, plus a
/// `worksAt` link (Person -> Company); an analyst granted Read on Person and Team.
async fn seeded(person: ObjectType) -> (MemoryControlPlane, SubjectId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(person).await.unwrap();
    cp.define_type(company_type()).await.unwrap();
    cp.define_type(team_type()).await.unwrap();
    // Self-link: from == to == Person.
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
    // Non-self link: Person -> Company.
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
    // Cyclic pair: Person -> Team -> Person.
    cp.define_link(LinkDef {
        name: "memberOf".into(),
        from: TypeName("Person".into()),
        to: TypeName("Team".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: "team_id".into(),
            to_column: "id".into(),
        },
    })
    .await
    .unwrap();
    cp.define_link(LinkDef {
        name: "hasMember".into(),
        from: TypeName("Team".into()),
        to: TypeName("Person".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "team_id".into(),
        },
    })
    .await
    .unwrap();
    // Non-cyclic tail: Person -> Company (so memberOf,worksAt does not return to Person).
    cp.define_link(LinkDef {
        name: "worksAt".into(),
        from: TypeName("Team".into()),
        to: TypeName("Company".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: "company_id".into(),
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
        PolicyTarget::Type(TypeName("Team".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    // Read on Company too, so a non-cyclic path that lands there is rejected for being
    // non-cyclic (NotCyclicPath) rather than short-circuited by the Read gate (Forbidden).
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

fn graph_query(path: &[&str]) -> GraphQuery {
    GraphQuery {
        type_name: "Person".into(),
        path: path.iter().map(|s| s.to_string()).collect(),
        depth: 3,
        filters: vec![],
        ids: vec![],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_non_cyclic_single_link() {
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    // `employer` lands on Company, not back on Person -> not a cycle.
    let err = read_graph_reach(&graph_query(&["employer"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::NotCyclicPath(p) if p == "employer"),
        "expected NotCyclicPath(employer), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_non_cyclic_multi_link_path() {
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    // Person --memberOf--> Team --worksAt--> Company: lands on Company, not Person.
    let err = read_graph_reach(
        &graph_query(&["memberOf", "worksAt"]),
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&err, QueryError::NotCyclicPath(p) if p == "memberOf,worksAt"),
        "expected NotCyclicPath(memberOf,worksAt), got {err:?}"
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
    };
    let err = read_graph_reach(&graph_query(&["knows"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::NoIdentity(t) if t == "Person"),
        "expected NoIdentity(Person), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn returns_reachable_objects_for_a_self_link() {
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
    };
    let rows = read_graph_reach(&graph_query(&["knows"]), &Subject(subj), &deps)
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
async fn returns_reachable_objects_for_a_cyclic_path() {
    // Person --memberOf--> Team --hasMember--> Person is a 2-link cycle; the path resolves
    // (Team is Read-gated + its filters loaded), and the stub's reachable rows come back.
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing {
        rows: vec![
            vec![SqlValue::Int(2), SqlValue::Text("Bob".into())],
            vec![SqlValue::Int(4), SqlValue::Text("Dana".into())],
        ],
    };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    let rows = read_graph_reach(
        &graph_query(&["memberOf", "hasMember"]),
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        rows.rows,
        vec![
            vec![SqlValue::Int(2), SqlValue::Text("Bob".into())],
            vec![SqlValue::Int(4), SqlValue::Text("Dana".into())],
        ]
    );
}
