//! read_graph_tree on an in-memory control plane + a stub serving engine. The stub returns
//! canned rows shaped as compile_graph_tree projects: [allowed cols..., __depth, __parent,
//! __id]. Asserts governance short-circuits (NoIdentity; masked/denied identity -> Forbidden),
//! and the happy path builds a tree with a parentless root and correct parent pointers.
//! Real recursive determinism is covered by the e2e (Task 5).

use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, Cardinality, Effect, LinkBacking, LinkDef, ObjectType, Ontology, Policy,
    PolicyTarget, PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::handler::{GraphQuery, QueryDeps, QueryError, Subject, read_graph_tree};
use query_api::serving::{Rows, ServingEngine, ServingError, SqlValue};

/// Serving stub returning canned tree rows: [id, name, __depth, __parent, __id].
struct TreeServing {
    rows: Vec<Vec<SqlValue>>,
}

#[async_trait]
impl ServingEngine for TreeServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
    ) -> std::result::Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec![
                "id".into(),
                "name".into(),
                "__depth".into(),
                "__parent".into(),
                "__id".into(),
            ],
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
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "name".into(),
                ty: "Text".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
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

/// Person with a `knows` FK self-link and an analyst granted Read on Person. Returns the CP,
/// the analyst subject, and its role (so a caller can layer a mask/deny policy).
async fn seeded(person: ObjectType) -> (MemoryControlPlane, SubjectId, RoleId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(person).await.unwrap();
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
    (cp, analyst, reader)
}

fn graph_query() -> GraphQuery {
    GraphQuery {
        type_name: "Person".into(),
        path: vec!["knows".into()],
        depth: 3,
        filters: vec![],
        ids: vec![],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_type_without_identity() {
    let (cp, subj, _role) = seeded(person_type(None)).await;
    let serving = TreeServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
        default_limit: 1000,
    };
    let err = read_graph_tree(&graph_query(), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::NoIdentity(t) if t == "Person"),
        "expected NoIdentity(Person), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn masked_identity_is_forbidden() {
    // The tree PROJECTS identity as id/parent, so a masked identity cannot be served.
    let (cp, subj, role) = seeded(person_type(Some("id".into()))).await;
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Person".into())),
            row_filter: None,
            deny_columns: vec![],
            mask_columns: vec!["id".into()],
        },
    )
    .await
    .unwrap();
    let serving = TreeServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
        default_limit: 1000,
    };
    let err = read_graph_tree(&graph_query(), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(err, QueryError::Forbidden),
        "masked identity -> Forbidden, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn denied_identity_is_forbidden() {
    // The spec names masked OR denied identity -> Forbidden. Denying the id column removes it
    // from the projection; the tree still needs to project id/parent, so -> Forbidden.
    let (cp, subj, role) = seeded(person_type(Some("id".into()))).await;
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Person".into())),
            row_filter: None,
            deny_columns: vec!["id".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    let serving = TreeServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
        default_limit: 1000,
    };
    let err = read_graph_tree(&graph_query(), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(err, QueryError::Forbidden),
        "denied identity -> Forbidden, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn builds_tree_with_root_and_parent_pointers() {
    let (cp, subj, _role) = seeded(person_type(Some("id".into()))).await;
    // A linear tree 5 -> 7 -> 9: 5 is the root (parent NULL), 7's parent is 5, 9's parent is 7.
    let serving = TreeServing {
        rows: vec![
            vec![
                SqlValue::Int(5),
                SqlValue::Text("ann".into()),
                SqlValue::Int(0),
                SqlValue::Null,
                SqlValue::Int(5),
            ],
            vec![
                SqlValue::Int(7),
                SqlValue::Text("bob".into()),
                SqlValue::Int(1),
                SqlValue::Int(5),
                SqlValue::Int(7),
            ],
            vec![
                SqlValue::Int(9),
                SqlValue::Text("cal".into()),
                SqlValue::Int(2),
                SqlValue::Int(7),
                SqlValue::Int(9),
            ],
        ],
    };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
        default_limit: 1000,
    };
    let tree = read_graph_tree(&graph_query(), &Subject(subj), &deps)
        .await
        .unwrap();
    assert_eq!(tree.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(tree.identity_type, "Long");
    assert_eq!(tree.nodes.len(), 3);
    // root
    assert_eq!(tree.nodes[0].id, SqlValue::Int(5));
    assert_eq!(tree.nodes[0].depth, 0);
    assert_eq!(tree.nodes[0].parent, SqlValue::Null);
    assert_eq!(
        tree.nodes[0].cells,
        vec![SqlValue::Int(5), SqlValue::Text("ann".into())]
    );
    // child
    assert_eq!(tree.nodes[1].parent, SqlValue::Int(5));
    assert_eq!(tree.nodes[2].parent, SqlValue::Int(7));

    // Render to JSON: roots list + node objects. NOTE: identity is `Long`, which renders as a
    // numeric STRING on the wire (int64 > JSON safe-int range -> JsonRepr::NumericString), so
    // id/parent/root values are the strings "5"/"7", not the numbers 5/7. `depth` stays a
    // number (a synthetic BFS integer). `name` is unknown-type -> natural string rendering.
    let body = query_api::render::tree_to_json(&tree);
    assert_eq!(body["roots"], serde_json::json!(["5"]));
    assert_eq!(body["nodes"][0]["id"], serde_json::json!("5"));
    assert_eq!(body["nodes"][0]["parent"], serde_json::Value::Null);
    assert_eq!(body["nodes"][0]["depth"], serde_json::json!(0));
    assert_eq!(body["nodes"][0]["object"]["name"], serde_json::json!("ann"));
    assert_eq!(body["nodes"][1]["parent"], serde_json::json!("5"));
}
