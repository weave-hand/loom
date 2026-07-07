//! resolve_governed is the single governance prologue: coarse Read gate (deny-by-default,
//! BEFORE existence is revealed), type resolution with the OnMissing 404/403/internal knob,
//! and the folded row-filter/denied/masked policy.

use std::time::Duration;

use control_plane_core::{
    Acl, Action, ActionDef, ActionName, CompareOp, ControlPlaneError, Effect, LinkDef, ObjectType,
    Ontology, Page, PageReq, Policy, PolicyTarget, PropertyDef, RoleId, RowFilter, ScalarValue,
    SubjectId, TableRef, TypeName, VectorIndexDef,
};
use control_plane_memory::MemoryControlPlane;
use query_api::governed::{OnMissing, resolve_governed};
use query_api::handler::QueryError;

/// An `Ontology` stub whose `get_type` unconditionally reports `NotFound`, standing in
/// for a corrupt-ontology-state read (e.g. an ACL grant that outlived its type). Every
/// other method is unreachable in these tests — `resolve_governed` only ever calls
/// `get_type`.
struct MissingType;

#[async_trait::async_trait]
impl Ontology for MissingType {
    async fn define_type(&self, _ty: ObjectType) -> control_plane_core::Result<()> {
        unreachable!("not exercised by resolve_governed")
    }
    async fn define_link(&self, _link: LinkDef) -> control_plane_core::Result<()> {
        unreachable!("not exercised by resolve_governed")
    }
    async fn delete_link(&self, _from: &TypeName, _name: &str) -> control_plane_core::Result<()> {
        unreachable!("not exercised by resolve_governed")
    }
    async fn derived_properties_referencing(
        &self,
        _from: &TypeName,
        _name: &str,
    ) -> control_plane_core::Result<Vec<String>> {
        unreachable!("not exercised by resolve_governed")
    }
    async fn get_type(&self, name: &TypeName) -> control_plane_core::Result<ObjectType> {
        Err(ControlPlaneError::NotFound(format!("type {}", name.0)))
    }
    async fn list_types(&self, _page: PageReq) -> control_plane_core::Result<Page<ObjectType>> {
        unreachable!("not exercised by resolve_governed")
    }
    async fn links(
        &self,
        _name: &TypeName,
        _page: PageReq,
    ) -> control_plane_core::Result<Page<LinkDef>> {
        unreachable!("not exercised by resolve_governed")
    }
    async fn links_to(
        &self,
        _name: &TypeName,
        _page: PageReq,
    ) -> control_plane_core::Result<Page<LinkDef>> {
        unreachable!("not exercised by resolve_governed")
    }
    async fn resolve(&self, _name: &TypeName) -> control_plane_core::Result<TableRef> {
        unreachable!("not exercised by resolve_governed")
    }
    async fn define_action(&self, _action: ActionDef) -> control_plane_core::Result<()> {
        unreachable!("not exercised by resolve_governed")
    }
    async fn delete_action(&self, _name: &ActionName) -> control_plane_core::Result<()> {
        unreachable!("not exercised by resolve_governed")
    }
    async fn get_action(&self, _name: &ActionName) -> control_plane_core::Result<ActionDef> {
        unreachable!("not exercised by resolve_governed")
    }
    async fn list_actions(&self, _page: PageReq) -> control_plane_core::Result<Page<ActionDef>> {
        unreachable!("not exercised by resolve_governed")
    }
    async fn define_vector_index(&self, _def: VectorIndexDef) -> control_plane_core::Result<()> {
        unreachable!("not exercised by resolve_governed")
    }
    async fn get_vector_index(
        &self,
        _type_name: &TypeName,
        _name: &str,
    ) -> control_plane_core::Result<Option<VectorIndexDef>> {
        unreachable!("not exercised by resolve_governed")
    }
    async fn vector_indexes_for(
        &self,
        _type_name: &TypeName,
    ) -> control_plane_core::Result<Vec<VectorIndexDef>> {
        unreachable!("not exercised by resolve_governed")
    }
}

fn order_type() -> ObjectType {
    ObjectType {
        name: TypeName("Order".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "status".into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "secret".into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "orders".into(),
        },
        identity: Some("id".into()),
    }
}

async fn seeded() -> (MemoryControlPlane, SubjectId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(order_type()).await.unwrap();
    let analyst = SubjectId("analyst".into());
    let reader = RoleId("reader".into());
    cp.define_subject(&analyst).await.unwrap();
    cp.define_role(&reader).await.unwrap();
    cp.assign_role(&analyst, &reader).await.unwrap();
    cp.grant(
        &reader,
        Action::Read,
        PolicyTarget::Type(TypeName("Order".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    (cp, analyst)
}

#[tokio::test]
async fn ungranted_subject_is_forbidden_before_existence_is_revealed() {
    let (cp, _) = seeded().await;
    let nobody = SubjectId("nobody".into());
    cp.define_subject(&nobody).await.unwrap();
    // A type that does NOT exist: an ungranted subject still gets Forbidden, not
    // UnknownType — the deny-before-existence-leak invariant.
    let err = resolve_governed(
        &cp,
        &cp,
        &nobody,
        &TypeName("Ghost".into()),
        OnMissing::NotFound,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::Forbidden));
}

#[tokio::test]
async fn granted_but_missing_type_maps_per_on_missing() {
    let (cp, analyst) = seeded().await;
    let reader = RoleId("reader".into());
    // MemoryControlPlane's `grant` validates that a `Type` target exists at grant time
    // (it has no type-deletion API to un-define it afterward), so define "Ghost" here
    // purely to satisfy that check. The `resolve_governed` calls below are given the
    // `MissingType` ontology stub instead of `&cp`, so at read time the type genuinely
    // resolves to `NotFound` — the granted-but-missing state `OnMissing` maps.
    cp.define_type(ObjectType {
        name: TypeName("Ghost".into()),
        properties: vec![],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "ghost".into(),
        },
        identity: None,
    })
    .await
    .unwrap();
    cp.grant(
        &reader,
        Action::Read,
        PolicyTarget::Type(TypeName("Ghost".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

    let missing = MissingType;

    let err = resolve_governed(
        &missing,
        &cp,
        &analyst,
        &TypeName("Ghost".into()),
        OnMissing::NotFound,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::UnknownType(t) if t == "Ghost"));

    let err = resolve_governed(
        &missing,
        &cp,
        &analyst,
        &TypeName("Ghost".into()),
        OnMissing::Forbidden,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::Forbidden));

    let err = resolve_governed(
        &missing,
        &cp,
        &analyst,
        &TypeName("Ghost".into()),
        OnMissing::Internal,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        err,
        QueryError::ControlPlane(ControlPlaneError::NotFound(_))
    ));
}

#[tokio::test]
async fn folds_row_filters_denied_and_masked_from_policy() {
    let (cp, analyst) = seeded().await;
    let reader = RoleId("reader".into());
    cp.set_policy(
        &reader,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: Some(RowFilter::Compare {
                property: "status".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("open".into()),
            }),
            deny_columns: vec!["secret".into()],
            mask_columns: vec!["status".into()],
        },
    )
    .await
    .unwrap();

    let g = resolve_governed(
        &cp,
        &cp,
        &analyst,
        &TypeName("Order".into()),
        OnMissing::NotFound,
    )
    .await
    .unwrap();
    assert_eq!(g.otype.name.0, "Order");
    assert_eq!(g.row_filters.len(), 1);
    assert!(g.denied.contains("secret"));
    assert!(g.masked.contains("status"));
    // allowed() = properties minus denied, in property order.
    assert_eq!(g.allowed(), vec!["id".to_string(), "status".to_string()]);
    // identity "id" is neither denied nor masked.
    assert!(!g.identity_governed());
}

#[tokio::test]
async fn identity_governed_when_identity_is_masked() {
    let (cp, analyst) = seeded().await;
    let reader = RoleId("reader".into());
    cp.set_policy(
        &reader,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: None,
            deny_columns: vec![],
            mask_columns: vec!["id".into()],
        },
    )
    .await
    .unwrap();
    let g = resolve_governed(
        &cp,
        &cp,
        &analyst,
        &TypeName("Order".into()),
        OnMissing::NotFound,
    )
    .await
    .unwrap();
    assert!(g.identity_governed());
}
