//! Every governance-read domain payload must round-trip through serde_json so it
//! can cross the engine-wire RPC boundary losslessly. This pins the serde derives
//! the wire-backed ControlPlane depends on.

use control_plane_core::{
    Action, ActionDef, ActionKind, ActionName, Aggregation, Cardinality, CompareOp, Decision,
    DerivedPropertyDef, IndexSpec, LinkBacking, LinkDef, Metric, ObjectType, Page, PageReq,
    ParamDef, Policy, PolicyTarget, PropertyDef, RowFilter, ScalarValue, SubjectId, TableRef,
    TypeName, VectorIndexDef,
};

fn roundtrip<T>(v: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let json = serde_json::to_string(v).expect("serialize");
    let back: T = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(v, &back, "round-trip mismatch for {json}");
}

#[test]
fn acl_payloads_roundtrip() {
    roundtrip(&SubjectId("alice".into()));
    roundtrip(&Action::Read);
    roundtrip(&Action::Write);
    roundtrip(&Decision::Allow);
    roundtrip(&Decision::Deny);
    roundtrip(&PolicyTarget::Type(TypeName("customer".into())));
    roundtrip(&PolicyTarget::Table(TableRef {
        schema: "main".into(),
        name: "orders".into(),
    }));
    let policy = Policy {
        target: PolicyTarget::Type(TypeName("customer".into())),
        row_filter: Some(RowFilter::Compare {
            property: "region".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Text("emea".into()),
        }),
        deny_columns: vec!["ssn".into()],
        mask_columns: vec!["email".into()],
    };
    roundtrip(&policy);
    roundtrip(&Page {
        items: vec![policy],
        next: None,
    });
}

#[test]
fn ontology_payloads_roundtrip() {
    let ty = ObjectType {
        name: TypeName("customer".into()),
        properties: vec![PropertyDef {
            name: "id".into(),
            ty: "int".into(),
            required: true,
            constraints: control_plane_core::PropertyConstraints::default(),
        }],
        derived: vec![DerivedPropertyDef {
            name: "order_count".into(),
            ty: "int".into(),
            link: "orders".into(),
            agg: Aggregation::Count,
        }],
        table: TableRef {
            schema: "main".into(),
            name: "customer".into(),
        },
        identity: Some("id".into()),
    };
    roundtrip(&ty);
    roundtrip(&Page {
        items: vec![LinkDef {
            name: "orders".into(),
            from: TypeName("customer".into()),
            to: TypeName("order".into()),
            cardinality: Cardinality::Many,
            backing: LinkBacking::ForeignKey {
                from_column: "id".into(),
                to_column: "customer_id".into(),
            },
        }],
        next: None,
    });
    roundtrip(&ActionDef {
        name: ActionName("create_customer".into()),
        target: TypeName("customer".into()),
        parameters: vec![],
        kind: ActionKind::Insert,
        assignments: vec![],
    });
    // ParamDef — the action-parameter wire payload; must survive serde independently.
    roundtrip(&ActionDef {
        name: ActionName("createOrder".into()),
        target: TypeName("order".into()),
        parameters: vec![
            ParamDef {
                name: "amount".into(),
                ty: "Long".into(),
                required: true,
                binds: None,
            },
            ParamDef {
                name: "note".into(),
                ty: "String".into(),
                required: false,
                binds: None,
            },
        ],
        kind: ActionKind::Insert,
        assignments: vec![],
    });
    roundtrip(&VectorIndexDef {
        name: "emb_idx".into(),
        type_name: TypeName("customer".into()),
        property: "embedding".into(),
        metric: Metric::Cosine,
        spec: IndexSpec::Flat,
    });
    // Non-default IndexSpec variants.
    roundtrip(&IndexSpec::IvfFlat { nlist: Some(128) });
    roundtrip(&IndexSpec::IvfFlat { nlist: None });
    roundtrip(&IndexSpec::Hnsw {
        m: Some(16),
        ef_construction: Some(200),
    });
    roundtrip(&IndexSpec::Hnsw {
        m: None,
        ef_construction: None,
    });
    // JoinTable LinkBacking variant.
    roundtrip(&LinkBacking::JoinTable {
        table: TableRef {
            schema: "main".into(),
            name: "customer_group".into(),
        },
        from_key: "id".into(),
        from_column: "customer_id".into(),
        to_column: "group_id".into(),
        to_key: "id".into(),
    });
    // Non-Count Aggregation variants.
    roundtrip(&Aggregation::Sum("amount".into()));
    roundtrip(&Aggregation::Avg("score".into()));
    roundtrip(&Aggregation::Min("created_at".into()));
    roundtrip(&Aggregation::Max("updated_at".into()));
    roundtrip(&PageReq::default());
}
