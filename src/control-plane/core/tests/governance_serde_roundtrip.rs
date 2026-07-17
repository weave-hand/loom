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
    let ty = ObjectType::build("customer", ("main", "customer"))
        .prop_req("id", "int")
        .derived(DerivedPropertyDef::new(
            "order_count",
            "int",
            "orders",
            Aggregation::Count,
        ))
        .identity("id")
        .done();
    roundtrip(&ty);
    roundtrip(&Page {
        items: vec![LinkDef::fk(
            "orders",
            "customer",
            "order",
            Cardinality::Many,
            "id",
            "customer_id",
        )],
        next: None,
    });
    roundtrip(&ActionDef::single_step(
        ActionName("create_customer".into()),
        TypeName("customer".into()),
        ActionKind::Insert,
        vec![],
        vec![],
    ));
    // ParamDef — the action-parameter wire payload; must survive serde independently.
    roundtrip(&ActionDef::single_step(
        ActionName("createOrder".into()),
        TypeName("order".into()),
        ActionKind::Insert,
        vec![
            ParamDef::new("amount", "Long").required(),
            ParamDef::new("note", "String"),
        ],
        vec![],
    ));
    roundtrip(&VectorIndexDef::new(
        "emb_idx",
        "customer",
        "embedding",
        Metric::Cosine,
        IndexSpec::Flat,
    ));
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
    roundtrip(&LinkBacking::join_table(
        ("main", "customer_group"),
        "id",
        "customer_id",
        "group_id",
        "id",
    ));
    // Non-Count Aggregation variants.
    roundtrip(&Aggregation::Sum("amount".into()));
    roundtrip(&Aggregation::Avg("score".into()));
    roundtrip(&Aggregation::Min("created_at".into()));
    roundtrip(&Aggregation::Max("updated_at".into()));
    roundtrip(&PageReq::default());
}

#[test]
fn description_absent_from_json_decodes_to_none() {
    // The engine-wire compat guarantee: ontology structs cross that wire as serde-JSON
    // strings, so a payload written before this field existed must still decode.
    let json = r#"{"name":"email","ty":"EmailAddress","required":true}"#;
    let p: PropertyDef = serde_json::from_str(json).unwrap();
    assert_eq!(p.description, None);
}

#[test]
fn none_description_is_omitted_from_json() {
    // ... and re-encodes byte-identically to today's payload.
    let json =
        serde_json::to_string(&PropertyDef::new("email", "EmailAddress").required()).unwrap();
    assert!(
        !json.contains("description"),
        "None must not serialize a key, got: {json}"
    );
}

#[test]
fn some_description_round_trips() {
    let p = PropertyDef::new("email", "EmailAddress").described("Primary email");
    let back: PropertyDef = serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
    assert_eq!(back, p);
}
