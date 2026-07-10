use control_plane_core::{
    ActionDef, ActionKind, ActionName, JobTemplate, ObjectType, Ontology, ParamDef, PropertyDef,
    TableRef, TypeName,
};
use control_plane_postgres::fixture::PgFixture;
use serde_json::json;

#[tokio::test]
async fn action_downstream_round_trips() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;

    // A target type must exist before an action can reference it.
    cp.define_type(ObjectType {
        name: TypeName("Widget".into()),
        table: TableRef {
            schema: "public".into(),
            name: "widget".into(),
        },
        identity: Some("sku".into()),
        version: None,
        properties: vec![PropertyDef {
            name: "sku".into(),
            ty: "String".into(),
            required: true,
            constraints: control_plane_core::PropertyConstraints::default(),
        }],
        derived: vec![],
    })
    .await
    .expect("define Widget type");

    let action = ActionDef::single_step(
        ActionName("createWidget".into()),
        TypeName("Widget".into()),
        ActionKind::Insert,
        vec![ParamDef {
            name: "sku".into(),
            ty: "String".into(),
            required: true,
            binds: None,
        }],
        vec![],
    )
    .downstream(vec![
        JobTemplate {
            kind: "transform".into(),
            payload: json!({ "sku": "@self.sku" }),
        },
        JobTemplate {
            kind: "flush_table".into(),
            payload: json!({}),
        },
    ]);
    cp.define_action(action.clone())
        .await
        .expect("define action with downstream");
    let got = cp
        .get_action(&ActionName("createWidget".into()))
        .await
        .expect("get_action");
    assert_eq!(got.downstream.len(), 2);
    assert_eq!(got.downstream[0].kind, "transform");
    assert_eq!(got.downstream[0].payload, json!({ "sku": "@self.sku" }));
    assert_eq!(got.downstream[1].kind, "flush_table");

    // Redefined with no downstream ⇒ cleared (mirror the clear-then-insert idempotency).
    cp.define_action(ActionDef::single_step(
        ActionName("createWidget".into()),
        TypeName("Widget".into()),
        ActionKind::Insert,
        vec![],
        vec![],
    ))
    .await
    .expect("redefine action without downstream");
    let got2 = cp
        .get_action(&ActionName("createWidget".into()))
        .await
        .expect("get_action after redefine");
    assert!(got2.downstream.is_empty(), "redefine clears downstream");
}
