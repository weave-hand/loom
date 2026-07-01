use control_plane_core::{
    ActionDef, ActionKind, ActionName, ObjectType, Ontology, ParamDef, PropertyDef, TableRef,
    TypeName,
};
use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn action_kind_round_trips() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;

    // A target type must exist before an action can reference it.
    cp.define_type(ObjectType {
        name: TypeName("Widget".into()),
        table: TableRef {
            schema: "public".into(),
            name: "widget".into(),
        },
        identity: Some("sku".into()),
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

    cp.define_action(ActionDef {
        name: ActionName("delWidget".into()),
        target: TypeName("Widget".into()),
        parameters: vec![ParamDef {
            name: "sku".into(),
            ty: "String".into(),
            required: true,
            binds: None,
        }],
        kind: ActionKind::Delete,
        assignments: vec![],
    })
    .await
    .expect("define Delete action");

    let got = cp
        .get_action(&ActionName("delWidget".into()))
        .await
        .expect("get_action");
    assert_eq!(got.kind, ActionKind::Delete);
}
