use control_plane_core::{
    ActionDef, ActionKind, ActionName, ObjectType, Ontology, ParamDef, TypeName,
};
use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn action_kind_round_trips() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;

    // A target type must exist before an action can reference it.
    cp.define_type(
        ObjectType::build("Widget", ("public", "widget"))
            .prop_req("sku", "String")
            .identity("sku")
            .done(),
    )
    .await
    .expect("define Widget type");

    cp.define_action(ActionDef::single_step(
        ActionName("delWidget".into()),
        TypeName("Widget".into()),
        ActionKind::Delete,
        vec![ParamDef::new("sku", "String").required()],
        vec![],
    ))
    .await
    .expect("define Delete action");

    let got = cp
        .get_action(&ActionName("delWidget".into()))
        .await
        .expect("get_action");
    assert_eq!(got.steps[0].kind, ActionKind::Delete);
}
