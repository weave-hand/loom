use control_plane_core::{ObjectType, Ontology, PropertyDef, TableRef, TypeName};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::ontology::identity_for_table;

#[tokio::test]
async fn resolves_identity_from_ontology() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    cp.define_type(ObjectType {
        name: TypeName("Widget".into()),
        table: TableRef {
            schema: "s".into(),
            name: "t".into(),
        },
        identity: Some("id".into()),
        properties: vec![PropertyDef {
            name: "id".into(),
            ty: "String".into(),
            required: true,
            constraints: control_plane_core::PropertyConstraints::default(),
        }],
        derived: vec![],
    })
    .await
    .expect("define Widget type");

    let got = identity_for_table(
        &pool,
        &TableRef {
            schema: "s".into(),
            name: "t".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(got.as_deref(), Some("id"));

    let none = identity_for_table(
        &pool,
        &TableRef {
            schema: "s".into(),
            name: "nope".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(none, None);
}
