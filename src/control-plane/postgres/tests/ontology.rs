use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_ontology_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::ontology_contract(&cp).await;
}

/// RED pre-migration: a corrupt persisted cardinality token must surface as a
/// loud Validation error from `links`, not silently coerce to One (the old
/// `cardinality_from_str` fallback). Whitelisted change 1 of
/// road-cp-adapter-hygiene.
#[tokio::test]
async fn corrupt_cardinality_token_fails_loud() {
    use control_plane_core::{
        Cardinality, ControlPlane, ControlPlaneError, LinkDef, ObjectType, PageReq, TypeName,
    };
    let fixture = control_plane_postgres::fixture::PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    for (ty, table) in [("A", "a"), ("B", "b")] {
        cp.ontology()
            .define_type(
                ObjectType::build(ty, ("wh", table))
                    .prop_req("id", "Long")
                    .identity("id")
                    .done(),
            )
            .await
            .expect("define_type");
    }
    cp.ontology()
        .define_link(LinkDef::fk(
            "a_to_b",
            "A",
            "B",
            Cardinality::One,
            "id",
            "id",
        ))
        .await
        .expect("define_link");
    // Corrupt the persisted token behind the adapter's back.
    sqlx::query("update ontology.link set cardinality = 'weird' where name = 'a_to_b'")
        .execute(cp.pool())
        .await
        .expect("corrupt row");
    let err = cp
        .ontology()
        .links(&TypeName("A".into()), PageReq::unbounded())
        .await
        .expect_err("corrupt cardinality must not silently coerce");
    assert!(
        matches!(err, ControlPlaneError::Validation(_)),
        "got {err:?}"
    );
}
