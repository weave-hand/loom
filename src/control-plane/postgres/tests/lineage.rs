use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_lineage_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::lineage_contract(&cp).await;
}

#[tokio::test]
async fn postgres_passes_lineage_closure_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::lineage_closure_contract(&cp).await;
}

#[tokio::test]
async fn postgres_passes_lineage_pagination_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::lineage_pagination_contract(&cp).await;
}

#[tokio::test]
async fn postgres_passes_type_table_binding_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::type_table_binding_contract(&cp).await;
}

#[tokio::test]
async fn postgres_binding_edge_is_source_guarded() {
    use control_plane_core::{ObjectType, Ontology, TableRef, TypeName};

    let fixture = PgFixture::shared();
    let (cp, db) = fixture.fresh_db().await;
    let pool = fixture.pool_for(&db).await;

    let otype = |schema: &str, table: &str| ObjectType {
        name: TypeName("GuardX".into()),
        properties: vec![],
        derived: vec![],
        table: TableRef {
            schema: schema.into(),
            name: table.into(),
        },
        identity: None,
    };

    // Count binding events whose output node is loom:type/GuardX.
    let count = || async {
        sqlx::query_scalar::<_, i64>(
            "select count(*) from lineage.event e \
             join lineage.event_dataset d on d.event_id = e.event_id \
             where e.payload->>'loom.kind' = 'type-table-binding' \
               and d.direction = 'output' and d.namespace = 'loom:type' and d.name = $1",
        )
        .bind("GuardX")
        .fetch_one(&pool)
        .await
        .unwrap()
    };

    // First define + an identical re-define: exactly ONE binding edge (guard suppresses
    // the redundant one).
    cp.define_type(otype("main", "guard_a")).await.unwrap();
    cp.define_type(otype("main", "guard_a")).await.unwrap();
    assert_eq!(count().await, 1, "redundant re-define emits no second edge");

    // Rebind to a different table: a SECOND edge is appended (append-only history).
    cp.define_type(otype("main", "guard_b")).await.unwrap();
    assert_eq!(
        count().await,
        2,
        "rebind appends a new edge; the old one is retained"
    );
}
