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
    use control_plane_core::{ObjectType, Ontology};

    let fixture = PgFixture::shared();
    let (cp, db) = fixture.fresh_db().await;
    let pool = fixture.pool_for(&db).await;

    let otype = |schema: &str, table: &str| ObjectType::build("GuardX", (schema, table)).done();

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

#[tokio::test]
async fn postgres_passes_events_for_hydration_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::events_for_hydration_contract(&cp).await;
}

#[tokio::test]
async fn corrupt_direction_token_is_a_loud_error() {
    use control_plane_core::{DatasetRef, EventType, Lineage, LineageEvent, PageReq, RunId};

    let fixture = PgFixture::shared();
    let (cp, db) = fixture.fresh_db().await;
    let pool = fixture.pool_for(&db).await;

    let run = RunId(uuid::Uuid::new_v4());
    cp.emit(LineageEvent {
        run_id: run,
        event_type: EventType::Start,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![DatasetRef {
            namespace: "main".into(),
            name: "src".into(),
        }],
        outputs: vec![],
        payload: serde_json::json!({}),
    })
    .await
    .unwrap();

    // Corrupt the persisted direction token via direct SQL — unreachable through
    // the public API, which only ever writes 'input'/'output'.
    sqlx::query("update lineage.event_dataset set direction = 'sideways'")
        .execute(&pool)
        .await
        .unwrap();

    // A corrupt token must be a loud error, never silently dropped (pre-batch
    // behavior) or misfiled into outputs (the collapsed query's naive bucketing).
    let err = cp
        .events_for(&run, PageReq::default())
        .await
        .expect_err("corrupt direction must not silently bucket");
    assert!(
        err.to_string()
            .contains("unknown lineage direction 'sideways'"),
        "unexpected error: {err}"
    );
}
