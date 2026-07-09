//! End-to-end proof of slice-4's insert-path atomic-enqueue contract.
//!
//! A single-step `Insert` action whose `downstream` declares a `JobTemplate`
//! enqueues its resolved [`NewJob`]s on the write's commit transaction — so a
//! job is visible to a worker **iff** the write committed (commit-or-neither).
//! Drives the real axum router (through the auth gate, via `post_action_raw`)
//! over a hermetic Postgres + `LocalFsStorage` warehouse with a real in-process
//! engine writer — `StubAction`'s default `write_steps` errors, so the atomic
//! commit tx can only be exercised by a real write engine.
//!
//! The queue is observed directly: a runtime `select ... from queue.jobs`
//! (mirrors the `job_count` helper in `engine-serving/tests/action_writer.rs`).
//! `queue.jobs` is a control-plane table outside query-api's `.sqlx` cache, so
//! the untyped `query_scalar` form is used (no compile-time check).
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, ControlPlane, Effect, JobTemplate, ObjectType,
    ParamDef, PolicyTarget, PropertyConstraints, RangeConstraint, RoleId, SubjectId,
    TRANSFORM_JOB_KIND, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{InProcessServingEngine, post_action_raw};
use query_api::serving::{ActionEngine, ServingEngine};
use serde_json::{Value, json};

fn tn(s: &str) -> TypeName {
    TypeName(s.into())
}

/// Count `queue.jobs` rows with the given kind. Mirrors the helper in
/// `engine-serving/tests/action_writer.rs` (runtime sqlx — `queue.jobs` is not
/// in query-api's `.sqlx` cache).
async fn job_count(pool: &sqlx::PgPool, kind: &str) -> i64 {
    sqlx::query_scalar::<_, i64>("select count(*)::bigint from queue.jobs where kind = $1")
        .bind(kind)
        .fetch_one(pool)
        .await
        .expect("job_count")
}

/// The `payload` (jsonb) of every queued job of `kind`, in creation order.
async fn job_payloads(pool: &sqlx::PgPool, kind: &str) -> Vec<Value> {
    sqlx::query_scalar::<_, Value>(
        "select payload from queue.jobs where kind = $1 order by created_at",
    )
    .bind(kind)
    .fetch_all(pool)
    .await
    .expect("job_payloads")
}

/// Define `subject` + `role`, attach the role, and grant `role` coarse `Write`
/// on every named type. Returns the subject (the role is retained so callers
/// can later `set_policy` on it if they want fine-grained plumbing).
async fn writer_on(cp: &PgControlPlane, types: &[&str]) -> (SubjectId, RoleId) {
    let subj = SubjectId("writer".into());
    let role = RoleId("writers".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    for t in types {
        cp.grant(
            &role,
            Action::Write,
            PolicyTarget::Type(TypeName((*t).into())),
            Effect::Allow,
        )
        .await
        .unwrap();
    }
    (subj, role)
}

/// Define `Order(id Long required identity)` — when `id_constraints` is non-empty
/// the `id` property carries them (used by the case-2 422 path).
async fn define_order(cp: &PgControlPlane, id_constraints: PropertyConstraints) {
    cp.ontology()
        .define_type(
            ObjectType::build("Order", ("main", "order"))
                .prop_with("id", "Long", true, id_constraints)
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
}

/// A single-step `createOrder` Insert action with the given `downstream`
/// templates (empty for the back-compat case).
fn create_order_action(downstream: Vec<JobTemplate>) -> ActionDef {
    ActionDef::single_step(
        ActionName("createOrder".into()),
        tn("Order"),
        ActionKind::Insert,
        vec![ParamDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
            binds: None,
        }],
        vec![],
    )
    .downstream(downstream)
}

/// One declared downstream: a `transform` job whose `orderId` is the written
/// row's `id` (an `@self.<prop>` reference resolved at invoke time).
fn order_transform_template() -> JobTemplate {
    JobTemplate {
        kind: TRANSFORM_JOB_KIND.into(),
        payload: json!({ "orderId": "@self.id" }),
    }
}

/// The shared fixture spawn: Order type (unconstrained `id`), a `createOrder`
/// action with one transform downstream, a Write grant, and the real engine.
/// Returns the live handles the three enqueue cases drive.
struct Harness {
    cp: Arc<PgControlPlane>,
    pool: sqlx::PgPool,
    serving: Arc<dyn ServingEngine>,
    action_engine: Arc<dyn ActionEngine>,
    subj: SubjectId,
    _warehouse: tempfile::TempDir,
    _eg: e2e_support::EngineGuard,
}

async fn harness_downstream(template: Option<JobTemplate>) -> Harness {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    define_order(&cp, PropertyConstraints::default()).await;
    let downstream = match template {
        Some(t) => vec![t],
        None => vec![],
    };
    cp.ontology()
        .define_action(create_order_action(downstream))
        .await
        .unwrap();

    let (subj, _role) = writer_on(&cp, &["Order"]).await;

    let (engine, eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving: Arc<dyn ServingEngine> = Arc::new(InProcessServingEngine::new(
        IcebergCatalog::new(pool.clone()),
    ));
    let action_engine: Arc<dyn ActionEngine> = Arc::new(engine);

    Harness {
        cp: Arc::new(cp),
        pool,
        serving,
        action_engine,
        subj,
        _warehouse: warehouse,
        _eg: eg,
    }
}

/// A committed insert enqueues exactly ONE `transform` job, and its resolved
/// `orderId` payload equals the new order's identity (the `@self.id` ref was
/// substituted from the written row). This is the atomic-enqueue happy path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_insert_enqueues_downstream_job() {
    let h = harness_downstream(Some(order_transform_template())).await;

    let (status, _headers, body) = post_action_raw(
        h.cp.clone(),
        h.serving.clone(),
        h.action_engine.clone(),
        "/actions/createOrder",
        &json!({ "id": "42" }),
        h.subj.0.as_str(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");

    // Exactly one transform job, resolved against the written row.
    assert_eq!(
        job_count(&h.pool, TRANSFORM_JOB_KIND).await,
        1,
        "one downstream transform job rides the committed write"
    );
    let payloads = job_payloads(&h.pool, TRANSFORM_JOB_KIND).await;
    assert_eq!(payloads.len(), 1);
    // SqlValue::Int(42) renders as a JSON number (see downstream_resolve.rs).
    assert_eq!(payloads[0]["orderId"], json!(42));
}

/// An insert rejected BEFORE the write commits (a per-value `range` constraint
/// violation ⇒ structured 422) enqueues NO downstream job — the job never
/// escapes a write that doesn't commit. `write_object` is never reached, so the
/// commit tx (the only enqueue site) never opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_write_rejection_enqueues_no_job() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // Order.id constrained to [0..=100]; an out-of-range insert 422s pre-write.
    define_order(
        &cp,
        PropertyConstraints {
            range: Some(RangeConstraint {
                min: Some(0.0),
                max: Some(100.0),
            }),
            ..PropertyConstraints::default()
        },
    )
    .await;
    cp.ontology()
        .define_action(create_order_action(vec![order_transform_template()]))
        .await
        .unwrap();
    let (subj, _role) = writer_on(&cp, &["Order"]).await;

    let (engine, eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving: Arc<dyn ServingEngine> = Arc::new(InProcessServingEngine::new(
        IcebergCatalog::new(pool.clone()),
    ));
    let action_engine: Arc<dyn ActionEngine> = Arc::new(engine);
    let cp = Arc::new(cp);

    // 9999 is outside [0..=100] ⇒ 422 ConstraintViolation, before write_object.
    let (status, _headers, body) = post_action_raw(
        cp.clone(),
        serving.clone(),
        action_engine.clone(),
        "/actions/createOrder",
        &json!({ "id": "9999" }),
        subj.0.as_str(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "out-of-range insert is a 422: {body}"
    );

    // The atomic contract: no job was enqueued for the rejected write.
    assert_eq!(
        job_count(&pool, TRANSFORM_JOB_KIND).await,
        0,
        "a pre-write rejection must enqueue no downstream job"
    );

    drop(eg);
}

/// A plain insert action with empty `downstream` enqueues nothing extra — a
/// regression guard that the slice-4 wiring did not perturb the back-compat
/// path (the pre-slice-4 behavior: inserts enqueue zero jobs of their own).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_downstream_enqueues_no_job() {
    let h = harness_downstream(None).await;

    let (status, _headers, body) = post_action_raw(
        h.cp.clone(),
        h.serving.clone(),
        h.action_engine.clone(),
        "/actions/createOrder",
        &json!({ "id": "7" }),
        h.subj.0.as_str(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");

    assert_eq!(
        job_count(&h.pool, TRANSFORM_JOB_KIND).await,
        0,
        "an action with no downstream enqueues no job"
    );
}
