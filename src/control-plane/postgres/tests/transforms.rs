use loom_test_seed::local_sql_catalog;

use control_plane_core::{
    OutputMode, TableRef, TransformBody, TransformDef, TransformName, Transforms,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use time::OffsetDateTime;

#[tokio::test]
async fn postgres_passes_transforms_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::transforms_contract(&cp).await;
}

async fn iceberg_cp(fx: &PgFixture) -> (IcebergControlPlane, tempfile::TempDir) {
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    (IcebergControlPlane::new(pg, catalog), wh)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_passes_transform_run_commit_success_contract() {
    let fx = PgFixture::shared();
    let (cp, _wh) = iceberg_cp(fx).await;
    control_plane_testkit::transform_run_commit_success_contract(&cp).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_passes_transform_data_trigger_contract() {
    let fx = PgFixture::shared();
    let (cp, _wh) = iceberg_cp(fx).await;
    control_plane_testkit::transform_data_trigger_contract(&cp).await;
}

fn sched_def(name: &str) -> TransformDef {
    TransformDef {
        name: TransformName(name.into()),
        body: TransformBody::Physical {
            inputs: vec![TableRef {
                schema: "main".into(),
                name: "in".into(),
            }],
            output: TableRef {
                schema: "main".into(),
                name: format!("out_{name}"),
            },
            sql: "select 1".into(),
            output_mode: OutputMode::Append,
        },
        schedule: Some("0 0 * * *".into()), // daily; define sets next_run_at to the next occurrence
        on_input_commit: false,
    }
}

/// One undecodable due schedule must not starve the batch: healthy schedules are
/// still claimed, and the poison row's next_run_at advances out of the due window.
#[tokio::test]
async fn claim_survives_poison_schedule_row() {
    let fx = PgFixture::shared();
    let (pg, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    pg.define_transform(sched_def("healthy"))
        .await
        .expect("define healthy");
    pg.define_transform(sched_def("poison"))
        .await
        .expect("define poison");

    // Force both into the due window, then corrupt the poison body.
    sqlx::query("update transforms.transform set next_run_at = now() - interval '1 hour'")
        .execute(&pool)
        .await
        .expect("make schedules due");
    sqlx::query("update transforms.transform set body = '\"nonsense\"'::jsonb where name = $1")
        .bind("poison")
        .execute(&pool)
        .await
        .expect("corrupt poison body");

    let now = OffsetDateTime::now_utc();
    let claimed = pg
        .claim_due_schedules(now, 10)
        .await
        .expect("claim commits despite a poison row");

    let names: Vec<String> = claimed.into_iter().map(|d| d.name.0).collect();
    assert!(
        names.contains(&"healthy".to_string()),
        "healthy schedule is claimed"
    );
    assert!(
        !names.contains(&"poison".to_string()),
        "poison schedule is skipped"
    );

    // Regression guard: the poison row's next_run_at advanced past `now`, so a
    // second tick no longer re-surfaces it earliest and starves the batch.
    let poison_next: OffsetDateTime =
        sqlx::query_scalar("select next_run_at from transforms.transform where name = 'poison'")
            .fetch_one(&pool)
            .await
            .expect("poison next_run_at");
    assert!(
        poison_next > now,
        "poison next_run_at advanced out of the due window"
    );
}
