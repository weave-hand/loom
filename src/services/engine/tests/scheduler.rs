//! Scheduler tick against the memory control plane: a due schedule fires
//! exactly once as a `trigger: Schedule` run.

use std::time::Duration;

use control_plane_core::{
    ControlPlane, OutputMode, PageReq, RunState, RunTrigger, TableRef, TransformBody, TransformDef,
    TransformName, Transforms,
};
use control_plane_memory::MemoryControlPlane;

fn scheduled_def(name: &str) -> TransformDef {
    TransformDef {
        name: TransformName(name.into()),
        body: TransformBody::Physical {
            inputs: vec![TableRef {
                schema: "main".into(),
                name: "src".into(),
            }],
            output: TableRef {
                schema: "main".into(),
                name: "dst".into(),
            },
            sql: "select * from src".into(),
            output_mode: OutputMode::Append,
        },
        schedule: Some("0 3 * * *".into()),
        on_input_commit: false,
    }
}

#[tokio::test]
async fn due_schedule_fires_once_as_schedule_run() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_transform(scheduled_def("nightly")).await.unwrap();

    // Not due yet: nothing fires.
    let now = time::OffsetDateTime::now_utc();
    assert_eq!(engine::scheduler::tick(&cp, now, 32).await, 0);

    // Probe two days ahead: exactly one fire, trigger Schedule, job enqueued.
    let probe = now + time::Duration::days(2);
    assert_eq!(engine::scheduler::tick(&cp, probe, 32).await, 1);
    let runs = cp
        .transforms()
        .list_runs(None, PageReq::default())
        .await
        .unwrap();
    assert_eq!(runs.items.len(), 1);
    let run = &runs.items[0];
    assert_eq!(run.trigger, RunTrigger::Schedule);
    assert_eq!(run.state, RunState::Queued);
    assert_eq!(run.transform, Some(TransformName("nightly".into())));
    let job = cp
        .queue()
        .dequeue(&["transform".to_string()], "sched-test")
        .await
        .unwrap()
        .expect("scheduled job enqueued");
    assert_eq!(
        job.payload["run_id"],
        serde_json::json!(run.run_id.to_string())
    );

    // Same probe again: the claim already advanced next_run_at — no re-fire.
    assert_eq!(engine::scheduler::tick(&cp, probe, 32).await, 0);
}

#[tokio::test]
async fn reconcile_sweeps_a_stranded_run_and_leaves_a_live_one() {
    use control_plane_core::{RetryPolicy, RunState, RunTrigger, TransformName, TransformRun};
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    cp.define_transform(scheduled_def("nightly")).await.unwrap();
    let def = cp
        .get_transform(&TransformName("nightly".into()))
        .await
        .unwrap();

    let mk = |rid| TransformRun {
        run_id: rid,
        transform: Some(TransformName("nightly".into())),
        trigger: RunTrigger::Manual,
        state: RunState::Queued,
        body: def.body.clone(),
        queued_at: time::OffsetDateTime::now_utc(),
        started_at: None,
        finished_at: None,
        snapshot_id: None,
        error: None,
    };

    // Stranded: dequeued, running, job abandoned, run never finished.
    let stranded = uuid::Uuid::new_v4();
    let sjob = def.body.to_job(stranded);
    let kinds = vec![sjob.kind.clone()];
    cp.submit_run(mk(stranded), sjob).await.unwrap();
    let dq = cp.queue().dequeue(&kinds, "w").await.unwrap().unwrap();
    cp.mark_run_running(stranded).await.unwrap();
    cp.queue()
        .fail(dq.id, "boom", RetryPolicy::Abandon)
        .await
        .unwrap();

    // Live: dequeued and running, job still live.
    let live = uuid::Uuid::new_v4();
    let ljob = def.body.to_job(live);
    let lkinds = vec![ljob.kind.clone()];
    cp.submit_run(mk(live), ljob).await.unwrap();
    let _ = cp.queue().dequeue(&lkinds, "w").await.unwrap().unwrap();
    cp.mark_run_running(live).await.unwrap();

    let cutoff = time::OffsetDateTime::now_utc() + time::Duration::hours(1);
    let swept = cp.reconcile_stranded_runs(cutoff).await.unwrap();
    assert_eq!(swept, vec![stranded]);
    assert_eq!(cp.get_run(stranded).await.unwrap().state, RunState::Failed);
    assert_eq!(cp.get_run(live).await.unwrap().state, RunState::Running);
}
