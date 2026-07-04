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
