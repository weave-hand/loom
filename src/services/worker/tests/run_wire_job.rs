use control_plane_core::{Job, JobId, RetryPolicy};
use loom_config::WorkerTuning;
use worker::handler::run_wire_job;

fn job(payload: serde_json::Value) -> Job {
    Job {
        id: JobId(uuid::Uuid::nil()),
        kind: "test".into(),
        payload,
        attempts: 1,
        run_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}

#[derive(serde::Deserialize)]
struct P {
    n: i64,
}

#[tokio::test]
async fn bad_payload_abandons() {
    let out = run_wire_job::<P, _, _, String>(
        job(serde_json::json!({ "wrong": 1 })),
        WorkerTuning::default(),
        "test",
        |_p| async { Ok(()) },
    )
    .await;
    let err = out.expect_err("should fail to parse");
    assert!(matches!(err.policy, RetryPolicy::Abandon));
    assert!(err.error.contains("bad test payload"));
}

#[tokio::test]
async fn rpc_error_retries() {
    let out = run_wire_job::<P, _, _, &str>(
        job(serde_json::json!({ "n": 7 })),
        WorkerTuning::default(),
        "test",
        |p| async move {
            assert_eq!(p.n, 7);
            Err("boom")
        },
    )
    .await;
    let err = out.expect_err("rpc failed");
    assert!(matches!(err.policy, RetryPolicy::Retry { .. }));
    assert!(err.error.contains("boom"));
}

#[tokio::test]
async fn ok_passes_through() {
    let out = run_wire_job::<P, _, _, String>(
        job(serde_json::json!({ "n": 1 })),
        WorkerTuning::default(),
        "test",
        |_p| async { Ok(()) },
    )
    .await;
    assert!(out.is_ok());
}
