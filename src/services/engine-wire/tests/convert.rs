use control_plane_core::{Job, JobId, RetryPolicy};
use engine_wire::convert::{job_from_pb, job_to_pb, retry_policy_from_pb, retry_policy_to_pb};
use std::time::Duration;

#[test]
fn job_roundtrips_through_pb() {
    let id = uuid::Uuid::new_v4();
    let job = Job {
        id: JobId(id),
        kind: "flush_table".into(),
        payload: serde_json::json!({"schema":"wh","name":"t"}),
        attempts: 2,
        run_at: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
    };
    let back = job_from_pb(job_to_pb(&job)).unwrap();
    assert_eq!(back.id.0, id);
    assert_eq!(back.kind, "flush_table");
    assert_eq!(back.payload["schema"], "wh");
    assert_eq!(back.attempts, 2);
    assert_eq!(back.run_at.unix_timestamp(), 1_700_000_000);
}

#[test]
fn retry_policy_roundtrips() {
    let r = retry_policy_from_pb(retry_policy_to_pb(&RetryPolicy::Retry {
        delay: Duration::from_millis(250),
    }));
    assert!(matches!(r, Ok(RetryPolicy::Retry { delay }) if delay == Duration::from_millis(250)));
    let a = retry_policy_from_pb(retry_policy_to_pb(&RetryPolicy::Abandon));
    assert!(matches!(a, Ok(RetryPolicy::Abandon)));
}
