use std::time::Duration;

use control_plane_core::{JobFailure, RetryPolicy};

#[test]
fn abandon_sets_abandon_policy() {
    let f = JobFailure::abandon("bad payload");
    assert_eq!(f.error, "bad payload");
    assert!(matches!(f.policy, RetryPolicy::Abandon));
}

#[test]
fn retry_carries_delay() {
    let f = JobFailure::retry(Duration::from_millis(250), format!("rpc: {}", "boom"));
    assert_eq!(f.error, "rpc: boom");
    match f.policy {
        RetryPolicy::Retry { delay } => assert_eq!(delay, Duration::from_millis(250)),
        RetryPolicy::Abandon => panic!("expected Retry"),
    }
}
