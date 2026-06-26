//! Config behaviour of `WorkerTuning` (poll interval + backoff bounds).
use std::collections::HashMap;
use std::time::Duration;

use loom_config::WorkerTuning;

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn defaults_match_constants() {
    let w = WorkerTuning::default();
    assert_eq!(w.poll_interval(), Duration::from_secs(5));
    assert_eq!(w.backoff_ceiling_secs, 60);
    assert_eq!(w.backoff_max_attempts, 6);
}

#[test]
fn backoff_matches_current_worker_formula() {
    // The worker handler's current backoff: 1s, 2s, 4s, 8s, 16s, 32s, then capped 60s.
    let w = WorkerTuning::default();
    let got: Vec<u64> = (0..8).map(|a| w.backoff(a).as_secs()).collect();
    assert_eq!(got, vec![1, 2, 4, 8, 16, 32, 60, 60]);
}

#[test]
fn partial_json_falls_to_default() {
    let w: WorkerTuning = serde_json::from_str(r#"{"poll_interval_ms": 250}"#).unwrap();
    assert_eq!(w.poll_interval_ms, 250);
    assert_eq!(w.backoff_max_attempts, 6);
}

#[test]
fn env_overrides_file() {
    let mut w: WorkerTuning = serde_json::from_str(r#"{"poll_interval_ms": 250}"#).unwrap();
    w.overlay_env(&map(&[("LOOM_WORKER_POLL_INTERVAL_MS", "1000")]))
        .unwrap();
    assert_eq!(w.poll_interval_ms, 1000);
}

#[test]
fn env_backoff_keys_apply() {
    let mut w = WorkerTuning::default();
    w.overlay_env(&map(&[
        ("LOOM_WORKER_BACKOFF_CEILING_SECS", "30"),
        ("LOOM_WORKER_BACKOFF_MAX_ATTEMPTS", "4"),
    ]))
    .unwrap();
    assert_eq!(w.backoff_ceiling_secs, 30);
    assert_eq!(w.backoff_max_attempts, 4);
}

#[test]
fn malformed_env_is_error_naming_key() {
    let mut w = WorkerTuning::default();
    let err = w
        .overlay_env(&map(&[("LOOM_WORKER_POLL_INTERVAL_MS", "soon")]))
        .unwrap_err();
    assert!(format!("{err}").contains("LOOM_WORKER_POLL_INTERVAL_MS"));
}

#[test]
fn validate_rejects_zero() {
    let w: WorkerTuning = serde_json::from_str(r#"{"poll_interval_ms": 0}"#).unwrap();
    assert!(w.validate().is_err());
    let w2: WorkerTuning = serde_json::from_str(r#"{"backoff_max_attempts": 0}"#).unwrap();
    assert!(w2.validate().is_err());
}
