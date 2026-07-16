//! StandaloneTuning::from_map: the composite's env-derived tunables, parsed once
//! in main from the snapshot (defaults when absent, fail-loud when malformed).
use std::collections::HashMap;
use std::time::Duration;

use standalone::StandaloneTuning;

#[test]
fn defaults_cover_all_three_domains() {
    let t = StandaloneTuning::from_map(&HashMap::new()).unwrap();
    assert_eq!(t.session_ttl, Duration::from_secs(86_400));
    assert_eq!(t.max_ttl, Duration::from_secs(90 * 24 * 3600));
    assert_eq!(t.engine.inline_byte_limit, 16 * 1024 * 1024);
    assert_eq!(t.engine.flush_byte_threshold, 64 * 1024 * 1024);
}

#[test]
fn malformed_ttl_is_startup_error() {
    let mut vars: HashMap<String, String> = HashMap::new();
    vars.insert("LOOM_SESSION_TTL_SECS".into(), "soon".into());
    let err = StandaloneTuning::from_map(&vars).unwrap_err();
    assert!(
        matches!(err, service_runtime::ConfigError::Invalid { ref var, .. }
        if var == "LOOM_SESSION_TTL_SECS")
    );
}

#[test]
fn defaults_cover_worker_job_config() {
    let t = StandaloneTuning::from_map(&HashMap::new()).unwrap();
    // Mirrors `loom_config::WorkerTuning::default()` (poll_interval_ms: 5000).
    assert_eq!(t.jobs.worker.poll_interval(), Duration::from_millis(5000));
    // Mirrors `worker-bin`'s default in `src/services/worker/src/main.rs:53`.
    assert_eq!(t.compact_threshold_bytes, 128 * 1024 * 1024);
}

#[test]
fn worker_job_config_overlays_from_env() {
    let mut vars: HashMap<String, String> = HashMap::new();
    vars.insert("LOOM_WORKER_POLL_INTERVAL_MS".into(), "250".into());
    vars.insert("LOOM_COMPACT_THRESHOLD_BYTES".into(), "4096".into());
    let t = StandaloneTuning::from_map(&vars).unwrap();
    assert_eq!(t.jobs.worker.poll_interval(), Duration::from_millis(250));
    assert_eq!(t.compact_threshold_bytes, 4096);
}

#[test]
fn malformed_compact_threshold_is_startup_error() {
    let mut vars: HashMap<String, String> = HashMap::new();
    vars.insert("LOOM_COMPACT_THRESHOLD_BYTES".into(), "big".into());
    let err = StandaloneTuning::from_map(&vars).unwrap_err();
    // Attribute the error to THIS var, like `malformed_ttl_is_startup_error` does:
    // a bare `is_err()` would still pass if a reorder of `from_map` made some other
    // field fail first, so it would stop proving the threshold is parsed fail-loud.
    assert!(
        matches!(err, service_runtime::ConfigError::Invalid { ref var, .. }
        if var == "LOOM_COMPACT_THRESHOLD_BYTES")
    );
}
