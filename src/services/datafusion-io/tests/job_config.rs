//! Config behaviour of `JobConfig` (the shared worker/transform composed config).
use std::collections::HashMap;

use datafusion_io::JobConfig;
use loom_config::LayeredConfig;

#[test]
fn overlays_env_onto_defaults() {
    let mut cfg = JobConfig::default();
    let mut env = HashMap::new();
    env.insert("LOOM_WORKER_POLL_INTERVAL_MS".to_string(), "42".to_string());
    cfg.overlay_env(&env).expect("overlay");
    cfg.validate().expect("valid");
    assert_eq!(cfg.worker.poll_interval().as_millis(), 42);
}
