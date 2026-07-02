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
