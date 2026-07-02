//! EngineTuning::from_map: the write-path byte thresholds parsed from the main's
//! env snapshot (was: live-env parse_env_or inside engine::run, violating the
//! one-env-snapshot-per-main rule).
use std::collections::HashMap;

use engine::EngineTuning;

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn defaults_are_16_mib_inline_and_64_mib_flush() {
    let t = EngineTuning::from_map(&map(&[])).unwrap();
    assert_eq!(t.inline_byte_limit, 16 * 1024 * 1024);
    assert_eq!(t.flush_byte_threshold, 64 * 1024 * 1024);
}

#[test]
fn overrides_parse() {
    let t = EngineTuning::from_map(&map(&[
        ("LOOM_INLINE_BYTE_LIMIT", "1024"),
        ("LOOM_FLUSH_BYTE_THRESHOLD", "2048"),
    ]))
    .unwrap();
    assert_eq!(t.inline_byte_limit, 1024);
    assert_eq!(t.flush_byte_threshold, 2048);
}

#[test]
fn malformed_value_is_startup_error_naming_key() {
    let err = EngineTuning::from_map(&map(&[("LOOM_INLINE_BYTE_LIMIT", "lots")])).unwrap_err();
    assert!(
        matches!(err, service_runtime::ConfigError::Invalid { ref var, .. }
        if var == "LOOM_INLINE_BYTE_LIMIT")
    );
}
