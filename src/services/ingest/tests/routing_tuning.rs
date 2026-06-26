//! Config behaviour of `RoutingTuning` (inline/flush byte routing knobs).
use std::collections::HashMap;

use ingest::config::RoutingTuning;

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
}

#[test]
fn defaults_match_constants() {
    let r = RoutingTuning::default();
    assert_eq!(r.inline_byte_limit, 16 * 1024 * 1024);
    assert_eq!(r.flush_byte_threshold, 64 * 1024 * 1024);
}

#[test]
fn partial_json_falls_to_default() {
    let r: RoutingTuning = serde_json::from_str(r#"{"inline_byte_limit": 1024}"#).unwrap();
    assert_eq!(r.inline_byte_limit, 1024);
    assert_eq!(r.flush_byte_threshold, 64 * 1024 * 1024);
}

#[test]
fn env_overrides_file() {
    let mut r: RoutingTuning = serde_json::from_str(r#"{"inline_byte_limit": 1024}"#).unwrap();
    r.overlay_env(&map(&[("LOOM_INLINE_BYTE_LIMIT", "2048")])).unwrap();
    assert_eq!(r.inline_byte_limit, 2048);
}

#[test]
fn file_survives_when_env_silent() {
    let mut r: RoutingTuning = serde_json::from_str(r#"{"flush_byte_threshold": 99}"#).unwrap();
    r.overlay_env(&map(&[])).unwrap();
    assert_eq!(r.flush_byte_threshold, 99);
}

#[test]
fn malformed_env_is_error_naming_key() {
    let mut r = RoutingTuning::default();
    let err = r.overlay_env(&map(&[("LOOM_INLINE_BYTE_LIMIT", "abc")])).unwrap_err();
    assert!(format!("{err}").contains("LOOM_INLINE_BYTE_LIMIT"));
}

#[test]
fn validate_rejects_zero() {
    let r: RoutingTuning = serde_json::from_str(r#"{"inline_byte_limit": 0}"#).unwrap();
    assert!(r.validate().is_err());
}
