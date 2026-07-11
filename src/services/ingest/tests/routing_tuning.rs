//! Config behaviour of `RoutingTuning` (inline/flush byte routing knobs).
use std::collections::HashMap;

use ingest::config::RoutingTuning;

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn defaults_match_constants() {
    let r = RoutingTuning::default();
    assert_eq!(r.inline_byte_limit, 16 * 1024 * 1024);
    assert_eq!(r.flush_byte_threshold, 64 * 1024 * 1024);
    assert_eq!(r.http_max_body_bytes, 64 * 1024 * 1024);
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
    r.overlay_env(&map(&[("LOOM_INLINE_BYTE_LIMIT", "2048")]))
        .unwrap();
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
    let err = r
        .overlay_env(&map(&[("LOOM_INLINE_BYTE_LIMIT", "abc")]))
        .unwrap_err();
    assert!(format!("{err}").contains("LOOM_INLINE_BYTE_LIMIT"));
}

#[test]
fn validate_rejects_zero() {
    let r: RoutingTuning = serde_json::from_str(r#"{"inline_byte_limit": 0}"#).unwrap();
    assert!(r.validate().is_err());
}

#[test]
fn validate_rejects_nonpositive_flush_threshold() {
    let r: RoutingTuning = serde_json::from_str(r#"{"flush_byte_threshold": 0}"#).unwrap();
    let err = r.validate().unwrap_err();
    assert!(format!("{err}").contains("LOOM_FLUSH_BYTE_THRESHOLD"));
}

#[test]
fn http_body_cap_env_overlay() {
    let mut r = RoutingTuning::default();
    r.overlay_env(&map(&[("LOOM_HTTP_MAX_BODY_BYTES", "134217728")]))
        .unwrap();
    assert_eq!(r.http_max_body_bytes, 128 * 1024 * 1024);
}

#[test]
fn validate_rejects_body_cap_at_or_below_inline_limit() {
    let r: RoutingTuning =
        serde_json::from_str(r#"{"inline_byte_limit": 1024, "http_max_body_bytes": 1024}"#)
            .unwrap();
    let err = r.validate().unwrap_err();
    assert!(format!("{err}").contains("LOOM_HTTP_MAX_BODY_BYTES"));
}

#[test]
fn compact_defaults_are_128_mib_and_8_files() {
    let r = RoutingTuning::default();
    assert_eq!(r.compact_small_file_bytes, 128 * 1024 * 1024);
    assert_eq!(r.compact_trigger_files, 8);
}

#[test]
fn compact_overrides_parse() {
    let mut r = RoutingTuning::default();
    r.overlay_env(&map(&[
        ("LOOM_COMPACT_THRESHOLD_BYTES", "4096"),
        ("LOOM_COMPACT_TRIGGER_FILES", "16"),
    ]))
    .unwrap();
    assert_eq!(r.compact_small_file_bytes, 4096);
    assert_eq!(r.compact_trigger_files, 16);
}

#[test]
fn compact_trigger_files_zero_disables_and_validates_ok() {
    let mut r = RoutingTuning::default();
    r.overlay_env(&map(&[("LOOM_COMPACT_TRIGGER_FILES", "0")]))
        .unwrap();
    assert_eq!(r.compact_trigger_files, 0);
    r.validate().unwrap();
}

#[test]
fn compact_trigger_files_one_is_rejected() {
    let mut r = RoutingTuning::default();
    r.overlay_env(&map(&[("LOOM_COMPACT_TRIGGER_FILES", "1")]))
        .unwrap();
    let err = r.validate().unwrap_err();
    assert!(format!("{err}").contains("LOOM_COMPACT_TRIGGER_FILES"));
}

#[test]
fn compact_trigger_files_negative_is_rejected() {
    let mut r = RoutingTuning::default();
    r.overlay_env(&map(&[("LOOM_COMPACT_TRIGGER_FILES", "-1")]))
        .unwrap();
    let err = r.validate().unwrap_err();
    assert!(format!("{err}").contains("LOOM_COMPACT_TRIGGER_FILES"));
}
