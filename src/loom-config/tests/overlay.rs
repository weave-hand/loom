//! Unit tests for the shared config-parse machinery.
use std::collections::HashMap;

use loom_config::{ConfigError, env_map, overlay_opt, parse_config_doc, parse_var, req_var};

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn overlay_opt_absent_is_noop() {
    let vars = map(&[]);
    let mut slot: u64 = 42;
    overlay_opt(&vars, "LOOM_X", &mut slot).unwrap();
    assert_eq!(slot, 42);
}

#[test]
fn overlay_opt_present_parses() {
    let vars = map(&[("LOOM_X", "7")]);
    let mut slot: u64 = 42;
    overlay_opt(&vars, "LOOM_X", &mut slot).unwrap();
    assert_eq!(slot, 7);
}

#[test]
fn overlay_opt_malformed_is_error_naming_key() {
    let vars = map(&[("LOOM_X", "abc")]);
    let mut slot: u64 = 42;
    let err = overlay_opt(&vars, "LOOM_X", &mut slot).unwrap_err();
    match err {
        ConfigError::Invalid { var, .. } => assert_eq!(var, "LOOM_X"),
        other => panic!("expected Invalid, got {other:?}"),
    }
    assert_eq!(slot, 42, "slot unchanged on error");
}

#[test]
fn parse_config_doc_rejects_bad_json() {
    let err =
        parse_config_doc::<std::collections::BTreeMap<String, u64>>("{ not json").unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { ref var, .. } if var == "LOOM_CONFIG_FILE"));
}

#[test]
fn parse_config_doc_accepts_good_json() {
    let m: std::collections::BTreeMap<String, u64> = parse_config_doc(r#"{"a": 1}"#).unwrap();
    assert_eq!(m.get("a"), Some(&1));
}

#[test]
fn env_map_is_a_snapshot() {
    // Just confirm it returns the process env without panicking.
    let m = env_map();
    let _ = m.len();
}

#[test]
fn req_var_present_returns_value() {
    let vars = map(&[("LOOM_X", "hello")]);
    assert_eq!(req_var(&vars, "LOOM_X").unwrap(), "hello");
}

#[test]
fn req_var_missing_is_missing_var_error() {
    let vars = map(&[]);
    let err = req_var(&vars, "LOOM_X").unwrap_err();
    assert!(matches!(err, ConfigError::MissingVar(k) if k == "LOOM_X"));
}

#[test]
fn parse_var_absent_returns_default() {
    let vars = map(&[]);
    assert_eq!(parse_var(&vars, "LOOM_X", 42_u64).unwrap(), 42);
}

#[test]
fn parse_var_present_parses() {
    let vars = map(&[("LOOM_X", "7")]);
    assert_eq!(parse_var(&vars, "LOOM_X", 42_u64).unwrap(), 7);
}

#[test]
fn parse_var_malformed_is_error_naming_key() {
    let vars = map(&[("LOOM_X", "abc")]);
    let err = parse_var(&vars, "LOOM_X", 42_u64).unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { ref var, .. } if var == "LOOM_X"));
}
