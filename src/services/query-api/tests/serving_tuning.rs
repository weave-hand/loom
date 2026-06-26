//! Config behaviour of `ServingTuning` (default page size).
use std::collections::HashMap;

use query_api::config::ServingTuning;

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn default_is_1000() {
    assert_eq!(ServingTuning::default().default_limit, 1000);
}

#[test]
fn partial_json_and_env_override() {
    let mut s: ServingTuning = serde_json::from_str("{}").unwrap();
    assert_eq!(s.default_limit, 1000);
    s.overlay_env(&map(&[("LOOM_SERVING_DEFAULT_LIMIT", "50")]))
        .unwrap();
    assert_eq!(s.default_limit, 50);
}

#[test]
fn malformed_env_is_error_naming_key() {
    let mut s = ServingTuning::default();
    let err = s
        .overlay_env(&map(&[("LOOM_SERVING_DEFAULT_LIMIT", "x")]))
        .unwrap_err();
    assert!(format!("{err}").contains("LOOM_SERVING_DEFAULT_LIMIT"));
}

#[test]
fn validate_rejects_zero() {
    let s: ServingTuning = serde_json::from_str(r#"{"default_limit": 0}"#).unwrap();
    assert!(s.validate().is_err());
}
