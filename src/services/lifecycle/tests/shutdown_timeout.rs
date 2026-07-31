//! `LOOM_SHUTDOWN_TIMEOUT_MS` parse: default, override, and a strict failure on a
//! malformed value (no silent fallback — the same fail-loud rule as loom's other
//! tuning seams).
use std::collections::HashMap;
use std::time::Duration;

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn defaults_to_twenty_seconds() {
    assert_eq!(
        loom_lifecycle::shutdown_timeout(&map(&[])).unwrap(),
        Duration::from_millis(20_000)
    );
}

#[test]
fn env_overrides_the_default() {
    let vars = map(&[("LOOM_SHUTDOWN_TIMEOUT_MS", "1500")]);
    assert_eq!(
        loom_lifecycle::shutdown_timeout(&vars).unwrap(),
        Duration::from_millis(1500)
    );
}

#[test]
fn a_malformed_value_fails_startup() {
    let vars = map(&[("LOOM_SHUTDOWN_TIMEOUT_MS", "soon")]);
    assert!(loom_lifecycle::shutdown_timeout(&vars).is_err());
}

#[test]
fn the_default_is_under_the_kubernetes_grace_period() {
    // The whole point of the bound: exit on our own terms before the container
    // runtime SIGKILLs us at the end of its 30s default grace period.
    assert!(loom_lifecycle::DEFAULT_SHUTDOWN_TIMEOUT_MS < 30_000);
}
