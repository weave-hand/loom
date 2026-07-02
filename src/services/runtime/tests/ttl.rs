//! Fail-loud auth TTL readers over the env snapshot (iss-config-silent-fallbacks):
//! absent => documented default; present-but-malformed => startup error naming
//! the key (the old *_from_env readers silently fell back to the default).
use std::collections::HashMap;
use std::time::Duration;

use service_runtime::{ConfigError, service_token_max_ttl, session_ttl};

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn session_ttl_defaults_to_24h() {
    assert_eq!(session_ttl(&map(&[])).unwrap(), Duration::from_secs(86_400));
}

#[test]
fn session_ttl_parses_override() {
    let vars = map(&[("LOOM_SESSION_TTL_SECS", "120")]);
    assert_eq!(session_ttl(&vars).unwrap(), Duration::from_secs(120));
}

#[test]
fn session_ttl_malformed_is_startup_error() {
    let vars = map(&[("LOOM_SESSION_TTL_SECS", "soon")]);
    let err = session_ttl(&vars).unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { ref var, .. }
        if var == "LOOM_SESSION_TTL_SECS"));
}

#[test]
fn max_ttl_defaults_to_90_days() {
    assert_eq!(
        service_token_max_ttl(&map(&[])).unwrap(),
        Duration::from_secs(90 * 24 * 3600)
    );
}

#[test]
fn max_ttl_parses_override() {
    let vars = map(&[("LOOM_SERVICE_TOKEN_MAX_TTL", "3600")]);
    assert_eq!(
        service_token_max_ttl(&vars).unwrap(),
        Duration::from_secs(3600)
    );
}

#[test]
fn max_ttl_malformed_is_startup_error() {
    let vars = map(&[("LOOM_SERVICE_TOKEN_MAX_TTL", "forever")]);
    let err = service_token_max_ttl(&vars).unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { ref var, .. }
        if var == "LOOM_SERVICE_TOKEN_MAX_TTL"));
}
