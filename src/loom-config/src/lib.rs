//! loom's shared configuration seam: the typed-error + typed-env-overlay machinery
//! every per-domain tuning struct uses, plus the cross-crate `WorkerTuning`. A light
//! leaf crate (deps: thiserror, serde, serde_json) so postgres-free crates
//! (`datafusion-io`, `control_plane_worker`, `worker-bin`) can depend on it without
//! pulling `service_runtime` (and thus postgres) into their closure.

use std::collections::HashMap;
use std::str::FromStr;

mod worker;
pub use worker::WorkerTuning;

/// A configuration parse/validation failure. Re-exported as `service_runtime::ConfigError`
/// so existing call sites are unchanged; every `main` already surfaces it as a startup error.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing required environment variable: {0}")]
    MissingVar(String),
    #[error("invalid value for {var}: {detail}")]
    Invalid { var: String, detail: String },
}

/// Construct an `Invalid` error naming the offending key.
#[must_use]
pub fn invalid(var: &str, detail: impl core::fmt::Display) -> ConfigError {
    ConfigError::Invalid { var: var.to_string(), detail: detail.to_string() }
}

/// Apply `vars[key]` over `slot` if present; `ConfigError::Invalid` (naming `key`) if
/// present-but-unparseable; no-op if absent. A present-but-malformed value is a startup
/// error, NOT a silent fallback — the fix for the old lossy `.ok()` reads.
pub fn overlay_opt<T>(
    vars: &HashMap<String, String>,
    key: &str,
    slot: &mut T,
) -> Result<(), ConfigError>
where
    T: FromStr,
    T::Err: core::fmt::Display,
{
    if let Some(raw) = vars.get(key) {
        *slot = raw.parse().map_err(|e| invalid(key, e))?;
    }
    Ok(())
}

/// Snapshot the process environment into a map — read once per `main` so config
/// loading is consistent and testable without touching the real environment.
#[must_use]
pub fn env_map() -> HashMap<String, String> {
    std::env::vars().collect()
}

/// Deserialize a JSON config document into a (defaulted) config struct. Container-level
/// `#[serde(default)]` on the target makes any omitted key fall to its `Default`, so a
/// partial document is valid. Parse failures surface as `Invalid` naming `LOOM_CONFIG_FILE`.
pub fn parse_config_doc<T: serde::de::DeserializeOwned>(doc: &str) -> Result<T, ConfigError> {
    serde_json::from_str(doc).map_err(|e| invalid("LOOM_CONFIG_FILE", e))
}
