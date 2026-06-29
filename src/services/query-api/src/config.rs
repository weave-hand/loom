//! Serving tuning for query-api: the default page size applied when a caller omits `limit`.

use std::collections::HashMap;

use loom_config::{ConfigError, invalid, overlay_opt};

/// Read-serving tuning.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ServingTuning {
    /// Default page size when a request does not specify `limit`.
    pub default_limit: u32,
}

impl Default for ServingTuning {
    fn default() -> Self {
        Self {
            default_limit: 1000,
        }
    }
}

impl ServingTuning {
    pub fn overlay_env(&mut self, vars: &HashMap<String, String>) -> Result<(), ConfigError> {
        overlay_opt(vars, "LOOM_SERVING_DEFAULT_LIMIT", &mut self.default_limit)?;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.default_limit == 0 {
            return Err(invalid("LOOM_SERVING_DEFAULT_LIMIT", "must be >= 1"));
        }
        Ok(())
    }
}

/// The query-api binary's composed config: serving tuning. `#[serde(default)]`
/// so a partial config file deserializes (omitted domains fall to their `Default`). Loaded
/// via `loom_config::load` (defaults < file < env) through the `LayeredConfig` impl below.
#[derive(Default, serde::Deserialize)]
#[serde(default)]
pub struct QueryApiConfig {
    pub serving: ServingTuning,
}

impl loom_config::LayeredConfig for QueryApiConfig {
    fn overlay_env(&mut self, env: &HashMap<String, String>) -> Result<(), ConfigError> {
        self.serving.overlay_env(env)?;
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        self.serving.validate()?;
        Ok(())
    }
}
