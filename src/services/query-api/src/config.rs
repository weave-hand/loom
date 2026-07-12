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

/// External Flight SQL wire tuning. `bind_addr` unset => the listener does not
/// start (opt-in, like the Flight export). Both external wires are typed
/// siblings on the layered config seam (see `FlightExportTuning`).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct SqlWireTuning {
    pub bind_addr: Option<String>,
    pub max_rows: u32,
}

impl Default for SqlWireTuning {
    fn default() -> Self {
        Self {
            bind_addr: None,
            max_rows: 1_000_000,
        }
    }
}

impl SqlWireTuning {
    pub fn overlay_env(&mut self, vars: &HashMap<String, String>) -> Result<(), ConfigError> {
        if let Some(v) = vars.get("LOOM_SQL_WIRE_BIND_ADDR") {
            self.bind_addr = Some(v.clone());
        }
        overlay_opt(vars, "LOOM_SQL_WIRE_MAX_ROWS", &mut self.max_rows)?;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(addr) = &self.bind_addr {
            addr.parse::<std::net::SocketAddr>()
                .map_err(|e| invalid("LOOM_SQL_WIRE_BIND_ADDR", e))?;
        }
        if self.max_rows == 0 {
            return Err(invalid("LOOM_SQL_WIRE_MAX_ROWS", "must be >= 1"));
        }
        Ok(())
    }
}

/// External Arrow Flight export listener tuning. `bind_addr` unset => the export
/// listener does not start (opt-in, like the SQL wire). A verbatim sibling of
/// `SqlWireTuning`; env names (`LOOM_FLIGHT_BIND_ADDR`, `LOOM_EXPORT_MAX_ROWS`)
/// are unchanged from the pre-seam raw reads (road-flight-export-config-seam).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct FlightExportTuning {
    pub bind_addr: Option<String>,
    pub max_rows: u32,
}

impl Default for FlightExportTuning {
    fn default() -> Self {
        Self {
            bind_addr: None,
            max_rows: 1_000_000,
        }
    }
}

impl FlightExportTuning {
    pub fn overlay_env(&mut self, vars: &HashMap<String, String>) -> Result<(), ConfigError> {
        if let Some(v) = vars.get("LOOM_FLIGHT_BIND_ADDR") {
            self.bind_addr = Some(v.clone());
        }
        overlay_opt(vars, "LOOM_EXPORT_MAX_ROWS", &mut self.max_rows)?;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(addr) = &self.bind_addr {
            addr.parse::<std::net::SocketAddr>()
                .map_err(|e| invalid("LOOM_FLIGHT_BIND_ADDR", e))?;
        }
        if self.max_rows == 0 {
            return Err(invalid("LOOM_EXPORT_MAX_ROWS", "must be >= 1"));
        }
        Ok(())
    }
}

/// The query-api binary's composed config: serving tuning + the two external-wire
/// tunings (SQL wire + Flight export). `#[serde(default)]` so a partial config file deserializes (omitted
/// domains fall to their `Default`). Loaded via `loom_config::load` (defaults <
/// file < env) through the `LayeredConfig` impl below.
#[derive(Default, serde::Deserialize)]
#[serde(default)]
pub struct QueryApiConfig {
    pub serving: ServingTuning,
    pub sql_wire: SqlWireTuning,
    pub flight_export: FlightExportTuning,
}

impl loom_config::LayeredConfig for QueryApiConfig {
    fn overlay_env(&mut self, env: &HashMap<String, String>) -> Result<(), ConfigError> {
        self.serving.overlay_env(env)?;
        self.sql_wire.overlay_env(env)?;
        self.flight_export.overlay_env(env)?;
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        self.serving.validate()?;
        self.sql_wire.validate()?;
        self.flight_export.validate()?;
        Ok(())
    }
}
