//! Routing tuning shared by ingest and query-api: the in-memory byte thresholds that
//! decide inline-vs-Parquet landing and when to enqueue a flush. Co-located with the
//! landing materializer that consumes them; query-api reads this via its ingest dep.

use std::collections::HashMap;

use loom_config::{ConfigError, invalid, overlay_opt};

/// The ingest binary's composed config: routing + write tuning. `#[serde(default)]` so a
/// partial config file deserializes (omitted domains fall to their `Default`).
#[derive(Default, serde::Deserialize)]
#[serde(default)]
pub struct IngestConfig {
    pub routing: RoutingTuning,
    pub write: datafusion_io::WriteConfig,
}

/// Inline/flush byte routing knobs.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct RoutingTuning {
    /// In-memory (uncompressed) Arrow byte size at/below which a request inlines
    /// (mirror-only rows) instead of writing real Parquet.
    pub inline_byte_limit: usize,
    /// Live-inline-byte total at/above which a `flush_table` job is enqueued.
    pub flush_byte_threshold: i64,
}

impl Default for RoutingTuning {
    fn default() -> Self {
        Self {
            inline_byte_limit: 16 * 1024 * 1024,
            flush_byte_threshold: 64 * 1024 * 1024,
        }
    }
}

impl RoutingTuning {
    /// Apply `LOOM_INLINE_BYTE_LIMIT` / `LOOM_FLUSH_BYTE_THRESHOLD` over the current values.
    pub fn overlay_env(&mut self, vars: &HashMap<String, String>) -> Result<(), ConfigError> {
        overlay_opt(vars, "LOOM_INLINE_BYTE_LIMIT", &mut self.inline_byte_limit)?;
        overlay_opt(
            vars,
            "LOOM_FLUSH_BYTE_THRESHOLD",
            &mut self.flush_byte_threshold,
        )?;
        Ok(())
    }

    /// Validate: both thresholds must be positive.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.inline_byte_limit == 0 {
            return Err(invalid("LOOM_INLINE_BYTE_LIMIT", "must be >= 1"));
        }
        if self.flush_byte_threshold <= 0 {
            return Err(invalid("LOOM_FLUSH_BYTE_THRESHOLD", "must be >= 1"));
        }
        Ok(())
    }
}
