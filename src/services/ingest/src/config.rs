//! Routing tuning shared by ingest and query-api: the in-memory byte thresholds that
//! decide inline-vs-Parquet landing and when to enqueue a flush. Co-located with the
//! landing materializer that consumes them; query-api reads this via its ingest dep.

use std::collections::HashMap;

use loom_config::{ConfigError, invalid, overlay_opt};

/// The ingest binary's composed config: routing + write tuning. `#[serde(default)]` so a
/// partial config file deserializes (omitted domains fall to their `Default`). Loaded via
/// `loom_config::load` (defaults < file < env) through the `LayeredConfig` impl below.
#[derive(Default, serde::Deserialize)]
#[serde(default)]
pub struct IngestConfig {
    pub routing: RoutingTuning,
    pub write: datafusion_io::WriteConfig,
}

impl loom_config::LayeredConfig for IngestConfig {
    fn overlay_env(&mut self, env: &HashMap<String, String>) -> Result<(), ConfigError> {
        self.routing.overlay_env(env)?;
        self.write.overlay_env(env)?;
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        self.routing.validate()?;
        self.write.validate()?;
        Ok(())
    }
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
    /// HTTP request-body cap (bytes) on the landing endpoints. Must exceed
    /// `inline_byte_limit`, or the real-Parquet branch (taken above that limit)
    /// is unreachable over HTTP: axum's stock 2 MB default did exactly that.
    pub http_max_body_bytes: usize,
    /// Accumulated CDC delta-row count (per declared stream table) at/above which
    /// a `stream_consolidate` job is enqueued.
    pub consolidate_delta_threshold: i64,
    /// Small-file cutoff for the compaction auto-trigger — the same env the
    /// worker's `small_files` selection reads, so one deploy value governs both
    /// (`LOOM_COMPACT_THRESHOLD_BYTES`, default 128 MiB).
    pub compact_small_file_bytes: i64,
    /// Number of small files (at/above `compact_small_file_bytes`) that must
    /// accumulate before a compaction job is auto-enqueued
    /// (`LOOM_COMPACT_TRIGGER_FILES`, default 8). `0` disables the trigger;
    /// `1` is rejected at startup (would re-enqueue immediately after every
    /// compaction whose output stays under the cutoff); `>= 2` enables.
    pub compact_trigger_files: i64,
}

impl Default for RoutingTuning {
    fn default() -> Self {
        Self {
            inline_byte_limit: 16 * 1024 * 1024,
            flush_byte_threshold: 64 * 1024 * 1024,
            http_max_body_bytes: 64 * 1024 * 1024,
            consolidate_delta_threshold: 128,
            compact_small_file_bytes: 128 * 1024 * 1024,
            compact_trigger_files: 8,
        }
    }
}

impl RoutingTuning {
    /// Apply `LOOM_INLINE_BYTE_LIMIT` / `LOOM_FLUSH_BYTE_THRESHOLD` /
    /// `LOOM_HTTP_MAX_BODY_BYTES` / `LOOM_CONSOLIDATE_DELTA_THRESHOLD` /
    /// `LOOM_COMPACT_THRESHOLD_BYTES` / `LOOM_COMPACT_TRIGGER_FILES` over the
    /// current values.
    pub fn overlay_env(&mut self, vars: &HashMap<String, String>) -> Result<(), ConfigError> {
        overlay_opt(vars, "LOOM_INLINE_BYTE_LIMIT", &mut self.inline_byte_limit)?;
        overlay_opt(
            vars,
            "LOOM_FLUSH_BYTE_THRESHOLD",
            &mut self.flush_byte_threshold,
        )?;
        overlay_opt(
            vars,
            "LOOM_HTTP_MAX_BODY_BYTES",
            &mut self.http_max_body_bytes,
        )?;
        overlay_opt(
            vars,
            "LOOM_CONSOLIDATE_DELTA_THRESHOLD",
            &mut self.consolidate_delta_threshold,
        )?;
        overlay_opt(
            vars,
            "LOOM_COMPACT_THRESHOLD_BYTES",
            &mut self.compact_small_file_bytes,
        )?;
        overlay_opt(
            vars,
            "LOOM_COMPACT_TRIGGER_FILES",
            &mut self.compact_trigger_files,
        )?;
        Ok(())
    }

    /// Validate: thresholds must be positive, and the HTTP body cap must exceed
    /// the inline limit or the Parquet landing branch is unreachable over HTTP.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.inline_byte_limit == 0 {
            return Err(invalid("LOOM_INLINE_BYTE_LIMIT", "must be >= 1"));
        }
        if self.flush_byte_threshold <= 0 {
            return Err(invalid("LOOM_FLUSH_BYTE_THRESHOLD", "must be >= 1"));
        }
        if self.http_max_body_bytes <= self.inline_byte_limit {
            return Err(invalid(
                "LOOM_HTTP_MAX_BODY_BYTES",
                "must exceed LOOM_INLINE_BYTE_LIMIT, or the Parquet landing branch is unreachable over HTTP",
            ));
        }
        if self.consolidate_delta_threshold < 1 {
            return Err(invalid("LOOM_CONSOLIDATE_DELTA_THRESHOLD", "must be >= 1"));
        }
        if self.compact_small_file_bytes <= 0 {
            return Err(invalid("LOOM_COMPACT_THRESHOLD_BYTES", "must be >= 1"));
        }
        if self.compact_trigger_files == 1 || self.compact_trigger_files < 0 {
            return Err(invalid("LOOM_COMPACT_TRIGGER_FILES", "must be 0 or >= 2"));
        }
        Ok(())
    }
}
