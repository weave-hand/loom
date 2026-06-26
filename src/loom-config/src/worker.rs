//! Worker-domain tuning: the polling fallback interval and the retry-backoff bounds.
//! Lives here (not in `control_plane_worker` or `service_runtime`) so the zero-postgres
//! `worker-bin` and the `control_plane_worker`/`transform` libs can all consume it.

use std::collections::HashMap;
use std::time::Duration;

use crate::{ConfigError, overlay_opt};

/// Tuning for the queue worker loop. Stored as raw unit fields (serde-friendly);
/// accessors return `Duration`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct WorkerTuning {
    /// Notification-miss fallback poll interval, in milliseconds. Default 5000.
    pub poll_interval_ms: u64,
    /// Exponential-backoff ceiling, in seconds. Default 60. Override: `LOOM_WORKER_BACKOFF_CEILING_SECS`.
    pub backoff_ceiling_secs: u64,
    /// Exponential-backoff iteration cap (attempts are clamped to this). Default 6.
    pub backoff_max_attempts: u32,
}

impl Default for WorkerTuning {
    fn default() -> Self {
        Self {
            poll_interval_ms: 5000,
            backoff_ceiling_secs: 60,
            backoff_max_attempts: 6,
        }
    }
}

impl WorkerTuning {
    /// The polling fallback interval.
    #[must_use]
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }

    /// Capped exponential backoff: `min(1s << clamp(attempts, 0, max_attempts), ceiling)`.
    /// The two spec knobs map cleanly: `backoff_max_attempts` is the shift clamp (the
    /// "iteration cap" — how many doublings), `backoff_ceiling_secs` is the value cap (the
    /// "exp-backoff cap" — the `.min`). The original worker formula used the literals `6`
    /// and `60` for these independently; at defaults (6 / 60) this is byte-identical
    /// (`[1,2,4,8,16,32,60,60,…]`). Coupling the shift to `backoff_max_attempts` is the
    /// deliberate redesign that makes the knob meaningful (reviewer note, blocking fix #1).
    #[must_use]
    pub fn backoff(&self, attempts: i32) -> Duration {
        let shift = attempts.clamp(0, self.backoff_max_attempts.min(63) as i32) as u32;
        let secs = 1u64
            .checked_shl(shift)
            .unwrap_or(u64::MAX)
            .min(self.backoff_ceiling_secs);
        Duration::from_secs(secs)
    }

    /// Apply any present `LOOM_WORKER_*` vars over the current values.
    pub fn overlay_env(&mut self, vars: &HashMap<String, String>) -> Result<(), ConfigError> {
        overlay_opt(
            vars,
            "LOOM_WORKER_POLL_INTERVAL_MS",
            &mut self.poll_interval_ms,
        )?;
        overlay_opt(
            vars,
            "LOOM_WORKER_BACKOFF_CEILING_SECS",
            &mut self.backoff_ceiling_secs,
        )?;
        overlay_opt(
            vars,
            "LOOM_WORKER_BACKOFF_MAX_ATTEMPTS",
            &mut self.backoff_max_attempts,
        )?;
        Ok(())
    }

    /// Validate bounds. `poll_interval_ms >= 1`, `backoff_max_attempts >= 1`.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.poll_interval_ms == 0 {
            return Err(crate::invalid(
                "LOOM_WORKER_POLL_INTERVAL_MS",
                "must be >= 1",
            ));
        }
        if self.backoff_max_attempts == 0 {
            return Err(crate::invalid(
                "LOOM_WORKER_BACKOFF_MAX_ATTEMPTS",
                "must be >= 1",
            ));
        }
        Ok(())
    }
}
