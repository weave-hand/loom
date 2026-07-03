//! Composed config for the job-processing service binaries (worker, transform):
//! worker tuning + Parquet write config, loaded via `loom_config::load` as
//! defaults < file < env through the `LayeredConfig` impl below. Lives here (not
//! `loom-config`) because `write` is this crate's `WriteConfig` and `datafusion-io`
//! already depends on `loom-config` — the reverse edge would cycle.
use crate::WriteConfig;

#[derive(Default, serde::Deserialize)]
#[serde(default)]
pub struct JobConfig {
    pub worker: loom_config::WorkerTuning,
    pub write: WriteConfig,
}

impl loom_config::LayeredConfig for JobConfig {
    fn overlay_env(
        &mut self,
        env: &std::collections::HashMap<String, String>,
    ) -> Result<(), loom_config::ConfigError> {
        self.worker.overlay_env(env)?;
        self.write.overlay_env(env)?;
        Ok(())
    }

    fn validate(&self) -> Result<(), loom_config::ConfigError> {
        self.worker.validate()?;
        self.write.validate()?;
        Ok(())
    }
}
