//! Unit tests for the generic `load` / `LayeredConfig` composition seam.
use std::collections::HashMap;

use loom_config::{ConfigError, LayeredConfig, invalid, load, overlay_opt};

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[derive(Debug, serde::Deserialize)]
#[serde(default)]
struct TestCfg {
    n: u64,
}

impl Default for TestCfg {
    fn default() -> Self {
        Self { n: 5 }
    }
}

impl LayeredConfig for TestCfg {
    fn overlay_env(&mut self, env: &HashMap<String, String>) -> Result<(), ConfigError> {
        overlay_opt(env, "LOOM_TEST_N", &mut self.n)
    }
    fn validate(&self) -> Result<(), ConfigError> {
        if self.n == 0 {
            return Err(invalid("LOOM_TEST_N", "must be >= 1"));
        }
        Ok(())
    }
}

#[test]
fn load_defaults_when_no_file_no_env() {
    let cfg: TestCfg = load(&map(&[])).unwrap();
    assert_eq!(cfg.n, 5);
}

#[test]
fn load_env_overlays_default() {
    let cfg: TestCfg = load(&map(&[("LOOM_TEST_N", "9")])).unwrap();
    assert_eq!(cfg.n, 9);
}

#[test]
fn load_validates_after_overlay() {
    let err = load::<TestCfg>(&map(&[("LOOM_TEST_N", "0")])).unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { ref var, .. } if var == "LOOM_TEST_N"));
}

#[test]
fn load_reads_file_then_env_wins() {
    use std::io::Write;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    write!(f, r#"{{"n": 3}}"#).unwrap();
    f.flush().unwrap();
    let path = f.path().to_str().unwrap().to_string();
    // File only: file value applies over the default.
    let from_file: TestCfg = load(&map(&[("LOOM_CONFIG_FILE", &path)])).unwrap();
    assert_eq!(from_file.n, 3, "file value applies over default");
    // File + env: env overlays the file (defaults < file < env).
    let env_wins: TestCfg = load(&map(&[("LOOM_CONFIG_FILE", &path), ("LOOM_TEST_N", "7")])).unwrap();
    assert_eq!(env_wins.n, 7, "env overlays file");
    // `f` drops at end of scope, removing the temp file — no manual cleanup.
}

#[test]
fn load_unreadable_file_is_error_naming_key() {
    let err = load::<TestCfg>(&map(&[(
        "LOOM_CONFIG_FILE",
        "/nonexistent/loom/config/path.json",
    )]))
    .unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { ref var, .. } if var == "LOOM_CONFIG_FILE"));
}
