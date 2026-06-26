//! Config behaviour of `WriteConfig` (the write-tuning seam struct).
use std::collections::HashMap;

use datafusion_io::WriteConfig;

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
}

#[test]
fn defaults_match_constants() {
    let c = WriteConfig::default();
    assert_eq!(c.target_file_size_bytes, 128 * 1024 * 1024);
    assert_eq!(c.max_files, 64);
    assert!((c.compression_factor - 0.3).abs() < 1e-9);
}

#[test]
fn partial_json_falls_to_default() {
    let c: WriteConfig = serde_json::from_str(r#"{"max_files": 8}"#).unwrap();
    assert_eq!(c.max_files, 8);
    assert_eq!(c.target_file_size_bytes, 128 * 1024 * 1024);
}

#[test]
fn env_overlay_applies() {
    let mut c = WriteConfig::default();
    c.overlay_env(&map(&[("LOOM_WRITE_MAX_FILES", "10")])).unwrap();
    assert_eq!(c.max_files, 10);
}

#[test]
fn env_overrides_file_value() {
    let mut c: WriteConfig = serde_json::from_str(r#"{"max_files": 8}"#).unwrap();
    c.overlay_env(&map(&[("LOOM_WRITE_MAX_FILES", "10")])).unwrap();
    assert_eq!(c.max_files, 10, "env wins over file");
}

#[test]
fn file_value_survives_when_env_silent() {
    let mut c: WriteConfig = serde_json::from_str(r#"{"max_files": 8}"#).unwrap();
    c.overlay_env(&map(&[])).unwrap();
    assert_eq!(c.max_files, 8, "file value survives");
}

#[test]
fn malformed_env_is_error_naming_key() {
    let mut c = WriteConfig::default();
    let err = c.overlay_env(&map(&[("LOOM_WRITE_MAX_FILES", "lots")])).unwrap_err();
    assert!(format!("{err}").contains("LOOM_WRITE_MAX_FILES"));
}

#[test]
fn validate_rejects_out_of_range_compression() {
    let c: WriteConfig = serde_json::from_str(r#"{"compression_factor": 1.5}"#).unwrap();
    assert!(c.validate().is_err());
    let c0: WriteConfig = serde_json::from_str(r#"{"compression_factor": 0.0}"#).unwrap();
    assert!(c0.validate().is_err());
}

#[test]
fn validate_rejects_zero_max_files() {
    let c: WriteConfig = serde_json::from_str(r#"{"max_files": 0}"#).unwrap();
    assert!(c.validate().is_err());
}

#[test]
fn validate_accepts_defaults() {
    assert!(WriteConfig::default().validate().is_ok());
}
