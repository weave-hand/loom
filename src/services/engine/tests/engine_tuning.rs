//! EngineTuning::from_map: the write-path byte thresholds parsed from the main's
//! env snapshot (was: live-env parse_env_or inside engine::run, violating the
//! one-env-snapshot-per-main rule).
use std::collections::HashMap;
use std::time::Duration;

use engine::EngineTuning;

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn defaults_are_16_mib_inline_and_64_mib_flush() {
    let t = EngineTuning::from_map(&map(&[])).unwrap();
    assert_eq!(t.inline_byte_limit, 16 * 1024 * 1024);
    assert_eq!(t.flush_byte_threshold, 64 * 1024 * 1024);
    assert_eq!(t.consolidate_delta_threshold, 128);
    assert_eq!(t.scheduler_tick, Duration::from_secs(5));
}

#[test]
fn overrides_parse() {
    let t = EngineTuning::from_map(&map(&[
        ("LOOM_INLINE_BYTE_LIMIT", "1024"),
        ("LOOM_FLUSH_BYTE_THRESHOLD", "2048"),
        ("LOOM_CONSOLIDATE_DELTA_THRESHOLD", "7"),
        ("LOOM_SCHEDULER_TICK_SECS", "30"),
    ]))
    .unwrap();
    assert_eq!(t.inline_byte_limit, 1024);
    assert_eq!(t.flush_byte_threshold, 2048);
    assert_eq!(t.consolidate_delta_threshold, 7);
    assert_eq!(t.scheduler_tick, Duration::from_secs(30));
}

#[test]
fn malformed_value_is_startup_error_naming_key() {
    let err = EngineTuning::from_map(&map(&[("LOOM_INLINE_BYTE_LIMIT", "lots")])).unwrap_err();
    assert!(
        matches!(err, service_runtime::ConfigError::Invalid { ref var, .. }
        if var == "LOOM_INLINE_BYTE_LIMIT")
    );
}

#[test]
fn compact_defaults_are_128_mib_and_8_files() {
    let t = EngineTuning::from_map(&map(&[])).unwrap();
    assert_eq!(t.compact_small_file_bytes, 128 * 1024 * 1024);
    assert_eq!(t.compact_trigger_files, 8);
}

#[test]
fn compact_overrides_parse() {
    let t = EngineTuning::from_map(&map(&[
        ("LOOM_COMPACT_THRESHOLD_BYTES", "4096"),
        ("LOOM_COMPACT_TRIGGER_FILES", "16"),
    ]))
    .unwrap();
    assert_eq!(t.compact_small_file_bytes, 4096);
    assert_eq!(t.compact_trigger_files, 16);
}

#[test]
fn compact_trigger_files_zero_disables_and_parses_ok() {
    let t = EngineTuning::from_map(&map(&[("LOOM_COMPACT_TRIGGER_FILES", "0")])).unwrap();
    assert_eq!(t.compact_trigger_files, 0);
}

#[test]
fn compact_trigger_files_one_is_rejected() {
    let err = EngineTuning::from_map(&map(&[("LOOM_COMPACT_TRIGGER_FILES", "1")])).unwrap_err();
    assert!(
        matches!(err, service_runtime::ConfigError::Invalid { ref var, .. }
        if var == "LOOM_COMPACT_TRIGGER_FILES")
    );
}

#[test]
fn compact_trigger_files_negative_is_rejected() {
    let err = EngineTuning::from_map(&map(&[("LOOM_COMPACT_TRIGGER_FILES", "-1")])).unwrap_err();
    assert!(
        matches!(err, service_runtime::ConfigError::Invalid { ref var, .. }
        if var == "LOOM_COMPACT_TRIGGER_FILES")
    );
}

#[test]
fn compact_small_file_bytes_zero_is_rejected() {
    let err = EngineTuning::from_map(&map(&[("LOOM_COMPACT_THRESHOLD_BYTES", "0")])).unwrap_err();
    assert!(
        matches!(err, service_runtime::ConfigError::Invalid { ref var, .. }
        if var == "LOOM_COMPACT_THRESHOLD_BYTES")
    );
}

#[test]
fn compact_small_file_bytes_negative_is_rejected() {
    let err = EngineTuning::from_map(&map(&[("LOOM_COMPACT_THRESHOLD_BYTES", "-1")])).unwrap_err();
    assert!(
        matches!(err, service_runtime::ConfigError::Invalid { ref var, .. }
        if var == "LOOM_COMPACT_THRESHOLD_BYTES")
    );
}

#[test]
fn sql_limit_defaults_are_1_gib_and_60_secs() {
    let t = EngineTuning::from_map(&map(&[])).unwrap();
    assert_eq!(t.sql_memory_limit_bytes, 1024 * 1024 * 1024);
    assert_eq!(t.sql_timeout_secs, 60);
    let l = t.governed_sql_limits();
    assert_eq!(l.memory_bytes, Some(1024 * 1024 * 1024));
    assert_eq!(l.deadline, Some(Duration::from_secs(60)));
    assert_eq!(t.sql_max_concurrent, 16);
    assert_eq!(t.sql_admission_wait_secs, 5);
}

#[test]
fn sql_limit_overrides_parse() {
    let t = EngineTuning::from_map(&map(&[
        ("LOOM_SQL_MEMORY_LIMIT_BYTES", "4096"),
        ("LOOM_SQL_TIMEOUT_SECS", "7"),
    ]))
    .unwrap();
    assert_eq!(t.sql_memory_limit_bytes, 4096);
    assert_eq!(t.sql_timeout_secs, 7);
    let l = t.governed_sql_limits();
    assert_eq!(l.memory_bytes, Some(4096));
    assert_eq!(l.deadline, Some(Duration::from_secs(7)));
}

#[test]
fn sql_admission_overrides_parse() {
    let t = EngineTuning::from_map(&map(&[
        ("LOOM_SQL_MAX_CONCURRENT", "3"),
        ("LOOM_SQL_ADMISSION_WAIT_SECS", "9"),
    ]))
    .unwrap();
    assert_eq!(t.sql_max_concurrent, 3);
    assert_eq!(t.sql_admission_wait_secs, 9);
}

#[test]
fn sql_admission_zero_disables_the_cap() {
    let t = EngineTuning::from_map(&map(&[("LOOM_SQL_MAX_CONCURRENT", "0")])).unwrap();
    assert_eq!(t.sql_max_concurrent, 0);
}

#[test]
fn zero_admission_wait_parses_as_immediate_rejection() {
    let t = EngineTuning::from_map(&map(&[("LOOM_SQL_ADMISSION_WAIT_SECS", "0")])).unwrap();
    assert_eq!(t.sql_admission_wait_secs, 0);
}

#[test]
fn malformed_sql_admission_limit_is_startup_error_naming_key() {
    let err = EngineTuning::from_map(&map(&[("LOOM_SQL_MAX_CONCURRENT", "many")])).unwrap_err();
    assert!(matches!(err, service_runtime::ConfigError::Invalid { ref var, .. } if var == "LOOM_SQL_MAX_CONCURRENT"));
}

#[test]
fn sql_admission_limit_rejects_values_above_tokio_maximum() {
    let max = tokio::sync::Semaphore::MAX_PERMITS;
    let max_value = max.to_string();
    assert!(
        EngineTuning::from_map(&map(&[("LOOM_SQL_MAX_CONCURRENT", &max_value)])).is_ok()
    );
    let too_large = max + 1;
    let too_large_value = too_large.to_string();
    let err = EngineTuning::from_map(&map(&[("LOOM_SQL_MAX_CONCURRENT", &too_large_value)]))
        .unwrap_err();
    assert!(matches!(err, service_runtime::ConfigError::Invalid { ref var, .. } if var == "LOOM_SQL_MAX_CONCURRENT"));
}

#[test]
fn zero_is_the_documented_unbounded_escape_hatch() {
    let t = EngineTuning::from_map(&map(&[
        ("LOOM_SQL_MEMORY_LIMIT_BYTES", "0"),
        ("LOOM_SQL_TIMEOUT_SECS", "0"),
    ]))
    .unwrap();
    assert_eq!(
        t.governed_sql_limits(),
        engine_serving::GovernedSqlLimits::unbounded()
    );
}

#[test]
fn malformed_sql_memory_limit_is_startup_error_naming_key() {
    let err = EngineTuning::from_map(&map(&[("LOOM_SQL_MEMORY_LIMIT_BYTES", "lots")])).unwrap_err();
    assert!(
        matches!(err, service_runtime::ConfigError::Invalid { ref var, .. }
        if var == "LOOM_SQL_MEMORY_LIMIT_BYTES")
    );
}

#[test]
fn malformed_sql_timeout_is_startup_error_naming_key() {
    let err = EngineTuning::from_map(&map(&[("LOOM_SQL_TIMEOUT_SECS", "soon")])).unwrap_err();
    assert!(
        matches!(err, service_runtime::ConfigError::Invalid { ref var, .. }
        if var == "LOOM_SQL_TIMEOUT_SECS")
    );
}
