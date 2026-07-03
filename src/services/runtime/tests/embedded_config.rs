use std::collections::HashMap;
use std::path::Path;

use service_runtime::{Config, EmbeddedSettings};

fn external_base() -> HashMap<String, String> {
    // The minimal external-mode keys Config::from_map requires.
    [
        ("LOOM_BIND_ADDR", "127.0.0.1:8080"),
        ("LOOM_DB_HOST", "/var/run/pg"),
        ("LOOM_DB_PORT", "5432"),
        ("LOOM_DB_USER", "postgres"),
        ("LOOM_DB_PASSWORD", ""),
        ("LOOM_DB_NAME", "loom"),
        ("LOOM_DATA_PATH", "/tmp/loomdata"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

/// Embedded mode with only the two keys embedded genuinely needs: mode + data path.
/// No LOOM_PG_BIN_DIR, no LOOM_DB_* — the regression this task fixes.
fn embedded_minimal() -> HashMap<String, String> {
    [
        ("LOOM_BIND_ADDR", "127.0.0.1:8080"),
        ("LOOM_DATA_PATH", "/tmp/loomdata"),
        ("LOOM_PG_MODE", "embedded"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

#[test]
fn external_mode_has_no_embedded_settings() {
    let cfg = Config::from_map(&external_base()).expect("parse external");
    assert!(cfg.embedded.is_none());
}

#[test]
fn embedded_without_pg_bin_dir_parses_with_bin_none() {
    // Today this fails at parse with MissingVar("LOOM_PG_BIN_DIR").
    let cfg = Config::from_map(&embedded_minimal()).expect("parse embedded, no PG bin");
    let e = cfg.embedded.expect("embedded settings present");
    assert!(e.bin.is_none(), "no LOOM_PG_BIN_DIR => bin is None");
    assert_eq!(e.data_dir, Path::new("/tmp/loomdata/pgdata"));
    assert_eq!(e.socket_dir, Path::new("/tmp/loomdata/pgrun"));
    assert_eq!(e.database, "loom");
}

#[test]
fn embedded_with_pg_bin_dir_populates_bin() {
    let mut vars = embedded_minimal();
    vars.insert("LOOM_PG_BIN_DIR".into(), "/opt/pg/bin".into());
    vars.insert("LOOM_PG_LD_LIBRARY_PATH".into(), "/opt/pg/lib".into());
    let cfg = Config::from_map(&vars).expect("parse embedded");
    let bin = cfg.embedded.expect("embedded").bin.expect("bin present");
    assert_eq!(bin.bin_dir, Path::new("/opt/pg/bin"));
    assert_eq!(bin.ld_library_path, "/opt/pg/lib");
}

#[test]
fn embedded_defaults_db_vars_from_data_path() {
    // No LOOM_DB_* at all: host defaults to the socket dir, user postgres, db loom.
    let cfg = Config::from_map(&embedded_minimal()).expect("parse embedded");
    assert_eq!(cfg.db.host, "/tmp/loomdata/pgrun");
    assert_eq!(cfg.db.port, 5432);
    assert_eq!(cfg.db.user, "postgres");
    assert_eq!(cfg.db.password, "");
    assert_eq!(cfg.db.dbname, "loom");
}

#[test]
fn embedded_db_name_override_wins_and_reaches_both_surfaces() {
    let mut vars = embedded_minimal();
    vars.insert("LOOM_DB_NAME".into(), "widgets".into());
    let cfg = Config::from_map(&vars).expect("parse embedded");
    assert_eq!(cfg.db.dbname, "widgets");
    // Consistent by construction: the embedded cluster's DB matches the pool target.
    assert_eq!(cfg.embedded.expect("embedded").database, "widgets");
}

#[test]
fn external_mode_still_requires_db_vars() {
    let mut vars = external_base();
    vars.remove("LOOM_DB_HOST");
    let err = Config::from_map(&vars).expect_err("external still requires LOOM_DB_HOST");
    assert!(
        matches!(err, service_runtime::ConfigError::MissingVar(ref k) if k == "LOOM_DB_HOST"),
        "got {err:?}"
    );
}

#[test]
fn embedded_settings_from_map_is_none_in_external_mode() {
    let s =
        EmbeddedSettings::from_map(&external_base(), Path::new("/tmp/loomdata")).expect("parse");
    assert!(s.is_none());
}

#[test]
fn embedded_settings_from_map_derives_dirs_and_optional_bin() {
    let e = EmbeddedSettings::from_map(&embedded_minimal(), Path::new("/tmp/loomdata"))
        .expect("parse")
        .expect("embedded settings present");
    assert_eq!(e.data_dir, Path::new("/tmp/loomdata/pgdata"));
    assert_eq!(e.socket_dir, Path::new("/tmp/loomdata/pgrun"));
    assert_eq!(e.database, "loom");
    assert!(e.bin.is_none());
}
