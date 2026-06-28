use std::collections::HashMap;

use service_runtime::Config;

fn base() -> HashMap<String, String> {
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

#[test]
fn external_mode_has_no_embedded_settings() {
    let cfg = Config::from_map(&base()).expect("parse external");
    assert!(cfg.embedded.is_none());
}

#[test]
fn embedded_mode_derives_pgdata_and_socket_under_data_path() {
    let mut vars = base();
    vars.insert("LOOM_PG_MODE".into(), "embedded".into());
    vars.insert("LOOM_PG_BIN_DIR".into(), "/opt/pg/bin".into());
    vars.insert("LOOM_MIGRATIONS_DIR".into(), "/opt/loom/migrations".into());
    let cfg = Config::from_map(&vars).expect("parse embedded");
    let e = cfg.embedded.expect("embedded settings present");
    assert_eq!(e.cfg.bin_dir, std::path::PathBuf::from("/opt/pg/bin"));
    assert_eq!(e.cfg.data_dir, std::path::PathBuf::from("/tmp/loomdata/pgdata"));
    assert_eq!(e.cfg.socket_dir, std::path::PathBuf::from("/tmp/loomdata/pgrun"));
    assert_eq!(e.cfg.database, "loom");
    assert_eq!(e.migrations_dir, std::path::PathBuf::from("/opt/loom/migrations"));
}
