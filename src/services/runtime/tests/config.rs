use std::collections::HashMap;
use std::time::Duration;

use service_runtime::{Config, ConfigError, DbConfig};

fn full() -> HashMap<String, String> {
    [
        ("LOOM_BIND_ADDR", "0.0.0.0:8080"),
        ("LOOM_DB_HOST", "db.internal"),
        ("LOOM_DB_PORT", "5432"),
        ("LOOM_DB_USER", "loom"),
        ("LOOM_DB_PASSWORD", "secret"),
        ("LOOM_DB_NAME", "loom"),
        ("LOOM_DATA_PATH", "/var/loom/data"),
        ("LOOM_LOCK_TIMEOUT_MS", "750"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

#[test]
fn parses_a_full_config() {
    let cfg = Config::from_map(&full()).expect("parse");
    assert_eq!(cfg.bind_addr, "0.0.0.0:8080".parse().unwrap());
    assert_eq!(cfg.db.host, "db.internal");
    assert_eq!(cfg.db.port, 5432);
    assert_eq!(cfg.db.user, "loom");
    assert_eq!(cfg.db.password, "secret");
    assert_eq!(cfg.db.dbname, "loom");
    assert_eq!(cfg.data_path, std::path::PathBuf::from("/var/loom/data"));
    assert_eq!(cfg.lock_timeout, Duration::from_millis(750));
}

#[test]
fn lock_timeout_defaults_to_5000ms() {
    let mut v = full();
    v.remove("LOOM_LOCK_TIMEOUT_MS");
    assert_eq!(
        Config::from_map(&v).unwrap().lock_timeout,
        Duration::from_millis(5000)
    );
}

#[test]
fn gc_retention_defaults_to_seven_days_and_parses_override() {
    let mut v = full();
    v.remove("LOOM_GC_RETENTION_SECS");
    assert_eq!(
        Config::from_map(&v).unwrap().gc_retention,
        Duration::from_secs(7 * 24 * 3600)
    );
    v.insert("LOOM_GC_RETENTION_SECS".into(), "60".into());
    assert_eq!(
        Config::from_map(&v).unwrap().gc_retention,
        Duration::from_secs(60)
    );
}

#[test]
fn missing_required_var_errors() {
    let mut v = full();
    v.remove("LOOM_DB_HOST");
    assert!(matches!(Config::from_map(&v), Err(ConfigError::MissingVar(k)) if k == "LOOM_DB_HOST"));
}

#[test]
fn invalid_values_error() {
    let mut bad_addr = full();
    bad_addr.insert("LOOM_BIND_ADDR".into(), "not-an-addr".into());
    assert!(matches!(
        Config::from_map(&bad_addr),
        Err(ConfigError::Invalid { .. })
    ));

    let mut bad_port = full();
    bad_port.insert("LOOM_DB_PORT".into(), "99999999".into());
    assert!(matches!(
        Config::from_map(&bad_port),
        Err(ConfigError::Invalid { .. })
    ));
}

#[test]
fn pg_url_tcp_and_socket() {
    let tcp = DbConfig {
        host: "db.internal".into(),
        port: 5432,
        user: "loom".into(),
        password: "secret".into(),
        dbname: "loom".into(),
    };
    assert_eq!(tcp.pg_url(), "postgres://loom:secret@db.internal:5432/loom");

    let socket = DbConfig {
        host: "/var/run/postgresql".into(),
        port: 5432,
        user: "loom".into(),
        password: "secret".into(),
        dbname: "loom".into(),
    };
    assert_eq!(
        socket.pg_url(),
        "postgres://loom:secret@localhost/loom?host=/var/run/postgresql"
    );
}
