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
        max_connections: None,
    };
    assert_eq!(tcp.pg_url(), "postgres://loom:secret@db.internal:5432/loom");

    // A non-default port must survive into the socket URL: libpq/sqlx derive the
    // socket filename `.s.PGSQL.<port>` from it, so dropping the port silently probes
    // the default 5432 and misses a cluster listening elsewhere.
    let socket = DbConfig {
        host: "/var/run/postgresql".into(),
        port: 54398,
        user: "loom".into(),
        password: "secret".into(),
        dbname: "loom".into(),
        max_connections: None,
    };
    assert_eq!(
        socket.pg_url(),
        "postgres://loom:secret@localhost:54398/loom?host=/var/run/postgresql"
    );
}

#[test]
fn pg_connect_options_honor_port_on_both_branches() {
    let tcp = DbConfig {
        host: "db.internal".into(),
        port: 6001,
        user: "loom".into(),
        password: "secret".into(),
        dbname: "loom".into(),
        max_connections: None,
    };
    let tcp_opts = tcp.pg_connect_options();
    assert_eq!(tcp_opts.get_port(), 6001);
    assert_eq!(tcp_opts.get_host(), "db.internal");

    // The socket branch must carry the port too — it selects the `.s.PGSQL.<port>`
    // socket file. Without it sqlx defaults to 5432 regardless of `port`.
    let socket = DbConfig {
        host: "/var/run/postgresql".into(),
        port: 54398,
        user: "loom".into(),
        password: "secret".into(),
        dbname: "loom".into(),
        max_connections: None,
    };
    let sock_opts = socket.pg_connect_options();
    assert_eq!(sock_opts.get_port(), 54398);
    assert_eq!(
        sock_opts
            .get_socket()
            .map(|p| p.to_string_lossy().into_owned()),
        Some("/var/run/postgresql".to_string())
    );
}

#[test]
fn max_connections_absent_defaults_none() {
    let cfg = Config::from_map(&full()).unwrap();
    assert_eq!(cfg.db.max_connections, None);
}

#[test]
fn max_connections_parsed() {
    let mut vars = full();
    vars.insert("LOOM_DB_MAX_CONNECTIONS".into(), "12".into());
    let cfg = Config::from_map(&vars).unwrap();
    assert_eq!(cfg.db.max_connections, Some(12));
}

#[test]
fn max_connections_malformed_is_error() {
    let mut vars = full();
    vars.insert("LOOM_DB_MAX_CONNECTIONS".into(), "lots".into());
    let err = Config::from_map(&vars).unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { ref var, .. }
        if var == "LOOM_DB_MAX_CONNECTIONS"));
}

#[test]
fn migrate_on_boot_defaults_false() {
    assert!(!Config::from_map(&full()).unwrap().migrate_on_boot);
}

#[test]
fn migrate_on_boot_true_parses() {
    let mut vars = full();
    vars.insert("LOOM_DB_MIGRATE_ON_BOOT".into(), "true".into());
    assert!(Config::from_map(&vars).unwrap().migrate_on_boot);
}

#[test]
fn migrate_on_boot_false_parses() {
    let mut vars = full();
    vars.insert("LOOM_DB_MIGRATE_ON_BOOT".into(), "false".into());
    assert!(!Config::from_map(&vars).unwrap().migrate_on_boot);
}

#[test]
fn migrate_on_boot_invalid_rejected() {
    let mut vars = full();
    vars.insert("LOOM_DB_MIGRATE_ON_BOOT".into(), "yes".into());
    assert!(
        matches!(Config::from_map(&vars), Err(ConfigError::Invalid { ref var, .. })
        if var == "LOOM_DB_MIGRATE_ON_BOOT")
    );
}

#[test]
fn db_config_from_map_parses_discrete_fields() {
    let mut v = full();
    v.insert("LOOM_DB_MAX_CONNECTIONS".into(), "9".into());
    let db = DbConfig::from_map(&v).unwrap();
    assert_eq!(db.host, "db.internal");
    assert_eq!(db.port, 5432);
    assert_eq!(db.user, "loom");
    assert_eq!(db.password, "secret");
    assert_eq!(db.dbname, "loom");
    assert_eq!(db.max_connections, Some(9));
}

#[test]
fn parse_migrate_on_boot_keeps_from_map_semantics() {
    use service_runtime::parse_migrate_on_boot;
    assert!(!parse_migrate_on_boot(&full()).unwrap());
    let mut v = full();
    v.insert("LOOM_DB_MIGRATE_ON_BOOT".into(), "true".into());
    assert!(parse_migrate_on_boot(&v).unwrap());
    v.insert("LOOM_DB_MIGRATE_ON_BOOT".into(), "yes".into());
    assert!(matches!(parse_migrate_on_boot(&v),
        Err(ConfigError::Invalid { ref var, .. }) if var == "LOOM_DB_MIGRATE_ON_BOOT"));
}

#[test]
fn migrate_requested_reads_the_snapshot() {
    let mut v = full();
    assert!(!service_runtime::migrate_requested(&v));
    v.insert("LOOM_MIGRATE".into(), "apply".into());
    assert!(service_runtime::migrate_requested(&v));
    v.insert("LOOM_MIGRATE".into(), "yes".into());
    assert!(!service_runtime::migrate_requested(&v));
}
