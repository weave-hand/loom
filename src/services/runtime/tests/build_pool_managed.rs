//! `build_pool_managed` moves the PG-binary requirement to the one path that
//! actually boots a cluster. An embedded Config whose `bin` is None (nobody
//! supplied LOOM_PG_BIN_DIR) fails with a config error naming the var — BEFORE
//! any spawn — not a panic or a pathless spawn failure. No live Postgres is
//! needed: the check short-circuits ahead of EmbeddedPg::start.

use std::collections::HashMap;

use service_runtime::{Config, ConfigError, RuntimeError, build_pool_managed};

fn embedded_no_bin() -> Config {
    let vars: HashMap<String, String> = [
        ("LOOM_BIND_ADDR", "127.0.0.1:8080"),
        ("LOOM_DATA_PATH", "/tmp/loom-nonexistent-spawn-test"),
        ("LOOM_PG_MODE", "embedded"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    Config::from_map(&vars).expect("embedded config parses without PG bin")
}

#[tokio::test]
async fn embedded_without_bin_fails_naming_the_pg_bin_var() {
    let cfg = embedded_no_bin();
    let err = build_pool_managed(&cfg)
        .await
        .expect_err("must fail without PG bin dir");
    assert!(
        matches!(
            err,
            RuntimeError::Config(ConfigError::Invalid { ref var, .. }) if var == "LOOM_PG_BIN_DIR"
        ),
        "expected Config(Invalid LOOM_PG_BIN_DIR), got {err:?}"
    );
    // The message explains it is only needed to boot the cluster.
    assert!(err.to_string().contains("LOOM_PG_BIN_DIR"), "{err}");
}
