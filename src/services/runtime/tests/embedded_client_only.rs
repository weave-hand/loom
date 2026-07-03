//! Acceptance 1 + 4 end-to-end: a client-only process (`loom create-admin`) in
//! embedded mode connects to an already-running cluster with NO LOOM_PG_BIN_DIR
//! and NO LOOM_DB_* vars — the defaults alone route it to the server's socket.
//! Server-role Config supplies LOOM_PG_BIN_DIR (from the fixture); the separate
//! client-role Config sets only mode + data path. Boots a real cluster, so it is
//! a `loom_fixture_test`.

use std::collections::HashMap;
use std::path::Path;

use control_plane_core::Auth;

fn server_config(data_path: &Path) -> service_runtime::Config {
    let bin_dir = std::env::var("POSTGRES_BIN_DIR").expect("POSTGRES_BIN_DIR");
    let ld = std::env::var("POSTGRES_LD_LIBRARY_PATH").unwrap_or_default();
    let vars: HashMap<String, String> = [
        ("LOOM_BIND_ADDR", "127.0.0.1:0".to_string()),
        ("LOOM_DATA_PATH", data_path.display().to_string()),
        ("LOOM_PG_MODE", "embedded".to_string()),
        ("LOOM_PG_BIN_DIR", bin_dir),
        ("LOOM_PG_LD_LIBRARY_PATH", ld),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    service_runtime::Config::from_map(&vars).expect("server config")
}

/// Client role: mode + data path only. No PG-bin, no LOOM_DB_*. Everything else
/// comes from the embedded defaults, which must point at the server's socket.
fn client_config(data_path: &Path) -> service_runtime::Config {
    let vars: HashMap<String, String> = [
        ("LOOM_BIND_ADDR", "127.0.0.1:0".to_string()),
        ("LOOM_DATA_PATH", data_path.display().to_string()),
        ("LOOM_PG_MODE", "embedded".to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    service_runtime::Config::from_map(&vars).expect("client config parses without PG bin")
}

#[tokio::test]
async fn embedded_client_only_connects_via_defaults_and_creates_admin() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path();

    // Boot the cluster in the server role (owns the PG binaries).
    let server_cfg = server_config(data);
    let (server_pool, embedded) = service_runtime::build_pool_managed(&server_cfg)
        .await
        .expect("boot embedded cluster");
    let embedded = embedded.expect("embedded handle present");

    // Client role: defaults must resolve to the same socket the server serves.
    let client_cfg = client_config(data);
    assert!(
        client_cfg
            .embedded
            .as_ref()
            .expect("embedded")
            .bin
            .is_none(),
        "client Config carries no PG-bin paths"
    );
    assert_eq!(
        client_cfg.db.host,
        data.join("pgrun").display().to_string(),
        "client host defaults to the server socket dir"
    );

    let client_pool = service_runtime::build_pool(&client_cfg.db)
        .await
        .expect("client connects via defaulted socket");
    let cp = service_runtime::control_plane(client_pool, client_cfg.lock_timeout);

    // create-admin over the client-only pool: the real acceptance-1 flow.
    service_runtime::create_admin::run_create_admin(&cp, "jack", "hunter2")
        .await
        .expect("create-admin succeeds without any PG-bin vars");
    assert!(cp.has_any_user().await.expect("has_any_user"));
    assert!(cp.is_bootstrap_sealed().await.expect("sealed"));

    server_pool.close().await;
    embedded.shutdown().await.expect("shutdown");
}
