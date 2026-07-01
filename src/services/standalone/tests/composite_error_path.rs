//! Error-path coverage for the standalone composite: when the engine dies before
//! signalling ready, `run()` must (a) return the engine's *real* error rather than
//! a generic "exited before ready" string, and (b) stop the embedded PG before
//! returning (no orphan postmaster). Regression guard for
//! `iss-standalone-composite-error-cascade`.
//!
//! The failure is induced config-locally, with no global env mutation: the embedded
//! PG boots on `<LOOM_DATA_PATH>/pgrun` (derived from `LOOM_DATA_PATH`, see
//! `src/services/runtime/src/lib.rs:194`), but the engine's Iceberg catalog opens
//! its own pool via `cfg.db.pg_url()` — built from `LOOM_DB_HOST`. Pointing
//! `LOOM_DB_HOST` at a `/`-prefixed socket dir with no cluster makes that catalog
//! connect fail fast (ENOENT on the socket file) while the composite otherwise
//! comes up, so the engine returns an error before it can signal ready.
use std::collections::HashMap;
use std::time::Duration;

/// Embedded-mode Config whose `LOOM_DB_HOST` points at a nonexistent socket dir,
/// so the engine's catalog connect fails before ready while embedded PG (on
/// `<data_path>/pgrun`) still boots. `POSTGRES_BIN_DIR` /
/// `POSTGRES_LD_LIBRARY_PATH` are injected by the `loom_fixture_test` macro.
fn config_with_bad_catalog_db(data_path: &std::path::Path) -> service_runtime::Config {
    let bin_dir = std::env::var("POSTGRES_BIN_DIR").unwrap();
    let ld = std::env::var("POSTGRES_LD_LIBRARY_PATH").unwrap();
    // Absolute (tempdir) path ⇒ starts with `/` ⇒ treated as a unix-socket dir by
    // `DbConfig::pg_url`; nothing ever creates it, so the socket file is absent.
    let bad_socket_dir = data_path.join("no-such-pgrun");
    let mut v: HashMap<String, String> = HashMap::new();
    v.insert("LOOM_BIND_ADDR".into(), "127.0.0.1:0".into());
    v.insert("LOOM_DB_HOST".into(), bad_socket_dir.display().to_string());
    v.insert("LOOM_DB_PORT".into(), "5432".into());
    v.insert("LOOM_DB_USER".into(), "postgres".into());
    v.insert("LOOM_DB_PASSWORD".into(), "postgres".into());
    v.insert("LOOM_DB_NAME".into(), "loom".into());
    v.insert("LOOM_DATA_PATH".into(), data_path.display().to_string());
    v.insert(
        "LOOM_WAREHOUSE_URI".into(),
        format!("file://{}", data_path.join("warehouse").display()),
    );
    v.insert("LOOM_PG_MODE".into(), "embedded".into());
    v.insert("LOOM_PG_BIN_DIR".into(), bin_dir);
    v.insert("LOOM_PG_LD_LIBRARY_PATH".into(), ld);
    service_runtime::Config::from_map(&v).expect("config")
}

async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

#[tokio::test]
async fn engine_dies_before_ready_surfaces_real_error_and_stops_pg() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("warehouse")).unwrap();

    let cfg = config_with_bad_catalog_db(tmp.path());
    let addrs = standalone::StandaloneAddrs {
        query_api: format!("127.0.0.1:{}", free_port().await).parse().unwrap(),
        ingest: format!("127.0.0.1:{}", free_port().await).parse().unwrap(),
        engine_socket: tmp.path().join("engine.sock").display().to_string(),
    };

    // `ready` must never fire (engine dies first); `shutdown` never resolves — the
    // composite has to return on its own via the engine-before-ready path. The outer
    // timeout is the regression guard: a stuck-open composite would hang here.
    let (ready_tx, _ready_rx) = tokio::sync::oneshot::channel::<()>();
    let res = tokio::time::timeout(
        Duration::from_secs(90),
        standalone::run(cfg, addrs, std::future::pending::<()>(), ready_tx),
    )
    .await
    .expect("composite hung instead of returning the engine error");

    // (a) The real engine error is surfaced, not the generic ready-timeout string.
    let err = res.expect_err("expected the composite to fail when the engine cannot start");
    let msg = err.to_string();
    assert!(
        msg.starts_with("engine failed before ready:"),
        "expected the surfaced engine error, got: {msg}"
    );

    // (b) The embedded PG was booted (initdb ran) and stopped cleanly on the way out.
    assert!(
        tmp.path().join("pgdata").join("PG_VERSION").exists(),
        "embedded PG never initialised — the failure happened too early to prove PG shutdown"
    );
    assert!(
        !tmp.path().join("pgdata").join("postmaster.pid").exists(),
        "postmaster.pid left behind — PG not stopped on the error path"
    );
}
