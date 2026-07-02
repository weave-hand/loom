#![allow(
    clippy::pedantic,
    clippy::restriction,
    reason = "browser e2e test-support harness, not a production path"
)]
//! Test-support harness for the UI login e2e: boot the composite against a fresh
//! DB on the shared Postgres fixture (external-PG mode, serving the wasm bundle),
//! and drive a vendored headless Chromium over WebDriver with fantoccini.

use std::collections::HashMap;
use std::net::TcpListener;
use std::process::{Child, Command};
use std::time::Duration;

use control_plane_postgres::fixture::PgFixture;
use fantoccini::{Client, ClientBuilder};
use serde_json::{Map, Value, json};

/// Credentials the harness seeds and the test signs in with.
pub const ADMIN_USER: &str = "admin";
pub const ADMIN_PASS: &str = "correct-horse-battery-staple";

/// A running composite. Dropping it triggers graceful shutdown.
pub struct Backend {
    pub base_url: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    _warehouse: tempfile::TempDir,
    _engine_dir: tempfile::TempDir,
    _data_dir: tempfile::TempDir,
    _task: tokio::task::JoinHandle<()>,
}

impl Drop for Backend {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Pick a currently-free TCP port by binding to :0 and releasing it. Small TOCTOU
/// race is acceptable for a single-process test.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}

/// Boot the composite in external-PG mode against a fresh migrated DB on the
/// shared fixture cluster, seed the first admin, and wait until it is listening.
pub async fn start_backend(fx: &'static PgFixture) -> Backend {
    // Fresh, already-migrated database on the shared cluster.
    let (cp, db) = fx.fresh_db().await;
    // Out-of-band first-admin bootstrap (also seals the instance).
    service_runtime::create_admin::run_create_admin(&cp, ADMIN_USER, ADMIN_PASS)
        .await
        .expect("seed admin");
    drop(cp);

    let warehouse = tempfile::tempdir().expect("warehouse tempdir");
    let engine_dir = tempfile::tempdir().expect("engine tempdir");
    let data_dir = tempfile::tempdir().expect("data tempdir");
    let engine_socket = engine_dir.path().join("engine.sock").display().to_string();
    let qapi_port = free_port();
    let ingest_port = free_port();

    // External-PG mode: no LOOM_PG_MODE=embedded, DB points at the fixture socket
    // (a directory path — sqlx treats it as a unix socket host). Postgres fixture
    // auth is user `postgres`, trust (empty password).
    let mut env: HashMap<String, String> = HashMap::new();
    env.insert("LOOM_BIND_ADDR".into(), "127.0.0.1:0".into());
    env.insert(
        "LOOM_DB_HOST".into(),
        fx.socket_path().display().to_string(),
    );
    env.insert("LOOM_DB_PORT".into(), "5432".into());
    env.insert("LOOM_DB_USER".into(), "postgres".into());
    env.insert("LOOM_DB_PASSWORD".into(), String::new());
    env.insert("LOOM_DB_NAME".into(), db);
    // Required by `Config::from_map` (`LOOM_DATA_PATH` is a req_var); unused for
    // anything beyond the object-store fallback (overridden by LOOM_WAREHOUSE_URI
    // below) and embedded-PG settings (not selected — LOOM_PG_MODE is unset).
    env.insert(
        "LOOM_DATA_PATH".into(),
        data_dir.path().display().to_string(),
    );
    env.insert(
        "LOOM_WAREHOUSE_URI".into(),
        format!("file://{}", warehouse.path().display()),
    );
    let cfg = service_runtime::Config::from_map(&env).expect("build config");
    // LOOM_UI_DIR is read from the process env by query_api::serve; the BUCK
    // target sets it to $(location //src/ui:bundle), so nothing to do here.

    let addrs = standalone::StandaloneAddrs {
        query_api: format!("127.0.0.1:{qapi_port}").parse().expect("qapi addr"),
        ingest: format!("127.0.0.1:{ingest_port}")
            .parse()
            .expect("ingest addr"),
        engine_socket,
    };
    let tuning = standalone::StandaloneTuning::from_map(&HashMap::new()).expect("tuning");
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (sd_tx, sd_rx) = tokio::sync::oneshot::channel::<()>();
    let shutdown = async move {
        let _ = sd_rx.await;
    };
    let task = tokio::spawn(async move {
        let _ = standalone::run(cfg, addrs, tuning, shutdown, ready_tx).await;
    });
    ready_rx.await.expect("backend became ready");

    Backend {
        base_url: format!("http://127.0.0.1:{qapi_port}"),
        shutdown: Some(sd_tx),
        _warehouse: warehouse,
        _engine_dir: engine_dir,
        _data_dir: data_dir,
        _task: task,
    }
}

/// A connected browser + its chromedriver child (killed on drop).
pub struct Browser {
    pub client: Client,
    _driver: DriverGuard,
}

struct DriverGuard(Child);
impl Drop for DriverGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

/// Spawn the vendored chromedriver on a free port, wait for it to listen, and
/// connect fantoccini pointed at the vendored headless Chromium. Returns `Err`
/// (not panic) when the browser can't start — the caller decides skip vs fail
/// based on `LOOM_UI_E2E` (see the test's gate). A chrome that can't load its
/// host libs manifests here as a failed WebDriver connect.
pub async fn start_browser() -> Result<Browser, String> {
    let driver_bin = std::env::var("CHROMEDRIVER_BIN").map_err(|_| "CHROMEDRIVER_BIN unset")?;
    let chrome_bin = std::env::var("CHROME_BIN").map_err(|_| "CHROME_BIN unset")?;
    let port = free_port();

    let mut child = Command::new(&driver_bin)
        .arg(format!("--port={port}"))
        .spawn()
        .map_err(|e| format!("spawn chromedriver: {e}"))?;

    // Wait for chromedriver to accept TCP connections (up to ~10s). If it exited
    // (e.g. it can't load its own libs), stop waiting and surface that.
    let addr = format!("127.0.0.1:{port}");
    let mut listening = false;
    for _ in 0..100 {
        if std::net::TcpStream::connect(&addr).is_ok() {
            listening = true;
            break;
        }
        if matches!(child.try_wait(), Ok(Some(_))) {
            return Err("chromedriver exited before listening".to_string());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !listening {
        let _ = child.kill();
        return Err("chromedriver never started listening".to_string());
    }

    let mut caps: Map<String, Value> = Map::new();
    caps.insert(
        "goog:chromeOptions".to_string(),
        json!({
            "binary": chrome_bin,
            "args": ["--headless=new", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        }),
    );

    // A chrome that can't load its host libs fails the session create here.
    let client = ClientBuilder::rustls()
        .map_err(|e| format!("rustls: {e}"))?
        .capabilities(caps)
        .connect(&format!("http://{addr}"))
        .await
        .map_err(|e| {
            let _ = child.kill();
            format!("connect webdriver (browser libs missing?): {e}")
        })?;

    Ok(Browser {
        client,
        _driver: DriverGuard(child),
    })
}
