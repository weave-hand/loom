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
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

use control_plane_postgres::fixture::PgFixture;
use fantoccini::{Client, ClientBuilder};
use serde_json::{Map, Value, json};
use tower_http::services::{ServeDir, ServeFile};

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

/// Pick a currently-free TCP port by binding to :0 and releasing it. `tests/
/// components.rs` now runs three `#[tokio::test]`s concurrently in one libtest
/// process, so there are several near-simultaneous bind-then-release windows
/// rather than one — the TOCTOU race is still low-probability, but "single-process
/// test" is no longer the reason it's acceptable. A collision would surface as a
/// nondeterministic "chromedriver exited before listening" rather than a flaky pass.
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
/// (not panic) when the browser can't start — the caller treats a failure as a
/// hard error (e.g. via `.expect(...)`). A chrome that can't load its host libs
/// manifests here as a failed WebDriver connect.
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

// ------------------------------------------------------- component harness

/// A static file server over one directory, on an ephemeral port. The wasm
/// bundle cannot boot from `file://` (browsers refuse to load ES modules from
/// it), and `:gallery-serve`'s `python3 -m http.server` is a dev script a test
/// cannot depend on — hence an in-process server for the life of the test.
pub struct StaticServer {
    /// e.g. `http://127.0.0.1:41234` — no trailing slash.
    pub base_url: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    _task: tokio::task::JoinHandle<()>,
}

impl Drop for StaticServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Serve `dir` over HTTP on an ephemeral port, falling back to its `index.html`
/// so any path boots the bundle.
pub async fn start_static_server(dir: impl Into<PathBuf>) -> StaticServer {
    let dir = dir.into();
    let index = dir.join("index.html");
    let app =
        axum::Router::new().fallback_service(ServeDir::new(dir).fallback(ServeFile::new(index)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind static server");
    let port = listener
        .local_addr()
        .expect("static server local_addr")
        .port();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await;
    });
    StaticServer {
        base_url: format!("http://127.0.0.1:{port}"),
        shutdown: Some(tx),
        _task: task,
    }
}

/// Percent-encode `s` for use as a URL query-string value: everything outside
/// the RFC 3986 unreserved set becomes `%XX`. Space becomes `%20` (NOT `+`), so
/// the harness can decode with `decodeURIComponent`.
#[must_use]
pub fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(*b));
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// A mounted component: the browser and the server that feeds it. Both are
/// killed/shut down when this is dropped, so the test must hold it for as long
/// as it drives the client (this is why `mount` returns the guard rather than a
/// bare `Client`).
///
/// Field order is load-bearing: Rust drops struct fields in declaration order,
/// so `browser` (whose drop kills chromedriver) must be dropped before `server`
/// shuts down — otherwise the static server could wind down while Chrome is
/// still fetching from it. Do not reorder these fields.
pub struct Harness {
    browser: Browser,
    server: StaticServer,
}

impl Harness {
    /// The driven WebDriver client.
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.browser.client
    }

    /// Navigate to `component` with `props`, and wait until it has mounted.
    pub async fn show(&self, component: &str, props: &Value) {
        let url = format!(
            "{}/?component={}&props={}",
            self.server.base_url,
            percent_encode(component),
            percent_encode(&props.to_string()),
        );
        self.browser.client.goto(&url).await.expect("goto harness");
        self.browser
            .client
            .wait()
            .for_element(fantoccini::Locator::Css("#mount"))
            .await
            .expect("harness mount point");
        // The harness renders its own diagnostics into #harness-error (bad props, unknown
        // component name). Surface that message instead of letting the caller fail later
        // with an opaque "no such element" on whatever it went looking for.
        if let Ok(err) = self
            .browser
            .client
            .find(fantoccini::Locator::Css("#harness-error"))
            .await
        {
            let msg = err.text().await.unwrap_or_default();
            panic!("harness error for component {component}: {msg}");
        }
    }
}

/// Serve the harness bundle (`LOOM_UI_DIR`), start the vendored headless
/// browser, and mount `component` with `props`.
pub async fn mount(component: &str, props: &Value) -> Harness {
    let dir = std::env::var("LOOM_UI_DIR").expect("LOOM_UI_DIR (the harness bundle) must be set");
    let server = start_static_server(dir).await;
    let browser = start_browser()
        .await
        .expect("vendored browser must start (RE image / dev-box host libs)");
    let harness = Harness { browser, server };
    harness.show(component, props).await;
    harness
}

/// Load the app and sign in as the seeded admin, returning once the Shell's `<nav>`
/// has rendered. Shared by the e2e tests that need an authenticated workspace;
/// `tests/login.rs` deliberately does not use it, because it asserts on the
/// intermediate states of the sign-in itself.
pub async fn sign_in(client: &Client, base_url: &str) {
    client.goto(base_url).await.expect("load app");
    client
        .wait()
        .for_element(fantoccini::Locator::Css("#login-username"))
        .await
        .expect("login form");
    client
        .find(fantoccini::Locator::Css("#login-username"))
        .await
        .expect("username field")
        .send_keys(ADMIN_USER)
        .await
        .expect("type username");
    client
        .find(fantoccini::Locator::Css("#login-password"))
        .await
        .expect("password field")
        .send_keys(ADMIN_PASS)
        .await
        .expect("type password");
    client
        .find(fantoccini::Locator::Css(".signin"))
        .await
        .expect("sign-in button")
        .click()
        .await
        .expect("click sign in");
    client
        .wait()
        .for_element(fantoccini::Locator::Css("nav"))
        .await
        .expect("shell nav after sign-in");
}
