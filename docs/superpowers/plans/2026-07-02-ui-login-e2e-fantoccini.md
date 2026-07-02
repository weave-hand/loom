# UI Login E2E (fantoccini + vendored headless browser) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A hermetic, RE-eligible `rust_test` that drives the real served loom UI bundle in a vendored headless Chromium and asserts the login flow against a real backend.

**Architecture:** One native `rust_test` (`//src/ui/e2e:login`) boots a fresh migrated DB on the shared `PgFixture` cluster, runs the composite via `standalone::run` in external-PG mode (serving `//src/ui:bundle` via `LOOM_UI_DIR`), seeds the first admin, spawns a vendored `chromedriver`, and drives a vendored `chrome-headless-shell` with fantoccini through render / happy-path / sad-path assertions. The browser + driver are vendored as per-arch `http_archive`s under `//third-party/browser`, materialised on the non-root RE worker exactly like `postgres-bin`.

**Tech Stack:** Rust, buck2, fantoccini 0.22 (WebDriver, rustls), Chrome for Testing (`chrome-headless-shell` + `chromedriver`), the `PgFixture` hermetic Postgres, `standalone::run`.

## Global Constraints

- **Browser pin:** Chrome for Testing `CHROME_FOR_TESTING_VERSION = "150.0.7871.46"`, linux64 only. `chrome-headless-shell` sha256 `0395e5db8d1631d5ed879d4f05fec423425537018fa1d73a58b1ade13288f447`; `chromedriver` sha256 `2bd858c27c5d913bc9574e4fb7196161acbefd7f911d9a4b1e01a4cd36095716`. Browser and driver share one version constant (must be the same Chrome major).
- **x86_64-linux only.** Chrome for Testing publishes no linux-arm64 build; do not add an arm64 arm (documented follow-up).
- **Vendored browser binary + host libs.** The CfT browser + chromedriver are vendored (reproducible pinned version), but their ~36 runtime libs (libnspr4/libnss3, GTK/X11, alsa, dbus, gbm/drm…) come from the **host loader path** — NOT vendored. The test therefore runs on the **local executor** (loom_fixture_test policy: a dev machine or the ubuntu-24.04 CI runner, where the libs are present/apt-installed), **not** on RE, and is **not hermetic**. The RE build image (`rbe-ubuntu24-04`) lacks the browser libs by design.
- **Auto-skip gate.** The test tries to start the browser; if that fails (no libs / no browser on this executor) it **skips (trivial pass)**, so `buck2 test //src/...` stays green everywhere. Setting `LOOM_UI_E2E=1` upgrades a browser-start failure to a hard error (so the dedicated CI lane, which installs the deb.deps, catches a genuinely broken browser instead of silently skipping). A successful browser start always runs the real assertions — app failures are never masked. Hardening to hermetic-RE is deferred (`fut-ui-e2e-hermetic-browser`).
- **Tests are `rust_test`/`loom_fixture_test` targets only** — never inline `#[cfg(test)]` (the `no-inline-tests` prek hook fails the build otherwise).
- **Strict clippy** (pedantic + restriction) covers `//src`. Test-support libraries that are not `rust_test` targets carry a crate-level `#![allow(clippy::…, reason = "…")]` (the harness lib below does).
- **fantoccini uses rustls, not native-tls** (`default-features = false, features = ["rustls-tls"]`) to avoid a system OpenSSL build dep on RE.
- **Stable DOM hooks** (already in `src/ui/src/main.rs` / `explorer.rs`): login username input `#login-username`, password `#login-password`, submit button `.signin`, error `p.error`; the Explorer shell renders a `<nav>`; the session token lives in **sessionStorage** under key `loom_token`.
- **Adding a third-party crate:** edit the crate's `Cargo.toml`, `cargo generate-lockfile` (hermetic cargo via `eval "$(./tools/env.sh)"`), then `./tools/buckify.sh`; depend on it as `//third-party:<crate>`. After any dep change run the full `buck2 test //src/...`.
- **Don't pipe `buck2 test`/`bxl` through `tail`** — redirect to a file and grep it.

**Pre-execution note (controller):** the styled login page (`src/ui/src/main.rs`, `src/ui/BUCK` stylist dep, button hover/focus polish) is already implemented and verified in the working tree but uncommitted. Commit it as the branch's first commit before Task 1 so the whole feature lands in one PR:
```bash
git add src/ui/src/main.rs src/ui/BUCK
git commit -m "feat(ui): styled two-panel login page (design tokens + component library)"
```

---

### Task 1: Vendor the headless browser + chromedriver

**Files:**
- Create: `third-party/browser/BUCK`

**Interfaces:**
- Produces: `//third-party/browser:chrome-headless-shell` (a genrule dir whose `chrome-headless-shell` binary is +x), `//third-party/browser:chromedriver` (executable genrule), `//third-party/browser:chromedriver-bin` (command_alias), `//third-party/browser:chrome-smoke` (RE build-time `--version` smoke).

- [ ] **Step 1: Write `third-party/browser/BUCK`**

```python
# Vendored headless Chromium + chromedriver (Chrome for Testing), pinned per the
# single CHROME_FOR_TESTING_VERSION so browser and driver never desync (they must
# share a Chrome major). x86_64-linux only — Chrome for Testing publishes no
# linux-arm64 build; the aarch64 arm is a documented follow-up. Drives the
# //src/ui/e2e:login fantoccini test. To bump: change CHROME_FOR_TESTING_VERSION
# and refresh both sha256s (download the two linux64 zips and `sha256sum`).

CHROME_FOR_TESTING_VERSION = "150.0.7871.46"

_CFT_URL = "https://storage.googleapis.com/chrome-for-testing-public/{ver}/linux64/{asset}-linux64.zip"

http_archive(
    name = "chrome-headless-shell-archive",
    urls = [_CFT_URL.format(ver = CHROME_FOR_TESTING_VERSION, asset = "chrome-headless-shell")],
    sha256 = "0395e5db8d1631d5ed879d4f05fec423425537018fa1d73a58b1ade13288f447",
    strip_prefix = "chrome-headless-shell-linux64",
    type = "zip",
)

http_archive(
    name = "chromedriver-archive",
    urls = [_CFT_URL.format(ver = CHROME_FOR_TESTING_VERSION, asset = "chromedriver")],
    sha256 = "2bd858c27c5d913bc9574e4fb7196161acbefd7f911d9a4b1e01a4cd36095716",
    strip_prefix = "chromedriver-linux64",
    type = "zip",
)

# Materialise the whole headless-shell dir with the binary +x (it loads sibling
# .pak/ICU/snapshot files from its own directory, so keep the tree intact).
genrule(
    name = "chrome-headless-shell",
    out = "chs",
    cmd = "mkdir -p $OUT && cp -r $(location :chrome-headless-shell-archive)/. $OUT/ && chmod +x $OUT/chrome-headless-shell",
    visibility = ["PUBLIC"],
)

# chromedriver is a self-contained ELF; expose it +x.
genrule(
    name = "chromedriver",
    out = "chromedriver",
    cmd = "cp $(location :chromedriver-archive)/chromedriver $OUT && chmod +x $OUT",
    executable = True,
    visibility = ["PUBLIC"],
)

command_alias(
    name = "chromedriver-bin",
    exe = ":chromedriver",
    visibility = ["PUBLIC"],
)

# RE-libs smoke: executes chrome-headless-shell --version at BUILD time (on RE),
# so a missing system lib (libnss3, fonts, …) surfaces here as a clear, early
# failure instead of a flaky browser hang inside the e2e test. If this fails on
# RE, the mitigation is vendoring the missing .so's alongside (mirroring the
# postgres :libxml2 rule) — escalate before wiring the test.
genrule(
    name = "chrome-smoke",
    out = "version.txt",
    cmd = "$(location :chrome-headless-shell)/chrome-headless-shell --version > $OUT 2>&1 || { echo 'chrome-headless-shell --version FAILED:' >&2; cat $OUT >&2; exit 1; }",
    visibility = ["PUBLIC"],
)
```

- [ ] **Step 2: Build the archives + driver**

Run: `buck2 build //third-party/browser:chrome-headless-shell //third-party/browser:chromedriver 2>&1 | tail -3`
Expected: `BUILD SUCCEEDED` (downloads + sha256-verifies both zips).

- [ ] **Step 3: Verify chromedriver runs**

Run: `buck2 run //third-party/browser:chromedriver-bin -- --version`
Expected: prints `ChromeDriver 150.0.7871.46 (...)`.

- [ ] **Step 4: Run the RE-libs smoke (the highest-risk gate)**

Run: `buck2 build //third-party/browser:chrome-smoke 2>&1 | tail -5` then inspect it:
`cat "$(buck2 build --show-full-output //third-party/browser:chrome-smoke 2>/dev/null | awk '{print $2}')"`
Expected: the file contains `Google Chrome for Testing 150.0.7871.46`. Then force it onto RE to prove the RE worker has the libs:
`buck2 build --config project.remote_enabled=true //third-party/browser:chrome-smoke 2>&1 | tail -5`
Expected: `BUILD SUCCEEDED`.
**If it fails on RE with a missing-library error** (`error while loading shared libraries: libnss3.so` or similar): STOP and report BLOCKED with the exact missing `.so` — the mitigation (vendor the lib alongside, like `:libxml2`) is a design decision the controller must make before the e2e test can run on RE/CI.

- [ ] **Step 5: Commit**

```bash
git add third-party/browser/BUCK
git commit -m "build(third-party): vendor Chrome for Testing headless-shell + chromedriver"
```

---

### Task 2: Scaffold the e2e crate and add fantoccini

**Files:**
- Create: `src/ui/e2e/Cargo.toml`
- Create: `src/ui/e2e/src/lib.rs` (placeholder; the real harness lands in Task 3)
- Modify: `Cargo.toml` (workspace `members`)
- Modify: `third-party/BUCK` (regenerated by buckify — do not hand-edit)

**Interfaces:**
- Produces: `//third-party:fantoccini` (buckified), a `loom-ui-e2e` workspace member with crate lib `loom_ui_e2e`.

- [ ] **Step 1: Create `src/ui/e2e/Cargo.toml`**

```toml
[package]
name = "loom-ui-e2e"
version = "0.0.0"
edition = "2024"
publish = false

[lib]
name = "loom_ui_e2e"
path = "src/lib.rs"

[dependencies]
fantoccini = { version = "0.22", default-features = false, features = ["rustls-tls"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "process", "net", "time"] }
serde_json = "1"
tempfile = "3"
```

- [ ] **Step 2: Create the placeholder `src/ui/e2e/src/lib.rs`**

```rust
#![allow(
    clippy::pedantic,
    clippy::restriction,
    reason = "browser e2e test-support harness, not a production path"
)]
//! Test-support harness for the UI login e2e (backend boot + browser driver).
//! Real implementation lands in Task 3.
```

- [ ] **Step 3: Add the crate to the workspace `members`**

In the root `Cargo.toml`, append `"src/ui/e2e"` to the `members` array (after `"src/ui"`).

- [ ] **Step 4: Refresh the lockfile and buckify**

Run:
```bash
eval "$(./tools/env.sh)"
cargo generate-lockfile
./tools/buckify.sh
```
Expected: `third-party/BUCK` now contains a `fantoccini` rule (and its transitive deps). Confirm: `grep -c 'name = "fantoccini"' third-party/BUCK` prints `1`.

- [ ] **Step 5: Verify fantoccini builds**

Run: `buck2 build //third-party:fantoccini 2>&1 | tail -3`
Expected: `BUILD SUCCEEDED`.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock src/ui/e2e/Cargo.toml src/ui/e2e/src/lib.rs third-party/BUCK
git commit -m "build(ui-e2e): scaffold loom-ui-e2e crate + vendor fantoccini"
```

---

### Task 3: The harness + login e2e test

**Files:**
- Modify: `src/ui/e2e/src/lib.rs` (the real harness, replacing the placeholder)
- Create: `src/ui/e2e/tests/login.rs` (the test)
- Create: `src/ui/e2e/BUCK`

**Interfaces:**
- Consumes: `PgFixture::shared()/fresh_db()/socket_path()` from `control_plane_postgres::fixture`; `standalone::{run, StandaloneAddrs}`; `service_runtime::{Config, run_create_admin}`; fantoccini 0.22 (`ClientBuilder::rustls()` → `Result`, `Client::{goto, find, wait, execute}`, `Locator::Css`); env `CHROMEDRIVER_BIN`, `CHROME_BIN`, `LOOM_UI_DIR` (set by the BUCK target), and the `PgFixture` env from `loom_fixture_test`.
- Produces: `//src/ui/e2e:e2e-support` (rust_library, crate `loom_ui_e2e`), `//src/ui/e2e:login` (the `loom_fixture_test`).

- [ ] **Step 1: Write the harness `src/ui/e2e/src/lib.rs`**

```rust
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
    service_runtime::run_create_admin(&cp, ADMIN_USER, ADMIN_PASS)
        .await
        .expect("seed admin");
    drop(cp);

    let warehouse = tempfile::tempdir().expect("warehouse tempdir");
    let engine_dir = tempfile::tempdir().expect("engine tempdir");
    let engine_socket = engine_dir.path().join("engine.sock").display().to_string();
    let qapi_port = free_port();
    let ingest_port = free_port();

    // External-PG mode: no LOOM_PG_MODE=embedded, DB points at the fixture socket
    // (a directory path — sqlx treats it as a unix socket host). Postgres fixture
    // auth is user `postgres`, trust (empty password).
    let mut env: HashMap<String, String> = HashMap::new();
    env.insert("LOOM_BIND_ADDR".into(), "127.0.0.1:0".into());
    env.insert("LOOM_DB_HOST".into(), fx.socket_path().display().to_string());
    env.insert("LOOM_DB_PORT".into(), "5432".into());
    env.insert("LOOM_DB_USER".into(), "postgres".into());
    env.insert("LOOM_DB_PASSWORD".into(), String::new());
    env.insert("LOOM_DB_NAME".into(), db);
    env.insert(
        "LOOM_WAREHOUSE_URI".into(),
        format!("file://{}", warehouse.path().display()),
    );
    let cfg = service_runtime::Config::from_map(&env).expect("build config");
    // LOOM_UI_DIR is read from the process env by query_api::serve; the BUCK
    // target sets it to $(location //src/ui:bundle), so nothing to do here.

    let addrs = standalone::StandaloneAddrs {
        query_api: format!("127.0.0.1:{qapi_port}").parse().expect("qapi addr"),
        ingest: format!("127.0.0.1:{ingest_port}").parse().expect("ingest addr"),
        engine_socket,
    };
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (sd_tx, sd_rx) = tokio::sync::oneshot::channel::<()>();
    let shutdown = async move {
        let _ = sd_rx.await;
    };
    let task = tokio::spawn(async move {
        let _ = standalone::run(cfg, addrs, shutdown, ready_tx).await;
    });
    ready_rx.await.expect("backend became ready");

    Backend {
        base_url: format!("http://127.0.0.1:{qapi_port}"),
        shutdown: Some(sd_tx),
        _warehouse: warehouse,
        _engine_dir: engine_dir,
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
```

- [ ] **Step 2: Write the test `src/ui/e2e/tests/login.rs`**

```rust
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "rust_test body"
)]
//! Full-page login e2e: render, sad path, happy path — one backend + one browser
//! (resource-light, per the shared-cluster/fresh-db model).

use fantoccini::Locator;
use loom_ui_e2e::{ADMIN_PASS, ADMIN_USER, start_backend, start_browser};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn login_flow() {
    let fx = control_plane_postgres::fixture::PgFixture::shared();
    let backend = start_backend(fx).await;

    // Auto-skip gate: the vendored browser needs host libs, present only on the
    // local executor (dev box / ubuntu-24.04 CI runner with the deb.deps), not the
    // RE workers. If the browser can't start, skip (trivial pass) so the default
    // sweep stays green — UNLESS LOOM_UI_E2E=1, which makes a start failure a hard
    // error so the dedicated CI lane catches a genuinely broken browser.
    let browser = match start_browser().await {
        Ok(b) => b,
        Err(e) => {
            if std::env::var("LOOM_UI_E2E").is_ok() {
                panic!("LOOM_UI_E2E=1 but browser failed to start: {e}");
            }
            eprintln!("skipping login_flow: browser unavailable ({e})");
            return;
        }
    };
    let c = &browser.client;

    // 1. Renders: the styled login mounts.
    c.goto(&backend.base_url).await.unwrap();
    c.wait().for_element(Locator::Css("#login-username")).await.unwrap();
    assert!(
        c.find(Locator::Css("#login-password")).await.is_ok(),
        "password field should render"
    );

    // 2. Sad path: wrong password → error, no Explorer, no stored token.
    c.find(Locator::Css("#login-username")).await.unwrap().send_keys(ADMIN_USER).await.unwrap();
    c.find(Locator::Css("#login-password")).await.unwrap().send_keys("wrong-password").await.unwrap();
    c.find(Locator::Css(".signin")).await.unwrap().click().await.unwrap();
    c.wait().for_element(Locator::Css("p.error")).await.unwrap();
    assert!(
        c.find(Locator::Css("#login-username")).await.is_ok(),
        "login form should still be present after a failed sign-in"
    );
    let tok = c
        .execute("return window.sessionStorage.getItem('loom_token');", vec![])
        .await
        .unwrap();
    assert!(tok.is_null(), "no session token should be stored on failure, got {tok:?}");

    // 3. Happy path: correct password → Explorer shell + stored token.
    // Clear the password field and retype the correct one.
    let pw = c.find(Locator::Css("#login-password")).await.unwrap();
    pw.clear().await.unwrap();
    pw.send_keys(ADMIN_PASS).await.unwrap();
    c.find(Locator::Css(".signin")).await.unwrap().click().await.unwrap();
    // The Explorer renders a <nav>; the login page has none.
    c.wait().for_element(Locator::Css("nav")).await.unwrap();
    assert!(
        c.find(Locator::Css("#login-username")).await.is_err(),
        "login form should be gone after a successful sign-in"
    );
    let tok = c
        .execute("return window.sessionStorage.getItem('loom_token');", vec![])
        .await
        .unwrap();
    assert!(tok.is_string(), "a session token should be stored on success, got {tok:?}");

    c.close().await.unwrap();
}
```

- [ ] **Step 3: Write `src/ui/e2e/BUCK`**

```python
# Full-page UI login e2e. Native (host-platform) test that consumes the wasm
# bundle as a data input (LOOM_UI_DIR) and a vendored headless browser. Uses
# loom_fixture_test so it inherits the PgFixture env (POSTGRES_BIN_DIR, slot dir)
# and the local/RE placement policy; the extra env wires the browser + bundle.
load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")

# Harness library (not a rust_test): carries a crate-level #![allow] for the
# panic-safety lints, like the query-api e2e-support library.
rust_library(
    name = "e2e-support",
    crate = "loom_ui_e2e",
    srcs = ["src/lib.rs"],
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//third-party:fantoccini",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:tokio",
        "//src/control-plane/postgres:postgres",
        "//src/services/runtime:runtime",
        "//src/services/standalone:standalone",
    ],
    visibility = ["PUBLIC"],
)

loom_fixture_test(
    name = "login",
    crate = "login",
    srcs = ["tests/login.rs"],
    crate_root = "tests/login.rs",
    deps = [
        ":e2e-support",
        "//third-party:fantoccini",
        "//third-party:serde_json",
        "//third-party:tokio",
        "//src/control-plane/postgres:postgres",
    ],
    env = {
        "CHROME_BIN": "$(location //third-party/browser:chrome-headless-shell)/chrome-headless-shell",
        "CHROMEDRIVER_BIN": "$(location //third-party/browser:chromedriver)",
        "LOOM_UI_DIR": "$(location //src/ui:bundle)",
    },
)
```

- [ ] **Step 4: Build the test target (catches wiring/type errors before running)**

Run: `buck2 build //src/ui/e2e:login 2>&1 | tail -5`
Expected: `BUILD SUCCEEDED`. If `Config::from_map` or `standalone`/`fantoccini` types mismatch, fix here (e.g. if `ClientBuilder::rustls()` needs a different call shape, or `.capabilities()` takes ownership differently, adjust to the crate's 0.22 API).

- [ ] **Step 5: Run the e2e test**

Run: `buck2 test //src/ui/e2e:login > /tmp/uie2e.log 2>&1; grep -E "Tests finished|Pass|FAIL" /tmp/uie2e.log | tail -5`
Expected: `Pass 1. Fail 0`. This dev host has chromium's libs, so the browser starts and the **real assertions run** (render / sad / happy). Confirm it did NOT auto-skip: `grep -c "skipping login_flow" /tmp/uie2e.log` should print `0` here. (On a host with no browser libs it would auto-skip and still show `Pass 1` — that is correct, but not what we want to see on this machine.) If the browser starts but the flow fails, a clear panic names the step (e.g. backend `ready_rx`, `LOOM_DB_HOST` socket connect, or a missing element).

- [ ] **Step 6: Run clippy on the new crate**

Run: `buck2 build '//src/ui/e2e:e2e-support[clippy.txt]' 2>&1 | tail -3` then
`cat "$(buck2 build --show-full-output '//src/ui/e2e:e2e-support[clippy.txt]' 2>/dev/null | awk '{print $2}')"`
Expected: empty (clean).

- [ ] **Step 7: Commit**

```bash
git add src/ui/e2e/src/lib.rs src/ui/e2e/tests/login.rs src/ui/e2e/BUCK
git commit -m "test(ui): full-page login e2e via fantoccini + vendored headless chromium"
```

---

### Task 4: Registers + subsystem docs

**Files:**
- Modify: `docs/FUTURE.md` (promote `fut-ui-browser-test-fixture`; add follow-ups)
- Modify: `src/ui/CLAUDE.md` (document the e2e test)

**Interfaces:** none (docs only). Must pass `bash tools/docs.sh validate` and the markdown-lint hooks (exactly one trailing newline, no trailing whitespace).

- [ ] **Step 1: Promote the browser-fixture item in `docs/FUTURE.md`**

Change the `fut-ui-browser-test-fixture` entry's tag block to `status:promoted` and set `pr:` to `-` (updated at land) and `spec:2026-07-02-ui-login-e2e-fantoccini-design`. Append a follow-up entry for the deferred hermetic/RE hardening:

```markdown
- [ ] **UI e2e browser: hermetic + RE hardening** `{#fut-ui-e2e-hermetic-browser area:test status:deferred from:2026-07-02-ui-login-e2e-fantoccini-design pr:- spec:-}`
  The [[fut-ui-browser-test-fixture]] login e2e vendors the x86_64-linux `chrome-headless-shell`/`chromedriver` binary from Chrome for Testing but takes its ~36 runtime libs (nspr/nss, GTK/X11, alsa, dbus, gbm/drm…) from the HOST loader — so it runs only on the local executor (dev box / ubuntu-24.04 CI runner with the deb.deps apt-installed) and is env-gated (`LOOM_UI_E2E=1`), not hermetic, never on the RE workers (whose `rbe-ubuntu24-04` build image lacks those libs). To make it hermetic + RE-eligible, either (a) publish a custom RBE `container-image` with the deb.deps baked in and point a browser-test execution platform at it, or (b) vendor the full lib closure as archives + `CHROME_LD_LIBRARY_PATH` (fragile; glibc coupling, re-done on every Chrome bump). Also note: Chrome for Testing has no linux-arm64 build, so an arm64 arm needs a different browser source.
```

- [ ] **Step 2: Validate the registers**

Run: `bash tools/docs.sh validate`
Expected: no errors.

- [ ] **Step 3: Add an e2e note to `src/ui/CLAUDE.md`**

Under the testing discussion, add a short paragraph:

```markdown
- **Login e2e (`//src/ui/e2e:login`)** — a full-page fantoccini test that boots the
  composite (fresh DB on the shared `PgFixture`, `standalone::run` serving the bundle
  via `LOOM_UI_DIR`) and drives a **vendored** headless Chromium (`//third-party/browser`,
  Chrome for Testing, x86_64-linux only) over WebDriver: asserts render, sad path
  (error + no `sessionStorage['loom_token']`), and happy path (Explorer `<nav>` + token).
  The Postgres side is hermetic (PgFixture); the **browser takes its libs from the host**,
  so the test runs on the local executor and is **env-gated by `LOOM_UI_E2E=1`** (trivial
  pass otherwise) — run it with `LOOM_UI_E2E=1 buck2 test //src/ui/e2e:login` on a host
  with the deb.deps installed. Hermetic-RE hardening is deferred (`fut-ui-e2e-hermetic-browser`).
  Selectors are the stable hooks in `main.rs`/`explorer.rs` (`#login-username`, `.signin`,
  `p.error`, `nav`).
```

- [ ] **Step 4: Run the markdown-lint hooks**

Run: `buck2 run //tools:prek -- run --all-files 2>&1 | tail -15`
Expected: hooks pass (or auto-fix trailing whitespace/newline — commit any changes they make).

- [ ] **Step 5: Commit**

```bash
git add docs/FUTURE.md src/ui/CLAUDE.md
git commit -m "docs(ui): promote browser-e2e fixture, document login e2e + arm64 follow-up"
```

---

## Final verification (whole branch)

- [ ] `buck2 build //src/... 2>&1 | tail -3` → `BUILD SUCCEEDED`.
- [ ] `buck2 test //src/ui/e2e:login > /tmp/uie2e.log 2>&1; grep -E "Tests finished" /tmp/uie2e.log` → `Pass 1. Fail 0`, and `grep -c "skipping login_flow" /tmp/uie2e.log` → `0` (it really ran the browser here, did not auto-skip).
- [ ] `buck2 test //src/... > /tmp/all.log 2>&1; grep -E "Tests finished" /tmp/all.log` → all pass (the dep/lock change touches the whole graph).
- [ ] `git log --oneline` shows the styling commit + Tasks 1–4.
