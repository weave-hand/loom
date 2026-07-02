# UI login e2e (fantoccini + vendored headless browser) — Design

**Goal:** A hermetic, RE-eligible end-to-end test that drives the *real* served
loom UI bundle in a headless browser and asserts the login flow against a real
backend — the full-page complement to the pure-logic `loom_ui_core` tests.

**Status:** design — supersedes and promotes `fut-ui-browser-test-fixture`.

## Context

`src/ui/` is a Yew→wasm app served by query-api via `LOOM_UI_DIR`. Today its only
automated coverage is the pure-logic `loom_ui_core` `rust_test`s (`logic`,
`tokens`, `objects`); component rendering and the whole login page are verified by
eye. The repo deferred two fixtures for this — `fut-ui-component-test-fixture`
(per-component render harness) and `fut-ui-browser-test-fixture` (full-page e2e).
This design builds the **full-page login e2e** and leaves the component harness
deferred.

The defining constraint chosen during brainstorming: stay in the Rust/buck2
toolchain and go **fully hermetic + RE-eligible**, on the same footing as loom's
Postgres fixtures (which run on the non-root `buildbuddy` RE worker with a vendored
`postgres-bin`). That rules out Playwright (no maintained modern Rust binding —
`playwright-rust` is stranded at 0.0.20 / 2022) in favour of **fantoccini**
(maintained Rust WebDriver client), and it requires **vendoring** a headless
Chromium + chromedriver as `http_archive`s, because an RE worker has no host
browser to borrow. This is a deliberate reversal of an initial "don't vendor
chromium" instinct, taken because hermetic-RE is loom's norm everywhere else.

## Non-goals

- The component render harness (`fut-ui-component-test-fixture`) — still deferred.
- Testing the Explorer with seeded data — nothing is landed yet; deferred until a
  screen with real backend data exists.
- OAuth / password-reset flows — the buttons are decorative/disabled.
- Cross-browser (Firefox/WebKit) — Chromium only for v1.
- linux-arm64 execution — v1 vendors the linux-x86_64 browser (the CI/RE arch);
  the aarch64 arm is a documented follow-up (Chrome for Testing publishes no
  linux-arm64 build, so it needs a different source).

## Architecture

One native (host-platform) `rust_test`, `//src/ui/e2e:login`, that in a single
process:

1. **Boots the backend against a fresh DB on the shared fixture cluster.** Reuse
   the vendored `postgres-bin` fixture (`BootThrottle` + `fresh_control_plane`):
   one throttled cluster, a brand-new database per test — the resource-light
   "share a PG, get a fresh DB" shape, *not* per-test embedded-PG extraction. Run
   loom's migrations on the fresh DB, then start the composite via
   `standalone::run(cfg, addrs, …)` in **external-PG mode** (the non-embedded
   branch: `LOOM_PG_MODE` unset/external, `LOOM_DB_*` → the fixture socket + fresh
   db name), with `LOOM_UI_DIR=$(location //src/ui:bundle)`, a tempdir
   `file://` warehouse, an ephemeral engine UDS, and ephemeral query-api/ingest
   ports. Seed the first admin with `service_runtime::run_create_admin`.

2. **Drives the browser.** Spawn the vendored `chromedriver` on an ephemeral port,
   connect fantoccini to it (`--headless=new`, `goog:chromeOptions.binary` → the
   vendored `chrome-headless-shell`), navigate to the query-api URL, and exercise
   the login page.

3. **Asserts + tears down.** Assertions below. The fixture cluster, spawned
   services, chromedriver child, and tempdirs drop at end of test.

```
 rust_test process (native, RE-eligible)
   ├─ postgres-bin fixture ── fresh DB (migrated)
   ├─ standalone::run (external PG) ── engine (UDS) + ingest + query-api(:PORT, LOOM_UI_DIR=bundle)
   ├─ run_create_admin ── admin/‹pw›
   └─ chromedriver(:PORT2) ── chrome-headless-shell ──HTTP──▶ query-api:PORT
        fantoccini client drives DOM, asserts
```

## Components

### 1. Vendored browser stack (`//third-party/browser`)

Two per-arch `http_archive`s pinned from **Chrome for Testing**
(`googlechromelabs.github.io/chrome-for-testing`), which publishes versioned,
per-platform `chrome-headless-shell-linux64.zip` and `chromedriver-linux64.zip`
with stable URLs + sha256 — the same pin-and-extract pattern loom uses for
`postgres-bin`, buck2, jq, etc. A single `CHROME_FOR_TESTING_VERSION` constant is
the source of truth so the browser and driver never desync (they must be the same
Chrome major). Exposed as `:chrome-headless-shell` and `:chromedriver` genrule
outputs.

A thin macro `loom_browser_test` (mirroring `loom_fixture_test`) injects
`CHROMEDRIVER_BIN`, `CHROME_BIN`, and any `CHROME_LD_LIBRARY_PATH` via
`$(location …)`, and composes with the fixture env (this test needs both a browser
*and* a Postgres).

### 2. `fantoccini` third-party dep

Add `fantoccini` to the e2e crate's `Cargo.toml`, `cargo generate-lockfile`,
`./tools/buckify.sh`, depend on `//third-party:fantoccini`. It pulls tokio (already
in-tree) and an async HTTP client. WebDriver-only; no Node driver.

### 3. Backend harness (`src/ui/e2e/src/harness.rs`)

A small support module (not itself a test) that owns steps 1–2 of *Architecture*:
`start_backend() -> BackendHandle { base_url, admin_user, admin_pw, _guards }` and
`start_browser() -> fantoccini::Client`. Keeps the test bodies declarative. Reuses
the postgres fixture from `//src/control-plane/postgres` and `standalone::run`
(precedent: the query-api `e2e-support` library composes the fixture + services in
a `rust_test`; this adds the browser).

### 4. The test (`src/ui/e2e/src/login.rs`, target `//src/ui/e2e:login`)

Native host-platform `rust_test` (its own BUCK so the wasm `default_target_platform`
of `//src/ui` doesn't apply); consumes the wasm bundle as a data input via
`LOOM_UI_DIR=$(location //src/ui:bundle)`. Assertions:

- **Renders:** navigating to `/` shows the `Sign in` heading and username/password
  fields (the styled login mounted).
- **Happy path:** fill `admin`/‹pw›, submit → the Explorer shell appears (nav with
  `Log out`, the `Types` sidebar) and a session token is persisted in
  `localStorage`.
- **Sad path:** fill `admin`/`wrong`, submit → an error message renders, the
  Explorer does *not* mount, and no session token is stored.

### 5. Hermeticity / RE

The test is native and RE-eligible. Placement follows `loom_fixture_test`'s model:
buck2 runs the test action locally by default and on RE when the invocation asks
(root host / CI) — the vendored `postgres-bin`, `chromedriver`, and
`chrome-headless-shell` are all materialized on the non-root RE worker, and none of
them need root. Builds always go to RE.

## Risks & mitigations

1. **Headless Chromium's system libs on the RE worker (highest risk).**
   `chrome-headless-shell` dynamically links `libnss3`, `libexpat`, fonts, etc.
   The RE container may lack them. Mitigation, in order of preference: (a) confirm
   the RE image already provides them; (b) vendor the missing `.so`s alongside and
   prepend `CHROME_LD_LIBRARY_PATH` (the fixture already does this for `libxml2`).
   **De-risk first:** a build-time smoke action runs `chrome-headless-shell
   --version` on RE (mirroring the postgres `-V` check) before wiring the full
   test, so a missing-lib failure surfaces as a clear early error, not a flaky
   browser hang.
2. **linux-arm64 has no Chrome for Testing build.** v1 pins linux-x86_64 only
   (the CI/RE arch); the aarch64 `select()` arm resolves to a clear "not available"
   error. Documented follow-up to source an arm64 headless build if RE ever runs
   arm.
3. **Flake/timing.** fantoccini waits explicitly (`wait().for_element(...)`) rather
   than sleeping; the harness polls the query-api port before driving, as dev-up
   does. A hard per-test timeout bounds a hung browser.
4. **Bundle freshness.** The `LOOM_UI_DIR` input is `$(location //src/ui:bundle)`,
   so buck rebuilds the wasm when the UI changes and the test re-runs — no stale
   bundle (the manual-cache-bust problem we hit driving it by hand does not apply
   to a fresh headless profile).

## Registers

- Promote `fut-ui-browser-test-fixture` → done, `pr:` set at land, `spec:` this file.
- New `fut-` entries as needed: the deferred component harness stays
  `fut-ui-component-test-fixture`; add a follow-up for linux-arm64 browser
  sourcing and (if system libs must be vendored) a note mirroring
  `iss-embedded-pg-libxml2`.
