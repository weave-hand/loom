# Configuration Seam Unification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consolidate loom's scattered operational tuning knobs onto one typed,
validated config seam loadable from a JSON config file (`LOOM_CONFIG_FILE`) and/or
environment variables, so a deployment can see and set them and a bad value fails
startup instead of silently falling back — with no behavior change at default values.

**Architecture:** A new light leaf crate `loom-config` holds the shared parse
machinery (`ConfigError`, `overlay_opt`, `env_map`, `parse_config_doc`) and the
cross-crate `WorkerTuning` struct. Each domain owns a small typed sub-struct
(`Default` + serde `Serialize`/`Deserialize` with container `#[serde(default)]` +
`overlay_env` + `validate`), co-located with the code it tunes. Each binary composes
only the sub-structs it needs and resolves them as **defaults < file < env** from a
single env snapshot in `main`.

**Tech Stack:** Rust 2024, buck2, serde + serde_json (already vendored), thiserror.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-06-25-config-seam-unification-design.md`. Item id `road-config-seam-unification`.
- **No default-value behavior change anywhere.** Every `Default` must encode today's exact constant; every wired knob at its default must behave identically to the current hardcoded path.
- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`. Each new test is a sibling `tests/<name>.rs` wired as its own `rust_test` in the crate's BUCK, loading `rust_test` from `//src:loom_test.bzl` (the `loom_rust_test` wrapper that exempts tests from panic-safety lints). The `no-inline-tests` prek hook fails on any `#[test]` under `src/`.
- **Strict clippy** (pedantic + restriction) on production lib/bin code: no `unwrap`/`expect`/`indexing_slicing`/`panic`/`todo` in non-test code. Use `?` + `ConfigError`. Local silences use `#[expect(lint, reason = "...")]`.
- **Format JSON only** via `serde_json` (no new dependency; YAML is deferred to `fut-config-yaml-format`).
- **Worker zero-postgres guard:** `//src/services/worker:worker-bin` must NOT pull `//src/control-plane/postgres` into its dep closure (documented guard in `src/services/worker/BUCK`). This is WHY the parse machinery lives in the light `loom-config` crate, not in `service_runtime` (which depends on postgres). `loom-config` deps are only `thiserror`, `serde`, `serde_json`.
- **Pure tests:** every tuning-struct test operates on a `HashMap<String,String>` + a JSON string — no Postgres/DuckDB fixtures — so they run on remote execution (a bare `rust_test`, not `loom_fixture_test`).
- **Build/test command:** `buck2 build //src/...` then `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log` (never pipe `buck2 test` through `tail`/`head`).
- After any BUCK/dep change, also run `bash tools/clippy-all.sh` and `buck2 run //tools:prek -- run --all-files` before pushing.

## Design decisions / deviations from the spec (for the plan reviewer)

The spec is explicitly exploratory ("defaults to argue with"). Three places where the
code forced a concrete choice, each preserving the spec's **intent** and **DoD**:

1. **`ConfigError` + `overlay_opt` live in a new `loom-config` crate, re-exported by `service_runtime`** (spec said "service_runtime gains the helper"). Reason: `datafusion-io`, `control_plane_worker`, and `worker-bin` need `overlay_env`/`overlay_opt` but must stay postgres-free; they cannot depend on `service_runtime`. `service_runtime` does `pub use loom_config::{ConfigError, overlay_opt, env_map, ...}`, so every existing `service_runtime::ConfigError` path is unchanged and the DoD's "overlay_opt test in service_runtime" is satisfied via the re-export. No cycle: `loom-config` is a leaf; `service_runtime`/`datafusion-io`/`control_plane_worker`/`transform`/`ingest`/`query-api` all depend *on* it.
2. **`WriteConfig` keeps its name** (spec proposed renaming to `WriteTuning`). Reason: ~10 call sites reference `WriteConfig`; the rename is pure churn for a cosmetic match and the spec permits "the implementation plan may adjust spelling." `WriteConfig` *is* the spec's "WriteTuning" struct.
3. **`backoff_ceiling`/`backoff_max_attempts` are wired into the worker handler only; the transform handler's backoff base is left unchanged this slice.** Reason: the worker handler's formula is already exactly `min(1s << clamp(attempts,0,6), 60s)` (ceiling 60, max_attempts 6), so sourcing those two from `WorkerTuning` is a **no-op at defaults**. The transform handler uses a *different* base (2s) and no explicit cap; unifying it would change its retry timing, violating "no default-value behavior change." The spec's "transform mirrors the same struct" was a "may include" nicety — deferred (note it in `docs/FUTURE.md` at finish). `poll_interval` IS wired into both binaries' `Worker` (default 5s == current, no change).

**Per-binary composition (adjusted to real consumers):**

| Binary | Sub-structs composed | Real consumers wired |
|---|---|---|
| ingest | `RoutingTuning`, `WriteConfig` | `IcebergMaterializer.{inline_byte_limit,flush_byte_threshold}`; `ingest::materialize::land` write path (`WriteConfig`) |
| query-api | `RoutingTuning`, `ServingTuning` | `IcebergActionWriter.{inline_byte_limit,flush_byte_threshold}`; `AppState.default_limit` |
| worker | `WorkerTuning`, `WriteConfig` | `Worker::with_poll_interval`; `handler::backoff` (ceiling/max_attempts); `CompactCtx`/compact `WriteConfig` |
| transform | `WorkerTuning`, `WriteConfig` | `Worker::with_poll_interval`; transform `run`/`compact` `WriteConfig` |

(query-api does not run the datafusion multi-file `write_dataset` path — its action write
goes through the postgres iceberg writer — so query-api composes routing+serving, not
write. ingest's `materialize::land` IS the datafusion write path, so ingest composes write.
`max_connections` is on `DbConfig`, resolved for every binary via `Config::from_*`.)

---

## File structure

- **Create** `src/loom-config/BUCK`, `src/loom-config/src/lib.rs`, `src/loom-config/tests/overlay.rs`, `src/loom-config/tests/worker_tuning.rs` — the seam crate.
- **Modify** `src/services/runtime/src/lib.rs` (re-export from loom-config; `DbConfig.max_connections`; `build_pool`), `src/services/runtime/BUCK` (dep), `src/services/runtime/tests/config.rs` (new field).
- **Modify** `src/services/datafusion-io/src/write.rs` (serde + `overlay_env` + `validate` on `WriteConfig`), `src/services/datafusion-io/BUCK` (serde + loom-config deps; new test). **Create** `src/services/datafusion-io/tests/write_tuning.rs`.
- **Create** `src/services/ingest/src/config.rs` (`RoutingTuning`, `IngestConfig`), `src/services/ingest/tests/routing_tuning.rs`. **Modify** `src/services/ingest/src/lib.rs`, `src/services/ingest/src/main.rs`, `src/services/ingest/src/materialize.rs`, `src/services/ingest/BUCK`.
- **Create** `src/services/query-api/src/config.rs` (`ServingTuning`, `QueryApiConfig`), `src/services/query-api/tests/serving_tuning.rs`. **Modify** `src/services/query-api/src/handler.rs`, `src/services/query-api/src/http.rs`, `src/services/query-api/src/main.rs`, `src/services/query-api/BUCK`.
- **Modify** `src/services/worker/src/main.rs`, `src/services/worker/src/handler.rs`, `src/services/worker/src/compact.rs`, `src/services/worker/BUCK`.
- **Modify** `src/services/transform/src/main.rs`, `src/services/transform/BUCK`.
- **Modify** `docs/FUTURE.md` / `docs/ROADMAP.md` at finish via `loom-docs-update`.

---

## Task 1: `loom-config` crate — parse machinery

**Files:**
- Create: `src/loom-config/src/lib.rs`
- Create: `src/loom-config/BUCK`
- Test: `src/loom-config/tests/overlay.rs`

**Interfaces:**
- Produces:
  - `pub enum ConfigError { MissingVar(String), Invalid { var: String, detail: String } }` (derives `Debug`, `thiserror::Error`; `Display` messages identical to runtime's current ones: `"missing required environment variable: {0}"` and `"invalid value for {var}: {detail}"`).
  - `pub fn invalid(var: &str, detail: impl core::fmt::Display) -> ConfigError`
  - `pub fn overlay_opt<T>(vars: &HashMap<String, String>, key: &str, slot: &mut T) -> Result<(), ConfigError> where T: FromStr, T::Err: core::fmt::Display` — if `vars[key]` present, parse into `*slot` or `Err(invalid(key, e))`; absent ⇒ `Ok(())` no-op.
  - `pub fn env_map() -> HashMap<String, String>` = `std::env::vars().collect()`.
  - `pub fn parse_config_doc<T: serde::de::DeserializeOwned>(doc: &str) -> Result<T, ConfigError>` — `serde_json::from_str(doc).map_err(|e| invalid("LOOM_CONFIG_FILE", e))`.

- [ ] **Step 1: Write the failing test**

Create `src/loom-config/tests/overlay.rs`:

```rust
//! Unit tests for the shared config-parse machinery.
use std::collections::HashMap;

use loom_config::{ConfigError, env_map, overlay_opt, parse_config_doc};

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
}

#[test]
fn overlay_opt_absent_is_noop() {
    let vars = map(&[]);
    let mut slot: u64 = 42;
    overlay_opt(&vars, "LOOM_X", &mut slot).unwrap();
    assert_eq!(slot, 42);
}

#[test]
fn overlay_opt_present_parses() {
    let vars = map(&[("LOOM_X", "7")]);
    let mut slot: u64 = 42;
    overlay_opt(&vars, "LOOM_X", &mut slot).unwrap();
    assert_eq!(slot, 7);
}

#[test]
fn overlay_opt_malformed_is_error_naming_key() {
    let vars = map(&[("LOOM_X", "abc")]);
    let mut slot: u64 = 42;
    let err = overlay_opt(&vars, "LOOM_X", &mut slot).unwrap_err();
    match err {
        ConfigError::Invalid { var, .. } => assert_eq!(var, "LOOM_X"),
        other => panic!("expected Invalid, got {other:?}"),
    }
    assert_eq!(slot, 42, "slot unchanged on error");
}

#[test]
fn parse_config_doc_rejects_bad_json() {
    let err = parse_config_doc::<std::collections::BTreeMap<String, u64>>("{ not json")
        .unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { ref var, .. } if var == "LOOM_CONFIG_FILE"));
}

#[test]
fn parse_config_doc_accepts_good_json() {
    let m: std::collections::BTreeMap<String, u64> =
        parse_config_doc(r#"{"a": 1}"#).unwrap();
    assert_eq!(m.get("a"), Some(&1));
}

#[test]
fn env_map_is_a_snapshot() {
    // Just confirm it returns the process env without panicking.
    let m = env_map();
    let _ = m.len();
}
```

- [ ] **Step 2: Run to verify it fails (crate does not exist yet)**

Run: `buck2 test //src/loom-config:overlay > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|Error" /tmp/t.log`
Expected: FAIL — target `//src/loom-config:overlay` does not exist.

- [ ] **Step 3: Write the crate**

Create `src/loom-config/src/lib.rs`:

```rust
//! loom's shared configuration seam: the typed-error + typed-env-overlay machinery
//! every per-domain tuning struct uses, plus the cross-crate `WorkerTuning`. A light
//! leaf crate (deps: thiserror, serde, serde_json) so postgres-free crates
//! (`datafusion-io`, `control_plane_worker`, `worker-bin`) can depend on it without
//! pulling `service_runtime` (and thus postgres) into their closure.

use std::collections::HashMap;
use std::str::FromStr;

mod worker;
pub use worker::WorkerTuning;

/// A configuration parse/validation failure. Re-exported as `service_runtime::ConfigError`
/// so existing call sites are unchanged; every `main` already surfaces it as a startup error.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing required environment variable: {0}")]
    MissingVar(String),
    #[error("invalid value for {var}: {detail}")]
    Invalid { var: String, detail: String },
}

/// Construct an `Invalid` error naming the offending key.
#[must_use]
pub fn invalid(var: &str, detail: impl core::fmt::Display) -> ConfigError {
    ConfigError::Invalid { var: var.to_string(), detail: detail.to_string() }
}

/// Apply `vars[key]` over `slot` if present; `ConfigError::Invalid` (naming `key`) if
/// present-but-unparseable; no-op if absent. A present-but-malformed value is a startup
/// error, NOT a silent fallback — the fix for the old lossy `.ok()` reads.
pub fn overlay_opt<T>(
    vars: &HashMap<String, String>,
    key: &str,
    slot: &mut T,
) -> Result<(), ConfigError>
where
    T: FromStr,
    T::Err: core::fmt::Display,
{
    if let Some(raw) = vars.get(key) {
        *slot = raw.parse().map_err(|e| invalid(key, e))?;
    }
    Ok(())
}

/// Snapshot the process environment into a map — read once per `main` so config
/// loading is consistent and testable without touching the real environment.
#[must_use]
pub fn env_map() -> HashMap<String, String> {
    std::env::vars().collect()
}

/// Deserialize a JSON config document into a (defaulted) config struct. Container-level
/// `#[serde(default)]` on the target makes any omitted key fall to its `Default`, so a
/// partial document is valid. Parse failures surface as `Invalid` naming `LOOM_CONFIG_FILE`.
pub fn parse_config_doc<T: serde::de::DeserializeOwned>(doc: &str) -> Result<T, ConfigError> {
    serde_json::from_str(doc).map_err(|e| invalid("LOOM_CONFIG_FILE", e))
}
```

Create `src/loom-config/src/worker.rs` (stub for now; filled in Task 5 — but define it so the crate compiles):

```rust
//! Worker-domain tuning: the polling fallback interval and the retry-backoff bounds.
//! Lives here (not in `control_plane_worker` or `service_runtime`) so the zero-postgres
//! `worker-bin` and the `control_plane_worker`/`transform` libs can all consume it.

use std::collections::HashMap;
use std::time::Duration;

use crate::{ConfigError, overlay_opt};

/// Tuning for the queue worker loop. Stored as raw unit fields (serde-friendly);
/// accessors return `Duration`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct WorkerTuning {
    /// Notification-miss fallback poll interval, in milliseconds. Default 5000.
    pub poll_interval_ms: u64,
    /// Exponential-backoff ceiling, in seconds. Default 60.
    pub backoff_ceiling_secs: u64,
    /// Exponential-backoff iteration cap (attempts are clamped to this). Default 6.
    pub backoff_max_attempts: u32,
}

impl Default for WorkerTuning {
    fn default() -> Self {
        Self { poll_interval_ms: 5000, backoff_ceiling_secs: 60, backoff_max_attempts: 6 }
    }
}

impl WorkerTuning {
    /// The polling fallback interval.
    #[must_use]
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }

    /// Capped exponential backoff: `min(1s << clamp(attempts, 0, max_attempts), ceiling)`.
    /// The two spec knobs map cleanly: `backoff_max_attempts` is the shift clamp (the
    /// "iteration cap" — how many doublings), `backoff_ceiling_secs` is the value cap (the
    /// "exp-backoff cap" — the `.min`). The original worker formula used the literals `6`
    /// and `60` for these independently; at defaults (6 / 60) this is byte-identical
    /// (`[1,2,4,8,16,32,60,60,…]`). Coupling the shift to `backoff_max_attempts` is the
    /// deliberate redesign that makes the knob meaningful (reviewer note, blocking fix #1).
    #[must_use]
    pub fn backoff(&self, attempts: i32) -> Duration {
        let shift = attempts.clamp(0, self.backoff_max_attempts.min(63) as i32) as u32;
        let secs = 1u64.checked_shl(shift).unwrap_or(u64::MAX).min(self.backoff_ceiling_secs);
        Duration::from_secs(secs)
    }

    /// Apply any present `LOOM_WORKER_*` vars over the current values.
    pub fn overlay_env(&mut self, vars: &HashMap<String, String>) -> Result<(), ConfigError> {
        overlay_opt(vars, "LOOM_WORKER_POLL_INTERVAL_MS", &mut self.poll_interval_ms)?;
        overlay_opt(vars, "LOOM_WORKER_BACKOFF_CEILING_MS", &mut self.backoff_ceiling_secs)
            .map(|()| ())?; // NB: ceiling is seconds; see validate/notes below
        overlay_opt(vars, "LOOM_WORKER_BACKOFF_MAX_ATTEMPTS", &mut self.backoff_max_attempts)?;
        Ok(())
    }

    /// Validate bounds. `poll_interval_ms >= 1`, `backoff_max_attempts >= 1`.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.poll_interval_ms == 0 {
            return Err(crate::invalid("LOOM_WORKER_POLL_INTERVAL_MS", "must be >= 1"));
        }
        if self.backoff_max_attempts == 0 {
            return Err(crate::invalid("LOOM_WORKER_BACKOFF_MAX_ATTEMPTS", "must be >= 1"));
        }
        Ok(())
    }
}
```

> **NOTE for implementer:** the spec's env var is `LOOM_WORKER_BACKOFF_CEILING_MS` (milliseconds) but the worker formula caps in **whole seconds**. Resolve in Task 5: store `backoff_ceiling_secs` and name the env var `LOOM_WORKER_BACKOFF_CEILING_SECS` for unit consistency (the `LOOM_`-prefixed, unit-suffixed convention is the fixed contract; exact spelling is the plan's to set). Update the doc-comment and the Task 5 tests to `_SECS`. The stub above is provisional — Task 5 is the source of truth for `WorkerTuning`.

Create `src/loom-config/BUCK`:

```python
load("//src:loom_test.bzl", "rust_test")

rust_library(
    name = "loom-config",
    crate = "loom_config",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//third-party:serde",
        "//third-party:serde_json",
        "//third-party:thiserror",
    ],
    visibility = ["PUBLIC"],
)

rust_test(
    name = "overlay",
    crate = "overlay",
    srcs = ["tests/overlay.rs"],
    crate_root = "tests/overlay.rs",
    edition = "2024",
    deps = [":loom-config"],
)

rust_test(
    name = "worker-tuning",
    crate = "worker_tuning",
    srcs = ["tests/worker_tuning.rs"],
    crate_root = "tests/worker_tuning.rs",
    edition = "2024",
    deps = [":loom-config"],
)
```

> The `worker-tuning` test target's file is created in Task 5. To keep Task 1 green on its own, create a minimal placeholder `src/loom-config/tests/worker_tuning.rs` now with a single `#[test] fn placeholder() {}` and flesh it out in Task 5. (Alternatively, add the `worker-tuning` target in Task 5; if so, omit it here.)

- [ ] **Step 4: Run to verify it passes**

Run: `buck2 test //src/loom-config:overlay //src/loom-config:worker-tuning > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (overlay: 6 tests; worker-tuning: placeholder).

- [ ] **Step 5: Clippy + commit**

```bash
bash tools/clippy-all.sh > /tmp/cl.log 2>&1; grep -iE "warning|error" /tmp/cl.log | head
git add src/loom-config
git commit -m "feat(config): add loom-config crate with shared parse machinery"
```

---

## Task 2: `service_runtime` re-export + `DbConfig.max_connections`

**Files:**
- Modify: `src/services/runtime/src/lib.rs`
- Modify: `src/services/runtime/BUCK`
- Test: `src/services/runtime/tests/config.rs` (extend), `src/services/runtime/tests/overlay_opt.rs` (new — DoD)

**Interfaces:**
- Consumes: `loom_config::{ConfigError, overlay_opt, env_map, invalid, parse_config_doc}`.
- Produces:
  - `service_runtime::ConfigError` is now `pub use loom_config::ConfigError`.
  - `pub use loom_config::{overlay_opt, env_map, invalid, parse_config_doc};`
  - `DbConfig` gains `pub max_connections: Option<u32>` (default `None` ⇒ sqlx default).
  - `Config::from_map` reads `LOOM_DB_MAX_CONNECTIONS` (typed `u32`, malformed ⇒ `Invalid`).
  - `build_pool` applies `.max_connections(n)` to `PgPoolOptions` when `Some`.

- [ ] **Step 1: Write the failing test** — add to `src/services/runtime/tests/config.rs`:

```rust
#[test]
fn max_connections_absent_defaults_none() {
    let cfg = service_runtime::Config::from_map(&base_vars()).unwrap();
    assert_eq!(cfg.db.max_connections, None);
}

#[test]
fn max_connections_parsed() {
    let mut vars = base_vars();
    vars.insert("LOOM_DB_MAX_CONNECTIONS".into(), "12".into());
    let cfg = service_runtime::Config::from_map(&vars).unwrap();
    assert_eq!(cfg.db.max_connections, Some(12));
}

#[test]
fn max_connections_malformed_is_error() {
    let mut vars = base_vars();
    vars.insert("LOOM_DB_MAX_CONNECTIONS".into(), "lots".into());
    let err = service_runtime::Config::from_map(&vars).unwrap_err();
    assert!(matches!(err, service_runtime::ConfigError::Invalid { ref var, .. }
        if var == "LOOM_DB_MAX_CONNECTIONS"));
}
```

> **Implementer:** inspect `tests/config.rs` for the existing helper that builds a valid var map (it constructs all `LOOM_DB_*` etc.). If it is named differently than `base_vars`, use the existing name; if there is none, add a small `base_vars()` helper mirroring the existing tests' setup. Also update EVERY existing `DbConfig { .. }` literal in this file to add `max_connections: None`.

Create `src/services/runtime/tests/overlay_opt.rs` (satisfies the DoD "overlay_opt test in service_runtime", testing the re-export):

```rust
//! The `overlay_opt` helper is re-exported from loom-config; exercise it via service_runtime.
use std::collections::HashMap;

#[test]
fn service_runtime_reexports_overlay_opt() {
    let mut vars: HashMap<String, String> = HashMap::new();
    vars.insert("LOOM_N".into(), "5".into());
    let mut slot: u32 = 1;
    service_runtime::overlay_opt(&vars, "LOOM_N", &mut slot).unwrap();
    assert_eq!(slot, 5);
    let err = {
        vars.insert("LOOM_N".into(), "x".into());
        service_runtime::overlay_opt(&vars, "LOOM_N", &mut slot).unwrap_err()
    };
    assert!(matches!(err, service_runtime::ConfigError::Invalid { ref var, .. } if var == "LOOM_N"));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/runtime:config //src/services/runtime:overlay-opt > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|Error" /tmp/t.log`
Expected: FAIL (field/method/target missing).

- [ ] **Step 3: Implement** in `src/services/runtime/src/lib.rs`:

Replace the local `ConfigError` definition (lines ~96–102) with a re-export, and add the helper re-exports near the other `pub use`s:

```rust
pub use loom_config::{ConfigError, env_map, invalid, overlay_opt, parse_config_doc};
```

Delete the `#[derive(Debug, thiserror::Error)] pub enum ConfigError { ... }` block (it now lives in loom-config). The existing `invalid` closure inside `from_map` still works since `ConfigError::Invalid` is the same shape; leave it or switch to the re-exported `invalid` fn.

Add the field to `DbConfig`:

```rust
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    /// Max pool connections. `None` ⇒ sqlx default. From `LOOM_DB_MAX_CONNECTIONS`.
    pub max_connections: Option<u32>,
}
```

In `Config::from_map`, parse it (place near the other optional reads) and set it in the `DbConfig { .. }` literal:

```rust
let max_connections = match vars.get("LOOM_DB_MAX_CONNECTIONS") {
    Some(s) => Some(
        s.parse::<u32>()
            .map_err(|e| invalid("LOOM_DB_MAX_CONNECTIONS", e))?,
    ),
    None => None,
};
// ... in the DbConfig literal:
db: DbConfig {
    host: req("LOOM_DB_HOST")?,
    port,
    user: req("LOOM_DB_USER")?,
    password: req("LOOM_DB_PASSWORD")?,
    dbname: req("LOOM_DB_NAME")?,
    max_connections,
},
```

In `build_pool`, apply it:

```rust
pub async fn build_pool(db: &DbConfig) -> Result<PgPool, RuntimeError> {
    let mut opts = PgPoolOptions::new();
    if let Some(n) = db.max_connections {
        opts = opts.max_connections(n);
    }
    opts.connect_with(db.pg_connect_options())
        .await
        .map_err(RuntimeError::Pool)
}
```

Update the `DbConfig { .. }` literal inside `lib.rs` (if any) to include `max_connections: None`.

Update `src/services/runtime/BUCK`: add `"//src/loom-config:loom-config"` to the `runtime` lib `deps`, and add a `rust_test` target `overlay-opt` mirroring the `config` target (crate `overlay_opt`, src `tests/overlay_opt.rs`, dep `:runtime`).

- [ ] **Step 4: Fix ALL `DbConfig` literals.** There are 4 construction sites (reviewer-confirmed): `src/services/runtime/src/lib.rs:152` (production — already handled in Step 3), `src/services/runtime/tests/config.rs:86` AND `:95` (two literals in the same test file — update both, else the test won't compile), and `src/services/ingest/tests/runtime_land.rs:59`. Add `max_connections: None` to each test literal. (Verify: `grep -rn "DbConfig {" src/ --include=*.rs`.)

- [ ] **Step 5: Run to verify pass**

Run: `buck2 test //src/services/runtime/... //src/services/ingest:runtime-land > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 6: Clippy + commit**

```bash
bash tools/clippy-all.sh > /tmp/cl.log 2>&1; grep -iE "warning|error" /tmp/cl.log | head
git add -A
git commit -m "refactor(config): re-export ConfigError from loom-config; add DbConfig.max_connections"
```

---

## Task 3: `WriteConfig` gains serde + `overlay_env` + `validate`

**Files:**
- Modify: `src/services/datafusion-io/src/write.rs:24-44`
- Modify: `src/services/datafusion-io/BUCK`
- Test: `src/services/datafusion-io/tests/write_tuning.rs`

**Interfaces:**
- Consumes: `loom_config::{ConfigError, overlay_opt, invalid}`.
- Produces (on `datafusion_io::WriteConfig`):
  - derives `Clone, Debug, serde::Serialize, serde::Deserialize` with container `#[serde(default)]`.
  - `pub fn overlay_env(&mut self, vars: &HashMap<String,String>) -> Result<(), ConfigError>` — keys `LOOM_WRITE_TARGET_FILE_BYTES` (u64), `LOOM_WRITE_MAX_FILES` (usize), `LOOM_WRITE_COMPRESSION_FACTOR` (f64).
  - `pub fn validate(&self) -> Result<(), ConfigError>` — `target_file_size_bytes >= 1`, `max_files >= 1`, `compression_factor` in `(0.0, 1.0]`.

- [ ] **Step 1: Write the failing test** — create `src/services/datafusion-io/tests/write_tuning.rs`:

```rust
//! Config behaviour of `WriteConfig` (the write-tuning seam struct).
use std::collections::HashMap;

use datafusion_io::WriteConfig;

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
}

#[test]
fn defaults_match_constants() {
    let c = WriteConfig::default();
    assert_eq!(c.target_file_size_bytes, 128 * 1024 * 1024);
    assert_eq!(c.max_files, 64);
    assert!((c.compression_factor - 0.3).abs() < 1e-9);
}

#[test]
fn partial_json_falls_to_default() {
    let c: WriteConfig = serde_json::from_str(r#"{"max_files": 8}"#).unwrap();
    assert_eq!(c.max_files, 8);
    assert_eq!(c.target_file_size_bytes, 128 * 1024 * 1024);
}

#[test]
fn env_overlay_applies() {
    let mut c = WriteConfig::default();
    c.overlay_env(&map(&[("LOOM_WRITE_MAX_FILES", "10")])).unwrap();
    assert_eq!(c.max_files, 10);
}

#[test]
fn env_overrides_file_value() {
    let mut c: WriteConfig = serde_json::from_str(r#"{"max_files": 8}"#).unwrap();
    c.overlay_env(&map(&[("LOOM_WRITE_MAX_FILES", "10")])).unwrap();
    assert_eq!(c.max_files, 10, "env wins over file");
}

#[test]
fn file_value_survives_when_env_silent() {
    let mut c: WriteConfig = serde_json::from_str(r#"{"max_files": 8}"#).unwrap();
    c.overlay_env(&map(&[])).unwrap();
    assert_eq!(c.max_files, 8, "file value survives");
}

#[test]
fn malformed_env_is_error_naming_key() {
    let mut c = WriteConfig::default();
    let err = c.overlay_env(&map(&[("LOOM_WRITE_MAX_FILES", "lots")])).unwrap_err();
    assert!(format!("{err}").contains("LOOM_WRITE_MAX_FILES"));
}

#[test]
fn validate_rejects_out_of_range_compression() {
    let c: WriteConfig = serde_json::from_str(r#"{"compression_factor": 1.5}"#).unwrap();
    assert!(c.validate().is_err());
    let c0: WriteConfig = serde_json::from_str(r#"{"compression_factor": 0.0}"#).unwrap();
    assert!(c0.validate().is_err());
}

#[test]
fn validate_rejects_zero_max_files() {
    let c: WriteConfig = serde_json::from_str(r#"{"max_files": 0}"#).unwrap();
    assert!(c.validate().is_err());
}

#[test]
fn validate_accepts_defaults() {
    assert!(WriteConfig::default().validate().is_ok());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/datafusion-io:write-tuning > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|Error" /tmp/t.log`
Expected: FAIL — target missing / methods missing.

- [ ] **Step 3: Implement** in `src/services/datafusion-io/src/write.rs`. Replace the struct + Default block (lines 24–44):

```rust
use std::collections::HashMap;

use loom_config::{ConfigError, invalid, overlay_opt};

/// Tunables for the DataFusion write. Defaults target ~128 MiB Snappy files.
/// Serde container `#[serde(default)]` lets a partial config document omit any field
/// (it falls to `Default`); `overlay_env` then applies `LOOM_WRITE_*` on top.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct WriteConfig {
    /// Desired size of each output Parquet file, in bytes.
    pub target_file_size_bytes: u64,
    /// Hard upper bound on the number of output files (= partitions).
    pub max_files: usize,
    /// In-memory Arrow bytes are larger than compressed Parquet; this factor maps
    /// estimated in-memory size to estimated on-disk size.
    pub compression_factor: f64,
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            target_file_size_bytes: 128 * 1024 * 1024,
            max_files: 64,
            compression_factor: 0.3,
        }
    }
}

impl WriteConfig {
    /// Apply any present `LOOM_WRITE_*` vars over the current values.
    pub fn overlay_env(&mut self, vars: &HashMap<String, String>) -> Result<(), ConfigError> {
        overlay_opt(vars, "LOOM_WRITE_TARGET_FILE_BYTES", &mut self.target_file_size_bytes)?;
        overlay_opt(vars, "LOOM_WRITE_MAX_FILES", &mut self.max_files)?;
        overlay_opt(vars, "LOOM_WRITE_COMPRESSION_FACTOR", &mut self.compression_factor)?;
        Ok(())
    }

    /// Validate ranges (run after file+env layers). `compression_factor` in `(0, 1]`,
    /// `max_files >= 1`, `target_file_size_bytes >= 1`.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.target_file_size_bytes == 0 {
            return Err(invalid("LOOM_WRITE_TARGET_FILE_BYTES", "must be >= 1"));
        }
        if self.max_files == 0 {
            return Err(invalid("LOOM_WRITE_MAX_FILES", "must be >= 1"));
        }
        if !(self.compression_factor > 0.0 && self.compression_factor <= 1.0) {
            return Err(invalid("LOOM_WRITE_COMPRESSION_FACTOR", "must be in (0, 1]"));
        }
        Ok(())
    }
}
```

> **Clippy note:** the `compression_factor` comparison may trip `clippy::float_cmp` style lints — the range check `> 0.0 && <= 1.0` is comparisons, not equality, so it is fine. If `clippy::manual_range_contains` fires, rewrite as appropriate or `#[expect(...)]` with a reason.

Update `src/services/datafusion-io/BUCK`: add `"//src/loom-config:loom-config"` and `"//third-party:serde"` to the `datafusion-io` lib `deps`, and add a `rust_test` target `write-tuning` (crate `write_tuning`, src `tests/write_tuning.rs`, deps `[":datafusion-io", "//third-party:serde_json"]`). **Confirm `//third-party:serde` carries the `derive` feature** (it must, for `#[derive(serde::Serialize, serde::Deserialize)]`) — `ingest` and `core` already depend on `//third-party:serde` for derives, so the vendored target has `derive` enabled; if the build errors on `serde::Serialize` not found, that is the signal (reviewer note, blocking fix #3).

- [ ] **Step 4: Run to verify pass + the existing write tests still pass**

Run: `buck2 test //src/services/datafusion-io/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (write-tuning + existing write/infer/single-file tests).

- [ ] **Step 5: Clippy + commit**

```bash
bash tools/clippy-all.sh > /tmp/cl.log 2>&1; grep -iE "warning|error" /tmp/cl.log | head
git add -A
git commit -m "feat(config): make datafusion-io WriteConfig file/env-configurable"
```

---

## Task 4: `RoutingTuning` in ingest

**Files:**
- Create: `src/services/ingest/src/config.rs`
- Modify: `src/services/ingest/src/lib.rs` (add `pub mod config;` + re-export)
- Modify: `src/services/ingest/BUCK` (add loom-config dep; new test target)
- Test: `src/services/ingest/tests/routing_tuning.rs`

**Interfaces:**
- Consumes: `loom_config::{ConfigError, overlay_opt}`.
- Produces:
  - `pub struct RoutingTuning { pub inline_byte_limit: usize, pub flush_byte_threshold: i64 }` — `Default` = `{ 16 * 1024 * 1024, 64 * 1024 * 1024 }`; derives `Clone, Copy, Debug, serde::Serialize, serde::Deserialize` + `#[serde(default)]`.
  - `pub fn overlay_env(&mut self, vars) -> Result<(), ConfigError>` — keys `LOOM_INLINE_BYTE_LIMIT` (usize), `LOOM_FLUSH_BYTE_THRESHOLD` (i64).
  - `pub fn validate(&self) -> Result<(), ConfigError>` — both `>= 1`.

- [ ] **Step 1: Write the failing test** — create `src/services/ingest/tests/routing_tuning.rs`:

```rust
//! Config behaviour of `RoutingTuning` (inline/flush byte routing knobs).
use std::collections::HashMap;

use ingest::config::RoutingTuning;

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
}

#[test]
fn defaults_match_constants() {
    let r = RoutingTuning::default();
    assert_eq!(r.inline_byte_limit, 16 * 1024 * 1024);
    assert_eq!(r.flush_byte_threshold, 64 * 1024 * 1024);
}

#[test]
fn partial_json_falls_to_default() {
    let r: RoutingTuning = serde_json::from_str(r#"{"inline_byte_limit": 1024}"#).unwrap();
    assert_eq!(r.inline_byte_limit, 1024);
    assert_eq!(r.flush_byte_threshold, 64 * 1024 * 1024);
}

#[test]
fn env_overrides_file() {
    let mut r: RoutingTuning = serde_json::from_str(r#"{"inline_byte_limit": 1024}"#).unwrap();
    r.overlay_env(&map(&[("LOOM_INLINE_BYTE_LIMIT", "2048")])).unwrap();
    assert_eq!(r.inline_byte_limit, 2048);
}

#[test]
fn file_survives_when_env_silent() {
    let mut r: RoutingTuning = serde_json::from_str(r#"{"flush_byte_threshold": 99}"#).unwrap();
    r.overlay_env(&map(&[])).unwrap();
    assert_eq!(r.flush_byte_threshold, 99);
}

#[test]
fn malformed_env_is_error_naming_key() {
    let mut r = RoutingTuning::default();
    let err = r.overlay_env(&map(&[("LOOM_INLINE_BYTE_LIMIT", "abc")])).unwrap_err();
    assert!(format!("{err}").contains("LOOM_INLINE_BYTE_LIMIT"));
}

#[test]
fn validate_rejects_zero() {
    let r: RoutingTuning = serde_json::from_str(r#"{"inline_byte_limit": 0}"#).unwrap();
    assert!(r.validate().is_err());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/ingest:routing-tuning > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|Error" /tmp/t.log`
Expected: FAIL.

- [ ] **Step 3: Implement** — create `src/services/ingest/src/config.rs`:

```rust
//! Routing tuning shared by ingest and query-api: the in-memory byte thresholds that
//! decide inline-vs-Parquet landing and when to enqueue a flush. Co-located with the
//! landing materializer that consumes them; query-api reads this via its ingest dep.

use std::collections::HashMap;

use loom_config::{ConfigError, invalid, overlay_opt};

/// Inline/flush byte routing knobs.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct RoutingTuning {
    /// In-memory (uncompressed) Arrow byte size at/below which a request inlines
    /// (mirror-only rows) instead of writing real Parquet.
    pub inline_byte_limit: usize,
    /// Live-inline-byte total at/above which a `flush_table` job is enqueued.
    pub flush_byte_threshold: i64,
}

impl Default for RoutingTuning {
    fn default() -> Self {
        Self { inline_byte_limit: 16 * 1024 * 1024, flush_byte_threshold: 64 * 1024 * 1024 }
    }
}

impl RoutingTuning {
    /// Apply `LOOM_INLINE_BYTE_LIMIT` / `LOOM_FLUSH_BYTE_THRESHOLD` over the current values.
    pub fn overlay_env(&mut self, vars: &HashMap<String, String>) -> Result<(), ConfigError> {
        overlay_opt(vars, "LOOM_INLINE_BYTE_LIMIT", &mut self.inline_byte_limit)?;
        overlay_opt(vars, "LOOM_FLUSH_BYTE_THRESHOLD", &mut self.flush_byte_threshold)?;
        Ok(())
    }

    /// Validate: both thresholds must be positive.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.inline_byte_limit == 0 {
            return Err(invalid("LOOM_INLINE_BYTE_LIMIT", "must be >= 1"));
        }
        if self.flush_byte_threshold <= 0 {
            return Err(invalid("LOOM_FLUSH_BYTE_THRESHOLD", "must be >= 1"));
        }
        Ok(())
    }
}
```

Add to `src/services/ingest/src/lib.rs` (with the other `pub mod`s): `pub mod config;` and optionally `pub use config::RoutingTuning;`.

Update `src/services/ingest/BUCK`: add `"//src/loom-config:loom-config"` to the `ingest` lib deps (serde is already present), and add a `rust_test` target `routing-tuning` (crate `routing_tuning`, src `tests/routing_tuning.rs`, deps `[":ingest", "//third-party:serde_json"]`).

- [ ] **Step 4: Run to verify pass**

Run: `buck2 test //src/services/ingest:routing-tuning > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Clippy + commit**

```bash
bash tools/clippy-all.sh > /tmp/cl.log 2>&1; grep -iE "warning|error" /tmp/cl.log | head
git add -A
git commit -m "feat(config): add ingest RoutingTuning struct"
```

---

## Task 5: `WorkerTuning` — finalize + tests

**Files:**
- Modify: `src/loom-config/src/worker.rs` (finalize per the Task 1 NOTE: rename ceiling to `_SECS`)
- Test: `src/loom-config/tests/worker_tuning.rs` (replace placeholder)

**Interfaces:** as the Task 1 stub, with the env var `LOOM_WORKER_BACKOFF_CEILING_SECS` (seconds) for unit consistency. Final field names: `poll_interval_ms: u64`, `backoff_ceiling_secs: u64`, `backoff_max_attempts: u32`. Methods: `poll_interval() -> Duration`, `backoff(attempts: i32) -> Duration`, `overlay_env`, `validate`.

- [ ] **Step 1: Replace placeholder test** — `src/loom-config/tests/worker_tuning.rs`:

```rust
//! Config behaviour of `WorkerTuning` (poll interval + backoff bounds).
use std::collections::HashMap;
use std::time::Duration;

use loom_config::WorkerTuning;

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
}

#[test]
fn defaults_match_constants() {
    let w = WorkerTuning::default();
    assert_eq!(w.poll_interval(), Duration::from_secs(5));
    assert_eq!(w.backoff_ceiling_secs, 60);
    assert_eq!(w.backoff_max_attempts, 6);
}

#[test]
fn backoff_matches_current_worker_formula() {
    // The worker handler's current backoff: 1s, 2s, 4s, 8s, 16s, 32s, then capped 60s.
    let w = WorkerTuning::default();
    let got: Vec<u64> = (0..8).map(|a| w.backoff(a).as_secs()).collect();
    assert_eq!(got, vec![1, 2, 4, 8, 16, 32, 60, 60]);
}

#[test]
fn partial_json_falls_to_default() {
    let w: WorkerTuning = serde_json::from_str(r#"{"poll_interval_ms": 250}"#).unwrap();
    assert_eq!(w.poll_interval_ms, 250);
    assert_eq!(w.backoff_max_attempts, 6);
}

#[test]
fn env_overrides_file() {
    let mut w: WorkerTuning = serde_json::from_str(r#"{"poll_interval_ms": 250}"#).unwrap();
    w.overlay_env(&map(&[("LOOM_WORKER_POLL_INTERVAL_MS", "1000")])).unwrap();
    assert_eq!(w.poll_interval_ms, 1000);
}

#[test]
fn env_backoff_keys_apply() {
    let mut w = WorkerTuning::default();
    w.overlay_env(&map(&[
        ("LOOM_WORKER_BACKOFF_CEILING_SECS", "30"),
        ("LOOM_WORKER_BACKOFF_MAX_ATTEMPTS", "4"),
    ])).unwrap();
    assert_eq!(w.backoff_ceiling_secs, 30);
    assert_eq!(w.backoff_max_attempts, 4);
}

#[test]
fn malformed_env_is_error_naming_key() {
    let mut w = WorkerTuning::default();
    let err = w.overlay_env(&map(&[("LOOM_WORKER_POLL_INTERVAL_MS", "soon")])).unwrap_err();
    assert!(format!("{err}").contains("LOOM_WORKER_POLL_INTERVAL_MS"));
}

#[test]
fn validate_rejects_zero() {
    let w: WorkerTuning = serde_json::from_str(r#"{"poll_interval_ms": 0}"#).unwrap();
    assert!(w.validate().is_err());
    let w2: WorkerTuning = serde_json::from_str(r#"{"backoff_max_attempts": 0}"#).unwrap();
    assert!(w2.validate().is_err());
}
```

- [ ] **Step 2: Run to verify failure** (env var name mismatch, etc.)

Run: `buck2 test //src/loom-config:worker-tuning > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|Error" /tmp/t.log`
Expected: FAIL.

- [ ] **Step 3: Finalize `worker.rs`** — change the ceiling env key to `LOOM_WORKER_BACKOFF_CEILING_SECS` and clean the provisional `.map(|()| ())?` line:

```rust
    pub fn overlay_env(&mut self, vars: &HashMap<String, String>) -> Result<(), ConfigError> {
        overlay_opt(vars, "LOOM_WORKER_POLL_INTERVAL_MS", &mut self.poll_interval_ms)?;
        overlay_opt(vars, "LOOM_WORKER_BACKOFF_CEILING_SECS", &mut self.backoff_ceiling_secs)?;
        overlay_opt(vars, "LOOM_WORKER_BACKOFF_MAX_ATTEMPTS", &mut self.backoff_max_attempts)?;
        Ok(())
    }
```

Update the doc-comment on `backoff_ceiling_secs` to name `LOOM_WORKER_BACKOFF_CEILING_SECS`.

- [ ] **Step 4: Run to verify pass**

Run: `buck2 test //src/loom-config:worker-tuning > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (8 tests).

- [ ] **Step 5: Clippy + commit**

```bash
bash tools/clippy-all.sh > /tmp/cl.log 2>&1; grep -iE "warning|error" /tmp/cl.log | head
git add -A
git commit -m "feat(config): finalize WorkerTuning (poll interval + backoff bounds)"
```

---

## Task 6: `ServingTuning` in query-api + thread `default_limit`

**Files:**
- Create: `src/services/query-api/src/config.rs`
- Modify: `src/services/query-api/src/lib.rs` (`pub mod config;`)
- Modify: `src/services/query-api/src/http.rs` (`AppState.default_limit`)
- Modify: `src/services/query-api/src/handler.rs` (use `default_limit` from state instead of the `const`)
- Modify: `src/services/query-api/BUCK`
- Test: `src/services/query-api/tests/serving_tuning.rs`

**Interfaces:**
- Produces: `pub struct ServingTuning { pub default_limit: u32 }` — `Default` = `{ 1000 }`; `Clone, Copy, Debug, serde::Serialize, serde::Deserialize` + `#[serde(default)]`; `overlay_env` (key `LOOM_SERVING_DEFAULT_LIMIT`, u32); `validate` (`>= 1`).
- `AppState` gains `pub default_limit: u32`.

- [ ] **Step 1: Write the failing test** — `src/services/query-api/tests/serving_tuning.rs` (mirror the `routing_tuning.rs` shape; keys/types: `LOOM_SERVING_DEFAULT_LIMIT` u32, default 1000):

```rust
//! Config behaviour of `ServingTuning` (default page size).
use std::collections::HashMap;

use query_api::config::ServingTuning;

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
}

#[test]
fn default_is_1000() {
    assert_eq!(ServingTuning::default().default_limit, 1000);
}

#[test]
fn partial_json_and_env_override() {
    let mut s: ServingTuning = serde_json::from_str(r#"{}"#).unwrap();
    assert_eq!(s.default_limit, 1000);
    s.overlay_env(&map(&[("LOOM_SERVING_DEFAULT_LIMIT", "50")])).unwrap();
    assert_eq!(s.default_limit, 50);
}

#[test]
fn malformed_env_is_error_naming_key() {
    let mut s = ServingTuning::default();
    let err = s.overlay_env(&map(&[("LOOM_SERVING_DEFAULT_LIMIT", "x")])).unwrap_err();
    assert!(format!("{err}").contains("LOOM_SERVING_DEFAULT_LIMIT"));
}

#[test]
fn validate_rejects_zero() {
    let s: ServingTuning = serde_json::from_str(r#"{"default_limit": 0}"#).unwrap();
    assert!(s.validate().is_err());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/query-api:serving-tuning > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|Error" /tmp/t.log`
Expected: FAIL.

- [ ] **Step 3: Implement** — create `src/services/query-api/src/config.rs`:

```rust
//! Serving tuning for query-api: the default page size applied when a caller omits `limit`.

use std::collections::HashMap;

use loom_config::{ConfigError, invalid, overlay_opt};

/// Read-serving tuning.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ServingTuning {
    /// Default page size when a request does not specify `limit`.
    pub default_limit: u32,
}

impl Default for ServingTuning {
    fn default() -> Self {
        Self { default_limit: 1000 }
    }
}

impl ServingTuning {
    pub fn overlay_env(&mut self, vars: &HashMap<String, String>) -> Result<(), ConfigError> {
        overlay_opt(vars, "LOOM_SERVING_DEFAULT_LIMIT", &mut self.default_limit)?;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.default_limit == 0 {
            return Err(invalid("LOOM_SERVING_DEFAULT_LIMIT", "must be >= 1"));
        }
        Ok(())
    }
}
```

Add `pub mod config;` to `src/services/query-api/src/lib.rs`.

Thread `default_limit` through `AppState` and the handlers:
- In `src/services/query-api/src/http.rs`, add `pub default_limit: u32,` to `AppState` (default `1000` at construction so existing test constructions can set it — see below).
- In `src/services/query-api/src/handler.rs`, replace the `const DEFAULT_LIMIT: u32 = 1000;` usages at the real call sites with the value carried from state. Each handler that calls `compile_select_with(..., DEFAULT_LIMIT)` has access to `st: &AppState` (or the per-request `deps`); pass `st.default_limit` instead. Keep the `const DEFAULT_LIMIT` ONLY if some non-state path still needs it; otherwise remove it.

> **Implementer:** read `handler.rs` around each `DEFAULT_LIMIT` site (lines ~306, 664, 760, 900, 1027, 1218) to see whether the enclosing fn has `st`/`deps` in scope. The `deps` struct (built with `serving: st.serving.as_ref()`) is the natural carrier — add a `default_limit: u32` field to that deps struct and populate from `st.default_limit`, then reference `deps.default_limit` at each `compile_select_with` call. Update every `AppState { .. }` construction in `src/` and in the e2e tests / `tests/e2e_support.rs` to add `default_limit: 1000` (grep `AppState {`).

Update `src/services/query-api/BUCK`: add `"//src/loom-config:loom-config"` to the `query-api` lib deps, and add a `rust_test` target `serving-tuning` (crate `serving_tuning`, src `tests/serving_tuning.rs`, deps `[":query-api", "//third-party:serde_json"]`).

- [ ] **Step 4: Run to verify pass (incl. existing e2e that construct AppState)**

Run: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Clippy + commit**

```bash
bash tools/clippy-all.sh > /tmp/cl.log 2>&1; grep -iE "warning|error" /tmp/cl.log | head
git add -A
git commit -m "feat(config): add query-api ServingTuning and thread default_limit"
```

---

## Task 7: ingest `main` composition (routing + write + file load)

**Files:**
- Modify: `src/services/ingest/src/main.rs`
- Modify: `src/services/ingest/src/materialize.rs` (accept `&WriteConfig` instead of `WriteConfig::default()`)
- Modify: `src/services/ingest/BUCK` (add loom-config dep to the binary if needed)

**Interfaces:**
- `IngestConfig { routing: RoutingTuning, write: WriteConfig }` with `#[derive(Default, serde::Deserialize)] #[serde(default)]` — define in `src/services/ingest/src/config.rs`.
- `ingest::materialize::land` / `materialize` gain a `write_cfg: &WriteConfig` param.

- [ ] **Step 1:** Add `IngestConfig` to `src/services/ingest/src/config.rs`:

```rust
/// The ingest binary's composed config: routing + write tuning. `#[serde(default)]` so a
/// partial config file deserializes (omitted domains fall to their `Default`).
#[derive(Default, serde::Deserialize)]
#[serde(default)]
pub struct IngestConfig {
    pub routing: RoutingTuning,
    pub write: datafusion_io::WriteConfig,
}
```

> Requires `ingest` to depend on `datafusion-io` (it already does). `WriteConfig` must be `Deserialize` (Task 3) and `Default` (yes). Add the import.

- [ ] **Step 2:** Thread `&WriteConfig` into `materialize::land`. Change the signature to add `write_cfg: &WriteConfig` and pass it to `write_dataset(...)` instead of `&WriteConfig::default()`. Update `materialize()` (the convenience wrapper) to accept + forward it, and update its test callers in `src/services/ingest/tests/materialize.rs` to pass `&WriteConfig::default()` (no behavior change — defaults preserved).

- [ ] **Step 3:** Rewrite the env-reading block in `src/services/ingest/src/main.rs` to compose `IngestConfig` from defaults < file < env:

```rust
    let env = service_runtime::env_map();
    let mut app_cfg = ingest::config::IngestConfig::default();
    if let Some(path) = env.get("LOOM_CONFIG_FILE") {
        let doc = std::fs::read_to_string(path)
            .map_err(|e| service_runtime::invalid("LOOM_CONFIG_FILE", e))?;
        app_cfg = service_runtime::parse_config_doc(&doc)?;
    }
    app_cfg.routing.overlay_env(&env)?;
    app_cfg.write.overlay_env(&env)?;
    app_cfg.routing.validate()?;
    app_cfg.write.validate()?;
```

Then build `IcebergMaterializer` with `inline_byte_limit: app_cfg.routing.inline_byte_limit, flush_byte_threshold: app_cfg.routing.flush_byte_threshold`, deleting the old lossy `std::env::var(...).ok().and_then(...)` reads and the `DEFAULT_INLINE_BYTE_LIMIT`/`DEFAULT_FLUSH_BYTE_THRESHOLD` consts. (The live HTTP landing path goes through the postgres iceberg writer; `app_cfg.write` is held for the `materialize`/datafusion write path and for forward-consistency — if `IcebergMaterializer` does not currently take a `WriteConfig`, do NOT add one to it this slice; the write knobs are validated + available, and `materialize::land` now accepts them. Document that the live HTTP path does not exercise `WriteConfig` — see the Design Decisions table.)

> **Implementer judgement:** the goal is "no behavior change at defaults" + "lossy reads gone." The minimum is: routing knobs flow from `IngestConfig` into `IcebergMaterializer` (replacing the lossy reads), and `WriteConfig` is composed + validated. Threading `app_cfg.write` into a live consumer is only required where one exists; `materialize::land` (Step 2) is that seam.

- [ ] **Step 4:** Build + the ingest e2e/runtime tests.

Run: `buck2 test //src/services/ingest/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Clippy + commit**

```bash
bash tools/clippy-all.sh > /tmp/cl.log 2>&1; grep -iE "warning|error" /tmp/cl.log | head
git add -A
git commit -m "feat(config): compose ingest config (routing+write) from file<env in main"
```

---

## Task 8: query-api `main` composition (routing + serving + file load)

**Files:**
- Modify: `src/services/query-api/src/main.rs`
- Modify: `src/services/query-api/src/config.rs` (`QueryApiConfig`)

**Interfaces:**
- `QueryApiConfig { routing: ingest::config::RoutingTuning, serving: ServingTuning }` with `#[derive(Default, serde::Deserialize)] #[serde(default)]`.

- [ ] **Step 1:** Add `QueryApiConfig` to `src/services/query-api/src/config.rs` (query-api already depends on `ingest`, so `ingest::config::RoutingTuning` is reachable):

```rust
#[derive(Default, serde::Deserialize)]
#[serde(default)]
pub struct QueryApiConfig {
    pub routing: ingest::config::RoutingTuning,
    pub serving: ServingTuning,
}
```

- [ ] **Step 2:** Rewrite the env block in `src/services/query-api/src/main.rs` to compose `QueryApiConfig` (defaults < file < env) exactly like Task 7 Step 3, then:
  - build `IcebergActionWriter::new(catalog, pool.clone(), cfg.routing.inline_byte_limit, cfg.routing.flush_byte_threshold)` (delete the lossy reads + consts),
  - set `AppState.default_limit = cfg.serving.default_limit`.

- [ ] **Step 3:** Build + e2e.

Run: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 4: Clippy + commit**

```bash
bash tools/clippy-all.sh > /tmp/cl.log 2>&1; grep -iE "warning|error" /tmp/cl.log | head
git add -A
git commit -m "feat(config): compose query-api config (routing+serving) from file<env in main"
```

---

## Task 9: worker `main` composition (worker tuning + write + backoff)

**Files:**
- Modify: `src/services/worker/src/main.rs`
- Modify: `src/services/worker/src/handler.rs` (backoff sourced from `WorkerTuning`)
- Modify: `src/services/worker/src/compact.rs` (`WriteConfig` from composed config)
- Modify: `src/services/worker/BUCK` (add `//src/loom-config:loom-config`)

**Interfaces:**
- `handler::backoff` becomes `handler::backoff(tuning: &WorkerTuning, attempts: i32) -> Duration` (or the handlers take a `WorkerTuning` and call `tuning.backoff(attempts)`).
- `Worker::new(...).with_poll_interval(tuning.poll_interval())`.

- [ ] **Step 1:** In `src/services/worker/src/handler.rs`, replace the free `backoff(attempts)` with `WorkerTuning::backoff`. Pass a `WorkerTuning` (Copy) into `handle_flush`/`handle_gc` (add a param), or thread it via the closure. At defaults the result is identical (1s,2s,…,60s).

> **Implementer:** `handle_flush(flush, job)` → `handle_flush(flush, &tuning, job)`; inside, `delay: tuning.backoff(job.attempts)`. Update the dispatch closure in `main.rs` to capture a `WorkerTuning` copy.

- [ ] **Step 2:** In `src/services/worker/src/main.rs`, build a `WorkerConfig { worker: WorkerTuning, write: WriteConfig }` (define a small struct in `main.rs` or `worker/src/lib.rs`) via defaults < file < env (use `loom_config::env_map`/`parse_config_doc`; worker-bin must NOT use `service_runtime` — call loom-config directly to stay postgres-free). Then:
  - `let worker = Worker::new(client, worker_id, lease).with_poll_interval(cfg.worker.poll_interval());`
  - pass `cfg.write` into `CompactCtx`/the compact path (replace `WriteConfig::default()` in `compact.rs` with the threaded value),
  - capture `cfg.worker` into the dispatch closure for backoff.

> **Worker zero-postgres guard:** verify with `buck2 uquery "deps(//src/services/worker:worker-bin)" 2>/dev/null | grep -i "control-plane/postgres"` → must be empty. `loom-config` has no postgres, so adding it is safe.

- [ ] **Step 3:** In `src/services/worker/src/compact.rs`, change the `&WriteConfig::default()` at line ~74 to use a `WriteConfig` carried on `CompactCtx` (add a `pub write_cfg: WriteConfig` field) populated from `cfg.write`. **Also** `compact.rs:22-33` has its OWN `retry()` with the same hardcoded `1u64.checked_shl(attempts.clamp(0,6)).unwrap_or(64).min(60)` backoff (a second worker-domain backoff site, in `worker-bin`'s compaction path). Add a `pub worker_tuning: WorkerTuning` field to `CompactCtx` and replace `compact.rs`'s `retry()` body with `self.worker_tuning.backoff(attempts)` so `LOOM_WORKER_BACKOFF_*` governs compaction retries too. At defaults this is byte-identical (`[1,2,4,8,16,32,60,…]`). This fully unifies the worker domain's backoff (the reviewer's blocking fix #2).

- [ ] **Step 4:** Build + worker e2e (fixture).

Run: `buck2 build //src/services/worker/... > /tmp/b.log 2>&1; grep -iE "error|BUILD FAILED|Build ID" /tmp/b.log | tail -3`
Run: `buck2 test //src/services/worker/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Verify guard + clippy + commit**

```bash
buck2 uquery "deps(//src/services/worker:worker-bin)" 2>/dev/null | grep -i "control-plane/postgres" || echo "GUARD OK: no postgres"
bash tools/clippy-all.sh > /tmp/cl.log 2>&1; grep -iE "warning|error" /tmp/cl.log | head
git add -A
git commit -m "feat(config): compose worker config (worker tuning+write) from file<env in main"
```

---

## Task 10: transform `main` composition (worker tuning + write)

**Files:**
- Modify: `src/services/transform/src/main.rs`
- Modify: `src/services/transform/BUCK` (add `//src/loom-config:loom-config`)

**Interfaces:**
- `TransformConfig { worker: WorkerTuning, write: WriteConfig }`.

- [ ] **Step 1:** In `src/services/transform/src/main.rs`, compose `TransformConfig` (defaults < file < env). transform-bin already depends on `service_runtime` (it is not zero-postgres), so it may use `service_runtime::{env_map, parse_config_doc}`. Then `Worker::new(pg, "transform-1", cfg.lock_timeout).with_poll_interval(tcfg.worker.poll_interval())`.

- [ ] **Step 2:** Thread `tcfg.write` into the transform run/compact paths that currently use `WriteConfig::default()` (`src/services/transform/src/run.rs:187`, `src/services/transform/src/compact.rs`). If threading the value all the way through the handler pipeline is invasive, scope this to passing it into the `CompactConfig`/run entry the handler constructs; otherwise leave `run.rs`'s internal default and document. **Minimum:** `poll_interval` is wired (no behavior change at default 5s). Write threading in transform is best-effort this slice — record any remainder in `docs/FUTURE.md` at finish.

- [ ] **Step 3:** Build + transform e2e (fixture).

Run: `buck2 test //src/services/transform/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 4: Clippy + commit**

```bash
bash tools/clippy-all.sh > /tmp/cl.log 2>&1; grep -iE "warning|error" /tmp/cl.log | head
git add -A
git commit -m "feat(config): compose transform config (worker tuning+write) from file<env in main"
```

---

## Task 11: Document guardrail consts + final full-suite gate

**Files:**
- Modify: source comments where the guardrail consts live (`MAX_CHAIN_DEPTH`, `MAX_GRAPH_DEPTH`, `DEFAULT_GRAPH_DEPTH`, `LOOM_DATASET_NAMESPACE`, `LOOM_TYPE_NAMESPACE`, `LOOM_STORE_URL`, `FLUSH_JOB_KIND`) — one line each noting they are deliberately NOT config.

- [ ] **Step 1:** Find the guardrail consts and add a one-line `// deliberately const, not config (operator must not tune): see road-config-seam-unification` comment to each (don't change values).

```bash
grep -rn "MAX_CHAIN_DEPTH\|MAX_GRAPH_DEPTH\|DEFAULT_GRAPH_DEPTH" src/ --include=*.rs | grep -v tests
```

- [ ] **Step 2: Full suite + lint gate**

Run:
```bash
buck2 build //src/... > /tmp/b.log 2>&1; grep -iE "BUILD FAILED|error:" /tmp/b.log | head
buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
bash tools/clippy-all.sh > /tmp/cl.log 2>&1; grep -iE "warning|error" /tmp/cl.log | head
buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -iE "Failed|error" /tmp/p.log | head
```
Expected: build clean, all tests pass, clippy clean, prek clean (commit any files prek fixes).

- [ ] **Step 3: Commit any prek fixups**

```bash
git add -A && git commit -m "docs(config): mark guardrail consts as deliberately non-config" || echo "nothing to commit"
```

---

## Finish

1. **`loom-docs-update`:** flip `road-config-seam-unification` to `- [x]` with `status:done`, `pr:#<n>`; record deferred remainders (transform backoff unification; transform write threading if incomplete) as `docs/FUTURE.md` entries cross-linked to the item. Stage alongside the work.
2. **`superpowers:finishing-a-development-branch`:** open a PR with head `work/road-config-seam-unification`; ensure `build-test`/`affected`/`lint` are green.

## Self-Review (completed by plan author)

**Spec coverage:** every scope-table knob has a struct + overlay + test + composition task (routing → T4/T7/T8; write → T3/T7/T9/T10; worker poll+backoff → T5/T9/T10; serving default_limit → T6/T8; max_connections → T2). File loading (`LOOM_CONFIG_FILE`, JSON, partial) → T7–T10 + `parse_config_doc` (T1). Lossy `.ok()` reads removed → T7/T8. ConfigError on bad file/env → T1 `overlay_opt`/`parse_config_doc` + per-struct validate. Guardrail consts documented → T11. `overlay_opt` test in service_runtime → T2.

**Placeholder scan:** no TBD/TODO; all code shown. The one judgement call (write threading depth in ingest/transform) is bounded with an explicit minimum + deferral note, not a placeholder.

**Type consistency:** `WriteConfig` (not WriteTuning) used throughout; `RoutingTuning`/`WorkerTuning`/`ServingTuning` field + method names consistent across struct defs, tests, and composition tasks; `overlay_env`/`validate`/`overlay_opt`/`parse_config_doc`/`env_map`/`invalid` signatures consistent.
