# road-service-bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Shared service bootstrap + config-parse helpers, rescoped to the
post-PR-#308 tree. `loom-config` gains the two snapshot primitives (`req_var`,
`parse_var`); `store-config` hosts `From<StoreConfigError> for ConfigError`;
`runtime::Config::from_map` (cc 35) decomposes onto `DbConfig::from_map` /
`EmbeddedSettings::from_map` / `parse_migrate_on_boot`; every remaining
silent-fallback / live-env config read converts to a fail-loud read over the
main's env snapshot (fixes `iss-config-silent-fallbacks`); and the three service
mains (query-api, ingest, engine) adopt one
`service_runtime::bootstrap(vars) -> Boot { Migrated | Ready(ServiceContext) }`
where `ServiceContext` owns the embedded-PG handle so the keep-alive is
structural, not a per-main `_pg` binding to remember.

**Rescope (binding — the spec § "road-service-bootstrap" predates PR #308):**

- **No `admin_subject` on `ServiceContext`.** The spec's
  config→pool→control-plane→auth→**bootstrap-admin** sequence lost its last leg
  when #308 moved first-admin creation to the `loom create-admin` CLI;
  `LOOM_BOOTSTRAP_ADMIN_*` is gone from the tree (verified: the only remaining
  mentions are a stale `docs/deploy.md` table row — removed in Task 7 — and
  historical plan/register prose). Do not reintroduce it.
- **Standalone does NOT adopt `bootstrap()`.** Its `run()` keeps
  `build_pool_managed` + `stop_pg` because it gracefully `shutdown().await`s the
  embedded PG with error folding on every exit path
  (`standalone/src/lib.rs:28-35,186-198`). Standalone only switches to the new
  snapshot readers, via a `StandaloneTuning` value parsed in its main.
- **Mains stay non-graceful.** Today the three mains drop `_pg` without calling
  `shutdown()`; `ServiceContext` ownership preserves exactly that. Do NOT add
  graceful embedded-PG shutdown to the mains (behavior-preserving).
- **Worker/transform stay out.** The zero-pool worker
  (`worker/src/main.rs`) already reads a snapshot; `transform/src/main.rs:46`
  keeps `Config::from_env` (which survives as the thin `from_map(env_map())`
  wrapper — transform is the legacy pillar, out of scope).

**Architecture (key decisions, verified against the tree):**

- **`From<StoreConfigError> for ConfigError` lives in `store-config`.**
  Dep-direction check: neither crate depends on the other today
  (`src/loom-config/BUCK` deps = serde/serde_json/thiserror only;
  `src/services/store-config/BUCK` deps = object_store/thiserror). The orphan
  rule allows the impl in either crate. `loom-config` is a deliberately *light
  leaf* (its module doc: postgres-free crates depend on it without pulling
  heavyweight deps) — giving it a `store-config` dep would drag `object_store`
  into `datafusion-io`/`worker-bin`'s closure via loom-config. So `store-config`
  gains a dep on `loom-config` (cheap: thiserror/serde only, no cycle — verified
  loom-config has no first-party deps) and hosts the impl next to
  `StoreConfigError`. This replaces the inline 10-line mapping at
  `src/services/runtime/src/lib.rs:174-183` (including its `Store(inner)` →
  `Invalid{var:"LOOM_WAREHOUSE_URI"}` leg, which the impl preserves). Both
  crates are **buck-only** (no `Cargo.toml` — verified), so there is **no
  cargo/lockfile/reindeer step anywhere in this plan**.
- **`engine::run` signature changes** from
  `run(listener, cfg, pool, ready, shutdown)` to
  `run(listener, cfg, pool, tuning: EngineTuning, ready, shutdown)`. Callers
  (complete list, verified by grep): `engine/src/main.rs:18` and
  `standalone/src/lib.rs:86`. **No engine test calls `run()`** (the eight
  `engine/tests/*.rs` files construct the tonic services directly). Both
  callers parse `EngineTuning::from_map(&env)` from their main's snapshot;
  `parse_env_or` (`engine/src/run.rs:92-101`) is deleted. Note the defect here
  was the **live-env read** (one-env-snapshot-per-main violation), not a silent
  fallback — `parse_env_or` already failed loud on malformed values; the
  register prose is corrected in Task 7.
- **Standalone's minimal-signature change is one value:**
  `StandaloneTuning { session_ttl, max_ttl, engine: EngineTuning }` with
  `from_map(vars)`, parsed in `standalone/src/main.rs` and threaded through
  `run()`/`serve_composite()`. This covers both the TTL readers *and* the
  engine tuning that `serve_composite`'s `engine::run` spawn now needs — one
  new parameter instead of three. Callers of `standalone::run` (complete list):
  `standalone/src/main.rs:53`, `tests/composite_e2e.rs:97`,
  `tests/composite_error_path.rs:68`.
- **`bootstrap()` parses the TTLs before the pool boots** (fail-fast: a typo'd
  TTL should not cost an embedded-PG initdb). Order inside `bootstrap`:
  `Config::from_map` → `migrate_requested` gate (`run_migrations` →
  `Boot::Migrated`) → TTL parse → `build_pool_managed` → `control_plane` →
  `AuthState`. The full config is still parsed *before* the migrate gate —
  identical to every main today.
- **`RuntimeError` gains `Config(#[from] ConfigError)`** so `bootstrap` has one
  error type and the mains keep their `Box<dyn Error>` returns unchanged.
- **Old lossy readers are deleted last** (Task 7), after every caller has
  migrated — the compiler proves the sweep is complete.

**Tech Stack:** Rust (edition 2024), buck2, `loom_rust_test` (pure) +
`loom_fixture_test` (hermetic Postgres, `PgFixture` not needed — the new
bootstrap test boots `EmbeddedPg` directly, mirroring
`runtime/tests/migrate_managed.rs`). No third-party dep changes, no
`Cargo.toml`/lockfile changes, `.sqlx` untouched.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md`
  (§ "road-service-bootstrap"), **as rescoped above** (the spec's
  `admin_subject: Option<SubjectId>` field is dropped). Registers:
  `docs/ROADMAP.md#road-service-bootstrap`,
  `docs/ISSUES.md#iss-config-silent-fallbacks`.
- **TDD:** every new fn/type lands with its test written first and observed red.
- Tests are separate `rust_test`/`loom_fixture_test` targets wired in the
  crate's BUCK — never inline `#[cfg(test)]`. New fixture tests MUST use
  `loom_fixture_test`.
- Clippy pedantic+restriction is on for prod code: no
  unwrap/expect/panic/indexing in `src/**.rs` (test targets get relaxations via
  the wrapper). `#[expect(lint, reason = "...")]` for local silences.
  **Known trap:** `ServiceContext` mixes `pub` fields with the private
  `_embedded` — if clippy fires `partial_pub_fields` (restriction, NOT in
  `CLIPPY_ALLOWS`), and/or `large_enum_variant` on `Boot`, add the documented
  `#[expect]` shown in Task 6's contingency (do not restructure).
- **Never pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to a
  file and grep it. Fixture runs use `-j 8` (postgres boot-slot starvation).
- Cloud session: build with `-M none` for any whole-tree check; scope tests to
  the touched packages (the Task 7 sweep lists them exactly).
- `buck2 run //tools:prek -- run --all-files` must report zero `Failed` before
  **every** commit (rustfmt is a separate hook from clippy). Conventional
  Commits messages.
- Behavior-preserving where pinned: all existing tests in
  `runtime/tests/{config,embedded_config,object_store_config,migrate_managed}.rs`,
  `standalone/tests/{composite_e2e,composite_error_path}.rs`, and the
  ingest/query-api e2e suites must stay green **unmodified except for the
  `standalone::run` signature** (the only test files a signature change
  touches; verified by grepping every changed symbol).

## Complete call-site inventory (verified by grep; the plan updates every one)

| Symbol | Call sites | Updated in |
| --- | --- | --- |
| `session_ttl_from_env` | `query-api/src/main.rs:24`, `ingest/src/main.rs:23`, `standalone/src/lib.rs:54` | Tasks 5 (standalone), 6 (mains), deleted Task 7 |
| `service_token_max_ttl_from_env` | `ingest/src/main.rs:25`, `query-api/src/serve.rs:51`, `standalone/src/lib.rs:56` | Tasks 4 (serve.rs), 5 (standalone), 6 (ingest), deleted Task 7 |
| `migrate_requested` (live-env) | `query-api/src/main.rs:9`, `ingest/src/main.rs:10`, `engine/src/main.rs:7`, `standalone/src/main.rs:28` | Task 6 (signature → `(vars)`; three mains via `bootstrap`, standalone main direct) |
| `parse_env_or` (live-env) | `engine/src/run.rs:51-52` (def :92-101) | Task 5 (deleted; `EngineTuning::from_map` in callers) |
| `Config::from_map` | prod: `runtime/src/lib.rs:236` (`from_env`), `standalone/src/main.rs:29,50,78`; tests: `runtime/tests/config.rs` (×14), `embedded_config.rs` (×2), `object_store_config.rs` (×8), `standalone/tests/composite_e2e.rs:41`, `composite_error_path.rs:42` | Task 3 (decomposed internally — zero caller changes) |
| `Config::from_env` | `query-api/src/main.rs:7`, `ingest/src/main.rs:9`, `engine/src/main.rs:6`, `transform/src/main.rs:46` | Task 6 (three mains → `bootstrap`); transform keeps it (out of scope) |
| `build_pool_managed` | `query-api/src/main.rs:17`, `ingest/src/main.rs:14`, `engine/src/main.rs:11`, `standalone/src/lib.rs:31`, `runtime/tests/migrate_managed.rs:89,113` | Task 6 (three mains via `bootstrap`); standalone + tests unchanged |
| `LOOM_ENGINE_SOCKET` (live-env) | `query-api/src/main.rs:27`, `engine/src/main.rs:13` (snapshot-correct already: `standalone/src/main.rs:98-101`, `worker/src/main.rs:54`) | Tasks 5 (engine main), 6 (query-api main) — `req_var` over the snapshot |
| `LOOM_UI_DIR` / `LOOM_CORS_ALLOWED_ORIGINS` / `LOOM_FLIGHT_BIND_ADDR` / `LOOM_EXPORT_MAX_ROWS` (live-env; the last is a lossy parse) | `query-api/src/serve.rs:83,88,93,111` — **found during plan verification, not enumerated by the issue**; serve.rs already holds the `env` snapshot | Task 4 (snapshot sweep; `parse_var` for the lossy one) |
| `engine::run` | `engine/src/main.rs:18`, `standalone/src/lib.rs:86` (no test callers) | Task 5 |
| `standalone::run` | `standalone/src/main.rs:53`, `tests/composite_e2e.rs:97`, `tests/composite_error_path.rs:68` | Task 5 |

## Deliberate behavior changes (everything else is byte-identical)

1. Malformed `LOOM_SESSION_TTL_SECS` / `LOOM_SERVICE_TOKEN_MAX_TTL` now **fail
   startup** naming the key (was: silent fallback to 86400s / 90d). Absent keys
   keep the identical defaults. This *is* the iss-config-silent-fallbacks fix.
   The engine main previously never read either key at all; via `bootstrap` it
   now parses both, so a malformed value newly fails engine startup too.
2. `LOOM_INLINE_BYTE_LIMIT` / `LOOM_FLUSH_BYTE_THRESHOLD` parse at main startup
   from the snapshot (was: live env inside `engine::run`, after two catalog
   builds). Malformed values already errored; they now error earlier with the
   `ConfigError::Invalid` text. Defaults identical (16 MiB / 64 MiB).
3. Missing `LOOM_ENGINE_SOCKET` in the query-api/engine mains reports
   `missing required environment variable: LOOM_ENGINE_SOCKET` (was: a custom
   string / a raw `VarError`). No test pins the old strings (verified).
4. When *multiple* config keys are simultaneously invalid, which error surfaces
   first may shift (from_map now parses in composed order). No test pins
   multi-error precedence (each existing test corrupts exactly one key).
5. Malformed `LOOM_EXPORT_MAX_ROWS` now fails query-api startup **when the
   flight export listener is enabled** (`LOOM_FLIGHT_BIND_ADDR` set) — was: a
   silent fallback to 1 000 000. The `LOOM_UI_DIR` / `LOOM_CORS_ALLOWED_ORIGINS`
   / `LOOM_FLIGHT_BIND_ADDR` reads move to the snapshot with identical
   semantics (verified: no test calls `query_api::serve` — the only callers are
   the query-api main and the standalone composite, and the flight-export e2e
   drives `FlightExportService` directly).

---

### Task 1: `loom-config` snapshot primitives — `req_var` + `parse_var`

**Files:**
- Modify: `src/loom-config/src/lib.rs`
- Test: `src/loom-config/tests/overlay.rs` (existing target
  `//src/loom-config:overlay` — no BUCK change)

**Interfaces:**
- Produces: `pub fn req_var(vars, key) -> Result<String, ConfigError>` and
  `pub fn parse_var<T: FromStr>(vars, key, default: T) -> Result<T, ConfigError>`.
- Consumed by Tasks 3–6 (runtime, engine) via the `service_runtime` re-export.

- [x] **Step 1: Write the failing tests**

Append to `src/loom-config/tests/overlay.rs`, and extend its import line to
`use loom_config::{ConfigError, env_map, overlay_opt, parse_config_doc, parse_var, req_var};`:

```rust
#[test]
fn req_var_present_returns_value() {
    let vars = map(&[("LOOM_X", "hello")]);
    assert_eq!(req_var(&vars, "LOOM_X").unwrap(), "hello");
}

#[test]
fn req_var_missing_is_missing_var_error() {
    let vars = map(&[]);
    let err = req_var(&vars, "LOOM_X").unwrap_err();
    assert!(matches!(err, ConfigError::MissingVar(k) if k == "LOOM_X"));
}

#[test]
fn parse_var_absent_returns_default() {
    let vars = map(&[]);
    assert_eq!(parse_var(&vars, "LOOM_X", 42_u64).unwrap(), 42);
}

#[test]
fn parse_var_present_parses() {
    let vars = map(&[("LOOM_X", "7")]);
    assert_eq!(parse_var(&vars, "LOOM_X", 42_u64).unwrap(), 7);
}

#[test]
fn parse_var_malformed_is_error_naming_key() {
    let vars = map(&[("LOOM_X", "abc")]);
    let err = parse_var(&vars, "LOOM_X", 42_u64).unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { ref var, .. } if var == "LOOM_X"));
}
```

- [x] **Step 2: Run to see them fail**

Run: `buck2 test //src/loom-config:overlay > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t1.log`
Expected: FAIL — compile error (`req_var`/`parse_var` unresolved).

- [x] **Step 3: Implement**

Add to `src/loom-config/src/lib.rs` (after `overlay_opt`, matching its style):

```rust
/// Read a required key from the env snapshot; `ConfigError::MissingVar` naming the
/// key when absent. The `req` closure `Config::from_map` used to hand-roll.
pub fn req_var(vars: &HashMap<String, String>, key: &str) -> Result<String, ConfigError> {
    vars.get(key)
        .cloned()
        .ok_or_else(|| ConfigError::MissingVar(key.to_string()))
}

/// Parse `vars[key]`, falling back to `default` only when the key is ABSENT. A
/// present-but-malformed value is `ConfigError::Invalid` naming the key — a startup
/// error, never a silent fallback (iss-config-silent-fallbacks). The by-value
/// sibling of [`overlay_opt`] for callers building a value instead of patching a slot.
pub fn parse_var<T>(vars: &HashMap<String, String>, key: &str, default: T) -> Result<T, ConfigError>
where
    T: FromStr,
    T::Err: core::fmt::Display,
{
    match vars.get(key) {
        Some(raw) => raw.parse().map_err(|e| invalid(key, e)),
        None => Ok(default),
    }
}
```

- [x] **Step 4: Run to green + clippy**

Run: `buck2 test //src/loom-config:overlay > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS.
Run: `buck2 build '//src/loom-config:loom-config[clippy.txt]' > /tmp/c1.log 2>&1` — artifact empty.

- [x] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p1.log 2>&1; grep -c Failed /tmp/p1.log` — expected `0`.

```bash
git add src/loom-config
git commit -m "feat(loom-config): req_var + parse_var snapshot primitives

req_var replaces the req closure Config::from_map hand-rolls; parse_var is
the by-value sibling of overlay_opt (absent => default, present-but-malformed
=> ConfigError::Invalid naming the key — fail-loud, never a silent fallback).

Part of road-service-bootstrap."
```

---

### Task 2: `From<StoreConfigError> for ConfigError` in store-config

**Files:**
- Modify: `src/services/store-config/src/lib.rs` (the impl)
- Modify: `src/services/store-config/BUCK` (lib + test deps gain loom-config;
  test target gains object_store)
- Modify: `src/services/runtime/src/lib.rs:174-183` (inline mapping →
  `ConfigError::from`)
- Test: `src/services/store-config/tests/config.rs` (existing target
  `//src/services/store-config:config`)

**Interfaces:**
- Produces: `impl From<StoreConfigError> for loom_config::ConfigError` (orphan
  rule satisfied: `StoreConfigError` is local to store-config).
- Consumed by: `Config::from_map` (this task) — and any future
  `ObjectStoreConfig` caller that surfaces `ConfigError`.

- [x] **Step 1: Write the failing test**

Append to `src/services/store-config/tests/config.rs` (add
`use store_config::StoreConfigError;` to its imports):

```rust
#[test]
fn store_config_error_converts_to_config_error() {
    use loom_config::ConfigError;
    let missing: ConfigError = StoreConfigError::Missing("AWS_ACCESS_KEY_ID".into()).into();
    assert!(matches!(missing, ConfigError::MissingVar(k) if k == "AWS_ACCESS_KEY_ID"));

    let invalid: ConfigError = StoreConfigError::Invalid {
        var: "LOOM_WAREHOUSE_URI".into(),
        detail: "bad scheme".into(),
    }
    .into();
    assert!(matches!(invalid, ConfigError::Invalid { ref var, ref detail }
        if var == "LOOM_WAREHOUSE_URI" && detail == "bad scheme"));

    // Store errors carry no key of their own; they pin to the warehouse URI that
    // selected the backend — same mapping runtime's from_map used inline.
    let store: ConfigError = StoreConfigError::Store(object_store::Error::Generic {
        store: "test",
        source: "boom".into(),
    })
    .into();
    assert!(matches!(store, ConfigError::Invalid { ref var, .. } if var == "LOOM_WAREHOUSE_URI"));
}
```

BUCK wiring in `src/services/store-config/BUCK` — the library:

```python
cargo.rust_library(
    name = "store-config",
    crate = "store_config",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//src/loom-config:loom-config",
        "//third-party:object_store",
        "//third-party:thiserror",
    ],
    visibility = ["PUBLIC"],
)
```

and the test target:

```python
rust_test(
    name = "config",
    crate = "config",
    srcs = ["tests/config.rs"],
    crate_root = "tests/config.rs",
    edition = "2024",
    deps = [
        ":store-config",
        "//src/loom-config:loom-config",
        "//third-party:object_store",
    ],
)
```

- [x] **Step 2: Run to see it fail**

Run: `buck2 test //src/services/store-config:config > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t2.log`
Expected: FAIL — no `From<StoreConfigError>` impl.

- [x] **Step 3: Implement the impl and switch runtime's mapping**

In `src/services/store-config/src/lib.rs`, directly below the
`StoreConfigError` enum:

```rust
/// Config-seam bridge: an object-store parse/build failure surfaces as the shared
/// `loom_config::ConfigError` (replacing the inline mapping `Config::from_map`
/// carried). `Store` errors have no more specific key than the warehouse URI
/// that selected the backend.
impl From<StoreConfigError> for loom_config::ConfigError {
    fn from(e: StoreConfigError) -> Self {
        match e {
            StoreConfigError::Missing(k) => loom_config::ConfigError::MissingVar(k),
            StoreConfigError::Invalid { var, detail } => {
                loom_config::ConfigError::Invalid { var, detail }
            }
            StoreConfigError::Store(inner) => loom_config::ConfigError::Invalid {
                var: "LOOM_WAREHOUSE_URI".into(),
                detail: inner.to_string(),
            },
        }
    }
}
```

In `src/services/runtime/src/lib.rs`, replace lines 174-183 (the
`.map_err(|e| match e { … })` block) with:

```rust
        let object_store = ObjectStoreConfig::parse(vars, &data_path)?;
```

(`?` uses the new `From` impl — `from_map` returns `Result<_, ConfigError>`.)

- [x] **Step 4: Run to green**

Run: `buck2 test //src/services/store-config:config //src/services/runtime:object-store-config //src/services/runtime:config > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS (the runtime object-store tests pin the mapping's observable
behavior — `Missing` → error on absent AWS keys, etc.).
Run: `buck2 build '//src/services/store-config:store-config[clippy.txt]' '//src/services/runtime:runtime[clippy.txt]' > /tmp/c2.log 2>&1` — artifacts empty.

- [x] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p2.log 2>&1; grep -c Failed /tmp/p2.log` — expected `0`.

```bash
git add src/services/store-config src/services/runtime/src/lib.rs
git commit -m "feat(store-config): From<StoreConfigError> for ConfigError

Hosted in store-config (loom-config stays a light leaf crate — giving it a
store-config dep would drag object_store into every loom-config consumer;
neither crate depended on the other, so no cycle). Replaces the inline
10-line mapping in runtime's Config::from_map, preserving the
Store -> Invalid{LOOM_WAREHOUSE_URI} leg.

Part of road-service-bootstrap."
```

---

### Task 3: Decompose `Config::from_map` onto per-domain parsers

**Files:**
- Modify: `src/services/runtime/src/lib.rs:131-238` (`from_map` + new fns;
  imports: add `Path` to the `std::path` import, `parse_var, req_var` to the
  `loom_config` re-export)
- Test: `src/services/runtime/tests/config.rs`, `tests/embedded_config.rs`
  (existing targets `//src/services/runtime:config`, `:embedded-config` — no
  BUCK change)

**Interfaces:**
- Produces: `pub fn DbConfig::from_map(vars) -> Result<DbConfig, ConfigError>`,
  `pub fn EmbeddedSettings::from_map(vars, data_path: &Path) -> Result<Option<EmbeddedSettings>, ConfigError>`,
  `pub fn parse_migrate_on_boot(vars) -> Result<bool, ConfigError>`;
  `Config::from_map` becomes a ~14-line composition. `service_runtime`
  re-exports `req_var`/`parse_var`.
- Behavior invariant: every existing test in `config.rs` /
  `embedded_config.rs` / `object_store_config.rs` passes **unmodified** —
  same variants, same `var` names, same `LOOM_DB_MIGRATE_ON_BOOT` message.

- [x] **Step 1: Write the failing tests**

Append to `src/services/runtime/tests/config.rs` (its imports already cover
`Config, ConfigError, DbConfig`):

```rust
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
```

Append to `src/services/runtime/tests/embedded_config.rs` (add
`use service_runtime::EmbeddedSettings;` and `use std::path::Path;` to its
imports):

```rust
#[test]
fn embedded_settings_from_map_is_none_in_external_mode() {
    let s = EmbeddedSettings::from_map(&base(), Path::new("/tmp/loomdata")).expect("parse");
    assert!(s.is_none());
}

#[test]
fn embedded_settings_from_map_derives_dirs_from_data_path() {
    let mut vars = base();
    vars.insert("LOOM_PG_MODE".into(), "embedded".into());
    vars.insert("LOOM_PG_BIN_DIR".into(), "/opt/pg/bin".into());
    let e = EmbeddedSettings::from_map(&vars, Path::new("/tmp/loomdata"))
        .expect("parse")
        .expect("embedded settings present");
    assert_eq!(e.cfg.bin_dir, std::path::PathBuf::from("/opt/pg/bin"));
    assert_eq!(e.cfg.data_dir, std::path::PathBuf::from("/tmp/loomdata/pgdata"));
    assert_eq!(e.cfg.socket_dir, std::path::PathBuf::from("/tmp/loomdata/pgrun"));
    assert_eq!(e.cfg.database, "loom");
}
```

- [x] **Step 2: Run to see them fail**

Run: `buck2 test //src/services/runtime:config //src/services/runtime:embedded-config > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t3.log`
Expected: FAIL — compile errors (the three fns don't exist).

- [x] **Step 3: Implement the decomposition**

In `src/services/runtime/src/lib.rs`:

1. Imports: change `use std::path::PathBuf;` to
   `use std::path::{Path, PathBuf};` and extend the loom-config re-export to

```rust
pub use loom_config::{
    ConfigError, LayeredConfig, env_map, invalid, load, overlay_opt, parse_config_doc,
    parse_var, req_var,
};
```

2. Inside `impl EmbeddedSettings` (add an impl block below the struct):

```rust
impl EmbeddedSettings {
    /// Parse the embedded-PG settings from the env snapshot: `Some` only when
    /// `LOOM_PG_MODE=embedded` (anything else, including absent, is external
    /// mode). The data/socket dirs derive from `data_path` (`pgdata`/`pgrun`).
    pub fn from_map(
        vars: &HashMap<String, String>,
        data_path: &Path,
    ) -> Result<Option<EmbeddedSettings>, ConfigError> {
        if vars.get("LOOM_PG_MODE").map(String::as_str) != Some("embedded") {
            return Ok(None);
        }
        Ok(Some(EmbeddedSettings {
            cfg: managed_postgres::EmbeddedPgConfig {
                bin_dir: PathBuf::from(req_var(vars, "LOOM_PG_BIN_DIR")?),
                ld_library_path: vars
                    .get("LOOM_PG_LD_LIBRARY_PATH")
                    .cloned()
                    .unwrap_or_default(),
                data_dir: data_path.join("pgdata"),
                socket_dir: data_path.join("pgrun"),
                database: req_var(vars, "LOOM_DB_NAME")?,
            },
        }))
    }
}
```

3. Inside `impl DbConfig` (alongside `pg_connect_options`/`pg_url`):

```rust
    /// Parse the discrete `LOOM_DB_*` connection fields from the env snapshot.
    pub fn from_map(vars: &HashMap<String, String>) -> Result<DbConfig, ConfigError> {
        let max_connections = match vars.get("LOOM_DB_MAX_CONNECTIONS") {
            Some(s) => Some(
                s.parse::<u32>()
                    .map_err(|e| invalid("LOOM_DB_MAX_CONNECTIONS", e))?,
            ),
            None => None,
        };
        Ok(DbConfig {
            host: req_var(vars, "LOOM_DB_HOST")?,
            port: req_var(vars, "LOOM_DB_PORT")?
                .parse::<u16>()
                .map_err(|e| invalid("LOOM_DB_PORT", e))?,
            user: req_var(vars, "LOOM_DB_USER")?,
            password: req_var(vars, "LOOM_DB_PASSWORD")?,
            dbname: req_var(vars, "LOOM_DB_NAME")?,
            max_connections,
        })
    }
```

4. A free fn next to `Config` (above the `impl Config` block):

```rust
/// Parse `LOOM_DB_MIGRATE_ON_BOOT` (default `false`). Only the literal
/// `true`/`false` are accepted; anything else fails startup naming the key.
pub fn parse_migrate_on_boot(vars: &HashMap<String, String>) -> Result<bool, ConfigError> {
    match vars.get("LOOM_DB_MIGRATE_ON_BOOT").map(String::as_str) {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(other) => Err(invalid(
            "LOOM_DB_MIGRATE_ON_BOOT",
            format!("expected `true` or `false`, got `{other}`"),
        )),
    }
}
```

5. Replace the whole body of `Config::from_map` (deleting the local `req` and
   `invalid` closures — the crate-level `invalid` re-export takes over):

```rust
    /// Parse from a key->value map. `from_env` wraps this with `std::env::vars()`.
    pub fn from_map(vars: &HashMap<String, String>) -> Result<Config, ConfigError> {
        let bind_addr = req_var(vars, "LOOM_BIND_ADDR")?
            .parse()
            .map_err(|e: std::net::AddrParseError| invalid("LOOM_BIND_ADDR", e))?;
        let lock_timeout =
            Duration::from_millis(parse_var(vars, "LOOM_LOCK_TIMEOUT_MS", 5000_u64)?);
        let gc_retention =
            Duration::from_secs(parse_var(vars, "LOOM_GC_RETENTION_SECS", 7 * 24 * 3600_u64)?);
        let data_path = PathBuf::from(req_var(vars, "LOOM_DATA_PATH")?);
        let object_store = ObjectStoreConfig::parse(vars, &data_path)?;
        Ok(Config {
            bind_addr,
            db: DbConfig::from_map(vars)?,
            data_path: data_path.clone(),
            object_store,
            lock_timeout,
            gc_retention,
            embedded: EmbeddedSettings::from_map(vars, &data_path)?,
            migrate_on_boot: parse_migrate_on_boot(vars)?,
        })
    }
```

(Note `data_path.clone()` — `EmbeddedSettings::from_map` borrows it inside the
struct literal; cloning one `PathBuf` at startup is fine and keeps the
composition flat.)

- [x] **Step 4: Run the full pinned-behavior suite to green**

Run: `buck2 test //src/services/runtime:config //src/services/runtime:embedded-config //src/services/runtime:object-store-config > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS — all pre-existing tests unmodified, plus the four new ones.
Run: `buck2 build '//src/services/runtime:runtime[clippy.txt]' > /tmp/c3.log 2>&1` — artifact empty.

- [x] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p3.log 2>&1; grep -c Failed /tmp/p3.log` — expected `0`.

```bash
git add src/services/runtime
git commit -m "refactor(runtime): decompose Config::from_map onto per-domain parsers

DbConfig::from_map / EmbeddedSettings::from_map / parse_migrate_on_boot,
all on loom-config's req_var/parse_var (which service_runtime now
re-exports); from_map itself is a ~14-line composition. Behavior-preserving:
same variants, same var names, same LOOM_DB_MIGRATE_ON_BOOT message; the
whole existing config/embedded/object-store suite passes unmodified.

Part of road-service-bootstrap."
```

---

### Task 4: Fail-loud auth TTL readers over the env snapshot

**Files:**
- Modify: `src/services/runtime/src/auth.rs` (new fns beside the old at
  :465/:474; add `use std::collections::HashMap;`)
- Modify: `src/services/runtime/src/lib.rs:6-9` (export the new names —
  keep the old exports until Task 7)
- Modify: `src/services/query-api/src/serve.rs` (`:51` switches to the snapshot
  reader — `serve` already holds `env` from line 26 — plus the `:83-114`
  live-env sweep found during verification: `LOOM_UI_DIR`,
  `LOOM_CORS_ALLOWED_ORIGINS`, `LOOM_FLIGHT_BIND_ADDR`, and the lossy
  `LOOM_EXPORT_MAX_ROWS` parse inside `spawn_flight_export`)
- Create: `src/services/runtime/tests/ttl.rs`
- Modify: `src/services/runtime/BUCK` (new `ttl` target)

**Interfaces:**
- Produces: `pub fn session_ttl(vars) -> Result<Duration, ConfigError>` and
  `pub fn service_token_max_ttl(vars) -> Result<Duration, ConfigError>`. Env
  keys and defaults identical to the lossy readers: `LOOM_SESSION_TTL_SECS`
  (86 400 s) and `LOOM_SERVICE_TOKEN_MAX_TTL` (90 days).
- `spawn_flight_export` gains a `max_rows: u32` parameter (its single caller is
  `serve` in the same file; no test calls either fn — verified).
- The old `*_from_env` fns stay (still called by the mains and standalone)
  until Tasks 5–6 migrate those callers; Task 7 deletes them.

- [x] **Step 1: Write the failing tests**

Create `src/services/runtime/tests/ttl.rs`:

```rust
//! Fail-loud auth TTL readers over the env snapshot (iss-config-silent-fallbacks):
//! absent => documented default; present-but-malformed => startup error naming
//! the key (the old *_from_env readers silently fell back to the default).
use std::collections::HashMap;
use std::time::Duration;

use service_runtime::{ConfigError, service_token_max_ttl, session_ttl};

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn session_ttl_defaults_to_24h() {
    assert_eq!(session_ttl(&map(&[])).unwrap(), Duration::from_secs(86_400));
}

#[test]
fn session_ttl_parses_override() {
    let vars = map(&[("LOOM_SESSION_TTL_SECS", "120")]);
    assert_eq!(session_ttl(&vars).unwrap(), Duration::from_secs(120));
}

#[test]
fn session_ttl_malformed_is_startup_error() {
    let vars = map(&[("LOOM_SESSION_TTL_SECS", "soon")]);
    let err = session_ttl(&vars).unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { ref var, .. }
        if var == "LOOM_SESSION_TTL_SECS"));
}

#[test]
fn max_ttl_defaults_to_90_days() {
    assert_eq!(
        service_token_max_ttl(&map(&[])).unwrap(),
        Duration::from_secs(90 * 24 * 3600)
    );
}

#[test]
fn max_ttl_parses_override() {
    let vars = map(&[("LOOM_SERVICE_TOKEN_MAX_TTL", "3600")]);
    assert_eq!(
        service_token_max_ttl(&vars).unwrap(),
        Duration::from_secs(3600)
    );
}

#[test]
fn max_ttl_malformed_is_startup_error() {
    let vars = map(&[("LOOM_SERVICE_TOKEN_MAX_TTL", "forever")]);
    let err = service_token_max_ttl(&vars).unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { ref var, .. }
        if var == "LOOM_SERVICE_TOKEN_MAX_TTL"));
}
```

Add to `src/services/runtime/BUCK` (next to the `config` target):

```python
rust_test(
    name = "ttl",
    crate = "ttl",
    srcs = ["tests/ttl.rs"],
    crate_root = "tests/ttl.rs",
    edition = "2024",
    deps = [":runtime"],
)
```

- [x] **Step 2: Run to see them fail**

Run: `buck2 test //src/services/runtime:ttl > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t4.log`
Expected: FAIL — `session_ttl`/`service_token_max_ttl` unresolved.

- [x] **Step 3: Implement**

In `src/services/runtime/src/auth.rs`: add `use std::collections::HashMap;` to
the imports, and add ABOVE the two `*_from_env` fns (which stay for now):

```rust
/// Fail-loud read of the service-token TTL cap from the env snapshot
/// (`LOOM_SERVICE_TOKEN_MAX_TTL`, seconds, default 90 days). Mint requests over
/// the cap are rejected (400). A present-but-malformed value is a startup error
/// naming the key — never a silent fallback (iss-config-silent-fallbacks).
pub fn service_token_max_ttl(
    vars: &HashMap<String, String>,
) -> Result<Duration, loom_config::ConfigError> {
    Ok(Duration::from_secs(loom_config::parse_var(
        vars,
        "LOOM_SERVICE_TOKEN_MAX_TTL",
        90 * 24 * 3600_u64,
    )?))
}

/// Fail-loud read of the session TTL from the env snapshot
/// (`LOOM_SESSION_TTL_SECS`, default 24h). Same fallback semantics as
/// [`service_token_max_ttl`].
pub fn session_ttl(vars: &HashMap<String, String>) -> Result<Duration, loom_config::ConfigError> {
    Ok(Duration::from_secs(loom_config::parse_var(
        vars,
        "LOOM_SESSION_TTL_SECS",
        86_400_u64,
    )?))
}
```

In `src/services/runtime/src/lib.rs`, extend the auth re-export (old names kept
until Task 7):

```rust
pub use auth::{
    AuthState, Subject, login_routes, protect, require_auth, service_account_routes,
    service_token_max_ttl, service_token_max_ttl_from_env, session_routes, session_ttl,
    session_ttl_from_env, status_for,
};
```

In `src/services/query-api/src/serve.rs:51`, replace

```rust
    let max_ttl = service_runtime::service_token_max_ttl_from_env();
```

with (the `env` snapshot from line 26 is in scope; `?` boxes the `ConfigError`
into `BoxErr`):

```rust
    let max_ttl = service_runtime::service_token_max_ttl(&env)?;
```

Then sweep the remaining live-env reads in the same file onto the snapshot.
Replace lines 80-95 (the `// Optional UI serving` comment line through the
flight-export gate — the comment is included in the replaced range so it is
not duplicated):

```rust
    // Optional UI serving (tight deploy) + CORS (detached deploy); both default off.
    let app = crate::web_static::with_static(
        app,
        env.get("LOOM_UI_DIR").map(std::path::PathBuf::from),
    );
    let origins = crate::web_static::parse_allowed_origins(
        env.get("LOOM_CORS_ALLOWED_ORIGINS").map_or("", String::as_str),
    );
    let app = crate::web_static::with_cors(app, &origins);

    // Optional external Arrow Flight export listener (opt-in via LOOM_FLIGHT_BIND_ADDR).
    if let Some(bind) = env.get("LOOM_FLIGHT_BIND_ADDR") {
        // Fail-loud: a malformed row cap is a startup error when the export
        // endpoint was explicitly requested (was: silent fallback to the default).
        let max_rows =
            service_runtime::parse_var(&env, "LOOM_EXPORT_MAX_ROWS", DEFAULT_EXPORT_MAX_ROWS)?;
        spawn_flight_export(bind, &engine_socket, auth_flight, cp_flight, max_rows).await?;
    }
```

and in `spawn_flight_export`, add the parameter and delete the internal read
(lines 111-114):

```rust
async fn spawn_flight_export(
    bind: &str,
    engine_socket: &str,
    auth_flight: Arc<dyn control_plane_core::Auth + Send + Sync>,
    cp_flight: Arc<dyn ControlPlane>,
    max_rows: u32,
) -> Result<(), BoxErr> {
```

(the `let max_rows = std::env::var("LOOM_EXPORT_MAX_ROWS")…` block is deleted;
the rest of the fn is unchanged).

- [x] **Step 4: Run to green**

Run: `buck2 test //src/services/runtime:ttl > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log`
Expected: PASS.
Run: `buck2 build '//src/services/runtime:runtime[clippy.txt]' '//src/services/query-api:query-api[clippy.txt]' > /tmp/c4.log 2>&1` — artifacts empty.

- [x] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p4.log 2>&1; grep -c Failed /tmp/p4.log` — expected `0`.

```bash
git add src/services/runtime src/services/query-api/src/serve.rs
git commit -m "feat(runtime): fail-loud auth TTL readers over the env snapshot

session_ttl/service_token_max_ttl take the main's env snapshot and return
ConfigError on a present-but-malformed value (the *_from_env readers
silently fell back to the default on a typo). Same keys, same defaults.
query-api's serve() switches (it already holds a snapshot) and its remaining
live-env reads (LOOM_UI_DIR / LOOM_CORS_ALLOWED_ORIGINS /
LOOM_FLIGHT_BIND_ADDR / the lossy LOOM_EXPORT_MAX_ROWS parse — stragglers
the issue did not enumerate) move onto the same snapshot. The remaining TTL
callers migrate with the standalone-tuning and bootstrap tasks, after which
the lossy readers are deleted.

Part of road-service-bootstrap, toward iss-config-silent-fallbacks."
```

---

### Task 5: `EngineTuning` + `engine::run` signature; `StandaloneTuning` threads it

**Files:**
- Modify: `src/services/engine/src/run.rs` (delete `parse_env_or`; add
  `EngineTuning`; `run` takes it)
- Modify: `src/services/engine/src/lib.rs` (`pub use run::{EngineTuning, run};`)
- Modify: `src/services/engine/src/main.rs` (interim: snapshot for
  `LOOM_ENGINE_SOCKET` + tuning; full bootstrap rewrite comes in Task 6)
- Modify: `src/services/standalone/src/lib.rs` (`StandaloneTuning`; `run`/
  `serve_composite` take it; TTL/live-env reads removed)
- Modify: `src/services/standalone/src/main.rs` (parse tuning, pass it)
- Modify: `src/services/standalone/tests/composite_e2e.rs:97`,
  `tests/composite_error_path.rs:68` (new `run` argument)
- Create: `src/services/engine/tests/engine_tuning.rs`,
  `src/services/standalone/tests/tuning.rs`
- Modify: `src/services/engine/BUCK`, `src/services/standalone/BUCK` (new pure
  test targets; standalone BUCK also gains the `rust_test` load)

**Interfaces:**
- Produces: `engine::EngineTuning { inline_byte_limit: usize, flush_byte_threshold: i64 }`
  with `from_map(vars) -> Result<Self, service_runtime::ConfigError>`;
  `engine::run(listener, cfg, pool, tuning: EngineTuning, ready, shutdown)`;
  `standalone::StandaloneTuning { session_ttl, max_ttl, engine: EngineTuning }`
  with `from_map`; `standalone::run(cfg, addrs, tuning, shutdown, ready)`.
- Callers updated here (complete): `engine/src/main.rs`,
  `standalone/src/{lib,main}.rs`, both composite tests. No other caller of
  `engine::run`/`standalone::run` exists (verified by grep — no engine test
  calls `run`).

- [x] **Step 1: Write the failing tests**

Create `src/services/engine/tests/engine_tuning.rs`:

```rust
//! EngineTuning::from_map: the write-path byte thresholds parsed from the main's
//! env snapshot (was: live-env parse_env_or inside engine::run, violating the
//! one-env-snapshot-per-main rule).
use std::collections::HashMap;

use engine::EngineTuning;

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn defaults_are_16_mib_inline_and_64_mib_flush() {
    let t = EngineTuning::from_map(&map(&[])).unwrap();
    assert_eq!(t.inline_byte_limit, 16 * 1024 * 1024);
    assert_eq!(t.flush_byte_threshold, 64 * 1024 * 1024);
}

#[test]
fn overrides_parse() {
    let t = EngineTuning::from_map(&map(&[
        ("LOOM_INLINE_BYTE_LIMIT", "1024"),
        ("LOOM_FLUSH_BYTE_THRESHOLD", "2048"),
    ]))
    .unwrap();
    assert_eq!(t.inline_byte_limit, 1024);
    assert_eq!(t.flush_byte_threshold, 2048);
}

#[test]
fn malformed_value_is_startup_error_naming_key() {
    let err = EngineTuning::from_map(&map(&[("LOOM_INLINE_BYTE_LIMIT", "lots")])).unwrap_err();
    assert!(matches!(err, service_runtime::ConfigError::Invalid { ref var, .. }
        if var == "LOOM_INLINE_BYTE_LIMIT"));
}
```

Add to `src/services/engine/BUCK` (next to `flight-membership-helper`, the
existing pure `rust_test`):

```python
rust_test(
    name = "engine-tuning",
    crate = "engine_tuning",
    srcs = ["tests/engine_tuning.rs"],
    crate_root = "tests/engine_tuning.rs",
    deps = [
        ":engine",
        "//src/services/runtime:runtime",
    ],
)
```

Create `src/services/standalone/tests/tuning.rs`:

```rust
//! StandaloneTuning::from_map: the composite's env-derived tunables, parsed once
//! in main from the snapshot (defaults when absent, fail-loud when malformed).
use std::collections::HashMap;
use std::time::Duration;

use standalone::StandaloneTuning;

#[test]
fn defaults_cover_all_three_domains() {
    let t = StandaloneTuning::from_map(&HashMap::new()).unwrap();
    assert_eq!(t.session_ttl, Duration::from_secs(86_400));
    assert_eq!(t.max_ttl, Duration::from_secs(90 * 24 * 3600));
    assert_eq!(t.engine.inline_byte_limit, 16 * 1024 * 1024);
    assert_eq!(t.engine.flush_byte_threshold, 64 * 1024 * 1024);
}

#[test]
fn malformed_ttl_is_startup_error() {
    let mut vars: HashMap<String, String> = HashMap::new();
    vars.insert("LOOM_SESSION_TTL_SECS".into(), "soon".into());
    let err = StandaloneTuning::from_map(&vars).unwrap_err();
    assert!(matches!(err, service_runtime::ConfigError::Invalid { ref var, .. }
        if var == "LOOM_SESSION_TTL_SECS"));
}
```

In `src/services/standalone/BUCK`: add the loader line
`load("//src:loom_test.bzl", "rust_test")` at the top (the file currently loads
only `loom_fixture_test`), and the target:

```python
rust_test(
    name = "tuning",
    crate = "tuning",
    srcs = ["tests/tuning.rs"],
    crate_root = "tests/tuning.rs",
    deps = [
        ":standalone",
        "//src/services/engine:engine",
        "//src/services/runtime:runtime",
    ],
)
```

- [x] **Step 2: Run to see them fail**

Run: `buck2 test //src/services/engine:engine-tuning //src/services/standalone:tuning > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t5.log`
Expected: FAIL — `EngineTuning`/`StandaloneTuning` unresolved.

- [x] **Step 3: Implement the engine side**

In `src/services/engine/src/run.rs`:

1. Delete `fn parse_env_or` (lines 92-101) and its two call sites (lines
   51-52).
2. Add above `run`:

```rust
/// Engine write-path byte thresholds, parsed once from the caller's env snapshot
/// (the engine main / the standalone composite) — `run` itself never reads the
/// live environment (one-env-snapshot-per-main).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EngineTuning {
    /// Row payloads at/below this size commit as inline PG rows
    /// (`LOOM_INLINE_BYTE_LIMIT`, default 16 MiB).
    pub inline_byte_limit: usize,
    /// Inline-row bytes above which a flush-to-Parquet job is enqueued
    /// (`LOOM_FLUSH_BYTE_THRESHOLD`, default 64 MiB).
    pub flush_byte_threshold: i64,
}

impl EngineTuning {
    /// Parse from the env snapshot: absent keys take the defaults, a
    /// present-but-malformed value fails startup naming the key.
    pub fn from_map(
        vars: &HashMap<String, String>,
    ) -> Result<Self, service_runtime::ConfigError> {
        Ok(EngineTuning {
            inline_byte_limit: service_runtime::parse_var(
                vars,
                "LOOM_INLINE_BYTE_LIMIT",
                16 * 1024 * 1024,
            )?,
            flush_byte_threshold: service_runtime::parse_var(
                vars,
                "LOOM_FLUSH_BYTE_THRESHOLD",
                64 * 1024 * 1024,
            )?,
        })
    }
}
```

3. `run`'s signature gains the param (between `pool` and `ready`):

```rust
pub async fn run(
    listener: UnixListener,
    cfg: &service_runtime::Config,
    pool: sqlx::PgPool,
    tuning: EngineTuning,
    ready: oneshot::Sender<()>,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), BoxErr> {
```

and the writer construction uses it:

```rust
    let writer = engine_serving::IcebergActionWriter::new(
        std::sync::Arc::new(writer_catalog),
        pool.clone(),
        tuning.inline_byte_limit,
        tuning.flush_byte_threshold,
    );
```

4. In `src/services/engine/src/lib.rs`:
   `pub use run::{EngineTuning, run};`

5. `src/services/engine/src/main.rs` — interim update (Task 6 replaces this
   with `bootstrap`); this is also item 2g for the engine main
   (`LOOM_ENGINE_SOCKET` via `req_var` over the snapshot):

```rust
//! engine binary: bind the UDS from the environment and serve via `engine::run`.
use tokio::net::UnixListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = service_runtime::Config::from_env()?;
    if service_runtime::migrate_requested() {
        service_runtime::run_migrations(&cfg.db).await?;
        return Ok(());
    }
    let (pool, _pg) = service_runtime::build_pool_managed(&cfg).await?;

    let env = service_runtime::env_map();
    let socket_path = service_runtime::req_var(&env, "LOOM_ENGINE_SOCKET")?;
    drop(std::fs::remove_file(&socket_path)); // remove stale socket (missing is fine)
    let listener = UnixListener::bind(&socket_path)?;
    let tuning = engine::EngineTuning::from_map(&env)?;

    let (ready_tx, _ready_rx) = tokio::sync::oneshot::channel();
    engine::run(listener, &cfg, pool, tuning, ready_tx, async {
        drop(tokio::signal::ctrl_c().await);
    })
    .await
}
```

- [x] **Step 4: Implement the standalone side**

In `src/services/standalone/src/lib.rs`:

1. Add below `StandaloneAddrs`:

```rust
/// Env-derived tunables the composite passes to its services: the auth TTLs and
/// the engine write-path byte thresholds. Parsed once from the main's env
/// snapshot (fail-loud on malformed values) and handed into [`run`]; the
/// composite itself never reads the live environment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StandaloneTuning {
    pub session_ttl: std::time::Duration,
    pub max_ttl: std::time::Duration,
    pub engine: engine::EngineTuning,
}

impl StandaloneTuning {
    /// Parse from the env snapshot. Absent keys take the documented defaults
    /// (24h session TTL, 90-day token cap, 16 MiB inline / 64 MiB flush).
    pub fn from_map(
        vars: &std::collections::HashMap<String, String>,
    ) -> Result<Self, service_runtime::ConfigError> {
        Ok(StandaloneTuning {
            session_ttl: service_runtime::session_ttl(vars)?,
            max_ttl: service_runtime::service_token_max_ttl(vars)?,
            engine: engine::EngineTuning::from_map(vars)?,
        })
    }
}
```

2. `run` gains the param and forwards it (`StandaloneTuning` is `Copy`):

```rust
pub async fn run(
    cfg: service_runtime::Config,
    addrs: StandaloneAddrs,
    tuning: StandaloneTuning,
    shutdown: impl Future<Output = ()> + Send + 'static,
    ready: tokio::sync::oneshot::Sender<()>,
) -> Result<(), BoxErr> {
    // Boot embedded PG (a no-op handle in external-DB mode). Owned here so PG is
    // stopped gracefully on EVERY exit of the composite below — clean shutdown,
    // a serve-task error, an engine-before-ready failure, or a startup bind error.
    let (pool, pg_handle) = service_runtime::build_pool_managed(&cfg).await?;
    let mut outcome = serve_composite(cfg, addrs, tuning, shutdown, ready, pool).await;
    stop_pg(pg_handle, &mut outcome).await;
    outcome
}
```

3. `serve_composite` gains the same param
   (`tuning: StandaloneTuning` after `addrs`), and its body switches the three
   env reads:

```rust
    let auth = service_runtime::AuthState {
        auth: pg.clone(),
        session_ttl: tuning.session_ttl,
    };
    let max_ttl = tuning.max_ttl;
```

and the engine spawn passes the engine slice through:

```rust
    tasks.spawn(async move {
        (
            "engine",
            engine::run(
                engine_listener,
                &engine_cfg,
                engine_pool,
                tuning.engine,
                eng_ready_tx,
                engine_sd,
            )
            .await,
        )
    });
```

4. In `src/services/standalone/src/main.rs`, after `let cfg = …from_map(&env)?;`
   (line 50) add the parse and pass it:

```rust
    let cfg = service_runtime::Config::from_map(&env)?;
    let addrs = resolve_addrs(&env)?;
    let tuning = standalone::StandaloneTuning::from_map(&env)?;

    standalone::run(cfg, addrs, tuning, shutdown_signal(), ready_noop()).await
```

5. Update both fixture tests' `run` calls. In
   `src/services/standalone/tests/composite_e2e.rs`, before the `tokio::spawn`
   (around line 95) add
   `let tuning = standalone::StandaloneTuning::from_map(&std::collections::HashMap::new()).expect("tuning");`
   and the call becomes:

```rust
        standalone::run(
            cfg,
            addrs,
            tuning,
            async move {
                drop(shutdown_rx.await);
            },
            ready_tx,
        )
        .await
```

   In `src/services/standalone/tests/composite_error_path.rs` (HashMap is
   already imported at the top), before the `timeout` call add
   `let tuning = standalone::StandaloneTuning::from_map(&HashMap::new()).expect("tuning");`
   and line 68 becomes:

```rust
        standalone::run(cfg, addrs, tuning, std::future::pending::<()>(), ready_tx),
```

- [x] **Step 5: Run to green (pure + fixture)**

Run: `buck2 test //src/services/engine:engine-tuning //src/services/standalone:tuning > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log`
Expected: PASS.
Run: `buck2 test -j 8 //src/services/standalone: > /tmp/t5b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5b.log`
Expected: PASS (composite-e2e, composite-error-path, create-admin, tuning).
Run: `buck2 build '//src/services/engine:engine[clippy.txt]' '//src/services/standalone:standalone[clippy.txt]' > /tmp/c5.log 2>&1` — artifacts empty.

- [x] **Step 6: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p5.log 2>&1; grep -c Failed /tmp/p5.log` — expected `0`.

```bash
git add src/services/engine src/services/standalone
git commit -m "refactor(engine): EngineTuning from the env snapshot, threaded through standalone

Deletes engine::run's live-env parse_env_or (the one-env-snapshot-per-main
violation in iss-config-silent-fallbacks): LOOM_INLINE_BYTE_LIMIT /
LOOM_FLUSH_BYTE_THRESHOLD now parse in the mains via
EngineTuning::from_map and flow into run(). The standalone composite gains
StandaloneTuning { session_ttl, max_ttl, engine } — one value parsed in its
main — replacing its lossy *_from_env TTL reads; standalone keeps its own
build_pool_managed + graceful stop_pg (it does NOT adopt bootstrap()).
engine main reads LOOM_ENGINE_SOCKET via req_var over the snapshot.

Part of road-service-bootstrap, toward iss-config-silent-fallbacks."
```

---

### Task 6: `service_runtime::bootstrap` (Boot / ServiceContext); the three mains adopt it

**Files:**
- Modify: `src/services/runtime/src/lib.rs` (`Boot`, `ServiceContext`,
  `bootstrap`, `RuntimeError::Config`, `migrate_requested(vars)`)
- Modify: `src/services/query-api/src/main.rs`, `src/services/ingest/src/main.rs`,
  `src/services/engine/src/main.rs` (adopt `bootstrap`),
  `src/services/standalone/src/main.rs:28` (`migrate_requested(&env)`)
- Create: `src/services/runtime/tests/bootstrap.rs`
- Modify: `src/services/runtime/BUCK` (new `bootstrap` fixture target),
  `src/services/runtime/tests/config.rs` (migrate_requested unit test)

**Interfaces:**
- Produces:

```rust
pub enum Boot { Migrated, Ready(ServiceContext) }
pub struct ServiceContext {
    pub cfg: Config,
    pub pool: PgPool,
    pub pg: Arc<PgControlPlane>,
    pub auth: AuthState,
    pub max_ttl: Duration,
    _embedded: Option<managed_postgres::EmbeddedPg>,
}
pub async fn bootstrap(vars: &HashMap<String, String>) -> Result<Boot, RuntimeError>;
pub fn migrate_requested(vars: &HashMap<String, String>) -> bool;  // was ()
```

- `migrate_requested` callers after this task (complete): `bootstrap`
  internally + `standalone/src/main.rs:28` (the three service mains reach it
  through `bootstrap`).
- Consumed by: the three mains. **NOT** standalone's `run` (see rescope).
- No `admin_subject` field — see rescope.

- [x] **Step 1: Write the failing tests**

Append to `src/services/runtime/tests/config.rs`:

```rust
#[test]
fn migrate_requested_reads_the_snapshot() {
    let mut v = full();
    assert!(!service_runtime::migrate_requested(&v));
    v.insert("LOOM_MIGRATE".into(), "apply".into());
    assert!(service_runtime::migrate_requested(&v));
    v.insert("LOOM_MIGRATE".into(), "yes".into());
    assert!(!service_runtime::migrate_requested(&v));
}
```

Create `src/services/runtime/tests/bootstrap.rs` (mirrors
`tests/migrate_managed.rs`'s embedded-as-external harness):

```rust
//! `service_runtime::bootstrap` over a real (embedded-as-external) Postgres:
//! the migrate-and-exit gate, the Ready context (fields + working pool +
//! structural embedded keep-alive slot), and the fail-loud TTL ordering
//! (TTLs parse BEFORE the pool boots).
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use managed_postgres::{EmbeddedPg, EmbeddedPgConfig};
use service_runtime::{Boot, ConfigError, RuntimeError};
use sqlx::postgres::PgPoolOptions;

// Mirrors tests/migrate_managed.rs (per-file fixture pattern; deliberate).
fn embedded_cfg(data: &Path, sock: &Path) -> EmbeddedPgConfig {
    EmbeddedPgConfig {
        bin_dir: PathBuf::from(std::env::var("POSTGRES_BIN_DIR").expect("POSTGRES_BIN_DIR")),
        ld_library_path: std::env::var("POSTGRES_LD_LIBRARY_PATH").unwrap_or_default(),
        data_dir: data.to_path_buf(),
        socket_dir: sock.to_path_buf(),
        database: "loom".to_string(),
    }
}

/// External-mode env snapshot pointing at `host` (a unix-socket dir).
fn vars_for(host: String, data_path: &Path) -> HashMap<String, String> {
    [
        ("LOOM_BIND_ADDR", "127.0.0.1:0".to_string()),
        ("LOOM_DB_HOST", host),
        ("LOOM_DB_PORT", "5432".to_string()),
        ("LOOM_DB_USER", "postgres".to_string()),
        ("LOOM_DB_PASSWORD", String::new()),
        ("LOOM_DB_NAME", "loom".to_string()),
        ("LOOM_DATA_PATH", data_path.display().to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

async fn loom_schema_present(pg: &EmbeddedPg) -> bool {
    let pool = PgPoolOptions::new()
        .connect_with(pg.connect_options())
        .await
        .expect("connect probe");
    let present: bool = sqlx::query_scalar(
        "select exists(select 1 from information_schema.tables \
         where table_schema = 'acl' and table_name = 'subject')",
    )
    .fetch_one(&pool)
    .await
    .expect("probe query");
    pool.close().await;
    present
}

#[tokio::test]
async fn bootstrap_migrate_mode_applies_schema_and_returns_migrated() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let pg = EmbeddedPg::start(embedded_cfg(
        &tmp.path().join("pgdata"),
        &tmp.path().join("pgrun"),
    ))
    .await
    .expect("start");

    let mut vars = vars_for(pg.socket_dir().to_string_lossy().into_owned(), tmp.path());
    vars.insert("LOOM_MIGRATE".into(), "apply".into());
    let boot = service_runtime::bootstrap(&vars).await.expect("bootstrap");
    assert!(matches!(boot, Boot::Migrated), "expected Boot::Migrated");
    assert!(loom_schema_present(&pg).await, "migrations applied");
    pg.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn bootstrap_ready_builds_the_full_context() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let pg = EmbeddedPg::start(embedded_cfg(
        &tmp.path().join("pgdata"),
        &tmp.path().join("pgrun"),
    ))
    .await
    .expect("start");

    let mut vars = vars_for(pg.socket_dir().to_string_lossy().into_owned(), tmp.path());
    vars.insert("LOOM_SESSION_TTL_SECS".into(), "123".into());
    vars.insert("LOOM_SERVICE_TOKEN_MAX_TTL".into(), "456".into());
    let boot = service_runtime::bootstrap(&vars).await.expect("bootstrap");
    let ctx = match boot {
        Boot::Ready(ctx) => ctx,
        Boot::Migrated => panic!("expected Boot::Ready"),
    };
    assert_eq!(ctx.auth.session_ttl, Duration::from_secs(123));
    assert_eq!(ctx.max_ttl, Duration::from_secs(456));
    assert_eq!(ctx.cfg.db.dbname, "loom");
    let one: i32 = sqlx::query_scalar("select 1")
        .fetch_one(&ctx.pool)
        .await
        .expect("pool works");
    assert_eq!(one, 1);
    ctx.pool.close().await;
    pg.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn bootstrap_malformed_ttl_fails_before_any_pool_build() {
    // No live PG anywhere near this host: if the TTL parse happened AFTER the
    // pool build, this would surface RuntimeError::Pool instead of Config.
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut vars = vars_for(
        tmp.path().join("no-such-pgrun").display().to_string(),
        tmp.path(),
    );
    vars.insert("LOOM_SESSION_TTL_SECS".into(), "soon".into());
    let Err(err) = service_runtime::bootstrap(&vars).await else {
        panic!("malformed TTL must fail bootstrap");
    };
    assert!(
        matches!(err, RuntimeError::Config(ConfigError::Invalid { ref var, .. })
            if var == "LOOM_SESSION_TTL_SECS"),
        "expected Config(Invalid LOOM_SESSION_TTL_SECS), got: {err}"
    );
}
```

Add to `src/services/runtime/BUCK` (next to `migrate-managed`, same dep shape):

```python
# bootstrap() end-to-end: migrate gate, Ready context, TTL fail-fast ordering.
# Boots a hermetic Postgres via EmbeddedPg, so it must route local.
loom_fixture_test(
    name = "bootstrap",
    crate = "bootstrap",
    srcs = ["tests/bootstrap.rs"],
    crate_root = "tests/bootstrap.rs",
    edition = "2024",
    deps = [
        ":runtime",
        "//src/services/managed-postgres:managed-postgres",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [x] **Step 2: Run to see them fail**

Run: `buck2 test -j 8 //src/services/runtime:bootstrap //src/services/runtime:config > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t6.log`
Expected: FAIL — `Boot`/`bootstrap` unresolved; `migrate_requested(&v)` takes 0 args.

- [x] **Step 3: Implement in `service_runtime`**

In `src/services/runtime/src/lib.rs`:

1. `RuntimeError` gains (last variant):

```rust
    #[error("config: {0}")]
    Config(#[from] ConfigError),
```

2. `migrate_requested` (at :327) becomes snapshot-based:

```rust
/// `true` when the process was started in migrate-and-exit mode
/// (`LOOM_MIGRATE=apply`, read from the caller's env snapshot). [`bootstrap`]
/// checks this before normal startup — and the standalone main before its
/// embedded self-extract — so a chart hook Job can run any service image as a
/// one-shot migrator.
pub fn migrate_requested(vars: &HashMap<String, String>) -> bool {
    vars.get("LOOM_MIGRATE").map(String::as_str) == Some("apply")
}
```

3. Add below `build_pool_managed`:

```rust
/// Outcome of [`bootstrap`]: migrate-and-exit mode completed, or the full
/// service context is ready to serve.
pub enum Boot {
    /// `LOOM_MIGRATE=apply`: migrations applied; the caller should exit 0.
    Migrated,
    /// Normal startup: everything a service main needs to serve.
    Ready(ServiceContext),
}

/// The shared startup product: config, control-plane pool, concrete
/// `PgControlPlane`, auth state, and the service-token TTL cap. Owns the
/// embedded-PG handle so the keep-alive is structural — the cluster lives
/// exactly as long as the context, replacing the per-main `_pg` binding every
/// caller had to remember. Mains deliberately do NOT gracefully stop the
/// embedded cluster (parity with the previous behavior); the standalone
/// composite keeps its own `build_pool_managed` + `stop_pg` for the graceful
/// path and does not use `bootstrap`.
pub struct ServiceContext {
    pub cfg: Config,
    pub pool: PgPool,
    pub pg: Arc<PgControlPlane>,
    pub auth: AuthState,
    pub max_ttl: Duration,
    _embedded: Option<managed_postgres::EmbeddedPg>,
}

/// One startup path for the service mains: parse config from the env snapshot,
/// honor migrate-and-exit mode, then build pool → control plane → auth. The
/// fail-loud TTL reads happen BEFORE the pool boots (a typo'd TTL should not
/// cost an embedded initdb). The full config is parsed before the migrate gate,
/// exactly as every main did.
pub async fn bootstrap(vars: &HashMap<String, String>) -> Result<Boot, RuntimeError> {
    let cfg = Config::from_map(vars)?;
    if migrate_requested(vars) {
        run_migrations(&cfg.db).await?;
        return Ok(Boot::Migrated);
    }
    let session_ttl = auth::session_ttl(vars)?;
    let max_ttl = auth::service_token_max_ttl(vars)?;
    let (pool, embedded) = build_pool_managed(&cfg).await?;
    let pg = Arc::new(control_plane(pool.clone(), cfg.lock_timeout));
    let auth = AuthState {
        auth: pg.clone(),
        session_ttl,
    };
    Ok(Boot::Ready(ServiceContext {
        cfg,
        pool,
        pg,
        auth,
        max_ttl,
        _embedded: embedded,
    }))
}
```

**Clippy contingency (apply only if the gate reports them; the toolchain
allowlist was checked — neither lint is pre-allowed):**
- `clippy::partial_pub_fields` on `ServiceContext` →
  `#[expect(clippy::partial_pub_fields, reason = "_embedded is a keep-alive guard, not API — private so callers cannot detach the cluster's lifetime from the context")]`
- `clippy::large_enum_variant` on `Boot` →
  `#[expect(clippy::large_enum_variant, reason = "one value per process at startup; boxing buys nothing")]`

- [x] **Step 4: The three mains adopt it; standalone main takes the snapshot**

`src/services/query-api/src/main.rs` — full replacement:

```rust
//! query-api binary: shared bootstrap, bind the HTTP listener, serve via `query_api::serve`.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    service_runtime::init_tracing();
    let env = service_runtime::env_map();
    // Shared bootstrap: config → migrate-and-exit gate → pool → control plane →
    // auth. `ctx` owns the embedded-PG handle (None here — query-api deploys
    // external), so the keep-alive is structural for the process lifetime.
    let ctx = match service_runtime::bootstrap(&env).await? {
        service_runtime::Boot::Migrated => return Ok(()),
        service_runtime::Boot::Ready(ctx) => ctx,
    };

    let engine_socket = service_runtime::req_var(&env, "LOOM_ENGINE_SOCKET")?;

    let listener = tokio::net::TcpListener::bind(ctx.cfg.bind_addr).await?;
    query_api::serve(
        &ctx.cfg,
        ctx.pg.clone(),
        ctx.auth.clone(),
        engine_socket,
        listener,
        std::future::pending(),
    )
    .await
}
```

(`ctx.pg.clone()` — `Arc<PgControlPlane>` — unsize-coerces to the
`Arc<dyn ControlPlane>` parameter, exactly as the old `pg` binding did. The
`use std::sync::Arc;` import is dropped — no longer referenced.)

`src/services/ingest/src/main.rs` — full replacement:

```rust
//! ingest binary: shared bootstrap, bind the HTTP listener, serve via `ingest::serve`.
use std::sync::Arc;

use control_plane_core::ControlPlane;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    service_runtime::init_tracing();
    let env = service_runtime::env_map();
    let ctx = match service_runtime::bootstrap(&env).await? {
        service_runtime::Boot::Migrated => return Ok(()),
        service_runtime::Boot::Ready(ctx) => ctx,
    };
    let cp: Arc<dyn ControlPlane> = ctx.pg.clone();

    let listener = tokio::net::TcpListener::bind(ctx.cfg.bind_addr).await?;
    ingest::serve(
        &ctx.cfg,
        ctx.pool.clone(),
        cp,
        ctx.auth.clone(),
        ctx.max_ttl,
        listener,
        std::future::pending(),
    )
    .await
}
```

`src/services/engine/src/main.rs` — full replacement (note: the engine main
has never called `init_tracing`; preserved as-is, behavior-preserving):

```rust
//! engine binary: shared bootstrap, bind the UDS from the env snapshot, serve
//! via `engine::run`.
use tokio::net::UnixListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let env = service_runtime::env_map();
    let ctx = match service_runtime::bootstrap(&env).await? {
        service_runtime::Boot::Migrated => return Ok(()),
        service_runtime::Boot::Ready(ctx) => ctx,
    };

    let socket_path = service_runtime::req_var(&env, "LOOM_ENGINE_SOCKET")?;
    drop(std::fs::remove_file(&socket_path)); // remove stale socket (missing is fine)
    let listener = UnixListener::bind(&socket_path)?;
    let tuning = engine::EngineTuning::from_map(&env)?;

    let (ready_tx, _ready_rx) = tokio::sync::oneshot::channel();
    engine::run(listener, &ctx.cfg, ctx.pool.clone(), tuning, ready_tx, async {
        drop(tokio::signal::ctrl_c().await);
    })
    .await
}
```

`src/services/standalone/src/main.rs:28` — the one remaining direct caller:

```rust
    if service_runtime::migrate_requested(&env) {
```

(The surrounding lines are unchanged — `env` is already in scope at line 23.)

- [x] **Step 5: Run to green**

Run: `buck2 test -j 8 //src/services/runtime:bootstrap //src/services/runtime:config //src/services/runtime:migrate-managed > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6.log`
Expected: PASS.
Run: `buck2 build -M none //src/services/query-api:query-api-bin //src/services/ingest:ingest-bin //src/services/engine:engine-bin //src/services/standalone:loom > /tmp/b6.log 2>&1; tail -5 /tmp/b6.log`
Expected: build success (binary target names verified against each BUCK file).
Run: `buck2 build '//src/services/runtime:runtime[clippy.txt]' > /tmp/c6.log 2>&1` — artifact empty (apply the Step 3 contingency if not).

- [x] **Step 6: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p6.log 2>&1; grep -c Failed /tmp/p6.log` — expected `0`.

```bash
git add src/services/runtime src/services/query-api/src/main.rs \
  src/services/ingest/src/main.rs src/services/engine/src/main.rs \
  src/services/standalone/src/main.rs
git commit -m "feat(runtime): shared service bootstrap - Boot { Migrated | Ready(ServiceContext) }

One startup path for the three service mains: from_map -> migrate-and-exit
gate -> fail-loud TTL reads -> build_pool_managed -> control plane -> auth.
ServiceContext owns the embedded-PG handle, making the keep-alive structural
instead of a per-main _pg binding; mains stay non-graceful on shutdown
(behavior-preserving). migrate_requested now reads the env snapshot.
No admin_subject: the spec's bootstrap-admin leg was removed by #308's
loom create-admin CLI. Standalone keeps its own graceful-composite path and
does not adopt bootstrap().

Part of road-service-bootstrap."
```

---

### Task 7: Delete the lossy readers; sweep; close the registers

**Files:**
- Modify: `src/services/runtime/src/auth.rs` (delete
  `service_token_max_ttl_from_env` + `session_ttl_from_env`),
  `src/services/runtime/src/lib.rs:6-9` (drop the two old names from the
  re-export)
- Modify: `docs/ROADMAP.md` (~line 290), `docs/ISSUES.md` (~line 102),
  `docs/deploy.md:191-192`

- [ ] **Step 1: Delete and prove zero callers**

Delete both `*_from_env` fns from `src/services/runtime/src/auth.rs` and remove
`service_token_max_ttl_from_env` / `session_ttl_from_env` from the `pub use
auth::{…}` list in `src/services/runtime/src/lib.rs`.

Verify: `grep -rn "_from_env\|parse_env_or" src --include=*.rs` — expected: only
`store_config::parse_from_env` hits (a different, snapshot-taking API that is
not part of this defect) and zero `session_ttl_from_env` /
`service_token_max_ttl_from_env` / `parse_env_or` hits.

Verify the one-snapshot rule for LOOM keys in production code:
`grep -rn 'std::env::var("LOOM_' src/services --include=*.rs | grep -v "/tests/"` —
expected: **no hits** (all remaining `std::env` uses are `std::env::vars()`
inside `Config::from_env` — kept for the transform main — and the
fixture-injected `POSTGRES_*` reads inside test files).

- [ ] **Step 2: Affected-package sweep**

Run: `buck2 test -j 8 //src/loom-config: //src/services/store-config: //src/services/runtime: //src/services/engine: //src/services/standalone: //src/services/ingest: //src/services/query-api: > /tmp/t7.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t7.log`
Expected: PASS (this covers the ingest/query-api e2e suites over the changed
serve/main paths). Verify `git status src/control-plane/postgres/.sqlx` is
clean and `git diff origin/main -- third-party/BUCK` is empty (no dep changes
were made anywhere in this plan).

- [ ] **Step 3: Close the register items + stale deploy rows**

In `docs/ROADMAP.md` (~line 290), flip `road-service-bootstrap` to
`- [x] … {#road-service-bootstrap area:quality status:done from:2026-07-02-pillar-idioms-audit-design pr:#N spec:2026-07-02-pillar-idioms-audit-design}`
and replace the prose paragraph with:

```markdown
  Done (PR #N, rescoped post-#308). Fixes [[iss-config-silent-fallbacks]]. `service_runtime::bootstrap(vars) -> Boot { Migrated | Ready(ServiceContext) }` is the one startup path for the query-api/ingest/engine mains (ServiceContext owns `_embedded`, making the keep-alive structural; mains stay non-graceful by design); `req_var`/`parse_var` land in `loom-config`; `From<StoreConfigError> for ConfigError` lives in `store-config` (loom-config stays a light leaf; no dep cycle); `from_map` (cc 35) decomposed onto `DbConfig::from_map`/`EmbeddedSettings::from_map`/`parse_migrate_on_boot`; every remaining silent-fallback/live-env read converted to fail-loud snapshot readers (auth TTLs, `EngineTuning`, `migrate_requested`, `LOOM_ENGINE_SOCKET`). The spec's `admin_subject` field was dropped: PR #308's `loom create-admin` CLI removed the bootstrap-admin leg (`LOOM_BOOTSTRAP_ADMIN_*` is gone tree-wide). Standalone keeps its graceful composite (`build_pool_managed` + `stop_pg`) and only adopts the snapshot readers via `StandaloneTuning`. Worker stays out (zero-pool by design).
```

In `docs/ISSUES.md` (~line 102), flip `iss-config-silent-fallbacks` to
`- [x] … status:fixed … pr:#N …` and replace the prose with:

```markdown
  Fixed (PR #N, by [[road-service-bootstrap]]). `auth.rs::{service_token_max_ttl,session_ttl}` are fail-loud readers over the env snapshot (`parse_var`: absent ⇒ default, malformed ⇒ startup error naming the key) — the lossy `*_from_env` pair is deleted; `engine/src/run.rs::parse_env_or` (whose defect was the live-env read — it already failed loud on malformed values) is replaced by `EngineTuning::from_map` parsed in the mains and passed into `engine::run`; `migrate_requested` and the query-api/engine `LOOM_ENGINE_SOCKET` reads take the snapshot too, as do four stragglers this entry never enumerated in `query-api/src/serve.rs` (`LOOM_UI_DIR`, `LOOM_CORS_ALLOWED_ORIGINS`, `LOOM_FLIGHT_BIND_ADDR`, and the genuinely lossy `LOOM_EXPORT_MAX_ROWS` parse, now fail-loud via `parse_var`). The `LOOM_BOOTSTRAP_ADMIN_*` leg named in this entry was removed outright by PR #308 (`loom create-admin`); the stale `docs/deploy.md` rows for it are deleted alongside.
```

In `docs/deploy.md`, delete the two stale table rows (lines 191-192):

```markdown
| `LOOM_BOOTSTRAP_ADMIN_USERNAME` | no | — | Create an admin user on first boot (requires `LOOM_BOOTSTRAP_ADMIN_PASSWORD`). |
| `LOOM_BOOTSTRAP_ADMIN_PASSWORD` | no | — | Password for the bootstrapped admin user. |
```

(These env vars no longer exist; first-admin creation is
`loom create-admin --username <u>` with the password on stdin.)

Substitute the real PR number for `#N` (open the PR first, then push this
commit to the same branch; or amend at PR time).

Validate: `bash tools/docs.sh validate` — expected: OK.

- [ ] **Step 4: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p7.log 2>&1; grep -c Failed /tmp/p7.log` — expected `0`.

```bash
git add src/services/runtime docs/ROADMAP.md docs/ISSUES.md docs/deploy.md
git commit -m "refactor(runtime): delete the lossy *_from_env TTL readers; close registers

Every caller now goes through the fail-loud snapshot readers, so the
compiler proves the iss-config-silent-fallbacks sweep is complete. Closes
road-service-bootstrap (done) and iss-config-silent-fallbacks (fixed);
drops docs/deploy.md's stale LOOM_BOOTSTRAP_ADMIN_* rows (removed by #308).

Fixes iss-config-silent-fallbacks. Part of road-service-bootstrap."
```

Then finish the branch per `superpowers:finishing-a-development-branch` (push +
open PR against `main`; poll CI via the commit-status endpoint + BuildBuddy MCP,
not `gh pr checks`).
