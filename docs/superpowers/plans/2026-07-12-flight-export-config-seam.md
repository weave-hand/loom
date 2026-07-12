# Flight-export config seam Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the Arrow Flight export listener's two raw `env.get`/`parse_var` reads in query-api's `serve()` (`LOOM_FLIGHT_BIND_ADDR`, `LOOM_EXPORT_MAX_ROWS`) onto a typed `FlightExportTuning` section of `QueryApiConfig` — a verbatim sibling of the existing `SqlWireTuning` — so both knobs load through the layered `defaults < file < env` seam with startup validation, env names unchanged.

**Architecture:** Add a `FlightExportTuning { bind_addr: Option<String>, max_rows: u32 }` struct to `query-api/src/config.rs` mirroring `SqlWireTuning` exactly (same `overlay_env` env-name mapping, same `validate` shape), wire it into `QueryApiConfig` (field + `LayeredConfig` delegation), then rewire `serve.rs` to consume `app_cfg.flight_export.{bind_addr,max_rows}` and delete the raw reads + the now-orphaned `DEFAULT_EXPORT_MAX_ROWS` const. Env-only deployments behave identically (same keys, same defaults, same opt-in-by-presence); the config-file layer and key-naming validation are the new capability.

**Tech Stack:** Rust, serde, the `loom_config` layered-config seam (`overlay_opt`/`invalid`/`load`/`LayeredConfig`), buck2 `rust_test`.

## Global Constraints

- **Strict clippy (pedantic + restriction)** on production lib/bin code: no `unwrap`/`expect`/`panic`/`indexing_slicing`/`get_unwrap`; use `#[expect(lint, reason = "...")]` locally. Test code is exempt from panic-safety lints via the test macros.
- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`. The new config test is a plain `rust_test` (no fixture needed — it exercises pure config loading), mirroring `sql-wire-config`.
- **Run `buck2 run //tools:prek -- run --all-files` before every commit** (`git add` new files first — prek skips untracked). Markdown: no trailing whitespace, one trailing newline.
- **Env names are frozen:** `LOOM_FLIGHT_BIND_ADDR` and `LOOM_EXPORT_MAX_ROWS` must map through unchanged — no deployment breaks.
- **No behavior change for env-only deployments:** same keys, same defaults (`bind_addr` None ⇒ listener off; `max_rows` default `1_000_000`), same opt-in-by-presence contract. The only deliberate improvement: a malformed `LOOM_FLIGHT_BIND_ADDR` now fails at config load naming the key (was: inside `spawn_flight_export`).
- **Build/test commands (foreground):** build `buck2 build -v0 --console none //src/...`; test `buck2 test --console none <target>`. No `.sqlx` change in this item.

## Design pins (verified against the code at 07188f67)

### `SqlWireTuning` is the exact template (`src/services/query-api/src/config.rs:40-75`)

```rust
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct SqlWireTuning {
    pub bind_addr: Option<String>,
    pub max_rows: u32,
}
impl Default for SqlWireTuning {
    fn default() -> Self { Self { bind_addr: None, max_rows: 1_000_000 } }
}
impl SqlWireTuning {
    pub fn overlay_env(&mut self, vars: &HashMap<String, String>) -> Result<(), ConfigError> {
        if let Some(v) = vars.get("LOOM_SQL_WIRE_BIND_ADDR") { self.bind_addr = Some(v.clone()); }
        overlay_opt(vars, "LOOM_SQL_WIRE_MAX_ROWS", &mut self.max_rows)?;
        Ok(())
    }
    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(addr) = &self.bind_addr {
            addr.parse::<std::net::SocketAddr>().map_err(|e| invalid("LOOM_SQL_WIRE_BIND_ADDR", e))?;
        }
        if self.max_rows == 0 { return Err(invalid("LOOM_SQL_WIRE_MAX_ROWS", "must be >= 1")); }
        Ok(())
    }
}
```

`FlightExportTuning` is this struct with `SQL_WIRE`→`FLIGHT`/`EXPORT` env names. `config.rs` already imports `use loom_config::{ConfigError, invalid, overlay_opt};` and `use std::collections::HashMap;` — no new imports.

### `QueryApiConfig` wiring (`config.rs:81-100`)

The struct has `serving`/`sql_wire` fields; `LayeredConfig::overlay_env`/`validate` delegate to each in turn. Add `flight_export` alongside, delegating in both methods.

### The two `serve.rs` reads to replace (`src/services/query-api/src/serve.rs:14-16, 100-107`)

```rust
const DEFAULT_EXPORT_MAX_ROWS: u32 = 1_000_000;               // line 16 — becomes orphaned, remove
// ...
if let Some(bind) = env.get("LOOM_FLIGHT_BIND_ADDR") {         // lines 100-107
    let max_rows =
        service_runtime::parse_var(&env, "LOOM_EXPORT_MAX_ROWS", DEFAULT_EXPORT_MAX_ROWS)?;
    spawn_flight_export(bind, &engine_socket, auth_flight, cp_flight, max_rows).await?;
}
```

`app_cfg` (a `QueryApiConfig`) is already in scope (`serve.rs:27`), and the SQL-wire block immediately below (`serve.rs:112-121`) already consumes `app_cfg.sql_wire.{bind_addr,max_rows}` — the flight-export block becomes its structural twin. `spawn_flight_export(bind: &str, …, max_rows: u32)` (`serve.rs:127-164`) keeps its signature and its internal `SocketAddr` re-parse (now redundant-but-harmless, since `validate` already parsed it) — only the CALLER changes.

### No env→`serve()` e2e exists

`governed-flight-export-e2e` (`tests/governed_flight_export_e2e.rs`) constructs `FlightExportService::new(...)` **directly** with a literal `max_rows`; it never drives the `env → serve() → spawn_flight_export` path. So Task 2's non-regression is: whole-tree build green + the new config test green + existing suites (incl. `governed-flight-export-e2e`, `sql-wire-config`) unaffected. `flight_export.rs` (the service) is not touched by this item.

## File Structure

**Modify (production):**
- `src/services/query-api/src/config.rs` — add `FlightExportTuning`; add `QueryApiConfig.flight_export` + its two `LayeredConfig` delegations.
- `src/services/query-api/src/serve.rs` — rewire the flight-export block to `app_cfg.flight_export`; delete the `DEFAULT_EXPORT_MAX_ROWS` const and the `parse_var`/`env.get` reads.

**Create (test):**
- `src/services/query-api/tests/flight_export_config.rs` — config-loading test, case-for-case mirror of `tests/sql_wire_config.rs`.

**Modify (build):**
- `src/services/query-api/BUCK` — a `flight-export-config` `rust_test` target mirroring `sql-wire-config` (`BUCK:189-199`).

**Docs / register (final task):**
- `docs/system-capabilities/query-api.md` — one-line update noting the flight-export knobs now ride the typed config seam.
- `docs/ROADMAP.md` — remove `#road-flight-export-config-seam`.

---

## Task 1: `FlightExportTuning` + `QueryApiConfig.flight_export` + config test

**Files:**
- Modify: `src/services/query-api/src/config.rs` (add the struct after `SqlWireTuning`, ~line 75; add the field + delegations in `QueryApiConfig`, lines 81-100)
- Create: `src/services/query-api/tests/flight_export_config.rs`
- Modify: `src/services/query-api/BUCK` (new `flight-export-config` target after the `sql-wire-config` block, lines 189-199)

**Interfaces:**
- Consumes: `loom_config::{ConfigError, invalid, overlay_opt, load, LayeredConfig}` (already imported in `config.rs`); `HashMap` (already imported).
- Produces (Task 2 relies on these EXACT names/types):
  - `pub struct FlightExportTuning { pub bind_addr: Option<String>, pub max_rows: u32 }` with `Default` (`bind_addr: None`, `max_rows: 1_000_000`), `overlay_env(&mut self, &HashMap<String,String>) -> Result<(), ConfigError>`, `validate(&self) -> Result<(), ConfigError>`.
  - `QueryApiConfig.flight_export: FlightExportTuning` (a public field).

- [ ] **Step 1: Write the failing config test**

Create `src/services/query-api/tests/flight_export_config.rs` (a case-for-case mirror of `tests/sql_wire_config.rs`, reading the `flight_export` domain and the `LOOM_FLIGHT_*`/`LOOM_EXPORT_*` keys):

```rust
//! Config behaviour of `FlightExportTuning` on the `QueryApiConfig` layered seam:
//! `bind_addr` is opt-in (unset => the Arrow Flight export listener does not
//! start) and validated as a `SocketAddr` at startup; `max_rows` is the
//! per-export row cap. Env names (`LOOM_FLIGHT_BIND_ADDR`, `LOOM_EXPORT_MAX_ROWS`)
//! are unchanged from the pre-seam raw reads — this test pins that contract.
use std::collections::HashMap;

use query_api::config::QueryApiConfig;

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn empty_env_defaults_bind_addr_none_and_max_rows_1e6() {
    let cfg: QueryApiConfig = loom_config::load(&map(&[])).unwrap();
    assert_eq!(cfg.flight_export.bind_addr, None);
    assert_eq!(cfg.flight_export.max_rows, 1_000_000);
}

#[test]
fn env_overlays_both_fields() {
    let cfg: QueryApiConfig = loom_config::load(&map(&[
        ("LOOM_FLIGHT_BIND_ADDR", "127.0.0.1:50052"),
        ("LOOM_EXPORT_MAX_ROWS", "500"),
    ]))
    .unwrap();
    assert_eq!(cfg.flight_export.bind_addr.as_deref(), Some("127.0.0.1:50052"));
    assert_eq!(cfg.flight_export.max_rows, 500);
}

#[test]
fn file_layer_sets_both_then_env_overrides() {
    // The headline capability of this item: the knobs now have a file layer
    // (defaults < file < env). Mirrors `loom-config/tests/load.rs`'s file test.
    use std::io::Write;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    write!(
        f,
        r#"{{"flight_export": {{"bind_addr": "0.0.0.0:50052", "max_rows": 500000}}}}"#
    )
    .unwrap();
    f.flush().unwrap();
    let path = f.path().to_str().unwrap().to_string();
    // File only: file values apply over the defaults.
    let from_file: QueryApiConfig = loom_config::load(&map(&[("LOOM_CONFIG_FILE", &path)])).unwrap();
    assert_eq!(from_file.flight_export.bind_addr.as_deref(), Some("0.0.0.0:50052"));
    assert_eq!(from_file.flight_export.max_rows, 500_000);
    // File + env: env overlays the file.
    let env_wins: QueryApiConfig = loom_config::load(&map(&[
        ("LOOM_CONFIG_FILE", &path),
        ("LOOM_EXPORT_MAX_ROWS", "42"),
    ]))
    .unwrap();
    assert_eq!(env_wins.flight_export.max_rows, 42, "env overlays file");
    // `f` drops at end of scope, removing the temp file — no manual cleanup.
}

#[test]
fn malformed_bind_addr_is_error() {
    // `QueryApiConfig` is intentionally not `Debug`, so match rather than `unwrap_err`.
    match loom_config::load::<QueryApiConfig>(&map(&[("LOOM_FLIGHT_BIND_ADDR", "not-an-addr")])) {
        Err(e) => assert!(format!("{e}").contains("LOOM_FLIGHT_BIND_ADDR")),
        Ok(_) => panic!("expected a validation error for a malformed bind_addr"),
    }
}

#[test]
fn zero_max_rows_is_error() {
    match loom_config::load::<QueryApiConfig>(&map(&[("LOOM_EXPORT_MAX_ROWS", "0")])) {
        Err(e) => assert!(format!("{e}").contains("LOOM_EXPORT_MAX_ROWS")),
        Ok(_) => panic!("expected a validation error for max_rows = 0"),
    }
}
```

Add the buck target in `src/services/query-api/BUCK` right after the `sql-wire-config` block (lines 189-199):

```python
rust_test(
    name = "flight-export-config",
    crate = "flight_export_config",
    srcs = ["tests/flight_export_config.rs"],
    crate_root = "tests/flight_export_config.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/loom-config:loom-config",
        "//third-party:tempfile",
    ],
)
```

(`//third-party:tempfile` is the dep the file-layer test needs — the same one `//src/loom-config:load`'s target uses; confirm the exact label by checking `loom-config/BUCK`'s `load` test target if `//third-party:tempfile` does not resolve.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/services/query-api:flight-export-config`
Expected: FAIL — `QueryApiConfig` has no `flight_export` field yet (compile error `no field 'flight_export'`).

- [ ] **Step 3: Add `FlightExportTuning` + wire it into `QueryApiConfig`**

In `src/services/query-api/src/config.rs`, add after the `SqlWireTuning` impl block (after line 75):

```rust
/// External Arrow Flight export listener tuning. `bind_addr` unset => the export
/// listener does not start (opt-in, like the SQL wire). A verbatim sibling of
/// `SqlWireTuning`; env names (`LOOM_FLIGHT_BIND_ADDR`, `LOOM_EXPORT_MAX_ROWS`)
/// are unchanged from the pre-seam raw reads (road-flight-export-config-seam).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct FlightExportTuning {
    pub bind_addr: Option<String>,
    pub max_rows: u32,
}

impl Default for FlightExportTuning {
    fn default() -> Self {
        Self {
            bind_addr: None,
            max_rows: 1_000_000,
        }
    }
}

impl FlightExportTuning {
    pub fn overlay_env(&mut self, vars: &HashMap<String, String>) -> Result<(), ConfigError> {
        if let Some(v) = vars.get("LOOM_FLIGHT_BIND_ADDR") {
            self.bind_addr = Some(v.clone());
        }
        overlay_opt(vars, "LOOM_EXPORT_MAX_ROWS", &mut self.max_rows)?;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(addr) = &self.bind_addr {
            addr.parse::<std::net::SocketAddr>()
                .map_err(|e| invalid("LOOM_FLIGHT_BIND_ADDR", e))?;
        }
        if self.max_rows == 0 {
            return Err(invalid("LOOM_EXPORT_MAX_ROWS", "must be >= 1"));
        }
        Ok(())
    }
}
```

Then extend `QueryApiConfig` (lines 81-100): add the field and both delegations.

- In the struct: add `pub flight_export: FlightExportTuning,` after `pub sql_wire: SqlWireTuning,`.
- In `overlay_env`: add `self.flight_export.overlay_env(env)?;` after the `sql_wire` line.
- In `validate`: add `self.flight_export.validate()?;` after the `sql_wire` line.

Also fix the now-stale `SqlWireTuning` doc comment (`config.rs`, currently ~lines 37-39). It reads:

```rust
/// External Flight SQL wire tuning. `bind_addr` unset => the listener does not
/// start (opt-in, like the Flight export). Typed from day one — the new wire's
/// knobs never join the export's raw env reads (fut-flight-export-config-seam).
```

The "never join the export's raw env reads" contrast is false once `FlightExportTuning` exists, and it cites the retired `fut-` id. Replace the last sentence:

```rust
/// External Flight SQL wire tuning. `bind_addr` unset => the listener does not
/// start (opt-in, like the Flight export). Both external wires are typed
/// siblings on the layered config seam (see `FlightExportTuning`).
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test --console none //src/services/query-api:flight-export-config`
Expected: PASS — all four cases (defaults, env overlay, malformed addr, zero max_rows).

- [ ] **Step 5: Confirm the config crate + its siblings still build/lint clean**

Run: `buck2 build -v0 --console none //src/services/query-api:query-api //src/services/query-api:sql-wire-config`
Expected: clean (exit 0). The new `flight_export` field is `pub` and read by the test, so no dead-code warning.

- [ ] **Step 6: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(query-api): FlightExportTuning on the layered config seam

A verbatim SqlWireTuning sibling: LOOM_FLIGHT_BIND_ADDR (opt-in, SocketAddr-
validated) + LOOM_EXPORT_MAX_ROWS (default 1_000_000, rejected at 0) now load
through QueryApiConfig (defaults < file < env). Env names unchanged. serve.rs
still reads them raw — rewired next; no behavior change yet."
```

---

## Task 2: Rewire `serve.rs` onto `app_cfg.flight_export`; delete the raw reads

**Files:**
- Modify: `src/services/query-api/src/serve.rs` (delete const at line 16; replace the block at lines 100-107)

**Interfaces:**
- Consumes: `QueryApiConfig.flight_export: FlightExportTuning` with `.bind_addr: Option<String>` and `.max_rows: u32` (Task 1). `app_cfg` is already bound at `serve.rs:27`.
- Produces: no new symbols — this task removes the raw `env.get("LOOM_FLIGHT_BIND_ADDR")` / `service_runtime::parse_var(…, "LOOM_EXPORT_MAX_ROWS", …)` reads and the `DEFAULT_EXPORT_MAX_ROWS` const.

- [ ] **Step 1: Replace the flight-export block**

In `src/services/query-api/src/serve.rs`, replace the current block (lines 100-107):

```rust
    // Optional external Arrow Flight export listener (opt-in via LOOM_FLIGHT_BIND_ADDR).
    if let Some(bind) = env.get("LOOM_FLIGHT_BIND_ADDR") {
        // Fail-loud: a malformed row cap is a startup error when the export
        // endpoint was explicitly requested (was: silent fallback to the default).
        let max_rows =
            service_runtime::parse_var(&env, "LOOM_EXPORT_MAX_ROWS", DEFAULT_EXPORT_MAX_ROWS)?;
        spawn_flight_export(bind, &engine_socket, auth_flight, cp_flight, max_rows).await?;
    }
```

with the typed-config twin of the SQL-wire block right below it:

```rust
    // Optional external Arrow Flight export listener (opt-in via LOOM_FLIGHT_BIND_ADDR;
    // typed seam — its knobs are the typed `FlightExportTuning`, validated at config
    // load, not raw env reads, per road-flight-export-config-seam).
    if let Some(bind) = app_cfg.flight_export.bind_addr.clone() {
        spawn_flight_export(
            &bind,
            &engine_socket,
            auth_flight,
            cp_flight,
            app_cfg.flight_export.max_rows,
        )
        .await?;
    }
```

(`spawn_flight_export(bind: &str, …)` is unchanged — it still re-parses `bind` to a `SocketAddr` internally, now redundant-but-harmless since `validate` already parsed it; keeping the signature is the minimal change and matches how `spawn_sql_wire` consumes its addr.)

Also fix the now-stale comment on the SQL-wire block just below (`serve.rs`, currently ~lines 109-111):

```rust
    // Optional external Flight SQL wire (opt-in via LOOM_SQL_WIRE_BIND_ADDR; typed seam —
    // this wire's knobs are the typed `SqlWireTuning`, not raw env reads, per
    // fut-flight-export-config-seam).
```

The "not raw env reads" contrast no longer holds (the flight-export block above is now its typed twin) and it cites the retired `fut-` id. Replace with:

```rust
    // Optional external Flight SQL wire (opt-in via LOOM_SQL_WIRE_BIND_ADDR; typed seam —
    // its knobs are the typed `SqlWireTuning`, the twin of the flight-export block above).
```

- [ ] **Step 2: Delete the orphaned const**

In `src/services/query-api/src/serve.rs`, remove lines 14-16 (the `DEFAULT_EXPORT_MAX_ROWS` const + its doc comment) — its only use was the `parse_var` default just deleted; the value now lives in `FlightExportTuning::default()`.

- [ ] **Step 3: Build to verify the rewire compiles clean**

Run: `buck2 build -v0 --console none //src/services/query-api:query-api`
Expected: clean (exit 0). No unused-import/dead-code/unused-var warning (the const is gone; `env` is still used by other reads — `LOOM_UI_DIR`, `LOOM_CORS_ALLOWED_ORIGINS`; `service_runtime::parse_var` may become an unused import ONLY if nothing else in the file uses it — check and drop the import if the compiler flags it).

- [ ] **Step 4: Verify non-regression across the tree + the affected suites**

Run: `buck2 build -v0 --console none //src/...`
Run: `buck2 test --console none //src/services/query-api:flight-export-config //src/services/query-api:sql-wire-config //src/services/query-api:governed-flight-export-e2e`
Expected: all PASS. `governed-flight-export-e2e` constructs `FlightExportService` directly (not via `serve()`), so it is unaffected — its green run confirms the export service path is intact.

- [ ] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "refactor(query-api): consume flight-export knobs from the typed config seam

serve() now sources LOOM_FLIGHT_BIND_ADDR/LOOM_EXPORT_MAX_ROWS from
app_cfg.flight_export (structural twin of the sql_wire block below it);
the raw env.get/parse_var reads and the DEFAULT_EXPORT_MAX_ROWS const are
gone. Env-only deployments behave identically; a malformed bind addr now
fails at config load naming the key. Closes the last raw-env corner in
query-api's config surface."
```

---

## Task 3: Docs + register close-out

**Files:**
- Modify: `docs/system-capabilities/query-api.md` (the config/serve description)
- Modify: `docs/ROADMAP.md` (remove `#road-flight-export-config-seam`)

- [ ] **Step 1: Update the capability doc**

In `docs/system-capabilities/query-api.md`, find where the external listeners / config are described (the paragraph mentioning the Flight export listener and/or `LOOM_SQL_WIRE_BIND_ADDR`'s typed `SqlWireTuning`). Add/adjust one sentence: the Arrow Flight export listener's knobs (`LOOM_FLIGHT_BIND_ADDR`, `LOOM_EXPORT_MAX_ROWS`) now ride the same typed `FlightExportTuning` config seam (defaults < file < env, startup validation naming the key) as the SQL wire — the last raw-env corner in query-api's config surface is closed. If the doc currently notes this as a deferred/leftover item, remove that note.

- [ ] **Step 2: Close the register item**

Remove the `#road-flight-export-config-seam` entry from `docs/ROADMAP.md` (registers carry open work only; the capability is now documented in `query-api.md`). Leave the `## query` section header even if it empties (matches the empty-section convention). Run the `loom-docs-update` skill if finishing the branch in the same session.

Run: `bash tools/docs.sh validate`
Expected: `OK` (no dangling `[[...]]` links to the removed id).

- [ ] **Step 3: Full verification + final commit**

Run: `buck2 build -v0 --console none //src/...`
Run: `buck2 test --console none //src/services/query-api:flight-export-config //src/services/query-api:sql-wire-config //src/services/query-api:governed-flight-export-e2e` — then the full suite (`buck2 test --console none //src/...`, `-j 8` locally).
Expected: all PASS (the pre-existing `src/ui/e2e:login` chromedriver/libnspr4 environmental failure, if present, is unrelated — this branch touches no UI).

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "docs(query-api): flight-export config seam shipped; close road-flight-export-config-seam"
```

Then finish per `superpowers:finishing-a-development-branch` (push + open PR with head `work/road-flight-export-config-seam` — never a local merge).
