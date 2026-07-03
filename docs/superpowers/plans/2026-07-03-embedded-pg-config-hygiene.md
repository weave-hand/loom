# Embedded-Postgres configuration hygiene Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make embedded mode (`LOOM_PG_MODE=embedded`) stop demanding PG-binary and `LOOM_DB_*` vars from processes that never boot a cluster, and turn a missing `libxml2.so.2` into a named fail-fast error pointing at the host-prereqs docs — resolving `#iss-embedded-config-requires-pg-bin-dir` and `#iss-embedded-pg-libxml2`, and folding in the already-removed `#fut-embedded-pg-db-vars-optional`.

**Architecture:** Role is learned structurally, not passed. `Config::from_map` keeps its signature; PG-path validation moves from parse time to the one code path that actually spawns a cluster (`build_pool_managed`'s embedded branch). `EmbeddedSettings` gains an `Option<PgBinPaths>` (never `req_var`'d); `DbConfig::from_map` defaults the five `LOOM_DB_*` vars in embedded mode so they are consistent-by-construction with `EmbeddedPg::connect_options()`. `EmbeddedPg::start` gains a cheap `postgres -V` preflight that classifies dynamic-loader failures into a new `MissingSharedLibrary` error. dev-up.sh sheds its placeholder blocks; deploy.md documents the host-libxml2 requirement.

**Tech Stack:** Rust 2024, buck2, `thiserror`, `sqlx`/`PgConnectOptions`, tokio, the `loom_fixture_test` macro (hermetic Postgres), `rust_test` integration targets only.

## Global Constraints

- **Tests are `rust_test`/`loom_fixture_test` integration targets only** — never inline `#[cfg(test)]` modules (the `no-inline-tests` prek hook fails the build otherwise). Each new test file is its own target in the crate's BUCK, mirroring an existing one.
- **New fixture tests use `loom_fixture_test`, not bare `rust_test`** (they need the injected PG/MinIO env). Pure-logic/error-path tests that never boot a real cluster stay plain `rust_test`.
- **Strict clippy** (pedantic + restriction): no `unwrap`/`expect`/`panic`/`indexing_slicing`/`todo` in production `src/**` code (test code is exempted from the panic-safety lints by the `rust_test` wrapper). Carry source errors; use `#[expect(lint, reason = "...")]` locally when unavoidable.
- **`Config::from_map` keeps its public signature** `pub fn from_map(vars: &HashMap<String, String>) -> Result<Config, ConfigError>`. `DbConfig::from_map(vars)` and `EmbeddedSettings::from_map(vars, data_path)` also keep their existing signatures — mode/role is read from `vars` internally, never added as a parameter.
- **External mode is byte-for-byte unchanged**: all five `LOOM_DB_*` stay required; `LOOM_PG_*` are `n/a`. Only `LOOM_PG_MODE == "embedded"` changes behavior.
- **`managed_postgres` gains no new external dependency** (its lib deps stay `sqlx`, `thiserror`, `tokio`). The libxml2 fix is a pure classifier + one preflight + one error variant.
- **Do not** bundle `libxml2.so.2`, statically link it, or rebuild Postgres `--without-libxml` (spec "Out of scope" — contradicts the standing direction that loom embeds only its own artifacts).
- **buck2 discipline:** build with the scoped targets below; redirect `buck2 test` output to a file and grep it (never pipe `test`/`bxl` through `tail`/`head`). On a root/cloud host the shim routes test runs to RE (non-root), which every embedded test already relies on.

---

## File Structure

- `src/services/managed-postgres/src/lib.rs` — add `classify_loader_error` (pure fn), `EmbeddedPgError::MissingSharedLibrary { lib }`, and a `postgres -V` preflight in `EmbeddedPg::start`. (Task 1)
- `src/services/managed-postgres/tests/loader_error.rs` — NEW. Classifier unit tests + a stub-`postgres`-script `start` test. (Task 1)
- `src/services/runtime/src/lib.rs` — add `PgBinPaths`; restructure `EmbeddedSettings`; embedded defaulting in `DbConfig::from_map`; move the PG-bin requirement into `build_pool_managed`'s embedded branch. (Task 2)
- `src/services/runtime/tests/embedded_config.rs` — MODIFY for the new `EmbeddedSettings` shape + add bin-optional and DB-default parse tests. (Task 2)
- `src/services/runtime/tests/build_pool_managed.rs` — NEW. `build_pool_managed` on an embedded `Config` with `bin: None` fails with the `LOOM_PG_BIN_DIR` config error before any spawn. (Task 2)
- `src/services/runtime/tests/embedded_client_only.rs` — NEW `loom_fixture_test`. Boots a cluster, then a client-only embedded `Config` with *no* PG-bin and *no* `LOOM_DB_*` vars connects via defaults and runs `create-admin`. (Task 3)
- `tools/dev-up.sh` — drop the placeholder `LOOM_DB_*` block and the create-admin PG-var workaround (keep the libxml2 shim + serve-path PG vars). (Task 4)
- `docs/deploy.md` — add a **Host prerequisites** subsection (libxml2 requirement + image posture). (Task 4; the `## Known gaps` id removal happens at register-close/finish.)
- The three crates' `BUCK` files — new test targets. (Tasks 1–3)

---

## Task 1: `managed_postgres` — libxml2 fail-fast

**Files:**
- Modify: `src/services/managed-postgres/src/lib.rs`
- Test: `src/services/managed-postgres/tests/loader_error.rs` (create)
- Modify: `src/services/managed-postgres/BUCK`

**Interfaces:**
- Produces: `pub fn classify_loader_error(stderr: &str) -> Option<String>`; `EmbeddedPgError::MissingSharedLibrary { lib: String }`.
- Consumes: existing `pg_command(program: PathBuf, ld_library_path: &str) -> Command`, `EmbeddedPg::start(cfg: EmbeddedPgConfig)`, `EmbeddedPgConfig { bin_dir, ld_library_path, data_dir, socket_dir, database }`.

- [ ] **Step 1: Write the failing classifier + preflight tests**

Create `src/services/managed-postgres/tests/loader_error.rs`:

```rust
//! libxml2 fail-fast: a dynamic-loader failure from a pg binary is classified
//! into `MissingSharedLibrary { lib }` (named, points at docs), not a raw
//! `Initdb`/`ServerExited` status. The classifier is a pure function (unit-tested
//! without a broken host); `start`'s preflight is exercised with a stub `postgres`
//! script that exits 127 with a loader error on `-V` — hermetic, no reliance on a
//! host actually lacking the library.

use std::path::{Path, PathBuf};

use managed_postgres::{EmbeddedPg, EmbeddedPgConfig, EmbeddedPgError, classify_loader_error};

#[test]
fn classifies_glibc_loader_error_naming_the_library() {
    let stderr = "/x/bin/postgres: error while loading shared libraries: \
                  libxml2.so.2: cannot open shared object file: No such file or directory\n";
    assert_eq!(
        classify_loader_error(stderr).as_deref(),
        Some("libxml2.so.2")
    );
}

#[test]
fn non_loader_stderr_is_not_classified() {
    assert_eq!(classify_loader_error("initdb: error: directory is not empty\n"), None);
    assert_eq!(classify_loader_error(""), None);
}

/// Write an executable stub named `postgres` under `dir` that prints `stderr_line`
/// to stderr and exits with `code`, regardless of args (so `postgres -V` hits it).
fn stub_postgres(dir: &Path, stderr_line: &str, code: i32) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join("postgres");
    std::fs::write(
        &path,
        format!("#!/bin/sh\necho '{stderr_line}' 1>&2\nexit {code}\n"),
    )
    .expect("write stub");
    let mut perm = std::fs::metadata(&path).expect("meta").permissions();
    perm.set_mode(0o755);
    std::fs::set_permissions(&path, perm).expect("chmod stub");
    path
}

#[tokio::test]
async fn start_preflight_maps_loader_failure_to_named_error() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let bin = tmp.path().join("bin");
    std::fs::create_dir_all(&bin).expect("mkdir bin");
    stub_postgres(
        &bin,
        "postgres: error while loading shared libraries: libxml2.so.2: \
         cannot open shared object file: No such file or directory",
        127,
    );
    let cfg = EmbeddedPgConfig {
        bin_dir: bin,
        ld_library_path: String::new(),
        data_dir: tmp.path().join("pgdata"),
        socket_dir: tmp.path().join("pgrun"),
        database: "loom".to_string(),
    };
    let err = EmbeddedPg::start(cfg).await.expect_err("preflight must fail");
    match err {
        EmbeddedPgError::MissingSharedLibrary { ref lib } => assert_eq!(lib, "libxml2.so.2"),
        other => panic!("expected MissingSharedLibrary, got {other:?}"),
    }
    // Message names the library and points operators at the deploy docs.
    let shown = err.to_string();
    assert!(shown.contains("libxml2.so.2"), "names the lib: {shown}");
    assert!(shown.contains("docs/deploy.md"), "points at docs: {shown}");
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test //src/services/managed-postgres:loader-error > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[|cannot find" /tmp/t1.log`
Expected: FAIL — `classify_loader_error` and `MissingSharedLibrary` don't exist yet (build error), and the `loader-error` target isn't defined. (Add the BUCK target in Step 3 so the target resolves; the compile still fails on the missing symbols until Step 4.)

- [ ] **Step 3: Add the BUCK target**

In `src/services/managed-postgres/BUCK`, after the `db-name` target, add:

```python
rust_test(
    name = "loader-error",
    crate = "loader_error",
    srcs = ["tests/loader_error.rs"],
    crate_root = "tests/loader_error.rs",
    edition = "2024",
    deps = [
        ":managed-postgres",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 4: Add the classifier and error variant**

In `src/services/managed-postgres/src/lib.rs`, add the pure classifier near `validate_db_name` (top-level `pub fn`):

```rust
/// Extract the missing library name from a dynamic-loader failure line, if the
/// stderr contains one. Matches the glibc loader message
/// `error while loading shared libraries: <lib>: cannot open shared object file`.
/// Pure so it is unit-testable without a host that actually lacks the library.
#[must_use]
pub fn classify_loader_error(stderr: &str) -> Option<String> {
    const MARKER: &str = "error while loading shared libraries: ";
    let start = stderr.find(MARKER)? + MARKER.len();
    let lib = stderr
        .get(start..)?
        .split([':', '\n'])
        .next()?
        .trim();
    if lib.is_empty() {
        None
    } else {
        Some(lib.to_string())
    }
}
```

Add the error variant to `EmbeddedPgError` (place it after `RunningAsRoot`):

```rust
    #[error(
        "embedded Postgres cannot start: missing shared library {lib}. loom bundles \
         only its own Postgres artifacts, not system libraries — install it on the host \
         (see the Host prerequisites section of docs/deploy.md)"
    )]
    MissingSharedLibrary { lib: String },
```

- [ ] **Step 5: Add the preflight to `EmbeddedPg::start`**

In `EmbeddedPg::start`, immediately after `validate_db_name(&cfg.database)?;` and before `ensure_dir_secure(&cfg.data_dir)?;`, insert the preflight call:

```rust
        // Cheap, side-effect-free preflight: a dynamic-loader failure (e.g. a
        // host missing libxml2.so.2) becomes a named error pointing at the deploy
        // docs, instead of a cryptic Initdb/ServerExited status later. Non-loader
        // failures fall through so the real init/spawn path surfaces its usual error.
        Self::preflight_shared_libs(&cfg).await?;
```

Add the preflight method inside `impl EmbeddedPg` (near `run_initdb`):

```rust
    /// Run `postgres -V` and convert a dynamic-loader failure into
    /// `MissingSharedLibrary`. Any other failure (or an I/O error spawning the
    /// probe) is swallowed so the normal init/spawn path reproduces today's error.
    async fn preflight_shared_libs(cfg: &EmbeddedPgConfig) -> Result<(), EmbeddedPgError> {
        let out = pg_command(cfg.bin_dir.join("postgres"), &cfg.ld_library_path)
            .arg("-V")
            .output()
            .await?;
        if out.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&out.stderr);
        if let Some(lib) = classify_loader_error(&stderr) {
            return Err(EmbeddedPgError::MissingSharedLibrary { lib });
        }
        Ok(())
    }
```

> Note: the `?` after `.output().await` propagates a genuine spawn `io::Error` (e.g. the `postgres` binary is absent) as `EmbeddedPgError::Io` — acceptable and strictly better than today (start would fail at init anyway). The stub test's `postgres` exists and exits 127, so it reaches the classifier.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `buck2 test //src/services/managed-postgres:loader-error > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS (3 tests). Then confirm no regression in the existing lifecycle/hardening suites:
Run: `buck2 test //src/services/managed-postgres:db-name //src/services/managed-postgres:embedded-hardening //src/services/managed-postgres:embedded-lifecycle > /tmp/t1b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1b.log`
Expected: PASS (the happy-path `postgres -V` preflight succeeds when the real libs are present, so lifecycle boots as before).

- [ ] **Step 7: Clippy + commit**

Run: `buck2 build '//src/services/managed-postgres:managed-postgres[clippy.txt]' > /tmp/t1c.log 2>&1; cat /tmp/t1c.log` (empty == clean).

```bash
git add src/services/managed-postgres/src/lib.rs src/services/managed-postgres/tests/loader_error.rs src/services/managed-postgres/BUCK
git commit -m "feat(managed-postgres): fail-fast named error for a missing shared library"
```

---

## Task 2: Runtime config — optional PG-bin + embedded DB defaulting + spawn-site gate

**Files:**
- Modify: `src/services/runtime/src/lib.rs`
- Modify: `src/services/runtime/tests/embedded_config.rs`
- Test: `src/services/runtime/tests/build_pool_managed.rs` (create)
- Modify: `src/services/runtime/BUCK`

**Interfaces:**
- Produces: `pub struct PgBinPaths { pub bin_dir: PathBuf, pub ld_library_path: String }`; restructured `pub struct EmbeddedSettings { pub data_dir: PathBuf, pub socket_dir: PathBuf, pub database: String, pub bin: Option<PgBinPaths> }`.
- Consumes: `managed_postgres::EmbeddedPgConfig { bin_dir, ld_library_path, data_dir, socket_dir, database }`; `EmbeddedPg::start`; `ConfigError::{MissingVar, Invalid}`; `RuntimeError::Config(#[from] ConfigError)`; `req_var`, `parse_var`.

- [ ] **Step 1: Write the failing config tests**

Rewrite `src/services/runtime/tests/embedded_config.rs` so it targets the new `EmbeddedSettings` shape and covers the bin-optional + DB-defaulting behavior. Replace the whole file with:

```rust
use std::collections::HashMap;
use std::path::Path;

use service_runtime::{Config, EmbeddedSettings};

fn external_base() -> HashMap<String, String> {
    // The minimal external-mode keys Config::from_map requires.
    [
        ("LOOM_BIND_ADDR", "127.0.0.1:8080"),
        ("LOOM_DB_HOST", "/var/run/pg"),
        ("LOOM_DB_PORT", "5432"),
        ("LOOM_DB_USER", "postgres"),
        ("LOOM_DB_PASSWORD", ""),
        ("LOOM_DB_NAME", "loom"),
        ("LOOM_DATA_PATH", "/tmp/loomdata"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

/// Embedded mode with only the two keys embedded genuinely needs: mode + data path.
/// No LOOM_PG_BIN_DIR, no LOOM_DB_* — the regression this task fixes.
fn embedded_minimal() -> HashMap<String, String> {
    [
        ("LOOM_BIND_ADDR", "127.0.0.1:8080"),
        ("LOOM_DATA_PATH", "/tmp/loomdata"),
        ("LOOM_PG_MODE", "embedded"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

#[test]
fn external_mode_has_no_embedded_settings() {
    let cfg = Config::from_map(&external_base()).expect("parse external");
    assert!(cfg.embedded.is_none());
}

#[test]
fn embedded_without_pg_bin_dir_parses_with_bin_none() {
    // Today this fails at parse with MissingVar("LOOM_PG_BIN_DIR").
    let cfg = Config::from_map(&embedded_minimal()).expect("parse embedded, no PG bin");
    let e = cfg.embedded.expect("embedded settings present");
    assert!(e.bin.is_none(), "no LOOM_PG_BIN_DIR => bin is None");
    assert_eq!(e.data_dir, Path::new("/tmp/loomdata/pgdata"));
    assert_eq!(e.socket_dir, Path::new("/tmp/loomdata/pgrun"));
    assert_eq!(e.database, "loom");
}

#[test]
fn embedded_with_pg_bin_dir_populates_bin() {
    let mut vars = embedded_minimal();
    vars.insert("LOOM_PG_BIN_DIR".into(), "/opt/pg/bin".into());
    vars.insert("LOOM_PG_LD_LIBRARY_PATH".into(), "/opt/pg/lib".into());
    let cfg = Config::from_map(&vars).expect("parse embedded");
    let bin = cfg.embedded.expect("embedded").bin.expect("bin present");
    assert_eq!(bin.bin_dir, Path::new("/opt/pg/bin"));
    assert_eq!(bin.ld_library_path, "/opt/pg/lib");
}

#[test]
fn embedded_defaults_db_vars_from_data_path() {
    // No LOOM_DB_* at all: host defaults to the socket dir, user postgres, db loom.
    let cfg = Config::from_map(&embedded_minimal()).expect("parse embedded");
    assert_eq!(cfg.db.host, "/tmp/loomdata/pgrun");
    assert_eq!(cfg.db.port, 5432);
    assert_eq!(cfg.db.user, "postgres");
    assert_eq!(cfg.db.password, "");
    assert_eq!(cfg.db.dbname, "loom");
}

#[test]
fn embedded_db_name_override_wins_and_reaches_both_surfaces() {
    let mut vars = embedded_minimal();
    vars.insert("LOOM_DB_NAME".into(), "widgets".into());
    let cfg = Config::from_map(&vars).expect("parse embedded");
    assert_eq!(cfg.db.dbname, "widgets");
    // Consistent by construction: the embedded cluster's DB matches the pool target.
    assert_eq!(cfg.embedded.expect("embedded").database, "widgets");
}

#[test]
fn external_mode_still_requires_db_vars() {
    let mut vars = external_base();
    vars.remove("LOOM_DB_HOST");
    let err = Config::from_map(&vars).expect_err("external still requires LOOM_DB_HOST");
    assert!(
        matches!(err, service_runtime::ConfigError::MissingVar(k) if k == "LOOM_DB_HOST"),
        "got {err:?}"
    );
}

#[test]
fn embedded_settings_from_map_is_none_in_external_mode() {
    let s = EmbeddedSettings::from_map(&external_base(), Path::new("/tmp/loomdata")).expect("parse");
    assert!(s.is_none());
}

#[test]
fn embedded_settings_from_map_derives_dirs_and_optional_bin() {
    let e = EmbeddedSettings::from_map(&embedded_minimal(), Path::new("/tmp/loomdata"))
        .expect("parse")
        .expect("embedded settings present");
    assert_eq!(e.data_dir, Path::new("/tmp/loomdata/pgdata"));
    assert_eq!(e.socket_dir, Path::new("/tmp/loomdata/pgrun"));
    assert_eq!(e.database, "loom");
    assert!(e.bin.is_none());
}
```

- [ ] **Step 2: Write the failing spawn-site test**

Create `src/services/runtime/tests/build_pool_managed.rs`:

```rust
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
        .err()
        .expect("must fail without PG bin dir");
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
```

- [ ] **Step 3: Add the BUCK target**

In `src/services/runtime/BUCK`, after the `embedded-config` target, add:

```python
rust_test(
    name = "build-pool-managed",
    crate = "build_pool_managed",
    srcs = ["tests/build_pool_managed.rs"],
    crate_root = "tests/build_pool_managed.rs",
    edition = "2024",
    deps = [
        ":runtime",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `buck2 test //src/services/runtime:embedded-config //src/services/runtime:build-pool-managed > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t2.log`
Expected: FAIL — `EmbeddedSettings` still has `cfg`, `bin`/`PgBinPaths` don't exist, embedded parse still `req_var`s `LOOM_PG_BIN_DIR`, and `build_pool_managed` doesn't gate on `bin`.

- [ ] **Step 5: Add `PgBinPaths` and restructure `EmbeddedSettings`**

In `src/services/runtime/src/lib.rs`, replace the `EmbeddedSettings` struct + its `from_map` (currently lines ~48–78) with:

```rust
/// The PG-binary paths needed only to *boot* an embedded cluster. Absent for
/// processes that merely connect (e.g. `loom create-admin`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgBinPaths {
    /// Postgres `bin/` directory (holds initdb, postgres, pg_ctl).
    pub bin_dir: PathBuf,
    /// `LD_LIBRARY_PATH` for the spawned binaries.
    pub ld_library_path: String,
}

/// Embedded-Postgres settings, present only when `LOOM_PG_MODE=embedded`. The
/// data/socket dirs and database name derive from `data_path` + the (defaulted)
/// DB name; `bin` is populated only when `LOOM_PG_BIN_DIR` is supplied — the
/// spawn site (`build_pool_managed`) requires it, parse time does not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddedSettings {
    /// Persistent cluster data dir (`<data_path>/pgdata`).
    pub data_dir: PathBuf,
    /// Unix-socket directory (`<data_path>/pgrun`).
    pub socket_dir: PathBuf,
    /// Application database name (defaults to `loom` in embedded mode).
    pub database: String,
    /// PG-binary paths; `None` when no cluster is booted in-process.
    pub bin: Option<PgBinPaths>,
}

impl EmbeddedSettings {
    /// Parse the embedded-PG settings from the env snapshot: `Some` only when
    /// `LOOM_PG_MODE=embedded`. `LOOM_PG_BIN_DIR` is never required here — it is
    /// captured into `bin` when present and left `None` otherwise. The database
    /// name uses the same embedded default as `DbConfig::from_map` so the two are
    /// consistent by construction.
    pub fn from_map(
        vars: &HashMap<String, String>,
        data_path: &Path,
    ) -> Result<Option<EmbeddedSettings>, ConfigError> {
        if vars.get("LOOM_PG_MODE").map(String::as_str) != Some("embedded") {
            return Ok(None);
        }
        let bin = vars.get("LOOM_PG_BIN_DIR").map(|dir| PgBinPaths {
            bin_dir: PathBuf::from(dir),
            ld_library_path: vars
                .get("LOOM_PG_LD_LIBRARY_PATH")
                .cloned()
                .unwrap_or_default(),
        });
        Ok(Some(EmbeddedSettings {
            data_dir: data_path.join("pgdata"),
            socket_dir: data_path.join("pgrun"),
            database: vars
                .get("LOOM_DB_NAME")
                .cloned()
                .unwrap_or_else(|| DEFAULT_EMBEDDED_DB_NAME.to_string()),
            bin,
        }))
    }
}
```

Add the shared default constant near the top of the file (after the imports/`use` block, above the structs):

```rust
/// Default application database name in embedded mode (used by both
/// `DbConfig::from_map` and `EmbeddedSettings::from_map` so `cfg.db` and the
/// embedded cluster's database agree by construction).
const DEFAULT_EMBEDDED_DB_NAME: &str = "loom";
```

- [ ] **Step 6: Add embedded defaulting to `DbConfig::from_map`**

Replace the body of `DbConfig::from_map` (currently lines ~92–115) with a mode-aware version. It keeps the `(vars)` signature and reads `LOOM_PG_MODE`/`LOOM_DATA_PATH` from `vars`:

```rust
    /// Parse the discrete `LOOM_DB_*` connection fields from the env snapshot.
    /// In embedded mode (`LOOM_PG_MODE=embedded`) the five vars default to values
    /// consistent with `EmbeddedPg::connect_options()` (socket under
    /// `<LOOM_DATA_PATH>/pgrun`, user `postgres`, trust auth, db `loom`); explicit
    /// vars still override. In external mode all five stay required.
    pub fn from_map(vars: &HashMap<String, String>) -> Result<DbConfig, ConfigError> {
        let max_connections = match vars.get("LOOM_DB_MAX_CONNECTIONS") {
            Some(s) => Some(
                s.parse::<u32>()
                    .map_err(|e| invalid("LOOM_DB_MAX_CONNECTIONS", e))?,
            ),
            None => None,
        };
        let embedded = vars.get("LOOM_PG_MODE").map(String::as_str) == Some("embedded");
        if embedded {
            // Socket dir matches EmbeddedSettings' `<data_path>/pgrun`. LOOM_DATA_PATH
            // is required by Config::from_map before this runs, so req_var is safe.
            let data_path = PathBuf::from(req_var(vars, "LOOM_DATA_PATH")?);
            let default_host = data_path.join("pgrun").display().to_string();
            return Ok(DbConfig {
                host: vars.get("LOOM_DB_HOST").cloned().unwrap_or(default_host),
                port: match vars.get("LOOM_DB_PORT") {
                    Some(s) => s.parse::<u16>().map_err(|e| invalid("LOOM_DB_PORT", e))?,
                    None => 5432,
                },
                user: vars
                    .get("LOOM_DB_USER")
                    .cloned()
                    .unwrap_or_else(|| "postgres".to_string()),
                password: vars.get("LOOM_DB_PASSWORD").cloned().unwrap_or_default(),
                dbname: vars
                    .get("LOOM_DB_NAME")
                    .cloned()
                    .unwrap_or_else(|| DEFAULT_EMBEDDED_DB_NAME.to_string()),
                max_connections,
            });
        }
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

> Ensure `invalid` and `req_var` are already imported (they are — via the `pub use loom_config::{...}` re-export at the top of the file). `PathBuf`/`Path` are already imported.

- [ ] **Step 7: Move the PG-bin requirement into `build_pool_managed`**

In `build_pool_managed`'s embedded branch (`Some(e) =>`, currently ~line 296), replace the `EmbeddedPg::start(e.cfg.clone())` line with an assembly that requires `bin`:

```rust
        Some(e) => {
            let bin = e.bin.as_ref().ok_or_else(|| {
                RuntimeError::Config(ConfigError::Invalid {
                    var: "LOOM_PG_BIN_DIR".to_string(),
                    detail: "required to boot the embedded Postgres cluster; client-only \
                             tools that merely connect do not need it"
                        .to_string(),
                })
            })?;
            let pg = managed_postgres::EmbeddedPg::start(managed_postgres::EmbeddedPgConfig {
                bin_dir: bin.bin_dir.clone(),
                ld_library_path: bin.ld_library_path.clone(),
                data_dir: e.data_dir.clone(),
                socket_dir: e.socket_dir.clone(),
                database: e.database.clone(),
            })
            .await
            .map_err(RuntimeError::Embedded)?;
            let mut opts = PgPoolOptions::new();
            if let Some(n) = cfg.db.max_connections {
                opts = opts.max_connections(n);
            }
            let pool = opts
                .connect_with(pg.connect_options())
                .await
                .map_err(RuntimeError::Pool)?;
            control_plane_postgres::run_embedded_migrations(&pool)
                .await
                .map_err(RuntimeError::Migrate)?;
            Ok((pool, Some(pg)))
        }
```

> `ConfigError` is in scope (re-exported at the top). Keep the `None =>` external branch unchanged.

- [ ] **Step 8: Run the tests to verify they pass**

Run: `buck2 test //src/services/runtime:embedded-config //src/services/runtime:build-pool-managed //src/services/runtime:config > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS. Then the existing bootstrap/migrate fixture tests (they set `LOOM_PG_BIN_DIR`, so `bin` is `Some` and behavior is unchanged):
Run: `buck2 test //src/services/runtime:bootstrap //src/services/runtime:migrate-managed > /tmp/t2b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2b.log`
Expected: PASS.

- [ ] **Step 9: Clippy + commit**

Run: `buck2 build '//src/services/runtime:runtime[clippy.txt]' > /tmp/t2c.log 2>&1; cat /tmp/t2c.log` (empty == clean).

```bash
git add src/services/runtime/src/lib.rs src/services/runtime/tests/embedded_config.rs src/services/runtime/tests/build_pool_managed.rs src/services/runtime/BUCK
git commit -m "feat(runtime): PG-bin optional at parse, required at spawn; embedded LOOM_DB_* defaults"
```

---

## Task 3: Acceptance-1 end-to-end — embedded client-only connects via defaults

**Files:**
- Test: `src/services/runtime/tests/embedded_client_only.rs` (create)
- Modify: `src/services/runtime/BUCK`

**Interfaces:**
- Consumes: `service_runtime::{Config, build_pool_managed, build_pool, control_plane, create_admin::run_create_admin}`; `managed_postgres::EmbeddedPg`; the fixture-injected `POSTGRES_BIN_DIR` / `POSTGRES_LD_LIBRARY_PATH` env; `control_plane_core::{Acl, Auth}` (for the sealed/has-user assertions).

- [ ] **Step 1: Write the failing end-to-end test**

Create `src/services/runtime/tests/embedded_client_only.rs`:

```rust
//! Acceptance 1 + 4 end-to-end: a client-only process (`loom create-admin`) in
//! embedded mode connects to an already-running cluster with NO LOOM_PG_BIN_DIR
//! and NO LOOM_DB_* vars — the defaults alone route it to the server's socket.
//! Server-role Config supplies LOOM_PG_BIN_DIR (from the fixture); the separate
//! client-role Config sets only mode + data path. Boots a real cluster, so it is
//! a `loom_fixture_test`.

use std::collections::HashMap;
use std::path::Path;

use control_plane_core::{Acl, Auth};

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
        client_cfg.embedded.as_ref().expect("embedded").bin.is_none(),
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
```

- [ ] **Step 2: Add the BUCK target**

In `src/services/runtime/BUCK`, after the `migrate-managed` target, add:

```python
# Acceptance 1: a client-only embedded Config (no PG bin, no LOOM_DB_*) connects
# to a booted cluster via defaults and runs create-admin. Boots a hermetic
# Postgres, so it must route local (loom_fixture_test).
loom_fixture_test(
    name = "embedded-client-only",
    crate = "embedded_client_only",
    srcs = ["tests/embedded_client_only.rs"],
    crate_root = "tests/embedded_client_only.rs",
    edition = "2024",
    deps = [
        ":runtime",
        "//src/control-plane/core:core",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run the test**

Run: `buck2 test //src/services/runtime:embedded-client-only > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/services/runtime/tests/embedded_client_only.rs src/services/runtime/BUCK
git commit -m "test(runtime): embedded client-only connects via defaults and seeds admin"
```

---

## Task 4: dev-up.sh hygiene + deploy.md host prerequisites

**Files:**
- Modify: `tools/dev-up.sh`
- Modify: `docs/deploy.md`

**Interfaces:** none (shell + docs). Behavior relies on Task 2's defaulting and Task 1's error.

- [ ] **Step 1: Shed the placeholder `LOOM_DB_*` block from `common_env`**

In `tools/dev-up.sh`, in the `common_env=(...)` array (lines ~66–75), delete the two `LOOM_DB_*` placeholder lines so the composite relies on embedded defaults. Change:

```bash
  LOOM_PG_MODE=embedded
  LOOM_DATA_PATH="$DATA_PATH"
  LOOM_DB_HOST="$DATA_PATH/pgrun" LOOM_DB_PORT=5432
  LOOM_DB_USER=postgres LOOM_DB_PASSWORD=postgres LOOM_DB_NAME=loom
  LOOM_WAREHOUSE_URI="file://$DATA_PATH/warehouse"
```

to:

```bash
  LOOM_PG_MODE=embedded
  LOOM_DATA_PATH="$DATA_PATH"
  # LOOM_DB_* are defaulted in embedded mode (host <data>/pgrun, user postgres,
  # trust auth, db loom) — see docs/deploy.md. No placeholders needed.
  LOOM_WAREHOUSE_URI="file://$DATA_PATH/warehouse"
```

- [ ] **Step 2: Drop the create-admin PG-var workaround**

In the `loom create-admin` invocation (lines ~136–146), remove the `LOOM_PG_BIN_DIR`/`LOOM_PG_LD_LIBRARY_PATH` overrides that existed only to satisfy config parsing, and update the comment. The client connects via the (now-defaulted) socket. Change the invocation block so it no longer passes those two vars:

```bash
# `create-admin` connects to the already-running embedded Postgres as a client via
# the defaulted embedded socket (it does not boot its own PG), so it needs no
# LOOM_PG_BIN_DIR/LOOM_PG_LD_LIBRARY_PATH.
if echo "$ADMIN_PASSWORD" | \
   env "${common_env[@]}" \
     "$LOOM_BIN" create-admin --username "$ADMIN_USER" --password-stdin; then
  :
else
  echo "dev-up: create-admin skipped (instance already sealed?)"
fi
```

> Preserve the exact surrounding control flow (the `if ... then ... else ... fi` and the success branch body) as it currently exists — only the `env` line's PG-var overrides and the comment change. The serve-path invocation (lines ~125–132) KEEPS `LOOM_PG_BIN_DIR`/`LOOM_PG_LD_LIBRARY_PATH` (it boots the cluster) and the libxml2 shim (`PGLD`) stays untouched.

- [ ] **Step 3: Verify the script still parses**

Run: `bash -n tools/dev-up.sh && echo "dev-up.sh OK"`
Expected: `dev-up.sh OK`.

- [ ] **Step 4: Add the Host prerequisites subsection to `docs/deploy.md`**

Under `## Single-binary `loom``, after the `### Embedded Postgres lifecycle` subsection (before `### First-admin bootstrap`), add:

```markdown
### Host prerequisites

Embedded mode runs loom's own bundled Postgres, but loom embeds **only its own
artifacts** — system libraries are the deployment environment's responsibility.
The bundled `postgres`/`initdb` dynamically link `libxml2.so.2`, so the host must
provide a system libxml2 exposing that soname (package `libxml2` on
Wolfi/Debian/Ubuntu/Fedora). If it is missing, `loom` fails fast at cluster start
with a named error —

```
embedded Postgres cannot start: missing shared library libxml2.so.2. loom bundles
only its own Postgres artifacts, not system libraries — install it on the host …
```

— rather than a cryptic loader failure. Some distros ship a newer soname (Arch
provides `libxml2.so.16`, not `.so.2`); `tools/dev-up.sh` carries a dev-only shim
that symlinks the newest system `libxml2.so.*` as `libxml2.so.2`, but that is a
local bridge, not a deployment posture.

**Image posture:** no standalone `loom` OCI image exists yet (the `deploy/`
images are the external-Postgres services, which do not need libxml2). When a
`deploy//images/loom` standalone image is added, its `apko.yaml` MUST include the
Wolfi `libxml2` package so the embedded cluster can boot.
```

> Match the surrounding heading depth (`###` under the `## Single-binary` section). End the file with exactly one trailing newline and no trailing whitespace (the `end-of-file-fixer`/`trim trailing whitespace` prek hooks police markdown too).

- [ ] **Step 5: Run prek to normalize the docs, then commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/t4.log 2>&1; grep -iE "failed|error" /tmp/t4.log || echo "prek clean"` (let it fix whitespace/EOF in place; stage the fixes).

```bash
git add tools/dev-up.sh docs/deploy.md
git commit -m "docs(deploy): host libxml2 prerequisite; dev-up sheds embedded-mode placeholders"
```

> The `## Known gaps` id removal (`#iss-embedded-config-requires-pg-bin-dir`, `#iss-embedded-pg-libxml2`) and the register-entry deletions happen at the **finish** step via `loom-docs-update`, not here.

---

## Self-Review

**1. Spec coverage:**
- Config-requirements matrix — PG-bin `not required` for client-only, `required at spawn site`: Task 2 (Steps 5–7). ✅
- `EmbeddedSettings` gains `bin: Option<PgBinPaths>`, never `req_var`'d: Task 2 Step 5. ✅
- `build_pool_managed` requires `bin`, assembles `EmbeddedPgConfig`, `ConfigError` naming `LOOM_PG_BIN_DIR`: Task 2 Step 7. ✅
- DB-var defaulting in embedded mode (folds `#fut-embedded-pg-db-vars-optional`), explicit override, external unchanged: Task 2 Step 6. ✅
- libxml2 fail-fast preflight + `MissingSharedLibrary` + pure classifier: Task 1. ✅
- deploy.md Host prerequisites + image posture: Task 4 Step 4. ✅
- dev-up.sh sheds placeholders + create-admin workaround, keeps libxml2 shim: Task 4 Steps 1–2. ✅
- Acceptance criteria 1–5: AC1 Task 3; AC2 Task 2 Step 2; AC3(a)(b) Task 1 Step 1; AC4 Task 2 Step 1 (`embedded_defaults_db_vars_from_data_path` + override + external-still-required); AC5 existing `composite-e2e`/`composite-error-path` (run in Step below) + Task 4. ✅

**2. Placeholder scan:** No `TBD`/`handle edge cases`/"write tests for the above" — every step carries concrete code. ✅

**3. Type consistency:** `PgBinPaths { bin_dir: PathBuf, ld_library_path: String }` used identically in Task 2 Steps 5/7 and the tests; `EmbeddedSettings { data_dir, socket_dir, database, bin }` field names match across struct def, `from_map`, `build_pool_managed`, and both test files; `classify_loader_error`/`MissingSharedLibrary { lib }` names match between Task 1 src and test. `DEFAULT_EMBEDDED_DB_NAME` shared by both `from_map`s. ✅

## Final verification (run after all tasks, before finishing)

Run the full touched-crate sweeps and the composite acceptance (AC5):

```bash
buck2 test //src/services/managed-postgres/... //src/services/runtime/... //src/services/standalone/... > /tmp/final.log 2>&1; grep -E "Tests finished|FAIL" /tmp/final.log
buck2 build '//src/services/managed-postgres:managed-postgres[clippy.txt]' '//src/services/runtime:runtime[clippy.txt]' > /tmp/final-clippy.log 2>&1; cat /tmp/final-clippy.log
```

Expected: all green; clippy empty. `composite-e2e`/`composite-error-path` (in the standalone sweep) confirm AC5 — the composite still boots with the embedded defaults.
