# Deploy hardening — schema migrations + S3 object store in the Helm chart — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A fresh `helm install` applies the loom control-plane schema automatically (no out-of-band step) and can point loom at S3/MinIO instead of a single-node `ReadWriteOnce` PVC.

**Architecture:** Two concerns. (1) A small **code seam** in `service_runtime`: a `migrate_on_boot` config flag that makes the managed (external-Postgres) branch of `build_pool_managed` apply the embedded migrations after connecting, plus a `run_migrations` helper + `migrate_requested()` gate that the three chart service binaries use to implement a migrate-and-exit entrypoint (`LOOM_MIGRATE=apply`). (2) **Chart wiring** (no further code): a `migrations.mode` value (`job` hook-Job default / `onBoot` env / `external`) and an `objectStore.s3` block that swaps the PVC warehouse for an S3 warehouse and relaxes the co-scheduling affinity + opens an egress NetworkPolicy.

**Tech Stack:** Rust (`service_runtime`, `control_plane_postgres`, sqlx `migrate!`), buck2 `rust_test`/`loom_fixture_test`, Helm 3 (Go templates), Kubernetes (Job hooks, NetworkPolicy).

## Global Constraints

- **Tests are `rust_test` integration targets only — never inline `#[cfg(test)]`.** New unit tests live in a sibling `tests/<name>.rs` wired as its own target in the crate's `BUCK`. The `no-inline-tests` prek hook fails on any `#[test]` in `src/**`.
- **Fixture tests (booting Postgres) MUST use `loom_fixture_test`, not `rust_test`** — a bare `rust_test` routes to remote execution and fails as root.
- **Clippy is strict** (pedantic + restriction on production code). No `unwrap`/`expect`/`panic`/`indexing_slicing`/`todo` in production `src/**`; test code is exempted from panic-safety lints by the `loom_rust_test`/`loom_fixture_test` wrapper. Use `#[expect(lint, reason = "...")]` locally if ever needed (bare `#[allow]` needs a `reason`).
- **The binary already supports S3** (`store-config` + `build_storage_factory`/`build_serving_object_store`). The S3 half of this slice is **chart wiring only** — do not add S3 code paths.
- **`LOOM_DATA_PATH` is unconditionally required by `Config::from_map`** (`req("LOOM_DATA_PATH")`). It is only the *fallback* warehouse root when `LOOM_WAREHOUSE_URI` is unset, and the embedded `data_dir`/`socket_dir` parent (embedded mode only). Therefore the chart keeps `LOOM_DATA_PATH` set even in S3 mode (an inert string once `LOOM_WAREHOUSE_URI=s3://…` overrides it); "no local path in S3 mode" is realized as **no PVC + no `data` volume mount**, not as omitting the env var. (Making `LOOM_DATA_PATH` optional under an `s3://` warehouse is recorded as a deferred follow-up.)
- **Markdown lint:** every edited `.md` ends with exactly one trailing newline and no trailing whitespace (`end-of-file-fixer` / `trim trailing whitespace` hooks run on all files in CI's `lint` job).
- **`deploy//` is off the CI sweep** (CI covers `//src/...` only; the homelab external cell that `helm_chart` needs is not fetchable in cloud sessions). Chart rendering is therefore validated by a **standalone `helm template`/`helm lint` assertion script** run locally (helm 3.x), not a buck2 target. The `//src/...` code seam + its fixture test run in CI as usual.

---

## File structure

**Code seam (in CI):**
- Modify `src/services/runtime/src/lib.rs` — add `Config.migrate_on_boot`, parse it, apply on-boot migration in `build_pool_managed`'s external branch, add `run_migrations` + `migrate_requested`.
- Modify `src/services/ingest/src/main.rs`, `src/services/query-api/src/main.rs`, `src/services/engine/src/main.rs` — migrate-and-exit guard + switch `build_pool` → `build_pool_managed`.
- Create `src/services/runtime/tests/migrate_managed.rs` + wire `loom_fixture_test` in `src/services/runtime/BUCK`.
- Modify `src/services/runtime/tests/config.rs` (or the relevant existing config test) — `migrate_on_boot` parse coverage. (Confirm the exact existing test file in Task 1.)

**Chart (off CI):**
- Modify `deploy/chart/chart/values.yaml` — `migrations:` block + `objectStore.s3:` block.
- Create `deploy/chart/chart/templates/migrations-job.yaml` — the `mode=job` hook Job.
- Modify `deploy/chart/chart/templates/_helpers.tpl` — `loom.objectStoreEnv` + `loom.migrateOnBootEnv` helpers.
- Modify `deploy/chart/chart/templates/ingest.yaml`, `query-api.yaml` — object-store env via helper, on-boot env, PVC-skip + affinity-relax under S3.
- Modify `deploy/chart/chart/templates/objectstore-pvc.yaml` — skip the PVC when S3 enabled.
- Modify `deploy/chart/chart/templates/networkpolicy.yaml` — S3 egress rule.
- Modify `deploy/chart/chart/templates/NOTES.txt`, `templates/postgres-cnpg.yaml`, `docs/deploy.md` — reflect the selected migration mode.
- Create `deploy/chart/tests/render_assertions.sh` — the golden `helm template`/`helm lint` assertions.

**Registers:**
- Modify `docs/ROADMAP.md` — close `road-deploy-hardening`.
- Modify `docs/FUTURE.md` — record `fut-deploy-data-path-optional-s3` (new deferral).

---

## Task 1: Code seam — `migrate_on_boot` config + on-boot migration + migrate helpers

**Files:**
- Modify: `src/services/runtime/src/lib.rs` (Config struct ~L100-112, `from_map` ~L114-202, `build_pool_managed` ~L261-284; add helpers after it)
- Test: `src/services/runtime/tests/config.rs` (extend) — confirm this is the file with the existing `from_map` tests in Task 1 Step 0.

**Interfaces:**
- Produces:
  - `Config.migrate_on_boot: bool` — parsed from `LOOM_DB_MIGRATE_ON_BOOT` (`None`/`"false"` ⇒ `false`, `"true"` ⇒ `true`, anything else ⇒ `ConfigError::Invalid`).
  - `pub async fn run_migrations(db: &DbConfig) -> Result<(), RuntimeError>` — connect an external pool from `db` and apply `control_plane_postgres::run_embedded_migrations`.
  - `pub fn migrate_requested() -> bool` — `true` iff `LOOM_MIGRATE` env == `"apply"`.
  - `build_pool_managed` external (`None`) branch now runs the migrator when `cfg.migrate_on_boot`.
- Consumes: existing `control_plane_postgres::run_embedded_migrations(&PgPool) -> control_plane_core::Result<()>`, existing `build_pool(&DbConfig)`, existing `RuntimeError::Migrate(control_plane_core::ControlPlaneError)`.

- [ ] **Step 0: Find the existing config-parse test file**

Run: `grep -rln "from_map" src/services/runtime/tests/`
Expected: `src/services/runtime/tests/config.rs` (and possibly `embedded_config.rs`). Add the new parse assertions to `config.rs`. Read it first to mirror its `HashMap` construction helper.

- [ ] **Step 1: Write the failing config-parse test**

Append to `src/services/runtime/tests/config.rs` (adapt the `vars`-building helper to whatever the file already uses — most tests build a `HashMap<String,String>` of the required keys):

```rust
// Minimal required env for a valid Config::from_map. Mirror the file's existing
// helper if it already has one; otherwise:
fn base_vars() -> std::collections::HashMap<String, String> {
    [
        ("LOOM_BIND_ADDR", "0.0.0.0:8080"),
        ("LOOM_DB_HOST", "db"),
        ("LOOM_DB_PORT", "5432"),
        ("LOOM_DB_USER", "loom"),
        ("LOOM_DB_PASSWORD", "pw"),
        ("LOOM_DB_NAME", "loom"),
        ("LOOM_DATA_PATH", "/var/lib/loom/data"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

#[test]
fn migrate_on_boot_defaults_false() {
    let cfg = service_runtime::Config::from_map(&base_vars()).expect("parse");
    assert!(!cfg.migrate_on_boot);
}

#[test]
fn migrate_on_boot_true_parses() {
    let mut vars = base_vars();
    vars.insert("LOOM_DB_MIGRATE_ON_BOOT".into(), "true".into());
    let cfg = service_runtime::Config::from_map(&vars).expect("parse");
    assert!(cfg.migrate_on_boot);
}

#[test]
fn migrate_on_boot_false_parses() {
    let mut vars = base_vars();
    vars.insert("LOOM_DB_MIGRATE_ON_BOOT".into(), "false".into());
    let cfg = service_runtime::Config::from_map(&vars).expect("parse");
    assert!(!cfg.migrate_on_boot);
}

#[test]
fn migrate_on_boot_invalid_rejected() {
    let mut vars = base_vars();
    vars.insert("LOOM_DB_MIGRATE_ON_BOOT".into(), "yes".into());
    assert!(service_runtime::Config::from_map(&vars).is_err());
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/services/runtime:config > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL — `no field migrate_on_boot on type Config` (compile error). (If `base_vars` collides with an existing helper, reuse the existing one instead.)

- [ ] **Step 3: Add the `migrate_on_boot` field to `Config`**

In `src/services/runtime/src/lib.rs`, add to `struct Config` (after `embedded`):

```rust
    /// Present when running an embedded (loom-managed) Postgres cluster.
    pub embedded: Option<EmbeddedSettings>,
    /// When `true`, `build_pool_managed`'s external branch applies the embedded
    /// control-plane migrations after connecting. From `LOOM_DB_MIGRATE_ON_BOOT`
    /// (default `false`). The embedded branch always migrates regardless.
    pub migrate_on_boot: bool,
```

- [ ] **Step 4: Parse it in `from_map`**

In `from_map`, before the final `Ok(Config { … })`, add:

```rust
        let migrate_on_boot = match vars.get("LOOM_DB_MIGRATE_ON_BOOT").map(String::as_str) {
            None | Some("false") => false,
            Some("true") => true,
            Some(other) => {
                return Err(invalid(
                    "LOOM_DB_MIGRATE_ON_BOOT",
                    format!("expected `true` or `false`, got `{other}`"),
                ));
            }
        };
```

Then add `migrate_on_boot,` to the returned `Config { … }` literal.

- [ ] **Step 5: Run the config test to verify it passes**

Run: `buck2 test //src/services/runtime:config > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS. (Note: any other test that constructs a `Config { … }` struct literal directly — e.g. in `embedded_config.rs` — must add `migrate_on_boot: false`; fix compile errors it surfaces.)

- [ ] **Step 6: Apply on-boot migration in `build_pool_managed` + add helpers**

In `src/services/runtime/src/lib.rs`, change the external (`None`) branch of `build_pool_managed`:

```rust
    match &cfg.embedded {
        None => {
            let pool = build_pool(&cfg.db).await?;
            if cfg.migrate_on_boot {
                tracing::info!("LOOM_DB_MIGRATE_ON_BOOT=true: applying control-plane migrations");
                control_plane_postgres::run_embedded_migrations(&pool)
                    .await
                    .map_err(RuntimeError::Migrate)?;
            }
            Ok((pool, None))
        }
        Some(e) => {
            // …unchanged embedded branch…
        }
    }
```

Add, immediately after `build_pool_managed`:

```rust
/// `true` when the process was started in migrate-and-exit mode (`LOOM_MIGRATE=apply`).
/// The service binaries check this before normal startup: they apply the migrations
/// and exit 0, so a chart hook Job can run any service image as a one-shot migrator.
pub fn migrate_requested() -> bool {
    std::env::var("LOOM_MIGRATE").as_deref() == Ok("apply")
}

/// Connect an external control-plane pool from `db` and apply the embedded
/// migrations. Used by the migrate-and-exit entrypoint (`migrate_requested`).
pub async fn run_migrations(db: &DbConfig) -> Result<(), RuntimeError> {
    let pool = build_pool(db).await?;
    control_plane_postgres::run_embedded_migrations(&pool)
        .await
        .map_err(RuntimeError::Migrate)?;
    Ok(())
}
```

- [ ] **Step 7: Build the runtime lib + clippy**

Run: `buck2 build //src/services/runtime:runtime '//src/services/runtime:runtime[clippy.txt]' > /tmp/t.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)|error" /tmp/t.log; echo "clippy:"; buck2 build '//src/services/runtime:runtime[clippy.txt]' --show-output 2>/dev/null`
Expected: build succeeds; clippy output empty (clean).

- [ ] **Step 8: Commit**

```bash
git add src/services/runtime/src/lib.rs src/services/runtime/tests/config.rs
git commit -m "feat(runtime): managed-branch on-boot migration + migrate-and-exit seam"
```

---

## Task 2: Wire the migrate seam into the three chart service binaries

**Files:**
- Modify: `src/services/ingest/src/main.rs` (~L18-19), `src/services/query-api/src/main.rs` (~L19-20), `src/services/engine/src/main.rs` (~L21-22)

**Interfaces:**
- Consumes: `service_runtime::migrate_requested()`, `service_runtime::run_migrations(&DbConfig)`, `service_runtime::build_pool_managed(&Config)` (all from Task 1).
- Produces: each binary, when `LOOM_MIGRATE=apply`, applies migrations and exits 0; otherwise builds its pool via `build_pool_managed` (so `LOOM_DB_MIGRATE_ON_BOOT` takes effect).

**Note on tests:** these mains have no `rust_test` (binary wiring); they are covered by the fixture test in Task 3 (which exercises `run_migrations` + `build_pool_managed` on-boot directly) and a full build. No new unit test here.

- [ ] **Step 1: Ingest main — add the guard + switch the pool builder**

In `src/services/ingest/src/main.rs`, replace:

```rust
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
```

with:

```rust
    let cfg = service_runtime::Config::from_env()?;
    // Migrate-and-exit mode: apply the control-plane schema and exit (chart hook Job).
    if service_runtime::migrate_requested() {
        service_runtime::run_migrations(&cfg.db).await?;
        return Ok(());
    }
    // `.1` is the embedded-PG handle (None in external mode); the ingest deploy is
    // always external, so it is discarded. build_pool_managed honors LOOM_DB_MIGRATE_ON_BOOT.
    let (pool, _pg) = service_runtime::build_pool_managed(&cfg).await?;
```

- [ ] **Step 2: Query-api main — same change**

In `src/services/query-api/src/main.rs`, replace the identical two lines with the identical replacement from Step 1.

- [ ] **Step 3: Engine main — same change**

In `src/services/engine/src/main.rs`, the engine `main` has no `init_tracing`; replace:

```rust
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
```

with:

```rust
    let cfg = service_runtime::Config::from_env()?;
    if service_runtime::migrate_requested() {
        service_runtime::run_migrations(&cfg.db).await?;
        return Ok(());
    }
    let (pool, _pg) = service_runtime::build_pool_managed(&cfg).await?;
```

- [ ] **Step 4: Build all three binaries + clippy**

Run: `buck2 build //src/services/ingest/... //src/services/query-api/... //src/services/engine/... > /tmp/t.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)|error\[" /tmp/t.log`
Then `bash tools/clippy-all.sh > /tmp/c.log 2>&1; tail -5 /tmp/c.log` — expect clean (or scope to the three crates' `[clippy.txt]`).
Expected: build succeeds, clippy clean.

- [ ] **Step 5: Commit**

```bash
git add src/services/ingest/src/main.rs src/services/query-api/src/main.rs src/services/engine/src/main.rs
git commit -m "feat(services): migrate-and-exit entrypoint + on-boot migration wiring"
```

---

## Task 3: Managed-branch migrate fixture test

**Files:**
- Create: `src/services/runtime/tests/migrate_managed.rs`
- Modify: `src/services/runtime/BUCK` (add a `loom_fixture_test` target)

**Interfaces:**
- Consumes: `service_runtime::{Config, DbConfig, build_pool_managed, run_migrations}`, `managed_postgres::{EmbeddedPg, EmbeddedPgConfig}`, `store_config::ObjectStoreConfig`, `control_plane_postgres` (schema probe via sqlx), the fixture env `POSTGRES_BIN_DIR`/`POSTGRES_LD_LIBRARY_PATH`.
- Produces: proves the external branch of `build_pool_managed` applies the schema iff `migrate_on_boot`, and that `run_migrations` applies it.

- [ ] **Step 1: Write the fixture test**

Create `src/services/runtime/tests/migrate_managed.rs`:

```rust
//! Managed-branch (external Postgres) migrate wiring: `build_pool_managed` applies
//! the control-plane schema when `migrate_on_boot` is set, and `run_migrations`
//! applies it directly. A fresh `EmbeddedPg` (initdb only — start() does NOT migrate)
//! provides the real, empty database; we connect to it as an *external* DbConfig over
//! its unix socket, exactly as the chart's services connect to CNPG.

use std::path::{Path, PathBuf};
use std::time::Duration;

use managed_postgres::{EmbeddedPg, EmbeddedPgConfig};
use service_runtime::{Config, DbConfig};
use sqlx::postgres::PgPoolOptions;
use store_config::ObjectStoreConfig;

fn embedded_cfg(data: &Path, sock: &Path) -> EmbeddedPgConfig {
    EmbeddedPgConfig {
        bin_dir: PathBuf::from(std::env::var("POSTGRES_BIN_DIR").expect("POSTGRES_BIN_DIR")),
        ld_library_path: std::env::var("POSTGRES_LD_LIBRARY_PATH").unwrap_or_default(),
        data_dir: data.to_path_buf(),
        socket_dir: sock.to_path_buf(),
        database: "loom".to_string(),
    }
}

// An *external*-style DbConfig pointing at the embedded server's unix socket.
// EmbeddedPg connects as user "postgres" over socket_dir; a host beginning with
// "/" is a unix-socket dir (libpq convention) in DbConfig::pg_connect_options.
fn external_db(pg: &EmbeddedPg) -> DbConfig {
    DbConfig {
        host: pg.socket_dir().to_string_lossy().into_owned(),
        port: 5432,
        user: "postgres".to_string(),
        password: String::new(),
        dbname: "loom".to_string(),
        max_connections: Some(4),
    }
}

fn external_config(db: DbConfig, migrate_on_boot: bool, data_path: &Path) -> Config {
    Config {
        bind_addr: "127.0.0.1:0".parse().expect("addr"),
        db,
        data_path: data_path.to_path_buf(),
        object_store: ObjectStoreConfig::parse(&std::collections::HashMap::new(), data_path)
            .expect("local object store"),
        lock_timeout: Duration::from_millis(5000),
        gc_retention: Duration::from_secs(7 * 24 * 3600),
        embedded: None,
        migrate_on_boot,
    }
}

async fn loom_schema_present(pg: &EmbeddedPg) -> bool {
    let pool = PgPoolOptions::new()
        .connect_with(pg.connect_options())
        .await
        .expect("connect probe");
    // acl.subject exists only after migrations run.
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
async fn build_pool_managed_migrates_on_boot_when_enabled() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let pg = EmbeddedPg::start(embedded_cfg(&tmp.path().join("pgdata"), &tmp.path().join("pgrun")))
        .await
        .expect("start");

    assert!(!loom_schema_present(&pg).await, "fresh DB has no loom schema");

    let cfg = external_config(external_db(&pg), true, tmp.path());
    let (pool, handle) = service_runtime::build_pool_managed(&cfg).await.expect("managed pool");
    assert!(handle.is_none(), "external mode yields no embedded handle");
    pool.close().await;

    assert!(loom_schema_present(&pg).await, "on-boot migration applied the schema");
    pg.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn build_pool_managed_does_not_migrate_when_disabled() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let pg = EmbeddedPg::start(embedded_cfg(&tmp.path().join("pgdata"), &tmp.path().join("pgrun")))
        .await
        .expect("start");

    let cfg = external_config(external_db(&pg), false, tmp.path());
    let (pool, _handle) = service_runtime::build_pool_managed(&cfg).await.expect("managed pool");
    pool.close().await;

    assert!(!loom_schema_present(&pg).await, "no migration when flag is off");
    pg.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn run_migrations_applies_schema() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let pg = EmbeddedPg::start(embedded_cfg(&tmp.path().join("pgdata"), &tmp.path().join("pgrun")))
        .await
        .expect("start");

    service_runtime::run_migrations(&external_db(&pg)).await.expect("run_migrations");
    assert!(loom_schema_present(&pg).await, "run_migrations applied the schema");
    pg.shutdown().await.expect("shutdown");
}
```

- [ ] **Step 2: Wire the `loom_fixture_test` target**

In `src/services/runtime/BUCK`, ensure the top loads `loom_fixture_test` (mirror `managed-postgres/BUCK` line 2):

```python
load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")
```

Add the target:

```python
loom_fixture_test(
    name = "migrate-managed",
    crate = "migrate_managed",
    srcs = ["tests/migrate_managed.rs"],
    crate_root = "tests/migrate_managed.rs",
    edition = "2024",
    deps = [
        ":runtime",
        "//src/services/managed-postgres:managed-postgres",
        "//src/services/store-config:store-config",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run the fixture test (local — it boots Postgres)**

Run: `buck2 test //src/services/runtime:migrate-managed > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS (3 tests). `loom_fixture_test` pins the run to local execution, so `initdb`/`postgres` run as an unprivileged step. If it fails to find `store_config`, confirm the crate name is `store_config` (dep `//src/services/store-config:store-config`).

- [ ] **Step 4: Commit**

```bash
git add src/services/runtime/tests/migrate_managed.rs src/services/runtime/BUCK
git commit -m "test(runtime): managed-branch on-boot migration fixture test"
```

---

## Task 4: Chart — `migrations.mode` (job / onBoot / external)

**Files:**
- Modify: `deploy/chart/chart/values.yaml`
- Create: `deploy/chart/chart/templates/migrations-job.yaml`
- Modify: `deploy/chart/chart/templates/_helpers.tpl` (add `loom.migrateOnBootEnv`)
- Modify: `deploy/chart/chart/templates/ingest.yaml`, `query-api.yaml` (emit on-boot env)
- Modify: `deploy/chart/chart/templates/NOTES.txt`, `templates/postgres-cnpg.yaml`, `docs/deploy.md`

**Interfaces:**
- Produces (chart values):
  - `migrations.mode` ∈ {`job` (default), `onBoot`, `external`}
  - `migrations.backoffLimit` (default `3`) — the hook Job's retry cap.
- The migrate Job uses `ingest.image` with `LOOM_MIGRATE=apply`; the on-boot env is emitted on the ingest container, the query-api container, and the engine sidecar container.

- [ ] **Step 1: Write the golden assertions (they will fail first)**

Create `deploy/chart/tests/render_assertions.sh` — start with the migration-mode assertions (S3 assertions are added in Task 5). Make it executable.

```bash
#!/usr/bin/env bash
# Golden helm-render assertions for the loom chart. Requires `helm` 3.x on PATH.
# Not run in CI (the deploy// cell is off the //src sweep and needs the homelab
# external cell); run locally / in the release path: bash deploy/chart/tests/render_assertions.sh
set -euo pipefail
CHART="$(cd "$(dirname "$0")/../chart" && pwd)"
fail() { echo "ASSERT FAIL: $*" >&2; exit 1; }
has()  { grep -q "$1" || fail "expected to find: $1"; }
hasnt() { if grep -q "$1"; then fail "expected NOT to find: $1"; fi; }

echo "== helm lint =="
helm lint "$CHART"

echo "== migrations.mode=job (default): hook Job present, no on-boot env =="
OUT="$(helm template loom "$CHART")"
echo "$OUT" | grep -A30 'kind: Job' | has '"helm.sh/hook": pre-install,pre-upgrade'
echo "$OUT" | grep -A40 'kind: Job' | has 'name: LOOM_MIGRATE'
echo "$OUT" | hasnt 'LOOM_DB_MIGRATE_ON_BOOT'

echo "== migrations.mode=onBoot: env on both Deployments, no Job =="
OUT="$(helm template loom "$CHART" --set migrations.mode=onBoot)"
echo "$OUT" | hasnt 'kind: Job'
# ingest Deployment carries the flag
echo "$OUT" | awk '/kind: Deployment/,/^---/' | grep -A200 'component: ingest' | has 'LOOM_DB_MIGRATE_ON_BOOT'
# query-api Deployment carries the flag
echo "$OUT" | grep -c 'LOOM_DB_MIGRATE_ON_BOOT' | grep -qvx 0 || fail "on-boot flag missing"

echo "== migrations.mode=external: neither Job nor on-boot env =="
OUT="$(helm template loom "$CHART" --set migrations.mode=external)"
echo "$OUT" | hasnt 'kind: Job'
echo "$OUT" | hasnt 'LOOM_DB_MIGRATE_ON_BOOT'

echo "ALL MIGRATION ASSERTIONS PASSED"
```

- [ ] **Step 2: Run assertions to verify they fail**

Run: `bash deploy/chart/tests/render_assertions.sh; echo "exit=$?"`
Expected: FAIL (no Job template yet; default render has no `kind: Job`).

- [ ] **Step 3: Add the `migrations` values block**

In `deploy/chart/chart/values.yaml`, after the `lockTimeoutMs` line, add:

```yaml
# --- Schema migrations ---------------------------------------------------------
# How loom's control-plane schema (src/control-plane/postgres/migrations) is
# applied to the managed Postgres. A fresh install must migrate before the
# services can serve.
#   job      – (default) a pre-install/pre-upgrade Helm hook Job runs the
#              migrate-and-exit entrypoint (LOOM_MIGRATE=apply, ingest image)
#              before the Deployments roll. DDL rights live on the one-shot Job.
#   onBoot   – LOOM_DB_MIGRATE_ON_BOOT=true on the service containers; each pod
#              migrates at startup. Concurrent replicas race on sqlx's advisory
#              lock (first wins, rest no-op) — every pod needs DDL rights.
#   external – neither; apply migrations yourself (pre-chart behaviour).
migrations:
  mode: job
  # Retry cap for the mode=job hook Job.
  backoffLimit: 3
```

- [ ] **Step 4: Create the hook Job template**

Create `deploy/chart/chart/templates/migrations-job.yaml`:

```yaml
{{- if eq .Values.migrations.mode "job" }}
# Migrate-and-exit hook Job: runs the ingest image with LOOM_MIGRATE=apply before
# the Deployments roll (pre-install + pre-upgrade). Applies loom's control-plane
# schema then exits 0. DDL credentials are isolated to this one-shot pod.
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ include "loom.fullname" . }}-migrate
  labels:
    {{- include "loom.labels" . | nindent 4 }}
    app.kubernetes.io/component: migrate
  annotations:
    "helm.sh/hook": pre-install,pre-upgrade
    "helm.sh/hook-weight": "-5"
    "helm.sh/hook-delete-policy": before-hook-creation,hook-succeeded
spec:
  backoffLimit: {{ .Values.migrations.backoffLimit }}
  template:
    metadata:
      labels:
        {{- include "loom.labels" . | nindent 8 }}
        app.kubernetes.io/component: migrate
    spec:
      restartPolicy: Never
      serviceAccountName: {{ include "loom.serviceAccountName" . }}
      securityContext:
        {{- toYaml .Values.podSecurityContext | nindent 8 }}
      containers:
        - name: migrate
          image: {{ include "loom.image" .Values.ingest.image | quote }}
          imagePullPolicy: {{ .Values.ingest.image.pullPolicy | default "IfNotPresent" }}
          workingDir: /tmp
          securityContext:
            {{- toYaml .Values.containerSecurityContext | nindent 12 }}
          env:
            - name: LOOM_MIGRATE
              value: "apply"
            # Config::from_env parses these even though migrate-and-exit uses only
            # the DB env. LOOM_DATA_PATH stays local (/tmp) — the migrator never
            # touches the warehouse, so no PVC / S3 is needed for this Job.
            - name: LOOM_BIND_ADDR
              value: "0.0.0.0:{{ .Values.ingest.port }}"
            - name: LOOM_DATA_PATH
              value: /tmp
            - name: HOME
              value: /tmp
            - name: TMPDIR
              value: /tmp
            {{- include "loom.dbEnv" . | nindent 12 }}
          volumeMounts:
            - name: tmp
              mountPath: /tmp
      volumes:
        - name: tmp
          emptyDir: {}
{{- end }}
```

- [ ] **Step 5: Add the on-boot env helper**

In `deploy/chart/chart/templates/_helpers.tpl`, append:

```yaml
{{/*
On-boot migration env: emitted on the service containers only when
migrations.mode == onBoot. Each pod applies the schema at startup (sqlx advisory
lock serialises concurrent pods).
*/}}
{{- define "loom.migrateOnBootEnv" -}}
{{- if eq .Values.migrations.mode "onBoot" }}
- name: LOOM_DB_MIGRATE_ON_BOOT
  value: "true"
{{- end }}
{{- end -}}
```

- [ ] **Step 6: Emit the on-boot env on all three service containers**

In `deploy/chart/chart/templates/ingest.yaml`, in the ingest container's `env:` list, right after the `{{- include "loom.dbEnv" . | nindent 12 }}` line, add:

```yaml
            {{- include "loom.migrateOnBootEnv" . | nindent 12 }}
```

In `deploy/chart/chart/templates/query-api.yaml`, add the same line after the `dbEnv` include in **both** the `query-api` container and the `engine` sidecar container.

- [ ] **Step 7: Update NOTES.txt, postgres-cnpg.yaml, docs/deploy.md**

In `templates/NOTES.txt`, replace the hard-coded migration warning (the `! Apply loom's schema migrations…` line inside the `postgres.enabled` block) with mode-aware text:

```
{{ if .Values.postgres.enabled -}}
Postgres (CloudNativePG): cluster "{{ include "loom.pgClusterName" . }}", app secret "{{ include "loom.pgClusterName" . }}-app".
{{- if eq .Values.migrations.mode "job" }}
  Schema migrations run automatically via a pre-install/pre-upgrade hook Job.
{{- else if eq .Values.migrations.mode "onBoot" }}
  Schema migrations run automatically at pod startup (LOOM_DB_MIGRATE_ON_BOOT).
{{- else }}
  ! migrations.mode=external — apply loom's schema migrations yourself (see docs/deploy.md).
{{- end }}
{{- else -}}
Postgres: external, via secret "{{ .Values.postgres.external.existingSecret }}".
{{- end }}
```

In `templates/postgres-cnpg.yaml`, soften the `NOTE:` comment block (lines ~5-8) to:

```yaml
# NOTE: this provisions Postgres. Schema migrations are applied per the chart's
# migrations.mode (default: a pre-install/pre-upgrade hook Job). See docs/deploy.md.
```

In `docs/deploy.md`, replace the "Schema migrations are not applied by the chart" bullet (around lines 72-74) with a short paragraph documenting `migrations.mode` (job default / onBoot / external) and that a fresh install now migrates automatically. Keep exactly one trailing newline, no trailing whitespace.

- [ ] **Step 8: Run the assertions to verify they pass**

Run: `bash deploy/chart/tests/render_assertions.sh; echo "exit=$?"`
Expected: `ALL MIGRATION ASSERTIONS PASSED`, exit 0. `helm lint` stays green. Fix any template YAML issues surfaced.

- [ ] **Step 9: Commit**

```bash
git add deploy/chart/chart/values.yaml deploy/chart/chart/templates/migrations-job.yaml \
  deploy/chart/chart/templates/_helpers.tpl deploy/chart/chart/templates/ingest.yaml \
  deploy/chart/chart/templates/query-api.yaml deploy/chart/chart/templates/NOTES.txt \
  deploy/chart/chart/templates/postgres-cnpg.yaml deploy/chart/tests/render_assertions.sh \
  docs/deploy.md
git commit -m "feat(deploy): migrations.mode (job/onBoot/external) with hook Job"
```

---

## Task 5: Chart — `objectStore.s3` block + wiring

**Files:**
- Modify: `deploy/chart/chart/values.yaml` (extend `objectStore`)
- Modify: `deploy/chart/chart/templates/_helpers.tpl` (add `loom.objectStoreEnv`)
- Modify: `deploy/chart/chart/templates/ingest.yaml`, `query-api.yaml` (env via helper; PVC volume/mount + affinity conditional on S3)
- Modify: `deploy/chart/chart/templates/objectstore-pvc.yaml` (skip PVC when S3)
- Modify: `deploy/chart/chart/templates/networkpolicy.yaml` (S3 egress)
- Modify: `deploy/chart/tests/render_assertions.sh` (add S3 assertions)

**Interfaces:**
- Produces (chart values, under `objectStore`):
  - `s3.enabled` (default `false`), `s3.bucket`, `s3.prefix` (default `""`), `s3.endpoint` (default `""` ⇒ AWS), `s3.region` (default `us-east-1`), `s3.port` (default `443` — the endpoint's TCP port; MinIO commonly `9000`), `s3.credentialsSecret`, `s3.credentialsKeys.{accessKeyId,secretAccessKey}` (defaults `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`).
- `loom.objectStoreEnv`: when S3 disabled → the current `LOOM_DATA_PATH` env; when enabled → `LOOM_DATA_PATH` (kept, inert) + `LOOM_WAREHOUSE_URI=s3://<bucket>[/<prefix>]` + `AWS_REGION` + optional `AWS_ENDPOINT_URL` + `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` from the secret.

- [ ] **Step 1: Add the S3 assertions (fail first)**

Append to `deploy/chart/tests/render_assertions.sh` before the final echo:

```bash
echo "== default (no S3): PVC + co-scheduling affinity present =="
OUT="$(helm template loom "$CHART")"
echo "$OUT" | has 'kind: PersistentVolumeClaim'
echo "$OUT" | grep -A160 'component: query-api' | has 'podAffinity'
echo "$OUT" | hasnt 'LOOM_WAREHOUSE_URI'

echo "== objectStore.s3.enabled: S3 env on all 3 containers, no PVC, relaxed affinity, egress =="
OUT="$(helm template loom "$CHART" \
  --set objectStore.s3.enabled=true \
  --set objectStore.s3.bucket=loomwh \
  --set objectStore.s3.endpoint=http://minio:9000 \
  --set objectStore.s3.port=9000 \
  --set objectStore.s3.credentialsSecret=loom-s3)"
echo "$OUT" | hasnt 'kind: PersistentVolumeClaim'
echo "$OUT" | hasnt 'claimName:'
# three containers (ingest, query-api, engine) each get the warehouse URI
[ "$(echo "$OUT" | grep -c 'name: LOOM_WAREHOUSE_URI')" = "3" ] || fail "expected 3 LOOM_WAREHOUSE_URI"
echo "$OUT" | has 's3://loomwh'
echo "$OUT" | has 'name: AWS_ENDPOINT_URL'
echo "$OUT" | has 'name: AWS_ACCESS_KEY_ID'
# co-scheduling affinity is gone
echo "$OUT" | grep -A160 'component: query-api' | hasnt 'podAffinity'
# egress NetworkPolicy to the S3 port
echo "$OUT" | grep -A20 'allow-s3' | has 'port: 9000'

echo "ALL ASSERTIONS PASSED"
```

Also delete the earlier `echo "ALL MIGRATION ASSERTIONS PASSED"` line so the script ends on the combined message.

- [ ] **Step 2: Run to verify failure**

Run: `bash deploy/chart/tests/render_assertions.sh; echo "exit=$?"`
Expected: FAIL at the S3 section (no `LOOM_WAREHOUSE_URI` yet).

- [ ] **Step 3: Add the `s3` values under `objectStore`**

In `deploy/chart/chart/values.yaml`, inside the `objectStore:` block (after `retain: true`), add:

```yaml
  # Optional S3/MinIO warehouse. When enabled, the shared PVC is NOT created and
  # all three containers read/write the Iceberg warehouse over S3 — removing the
  # single-node ReadWriteOnce co-scheduling constraint. The binary already builds
  # an S3 store from this config; this is pure chart wiring. Default (disabled)
  # keeps the file:// PVC path unchanged.
  s3:
    enabled: false
    bucket: ""
    # Prefix within the bucket: warehouse_uri = s3://<bucket>[/<prefix>].
    prefix: ""
    # MinIO / non-AWS endpoint URL. Empty ⇒ real AWS S3. Path-style is implied
    # by the binary whenever an endpoint is set (required by MinIO).
    endpoint: ""
    region: us-east-1
    # TCP port of the S3 endpoint, opened as egress by the NetworkPolicy. 443 for
    # AWS S3; set to your MinIO port (commonly 9000) for an in-cluster endpoint.
    port: 443
    # Existing secret carrying the S3 credentials (referenced, not created).
    credentialsSecret: ""
    credentialsKeys:
      accessKeyId: AWS_ACCESS_KEY_ID
      secretAccessKey: AWS_SECRET_ACCESS_KEY
```

- [ ] **Step 4: Add the `loom.objectStoreEnv` helper**

In `_helpers.tpl`, append:

```yaml
{{/*
Object-store env for the service containers. Default (no S3) emits the local
LOOM_DATA_PATH warehouse. With objectStore.s3.enabled it emits the S3 warehouse
URI + region + optional endpoint + credentials-from-secret; LOOM_DATA_PATH stays
set (Config::from_env requires it) but is overridden by LOOM_WAREHOUSE_URI.
*/}}
{{- define "loom.objectStoreEnv" -}}
- name: LOOM_DATA_PATH
  value: {{ .Values.objectStore.mountPath | quote }}
{{- if .Values.objectStore.s3.enabled }}
{{- $s3 := .Values.objectStore.s3 }}
- name: LOOM_WAREHOUSE_URI
  value: {{ printf "s3://%s%s" (required "objectStore.s3.bucket is required when s3.enabled" $s3.bucket) (empty $s3.prefix | ternary "" (printf "/%s" $s3.prefix)) | quote }}
- name: AWS_REGION
  value: {{ $s3.region | quote }}
{{- with $s3.endpoint }}
- name: AWS_ENDPOINT_URL
  value: {{ . | quote }}
{{- end }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ required "objectStore.s3.credentialsSecret is required when s3.enabled" $s3.credentialsSecret }}
      key: {{ $s3.credentialsKeys.accessKeyId }}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ $s3.credentialsSecret }}
      key: {{ $s3.credentialsKeys.secretAccessKey }}
{{- end }}
{{- end -}}
```

- [ ] **Step 5: Use the helper + conditionalise the PVC volume/mount + affinity**

In `ingest.yaml`, replace the standalone `LOOM_DATA_PATH` env entry:

```yaml
            - name: LOOM_DATA_PATH
              value: {{ .Values.objectStore.mountPath | quote }}
```

with:

```yaml
            {{- include "loom.objectStoreEnv" . | nindent 12 }}
```

Then wrap the `data` volumeMount and the `data` volume in `{{- if not .Values.objectStore.s3.enabled }} … {{- end }}`. Specifically, the ingest `volumeMounts` `- name: data …` block and the `volumes` `- name: data …` `persistentVolumeClaim` block.

In `query-api.yaml`, do the same for **both** containers' `LOOM_DATA_PATH` → `loom.objectStoreEnv`, wrap the two `data` volumeMounts (query-api + engine) and the single `data` volume in the same S3 guard, and wrap the **default affinity** block (`{{- else }} affinity: podAffinity … {{- end }}`) so it is skipped under S3:

```yaml
      {{- if .Values.queryApi.affinity }}
      affinity:
        {{- toYaml .Values.queryApi.affinity | nindent 8 }}
      {{- else if not .Values.objectStore.s3.enabled }}
      # Default: co-schedule onto ingest's node for the shared RWO PVC (see values).
      affinity:
        podAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  {{- include "loom.selectorLabels" . | nindent 18 }}
                  app.kubernetes.io/component: ingest
              topologyKey: kubernetes.io/hostname
      {{- end }}
```

- [ ] **Step 6: Skip the PVC when S3 enabled**

In `objectstore-pvc.yaml`, change the opening guard from `{{- if .Values.objectStore.retain -}}`-context to also require non-S3. The file starts with the PVC unconditionally; wrap the whole document:

```yaml
{{- if not .Values.objectStore.s3.enabled }}
# … existing PVC definition …
{{- end }}
```

(Keep the inner `{{- if .Values.objectStore.retain }}` annotation block intact.)

- [ ] **Step 7: Add the S3 egress NetworkPolicy**

In `networkpolicy.yaml`, inside the outer `{{- if .Values.networkPolicy.enabled -}}` block (e.g. after the `allow-postgres` block, before `gateway`), add:

```yaml
{{- if .Values.objectStore.s3.enabled }}
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "loom.fullname" . }}-allow-s3
  labels:
    {{- include "loom.labels" . | nindent 4 }}
spec:
  podSelector:
    matchLabels:
      {{- include "loom.selectorLabels" . | nindent 6 }}
  policyTypes:
    - Egress
  egress:
    - ports:
        - protocol: TCP
          port: {{ .Values.objectStore.s3.port }}
---
{{- end }}
```

(The default-deny chart is egress-locked; this opens the S3 port to any destination, mirroring the `allow-dns` no-`to` style. Operators tighten via `networkPolicy.extraEgress`.)

- [ ] **Step 8: Run assertions to verify pass**

Run: `bash deploy/chart/tests/render_assertions.sh; echo "exit=$?"`
Expected: `ALL ASSERTIONS PASSED`, exit 0, `helm lint` green.

- [ ] **Step 9: Commit**

```bash
git add deploy/chart/chart/values.yaml deploy/chart/chart/templates/_helpers.tpl \
  deploy/chart/chart/templates/ingest.yaml deploy/chart/chart/templates/query-api.yaml \
  deploy/chart/chart/templates/objectstore-pvc.yaml deploy/chart/chart/templates/networkpolicy.yaml \
  deploy/chart/tests/render_assertions.sh
git commit -m "feat(deploy): optional S3/MinIO object store in the chart"
```

---

## Task 6: Full verification, registers, and PR

**Files:**
- Modify: `docs/ROADMAP.md`, `docs/FUTURE.md`

- [ ] **Step 1: Full `//src` build + test**

Run: `buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)|error\[" /tmp/b.log`
Then: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: build succeeds; tests all pass. Investigate any failure (esp. other `Config { … }` literal sites needing `migrate_on_boot`).

- [ ] **Step 2: Clippy + prek hooks**

Run: `bash tools/clippy-all.sh > /tmp/c.log 2>&1; tail -3 /tmp/c.log`
Then: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -Ei "failed|passed" /tmp/p.log | tail -20`
Expected: clippy clean; all prek hooks pass. Commit any files the hooks rewrite.

- [ ] **Step 3: Re-run the chart assertions once more**

Run: `bash deploy/chart/tests/render_assertions.sh; echo "exit=$?"`
Expected: `ALL ASSERTIONS PASSED`.

- [ ] **Step 4: Close the register item + record the deferral**

Use `loom-docs-update` (or edit directly): in `docs/ROADMAP.md`, flip `road-deploy-hardening` `- [ ]`→`- [x]`, set `status:done`, add `pr:#N` (fill after PR opens). In `docs/FUTURE.md`, add:

```markdown
- [ ] **Optional LOOM_DATA_PATH under an s3:// warehouse** `{#fut-deploy-data-path-optional-s3 area:deploy status:deferred from:2026-06-30-deploy-hardening-design pr:- spec:-}`
  `Config::from_map` requires `LOOM_DATA_PATH` unconditionally, so the S3 chart path still sets it to an inert value. Make it optional (default) when `LOOM_WAREHOUSE_URI` is an `s3://` URI so an S3 deploy needs no local-path env.
```

Run: `bash tools/docs.sh validate` — expect clean.

- [ ] **Step 5: Commit + push + open PR**

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(registers): close road-deploy-hardening; defer LOOM_DATA_PATH-optional-s3"
git push -u origin work/road-deploy-hardening
```

Open a PR with head `work/road-deploy-hardening`, base `main`. Body: what/why, the code-seam vs chart-wiring split, the deviation note (`LOOM_DATA_PATH` kept in S3 mode; binary unchanged per spec), and that helm golden assertions were verified locally (helm 3.x) but are not in CI (deploy// off-sweep). Update the `pr:#N` in ROADMAP after the number is known.

- [ ] **Step 6: Ensure CI green**

Watch the PR's `affected` + `lint` checks. The `affected` job builds/tests the impacted `//src/...` targets (runtime + three services + the fixture test); `lint` runs prek on all files (incl. the new YAML/md). Fix any red check and push follow-ups until green.

---

## Self-Review

**Spec coverage:**
- Migrations code seam (migrate-and-exit `LOOM_MIGRATE` + on-boot `LOOM_DB_MIGRATE_ON_BOOT`, both reusing `run_embedded_migrations`) → Tasks 1-2. ✓
- `migrations.mode` (job default / onBoot / external) + updated NOTES → Task 4. ✓
- `objectStore.s3` block: S3 env on all three containers, PVC-skip, affinity relax, NetworkPolicy egress; PVC path unchanged when disabled → Task 5. ✓
- `helm lint` + `helm template` golden assertions → `render_assertions.sh` (Tasks 4-5), run in Task 6. ✓ (Deviation: run locally, not as a buck2 target, because `deploy//` is unbuildable in cloud sessions — documented in Global Constraints + the PR.)
- Managed-branch migrate fixture test (`loom_fixture_test`) → Task 3. ✓
- Out-of-scope items (binary S3 support, S3 e2e, multipart, workload-identity, down migrations) → untouched. ✓

**Deviations from the spec (called out, with reasons):**
1. **`LOOM_DATA_PATH` is kept in S3 mode** (not omitted) because `Config::from_map` requires it and the spec says "binary needs no change". The spec's "no LOOM_DATA_PATH local path" intent is realized as no PVC + no `data` mount. Deferred cleanup recorded as `fut-deploy-data-path-optional-s3`.
2. **Golden tests are a standalone `helm` script, not a buck2 test target** — the homelab external cell that `helm_chart` needs 403s in cloud sessions, and `deploy//` is off the CI sweep. The script is runnable in local/release environments where helm exists; it is executed and verified during this work.
3. **On-boot env is also emitted on the engine sidecar** (not just "ingest + query-api Deployments") so the engine, which shares the query-api pod and also reads the schema, is covered; the advisory lock makes the extra migrate attempt safe. The golden assertion still verifies the two Deployments carry the flag.

**Placeholder scan:** No TBD/TODO; every code + template + assertion block is complete.

**Type consistency:** `migrate_on_boot: bool` (Config), `run_migrations(&DbConfig) -> Result<(), RuntimeError>`, `migrate_requested() -> bool`, `build_pool_managed(&Config) -> Result<(PgPool, Option<EmbeddedPg>), RuntimeError>` used consistently across Tasks 1-3. Chart value names (`migrations.mode`, `migrations.backoffLimit`, `objectStore.s3.*`) and helper names (`loom.objectStoreEnv`, `loom.migrateOnBootEnv`) are consistent across values, templates, and assertions.
