# Fix `SqlCatalog::execute` swallowed commit error — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the no-transaction (auto-commit) arm of `SqlCatalog::execute` propagate a failed `COMMIT` to the caller instead of silently discarding it, and commit only after a successful statement.

**Architecture:** A 3-line behavior fix in one branch of one method, plus a hermetic-Postgres `rust_test` that exercises the auto-commit arm directly. The method is currently private, so we widen it to `pub` to let an integration-test crate call it (every public `Catalog` method guards before reaching the `None` arm, so there is no public path that can trigger a statement/commit failure inside it).

**Tech Stack:** Rust, sqlx 0.9 (Postgres), apache/iceberg-rust `Catalog` trait, buck2 `loom_fixture_test`, hermetic Postgres fixture.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-06-29-sqlcatalog-execute-commit-error-design.md`; register item `[[iss-sqlcatalog-execute-commit-swallow]]` in `docs/ISSUES.md`.
- **No inline tests.** Tests are `rust_test` integration targets in `tests/<name>.rs`, never `#[cfg(test)] mod tests`. The `no-inline-tests` prek hook enforces this.
- **Fixture tests use `loom_fixture_test`** (not bare `rust_test`) — they boot the hermetic Postgres cluster, which refuses to run as root on RE, so the macro pins the test command local.
- **Strict clippy** (pedantic + restriction) on production lib code. `missing_errors_doc` / `missing_panics_doc` are in the toolchain allowlist; `#![deny(missing_docs)]` is set on the `iceberg_sql_catalog` module, so any new/`pub`-widened item needs a `///` doc comment (the target method already has one).
- **Don't pipe `buck2 test` through `tail`/`head`** — redirect to a file and grep it.
- **Conventional Commits** enforced on the commit message (commit-msg hook). Use `fix(iceberg): …`.

---

## Context the implementer needs

`SqlCatalog` (`src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs`) is loom's
vendored apache/iceberg-rust `Catalog` impl and the single object through which every
table mutation commits. The buggy branch is the **`None`** (no caller-supplied
transaction) arm of the inherent `execute` method (catalog.rs:386-394 on `main`):

```rust
match transaction {
    Some(t) => sqlx_query.execute(&mut **t).await.map_err(from_sqlx_error),
    None => {
        let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;
        let result = sqlx_query.execute(&mut *tx).await.map_err(from_sqlx_error);
        drop(tx.commit().await.map_err(from_sqlx_error));   // <-- bug: commit Result discarded
        result
    }
}
```

`execute` is an **inherent** method on `SqlCatalog` (in `impl SqlCatalog`, not
`impl Catalog for SqlCatalog`), currently **private**. `SqlCatalog` and
`SqlCatalogBuilder` are already `pub` and are constructed directly in other crates'
tests via `SqlCatalogBuilder::default().with_storage_factory(...).load("loom", props)`
(see `src/control-plane/postgres/tests/iceberg_compact.rs`).

**Two observable behaviors of the `None` arm to lock down:**

1. **Statement failure** (e.g. duplicate-PK insert): `sqlx_query.execute` returns
   `Err`. The method must return `Err`, and nothing must persist. *This holds on both
   pre- and post-fix code* — in Postgres a `COMMIT` issued on an already-aborted
   transaction is downgraded to `ROLLBACK` — so a statement-failure test is a behavior
   **lock**, not a red→green catcher for the swallowed-commit bug.
2. **Commit failure** (statement succeeds, `COMMIT` fails): a `UNIQUE … DEFERRABLE
   INITIALLY DEFERRED` constraint whose check fires at `COMMIT`. *Here pre- and
   post-fix diverge:* pre-fix returns the earlier `Ok` (bug); post-fix propagates the
   `Err`. This is the genuine red→green regression test for the fix.

> **Note on spec scope.** The spec's *Testing* section mandates the statement-failure
> test and records the commit-failure test as "out of scope … fixture-fragile." This
> plan **also** adds the commit-failure test because, on technical examination, a
> `DEFERRABLE INITIALLY DEFERRED` unique violation fails at `COMMIT` deterministically
> in the hermetic fixture (it is the only path that actually exercises the defect, and
> Step 3 below verifies the red empirically before the fix is applied). Both tests ship.
> If Step 3 shows the commit-failure test is *not* a clean red on pre-fix code, that is
> a real finding: keep the spec-mandated statement-failure test, drop the
> commit-failure test, and record the divergence in the PR body — do not paper over it.

---

## Task 1: Fix the swallowed commit error, guarded by a hermetic-Postgres regression test

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` (the `execute` method: widen to `pub`; rewrite the `None` arm)
- Create: `src/control-plane/postgres/tests/sqlcatalog_execute_commit.rs`
- Modify: `src/control-plane/postgres/BUCK` (add the `loom_fixture_test` target)

**Interfaces:**
- Consumes (from the lib, already `pub`): `control_plane_postgres::fixture::PgFixture`
  (`fn pg_dsn(&self, db: &str) -> String`, `async fn pool_for(&self, db: &str) -> PgPool`,
  `async fn fresh_db(&self) -> (PgControlPlane, String)`);
  `control_plane_postgres::iceberg_sql_catalog::{SqlCatalog, SqlCatalogBuilder,
  SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE}`; `iceberg::CatalogBuilder`;
  `iceberg::io::LocalFsStorageFactory`.
- Produces: `SqlCatalog::execute` becomes
  `pub async fn execute(&self, query: &str, args: Vec<Option<&str>>,
  transaction: Option<&mut Transaction<'_, Postgres>>) -> Result<PgQueryResult>`
  (signature unchanged; visibility widened private → `pub`).

- [ ] **Step 1: Widen `execute` to `pub` (enabling change, no behavior change)**

In `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs`, change the method
declaration (keep its existing `/// Execute statements in a transaction, provided or not`
doc comment immediately above it so `#![deny(missing_docs)]` is satisfied):

```rust
    /// Execute statements in a transaction, provided or not
    pub async fn execute(
        &self,
        query: &str,
        args: Vec<Option<&str>>,
        transaction: Option<&mut Transaction<'_, Postgres>>,
    ) -> Result<PgQueryResult> {
```

Leave the method **body unchanged** in this step (the `None` arm is still buggy — that
is intentional, so the new commit-failure test goes red in Step 3).

- [ ] **Step 2: Write the test file and wire its buck2 target**

Create `src/control-plane/postgres/tests/sqlcatalog_execute_commit.rs`:

```rust
//! Regression + characterization tests for the no-transaction (auto-commit) arm of
//! `SqlCatalog::execute` — the path that begins a tx, runs one statement, then commits.
//! Fixes [[iss-sqlcatalog-execute-commit-swallow]]
//! (docs/superpowers/specs/2026-06-29-sqlcatalog-execute-commit-error-design.md).
//!
//! `loom_fixture_test`, not an inline module — loom forbids inline `#[test]`.

use std::collections::HashMap;
use std::sync::Arc;

use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{warehouse}"),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

/// Behavior lock: a statement that fails *at execution time* (duplicate primary key)
/// surfaces as `Err`, and the auto-commit transaction rolls back so nothing extra
/// persists. Passes on both pre- and post-fix code (Postgres downgrades COMMIT on an
/// aborted tx to ROLLBACK), so this guards the statement-failure surface rather than
/// catching the swallowed-commit bug.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn statement_failure_rolls_back_and_errors() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    catalog
        .execute(
            "CREATE TABLE loom_exec_probe (id TEXT PRIMARY KEY)",
            vec![],
            None,
        )
        .await
        .expect("create probe table");

    catalog
        .execute(
            "INSERT INTO loom_exec_probe (id) VALUES (?)",
            vec![Some("a")],
            None,
        )
        .await
        .expect("first insert persists");

    // Duplicate primary key -> the statement fails at execution time.
    let dup = catalog
        .execute(
            "INSERT INTO loom_exec_probe (id) VALUES (?)",
            vec![Some("a")],
            None,
        )
        .await;
    assert!(dup.is_err(), "duplicate insert must surface an error");

    // The failed statement's auto-commit tx rolled back: exactly the first row remains.
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM loom_exec_probe")
        .fetch_one(&pool)
        .await
        .expect("count rows");
    assert_eq!(count, 1, "only the first insert persisted");
}

/// Regression test for the swallowed-commit bug. The duplicate rows pass the
/// `DEFERRABLE INITIALLY DEFERRED` unique check at statement time, so `execute`
/// succeeds at the statement and then fails at `COMMIT`. Pre-fix code dropped the
/// commit `Result` and returned the earlier `Ok`; the fix propagates the failure as
/// `Err`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_failure_propagates_as_error() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    catalog
        .execute(
            "CREATE TABLE loom_commit_probe (\
                 id TEXT, \
                 UNIQUE (id) DEFERRABLE INITIALLY DEFERRED\
             )",
            vec![],
            None,
        )
        .await
        .expect("create deferred-constraint table");

    // One statement inserts two equal rows; the deferred UNIQUE check fires at COMMIT.
    let result = catalog
        .execute(
            "INSERT INTO loom_commit_probe (id) VALUES (?), (?)",
            vec![Some("dup"), Some("dup")],
            None,
        )
        .await;
    assert!(
        result.is_err(),
        "a commit-time constraint violation must propagate as Err, not be swallowed"
    );
}
```

In `src/control-plane/postgres/BUCK`, add a target next to the other
`loom_fixture_test` entries (mirror `iceberg-compact`'s style):

```python
loom_fixture_test(
    name = "sqlcatalog-execute-commit",
    crate = "sqlcatalog_execute_commit",
    srcs = ["tests/sqlcatalog_execute_commit.rs"],
    crate_root = "tests/sqlcatalog_execute_commit.rs",
    deps = [
        ":postgres",
        "//third-party:iceberg",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run both tests against the PRE-FIX body — confirm the red**

Run (redirect, then grep — never pipe `buck2 test` to `tail`/`head`):

```bash
buck2 test //src/control-plane/postgres:sqlcatalog-execute-commit > /tmp/t.log 2>&1
grep -E "Tests finished|PASS|FAIL|statement_failure|commit_failure" /tmp/t.log
```

Expected with the fix **not yet applied**:
- `commit_failure_propagates_as_error` — **FAIL** (pre-fix returns `Ok`, so
  `assert!(result.is_err())` fails). This is the red that proves the test catches the bug.
- `statement_failure_rolls_back_and_errors` — **PASS** (behavior lock).

If `commit_failure_propagates_as_error` does **not** fail here, stop and follow the
"Note on spec scope" guidance above: drop that test, keep the statement-failure test,
and document the divergence in the PR. Do not proceed to Step 4 pretending it was red.

- [ ] **Step 4: Apply the fix to the `None` arm**

In `catalog.rs`, replace the `None` arm body:

```rust
            None => {
                let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;
                let result = sqlx_query.execute(&mut *tx).await.map_err(from_sqlx_error)?;
                tx.commit().await.map_err(from_sqlx_error)?;
                Ok(result)
            }
```

The `Some(t)` arm is unchanged. Net effect: `?` short-circuits on a statement error
(the dropped `tx` rolls back), `COMMIT` runs only after a successful statement, and a
commit failure now surfaces to the caller.

- [ ] **Step 5: Run both tests against the POST-FIX body — confirm the green**

```bash
buck2 test //src/control-plane/postgres:sqlcatalog-execute-commit > /tmp/t.log 2>&1
grep -E "Tests finished|PASS|FAIL" /tmp/t.log
```

Expected: **both PASS**.

- [ ] **Step 6: Confirm no regression in the catalog contract + clippy**

```bash
buck2 test //src/control-plane/postgres:iceberg_catalog > /tmp/t2.log 2>&1
grep -E "Tests finished|PASS|FAIL" /tmp/t2.log
buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log
```

Expected: contract tests **PASS** (success-commit path unchanged); clippy output empty
(the `pub` widening adds no lint — `missing_errors_doc` is allow-listed and the doc
comment satisfies `missing_docs`).

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs \
        src/control-plane/postgres/tests/sqlcatalog_execute_commit.rs \
        src/control-plane/postgres/BUCK
git commit -m "fix(iceberg): propagate SqlCatalog::execute auto-commit failure

The no-transaction arm of SqlCatalog::execute dropped its tx.commit()
Result, so a failed COMMIT returned the earlier successful execute result
as if it had persisted; it also committed even when the statement errored.
Propagate both via ?: commit only after a successful statement, and surface
a commit failure to the caller. Widen execute to pub so the hermetic-Postgres
regression test can drive the auto-commit arm directly (no public Catalog
method reaches the None arm with a failing statement/commit)."
```

(End the commit message body before any tooling/identifier footer — the harness adds
the required trailers.)

---

## Task 2: Close the register item

**Files:**
- Modify: `docs/ISSUES.md` (the `iss-sqlcatalog-execute-commit-swallow` entry)

- [ ] **Step 1: Update the register via `loom-docs-update`**

Invoke the `loom-docs-update` skill (or edit directly): flip the checkbox
`- [ ]` → `- [x]`, set `status:open` → `status:fixed`, and set `pr:-` → `pr:#<N>` once
the PR number is known. The entry currently reads:

```
- [ ] **`SqlCatalog::execute` swallows its commit error** `{#iss-sqlcatalog-execute-commit-swallow area:iceberg status:open from:clippy-strict-lints pr:- spec:2026-06-29-sqlcatalog-execute-commit-error-design}`
```

- [ ] **Step 2: Validate the registers**

```bash
bash tools/docs.sh validate > /tmp/v.log 2>&1; cat /tmp/v.log
```

Expected: validation passes.

- [ ] **Step 3: Stage and commit the register edit**

```bash
git add docs/ISSUES.md
git commit -m "docs(issues): close iss-sqlcatalog-execute-commit-swallow"
```

---

## Self-Review

**1. Spec coverage:**
- *Problem / Fix* (replace the `None` arm with `?`-propagation, `Some(t)` arm unchanged,
  no signature change) → Task 1 Step 4.
- *Testing* (statement-failure path: drive a failing statement through
  `execute(query, args, None)`, assert `Err`, assert nothing persisted via a follow-up
  read; `loom_fixture_test`, sibling target) → Task 1 Steps 2-3,5
  (`statement_failure_rolls_back_and_errors`). The spec-recorded commit-failure gap is
  additionally covered by `commit_failure_propagates_as_error`, with the scope note and
  an empirical red-check (Step 3) documenting the deviation.
- *Scope* (exactly the one branch; other commit sites untouched) → Task 1 Step 4 only
  touches the `None` arm.
- *Register close* (`[[iss-sqlcatalog-execute-commit-swallow]]`) → Task 2.

**2. Placeholder scan:** No TBD/TODO/"handle errors"/"similar to" — every code and
command step is concrete. The only conditional (Step 3 red-check fallback) is an
explicit, spec-grounded decision branch with a defined action, not a placeholder.

**3. Type consistency:** `execute(&str, Vec<Option<&str>>, Option<&mut Transaction<'_,
Postgres>>) -> Result<PgQueryResult>` used identically in the fix and the test calls
(`vec![]`/`vec![Some("a")]` for args, `None` for the transaction — both infer from the
fixed signature). `make_catalog` returns the concrete `SqlCatalog`, so `.execute`
resolves to the inherent `pub` method. `fx.pg_dsn` → `String`, `fx.pool_for` → `PgPool`,
`fx.fresh_db` → `(PgControlPlane, String)` match their call sites. BUCK deps
(`:postgres`, `//third-party:{iceberg,sqlx,tempfile,tokio}`) match the test's `use`s.
