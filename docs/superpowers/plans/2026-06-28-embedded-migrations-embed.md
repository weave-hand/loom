# Embed migrations via `sqlx::migrate!` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bake the control-plane migrations into the binary at compile time via `sqlx::migrate!`, and delete the on-disk migration machinery so embedded mode needs no migrations directory.

**Architecture:** Add `embedded_migrator()`/`run_embedded_migrations()` to `control_plane_postgres`, backed by `sqlx::migrate!("./migrations")` with the `migrations/*.sql` declared into the compile sandbox via buck2 `mapped_srcs` (proven hermetic on RE). Then consolidate every migration-application site onto it and delete `run_migrations(pool, dir)`, the `:migrations` filegroup, and all `LOOM_MIGRATIONS_DIR` plumbing.

**Tech Stack:** Rust, sqlx 0.9.0 (`migrate` feature, already enabled), buck2 (`mapped_srcs`, `loom_fixture_test`), BuildBuddy RE.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`. Pure-logic tests are plain `rust_test` (RE-eligible); Postgres-booting tests use `loom_fixture_test`. (CLAUDE.md → Testing.)
- **`sqlx::migrate!` path must be `"./migrations"`** — a bare `"migrations"` is rejected by sqlx 0.9 (`resolve_path` requires a non-empty parent component).
- **`CARGO_MANIFEST_DIR="."` on the `postgres` rust_library stays unchanged** — it satisfies clippy (`clippy.toml` discovery) and the `query!` offline resolver. Do not repoint it.
- **No new third-party dependencies.** The embed uses only the already-enabled sqlx `migrate` feature.
- **Don't pipe `buck2 test` through `tail`/`head`** — redirect to a file and grep (`buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`). (CLAUDE.md → Testing.)
- **`tools/sqlx-prepare.sh` is out of scope and must NOT change** — it applies migrations by globbing the source `migrations/*.sql` through `psql` directly.

---

### Task 1: Add the embedded migrator + buck wiring + unit proof

Adds the new public surface and the `mapped_srcs` wiring, and proves both that the macro embeds the real migration set and that `migrate!` coexists with the crate's existing `query!` macros on RE. `run_migrations(pool, dir)` is left in place this task (its callers are switched in Task 2), so the tree stays green.

**Files:**
- Modify: `src/control-plane/postgres/src/lib.rs` (add functions after the existing `run_migrations`, ~line 90)
- Modify: `src/control-plane/postgres/BUCK` (add `mapped_srcs` to the `postgres` rust_library ~line 121; correct the comment at lines 99–101; add a new `rust_test` target)
- Create: `src/control-plane/postgres/tests/embedded_migrations_unit.rs`

**Interfaces:**
- Produces:
  - `control_plane_postgres::embedded_migrator() -> sqlx::migrate::Migrator`
  - `control_plane_postgres::run_embedded_migrations(pool: &sqlx::PgPool) -> control_plane_core::Result<()>` (same `Result` alias `run_migrations` returns)

- [ ] **Step 1: Write the failing unit test**

Create `src/control-plane/postgres/tests/embedded_migrations_unit.rs`:

```rust
//! Proves the compile-time embed is non-empty and complete: `sqlx::migrate!`
//! baked every `migrations/*.sql` into the binary, with contiguous versions
//! starting at 1. Pure-logic (no DB) — RE-eligible.

#[test]
fn embedded_migrator_versions_are_contiguous_from_one() {
    let migrator = control_plane_postgres::embedded_migrator();
    let mut versions: Vec<i64> = migrator.iter().map(|m| m.version).collect();
    versions.sort_unstable();

    assert!(!versions.is_empty(), "embedded migrations must not be empty");
    let expected: Vec<i64> = (1..=versions.len() as i64).collect();
    assert_eq!(
        versions, expected,
        "embedded migration versions must be contiguous starting at 1"
    );
}
```

- [ ] **Step 2: Wire the unit-test target in BUCK**

In `src/control-plane/postgres/BUCK`, after the `iceberg-schema-evolution` test target (~line 178), add:

```python
rust_test(
    name = "embedded-migrations-unit",
    crate = "embedded_migrations_unit",
    srcs = ["tests/embedded_migrations_unit.rs"],
    crate_root = "tests/embedded_migrations_unit.rs",
    edition = "2024",
    deps = [":postgres", "//third-party:sqlx"],
)
```

- [ ] **Step 3: Run the test — verify it fails**

Run: `buck2 test //src/control-plane/postgres:embedded-migrations-unit > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log`
Expected: FAIL — `cannot find function 'embedded_migrator' in crate 'control_plane_postgres'`.

- [ ] **Step 4: Add the embedded migrator to `lib.rs`**

In `src/control-plane/postgres/src/lib.rs`, immediately after the existing `run_migrations` function (after the closing `}` at ~line 90), add:

```rust
/// The control-plane migrations, baked into the binary at compile time via
/// `sqlx::migrate!` (no migrations-on-disk). The `./` prefix is required: sqlx
/// rejects a bare single-component path. The `migrations/*.sql` files are
/// declared into the compile sandbox by the `mapped_srcs` on this crate's
/// buck target, so the macro reads them hermetically (incl. on RE).
pub fn embedded_migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate!("./migrations")
}

/// Apply the embedded migrations (tracked in `_sqlx_migrations`; idempotent —
/// re-runs as a no-op).
pub async fn run_embedded_migrations(pool: &PgPool) -> Result<()> {
    embedded_migrator()
        .run(pool)
        .await
        .map_err(|e| ControlPlaneError::Backend(Box::new(e)))?;
    Ok(())
}
```

- [ ] **Step 5: Add `mapped_srcs` to the `postgres` rust_library + fix the comment**

In `src/control-plane/postgres/BUCK`, inside the `rust_library(name = "postgres", ...)` block, add this line alongside `srcs`/`crate_root` (~line 120):

```python
    mapped_srcs = {f: f for f in glob(["migrations/*.sql"])},
```

Then replace the now-false comment above the `:migrations` filegroup (lines 99–101) with:

```python
# Migrations are embedded into the binary at compile time via sqlx::migrate!
# (see control_plane_postgres::embedded_migrator). The `mapped_srcs` on the
# :postgres library declares migrations/*.sql into the compile sandbox so the
# macro reads them hermetically; this filegroup remains only as a PUBLIC
# input for any out-of-tree consumer.
```

(The `:migrations` filegroup itself is deleted in Task 2, once `LOOM_MIGRATIONS_DIR` is gone.)

- [ ] **Step 6: Run the unit test — verify it passes**

Run: `buck2 test //src/control-plane/postgres:embedded-migrations-unit > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS (`Tests finished: Pass 1. Fail 0.`).

- [ ] **Step 7: Prove `migrate!` + `query!` coexist by building the lib on RE**

Run: `buck2 build //src/control-plane/postgres:postgres 2>&1 | tail -3`
Expected: `BUILD SUCCEEDED`. (This is the one open risk from the spec — the lib expands both the offline `query!` macros and the new `migrate!`. A green build confirms they coexist.)

- [ ] **Step 8: Lint the touched library**

Run: `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' 2>&1 | tail -3 && cat "$(buck2 build --show-output '//src/control-plane/postgres:postgres[clippy.txt]' 2>/dev/null | awk '{print $2}')"`
Expected: empty clippy output (clean).

- [ ] **Step 9: Commit**

```bash
git add src/control-plane/postgres/src/lib.rs src/control-plane/postgres/BUCK src/control-plane/postgres/tests/embedded_migrations_unit.rs
git commit -m "feat(postgres): embed migrations via sqlx::migrate! (mapped_srcs)

Adds embedded_migrator()/run_embedded_migrations() backed by
sqlx::migrate!(\"./migrations\"); mapped_srcs declares migrations/*.sql into
the compile sandbox so the macro embeds them hermetically (proven on RE).
run_migrations(dir) callers are switched in the next commit.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Consolidate all callers onto the embedded migrator; delete the dir path

Atomic change: switch `fixture.rs`, the runtime embedded arm, and the slice-1 lifecycle test to `run_embedded_migrations`, drop `migrations_dir`/`LOOM_MIGRATIONS_DIR`, and delete `run_migrations` + the `:migrations` filegroup. The tree cannot build with `run_migrations` deleted but a caller un-switched, so these land together.

**Files:**
- Modify: `src/control-plane/postgres/src/lib.rs` (delete `run_migrations` ~lines 80–90; remove `use std::path::Path;` line 11)
- Modify: `src/control-plane/postgres/src/fixture.rs:306-308`
- Modify: `src/control-plane/postgres/BUCK` (delete the `:migrations` filegroup, lines ~99–106)
- Modify: `src/control-plane/postgres/defs.bzl:40` (remove the `LOOM_MIGRATIONS_DIR` env entry)
- Modify: `src/services/runtime/src/lib.rs` (struct ~37–42, parse ~166–184, use site ~278–280)
- Modify: `src/services/runtime/tests/embedded_config.rs` (remove the `LOOM_MIGRATIONS_DIR` insert + `migrations_dir` assertion)
- Modify: `src/services/managed-postgres/tests/embedded_lifecycle.rs` (lines 3, 25–26, 37, 60)

**Interfaces:**
- Consumes: `control_plane_postgres::run_embedded_migrations` (from Task 1).
- Produces: `EmbeddedSettings { cfg }` — the `migrations_dir` field is removed.

- [ ] **Step 1: Update the failing tests first (they drive the change)**

In `src/services/runtime/tests/embedded_config.rs`, delete the `LOOM_MIGRATIONS_DIR` insert (line 32):

```rust
    vars.insert("LOOM_MIGRATIONS_DIR".into(), "/opt/loom/migrations".into());
```

and delete the trailing `migrations_dir` assertion (the final `assert_eq!` block, lines ~44–47):

```rust
    assert_eq!(
        e.migrations_dir,
        std::path::PathBuf::from("/opt/loom/migrations")
    );
```

In `src/services/managed-postgres/tests/embedded_lifecycle.rs`:
- Line 3: drop `/ LOOM_MIGRATIONS_DIR` from the doc comment so it reads `(POSTGRES_BIN_DIR / POSTGRES_LD_LIBRARY_PATH).`
- Delete the `migrations` binding (lines 25–26):

```rust
    let migrations =
        PathBuf::from(std::env::var("LOOM_MIGRATIONS_DIR").expect("LOOM_MIGRATIONS_DIR"));
```

- Replace **both** migration calls (lines 37 and 60):

```rust
    control_plane_postgres::run_migrations(&pool, &migrations)
        .await
        .expect("migrate 1");
```
becomes
```rust
    control_plane_postgres::run_embedded_migrations(&pool)
        .await
        .expect("migrate 1");
```
(and the second occurrence with `.expect("migrate 2")`).

- [ ] **Step 2: Run the edited tests — verify they fail to compile**

Run: `buck2 build //src/services/runtime:* //src/services/managed-postgres:* 2>&1 | grep -E "error|FAILED|SUCCEEDED" | head`
Expected: FAIL — `embedded_config` references nothing removed yet so it builds, but `embedded-lifecycle` now calls `run_embedded_migrations` (exists) while `EmbeddedSettings` still has `migrations_dir` unused; the real failures appear once Step 3–5 land. (If everything builds here, that's fine — the driving failure is the deletion in Step 6.)

- [ ] **Step 3: Switch `fixture.rs` to the embedded migrator**

In `src/control-plane/postgres/src/fixture.rs`, replace lines 306–308:

```rust
        let migrations = std::env::var("LOOM_MIGRATIONS_DIR").expect("LOOM_MIGRATIONS_DIR");
        crate::run_migrations(&pool, std::path::Path::new(&migrations))
            .await
            .expect("run migrations");
```
with:
```rust
        crate::run_embedded_migrations(&pool)
            .await
            .expect("run migrations");
```

- [ ] **Step 4: Switch the runtime embedded arm + drop `migrations_dir`**

In `src/services/runtime/src/lib.rs`:

(a) `EmbeddedSettings` struct (~37–42) — remove the field and its doc:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddedSettings {
    pub cfg: managed_postgres::EmbeddedPgConfig,
}
```

(b) Parse block (~166–184) — delete the `migrations_dir` binding and the struct field:

```rust
        let embedded = if vars.get("LOOM_PG_MODE").map(String::as_str) == Some("embedded") {
            let bin_dir = PathBuf::from(req("LOOM_PG_BIN_DIR")?);
            Some(EmbeddedSettings {
                cfg: managed_postgres::EmbeddedPgConfig {
                    bin_dir,
                    ld_library_path: vars
                        .get("LOOM_PG_LD_LIBRARY_PATH")
                        .cloned()
                        .unwrap_or_default(),
                    data_dir: data_path.join("pgdata"),
                    socket_dir: data_path.join("pgrun"),
                    database: req("LOOM_DB_NAME")?,
                },
            })
        } else {
            None
        };
```

(c) Use site (~278–280) — switch the call:

```rust
            control_plane_postgres::run_embedded_migrations(&pool)
                .await
                .map_err(RuntimeError::Migrate)?;
```

- [ ] **Step 5: Delete `run_migrations` + its orphaned import**

In `src/control-plane/postgres/src/lib.rs`, delete the whole `run_migrations` function (the doc comment + fn, lines ~80–90) and remove the now-unused import at line 11:

```rust
use std::path::Path;
```

- [ ] **Step 6: Delete the `:migrations` filegroup + the `LOOM_MIGRATIONS_DIR` env**

In `src/control-plane/postgres/BUCK`, delete the `:migrations` filegroup block (the comment you rewrote in Task 1 + the `filegroup(name = "migrations", ...)`, lines ~99–106).

In `src/control-plane/postgres/defs.bzl`, delete the `LOOM_MIGRATIONS_DIR` entry (line 40):

```python
        "LOOM_MIGRATIONS_DIR": "$(location //src/control-plane/postgres:migrations)/migrations",
```

- [ ] **Step 7: Build the affected crates — verify green**

Run: `buck2 build //src/control-plane/postgres:postgres //src/services/runtime:runtime //src/services/managed-postgres:managed-postgres 2>&1 | tail -3`
Expected: `BUILD SUCCEEDED` (no dangling `run_migrations`, `migrations_dir`, `Path`, or `LOOM_MIGRATIONS_DIR` references).

- [ ] **Step 8: Run the full fixture-backed suite (the regression backstop)**

Run: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass <N>. Fail 0.` The hermetic-PG tests (`postgres` contract tests, the `.sqlx` freshness test, `query-api`/`worker` e2e, and the rewritten `embedded-lifecycle`) all boot through `fixture.rs`, now migrating via `run_embedded_migrations` — so a green sweep proves the embed is equivalent to the old dir path.

- [ ] **Step 9: Lint all touched crates**

Run: `./tools/clippy-all.sh 2>&1 | tail -5`
Expected: clean (no findings).

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "refactor(postgres): consolidate onto embedded migrator; delete dir path

Switches fixture.rs, the runtime embedded arm, and the slice-1 lifecycle
test to run_embedded_migrations. Deletes run_migrations(pool, dir), the
:migrations filegroup, the migrations_dir EmbeddedSettings field, and all
LOOM_MIGRATIONS_DIR plumbing. Embedded mode no longer needs migrations on
disk. sqlx-prepare.sh is untouched (it globs the source SQL via psql).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Update the documentation registers

Records the capability as shipped and corrects the register that tracked it. Use the loom-docs-update conventions; the exact edits are below.

**Files:**
- Modify: `docs/ROADMAP.md` (add a `status:done` slice-2a item)
- Modify: `docs/FUTURE.md` (annotate `fut-embedded-postgres-embed-extract` — migrations done, PG binaries pending)

- [ ] **Step 1: Add the ROADMAP item**

In `docs/ROADMAP.md`, add (in the `deploy` area, near `road-embedded-postgres-slice1`):

```markdown
- [x] **Embedded Postgres: migrations baked into the binary (slice 2, PR-a)** `{#road-embedded-migrations-embed area:deploy status:done from:2026-06-28-embedded-migrations-embed-design pr:- spec:2026-06-28-embedded-migrations-embed-design}`
  `sqlx::migrate!("./migrations")` embeds the control-plane migrations at compile time (migrations declared into the compile sandbox via buck `mapped_srcs`); consolidated all callers onto `run_embedded_migrations` and deleted the on-disk `run_migrations(dir)` / `:migrations` filegroup / `LOOM_MIGRATIONS_DIR` machinery. Embedded mode needs no migrations on disk. PG-binary embedding is the remaining half — see [[fut-embedded-postgres-embed-extract]].
```

- [ ] **Step 2: Annotate the FUTURE item**

In `docs/FUTURE.md`, update the body of `fut-embedded-postgres-embed-extract` (line ~249) to record the migrations half as done and scope the item to the PG binaries:

```markdown
  Slice 2's PG-binary half: compress the PG distribution into the loom binary (`include_bytes!`) and extract to a content-addressed cache dir at startup, so no PG binaries need to exist on disk. **Migrations half done** (see [[road-embedded-migrations-embed]]) — embedded via `sqlx::migrate!`; this item now tracks only the PG-binary embed. Builds on [[road-embedded-postgres-slice1]].
```

- [ ] **Step 3: Validate the registers**

Run: `bash tools/docs.sh validate 2>&1 | tail -5`
Expected: no errors (grammar/ids/vocab/links valid; the `spec:` slug resolves to the on-disk design doc).

- [ ] **Step 4: Run the markdown lint hooks (registers must pass `lint` CI)**

Run: `buck2 run //tools:prek -- run --all-files 2>&1 | tail -15`
Expected: all hooks pass; if `end-of-file-fixer`/`trim trailing whitespace` rewrite anything, re-stage it.

- [ ] **Step 5: Commit**

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(registers): record embedded-migrations slice (2a) shipped

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- Mechanism (mapped_srcs + `migrate!("./migrations")` + unchanged `CARGO_MANIFEST_DIR`) → Task 1 Steps 4–5, proven Step 6–7.
- `embedded_migrator()` / `run_embedded_migrations()` → Task 1 Step 4.
- Switch every application site (fixture, runtime arm, lifecycle test) → Task 2 Steps 1, 3, 4.
- Delete `run_migrations`, `:migrations` filegroup, `LOOM_MIGRATIONS_DIR`, `migrations_dir` → Task 2 Steps 4–6.
- `sqlx-prepare.sh` untouched → Global Constraints + not in any task's file list.
- Correct the false BUCK comment → Task 1 Step 5.
- Pure-logic unit (count/contiguity) + fixture regression → Task 1 Steps 1–6, Task 2 Step 8.
- Register updates → Task 3.

**Placeholder scan:** none — every code step shows exact old/new text; every command has expected output.

**Type consistency:** `embedded_migrator() -> sqlx::migrate::Migrator` and `run_embedded_migrations(&PgPool) -> Result<()>` are used identically in Tasks 1–2; `EmbeddedSettings { cfg }` (field removed) is consistent across the struct, parse block, and `embedded_config` test; `RuntimeError::Migrate` is reused unchanged at the runtime use site.
