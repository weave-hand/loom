# Compile-Time sqlx Queries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate the postgres adapter from sqlx runtime `query()` to compile-time-checked `query!`/`query_as!`, working under hermetic buck2 (proven by the spike), enforced by a `.sqlx` cache + prepare harness + pre-push hook.

**Architecture:** Bump sqlx 0.8.6→0.9.0; commit a prepared `.sqlx` cache; wire it into the postgres `rust_library` via `SQLX_OFFLINE_DIR=$(location :sqlx-cache)/.sqlx`. Migrate one concern per task, regenerating `.sqlx` and testing after each so the tree always builds. `fixture.rs` stays runtime (its DuckLake tables aren't in the prepared schema).

**Tech Stack:** Rust, buck2, sqlx 0.9 (postgres), reindeer, the pinned `:postgres-bin`, prek.

**Spec:** `docs/superpowers/specs/2026-06-08-sqlx-compile-time-queries-design.md` — read the migration-pattern + caveats section before Task 3.

---

## Conventions

- **Build:** `env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/postgres:postgres`. **RE proof** (CI-critical): `env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/postgres:postgres --remote-only --no-remote-cache`.
- **Test:** `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres/...` (boots ephemeral pg per test).
- **Regenerate the cache:** `./tools/sqlx-prepare.sh` (built in Task 2). Run it after editing ANY `query!`, then commit the `.sqlx` change.
- **Format before commit:** `eval "$(./tools/env.sh)"` then `rustfmt --edition 2024 <files>`.
- **Dep workflow:** Cargo.toml edit → `buck2 run //tools:reindeer -- update` → `./tools/buckify.sh` → BUCK deps. All four or `reindeer-check` fails.
- **No behaviour change.** `query!` is a compile-time substitution; the SQL strings, args, and resulting domain values must be identical. Tests prove it.

---

## Task 1: Bump sqlx 0.8.6 → 0.9.0 (still runtime queries)

Get the crate green on 0.9 BEFORE introducing any macro, so the version bump is isolated from the migration.

**Files:** `src/control-plane/postgres/Cargo.toml`, `…/src/lib.rs`, `…/src/queue.rs`, `…/src/catalog.rs`, `…/src/ontology.rs`, `…/src/acl.rs`, `…/src/lineage.rs`, `…/src/fixture.rs`; `Cargo.lock`, `third-party/BUCK`.

- [ ] **Step 1: Baseline.** `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres/...` — record pass counts (must be identical at the end).

- [ ] **Step 2: Bump the dep.** In `src/control-plane/postgres/Cargo.toml` change `sqlx = { version = "0.8", … }` to `version = "0.9"`, keeping the same features and adding `"macros"` if not present. Then `buck2 run //tools:reindeer -- update` and `./tools/buckify.sh`. (Only one sqlx version now — no alias collision.)

- [ ] **Step 3: Port `SqlSafeStr` (#3723).** The build will now fail wherever `sqlx::query(...)`/`query_scalar(...)` takes a `&str`. For EVERY such call in the adapter (queue/catalog/ontology/acl/lineage — incl. `pg_insert`/`pg_emit`) AND in `fixture.rs`, wrap the SQL string literal in `sqlx::query(sqlx::types::… )` → use `AssertSqlSafe`: `sqlx::query(AssertSqlSafe("…"))`. Import path: `use sqlx::query::AssertSqlSafe;` (verify the exact path against the 0.9 docs; the build error names it). This is interim for the adapter (Tasks 3–6 replace these with `query!`) but permanent for `fixture.rs`.

- [ ] **Step 4: Port the Migrator (#3383).** Check `run_migrations` in `lib.rs:47` (`sqlx::migrate::Migrator::new(dir).run(pool)`). If the 0.9 `Migrator::new`/`Migrate` API changed, adjust minimally to compile with identical behaviour. If unchanged, leave it.

- [ ] **Step 5: Build + clippy.** `buck2 build //src/control-plane/postgres:postgres && tools/clippy-all.sh 2>&1 | grep -i postgres`. Fix any other 0.9 break the compiler surfaces (e.g. `#3800` conn options, `#3541`), changing only what's needed for parity.

- [ ] **Step 6: Test + RE build.** `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres/...` (counts == Step 1). Then the RE build command. Both green.

- [ ] **Step 7: Format + commit.**
```bash
eval "$(./tools/env.sh)" && rustfmt --edition 2024 src/control-plane/postgres/src/*.rs
git add src/control-plane/postgres/Cargo.toml src/control-plane/postgres/src/ Cargo.lock third-party/BUCK
git commit -m "build(postgres): bump sqlx 0.8.6 -> 0.9.0 (runtime queries, AssertSqlSafe)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: `tools/sqlx-prepare.sh` + wire BUCK + migrate the `queue` concern (pipeline proof)

This task builds the generation harness and proves the whole offline pipeline end-to-end on real queries (queue has 6, incl. `pg_insert`).

**Files:** Create `tools/sqlx-prepare.sh`; modify `src/control-plane/postgres/BUCK`, `…/src/queue.rs`, `…/src/lib.rs` (the `row_to_job` helper); create `src/control-plane/postgres/.sqlx/` (generated).

- [ ] **Step 1: Write `tools/sqlx-prepare.sh`.** Behaviour (see spec "generation harness"):
  - Resolve repo root; `set -euo pipefail`.
  - Build/cache sqlx-cli: if `.loom/bin/sqlx` absent, `eval "$(./tools/env.sh)"` then `cargo install --root "$PWD/.loom" --version '^0.9' --no-default-features --features postgres,rustls sqlx-cli`.
  - Materialize pg: `BIN=$(buck2 build //src/control-plane/postgres:postgres-bin --show-output | awk '{print $2}')` (abs-ify with `$PWD/`); same for `:libxml2`; set `LD_LIBRARY_PATH=$BIN/lib:$XML`.
  - `DATA=$(mktemp -d)/pgdata; SOCK=$(mktemp -d)`; `"$BIN/bin/initdb" -D "$DATA" -U postgres --auth=trust`; start on a private port + socket with `listen_addresses=''`; `trap` to stop the cluster + `rm -rf` the temp dirs on exit.
  - `createdb` a `loom` db; apply migrations: `"$BIN/bin/psql" -h "$SOCK" -p <port> -U postgres -d loom -f` each file in `src/control-plane/postgres/migrations/*.sql` in sorted order (or run the loom `Migrator` — psql is simpler for a shell tool).
  - `export DATABASE_URL="postgres://postgres@localhost:<port>/loom?host=$SOCK"`; `export PATH="$PWD/.loom/bin:$PATH"`.
  - `cd src/control-plane/postgres && cargo sqlx prepare --check`-less prepare: `cargo sqlx prepare -- -p control-plane-postgres` (writes `.sqlx/`). Run with `SQLX_OFFLINE` unset.
  - Support `--check`: after preparing, `git diff --exit-code src/control-plane/postgres/.sqlx` and exit non-zero on drift.
  - `chmod +x tools/sqlx-prepare.sh`.

- [ ] **Step 2: Wire the BUCK env (cache empty for now).** In `src/control-plane/postgres/BUCK`, add before the `rust_library`:
```python
filegroup(
    name = "sqlx-cache",
    srcs = glob([".sqlx/**"]),
)
```
and add to the `rust_library`'s `env` (create the `env` attr):
```python
    env = {
        "CARGO_MANIFEST_DIR": "control_plane_postgres",
        "SQLX_OFFLINE": "true",
        "SQLX_OFFLINE_DIR": "$(location :sqlx-cache)/.sqlx",
    },
```

- [ ] **Step 3: Migrate `queue.rs` to `query!`.** Convert all 6 `sqlx::query(AssertSqlSafe("…")).bind(…)` calls (incl. `pg_insert`) to `sqlx::query!("…", arg1, arg2, …)` (args inline, no `.bind`). For row-returning queries (`dequeue`), map the macro's anonymous struct into `Job` (drop the call to `row_to_job`; inline the mapping — see spec example). Apply `as "col!"` nullability overrides where the build demands them (the `returning` columns from the `update`/`insert` of NOT-NULL columns should be provable, but `attempts`/expressions may need `!`). Remove now-unused `AssertSqlSafe` imports from queue.rs.

- [ ] **Step 4: Generate the cache.** `./tools/sqlx-prepare.sh`. Confirm `src/control-plane/postgres/.sqlx/query-*.json` appear (one per `query!`).

- [ ] **Step 5: Build offline (local + RE).** `env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/postgres:postgres` then the `--remote-only --no-remote-cache` build. Both must succeed — this proves the offline pipeline on real queries, on RE.

- [ ] **Step 6: Test.** `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres/...` — counts unchanged.

- [ ] **Step 7: `row_to_job` cleanup.** If `queue.rs` was the only user of `row_to_job` in `lib.rs`, remove it (clippy will flag it dead). If still used by an un-migrated concern, leave it (later tasks remove it).

- [ ] **Step 8: Format + commit** (include `tools/sqlx-prepare.sh`, the BUCK, `queue.rs`, `lib.rs`, and `.sqlx/`):
```bash
eval "$(./tools/env.sh)" && rustfmt --edition 2024 src/control-plane/postgres/src/*.rs
git add tools/sqlx-prepare.sh src/control-plane/postgres/
git commit -m "feat(postgres): compile-time query! for queue + sqlx-prepare harness

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Extend the harness for DuckLake + migrate `catalog.rs` (5 queries)

`catalog.rs` reads DuckDB's `ducklake_*` catalog tables (NOT loom migrations), so the
prepare harness must first stand up a real DuckLake catalog in the prepare pg.

**Files:** `tools/sqlx-prepare.sh` (extend), `…/src/catalog.rs`, `…/src/lib.rs` (`row_to_snapshot` + `resolve_table`), `.sqlx/`.

- [ ] **Step 1: Extend `tools/sqlx-prepare.sh` to create the DuckLake catalog.** After applying the loom migrations and BEFORE `cargo sqlx prepare`, run the pinned duckdb-cli to attach a DuckLake catalog backed by the prepare postgres (this creates the `ducklake_*` metadata tables in the `loom` db). Mirror `fixture.rs`'s `DuckLakeWriter`:
  ```sh
  DUCKDB="$PWD/$(env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/postgres:duckdb-cli --show-output 2>/dev/null | awk '{print $2}')"
  EXTDIR="$PWD/$(env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/postgres:duckdb-extensions --show-output 2>/dev/null | awk '{print $2}')"
  DLDATA="$(mktemp -d)"   # add to the cleanup trap's rm -rf
  "$DUCKDB" -c "SET extension_directory='$EXTDIR';
  LOAD ducklake; LOAD postgres_scanner;
  ATTACH 'ducklake:postgres:dbname=loom host=$SOCK user=postgres' AS lake (DATA_PATH '$DLDATA/', DATA_INLINING_ROW_LIMIT 0);"
  ```
  A bare attach creates `ducklake_snapshot/table/schema/data_file/column` (empty is fine — prepare only needs the columns to exist). Verify with `psql -c '\dt ducklake_*'` if debugging.

- [ ] **Step 2: Migrate `catalog.rs`.** Convert all 5 `sqlx::query(AssertSqlSafe(…))` to `query!`, mapping rows into `Snapshot`/`FileRef`/`TableSchema`/`ColumnDef` (inline the mapping; drop `row_to_snapshot` calls). `resolve_table` (returns a table id `i64`) → `query_scalar!`. DuckLake metadata columns will very likely need nullability overrides (`as "col!"`/`as "col?"`) — DuckLake's schema is permissive; the build error names each. Remove unused `AssertSqlSafe` imports.
- [ ] **Step 3:** `./tools/sqlx-prepare.sh` to regenerate `.sqlx` (now incl. the catalog queries validated against the real DuckLake schema).
- [ ] **Step 4:** Build (local + `--remote-only --no-remote-cache`) — both green.
- [ ] **Step 5:** `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres/...` — counts unchanged.
- [ ] **Step 6:** Remove `row_to_snapshot` from `lib.rs` if now unused (clippy).
- [ ] **Step 7:** Format + commit: `feat(postgres): DuckLake-aware sqlx-prepare + compile-time query! for catalog`.

---

## Task 4: Migrate `ontology.rs` (11 queries)

**Files:** `…/src/ontology.rs`, `.sqlx/`.

- [ ] **Step 1:** Convert all 11 to `query!`/`query_as!`/`query_scalar!`, mapping into `ObjectType`/`PropertyDef`/`LinkDef`/`TableRef`. `define_type`/`define_link` are multi-statement writes — each `sqlx::query(...)` becomes its own `query!`. Mind nullability on `select`ed columns. Remove unused `AssertSqlSafe`.
- [ ] **Step 2:** `./tools/sqlx-prepare.sh`.
- [ ] **Step 3:** Build local + RE.
- [ ] **Step 4:** Test — counts unchanged.
- [ ] **Step 5:** Format + commit: `feat(postgres): compile-time query! for ontology`.

---

## Task 5: Migrate `acl.rs` (14 queries)

**Files:** `…/src/acl.rs`, `.sqlx/`.

- [ ] **Step 1:** Convert all 14 to macros, mapping into the ACL domain types (`SubjectId`/`RoleId`/`Action`/`PolicyTarget`/`Policy`/`RowFilter`/`Decision`). `row_filter` is jsonb → `RowFilter` via `serde_json::Value` then `serde_json::from_value`, or a `as "row_filter: sqlx::types::Json<RowFilter>"` override — pick whichever matches the current decode and keeps behaviour identical. `check`/`policies_for` are reads (NOT instrumented, but still migrate the SQL). Remove unused `AssertSqlSafe`.
- [ ] **Step 2:** `./tools/sqlx-prepare.sh`.
- [ ] **Step 3:** Build local + RE.
- [ ] **Step 4:** Test — counts unchanged.
- [ ] **Step 5:** Format + commit: `feat(postgres): compile-time query! for acl`.

---

## Task 6: Migrate `lineage.rs` (4 queries, incl. `pg_emit`)

**Files:** `…/src/lineage.rs`, `…/src/lib.rs` (any remaining row helpers + the `event_type`/`cardinality` mapping free fns), `.sqlx/`.

- [ ] **Step 1:** Convert all 4 to macros (`emit`/`pg_emit`, `events_for`, `event_datasets`, `graph_step`). `payload` jsonb → `serde_json::Value`. `event_type` maps via the existing `event_type_to_str`/`_from_str` helpers — keep them (used for the enum<->text mapping) but feed them the macro's typed column. `inputs`/`outputs` (DatasetRef arrays) map as today. Nullability overrides as needed. Remove unused `AssertSqlSafe`.
- [ ] **Step 2:** `./tools/sqlx-prepare.sh`.
- [ ] **Step 3:** Build local + RE.
- [ ] **Step 4:** Test — counts unchanged.
- [ ] **Step 5:** Remove any now-dead row-mapping free fns from `lib.rs` (clippy). `transaction.rs` needs no change (it calls the migrated `pg_insert`/`pg_emit`).
- [ ] **Step 6:** Format + commit: `feat(postgres): compile-time query! for lineage`.

---

## Task 7: prek pre-push hook + docs

**Files:** `prek.toml`; `CLAUDE.md`.

- [ ] **Step 1:** Add a `sqlx-prepare` hook to `prek.toml` at the **pre-push** stage (next to `buck2-build`/`buck2-test`): a local hook that runs `./tools/sqlx-prepare.sh --check`. Match the existing local-hook syntax in `prek.toml`.
- [ ] **Step 2:** Verify the hook: `buck2 run //tools:prek -- run --hook-stage pre-push sqlx-prepare` (or the prek equivalent) — passes with no `.sqlx` diff.
- [ ] **Step 3:** Document in `CLAUDE.md`: a "Compile-time SQL" subsection under the postgres/dev-tools area — that the adapter uses `query!`, the `.sqlx` cache is generated by `tools/sqlx-prepare.sh` (which boots the pinned pg + applies migrations), the offline build wiring (`SQLX_OFFLINE_DIR` via `$(location)`), the pre-push hook, and that `fixture.rs` stays runtime. Keep it concise and in the file's voice.
- [ ] **Step 4:** Commit: `chore(postgres): pre-push sqlx-prepare hook + docs`.

---

## Task 8: Full verification

- [ ] **Step 1:** `git grep -n 'sqlx::query(' src/control-plane/postgres/src/{queue,catalog,ontology,acl,lineage}.rs` → empty (all migrated). `fixture.rs` retains its 2 `query_scalar` + `AssertSqlSafe`.
- [ ] **Step 2:** `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...` → all green, total count identical to before this effort.
- [ ] **Step 3:** Clean RE build: `env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/postgres:postgres --remote-only --no-remote-cache` → SUCCEEDED.
- [ ] **Step 4:** `buck2 run //tools:prek -- run --all-files` → green (incl. reindeer-in-sync). Then confirm the pre-push hook passes.
- [ ] **Step 5:** `git diff main --stat` sanity: only `postgres/` (Cargo.toml, BUCK, `src/*.rs`, `.sqlx/`), `tools/sqlx-prepare.sh`, `prek.toml`, `CLAUDE.md`, `Cargo.lock`, `third-party/BUCK` changed. `memory`/`core`/`testkit`/`worker` untouched.
