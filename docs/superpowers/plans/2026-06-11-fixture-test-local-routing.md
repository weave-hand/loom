# Fixture Test Local-Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every hermetic-fixture `rust_test` pin its test command to local execution (build stays on RE) via a shared `loom_fixture_test` macro, removing the `env -u BUCK_PREFER_REMOTE … --local-only` ceremony from CI and local dev.

**Architecture:** A new `src/control-plane/postgres/defs.bzl` defines `loom_fixture_test`, which wraps the prelude `rust_test` rule, injecting the shared fixture `env` block plus `remote_execution = "disabled"`. The 18 fixture-backed test targets across three BUCK files are rewritten to call it. Pure-logic tests stay as plain `rust_test` and keep running on RE. CI's test invocations drop the `--local-only` flag; `CLAUDE.md` is updated to match.

**Tech Stack:** buck2 (Starlark macros, `rust_test`, `remote_execution` attr from `re_test_common`), BuildBuddy RE, hermetic Postgres/DuckDB test fixtures.

**Reference:** spec at `docs/superpowers/specs/2026-06-11-fixture-test-local-routing-design.md`. The mechanism is spike-proven: `remote_execution = "disabled"` gave `:queue` a local-only run executor while its build ran on RE (`Commands: 2 (remote: 2, local: 0)`, test ran as non-root user).

**Conventions (loom-specific — read before starting):**
- Tests are `rust_test` integration targets only; never inline `#[cfg(test)]`.
- Run tests with `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...` **until Task 6** removes that need — but for verifying *this* change, deliberately run **`buck2 test`** with `BUCK_PREFER_REMOTE=true` and no `--local-only` to prove routing.
- **Never pipe `buck2 test` through `tail`** (it can stall). Redirect to a file and grep: `buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- `buck2 build … | tail` is fine.
- rustfmt is a **check-only** commit hook — it does not apply changes; there is no `.rs` in this plan so it is moot, but never use `--no-verify`.
- Commit messages: Conventional Commits, ending with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Work on branch `test/fixture-test-local-routing` (already created off `main`). Do NOT switch branches.

---

### Task 1: Create the `loom_fixture_test` macro and prove it on one target

This task de-risks the single unknown — whether `rust_test` is callable from a `.bzl` — by creating the macro and converting exactly one target (`:queue`) before the bulk rewrite.

**Files:**
- Create: `src/control-plane/postgres/defs.bzl`
- Modify: `src/control-plane/postgres/BUCK` (add a `load` line at top; convert the `queue` target at lines 144-156)

- [ ] **Step 1: Create the macro file**

Create `src/control-plane/postgres/defs.bzl`:

```python
# Shared macro for hermetic-fixture rust_test targets.
#
# Fixture tests boot real initdb/postgres/duckdb processes, which refuse to run
# as root. BuildBuddy RE runs as root, so these tests must run their test command
# LOCALLY. `remote_execution = "disabled"` gives the test a local-only run
# executor WITHOUT forcing its build off RE (buck2 separates build execution from
# test-run execution). Centralising it here means a new fixture test gets correct
# routing by construction — there is no separate lint to forget.
#
# The $(location //src/control-plane/postgres:...) labels are absolute, so this
# produces identical env whether called from postgres/, query-api/, or worker/.

def loom_fixture_test(
        name,
        crate,
        srcs,
        crate_root,
        deps,
        duckdb = False,
        edition = "2024",
        env = {},
        **kwargs):
    fixture_env = {
        "POSTGRES_BIN_DIR": "$(location //src/control-plane/postgres:postgres-bin)/bin",
        "POSTGRES_LD_LIBRARY_PATH": "$(location //src/control-plane/postgres:postgres-bin)/lib:$(location //src/control-plane/postgres:libxml2)",
        "LOOM_MIGRATIONS_DIR": "$(location //src/control-plane/postgres:migrations)/migrations",
    }
    if duckdb:
        fixture_env["DUCKDB_BIN"] = "$(location //src/control-plane/postgres:duckdb-cli)"
        fixture_env["DUCKDB_EXTENSION_DIR"] = "$(location //src/control-plane/postgres:duckdb-extensions)"
    fixture_env.update(env)
    rust_test(
        name = name,
        crate = crate,
        srcs = srcs,
        crate_root = crate_root,
        edition = edition,
        env = fixture_env,
        remote_execution = "disabled",
        deps = deps,
        **kwargs
    )
```

- [ ] **Step 2: Add the load line to `src/control-plane/postgres/BUCK`**

At the very top of the file (line 1, before the first `http_archive`), add:

```python
load(":defs.bzl", "loom_fixture_test")
```

- [ ] **Step 3: Convert the `queue` target**

Replace the `queue` `rust_test` block (currently lines 144-156) with:

```python
loom_fixture_test(
    name = "queue",
    crate = "queue",
    srcs = ["tests/queue.rs"],
    crate_root = "tests/queue.rs",
    deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

- [ ] **Step 4: Verify the macro resolves and the target builds**

Run: `buck2 build //src/control-plane/postgres:queue 2>&1 | tail -20`

Expected: builds successfully (the binary, on RE or locally — either is fine for a build).

**If it fails with `name 'rust_test' is not defined`:** the prelude rules are not auto-global in this `.bzl`. Fix by prefixing the call in `defs.bzl` with `native.`: change `rust_test(` to `native.rust_test(`. Re-run Step 4. (Only one of bare or `native.` will be correct; keep whichever builds.)

- [ ] **Step 5: Prove the test routes local with RE preferred**

Run: `BUCK_PREFER_REMOTE=true buck2 test //src/control-plane/postgres:queue > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|owned by user|Commands:" /tmp/t1.log`

Expected:
- `Tests finished: Pass 1.` (the target reports one test entry; internally 3 contract tests pass)
- A line containing `owned by user "<your-non-root-user>"` (initdb ran as non-root — proves local execution)
- `Commands: N (… remote: …, local: …)` with the test command local

This reproduces the spike. If `initdb` fails with a root error, routing did not take effect — stop and report.

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/defs.bzl src/control-plane/postgres/BUCK
git commit -m "$(cat <<'EOF'
test(postgres): loom_fixture_test macro pins fixture test runs local

remote_execution = "disabled" gives the test a local-only run executor (build
stays on RE), so initdb/postgres/duckdb run as non-root without --local-only.
Converts :queue as the first user; remaining targets follow.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Convert the remaining PG-only postgres targets

**Files:**
- Modify: `src/control-plane/postgres/BUCK` (targets `ontology`, `acl`, `lineage`, `tx`, `lineage-roundtrip`)

These five have the PG-only env (no DuckDB). `lineage-roundtrip` carries extra third-party deps — preserve its full `deps` list.

- [ ] **Step 1: Convert `ontology`, `acl`, `lineage`, `tx`**

Each currently is a `rust_test` block with the PG-only env and `deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"]`. Replace each block with the macro call (substituting `<name>` and `<name>.rs` per target — `ontology`, `acl`, `lineage`, `tx`; crate names equal the target names):

```python
loom_fixture_test(
    name = "ontology",
    crate = "ontology",
    srcs = ["tests/ontology.rs"],
    crate_root = "tests/ontology.rs",
    deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

```python
loom_fixture_test(
    name = "acl",
    crate = "acl",
    srcs = ["tests/acl.rs"],
    crate_root = "tests/acl.rs",
    deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

```python
loom_fixture_test(
    name = "lineage",
    crate = "lineage",
    srcs = ["tests/lineage.rs"],
    crate_root = "tests/lineage.rs",
    deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

```python
loom_fixture_test(
    name = "tx",
    crate = "tx",
    srcs = ["tests/tx.rs"],
    crate_root = "tests/tx.rs",
    deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

- [ ] **Step 2: Convert `lineage-roundtrip`** (PG-only env, but a longer deps list — copy it exactly)

Replace the `lineage-roundtrip` block with:

```python
loom_fixture_test(
    name = "lineage-roundtrip",
    crate = "lineage_roundtrip",
    srcs = ["tests/lineage_roundtrip.rs"],
    crate_root = "tests/lineage_roundtrip.rs",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:proptest",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 3: Build all five**

Run: `buck2 build //src/control-plane/postgres:ontology //src/control-plane/postgres:acl //src/control-plane/postgres:lineage //src/control-plane/postgres:tx //src/control-plane/postgres:lineage-roundtrip 2>&1 | tail -5`

Expected: all build successfully.

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/postgres/BUCK
git commit -m "$(cat <<'EOF'
test(postgres): route PG-only fixture tests local via loom_fixture_test

Converts ontology, acl, lineage, tx, lineage-roundtrip.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Convert the DuckDB-backed postgres targets

**Files:**
- Modify: `src/control-plane/postgres/BUCK` (targets `ducklake-smoke`, `snapshot-append`, `snapshot-create`, `ducklake-interop`, `snapshot-rollback`, `snapshot-conformance`, `catalog`, `sqlx-cache-check`)

All pass `duckdb = True`. `sqlx-cache-check` additionally needs `LOOM_SQLX_DIR` via the `env` extra.

- [ ] **Step 1: Convert the seven plain DuckDB targets**

Replace each block with its macro call. Preserve each target's exact `deps`:

```python
loom_fixture_test(
    name = "ducklake-smoke",
    crate = "ducklake_smoke",
    srcs = ["tests/ducklake_smoke.rs"],
    crate_root = "tests/ducklake_smoke.rs",
    duckdb = True,
    deps = [":postgres", "//third-party:tokio"],
)
```

```python
loom_fixture_test(
    name = "snapshot-append",
    crate = "snapshot_append",
    srcs = ["tests/snapshot_append.rs"],
    crate_root = "tests/snapshot_append.rs",
    duckdb = True,
    deps = [":postgres", "//src/control-plane/core:core", "//third-party:tokio"],
)
```

```python
loom_fixture_test(
    name = "snapshot-create",
    crate = "snapshot_create",
    srcs = ["tests/snapshot_create.rs"],
    crate_root = "tests/snapshot_create.rs",
    duckdb = True,
    deps = [":postgres", "//src/control-plane/core:core", "//third-party:tokio"],
)
```

```python
loom_fixture_test(
    name = "ducklake-interop",
    crate = "ducklake_interop",
    srcs = ["tests/ducklake_interop.rs"],
    crate_root = "tests/ducklake_interop.rs",
    duckdb = True,
    deps = [":postgres", "//src/control-plane/core:core", "//third-party:tokio"],
)
```

```python
loom_fixture_test(
    name = "snapshot-rollback",
    crate = "snapshot_rollback",
    srcs = ["tests/snapshot_rollback.rs"],
    crate_root = "tests/snapshot_rollback.rs",
    duckdb = True,
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

```python
loom_fixture_test(
    name = "snapshot-conformance",
    crate = "snapshot_conformance",
    srcs = ["tests/snapshot_conformance.rs"],
    crate_root = "tests/snapshot_conformance.rs",
    duckdb = True,
    deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

```python
loom_fixture_test(
    name = "catalog",
    crate = "catalog",
    srcs = ["tests/catalog.rs"],
    crate_root = "tests/catalog.rs",
    duckdb = True,
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//src/control-plane/testkit:testkit",
        "//third-party:async-trait",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Convert `sqlx-cache-check`** (DuckDB + the `LOOM_SQLX_DIR` extra)

Replace its block with:

```python
loom_fixture_test(
    name = "sqlx-cache-check",
    crate = "sqlx_cache",
    srcs = ["tests/sqlx_cache.rs"],
    crate_root = "tests/sqlx_cache.rs",
    duckdb = True,
    env = {"LOOM_SQLX_DIR": "$(location :sqlx-cache)/.sqlx"},
    deps = [
        ":postgres",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tokio",
    ],
)
```

Note: `:sqlx-cache` is package-relative and resolves correctly because this call lives in `postgres/BUCK`.

- [ ] **Step 3: Build all eight**

Run: `buck2 build //src/control-plane/postgres:ducklake-smoke //src/control-plane/postgres:snapshot-append //src/control-plane/postgres:snapshot-create //src/control-plane/postgres:ducklake-interop //src/control-plane/postgres:snapshot-rollback //src/control-plane/postgres:snapshot-conformance //src/control-plane/postgres:catalog //src/control-plane/postgres:sqlx-cache-check 2>&1 | tail -5`

Expected: all build successfully.

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/postgres/BUCK
git commit -m "$(cat <<'EOF'
test(postgres): route DuckDB-backed fixture tests local via loom_fixture_test

Converts ducklake-smoke, snapshot-{append,create,rollback,conformance},
ducklake-interop, catalog, and sqlx-cache-check (duckdb + LOOM_SQLX_DIR).

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Convert the query-api fixture targets

**Files:**
- Modify: `src/services/query-api/BUCK` (add a `load` line; convert `serving-engine`, `governed-read`, `spike-duckdb`)

These reference postgres targets by absolute path; the macro already uses absolute labels, so the env is produced identically. All three are `duckdb = True`. Leave `http-smoke` and `sql-compile` as plain `rust_test`.

- [ ] **Step 1: Add the load line**

At the top of `src/services/query-api/BUCK` (line 1), add:

```python
load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")
```

- [ ] **Step 2: Convert the three targets**

```python
loom_fixture_test(
    name = "serving-engine",
    crate = "serving_engine",
    srcs = ["tests/serving_engine.rs"],
    crate_root = "tests/serving_engine.rs",
    duckdb = True,
    deps = [
        ":query-api",
        "//src/control-plane/postgres:postgres",
        "//third-party:tokio",
    ],
)
```

```python
loom_fixture_test(
    name = "governed-read",
    crate = "governed_read",
    srcs = ["tests/governed_read.rs"],
    crate_root = "tests/governed_read.rs",
    duckdb = True,
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:tokio",
    ],
)
```

```python
loom_fixture_test(
    name = "spike-duckdb",
    crate = "spike_duckdb",
    srcs = ["tests/spike_duckdb.rs"],
    crate_root = "tests/spike_duckdb.rs",
    duckdb = True,
    deps = [
        "//src/control-plane/postgres:postgres",
        "//third-party:duckdb",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Build all three**

Run: `buck2 build //src/services/query-api:serving-engine //src/services/query-api:governed-read //src/services/query-api:spike-duckdb 2>&1 | tail -5`

Expected: all build successfully (this also confirms cross-package `.bzl` load works).

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api/BUCK
git commit -m "$(cat <<'EOF'
test(query-api): route DuckDB fixture tests local via loom_fixture_test

Converts serving-engine, governed-read, spike-duckdb (cross-package load of
//src/control-plane/postgres:defs.bzl). sql-compile/http-smoke stay on RE.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Convert the worker fixture target

**Files:**
- Modify: `src/control-plane/worker/BUCK` (add a `load` line; convert `postgres-integration`)

PG-only. Leave `worker_test` as plain `rust_test`.

- [ ] **Step 1: Add the load line**

At the top of `src/control-plane/worker/BUCK` (line 1), add:

```python
load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")
```

- [ ] **Step 2: Convert `postgres-integration`**

```python
loom_fixture_test(
    name = "postgres-integration",
    crate = "worker_postgres_integration",
    srcs = ["tests/postgres.rs"],
    crate_root = "tests/postgres.rs",
    deps = [
        ":worker",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:serde_json",
        "//third-party:tokio",
        "//third-party:tokio-util",
    ],
)
```

- [ ] **Step 3: Build it**

Run: `buck2 build //src/control-plane/worker:postgres-integration 2>&1 | tail -5`

Expected: builds successfully.

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/worker/BUCK
git commit -m "$(cat <<'EOF'
test(worker): route postgres-integration local via loom_fixture_test

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Drop the `--local-only` ceremony from CI

**Files:**
- Modify: `.github/workflows/ci.yml` (the `build-test` job test step and the `affected` job test step, plus their explanatory comments)

- [ ] **Step 1: Re-read the two test invocations and their comments**

Run: `grep -n "local-only\|BUCK_PREFER_REMOTE\|buck2 test" .github/workflows/ci.yml`

Identify the two `env -u BUCK_PREFER_REMOTE buck2 test --local-only …` lines (build-test job ~line 61, affected job ~line 135) and the comment blocks above them.

- [ ] **Step 2: Simplify the `build-test` job test step**

Replace the comment block + `run:` line for the full-suite test (currently the lines explaining `--local-only`/`env -u` followed by `run: env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...`) with:

```yaml
        # Fixture-backed tests pin their own run local via loom_fixture_test
        # (remote_execution = "disabled"); pure-logic tests run on RE. No
        # --local-only needed.
        run: buck2 test //src/...
```

- [ ] **Step 3: Simplify the `affected` job test step**

Replace the corresponding comment + `env -u BUCK_PREFER_REMOTE buck2 test --local-only "${targets[@]}"` line with:

```yaml
          # Fixture tests self-pin local via loom_fixture_test; no --local-only.
          buck2 test "${targets[@]}"
```

Leave the `buck2 build -M none …` lines and the global `BUCK_PREFER_REMOTE: "true"` env untouched.

- [ ] **Step 4: Sanity-check the YAML**

Run: `grep -n "buck2 test\|local-only" .github/workflows/ci.yml`

Expected: two plain `buck2 test …` invocations; **zero** `--local-only` occurrences.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "$(cat <<'EOF'
ci: drop --local-only; fixture tests self-route local

loom_fixture_test pins fixture test runs local via remote_execution="disabled",
so the full suite runs with a plain `buck2 test` — logic tests on RE, fixtures
local. Removes the `env -u BUCK_PREFER_REMOTE … --local-only` workaround.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md` (the "## Testing" section and any other `--local-only` mention)

- [ ] **Step 1: Find every `--local-only` / `env -u BUCK_PREFER_REMOTE` mention**

Run: `grep -n "local-only\|BUCK_PREFER_REMOTE\|loom_fixture_test" CLAUDE.md`

- [ ] **Step 2: Update the "Run the suite" bullet in the Testing section**

Replace the current run-the-suite text (`env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...` and its mandatory-flag explanation) with:

```markdown
- **Run the suite:** `buck2 test //src/...`. Fixture-backed tests (hermetic
  Postgres/DuckDB) pin their own run to local execution via the
  **`loom_fixture_test`** macro (`src/control-plane/postgres/defs.bzl`, which sets
  `remote_execution = "disabled"`) — they boot `initdb`/`postgres`/`duckdb`, which
  refuse to run as root on RE, so only the *test command* runs local while the
  build stays on RE. Pure-logic tests run on RE. `--local-only` still works as a
  manual override but is no longer required. **New fixture tests must use
  `loom_fixture_test`, not a bare `rust_test`**, or they will route to RE and fail
  as root.
```

- [ ] **Step 3: Reword any other `--local-only` references**

For each remaining hit (e.g. the `sqlx-cache-check` description, the "Don't pipe `buck2 test` through `tail`" bullet if it embeds the flag), drop the mandatory `env -u … --local-only` framing and reference plain `buck2 test`. Keep the "don't pipe to tail" guidance itself. Do not invent new claims — only adjust the command shown.

- [ ] **Step 4: Verify no stale mandatory-flag wording remains**

Run: `grep -n "mandatory\|must.*local-only\|refuse to run as root" CLAUDE.md`

Expected: the only "refuse to run as root" mention is the new one in the Testing bullet; no remaining text calls `--local-only` mandatory.

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md
git commit -m "$(cat <<'EOF'
docs: CLAUDE.md — fixture tests self-route local via loom_fixture_test

Plain `buck2 test //src/...` now works; the macro is the single source of truth
for fixture-test routing. --local-only is an optional override.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Full-suite verification

Prove the end-state: plain `buck2 test //src/...` passes with fixtures local and logic tests RE-eligible, under both env conditions.

**Files:** none (verification only).

- [ ] **Step 1: Full suite with RE preferred (the real CI condition)**

Run: `BUCK_PREFER_REMOTE=true buck2 test //src/... > /tmp/full_re.log 2>&1; grep -E "Tests finished|FAIL|BUILD FAILED" /tmp/full_re.log`

Expected: `Tests finished: Pass <N>. Fail 0. … Build failure 0` — all targets pass, no build failures. (Do not pipe to `tail`; this log can be large — grep it.)

- [ ] **Step 2: Full suite with RE unset (local dev condition)**

Run: `env -u BUCK_PREFER_REMOTE buck2 test //src/... > /tmp/full_local.log 2>&1; grep -E "Tests finished|FAIL|BUILD FAILED" /tmp/full_local.log`

Expected: same — all pass.

- [ ] **Step 3: Confirm a fixture test ran as non-root under Step 1**

Run: `grep -m1 "owned by user" /tmp/full_re.log`

Expected: a line naming your non-root user (proves fixtures ran local even with `BUCK_PREFER_REMOTE=true`). If empty, re-check that the relevant fixture target actually ran (it may have been cached); force it: `BUCK_PREFER_REMOTE=true buck2 test //src/control-plane/postgres:queue --no-cache 2>&1 | grep "owned by user"`.

- [ ] **Step 4: Lint clean**

Run: `tools/clippy-all.sh 2>&1 | tail -5` and `buck2 run //tools:prek -- run --all-files 2>&1 | tail -20`

Expected: clippy clean; all prek hooks pass (BUCK files are not rustfmt/clippy targets, but the file-hygiene hooks run).

- [ ] **Step 5: No further commit needed** (verification only). If any lint hook auto-fixed whitespace, `git add -A && git commit` it with `chore: lint`.

---

## Notes for the implementer

- **Do not** create the `constraints/` directory or touch `platforms/defs.bzl` — that was rejected spike scaffolding (see spec "Rejected alternative").
- **Do not** weaken or skip any test. If a converted target fails to build or run, the macro call is wrong (most likely a mismatched `deps` list or a missing `duckdb = True`) — fix the call, not the test.
- The only genuinely uncertain step is Task 1 Step 4 (`rust_test` vs `native.rust_test` in a `.bzl`). Everything after is mechanical and gated by a per-task build.
