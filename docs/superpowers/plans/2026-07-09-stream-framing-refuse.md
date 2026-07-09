# Refuse Stream-Table Targets on Legacy Transform Write Paths — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the latent framing-corruption class on the two framing-unaware write paths — `iceberg_landing::write_steps` and the `IcebergTx` transform-commit seam — by **refusing** stream/CDC-registered target tables (including CDC changelog tables) with a typed `Validation` error, at define time (transform admin UX) and at commit time (the hard gate). Framing is NOT threaded (operator decision 2026-07-09); `EngineControl::CommitMicroBatch` (slice 4) stays the sole sanctioned stream-output path.

**Architecture:** One shared postgres helper `pg_refuse_stream_target` (mirror-id resolve + one `stream.stream_table` lookup covering `table_id` OR `changelog_table_id`) is called inside both commit transactions and from the postgres `define_transform`. Four small error-mapping fixes carry the refusal as `invalid_argument` over the engine wire so the worker abandons (never retries) and a multi-step action surfaces HTTP 422. Batch/log/CDC tables on their sanctioned paths stay byte-identical.

**Spec:** `docs/superpowers/specs/2026-07-09-stream-framing-refuse-design.md` (read it first — it carries the verified guard-site refs and the decision record).

**Tech Stack:** Rust, sqlx (runtime `AssertSqlSafe` only — no `.sqlx` regen), tonic/gRPC, buck2 `loom_fixture_test`/`rust_test`.

## Global Constraints

Carried from the spec and CLAUDE.md; every task implicitly includes these:

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New fixture tests MUST use `loom_fixture_test` (`src/control-plane/postgres/defs.bzl`) with a BUCK target mirroring a named sibling, or the fixture env (PG binaries, MinIO, boot-slot dir) is missing. The `no-inline-tests` prek hook fails on any inline `#[test]`.
- **No compile-time sqlx for the new SQL.** The one new query in `pg_refuse_stream_target` uses runtime `sqlx::query_scalar(AssertSqlSafe(...))`, mirroring `version_for_table` — cloud sessions cannot run `tools/sqlx-prepare.sh` (initdb-as-root). Do not touch existing `query!` sites.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/`indexing_slicing`/`panic`/`todo`/`map_err_ignore` in production lib/bin code. `#[expect(lint, reason = "...")]` for justified local exceptions. Test code is exempted from the panic-safety lints via the test macros.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files` (`git add` new files FIRST — prek skips untracked files). Markdown ends with exactly one trailing newline, no trailing whitespace.
- **Byte-identical for non-stream targets.** Every guard is a no-op for a table with no `stream.stream_table` hit. Existing suites (`transform-e2e`, `typed-transform-e2e`, `data-triggers`, `multi-step-run`, `action-multi-object-e2e`, `write-steps-e2e`, the `stream-*` family, transforms testkit contracts) must stay green **unmodified**.
- **Build/test commands:** build `buck2 build -v0 --console none //src/...`; test `buck2 test --console none <targets>` (cloud: add `-M none` to builds, scope tests to touched targets, `buck2 clean` between heavy phases; a full local suite needs `-j 8`).
- **Refusal message contract:** every refusal message starts `stream-table target refused:` (stable, matchable — tests assert on the prefix, not the whole string).

---

## File Structure

**Create:**
- `src/control-plane/postgres/tests/stream_refuse_targets.rs` — commit-time guard fixture tests (`write_steps` + `IcebergTx`).
- `src/control-plane/postgres/tests/transform_stream_target_define.rs` — define-time guard fixture tests.
- `src/services/worker/tests/transform_stream_refuse_e2e.rs` — worker→engine e2e: commit refused, job abandons.

**Modify (production):**
- `src/control-plane/postgres/src/stream.rs` — `pg_refuse_stream_target` helper.
- `src/control-plane/postgres/src/iceberg_landing.rs` — guard in `write_steps` Phase-2 tx (`:571-584`).
- `src/control-plane/postgres/src/iceberg_control_plane.rs` — guard in `IcebergTx::commit` (`:146-164`).
- `src/control-plane/postgres/src/transforms.rs` — define-time check in `define_transform` (`:298-378`).
- `src/services/engine/src/service.rs` — `status()` gains `Validation → invalid_argument` (`:21-28`); `write_steps` handler narrow-maps the new serving variant (`:412-416`).
- `src/services/engine/src/flight.rs` — `serving_status` arm for the new variant (`:38-47`).
- `src/services/engine-serving/src/serving.rs` — `EngineServingError::Validation(String)` variant (`:36-60`).
- `src/services/engine-serving/src/action_writer.rs` — `write_steps` maps `ControlPlaneError::Validation` to it (`:138-140`).
- `src/services/worker/src/transform.rs` — commit step abandons on `Validation` (`:360-365`).
- `src/services/query-api/src/serving.rs` — `ServingError::Unsupported(String)` variant (`:76-97`).
- `src/services/query-api/src/engine_action_client.rs` — `to_serving_write` arm (`:24-29`).
- `src/services/query-api/src/action.rs` — `run_multi_step` maps `ServingError::Unsupported → ActionError::Unsupported` at the `write_steps` call (`:1415`).

**Modify (tests/BUCK/docs):**
- `src/control-plane/postgres/BUCK`, `src/services/worker/BUCK` — new test targets.
- `src/services/query-api/tests/write_steps_e2e.rs` — wire-level refusal case (existing target).
- `docs/ROADMAP.md` — remove `#road-stream-framing-write-paths` (close).
- `docs/system-capabilities/stream.md` — document the refusal; drop the stale `#fut-stream-framing-write-paths` known-gap bullet.

---

## Task 1: `pg_refuse_stream_target` + guard in `iceberg_landing::write_steps`

**Files:**
- Modify: `src/control-plane/postgres/src/stream.rs` (helper, after `pg_set_changelog_table_id`, `:418`)
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs:571-584` (Phase-2 tx)
- Test: `src/control-plane/postgres/tests/stream_refuse_targets.rs` (new) + `src/control-plane/postgres/BUCK`

**Interfaces:**
- Consumes: `live_table_id` (`iceberg_mirror.rs:264`), `stream.stream_table` (incl. `kind`, `changelog_table_id`).
- Produces (later tasks rely on this EXACT name): `pub(crate) async fn pg_refuse_stream_target(conn: &mut sqlx::PgConnection, table: &TableRef) -> Result<()>`.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/stream_refuse_targets.rs`, mirroring `stream_overwrite_framing.rs`'s harness exactly (`PgFixture::shared` / `local_sql_catalog` / `ensure_table` + `declare_cdc` / `declare_stream` seeding; its `id_spec`/`id_batch`/`lin` helpers). Cases for the `write_steps` seam (drive `iceberg_landing::{write_steps, StepLand}` directly, as `data_triggers.rs:262` does):

1. **CDC target refused** — declare `("s","cdc_base")` CDC (bucket_count 1, `MergeEngine::LastRow`, exactly as `stream_overwrite_framing.rs:100-108`); `write_steps` with one `StepLand { table: cdc_base, overwrite: false, .. }` ⇒ `Err(ControlPlaneError::Validation(m))` with `m.starts_with("stream-table target refused:")`.
2. **Log target refused** — `ensure_table` + `cp.declare_stream(tid, 2)` on `("s","log_t")`; same shape ⇒ `Err(Validation)`.
3. **Changelog target refused** — after case 1's CDC declare (which registers `s.cdc_base__changelog` and points `changelog_table_id` at it — seed via `inline_append` + `land_cdc`-style declare if the bare `declare_cdc` does not create the changelog row: simplest is to mirror `stream_cdc_declare.rs`'s `land_cdc` harness for this one case), `write_steps` targeting `TableRef { schema: "s", name: "cdc_base__changelog" }` ⇒ `Err(Validation)`.
4. **Atomicity** — `write_steps` with TWO steps: `("s","plain")` (batch) + the CDC table ⇒ `Err(Validation)` AND `live_table_id` for `s.plain` shows no live files / the table has no committed snapshot from this call (assert via `iceberg_mirror` queries or `IcebergCatalog::files_with_stats` emptiness) — nothing landed.
5. **Batch tables unaffected** — `write_steps` with two plain-table steps ⇒ `Ok(snapshot)`, exactly as `data_triggers.rs:232` proves today.

Add a `loom_fixture_test` target `stream-refuse-targets` to `src/control-plane/postgres/BUCK` mirroring `stream-overwrite-framing` (`BUCK:1339`), deps mirroring it plus `loom-test-seed` if not present.

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:stream-refuse-targets`
Expected: FAIL — `write_steps` currently commits against the stream table (cases 1-4 get `Ok`).

- [ ] **Step 3: Add the helper**

In `src/control-plane/postgres/src/stream.rs`, after `pg_set_changelog_table_id`:

```rust
/// Refuse `table` as the target of a legacy (framing-unaware) write path —
/// the multi-target `write_steps` commit and the `IcebergTx` transform seam —
/// when it is a declared stream table (log or cdc) OR a CDC changelog table
/// (pointed at by a base row's `changelog_table_id`; the changelog has no
/// registry row of its own). Those paths register framing-free column sets,
/// which would silently strand a stream table's rows without
/// `loom_bucket`/`loom_offset` (pre-first-flush) or fail as an opaque
/// dropped-column schema change (post-flush). Stream tables are written via
/// the framing-aware paths only (inline/flush/land/overwrite/write_delta;
/// `CommitMicroBatch` once road-stream-continuous lands). A table with no
/// live mirror row passes — it cannot be stream-declared. See
/// `docs/superpowers/specs/2026-07-09-stream-framing-refuse-design.md`.
pub(crate) async fn pg_refuse_stream_target(
    conn: &mut sqlx::PgConnection,
    table: &TableRef,
) -> Result<()> {
    let Some(tid) =
        crate::iceberg_mirror::live_table_id(conn, &table.schema, &table.name).await?
    else {
        return Ok(());
    };
    // AssertSqlSafe: static query; sqlx regen unavailable in cloud sessions
    // (initdb-as-root) — mirrors `version_for_table` (ontology.rs:683).
    let hit: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(
        "select case when table_id = $1 then kind else 'changelog' end \
         from stream.stream_table \
         where table_id = $1 or changelog_table_id = $1 limit 1",
    ))
    .bind(tid)
    .fetch_optional(&mut *conn)
    .await
    .map_err(backend)?;
    if let Some(kind) = hit {
        return Err(ControlPlaneError::Validation(format!(
            "stream-table target refused: {}.{} is a declared stream table \
             (kind={kind}); this write path does not carry loom_ framing — \
             write it via the stream-aware paths instead",
            table.schema, table.name
        )));
    }
    Ok(())
}
```

(`stream.rs` has no `AssertSqlSafe` import today — either fully-qualify as above or add `use sqlx::AssertSqlSafe;`, matching `iceberg_mirror.rs:15`.)

- [ ] **Step 4: Guard `write_steps`**

In `src/control-plane/postgres/src/iceberg_landing.rs`, inside Phase 2's transaction (after `let at = next_snapshot(...)` at `:572`, before the `register_files` loop at `:573`):

```rust
    // Legacy multi-target writes are framing-unaware: refuse any staged target
    // that is a declared stream/CDC (or changelog) table, atomically with the
    // whole commit — if ANY step is refused, NOTHING lands. See
    // 2026-07-09-stream-framing-refuse-design.
    for s in &staged {
        crate::stream::pg_refuse_stream_target(&mut tx, &s.table).await?;
    }
```

Also update the stale comment at `:555` (`// Multi-target writes don't carry stream framing (out of this slice's scope).`) to say stream targets are refused at commit (Phase 2) rather than "out of scope".

- [ ] **Step 5: Run the test + non-regression**

Run: `buck2 test --console none //src/control-plane/postgres:stream-refuse-targets //src/control-plane/postgres:data-triggers`
Expected: PASS (case 5 + the `data_triggers` `write_steps` leg prove batch behavior unchanged).

- [ ] **Step 6: prek + commit**

```bash
git add -A
buck2 run //tools:prek -- run --all-files
git commit -m "feat(stream): refuse stream-table targets in write_steps

Add pg_refuse_stream_target (registry lookup incl. changelog_table_id) and
call it per staged target inside write_steps' commit tx — a multi-target
write touching a declared stream/CDC/changelog table refuses atomically
(nothing lands) with a typed Validation error. Batch targets byte-identical."
```

---

## Task 2: Guard the `IcebergTx` transform-commit seam

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_control_plane.rs:146-164`
- Test: extend `src/control-plane/postgres/tests/stream_refuse_targets.rs`

- [ ] **Step 1: Extend the failing test**

Add to `stream_refuse_targets.rs` (same harness), driving the seam exactly as the engine's `CommitTransform` handler does (`engine/src/service.rs:301-317`):

1. **Transform append refused** — build `IcebergControlPlane::new(cp, catalog)`, `begin_table()`, `create_table(&cdc_base, &cols)` + `append_files(&cdc_base, &[file])` (mirror the `DataFile` literal from testkit's `replace_files` contract, `testkit/src/lib.rs:4202-4250`), `commit()` ⇒ `Err(Validation)` with the prefix; assert no new snapshot registered for the table.
2. **Transform replace refused** — same with `replace_files` ⇒ `Err(Validation)`.
3. **Batch transform commit unaffected** — same sequence against `("s","plain2")` ⇒ `Ok(Some(snapshot))`.

Note: `IcebergControlPlane`/`begin_table` need the `TableControlPlane`/`TableTx` traits in scope (`control_plane_core::{TableControlPlane, TableTx, Tx}`), and constructing it consumes a `PgControlPlane` — build a second one from the fixture pool if the test already used `cp` (mirror how `write_steps_e2e.rs` builds its engine-side control plane).

Run: `buck2 test --console none //src/control-plane/postgres:stream-refuse-targets` — expected FAIL on the two new cases.

- [ ] **Step 2: Guard `IcebergTx::commit`**

In `iceberg_control_plane.rs`, inside the held tx — after `let at = next_snapshot(&mut tx, None)` (`:147`), before the `staged_files` register loop (`:151`):

```rust
        // Transform commits are framing-unaware (staged creates pass
        // include_framing=false; registers carry framing-free columns): refuse
        // any staged-files target that is a declared stream/CDC/changelog
        // table. staged_compacts are exempt — compaction is schema-invariant.
        for (table, _, _) in &staged_files {
            crate::stream::pg_refuse_stream_target(&mut tx, table).await?;
        }
```

Also update the stale comment at `:142-143` ("Transform-committed tables don't carry stream framing (out of this slice's scope…") to reference the refusal.

- [ ] **Step 3: Run tests + the testkit transaction contracts**

Run: `buck2 test --console none //src/control-plane/postgres:stream-refuse-targets`
Then the existing seam consumers: `buck2 test --console none //src/services/query-api:write-steps-e2e //src/services/worker:transform-e2e //src/services/worker:typed-transform-e2e` — expected PASS unmodified (their outputs are batch tables). Also run the postgres transaction-contract target that exercises `replace_files` (find it via `grep -rn "replace_files" src/control-plane/postgres/BUCK` / the testkit contract's wiring) — PASS.

- [ ] **Step 4: prek + commit**

```bash
git add -A
buck2 run //tools:prek -- run --all-files
git commit -m "feat(stream): refuse stream-table targets on the IcebergTx transform seam

CommitTransform's staging tx (create_table + append/replace_files ->
register_files) now refuses declared stream/CDC/changelog targets inside the
held tx, before any register. Compaction staging is exempt
(schema-invariant). Batch transform commits byte-identical."
```

---

## Task 3: Wire error contract — engine `invalid_argument`, worker abandons; transform-refusal e2e

**Files:**
- Modify: `src/services/engine/src/service.rs:21-28` (`status()`)
- Modify: `src/services/worker/src/transform.rs:360-365` (commit error match)
- Test: `src/services/worker/tests/transform_stream_refuse_e2e.rs` (new) + `src/services/worker/BUCK`

- [ ] **Step 1: Write the failing e2e**

Create `src/services/worker/tests/transform_stream_refuse_e2e.rs`, mirroring `worker/tests/transform_e2e.rs`'s harness (`loom_test_flight::spawn_engine_uds`, `loom_test_seed::local_sql_catalog`, the `TransformCtx` construction and job-payload shape):

1. Seed a batch input table with rows (as `transform_e2e` does).
2. Create + declare the OUTPUT table `("s","stream_out")` as CDC (`ensure_table` + `declare_cdc(tid, 1, "id", MergeEngine::LastRow)` — the `stream_overwrite_framing.rs:100-108` shape) **before** running the job. (Define-time rejection is Task 4 and does not apply here — this drives `handle_transform` directly with a job payload, the ad-hoc path that skips `define_transform`.)
3. Build the `TransformJob` payload targeting `stream_out`, call `handle_transform(&ctx, job)`.
4. Assert `Err(JobFailure)` that is an **abandon** (not a retry — assert on `JobFailure`'s shape/accessors as `transform_e2e`'s failure cases do) and that the message contains `stream-table target refused:`.
5. Assert the stream table is untouched: no live data files registered for `stream_out` (via the fixture pool: `iceberg_mirror.data_file` empty for its tid).
6. Non-regression twin: the same job against a plain output table succeeds (or rely on `transform-e2e` for this — then this file carries only the refusal case; prefer relying on the existing suite to keep the new file focused).

Add a `loom_fixture_test` target `transform-stream-refuse-e2e` to `src/services/worker/BUCK` mirroring `transform-e2e` (`BUCK:185-188`).

Run: `buck2 test --console none //src/services/worker:transform-stream-refuse-e2e`
Expected: FAIL — today the refusal comes back as `Status::internal` and the worker wraps it in `JobFailure::retry` (the test's abandon assertion fails).

- [ ] **Step 2: Map `Validation` on the engine control wire**

In `src/services/engine/src/service.rs`, `status()` (`:21-28`), add before the catch-all:

```rust
        Validation(m) => Status::invalid_argument(m.to_string()),
```

(match the existing arms' style; `engine-wire/src/client.rs:59` already maps `InvalidArgument` back to `ControlPlaneError::Validation`, so the refusal round-trips typed.)

- [ ] **Step 3: Worker abandons on a deterministic refusal**

In `src/services/worker/src/transform.rs`, replace the commit step's blanket retry closure (`:360-365`) with:

```rust
        .map_err(|e| match e {
            // A Validation off the commit wire is deterministic (e.g. a
            // stream-table target refusal) — retrying can never succeed.
            ControlPlaneError::Validation(m) => {
                JobFailure::abandon(format!("commit_transform: {m}"))
            }
            other => JobFailure::retry(
                ctx.worker_tuning.backoff(attempts),
                format!("commit_transform: {other}"),
            ),
        })?;
```

(`ControlPlaneError` is already imported at `transform.rs:10`.)

- [ ] **Step 4: Run the e2e + the transform suites**

Run: `buck2 test --console none //src/services/worker:transform-stream-refuse-e2e //src/services/worker:transform-e2e //src/services/worker:typed-transform-e2e //src/services/worker:run-wire-job`
Expected: PASS. Also grep for tests asserting the old `internal` code for Validation on the engine wire (`grep -rn "internal" src/services/engine/tests src/services/query-api/tests | grep -i valid`) — update any that pinned the old mangled mapping (expected: none).

- [ ] **Step 5: prek + commit**

```bash
git add -A
buck2 run //tools:prek -- run --all-files
git commit -m "feat(engine): carry Validation as invalid_argument; worker abandons on it

The engine control status() mapping gains Validation -> invalid_argument
(previously mangled to internal), round-tripping typed through the wire
client. The worker's commit_transform step abandons on a Validation
(deterministic refusal, e.g. a stream-table target) instead of retrying.
E2E: a transform run against a CDC-declared output abandons with the
refusal message and registers nothing."
```

---

## Task 4: Define-time UX guard in postgres `define_transform`

**Files:**
- Modify: `src/control-plane/postgres/src/transforms.rs:298-378`
- Test: `src/control-plane/postgres/tests/transform_stream_target_define.rs` (new) + BUCK target

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/transform_stream_target_define.rs` (`loom_fixture_test`, `PgFixture` harness; build `TransformDef`s the way `testkit`'s transforms contract does):

1. **Physical output = stream table ⇒ refused**: declare `("s","log_t")` a log table (ensure_table + `declare_stream`); `define_transform` of a Physical def with `output: ("s","log_t")` ⇒ `Err(Validation)` with the `stream-table target refused:` prefix.
2. **Typed output bound to a CDC table ⇒ refused**: define an ontology type `Widget` backed by a table declared CDC; a Typed def with `output: "Widget"` (and a defined input type, so the typed-refs-known check passes) ⇒ `Err(Validation)`.
3. **Batch output ⇒ Ok**: Physical def targeting a plain table defines fine.
4. **Not-yet-existing output ⇒ Ok**: Physical def whose output table has no mirror row defines fine (the commit-time guard covers the later-declared case — Task 3's e2e).

Add `loom_fixture_test` target `transform-stream-target-define` to `src/control-plane/postgres/BUCK` (mirror `data-triggers`, `BUCK:367`).

Run: `buck2 test --console none //src/control-plane/postgres:transform-stream-target-define` — expected FAIL (defines succeed today).

- [ ] **Step 2: Add the check**

In `postgres/src/transforms.rs` `define_transform`, after the typed-refs-known check (`:311-329`), resolve the def's output table and refuse a stream target on the same `tx`:

```rust
        // Define-time UX guard (the commit-time guard in IcebergTx is the hard
        // gate — ad-hoc runs and post-define stream declarations bypass this):
        // a def whose output resolves to a declared stream/CDC table is
        // refused up front. Physical outputs are literal; typed outputs
        // resolve through the ontology binding (same lookup the trigger
        // machinery uses). An unresolved/absent output passes.
        let output_table: Option<TableRef> = match &def.body {
            TransformBody::Physical { output, .. } => Some(output.clone()),
            TransformBody::Typed { output, .. } => {
                let bodies = [(def.name.clone(), def.body.clone())];
                pg_type_tables(&mut *tx, &bodies).await?.remove(output)
            }
        };
        if let Some(t) = &output_table {
            crate::stream::pg_refuse_stream_target(&mut tx, t).await?;
        }
```

(Adjust to `pg_type_tables`'s actual signature — it is already called at `:353`; if it takes a slice of `(TransformName, TransformBody)` pairs this compiles as written. If the `TransformBody` enum gains variants later — slice 4's `MicroBatch` — the match must stay exhaustive; a `MicroBatch` output is a stream table *by design* and is committed via `CommitMicroBatch`, not this seam, so its arm would resolve `None` here. Leave a comment saying so.)

The memory adapter's `define_transform` is deliberately unchanged (no `TableRef → table_id` mirror to resolve against) — do NOT add the assertion to the testkit `transforms_contract` (it runs against both adapters).

- [ ] **Step 3: Run tests**

Run: `buck2 test --console none //src/control-plane/postgres:transform-stream-target-define //src/control-plane/postgres:transforms //src/control-plane/memory:transforms`
(the second/third names are the existing transforms-contract targets — check their exact BUCK names and substitute.) Expected: PASS — new checks green, both adapter contracts unmodified and green.

- [ ] **Step 4: prek + commit**

```bash
git add -A
buck2 run //tools:prek -- run --all-files
git commit -m "feat(transform): refuse stream-table outputs at define time

Postgres define_transform resolves the def's output (physical literal /
typed via the ontology binding) and refuses a declared stream/CDC target
with the typed Validation error — the UX layer over the commit-time hard
gate. Memory adapter unchanged (no TableRef->table_id mirror); testkit
contracts untouched."
```

---

## Task 5: Typed refusal on the multi-step action path (422, never 500)

**Files:**
- Modify: `src/services/engine-serving/src/serving.rs:36-60` (`EngineServingError::Validation`)
- Modify: `src/services/engine-serving/src/action_writer.rs:114-141` (`write_steps` mapping)
- Modify: `src/services/engine/src/flight.rs:38-47` (`serving_status` arm)
- Modify: `src/services/engine/src/service.rs:412-416` (`write_steps` handler narrow match)
- Modify: `src/services/query-api/src/serving.rs:76-97` (`ServingError::Unsupported`)
- Modify: `src/services/query-api/src/engine_action_client.rs:24-29` (`to_serving_write` arm)
- Modify: `src/services/query-api/src/action.rs:1415` (`run_multi_step` mapping)
- Test: extend `src/services/query-api/tests/write_steps_e2e.rs` (existing `write-steps-e2e` target)

- [ ] **Step 1: Write the failing test**

In `write_steps_e2e.rs` (which already drives `EngineActionClient` → proto → engine server → `action_writer::write_steps` → `iceberg_landing::write_steps`), add a test: declare one target table CDC (the Task 1 seeding shape), then call `.write_steps(...)` with a step targeting it. Assert `Err(ServingError::Unsupported(m))` with the `stream-table target refused:` prefix, and that the co-staged batch target got nothing.

Run: `buck2 test --console none //src/services/query-api:write-steps-e2e` — expected FAIL (today it surfaces as `ServingError::Engine` off a `Status::internal`).

- [ ] **Step 2: Thread the variant through the four seams**

1. `engine-serving/src/serving.rs` — add to `EngineServingError`:

```rust
    /// A deterministic target-validation refusal from the commit layer (e.g.
    /// a stream-table target on a framing-unaware write path). Wire callers
    /// map this to `invalid_argument`; query-api surfaces 422.
    #[error("validation: {0}")]
    Validation(String),
```

2. `engine-serving/src/action_writer.rs` `write_steps` — replace the blanket map at `:138-140`:

```rust
        iceberg_landing::write_steps(&self.pool, &self.catalog, steps, event)
            .await
            .map_err(|e| match e {
                ControlPlaneError::Validation(m) => EngineServingError::Validation(m),
                other => EngineServingError::Engine(other.to_string()),
            })
```

3. `engine/src/flight.rs` `serving_status` — add `E::Validation(m) => Status::invalid_argument(m),`.
4. `engine/src/service.rs` `write_steps` handler (`:412-416`) — narrow match so ONLY the new variant changes code (other errors stay `internal`, byte-identical):

```rust
            .map_err(|e| match e {
                engine_serving::EngineServingError::Validation(m) => {
                    Status::invalid_argument(m)
                }
                other => Status::internal(other.to_string()),
            })?;
```

5. `query-api/src/serving.rs` — add to `ServingError`:

```rust
    /// The engine refused the write's target as unsupported on this path
    /// (e.g. a stream-table target on the multi-step seam) → 422.
    #[error("unsupported: {0}")]
    Unsupported(String),
```

6. `query-api/src/engine_action_client.rs` `to_serving_write` — add `ControlPlaneError::Validation(m) => ServingError::Unsupported(m),` before the catch-all (the gRPC layer maps `invalid_argument → Validation`, `client.rs:59`).
7. `query-api/src/action.rs` `run_multi_step` — at the `write_steps` call (`:1415`):

```rust
    deps.action_engine
        .write_steps(&writes, event, &[])
        .await
        .map_err(|e| match e {
            ServingError::Unsupported(m) => ActionError::Unsupported(m),
            other => ActionError::from(other),
        })?;
```

`ActionError::Unsupported` already renders as HTTP 422 with the message (`http.rs:1098-1100`) — no HTTP-layer change.

If any exhaustive `match` over `EngineServingError`/`ServingError` elsewhere breaks, add the new arm conservatively (map like `Engine`) — the compiler enumerates them.

- [ ] **Step 3: Run tests**

Run: `buck2 test --console none //src/services/query-api:write-steps-e2e //src/services/query-api:multi-step-run //src/services/query-api:action-multi-object-e2e //src/services/query-api:action-response-http`
(substitute exact BUCK target names — `grep -n 'multi_step_run\|action_multi_object' src/services/query-api/BUCK`.) Expected: PASS — new refusal case green, existing multi-step suites unmodified and green.

- [ ] **Step 4: prek + commit**

```bash
git add -A
buck2 run //tools:prek -- run --all-files
git commit -m "feat(query-api): surface stream-target write_steps refusal as 422

Thread the typed refusal across the action seam: EngineServingError::
Validation (engine-serving) -> invalid_argument (wire) -> ServingError::
Unsupported (query-api) -> ActionError::Unsupported (existing 422). A
multi-step action against a stream/CDC-bound target now fails with a clear
client error and writes nothing; all other error codes byte-identical."
```

---

## Task 6: Full non-regression sweep, docs, register close

- [ ] **Step 1: Whole-tree build + scoped suite**

Run: `buck2 build -v0 --console none //src/...`
Then the touched-crate suites (local: add `-j 8` if running wide):

```bash
buck2 test --console none //src/control-plane/postgres: //src/services/worker: //src/services/query-api: //src/services/engine: //src/services/engine-serving:
```

(or the btd-scoped set in a cloud session). Expected: all PASS; the pre-existing stream suites (`stream-overwrite-framing`, `stream-cdc-*`, `stream-consolidate-job`) and both transforms contracts unchanged and green.

- [ ] **Step 2: Update `docs/system-capabilities/stream.md`**

- Replace the known-gaps bullet `#fut-stream-framing-write-paths` (`stream.md:274-276`) — the id no longer exists (promoted, now closing). Replace with shipped prose in the write-paths discussion: the transform (`CommitTransform`/`IcebergTx`) and multi-target (`write_steps`) paths **refuse** declared stream/CDC/changelog targets with a typed `stream-table target refused:` error (define-time UX guard on `define_transform` + commit-time hard gate; worker abandons; multi-step actions 422); the framing-aware paths (inline/flush/land/overwrite/`write_delta`) remain the only stream write paths until `CommitMicroBatch` lands.
- Refresh the `_As of <commit>._` line.

- [ ] **Step 3: Close the register item**

Invoke the `loom-docs-update` skill. It should: remove the `#road-stream-framing-write-paths` entry from `docs/ROADMAP.md` (`:30-31` — entries are removed on close; git history keeps the record), and verify no new deferrals arose (the exit-condition note lives in the spec, not a register item). Then:

```bash
bash tools/docs.sh validate
```

Expected: clean.

- [ ] **Step 4: prek + final commit**

```bash
git add -A
buck2 run //tools:prek -- run --all-files
git commit -m "docs(stream): close road-stream-framing-write-paths

Legacy transform/multi-target write paths now refuse stream-table targets
(define-time + commit-time, typed error); document in system-capabilities
and remove the register entry."
```

- [ ] **Step 5: Finish the branch**

Push and open a PR (the standing convention — never a local merge), title `feat(stream): refuse stream-table targets on legacy transform write paths`, body linking the spec and this plan. Poll CI via the commit-status endpoint + BuildBuddy MCP (not `gh pr checks`).
