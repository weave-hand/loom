# Compaction Auto-Trigger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Event-driven automatic compaction triggering (`#road-compaction-auto-trigger`): every file-adding commit counts the table's live sub-cutoff files in its own commit tx and enqueues a deduped `compact_table` job when the count crosses N. Compaction's own commit never re-triggers. Spec: `docs/superpowers/specs/2026-07-09-compaction-auto-trigger-design.md`.

**Architecture:** One stateless helper (`maybe_enqueue_compact` in `iceberg_compact.rs`: eligibility guards + indexed live-small-file count + `pg_insert_if_absent`) called from the three file-adding commit tails (`write_mirror`, `land_additive`, `IcebergTx::commit`). Config is `Option<CompactTriggerCfg>` on the `SqlCatalog` (`None` = disabled = byte-identical today), set at builder time by the engine and ingest mains from two env knobs.

**Tech Stack:** Rust, sqlx (compile-time `query!`), buck2 `loom_fixture_test`/`rust_test`, Postgres. No migrations.

## Global Constraints

Carried from the spec; every task implicitly includes these:

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New fixture tests MUST use `loom_fixture_test` (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`, or the fixture env (PG binaries, MinIO, boot-slot dir) is missing. The `no-inline-tests` prek hook fails the build on any inline `#[test]`.
- **After changing any `query!`/`query_scalar!` SQL**, run `tools/sqlx-prepare.sh` and commit the `.sqlx/` change; the `sqlx-cache-check` test enforces freshness. **Cloud/automated sessions cannot run `sqlx-prepare.sh`** (`initdb` refuses root) — there, write the new queries as runtime `sqlx::query(AssertSqlSafe(...))` with the standard comment (mirror `has_shadow`, `iceberg_inline.rs:784`), and note the query!-conversion as a local follow-up.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/`indexing_slicing`/`panic`/`todo`/`map_err_ignore` in production lib/bin code. `#[expect(lint, reason = "...")]` for a justified local exception. Test code is exempted from the panic-safety lints via the test macros.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files` (`git add` new files first — prek skips untracked files). Markdown: no trailing whitespace, exactly one trailing newline.
- **Default-off is the non-regression pin.** `compact_trigger: None` must leave every existing path byte-identical; existing suites (`iceberg_compact`, `compact_e2e`, `compact_endpoint`, `compact_wire`, `inline_flush_trigger`, `iceberg_flush`, `iceberg_land`) stay green and unchanged.
- **No object-store I/O inside the commit tx** (`iss-iceberg-tx-objectstore`): the helper is Postgres-only.
- **Build/test commands:** build `buck2 build -v0 --console none //src/...`; test `buck2 test --console none <targets>` (full suite locally: `-j 8` to avoid starving the 8 PG boot-slots; cloud: `-M none` builds, scope tests, `buck2 clean` between heavy phases).

---

## File Structure

**Create:**
- `src/control-plane/postgres/tests/compact_trigger.rs` — the trigger fixture test (CAS + register tails, guards, dedup, no-re-trigger, disabled).
- `src/services/worker/tests/compact_auto_e2e.rs` — the acceptance e2e (auto-enqueue → worker drains → converged, no re-trigger).

**Modify (production):**
- `src/control-plane/postgres/src/iceberg_compact.rs` — `CompactTriggerCfg` + `maybe_enqueue_compact`.
- `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` — `SqlCatalog.compact_trigger` field; `SqlCatalogBuilder::with_compact_trigger`; `SqlCatalog::with_compact_trigger` setter.
- `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs` — `write_mirror` calls the helper.
- `src/control-plane/postgres/src/iceberg_landing.rs` — `land_additive` calls the helper.
- `src/control-plane/postgres/src/iceberg_control_plane.rs` — `IcebergTx::commit` calls the helper per written table.
- `src/services/engine/src/run.rs` — `EngineTuning` gains the two knobs; catalog built `with_compact_trigger`.
- `src/services/ingest/src/config.rs` — `RoutingTuning` gains the two knobs (+ validation).
- `src/services/ingest/src/serve.rs` — `build_iceberg_catalog` applies the cfg.
- `src/control-plane/postgres/.sqlx/` — regenerated (one new query).

**Modify (tests/registers):**
- `src/control-plane/postgres/BUCK`, `src/services/worker/BUCK` — new test targets.
- `src/services/engine/tests/engine_tuning.rs`, `src/services/ingest/tests/routing_tuning.rs` — knob cases.
- `docs/ROADMAP.md` (remove closed item), `docs/ISSUES.md` (new operator-endpoint-guard entry), `docs/system-capabilities/engine.md` (landed capability).

---

## Task 1: The trigger helper + `SqlCatalog` config + the CAS seam (`write_mirror`)

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_compact.rs`
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` (struct ~`:216` ctor + builder `:68-115`)
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs:145-190` (`write_mirror`)
- Test: `src/control-plane/postgres/tests/compact_trigger.rs` (new) + `src/control-plane/postgres/BUCK`
- Regenerate + commit: `src/control-plane/postgres/.sqlx/`

**Interfaces produced (later tasks rely on these EXACT names):** `CompactTriggerCfg { small_file_bytes: i64, min_small_files: i64 }`; `maybe_enqueue_compact(conn, table, cfg) -> Result<Option<JobId>>`; `SqlCatalogBuilder::with_compact_trigger(CompactTriggerCfg)`; `SqlCatalog::with_compact_trigger(self, CompactTriggerCfg) -> Self`.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/compact_trigger.rs`. Harness: mirror `src/services/worker/tests/compact_e2e.rs`'s land-small-Parquet-files setup (its `columns()` / `ipc_body()` / `lineage()` helpers and `land` + `InlineLimits` usage — `InlineLimits` with `inline_byte_limit: 0` forces the Parquet branch so each land emits one small file) and `src/control-plane/postgres/tests/inline_flush_trigger.rs`'s `job_count` helper, narrowed to `state = 'available'`:

```rust
async fn available_jobs(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "select count(*) from queue.jobs where kind = $1 and state = 'available'",
    )
    .bind(control_plane_core::COMPACT_JOB_KIND)
    .fetch_one(pool)
    .await
    .unwrap()
}
```

Build the catalog with `loom_test_seed::local_sql_catalog(dsn, warehouse).await.with_compact_trigger(CompactTriggerCfg { small_file_bytes: 1 << 20, min_small_files: 3 })` (1 MiB cutoff — the test's tiny files all qualify; N = 3 keeps the test fast).

Cases (each its own `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` on a fresh db):

1. `below_threshold_enqueues_nothing` — land 2 small files ⇒ `available_jobs == 0`.
2. `crossing_enqueues_exactly_one` — land 3 small files ⇒ `available_jobs == 1`; assert the payload equals `serde_json::to_value(CompactJob { schema, name })` (dedup-coherence with the operator endpoint).
3. `dedup_holds_past_threshold` — land 5 ⇒ still `1`.
4. `large_files_do_not_count` — `small_file_bytes: 1` (nothing qualifies), land 5 ⇒ `0`.
5. `compact_commit_does_not_retrigger` — land 3 (job present), then call `iceberg_compact::compact_table(&pool, &table, &small_paths, &[one_big_datafile])` expiring the smalls; take the small paths from the mirror (`select path from iceberg_mirror.data_file where end_snapshot is null ...`) and build the replacement `DataFile` by hand (mirror `compact_e2e`'s `DataFile` construction) ⇒ `available_jobs` unchanged at `1`.
6. `disabled_catalog_enqueues_nothing` — plain `local_sql_catalog` (no cfg), land 5 ⇒ `0`.
7. `stream_shadow_and_changelog_tables_skip` — three sub-cases on the crossing setup: (a) declare the table a stream table (`cp.declare_cdc(...)` or direct `pg_declare_cdc` — mirror `stream_cdc_bucket.rs`'s declaration harness) ⇒ `0`; (b) `set_has_shadow(&mut conn, tid)` then land ⇒ `0`; (c) a table whose tid is referenced by another's `changelog_table_id` (`set_changelog_table_id` or direct update — mirror `stream_cdc_dual_flush.rs`) ⇒ `0`.

Wire a `loom_fixture_test` target `compact-trigger` in `src/control-plane/postgres/BUCK` mirroring the `inline-flush-trigger` target (same deps + `//src/testing:loom-test-seed` if that's how `compact_e2e` gets `local_sql_catalog` — copy that target's dep form).

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:compact-trigger`
Expected: FAIL — `CompactTriggerCfg` / `with_compact_trigger` / `maybe_enqueue_compact` not found (compile error).

- [ ] **Step 3: Add `CompactTriggerCfg` + `maybe_enqueue_compact`**

In `src/control-plane/postgres/src/iceberg_compact.rs`, add (doc comments per the spec's Design section):

```rust
#[derive(Debug, Clone, Copy)]
pub struct CompactTriggerCfg {
    pub small_file_bytes: i64,
    pub min_small_files: i64,
}

pub async fn maybe_enqueue_compact(
    conn: &mut sqlx::PgConnection,
    table: &TableRef,
    cfg: &CompactTriggerCfg,
) -> Result<Option<control_plane_core::JobId>> {
    let Some(tid) = crate::iceberg_mirror::live_table_id(conn, &table.schema, &table.name).await?
    else {
        return Ok(None);
    };
    // One round trip: eligibility (stream / changelog / shadow) + live small count.
    let row = sqlx::query!(
        "select \
           exists(select 1 from stream.stream_table s \
                  where s.table_id = $1 or s.changelog_table_id = $1) as \"is_stream!\", \
           exists(select 1 from iceberg_mirror.shadow_flag f \
                  where f.table_id = $1) as \"has_shadow!\", \
           (select count(*) from iceberg_mirror.data_file d \
             where d.table_id = $1 and d.end_snapshot is null \
               and d.file_size_bytes < $2) as \"small_count!\"",
        tid,
        cfg.small_file_bytes,
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    if row.is_stream || row.has_shadow || row.small_count < cfg.min_small_files {
        return Ok(None);
    }
    let payload = serde_json::to_value(control_plane_core::CompactJob {
        schema: table.schema.clone(),
        name: table.name.clone(),
    })
    .map_err(|e| ControlPlaneError::Backend(format!("compact trigger payload: {e}").into()))?;
    crate::queue::pg_insert_if_absent(
        &mut *conn,
        &control_plane_core::NewJob {
            kind: control_plane_core::COMPACT_JOB_KIND.to_string(),
            payload,
            run_at: None,
            priority: 0,
        },
    )
    .await
}
```

(Exact error construction: match the file's existing `ControlPlaneError` usage. In a cloud session, write the combined query as `sqlx::query(AssertSqlSafe(...))` + manual row decode instead — see Global Constraints.)

- [ ] **Step 4: Config on the `SqlCatalog`**

In `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs`:
- Add field `pub(crate) compact_trigger: Option<crate::iceberg_compact::CompactTriggerCfg>` to `SqlCatalog`, defaulted `None` in `SqlCatalog::new` (`:216`).
- Builder: field + `pub fn with_compact_trigger(mut self, cfg: CompactTriggerCfg) -> Self` on `SqlCatalogBuilder` (mirror `with_storage_factory`, threading through `load`).
- Consuming setter on `SqlCatalog` itself: `pub fn with_compact_trigger(mut self, cfg: CompactTriggerCfg) -> Self { self.compact_trigger = Some(cfg); self }` (what tests and the ingest/engine mains call on the built catalog — if the builder threading is awkward against the vendored `CatalogBuilder::load` shape, the setter alone is sufficient; keep the vendored-file diff minimal).

- [ ] **Step 5: Call the helper at the CAS tail**

In `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs`, at the end of `write_mirror` (`:145-190`), after `stamp_schema_version` and before `Ok(at)`:

```rust
if let Some(cfg) = &self.compact_trigger {
    let table = TableRef { schema: ns.clone(), name: name.to_string() };
    crate::iceberg_compact::maybe_enqueue_compact(conn, &table, cfg).await?;
}
```

(`write_mirror` returns `control_plane_core::Result` — no error-type bridging needed. This single call covers `do_update_table` and `do_update_table_in_tx`, i.e. ingest multi-file land, flush, COW overwrite, and the stream direct write.)

- [ ] **Step 6: Regenerate `.sqlx`, run the test**

Run: `tools/sqlx-prepare.sh` (local step; commit the new `.sqlx/query-*.json`).
Run: `buck2 test --console none //src/control-plane/postgres:compact-trigger`
Expected: PASS for cases 1–6 and 7(b)/7(c); 7(a) may already pass (declaration goes through the same guards). All-green before moving on.

- [ ] **Step 7: Non-regression + prek + commit**

Run: `buck2 test --console none //src/control-plane/postgres:iceberg-compact //src/control-plane/postgres:inline-flush-trigger //src/control-plane/postgres:iceberg-flush //src/control-plane/postgres:sqlx-cache-check` (use the actual target names from `postgres/BUCK`).
Expected: PASS, unchanged.

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(catalog): stateless compact trigger on the CAS commit tail

CompactTriggerCfg + maybe_enqueue_compact (eligibility guards + live
small-file count + pg_insert_if_absent) evaluated at the end of
write_mirror when the SqlCatalog carries a cfg. Default None = disabled =
byte-identical. Stream/changelog/shadow tables skip; compaction's own
commit never reaches this seam."
```

---

## Task 2: The mirror-only seams — `land_additive` + `IcebergTx::commit`

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs:423-460` (`land_additive`)
- Modify: `src/control-plane/postgres/src/iceberg_control_plane.rs:115-193` (`IcebergTx::commit`)
- Test: extend `src/control-plane/postgres/tests/compact_trigger.rs`

- [ ] **Step 1: Write the failing tests**

Add to `compact_trigger.rs`:

8. `transform_commit_triggers` — via `IcebergControlPlane::new(cp, catalog_with_cfg)`: `begin_table()`, `create_table`, `append_files` with 3 sub-cutoff `DataFile`s (hand-built, sizes < `small_file_bytes`; the files need not physically exist — the register path is mirror-only and reads sizes off the `DataFile`), `commit()` ⇒ `available_jobs == 1`.
9. `transform_compact_files_does_not_trigger` — same tx shape but `compact_files(table, expire, write)` after seeding smalls below N ⇒ `available_jobs == 0`.

Run: `buck2 test --console none //src/control-plane/postgres:compact-trigger` — case 8 FAILS (no trigger on the register tail yet).

- [ ] **Step 2: `IcebergTx::commit`**

In `iceberg_control_plane.rs`, after the `staged_files` register loop and the `written` vec construction (`:187-189`) but before `tx.commit()`, add:

```rust
if let Some(cfg) = &catalog.compact_trigger {
    for table in &written {
        crate::iceberg_compact::maybe_enqueue_compact(&mut tx, table, cfg).await?;
    }
}
```

(`written` is already deduped and covers Append/Overwrite staged files only — `staged_compacts` tables are deliberately excluded, matching the data-trigger exclusion on the same lines. `compact_trigger` is `pub(crate)` on `SqlCatalog`, same crate.)

- [ ] **Step 3: `land_additive`**

In `iceberg_landing.rs` `land_additive` (`:423`), after its `register_files(..., WriteMode::Append, at)` call (`:441`), add the same `if let Some(cfg) = &catalog.compact_trigger { maybe_enqueue_compact(&mut tx, table, cfg).await?; }` on the held tx, before the lineage/extras tail.

- [ ] **Step 4: Run + prek + commit**

Run: `buck2 test --console none //src/control-plane/postgres:compact-trigger`
Expected: PASS (all 9 cases). Also: `buck2 build -v0 --console none //src/...` (silent success).

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(catalog): compact trigger on the mirror-only commit tails

IcebergTx::commit (per written table; staged_compacts excluded) and
land_additive evaluate the same helper. All three file-adding commit
tails now covered; the WriteMode::Compact path structurally cannot
re-trigger."
```

---

## Task 3: Engine knobs — `EngineTuning` + catalog wiring

**Files:**
- Modify: `src/services/engine/src/run.rs:33-118` (`EngineTuning` + `run`'s catalog build)
- Test: `src/services/engine/tests/engine_tuning.rs` (existing `rust_test` target `engine-tuning`, `engine/BUCK:168`)

- [ ] **Step 1: Write the failing tests**

Extend `engine_tuning.rs`: defaults (`compact_small_file_bytes == 128 * 1024 * 1024`, `compact_trigger_files == 8`); overrides parse (`LOOM_COMPACT_THRESHOLD_BYTES`, `LOOM_COMPACT_TRIGGER_FILES`); `"0"` parses (disable sentinel); `"1"` is an `Err` naming `LOOM_COMPACT_TRIGGER_FILES` (must be 0 or >= 2 — a min of 1 would re-enqueue immediately after every compaction whose output stays under the cutoff).

Run: `buck2 test --console none //src/services/engine:engine-tuning` — FAIL (fields missing).

- [ ] **Step 2: Implement**

`EngineTuning` gains `compact_small_file_bytes: i64` and `compact_trigger_files: i64`; `from_map` parses both (`parse_var` defaults `128 * 1024 * 1024` / `8_i64`) and, since `from_map` does no range validation today, adds an explicit post-parse check rejecting `compact_trigger_files == 1` (and negatives) via `service_runtime::invalid("LOOM_COMPACT_TRIGGER_FILES", "must be 0 or >= 2")` (yields `ConfigError::Invalid { var, .. }`, the shape the existing `engine_tuning.rs` tests match on). Add `use control_plane_postgres::iceberg_compact::CompactTriggerCfg;` to `run.rs` (used unqualified below). In `run` (`run.rs:113-118`), after the builder `.load(...)`, apply:

```rust
let catalog = if tuning.compact_trigger_files >= 2 {
    catalog.with_compact_trigger(CompactTriggerCfg {
        small_file_bytes: tuning.compact_small_file_bytes,
        min_small_files: tuning.compact_trigger_files,
    })
} else {
    catalog
};
```

before the `Arc::new` (adjust to the actual construction order; `0` ⇒ no cfg ⇒ disabled).

- [ ] **Step 3: Run + prek + commit**

Run: `buck2 test --console none //src/services/engine:engine-tuning` and `buck2 build -v0 --console none //src/services/engine:engine`.
Expected: PASS / silent success.

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(engine): LOOM_COMPACT_THRESHOLD_BYTES/_TRIGGER_FILES tuning

The engine-side commits (flush, transform, COW, stream) now evaluate the
compact trigger; 0 disables, 1 rejected at startup, cutoff env name shared
with the worker's selection knob so one deploy value governs both."
```

---

## Task 4: Ingest knobs — `RoutingTuning` + catalog wiring

**Files:**
- Modify: `src/services/ingest/src/config.rs:36-107` (`RoutingTuning` fields/default/`overlay_env`/`validate`)
- Modify: `src/services/ingest/src/serve.rs:58-70` (`build_iceberg_catalog` applies the cfg)
- Test: `src/services/ingest/tests/routing_tuning.rs` (existing target)

- [ ] **Step 1: Write the failing tests**

Extend `routing_tuning.rs` with the same four cases as Task 3 (defaults / overrides / `0` valid / `1` invalid via `validate()` naming `LOOM_COMPACT_TRIGGER_FILES`).

Run: `buck2 test --console none //src/services/ingest:routing-tuning` (target name `routing-tuning`, `ingest/BUCK:263`) — FAIL.

- [ ] **Step 2: Implement**

`RoutingTuning` gains `compact_small_file_bytes: i64` (default `128 * 1024 * 1024`) and `compact_trigger_files: i64` (default `8`); `overlay_env` reads the two env keys; `validate` rejects `compact_trigger_files == 1` or `< 0`, and `compact_small_file_bytes <= 0`. `build_iceberg_catalog` gains the tuning (thread `RoutingTuning` in from the caller — it already flows into `serve`) and applies `with_compact_trigger` when `compact_trigger_files >= 2`.

- [ ] **Step 3: Run + prek + commit**

Run: `buck2 test --console none //src/services/ingest:routing-tuning` plus `buck2 build -v0 --console none //src/services/ingest:ingest`; then the ingest landing suite: `buck2 test --console none //src/services/ingest:iceberg-land //src/services/ingest:compact-endpoint` (actual target names from `ingest/BUCK`).
Expected: PASS.

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(ingest): compact-trigger knobs on RoutingTuning + landing catalog"
```

---

## Task 5: Acceptance e2e — auto-enqueue, drain, converge

**Files:**
- Create: `src/services/worker/tests/compact_auto_e2e.rs`
- Modify: `src/services/worker/BUCK` (new `loom_fixture_test` target `compact-auto-e2e`, mirroring `compact-e2e` at `worker/BUCK:263`)

- [ ] **Step 1: Write the test**

Mirror `compact_e2e.rs`'s harness (spawn engine UDS, `local_sql_catalog`, `land` small files, `CompactCtx` + `handle_compact`) with two changes: build the catalog `with_compact_trigger(CompactTriggerCfg { small_file_bytes: <cutoff matching ctx.threshold_bytes>, min_small_files: 3 })`, and **do not enqueue manually** — assert the flow:

1. Land 3 small files ⇒ exactly one `available` `compact_table` job (the auto-enqueue; use the Task-1 `available_jobs` helper against the fixture pool).
2. Dequeue it (over the engine wire, as `compact_e2e` does) and run `handle_compact` ⇒ Ok.
3. Post-compaction: the small files are end-capped, the coalesced file is live, row set preserved (reuse `compact_e2e`'s assertions), and `available_jobs == 0` — **compaction's commit did not re-trigger** end-to-end.
4. Land 3 more small files ⇒ a fresh job appears (the trigger re-arms on genuine new writes).

- [ ] **Step 2: Run + prek + commit**

Run: `buck2 test --console none //src/services/worker:compact-auto-e2e //src/services/worker:compact-e2e`
Expected: PASS (both — the manual-enqueue e2e must stay green untouched).

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "test(worker): compact auto-trigger acceptance e2e

Land >N small files with no operator POST: exactly one auto job, the
worker drains it, and the swap commit does not re-trigger; new writes
re-arm."
```

---

## Task 6: Full-suite verification

- [ ] **Step 1: Whole-tree build + test**

Run: `buck2 build -v0 --console none //src/...` then `buck2 test --console none -j 8 //src/...` (local; on a cloud host scope to the btd-affected targets instead — see Global Constraints).
Expected: silent build; `Tests finished: Pass N. Fail 0`. Pay attention to the default-off pins: `inline-flush-trigger`, `iceberg-flush`, `compact-e2e`, `compact-endpoint`, `compact-wire`, `stream-cdc-*` must be untouched-green.

- [ ] **Step 2: prek**

Run: `buck2 run //tools:prek -- run --all-files` — clean (rustfmt, clippy, docs-validate, reindeer unchanged).

---

## Task 7: Close the register item

- [ ] **Step 1: Update the registers**

Use the `loom-docs-update` skill flow, staged with the work:

- `docs/ROADMAP.md` — remove the `#road-compaction-auto-trigger` entry (items close by deletion; git history keeps the record).
- `docs/ISSUES.md` — add the observation the spec surfaced: the **operator** `POST /tables/{schema}/{table}/compact` endpoint enqueues for stream/changelog/shadow-flagged tables with none of the trigger's eligibility guards (`ingest/src/http.rs:142-163`) — a pre-existing footgun now inconsistent with the auto path. Suggested tag block: `{#iss-compact-endpoint-unguarded area:ingest status:open from:2026-07-09-compaction-auto-trigger-design pr:- spec:-}`, cross-linking `[[road-cow-compaction-consolidation]]`.
- `docs/system-capabilities/engine.md` — extend the compaction capability prose (around the existing compaction-worker paragraph, `:176`) with the event-driven trigger: the three commit tails, the two knobs, the shared cutoff env, and the one-available-job dedup guarantee.

- [ ] **Step 2: Validate + commit**

```bash
bash tools/docs.sh validate
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "docs(registers): close road-compaction-auto-trigger; record compact-endpoint guard gap"
```

- [ ] **Step 3: Finish the branch**

Push and open a PR (the standing convention — never local-merge), title `feat(catalog): event-driven automatic compaction triggering`, body summarizing the seam/guards/knobs and linking the spec. Poll CI via the commit-status endpoint + BuildBuddy invocation, not `gh pr checks`.
