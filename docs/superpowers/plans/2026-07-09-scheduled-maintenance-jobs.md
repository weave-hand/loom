# Scheduled maintenance jobs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cron schedules for maintenance job kinds (`gc_table`, `compact_table`) — a `queue.schedule` row carries `(name, kind, payload, cron)` with derived `next_run_at`; the engine scheduler's existing loop fires due rows via an atomic claim-advance-AND-deduped-enqueue on the queue concern; admin CRUD over `POST/GET /admin/schedules` + `DELETE /admin/schedules/{name}` (spec: `docs/superpowers/specs/2026-07-09-scheduled-maintenance-jobs-design.md`, item `road-scheduled-maintenance-jobs`).

**Architecture:** Sibling to transform schedules, sharing the mechanism, not the table: the core cron helpers (`validate_cron`/`next_cron_occurrence`, `core/src/transforms.rs:230`/`:242`) are reused as-is; the claim SQL mirrors `claim_due_schedules`' `FOR UPDATE SKIP LOCKED` shape (`postgres/src/transforms.rs:585`); the fire folds `pg_insert_if_absent` (`postgres/src/queue.rs:37`) into the claim transaction, so a firing is exactly-once (advance + enqueue commit together). The schedule surface lives on the `Queue` concern; the engine's `scheduler_loop` (`engine/src/scheduler.rs:53`) gains a `maintenance_tick` leg — no new knob, no `run.rs` change. Transform schedules are untouched.

**Tech Stack:** Rust/buck2, croner (already vendored), sqlx compile-time queries, axum + utoipa admin routes.

## Global Constraints

- **Fire semantics:** `fire_due_job_schedules(now, limit)` advances each due row's `next_run_at` to the next occurrence **after `now`** AND enqueues the deduped job **in one unit of work** — exactly-once per firing; concurrent callers never both fire the same schedule. Dedup: an identical `(kind, payload)` job still `state='available'` suppresses the insert (`ScheduleFired.job = None`) but the schedule still advances.
- **Define semantics:** validated upsert (`validate_job_schedule`: non-empty name, valid cron, kind ∈ `SCHEDULABLE_JOB_KINDS`, payload decodes as the kind's typed contract). `next_run_at` computed from `now`; a redefine resets the schedule clock (transform-schedules refinement, carried over). Table-existence is checked in the **admin handler** (`Catalog::current_snapshot` → `NotFound` ⇒ 400), NOT in the concern.
- Tests are `rust_test` integration targets under `tests/` (never inline `#[cfg(test)]`); postgres fixture tests use `loom_fixture_test`; after SQL changes run `bash tools/sqlx-prepare.sh` and commit `.sqlx`.
- Prefer `--console none` (and `-v0` for builds): `buck2 test --console none <target>` prints only the summary line; never pipe superconsole output through `head`/`tail`.
- On a root/cloud host add `--unstable-allow-all-tests-on-re` for fixture-test runs; locally cap the full suite with `-j 8` (postgres boot slots).
- Before EVERY commit: `git add` new files first, then `buck2 run //tools:prek -- run --all-files` must pass (rustfmt is a separate hook — clippy-clean ≠ lint-clean).
- Production code: clippy pedantic+restriction (no `unwrap`/`expect`/`panic`/`indexing_slicing`).
- Commit messages are Conventional Commits and end with the `Co-Authored-By: Claude Fable 5` + `Claude-Session` trailer block.

**Push discipline:** do NOT `git push` between Tasks 2 and 3 — the tree is deliberately red at test level (postgres stubs) in that window; prek's pre-push hooks run the full tree. Push only after Task 3's gate at the earliest.

---

### Task 1: core — `JobSchedule` contract + validation

**Files:**
- Create: `src/control-plane/core/src/job_schedule.rs`
- Modify: `src/control-plane/core/src/lib.rs` (module + re-exports)
- Create: `src/control-plane/core/tests/job_schedule.rs`
- Modify: `src/control-plane/core/BUCK` (new `rust_test` target)

**Interfaces:**
- Produces: `pub struct JobSchedule { name: String, kind: String, payload: serde_json::Value, cron: String }` (serde + `Debug, Clone, PartialEq, Eq`); `pub const SCHEDULABLE_JOB_KINDS: &[&str] = &[crate::GC_JOB_KIND, crate::COMPACT_JOB_KIND]`; `pub fn validate_job_schedule(s: &JobSchedule) -> Result<()>`.

- [ ] **Step 1: Write the failing tests** — `src/control-plane/core/tests/job_schedule.rs`: `validate_job_schedule` accepts a good `gc_table` schedule (`{"schema": "main", "name": "orders"}` payload, cron `"0 3 * * *"`) and a good `compact_table` one; rejects (as `ControlPlaneError::Validation`) an empty name, cron `"not a cron"`, kind `"transform"` (known but not schedulable), kind `"nope"`, and a `gc_table` payload missing `name`. Wire the BUCK target mirroring the sibling `transforms` test target (`core/BUCK:422`); deps `[":core", "//third-party:serde_json"]`.
- [ ] **Step 2: Run to verify failure** — `buck2 test --console none //src/control-plane/core:job-schedule` → compile error (`validate_job_schedule` not found).
- [ ] **Step 3: Implement** `job_schedule.rs` per the spec's Core-domain section: module doc mirrors `gc.rs`/`compact_job.rs` ("shared producer/consumer contract"); `validate_job_schedule` checks name non-empty → `validate_cron(&s.cron)` (import from crate root) → kind membership → per-kind typed decode (`serde_json::from_value::<GcJob>(s.payload.clone())` / `CompactJob`), each failure a `Validation` error naming the field. Re-export `JobSchedule`, `SCHEDULABLE_JOB_KINDS`, `validate_job_schedule` from `lib.rs`.
- [ ] **Step 4: Run** — `buck2 test --console none //src/control-plane/core:` → all pass.
- [ ] **Step 5: prek + commit** (`feat(control-plane): JobSchedule contract + validation in core`).

---

### Task 2: `Queue` trait growth + memory adapter + testkit contract

**Files:**
- Modify: `src/control-plane/core/src/queue.rs` (types + 4 trait methods)
- Modify: `src/control-plane/core/src/lib.rs` (re-exports)
- Modify: `src/control-plane/memory/src/queue.rs` + `src/control-plane/memory/src/lib.rs` (schedule state)
- Modify: `src/control-plane/postgres/src/queue.rs` (**compiling stubs only** — Task 3 replaces them)
- Modify: `src/services/engine-wire/src/client.rs` (**compiling stubs only** — `GrpcQueueClient` is a third `impl Queue`; the trait has no default bodies so it must gain all four methods or the tree stops building)
- Modify: `src/control-plane/testkit/src/lib.rs` (`job_schedules_contract`)
- Create: `src/control-plane/memory/tests/job_schedules.rs`
- Modify: `src/control-plane/memory/BUCK` (new `rust_test`, mirror the `queue` target at `memory/BUCK:22`)

**Interfaces:**
- Produces (Tasks 3-6 rely on):

```rust
pub struct JobScheduleStatus { pub schedule: JobSchedule, pub next_run_at: OffsetDateTime }
pub struct ScheduleFired { pub name: String, pub job: Option<JobId> }
// on trait Queue (core/src/queue.rs:89):
async fn define_job_schedule(&self, s: JobSchedule) -> Result<()>;
async fn list_job_schedules(&self) -> Result<Vec<JobScheduleStatus>>;
async fn delete_job_schedule(&self, name: &str) -> Result<()>;
async fn fire_due_job_schedules(&self, now: OffsetDateTime, limit: u32) -> Result<Vec<ScheduleFired>>;
```

- [ ] **Step 1: Write the testkit contract.** Add `pub async fn job_schedules_contract<CP: ControlPlane + Queue>(cp: &CP)` to `testkit/src/lib.rs` (near `queue_contract`, `lib.rs:70`), asserting the spec's Acceptance-contract legs: invalid cron / unknown kind / bad payload rejected as `Validation`; a good `gc_table` schedule (`nightly-gc`) lists with `next_run_at > before` (capture `before` BEFORE defining — strictly-after semantics, no flake); redefine resets the clock; `fire_due_job_schedules(before, 32)` → empty; fire at `probe = before + 2 days` → exactly one `ScheduleFired` with `job: Some(_)`, and `dequeue(&["gc_table"], "w")` yields the job with the schedule's payload; re-fire at the same probe → empty (advance was atomic); dedup leg: define a second schedule with the SAME kind+payload, fire at a due probe WITHOUT dequeuing the first job → `job: None` for the duplicate but its `next_run_at` advanced; concurrent leg: `tokio::join!` two fire calls at a fresh probe → total jobs enqueued for that firing == 1; `delete_job_schedule("nightly-gc")` → subsequent list omits it; delete again → `NotFound`.
- [ ] **Step 2: Wire the memory test** — `memory/tests/job_schedules.rs` calls the contract (mirror `memory/tests/queue.rs`); add the BUCK target. `buck2 test --console none //src/control-plane/memory:job-schedules` → compile error (methods not on the trait).
- [ ] **Step 3: Implement.** Core: types + trait methods with the doc comments from the spec's Core-domain section; re-export from `lib.rs`. Memory (`memory/src/queue.rs` + `lib.rs`): `MemoryControlPlane` gains `schedules: Arc<Mutex<HashMap<String, (JobSchedule, OffsetDateTime)>>>` (add to the constructor at `lib.rs:82` and the `Clone` at `lib.rs:229`); `define_job_schedule` = `validate_job_schedule` then insert with `next_cron_occurrence(&s.cron, now_utc())`; `list` = name-sorted snapshot; `delete` = remove-or-`NotFound`; `fire_due_job_schedules` locks `schedules` then `rows`, collects due names sorted (deterministic), truncates to `limit`, and per schedule: advance `next_run_at` via `next_cron_occurrence(cron, now)`, then insert-if-absent into `rows` (scan for `state == "available" && kind == s.kind && payload == s.payload` — the queue-`Row` available-state idiom, `memory/src/queue.rs:28` inside `dequeue`, where `Row.state: &'static str` is `"available"|"running"|"failed"` per `lib.rs:42`; NOT the `transaction.rs:294` run-ledger `RunState::Queued` scan, which is a different structure); on suppress push `ScheduleFired { job: None }`, else `Self::insert` + `notify.notify_waiters()`. Keep the established lock order (take `schedules` before `rows`; document it beside the fields).
- [ ] **Step 3b: COMPILING STUBS for the other two `impl Queue` sites** (the trait has no default bodies; without stubs the whole tree stops building and prek's clippy hook fails). There are exactly THREE `impl Queue` in-tree — memory (implemented above), plus these two that must gain all four methods:
  - `postgres/src/queue.rs:68` (`PgControlPlane`): all four methods return `Err(ControlPlaneError::Backend("job schedules: postgres storage lands in the next commit (migration 0043)".into()))`. Task 3 replaces these.
  - `src/services/engine-wire/src/client.rs:629` (`GrpcQueueClient`): all four methods mirror its existing `enqueue` stub — `Err(ControlPlaneError::Backend("job schedules are not available over the engine-wire".into()))`. This impl is in the `//src/...` build graph (worker/query-api/engine depend on it), so it MUST compile or Task 2 Step 4's build fails.

  The postgres queue-contract targets stay green (they don't call the new methods yet); only a postgres `job_schedules_contract` run would be red, and none exists until Task 3.
- [ ] **Step 4: Run** — `buck2 test --console none //src/control-plane/memory:` → all pass; `buck2 build -v0 --console none //src/...` → succeeds (stub compile check across engine/worker/runtime).
- [ ] **Step 5: prek + commit** (`feat(control-plane): job-schedule surface on Queue — trait, memory, contract`).

---

### Task 3: postgres adapter — migration 0043, atomic fire, `.sqlx`

**Files:**
- Create: `src/control-plane/postgres/migrations/0043_job_schedules.sql`
- Modify: `src/control-plane/postgres/src/queue.rs` (replace stubs)
- Create: `src/control-plane/postgres/tests/job_schedules.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test`, mirror the `queue` target at `postgres/BUCK:259`)
- Regenerate: `src/control-plane/postgres/.sqlx/`

- [ ] **Step 1: Migration** `0043_job_schedules.sql` — exactly the spec's Schema section (`queue.schedule`: `name text primary key, kind text not null, payload jsonb not null, cron text not null, next_run_at timestamptz not null`; index `schedule_due` on `next_run_at`). **NOTE: the next free slot is 0043, not 0042** — `0042_mv_watermark.sql` landed in PR #418 (highest existing is 0042); a duplicate `0042` breaks `embedded_migrations_unit`'s contiguity assert. The migrations glob (`postgres/BUCK:127`) picks it up; `embedded_migrations_unit` asserts contiguity, not an exact count — no test edit needed.
- [ ] **Step 2: Wire the fixture test** — `postgres/tests/job_schedules.rs` boots `PgFixture` and runs `job_schedules_contract` (mirror `postgres/tests/queue.rs`). `buck2 test --console none //src/control-plane/postgres:job-schedules` (add `--unstable-allow-all-tests-on-re` on a root host) → FAILS (stubs return `Backend`).
- [ ] **Step 3: Implement** in `postgres/src/queue.rs`:
  - `define_job_schedule`: `validate_job_schedule`, compute `next_run_at = next_cron_occurrence(&s.cron, OffsetDateTime::now_utc())?`, then a single `query!` upsert (`insert into queue.schedule … on conflict (name) do update set kind/payload/cron/next_run_at = excluded.*`).
  - `list_job_schedules`: `select name, kind, payload, cron, next_run_at from queue.schedule order by name`.
  - `delete_job_schedule`: `delete … returning name` via `fetch_optional`; `None` ⇒ `NotFound`.
  - `fire_due_job_schedules`: one transaction — `select name, kind, payload, cron from queue.schedule where next_run_at <= $1 order by next_run_at, name limit $2 for update skip locked` (the `claim_due_schedules` shape, `transforms.rs:585`); per row: `next_cron_occurrence(&cron, now)?`, `update queue.schedule set next_run_at = $2 where name = $1`, then `pg_insert_if_absent(&mut *tx, &NewJob { kind, payload, run_at: None, priority: 0 })` (same file, `queue.rs:37` — its `pg_notify` is buffered until this tx commits, matching the enqueue contract) and push `ScheduleFired { name, job }`; commit.
- [ ] **Step 4: sqlx** — `bash tools/sqlx-prepare.sh`; commit the `.sqlx` diff (expect ~4 new query files; no existing hashes change — the transform queries are untouched, that's the sibling-table point).
- [ ] **Step 5: Run** — the `job-schedules` fixture target + `//src/control-plane/postgres:sqlx-cache-check` → PASS; then `buck2 build -v0 --console none //src/...` → succeeds.
- [ ] **Step 6: prek + commit** (`feat(control-plane): postgres job schedules — migration 0043, atomic fire-with-dedup`).

---

### Task 4: engine — `maintenance_tick` in the scheduler loop

**Files:**
- Modify: `src/services/engine/src/scheduler.rs`
- Modify: `src/services/engine/tests/scheduler.rs` (existing plain `rust_test`, target `scheduler` at `engine/BUCK:177` — extend, no new target)

- [ ] **Step 1: Write the failing test** — new fn in `engine/tests/scheduler.rs` (memory control plane, like `due_schedule_fires_once_as_schedule_run`): define a `gc_table` schedule via `cp.define_job_schedule(...)`; `maintenance_tick(&cp, now, 32)` → 0 (not due); at `probe = now + 2 days` → 1, and `cp.dequeue(&["gc_table".into()], "w")` yields the job with the schedule's payload; re-tick at the same probe → 0. Add a second leg proving transform and maintenance schedules coexist on one tick pass if both due (define one of each; run both ticks; both fire).
- [ ] **Step 2: Run to verify failure** — `buck2 test --console none //src/services/engine:scheduler` → compile error (`maintenance_tick` not found).
- [ ] **Step 3: Implement** — `pub async fn maintenance_tick(cp: &dyn ControlPlane, now: OffsetDateTime, limit: u32) -> usize` in `scheduler.rs`: call `cp.queue().fire_due_job_schedules(now, limit)`; on `Err` → `tracing::warn!` + 0 (the `tick` posture, `scheduler.rs:15`); count firings with `job.is_some()`, `tracing::info!`-log dedup-suppressed ones at the loop level. In `scheduler_loop` (`scheduler.rs:53`), call `maintenance_tick(cp.as_ref(), OffsetDateTime::now_utc(), 32)` beside the existing `tick` call; update the module + fn doc comments (the loop now fires transform AND maintenance schedules). No `run.rs` change; `Queue` must be in scope for method syntax.
- [ ] **Step 4: Run** — `buck2 test --console none //src/services/engine:scheduler` and `//src/services/engine:engine-tuning` → pass.
- [ ] **Step 5: prek + commit** (`feat(engine): maintenance tick — fire due job schedules on the scheduler loop`).

---

### Task 5: admin surface — `/admin/schedules` CRUD

**Files:**
- Modify: `src/services/runtime/src/admin.rs`
- Modify: `src/services/runtime/tests/admin_management.rs`
- Modify: `src/services/query-api/tests/openapi.rs` (drift guard `expected()`)

- [ ] **Step 1: Write the failing tests** in `admin_management.rs` (reuse `seed_admin_session`/`app`/`send`, `admin_management.rs:59/:82/:87`):
  - `schedule_crud_roundtrip`: seed a landed table (whatever existing helper the file's transform tests use to satisfy the catalog check — if none seeds the mirror, use `MemoryControlPlane`'s catalog seeding as the existing catalog tests do); `POST /admin/schedules` with `{"name":"nightly-gc","kind":"gc_table","payload":{"schema":"main","name":"orders"},"cron":"0 3 * * *"}` → 201; `GET /admin/schedules` → the row with an RFC3339 `next_run_at`; `DELETE /admin/schedules/nightly-gc` → 204; `GET` → empty; `DELETE` again → 404.
  - `schedule_define_rejects_bad_shapes`: 400 legs for invalid cron, kind `"transform"`, payload missing `name`, and a payload naming a table absent from the mirror (assert the body names the table).
  - Gate legs: no token → 401; non-admin token → 403 (mirror the existing per-route gate assertions).
- [ ] **Step 2: Run to verify failure** — `buck2 test --console none //src/services/runtime:admin-management` (use the actual target name from `runtime/BUCK`) → 404s (routes absent).
- [ ] **Step 3: Implement** in `admin.rs`:
  - Request/response types (`serde` + `utoipa::ToSchema`): `JobScheduleReq { name, kind, payload, cron }`, `JobScheduleView { name, kind, payload, cron, next_run_at }` (RFC3339 via the `rfc3339` helper, `admin.rs:1288`), `ListSchedulesResp { schedules }`.
  - `define_schedule_route`: decode → build `JobSchedule` → **catalog existence check** (decode the payload's `{schema, name}` per kind, `st.cp.catalog().current_snapshot(&TableRef{..})`, `NotFound` ⇒ 400 naming the table) → `st.cp.queue().define_job_schedule(...)` → 201; concern `Validation` errors map through `status_for` (`runtime/src/auth.rs:41`) to 400.
  - `list_schedules_route` → `list_job_schedules` → `ListSchedulesResp`; `delete_schedule_route` → `delete_job_schedule` → 204 / `status_for` 404.
  - Mount in `admin_routes` (`admin.rs:1581`): `.route("/admin/schedules", post(define_schedule_route).get(list_schedules_route))` + `.route("/admin/schedules/:name", delete(delete_schedule_route))`.
  - Register the three handlers in `AdminApiDoc`'s `paths(...)` and the new types in `components(schemas(...))` (`admin.rs:1650-1700`); full `#[utoipa::path]` docs incl. the 400 catalog leg, `tag = "admin"`.
- [ ] **Step 4: Drift guard** — add `("post", "/admin/schedules")`, `("get", "/admin/schedules")`, `("delete", "/admin/schedules/{name}")` to `expected()` in `query-api/tests/openapi.rs`. Run `buck2 test --console none //src/services/runtime:` and the query-api openapi target → pass.
- [ ] **Step 5: prek + commit** (`feat(api): /admin/schedules CRUD — cron maintenance schedules`).

---

### Task 6: fixture e2e — schedule → fire → worker drains over the wire

**Files:**
- Create: `src/services/worker/tests/scheduled_maintenance_e2e.rs`
- Modify: `src/services/worker/BUCK` (new `loom_fixture_test`, mirror the `e2e` target at `worker/BUCK:81`)

- [ ] **Step 1: Write the test** (the `worker/tests/e2e.rs` drive form — bare dequeue→handle→complete through `GrpcQueueClient`, avoiding `Worker::run` cancellation): boot `PgFixture` + the engine wire (copy the e2e harness setup); land a small table `main.gc_target`; `define_job_schedule` for `gc_table` on it (cron `"0 3 * * *"`); assert `fire_due_job_schedules(now, 32)` is empty (not due), then fire at `probe = now + 2 days` → one `ScheduleFired` with a job id; re-fire at the same probe → empty; `dequeue(&[GC_JOB_KIND], "sched-e2e")` → the job; `handle_gc(client, tuning, job)` → `Ok`; `complete(id)`. Add a `compact_table` leg (schedule → fire → `handle_compact`) reusing the compact harness pieces from `worker/tests` if present, or assert kind+payload dequeue only and note that `handle_compact`'s full drain is covered by the existing compact e2e — prefer the full drain if the harness makes it cheap.
- [ ] **Step 2: Run** — `buck2 test --console none //src/services/worker:scheduled-maintenance-e2e` (add `--unstable-allow-all-tests-on-re` on a root host) → pass.
- [ ] **Step 3: prek + commit** (`test(worker): scheduled maintenance e2e — cron fire to worker drain`).

---

### Task 7: docs, register close, full sweep

**Files:**
- Modify: `docs/system-capabilities/control-plane.md` (queue concern gains the job-schedule surface: `queue.schedule`, exactly-once fire-with-dedup, `SCHEDULABLE_JOB_KINDS`)
- Modify: `docs/system-capabilities/engine.md` (scheduler loop now also fires maintenance schedules; same `LOOM_SCHEDULER_TICK_SECS`)
- Modify: `docs/system-capabilities/query-api.md` or the admin-surface capability doc (the `/admin/schedules` routes) — put it wherever `/admin/transforms` is documented
- Modify: `docs/ROADMAP.md` (REMOVE the `road-scheduled-maintenance-jobs` entry at `ROADMAP.md:30`. Note: the planning PR already retired the old `[[fut-scheduled-jobs]]` cross-link — there is NO dangling `fut-scheduled-jobs` link in ROADMAP to fix, so removing the entry is the only ROADMAP edit.)
- Modify: `docs/FUTURE.md` (`fut-iceberg-gc-orphan-sweep` at `FUTURE.md:232` already links `[[road-scheduled-maintenance-jobs]]` — NOT `[[fut-scheduled-jobs]]`, which the planning PR already retired. This link is **load-bearing**: Task 7 removes the `road-scheduled-maintenance-jobs` ROADMAP entry, so this cross-link MUST be reworded away or `bash tools/docs.sh validate` fails on a dangling link. Reword to cite the landed capability: "Pairs with scheduled GC (`/admin/schedules`, landed)". Then verify nothing else dangles: `grep -rn "fut-scheduled-jobs\|road-scheduled-maintenance-jobs" docs/` — note that spec files outside the three validated registers (`ROADMAP`/`FUTURE`/`ISSUES`) may mention the id and do NOT block `docs.sh validate`.)

**Steps:**

- [ ] **Step 1:** Docs edits above; `bash tools/docs.sh validate` → OK.
- [ ] **Step 2:** Full gate: `buck2 build -v0 --console none -M none //src/...` → succeeds; `buck2 test --console none //src/...` (with `--unstable-allow-all-tests-on-re` on a root host, `-j 8` locally) → all pass.
- [ ] **Step 3:** prek + commit (`docs: close road-scheduled-maintenance-jobs — scheduled maintenance jobs capability`).

---

## Final review gate (per loom-work-checkout)

After all tasks: whole-branch review subagent + metric gates (`loom-complexity diff`, `loom-duplication diff`, print-only). Pre-known candidates: `job_schedules_contract` is one long test fn (testkit precedent — `transforms_contract` is the same shape, justified); the postgres `fire_due_job_schedules` is a loop-in-txn mirroring `claim_due_schedules` (flat, precedent-justified); the memory fire's insert-if-absent scan mirrors the queue-`Row` available-state idiom (`memory/src/queue.rs:28`) — if the reviewer flags it as duplication, extract a shared `insert_if_absent` helper on the memory adapter rather than waiving. Fix or justify anything new in the PR body.
