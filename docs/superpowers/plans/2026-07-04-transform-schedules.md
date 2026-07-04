# Transform ergonomics slice 2 — cron schedules Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `TransformDef.schedule` goes live — 5-field UTC cron validated at define, stored `next_run_at`, atomic `claim_due_schedules` on both adapters, and an engine-hosted scheduler loop creating `trigger: Schedule` runs (spec: `docs/superpowers/specs/2026-07-04-transform-ergonomics-design.md` §Schedules, item `road-transform-schedules`).

**Architecture:** Cron parsing lives in `control-plane/core` (`croner` crate — chrono 0.4.45 is already vendored via arrow, so one new crate) as `validate_cron`/`next_cron_occurrence` helpers; `validate_transform_def`'s slice-1 schedule rejection becomes real validation. The `Transforms` trait gains `claim_due_schedules` (atomic claim-and-advance: memory under the `transforms` mutex, postgres via `FOR UPDATE SKIP LOCKED`) and a `next_run_at` read. The engine service spawns its first background task: a tick loop that claims due defs and submits `trigger: Schedule` runs through the existing `submit_run`.

**Tech Stack:** Rust/buck2, croner (new, via reindeer), sqlx compile-time queries, tokio + tokio-util CancellationToken.

## Global Constraints

- **Schedule format:** standard 5-field cron, evaluated in **UTC**. Invalid expression → `Validation` → 400 at define. (If croner's default parser also tolerates a 6th seconds field, that leniency is acceptable — document it in the helper's doc comment.)
- **Claim semantics:** `claim_due_schedules(now, limit)` returns defs with `schedule` set and `next_run_at <= now`, atomically advancing each `next_run_at` to the next occurrence **after `now`** — concurrent claimers never both get the same due def. **Crash between claim and submit skips that occurrence (at-most-once per firing), never double-fires** — deliberate; record as a spec refinement.
- Define/upsert computes `next_run_at` from `now` when `schedule` is `Some`; clears it when `None`. A redefine resets the schedule clock — deliberate; record as refinement.
- `next_run_at` is **derived state, not part of `TransformDef`** (the authored shape stays byte-identical); it is exposed via a new trait read and on `GET /admin/transforms/{name}` only (`TransformDefView.next_run_at`, `skip_serializing_if = "Option::is_none"` so list output is unchanged).
- Scheduler tick: `LOOM_SCHEDULER_TICK_SECS`, default `5`, parsed via the `parse_var` convention; claim limit 32 per tick; per-def submit errors are logged (`tracing::warn!`) and never kill the loop.
- The `on_input_commit: true` rejection (slice 3) stays untouched.
- **After `cargo generate-lockfile`: diff `Cargo.lock` against the merge-base for unrelated churn** (native/`links` crates especially) — adding croner must add croner (+ its direct deps only). Then `./tools/buckify.sh` and commit `third-party/BUCK` together with the manifests.
- Tests are `rust_test` integration targets under `tests/` (never inline `#[cfg(test)]`); postgres fixture tests use `loom_fixture_test`; after SQL changes run `bash tools/sqlx-prepare.sh` and commit `.sqlx`.
- Never pipe `buck2 test` through head/tail — redirect to a file, grep it.
- Before EVERY commit: `buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -c Failed /tmp/prek.log` prints 0 (re-stage hook fixes; fix rustfmt manually).
- Production code: clippy pedantic+restriction (no unwrap/expect/panic/indexing).
- Every commit message ends with:
  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01KrF8cSY7q1odAy9AW6aAnd
  ```

**Spec refinements to record in the PR body:** (1) skip-not-double-fire crash semantics above; (2) redefine resets the schedule clock; (3) `next_run_at` exposed via a separate trait read instead of widening `TransformDef`; (4) scheduler-loop e2e is the memory-adapter tick test the spec names — no additional fixture e2e of the timer plumbing (the loop body is `tick()`, which is fully tested; the timer is `tokio::time::interval`).

---

### Task 1: croner dep + core cron helpers + validation swap

**Files:**
- Modify: `src/control-plane/core/Cargo.toml` (add `croner = "2"`)
- Modify: `Cargo.lock` (generate-lockfile), `third-party/BUCK` (buckify)
- Modify: `src/control-plane/core/BUCK` (deps += `//third-party:croner`)
- Modify: `src/control-plane/core/src/transforms.rs`
- Modify: `src/control-plane/core/tests/transforms.rs`

**Interfaces:**
- Produces: `pub fn validate_cron(expr: &str) -> Result<()>`; `pub fn next_cron_occurrence(expr: &str, after: OffsetDateTime) -> Result<OffsetDateTime>` (both re-exported from core's lib.rs alongside `validate_transform_def`); `validate_transform_def` now ACCEPTS valid cron schedules and rejects invalid ones.

- [ ] **Step 1: Add the dependency.** In `src/control-plane/core/Cargo.toml` under `[dependencies]`: `croner = "2"`. Then, with the hermetic toolchain (`eval "$(./tools/env.sh)"`): `cargo generate-lockfile`. **Guard:** `git diff origin/main -- Cargo.lock` must show ONLY croner (+ any new direct deps of croner not already in tree) — no version moves of existing crates (`zstd-sys`, `ring`, etc.). If unrelated churn appears, re-pin per the repo's lockfile discipline before proceeding. Then `./tools/buckify.sh` and check `git diff third-party/BUCK` adds a croner target (chrono already vendored). Add `"//third-party:croner"` to `src/control-plane/core/BUCK` deps.

- [ ] **Step 2: Write the failing tests** — append to `src/control-plane/core/tests/transforms.rs`:

```rust
#[test]
fn cron_validation_accepts_5_field_and_rejects_garbage() {
    use control_plane_core::validate_cron;
    assert!(validate_cron("0 3 * * *").is_ok());
    assert!(validate_cron("*/5 * * * *").is_ok());
    assert!(validate_cron("not a cron").is_err());
    assert!(validate_cron("99 99 99 99 99").is_err());
    assert!(validate_cron("").is_err());
}

#[test]
fn next_cron_occurrence_is_deterministic_utc() {
    use control_plane_core::next_cron_occurrence;
    use time::macros::datetime;
    // Hourly at :00, from 00:30 UTC -> 01:00 UTC the same day.
    let after = datetime!(2026-01-01 00:30 UTC);
    let next = next_cron_occurrence("0 * * * *", after).unwrap();
    assert_eq!(next, datetime!(2026-01-01 01:00 UTC));
    // Strictly after: from exactly 01:00, the next hourly fire is 02:00.
    let next2 = next_cron_occurrence("0 * * * *", next).unwrap();
    assert_eq!(next2, datetime!(2026-01-01 02:00 UTC));
}

#[test]
fn valid_schedule_now_passes_definition_validation() {
    // slice 2: schedules are live — a valid cron is accepted, an invalid one rejected.
    let mut def = TransformDef {
        name: TransformName("t".into()),
        body: physical_body(),
        schedule: Some("0 3 * * *".into()),
        on_input_commit: false,
    };
    assert!(validate_transform_def(&def).is_ok());
    def.schedule = Some("not a cron".into());
    assert!(validate_transform_def(&def).is_err());
}
```

Also UPDATE the existing `slice1_rejects_schedule_and_data_trigger` test: its schedule-rejection leg is now wrong — rename the fn to `rejects_data_trigger_until_slice_3` and keep only the `on_input_commit` leg (the schedule legs are covered by the new test above). Check whether `time` macros are already available to this test target (the `datetime!` macro needs the `time` crate's `macros` feature — check `third-party/BUCK`'s time target features; if `macros` is absent, construct via `OffsetDateTime::from_unix_timestamp(1767225000)`-style constants instead and assert unix timestamps).

- [ ] **Step 3: Run to verify failure** — `buck2 test //src/control-plane/core:transforms > /tmp/s2t1.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/s2t1.log` → compile error (`validate_cron` not found).

- [ ] **Step 4: Implement** in `src/control-plane/core/src/transforms.rs`:

```rust
/// Validate a cron expression (standard 5-field, UTC evaluation; croner's
/// parser also tolerates an optional seconds field — accepted leniency).
pub fn validate_cron(expr: &str) -> Result<()> {
    croner::Cron::new(expr)
        .parse()
        .map(|_| ())
        .map_err(|e| ControlPlaneError::Validation(format!("invalid cron expression: {e}")))
}

/// The next UTC occurrence of `expr` strictly after `after`.
pub fn next_cron_occurrence(expr: &str, after: OffsetDateTime) -> Result<OffsetDateTime> {
    let cron = croner::Cron::new(expr)
        .parse()
        .map_err(|e| ControlPlaneError::Validation(format!("invalid cron expression: {e}")))?;
    let after_c = chrono::DateTime::<chrono::Utc>::from_timestamp(
        after.unix_timestamp(),
        after.nanosecond(),
    )
    .ok_or_else(|| ControlPlaneError::Validation("timestamp out of range".into()))?;
    let next = cron
        .find_next_occurrence(&after_c, false)
        .map_err(|e| ControlPlaneError::Validation(format!("no next cron occurrence: {e}")))?;
    OffsetDateTime::from_unix_timestamp(next.timestamp())
        .map_err(|e| ControlPlaneError::Validation(format!("cron occurrence out of range: {e}")))
}
```

(Adjust to croner 2.x's actual API surface — `Cron::new(expr).parse()` returning `Result<Cron>` and `find_next_occurrence(&DateTime<Tz>, inclusive: bool)` is the documented shape; if the vendored version differs, e.g. builder methods for field counts, adapt and note in the report. `chrono` needs adding to core's Cargo.toml/BUCK too if not transitively importable — it IS a direct usage, so add `chrono = { version = "0.4", default-features = false, features = ["clock"] }`... check what feature set the vendored `chrono-0.4` target carries first and depend accordingly; `from_timestamp` and `Utc` need no exotic features.)

In `validate_transform_def`, replace the schedule-rejection block with:

```rust
    if let Some(expr) = &def.schedule {
        validate_cron(expr)?;
    }
```

Re-export the two helpers from `lib.rs` (extend the existing `pub use transforms::{...}` list).

- [ ] **Step 5: Run** — `buck2 test //src/control-plane/core: > /tmp/s2t1b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/s2t1b.log` → all pass. NOTE: the testkit contract still asserts schedule-rejection — the memory/postgres contract targets will FAIL from here until Task 2 updates the contract. That cross-task red is expected; do NOT run those targets in this task.

- [ ] **Step 6: prek + commit** (`feat(control-plane): live cron validation — croner helpers in core`). The reindeer-check hook runs on Cargo.toml/lock changes; buckify output must be committed in the same commit.

---

### Task 2: trait growth + memory adapter + contract update

**Files:**
- Modify: `src/control-plane/core/src/transforms.rs` (trait: 2 new methods)
- Modify: `src/control-plane/memory/src/transforms.rs`
- Modify: `src/control-plane/testkit/src/lib.rs` (contract update + schedule legs)
- Modify: `src/control-plane/memory/tests/transforms.rs` (nothing new expected — contract runs there)

**Interfaces:**
- Produces (Task 3/4 rely on):
  ```rust
  // on trait Transforms:
  /// Atomically claim schedule-due definitions: `schedule` set and
  /// `next_run_at <= now`, at most `limit`, advancing each claimed def's
  /// `next_run_at` to the next occurrence after `now`. Concurrent claimers
  /// never both receive the same due def. A claimed occurrence that the
  /// caller fails to submit is SKIPPED, not retried (at-most-once).
  async fn claim_due_schedules(&self, now: OffsetDateTime, limit: u32) -> Result<Vec<TransformDef>>;
  /// Derived schedule state: when the def would next fire (`None` when
  /// unscheduled). `NotFound` for an unknown transform.
  async fn next_run_at(&self, name: &TransformName) -> Result<Option<OffsetDateTime>>;
  ```
- Memory semantics: `TransformsState` gains `pub(crate) next_run_at: HashMap<String, OffsetDateTime>`; `define_transform` computes/clears it (after validation, inside the same `transforms` lock as the def insert); `delete_transform` removes it. Lock order unchanged (`transforms` still last; these methods take only `transforms`).

- [ ] **Step 1: Update the testkit contract.** In `transforms_contract`, REPLACE the slice-1 schedule-rejection block (the `let sched = ...` + assert at testkit/src/lib.rs:4125-4135) with schedule-lifecycle legs (the `on_input_commit` rejection block stays):

```rust
    // Schedules (slice 2): invalid cron rejected; valid cron accepted with
    // derived next_run_at in the future; unscheduling clears it.
    let bad_cron = TransformDef { schedule: Some("not a cron".into()), ..def.clone() };
    assert!(matches!(
        cp.define_transform(bad_cron).await,
        Err(ControlPlaneError::Validation(_))
    ), "invalid cron rejected");
    let scheduled = TransformDef {
        name: TransformName("nightly".into()),
        body: def.body.clone(),
        schedule: Some("0 3 * * *".into()),
        on_input_commit: false,
    };
    cp.define_transform(scheduled.clone()).await.unwrap();
    let before = time::OffsetDateTime::now_utc();
    let nra = cp.next_run_at(&scheduled.name).await.unwrap().expect("scheduled => Some");
    assert!(nra > before, "next_run_at is in the future");
    assert_eq!(cp.next_run_at(&def.name).await.unwrap(), None, "unscheduled => None");
    assert!(matches!(
        cp.next_run_at(&TransformName("nope".into())).await,
        Err(ControlPlaneError::NotFound(_))
    ));

    // Claim: not due yet -> empty; due (probe far in the future) -> claimed
    // exactly once with next_run_at advanced past the probe; immediate
    // re-claim at the same probe -> empty (advance happened atomically).
    let none_due = cp.claim_due_schedules(before, 32).await.unwrap();
    assert!(none_due.is_empty(), "nothing due before next_run_at");
    let probe = before + time::Duration::days(2);
    let claimed = cp.claim_due_schedules(probe, 32).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].name, scheduled.name);
    let advanced = cp.next_run_at(&scheduled.name).await.unwrap().expect("still scheduled");
    assert!(advanced > probe, "claim advanced next_run_at past the probe");
    assert!(cp.claim_due_schedules(probe, 32).await.unwrap().is_empty(), "claim-once");

    // Concurrent claimers: exactly one wins the single due def.
    let probe2 = advanced + time::Duration::days(2);
    let (a, b) = tokio::join!(
        cp.claim_due_schedules(probe2, 32),
        cp.claim_due_schedules(probe2, 32)
    );
    let total = a.unwrap().len() + b.unwrap().len();
    assert_eq!(total, 1, "concurrent claims never double-fire");

    // Unschedule clears the derived state.
    let unscheduled = TransformDef { schedule: None, ..scheduled.clone() };
    cp.define_transform(unscheduled).await.unwrap();
    assert_eq!(cp.next_run_at(&scheduled.name).await.unwrap(), None);
```

Place this where the old rejection block was; the later list-ordering assert expects defs named `daily`/`typed` — the new `nightly` def changes the expected list. UPDATE the list assertion accordingly (it becomes `["daily", "nightly", "typed"]` at that point in the flow — check the exact sequence when editing; the contract must stay internally consistent, and the delete-leg at the bottom operates on `daily` unchanged).

- [ ] **Step 2: Run to verify failure** — `buck2 test //src/control-plane/memory:transforms > /tmp/s2t2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/s2t2.log` → compile error (`claim_due_schedules` not on the trait).

- [ ] **Step 3: Implement.** Trait methods (doc comments from Interfaces above) in core; memory impl in `src/control-plane/memory/src/transforms.rs`:

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn claim_due_schedules(
        &self,
        now: OffsetDateTime,
        limit: u32,
    ) -> Result<Vec<TransformDef>> {
        let mut st = self.transforms.lock();
        let mut due: Vec<String> = st
            .next_run_at
            .iter()
            .filter(|(_, at)| **at <= now)
            .map(|(name, _)| name.clone())
            .collect();
        due.sort(); // deterministic order
        due.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        let mut claimed = Vec::with_capacity(due.len());
        for name in due {
            let Some(def) = st.defs.get(&name).cloned() else { continue };
            let Some(expr) = def.schedule.as_deref() else { continue };
            let next = next_cron_occurrence(expr, now)?;
            st.next_run_at.insert(name, next);
            claimed.push(def);
        }
        Ok(claimed)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn next_run_at(&self, name: &TransformName) -> Result<Option<OffsetDateTime>> {
        let st = self.transforms.lock();
        if !st.defs.contains_key(&name.0) {
            return Err(ControlPlaneError::NotFound(format!("transform {}", name.0)));
        }
        Ok(st.next_run_at.get(&name.0).copied())
    }
```

`define_transform` gains, inside its existing `transforms` lock section (after validation): compute `let next = def.schedule.as_deref().map(|e| next_cron_occurrence(e, time::OffsetDateTime::now_utc())).transpose()?;` BEFORE taking the lock (it's pure), then under the lock `match next { Some(n) => { st.next_run_at.insert(def.name.0.clone(), n); } None => { st.next_run_at.remove(&def.name.0); } }` alongside the def insert. `delete_transform` also removes the entry. Add the needed imports (`next_cron_occurrence`, `OffsetDateTime`).

- [ ] **Step 4: Run** — memory transforms target → PASS. NOTE: the postgres contract target is still red until Task 3 — expected, don't run it.

- [ ] **Step 5: prek + commit** (`feat(control-plane): claim_due_schedules + next_run_at — trait, memory, contract`)

---

### Task 3: postgres adapter — migration 0032, claim, `.sqlx`

**Files:**
- Create: `src/control-plane/postgres/migrations/0032_transform_schedule.sql`
- Modify: `src/control-plane/postgres/src/transforms.rs`
- Regenerate: `src/control-plane/postgres/.sqlx/`

**Interfaces:**
- Consumes: Task 2's trait methods + contract.

- [ ] **Step 1: Migration** `0032_transform_schedule.sql`:

```sql
-- Slice 2: derived schedule state. next_run_at is computed at define/claim
-- time from the cron expression; the partial index serves the scheduler scan.
alter table transforms.transform add column next_run_at timestamptz;

create index transform_due on transforms.transform (next_run_at)
    where schedule is not null;
```

- [ ] **Step 2: Run the (updated) contract to verify failure** — `buck2 test //src/control-plane/postgres:transforms --unstable-allow-all-tests-on-re > /tmp/s2t3.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/s2t3.log` → compile error (trait methods unimplemented for PgControlPlane).

- [ ] **Step 3: Implement** in `src/control-plane/postgres/src/transforms.rs`:

`define_transform`: compute `let next_run_at = def.schedule.as_deref().map(|e| next_cron_occurrence(e, OffsetDateTime::now_utc())).transpose()?;` after `validate_transform_def`, and extend the upsert:

```rust
        sqlx::query!(
            "insert into transforms.transform (name, body, schedule, on_input_commit, next_run_at) \
             values ($1, $2, $3, $4, $5) \
             on conflict (name) do update set \
                 body = excluded.body, schedule = excluded.schedule, \
                 on_input_commit = excluded.on_input_commit, \
                 next_run_at = excluded.next_run_at",
            def.name.0,
            body,
            def.schedule,
            def.on_input_commit,
            next_run_at,
        )
```

New trait methods:

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn claim_due_schedules(
        &self,
        now: OffsetDateTime,
        limit: u32,
    ) -> Result<Vec<TransformDef>> {
        let mut tx = self.pool().begin().await.map_err(backend)?;
        let rows = sqlx::query!(
            "select name, body, schedule, on_input_commit from transforms.transform \
             where schedule is not null and next_run_at is not null and next_run_at <= $1 \
             order by next_run_at, name \
             limit $2 \
             for update skip locked",
            now,
            i64::from(limit),
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(backend)?;
        let mut claimed = Vec::with_capacity(rows.len());
        for r in rows {
            let def = TransformDef {
                name: TransformName(r.name),
                body: de_body(r.body)?,
                schedule: r.schedule,
                on_input_commit: r.on_input_commit,
            };
            let Some(expr) = def.schedule.as_deref() else { continue };
            let next = next_cron_occurrence(expr, now)?;
            sqlx::query!(
                "update transforms.transform set next_run_at = $2 where name = $1",
                def.name.0,
                next,
            )
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
            claimed.push(def);
        }
        tx.commit().await.map_err(backend)?;
        Ok(claimed)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn next_run_at(&self, name: &TransformName) -> Result<Option<OffsetDateTime>> {
        let row = sqlx::query!(
            "select next_run_at from transforms.transform where name = $1",
            name.0,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(format!("transform {}", name.0)))?;
        Ok(row.next_run_at)
    }
```

(Imports: `next_cron_occurrence`, `time::OffsetDateTime`. `for update skip locked` inside `query!` is fine; nullability of `next_run_at` in the returned row is `Option<OffsetDateTime>` naturally.)

- [ ] **Step 4:** `bash tools/sqlx-prepare.sh`; commit the `.sqlx` diff (expect updated define hash + 3 new query files).

- [ ] **Step 5: Run** — postgres transforms contract + `sqlx-cache-check`, both `--unstable-allow-all-tests-on-re` → PASS. Then `buck2 build -M none //src/... > /tmp/s2t3b.log 2>&1; tail -3 /tmp/s2t3b.log` → SUCCEEDED (trait change breaks nothing else — Wire/Iceberg delegate at the accessor level).

- [ ] **Step 6: prek + commit** (`feat(control-plane): postgres schedule claim — migration 0032, skip-locked advance`)

---

### Task 4: engine scheduler loop

**Files:**
- Create: `src/services/engine/src/scheduler.rs`
- Modify: `src/services/engine/src/lib.rs` (module + re-export as needed)
- Modify: `src/services/engine/src/run.rs` (spawn + cancel around serve)
- Modify: `src/services/engine/src/tuning.rs` or wherever `EngineTuning::from_map` lives (new knob — find it: `grep -rn "EngineTuning" src/services/engine/src/`)
- Create: `src/services/engine/tests/scheduler.rs`
- Modify: `src/services/engine/BUCK` (new plain `rust_test` + `//src/control-plane/memory:memory` test dep; `tokio-util` dep for the lib if not present)

**Interfaces:**
- Consumes: `claim_due_schedules`, `submit_run`, `TransformBody::to_job`, `RunTrigger::Schedule`.
- Produces:
  ```rust
  /// One scheduler pass: claim due schedules and submit a `Schedule` run per
  /// claimed def. Per-def submit failures are logged and skipped (the claim
  /// already advanced next_run_at — at-most-once). Returns submitted count.
  pub async fn tick(cp: &(dyn ControlPlane), now: OffsetDateTime, limit: u32) -> usize;
  /// Tick every `every` until cancelled.
  pub async fn scheduler_loop(cp: Arc<dyn ControlPlane>, every: Duration, cancel: CancellationToken);
  ```

- [ ] **Step 1: Write the failing test** — `src/services/engine/tests/scheduler.rs`:

```rust
//! Scheduler tick against the memory control plane: a due schedule fires
//! exactly once as a `trigger: Schedule` run.

use std::time::Duration;

use control_plane_core::{
    ControlPlane, OutputMode, PageReq, RunState, RunTrigger, TableRef, TransformBody,
    TransformDef, TransformName, Transforms,
};
use control_plane_memory::MemoryControlPlane;

fn scheduled_def(name: &str) -> TransformDef {
    TransformDef {
        name: TransformName(name.into()),
        body: TransformBody::Physical {
            inputs: vec![TableRef { schema: "main".into(), name: "src".into() }],
            output: TableRef { schema: "main".into(), name: "dst".into() },
            sql: "select * from src".into(),
            output_mode: OutputMode::Append,
        },
        schedule: Some("0 3 * * *".into()),
        on_input_commit: false,
    }
}

#[tokio::test]
async fn due_schedule_fires_once_as_schedule_run() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_transform(scheduled_def("nightly")).await.unwrap();

    // Not due yet: nothing fires.
    let now = time::OffsetDateTime::now_utc();
    assert_eq!(engine::scheduler::tick(&cp, now, 32).await, 0);

    // Probe two days ahead: exactly one fire, trigger Schedule, job enqueued.
    let probe = now + time::Duration::days(2);
    assert_eq!(engine::scheduler::tick(&cp, probe, 32).await, 1);
    let runs = cp.transforms().list_runs(None, PageReq::default()).await.unwrap();
    assert_eq!(runs.items.len(), 1);
    let run = &runs.items[0];
    assert_eq!(run.trigger, RunTrigger::Schedule);
    assert_eq!(run.state, RunState::Queued);
    assert_eq!(run.transform, Some(TransformName("nightly".into())));
    let job = cp
        .queue()
        .dequeue(&["transform".to_string()], "sched-test")
        .await
        .unwrap()
        .expect("scheduled job enqueued");
    assert_eq!(job.payload["run_id"], serde_json::json!(run.run_id.to_string()));

    // Same probe again: the claim already advanced next_run_at — no re-fire.
    assert_eq!(engine::scheduler::tick(&cp, probe, 32).await, 0);
}
```

BUCK target (plain `rust_test`, engine's unit-test style — mirror `engine_tuning`): deps `[":engine", "//src/control-plane/core:core", "//src/control-plane/memory:memory", "//third-party:serde_json", "//third-party:time", "//third-party:tokio"]`. (Check the engine lib target's name and whether `engine::scheduler` needs `pub mod`.)

- [ ] **Step 2: Run to verify failure** — `buck2 test //src/services/engine:scheduler > /tmp/s2t4.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/s2t4.log` → compile error (no `scheduler` module).

- [ ] **Step 3: Implement** `src/services/engine/src/scheduler.rs`:

```rust
//! The transform scheduler: the engine's background loop that fires cron
//! schedules. Each pass claims due definitions (the claim itself advances
//! `next_run_at`, so a crash after claiming skips the occurrence rather than
//! double-firing) and submits one `trigger: Schedule` run per claim.

use std::sync::Arc;
use std::time::Duration;

use control_plane_core::{ControlPlane, RunState, RunTrigger, TransformRun};
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

/// One scheduler pass; returns how many runs were submitted.
pub async fn tick(cp: &(dyn ControlPlane), now: OffsetDateTime, limit: u32) -> usize {
    let due = match cp.transforms().claim_due_schedules(now, limit).await {
        Ok(due) => due,
        Err(e) => {
            tracing::warn!(error = %e, "scheduler: claim_due_schedules failed");
            return 0;
        }
    };
    let mut submitted = 0;
    for def in due {
        let run_id = uuid::Uuid::new_v4();
        let run = TransformRun {
            run_id,
            transform: Some(def.name.clone()),
            trigger: RunTrigger::Schedule,
            state: RunState::Queued,
            body: def.body.clone(),
            queued_at: OffsetDateTime::now_utc(),
            started_at: None,
            finished_at: None,
            snapshot_id: None,
            error: None,
        };
        match cp.transforms().submit_run(run, def.body.to_job(run_id)).await {
            Ok(_) => submitted += 1,
            Err(e) => {
                // The occurrence is skipped (claim already advanced the clock).
                tracing::warn!(transform = %def.name.0, error = %e, "scheduler: submit_run failed");
            }
        }
    }
    submitted
}

/// Tick every `every` until `cancel` fires.
pub async fn scheduler_loop(
    cp: Arc<dyn ControlPlane>,
    every: Duration,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(every);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            _ = interval.tick() => {
                let n = tick(cp.as_ref(), OffsetDateTime::now_utc(), 32).await;
                if n > 0 {
                    tracing::info!(submitted = n, "scheduler: fired due transforms");
                }
            }
        }
    }
}
```

Wire the knob: wherever `EngineTuning::from_map` lives, add `scheduler_tick: Duration` parsed as `Duration::from_secs(parse_var(vars, "LOOM_SCHEDULER_TICK_SECS", 5_u64)?)` (mirror the `LOOM_GC_RETENTION_SECS` precedent exactly, including its test file if `engine_tuning.rs` tests knob parsing — add a leg there). In `run.rs`, before `Server::builder()`:

```rust
    let sched_cancel = CancellationToken::new();
    let sched = tokio::spawn(scheduler::scheduler_loop(
        Arc::new(cp.clone()),
        tuning.scheduler_tick,
        sched_cancel.clone(),
    ));
```

and after `.serve_with_incoming_shutdown(...).await?`:

```rust
    sched_cancel.cancel();
    drop(sched.await);
```

(`cp` is the `PgControlPlane` built at the top of `run` — it is `Clone`; `Arc<PgControlPlane>` coerces to `Arc<dyn ControlPlane>`. If `tuning` isn't in scope where needed, thread it. Add `tokio-util` to the engine lib's BUCK deps + Cargo.toml if absent — check; the worker already depends on it, so the crate is vendored.)

- [ ] **Step 4: Run** — scheduler test PASS; then existing engine tests (`buck2 test //src/services/engine: --unstable-allow-all-tests-on-re > /tmp/s2t4b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/s2t4b.log`) → all pass (the loop spawn must not disturb the wire tests, which drive `engine::run` — they'll now spawn an idle scheduler too; if a wire test uses a `pause`d tokio clock or asserts exact task counts, adjust; none is expected to).

- [ ] **Step 5: prek + commit** (`feat(engine): transform scheduler loop — claim-and-fire on LOOM_SCHEDULER_TICK_SECS`)

---

### Task 5: admin surface — schedule accepted, next_run_at exposed

**Files:**
- Modify: `src/services/runtime/src/admin.rs`
- Modify: `src/services/runtime/tests/admin_management.rs`

**Interfaces:**
- Consumes: `next_run_at` trait read; live cron validation via `define_transform`.

- [ ] **Step 1: Write the failing tests.** In `admin_management.rs`:
  - UPDATE `define_transform_rejects_bad_shapes`: the `"* * * * *"` schedule leg currently asserts 400 — a valid cron is now ACCEPTED; change that leg to assert `StatusCode::CREATED`, and add an invalid-cron leg (`"schedule": "not a cron"`) asserting 400.
  - ADD `scheduled_transform_exposes_next_run_at`:

```rust
#[tokio::test]
async fn scheduled_transform_exposes_next_run_at() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    let scheduled = TRANSFORM_BODY.replace(
        r#""name": "daily""#,
        r#""name": "daily", "schedule": "0 3 * * *""#,
    );
    let (status, _) = send(app(cp.clone()), req_json("POST", "/admin/transforms", &token, &scheduled)).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = send(app(cp.clone()), req_empty("GET", "/admin/transforms/daily", &token)).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["schedule"], "0 3 * * *");
    let nra = v["next_run_at"].as_str().expect("next_run_at present when scheduled");
    assert!(nra.contains('T'), "RFC3339 timestamp: {nra}");
    // Unscheduled defs omit the field entirely.
    let (_, body) = send(app(cp.clone()), req_json("POST", "/admin/transforms", &token, TRANSFORM_BODY)).await;
    let (_, body) = send(app(cp), req_empty("GET", "/admin/transforms/daily", &token)).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(v.get("next_run_at").is_none(), "field omitted when unscheduled");
}
```

(The second half redefines `daily` without a schedule and asserts omission — keep the double-`body` shadowing tidy when writing it for real.)

- [ ] **Step 2: Run to verify failure** — `buck2 test //src/services/runtime: > /tmp/s2t5.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/s2t5.log` → the updated legs fail (schedule accepted → old 400 assert breaks; `next_run_at` absent).

- [ ] **Step 3: Implement** in `admin.rs`:
  - `TransformDefView` gains:
    ```rust
        /// Next scheduled fire (RFC3339, UTC) — present only when scheduled.
        #[serde(skip_serializing_if = "Option::is_none")]
        next_run_at: Option<String>,
    ```
    `def_view(d)` sets `next_run_at: None` (list responses unchanged).
  - `get_transform_route`: after fetching the def, fetch `st.cp.transforms().next_run_at(&name)` (ignore `NotFound` races by mapping errors through `status_for` as usual — fetch def first, then next_run_at; a `NotFound` between the two is a legitimate 404), build `let mut view = def_view(&def); view.next_run_at = nra.map(rfc3339);` and return it.
  - Update `define_transform_route`'s `#[utoipa::path]` request-body description + 400 description: schedule is now a live 5-field UTC cron (invalid → 400); mention `next_run_at` on the GET route's 200 description.

- [ ] **Step 4: Run** — runtime package → all pass. Also the query-api openapi drift target (no route changes — should stay green; run it to be sure).

- [ ] **Step 5: prek + commit** (`feat(api): accept cron schedules + expose next_run_at on transform detail`)

---

### Task 6: docs, register close, full sweep

**Files:**
- Modify: `docs/system-capabilities/transform.md` (schedules capability paragraph; Known gaps drop the scheduling gap, keep data triggers)
- Modify: `docs/system-capabilities/engine.md` (scheduler loop — the engine's first background task; knob `LOOM_SCHEDULER_TICK_SECS` default 5)
- Modify: `docs/system-capabilities/control-plane.md` (transforms concern paragraph gains claim_due_schedules/next_run_at + the skip-not-double-fire semantics)
- Modify: `docs/ROADMAP.md` (REMOVE `road-transform-schedules`; slice-3's entry does not reference it — verify with grep)
- Modify: `docs/FUTURE.md` (**the `#fut-scheduled-jobs` entry links `[[road-transform-schedules]]` — that would dangle.** Reword to: "Scheduled *transforms* landed (`#road-transform-schedules`, PR #NN) — this item is the residual: a generic schedule surface for maintenance kinds (GC, compaction), presumably reusing the croner/`next_run_at`/atomic-claim mechanism.")

**Steps:**

- [ ] **Step 1:** Docs edits above; `bash tools/docs.sh validate` → OK (`grep -rn "road-transform-schedules" docs/` must show only code-span prose mentions, no `[[…]]`).
- [ ] **Step 2:** Full gate: `buck2 build -M none //src/... > /tmp/s2t6.log 2>&1; tail -3 /tmp/s2t6.log` → SUCCEEDED; `buck2 test //src/... --unstable-allow-all-tests-on-re > /tmp/s2t6b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/s2t6b.log` → all pass (single fixture flake: re-run that target once).
- [ ] **Step 3:** prek + commit (`docs: close road-transform-schedules — cron schedules capability`). `#NN` placeholders patched by the controller after the PR exists (targeted sed, branch-owned files only).

---

## Final review gate (per loom-work-checkout)

After all tasks: final whole-branch review subagent + metric gates (`loom-complexity diff`, `loom-duplication diff`, print-only). Pre-known candidates: `transforms_contract` grows again (test scaffolding, precedent-justified); `claim_due_schedules` postgres impl is a loop-in-txn (flat); the scheduler `tick` is small. Fix or justify anything new in the PR body.
