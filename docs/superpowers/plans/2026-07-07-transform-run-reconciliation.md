# Transform run reconciliation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reconciliation sweep that fails-out transform runs stuck `Running` because a terminal `FinishRunFailed` report was lost — a new `Transforms::reconcile_stranded_runs` trait method (both adapters + contract), an engine `reconcile_loop` that drives it, and a behavior-preserving worker cleanup.

**Architecture:** A run is *stranded* iff it reads `Running`, its queue job is no longer live (neither `available` nor `running` in `queue.jobs`), and its `started_at` predates a caller-supplied cutoff (a grace guard against the two-step terminal-report race). The sweep marks each `Failed("reporting lost")` — the same terminal write `finish_run(Failed)` performs. The engine (sole Postgres owner) hosts the loop, mirroring `scheduler_loop`. The worker cleanup dedups the `mark_run_running` block and drops a redundant param.

**Tech Stack:** Rust, sqlx (one NEW `query!` → **`.sqlx` regen required**), buck2 fixture + testkit contract, tokio interval loop + `CancellationToken`.

## Global Constraints

- **`.sqlx` regen required** — Task 1 adds ONE new postgres `query!` (the cross-schema `UPDATE … RETURNING`). Run `tools/sqlx-prepare.sh` and commit the new `.sqlx/*.json`. The `//src/control-plane/postgres:sqlx-cache-check` test must be green after.
- **Sweep predicate is exact and identical across adapters**: `state='running' AND started_at <= cutoff AND NOT (a live job with matching run_id exists)`, where *live* ⟺ queue job `state IN ('available','running')`. Idempotent — an already-`Failed`/`Succeeded`/`Queued` run is never matched.
- **Terminal write matches `finish_run(Failed)`**: `state='failed'`, `error='reporting lost'`, `finished_at=now()`, `snapshot_id` untouched (stays null).
- **No change to the worker's best-effort reporting** (Task 3 is pure refactor, no behavior change) and **no change to the queue's crashed-worker reclaim**.
- **Tests are `rust_test` integration targets** — contract cases in testkit (run against both adapters), engine test mirrors the existing scheduler test.
- **Clippy strict** on production code — no `unwrap`/`expect`/indexing; errors via `map_err(backend)` / `?`.
- Commit messages end with the two required trailers; subjects follow Conventional Commits.

---

### Task 1: `reconcile_stranded_runs` trait method, both adapters, testkit contract

**Files:**
- Modify: `src/control-plane/core/src/transforms.rs:398` (add the trait method after `finish_run`)
- Modify: `src/control-plane/postgres/src/transforms.rs` (impl after `finish_run` ~line 456; NEW `query!`)
- Modify: `src/control-plane/memory/src/transforms.rs` (impl after `finish_run` ~line 147)
- Modify: `src/control-plane/testkit/src/lib.rs` (contract cases after the run-lifecycle block ~line 4546)
- Regenerate: `src/control-plane/postgres/.sqlx/` (via `tools/sqlx-prepare.sh`)

**Interfaces:**
- Produces: `async fn reconcile_stranded_runs(&self, running_since_before: OffsetDateTime) -> Result<Vec<Uuid>>` on `Transforms`. Task 2 consumes it via `ControlPlane::transforms()`.
- Consumes: `RunState::{Running,Failed}`; `TransformRun` fields `state`/`started_at`/`error`/`finished_at`/`run_id`; postgres `backend` helper + `queue.jobs(payload jsonb, state text)`; memory `self.rows: Arc<Mutex<Vec<Row>>>` (`Row.payload: serde_json::Value`, `Row.state: &'static str`) + `self.transforms` (`runs: HashMap<Uuid, TransformRun>`); testkit `cp.queue().dequeue/fail`, `RetryPolicy::Abandon`.

- [ ] **Step 1: Write the failing contract cases**

In `src/control-plane/testkit/src/lib.rs`, immediately after the existing run-lifecycle block (the `RunOutcome::Succeeded { snapshot_id: 41 }` assertions end ~line 4546), add (uses `cp.queue()`, `RetryPolicy` — both already in scope in this contract):

```rust
    // --- reconcile_stranded_runs: a Running run whose queue job is no longer
    // live (terminal-report lost) is swept to Failed("reporting lost") ---
    let mk_run = |rid: uuid::Uuid| TransformRun {
        run_id: rid,
        transform: Some(TransformName("daily".into())),
        trigger: RunTrigger::Manual,
        state: RunState::Queued,
        body: redefined.body.clone(),
        queued_at: time::OffsetDateTime::now_utc(),
        started_at: None,
        finished_at: None,
        snapshot_id: None,
        error: None,
    };
    let future = || time::OffsetDateTime::now_utc() + time::Duration::hours(1);
    let past = || time::OffsetDateTime::now_utc() - time::Duration::hours(1);

    // (1) Stranded -> Failed: dequeued, marked running, then job abandoned
    // (terminal) with the run never finished.
    let s_rid = uuid::Uuid::new_v4();
    let s_job = redefined.body.to_job(s_rid);
    let s_kinds = vec![s_job.kind.clone()];
    cp.submit_run(mk_run(s_rid), s_job).await.unwrap();
    let s_dq = cp.queue().dequeue(&s_kinds, "w").await.unwrap().expect("s job");
    cp.mark_run_running(s_rid).await.unwrap();
    cp.queue().fail(s_dq.id, "boom", RetryPolicy::Abandon).await.unwrap();
    let swept = cp.reconcile_stranded_runs(future()).await.unwrap();
    assert!(swept.contains(&s_rid), "stranded run is swept: {swept:?}");
    let r = cp.get_run(s_rid).await.unwrap();
    assert_eq!(r.state, RunState::Failed);
    assert_eq!(r.error.as_deref(), Some("reporting lost"));
    assert!(r.finished_at.is_some());

    // (1b) Idempotent: a second sweep does not re-touch the now-Failed run.
    assert!(
        !cp.reconcile_stranded_runs(future()).await.unwrap().contains(&s_rid),
        "already-Failed run is not re-swept"
    );

    // (2) Live in-flight (job 'running', not failed) is NOT swept.
    let l_rid = uuid::Uuid::new_v4();
    let l_job = redefined.body.to_job(l_rid);
    let l_kinds = vec![l_job.kind.clone()];
    cp.submit_run(mk_run(l_rid), l_job).await.unwrap();
    let _l_dq = cp.queue().dequeue(&l_kinds, "w").await.unwrap().expect("l job");
    cp.mark_run_running(l_rid).await.unwrap();
    assert!(
        !cp.reconcile_stranded_runs(future()).await.unwrap().contains(&l_rid),
        "in-flight run is not swept"
    );
    assert_eq!(cp.get_run(l_rid).await.unwrap().state, RunState::Running);

    // (4) Grace guard: a fresh Running run with a failed job is NOT swept when the
    // cutoff predates started_at; a later cutoff catches it.
    let g_rid = uuid::Uuid::new_v4();
    let g_job = redefined.body.to_job(g_rid);
    let g_kinds = vec![g_job.kind.clone()];
    cp.submit_run(mk_run(g_rid), g_job).await.unwrap();
    let g_dq = cp.queue().dequeue(&g_kinds, "w").await.unwrap().expect("g job");
    cp.mark_run_running(g_rid).await.unwrap();
    cp.queue().fail(g_dq.id, "boom", RetryPolicy::Abandon).await.unwrap();
    assert!(
        !cp.reconcile_stranded_runs(past()).await.unwrap().contains(&g_rid),
        "grace: a fresh run is not swept by an earlier cutoff"
    );
    assert_eq!(cp.get_run(g_rid).await.unwrap().state, RunState::Running);
    assert!(
        cp.reconcile_stranded_runs(future()).await.unwrap().contains(&g_rid),
        "a later cutoff catches the stranded run"
    );

    // (3) Live available (job still 'available', self-heal case) is NOT swept.
    // MUST run LAST: it deliberately leaves an un-dequeued 'available' job in the
    // shared queue pool. All scenarios build jobs from `redefined.body` (one
    // physical job kind), and dequeue returns the earliest available row — so an
    // available job left dangling before a later scenario's dequeue would be
    // returned in place of that scenario's own job. Keeping (3) last avoids it.
    let a_rid = uuid::Uuid::new_v4();
    let a_job = redefined.body.to_job(a_rid);
    cp.submit_run(mk_run(a_rid), a_job).await.unwrap(); // job stays 'available'
    cp.mark_run_running(a_rid).await.unwrap();
    assert!(
        !cp.reconcile_stranded_runs(future()).await.unwrap().contains(&a_rid),
        "run with an available job is not swept"
    );
```

- [ ] **Step 2: Run the contract to verify it fails (RED)**

Run: `buck2 test --console none //src/control-plane/testkit/...`
Expected: FAILS TO COMPILE — `reconcile_stranded_runs` is not a method on `Transforms`. (This is the expected RED for a new-trait-method contract; the method must exist on the trait + both adapters before it compiles.)

- [ ] **Step 3: Add the trait method (core)**

In `src/control-plane/core/src/transforms.rs`, after `finish_run` (line 398) add:

```rust
    /// Sweep runs stuck `Running` whose queue job is no longer live (neither
    /// `available` nor in-flight `running`) and whose `started_at` predates
    /// `running_since_before`: mark each `Failed("reporting lost")`. Returns the
    /// swept run ids. Runs with a live job are left untouched — they self-heal on
    /// retry or crashed-worker reclaim.
    async fn reconcile_stranded_runs(
        &self,
        running_since_before: OffsetDateTime,
    ) -> Result<Vec<Uuid>>;
```

- [ ] **Step 4: Postgres impl (NEW query!)**

In `src/control-plane/postgres/src/transforms.rs`, after `finish_run` (~line 456) add:

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn reconcile_stranded_runs(
        &self,
        running_since_before: OffsetDateTime,
    ) -> Result<Vec<Uuid>> {
        let rows = sqlx::query!(
            "update transforms.run as r \
                set state = 'failed', error = 'reporting lost', finished_at = now() \
              where r.state = 'running' \
                and r.started_at <= $1 \
                and not exists ( \
                  select 1 from queue.jobs j \
                   where j.state in ('available', 'running') \
                     and (j.payload->>'run_id')::uuid = r.run_id \
                ) \
            returning r.run_id",
            running_since_before,
        )
        .fetch_all(self.pool())
        .await
        .map_err(backend)?;
        Ok(rows.into_iter().map(|r| r.run_id).collect())
    }
```

- [ ] **Step 5: Memory impl**

In `src/control-plane/memory/src/transforms.rs`, after `finish_run` (~line 147) add (lock `rows` first into a set, drop it, then lock `transforms` — never nested, matching the adapter's lock-ordering discipline):

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn reconcile_stranded_runs(
        &self,
        running_since_before: OffsetDateTime,
    ) -> Result<Vec<Uuid>> {
        // Snapshot the run_ids that still have a LIVE queue job (available or
        // running). run_id rides in the job payload as a string.
        let live: std::collections::HashSet<String> = {
            let rows = self.rows.lock();
            rows.iter()
                .filter(|r| matches!(r.state, "available" | "running"))
                .filter_map(|r| {
                    r.payload.get("run_id").and_then(|v| v.as_str()).map(String::from)
                })
                .collect()
        };
        let mut swept = Vec::new();
        let mut st = self.transforms.lock();
        for run in st.runs.values_mut() {
            if run.state == RunState::Running
                && run.started_at.is_some_and(|s| s <= running_since_before)
                && !live.contains(&run.run_id.to_string())
            {
                run.state = RunState::Failed;
                run.error = Some("reporting lost".into());
                run.finished_at = Some(OffsetDateTime::now_utc());
                swept.push(run.run_id);
            }
        }
        Ok(swept)
    }
```

- [ ] **Step 6: Regenerate `.sqlx`, then run the contract (GREEN)**

Run: `bash tools/sqlx-prepare.sh` (boots the pinned postgres, applies migrations, `cargo sqlx prepare`), then confirm a new `.sqlx/query-*.json` appeared.
Run: `buck2 test --console none //src/control-plane/testkit/... //src/control-plane/postgres/... //src/control-plane/memory/...`
Expected: `Pass N. Fail 0` — the reconcile contract passes on BOTH adapters; `sqlx-cache-check` green.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/core/src/transforms.rs \
        src/control-plane/postgres/src/transforms.rs \
        src/control-plane/memory/src/transforms.rs \
        src/control-plane/testkit/src/lib.rs \
        src/control-plane/postgres/.sqlx
git commit -m "feat(transform): reconcile_stranded_runs sweep for lost terminal reports"
```

---

### Task 2: Engine `reconcile_loop` + tuning + startup wiring + test

**Files:**
- Modify: `src/services/engine/src/scheduler.rs` (add `reconcile_loop` next to `scheduler_loop`)
- Modify: `src/services/engine/src/run.rs` (add `reconcile_tick`/`reconcile_grace` to `EngineTuning` + defaults; spawn + cancel the loop)
- Modify: `src/services/engine/BUCK` (add `//third-party:uuid` to the `scheduler` `rust_test` target's `deps`)
- Test: `src/services/engine/tests/scheduler.rs` (a reconcile test mirroring `due_schedule_fires_once_as_schedule_run`)

**Interfaces:**
- Consumes: `Transforms::reconcile_stranded_runs` (Task 1) via `ControlPlane::transforms()` (`transaction.rs:32`); the `scheduler_loop` shape (`scheduler.rs:53-67`); `EngineTuning` (`run.rs:33`).

- [ ] **Step 0: Add the `uuid` test dep**

The new test calls `uuid::Uuid::new_v4()` but the `scheduler` `rust_test` target in `src/services/engine/BUCK` (~lines 177-189) does not depend on `//third-party:uuid` yet. Add `"//third-party:uuid",` to that target's `deps` (alongside the existing `//third-party:time`, `//third-party:tokio`, etc.). Without it the test fails to compile with "can't find crate `uuid`".

- [ ] **Step 1: Write the failing engine test**

In `src/services/engine/tests/scheduler.rs`, add (mirrors the existing scheduler test's use of `MemoryControlPlane`; uses `submit_run`/`mark_run_running`/`fail(Abandon)` to build a stranded run):

```rust
#[tokio::test]
async fn reconcile_sweeps_a_stranded_run_and_leaves_a_live_one() {
    use control_plane_core::{RetryPolicy, RunState, RunTrigger, TransformName, TransformRun};
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    cp.define_transform(scheduled_def("nightly")).await.unwrap();
    let def = cp.get_transform(&TransformName("nightly".into())).await.unwrap();

    let mk = |rid| TransformRun {
        run_id: rid,
        transform: Some(TransformName("nightly".into())),
        trigger: RunTrigger::Manual,
        state: RunState::Queued,
        body: def.body.clone(),
        queued_at: time::OffsetDateTime::now_utc(),
        started_at: None,
        finished_at: None,
        snapshot_id: None,
        error: None,
    };

    // Stranded: dequeued, running, job abandoned, run never finished.
    let stranded = uuid::Uuid::new_v4();
    let sjob = def.body.to_job(stranded);
    let kinds = vec![sjob.kind.clone()];
    cp.submit_run(mk(stranded), sjob).await.unwrap();
    let dq = cp.queue().dequeue(&kinds, "w").await.unwrap().unwrap();
    cp.mark_run_running(stranded).await.unwrap();
    cp.queue().fail(dq.id, "boom", RetryPolicy::Abandon).await.unwrap();

    // Live: dequeued and running, job still live.
    let live = uuid::Uuid::new_v4();
    let ljob = def.body.to_job(live);
    let lkinds = vec![ljob.kind.clone()];
    cp.submit_run(mk(live), ljob).await.unwrap();
    let _ = cp.queue().dequeue(&lkinds, "w").await.unwrap().unwrap();
    cp.mark_run_running(live).await.unwrap();

    let cutoff = time::OffsetDateTime::now_utc() + time::Duration::hours(1);
    let swept = cp.reconcile_stranded_runs(cutoff).await.unwrap();
    assert_eq!(swept, vec![stranded]);
    assert_eq!(cp.get_run(stranded).await.unwrap().state, RunState::Failed);
    assert_eq!(cp.get_run(live).await.unwrap().state, RunState::Running);
}
```

- [ ] **Step 2: Run it (RED)**

Run: `buck2 test --console none //src/services/engine/...`
Expected: with the `uuid` dep from Step 0 present and Task 1 landed, this compiles and passes — it calls the trait method directly, so it needs no engine loop code; the `reconcile_loop` below is the production wiring this test's scenario documents. (This test is the spec's "engine tick-style test": one sweep over the memory adapter, one stranded run reconciled, one live run left alone.)

- [ ] **Step 3: Add `reconcile_loop` (scheduler.rs)**

In `src/services/engine/src/scheduler.rs`, after `scheduler_loop` add:

```rust
/// Sweep stranded `Running` runs (a lost terminal report) every `every`, using
/// `grace` as the minimum age before a run is eligible, until `cancel` fires.
pub async fn reconcile_loop(
    cp: Arc<dyn ControlPlane>,
    every: Duration,
    grace: Duration,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(every);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            _ = interval.tick() => {
                let cutoff = OffsetDateTime::now_utc() - grace;
                match cp.transforms().reconcile_stranded_runs(cutoff).await {
                    Ok(ids) if !ids.is_empty() => {
                        tracing::info!(reconciled = ids.len(), "reconcile: swept stranded runs");
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "reconcile: sweep failed"),
                }
            }
        }
    }
}
```

(`OffsetDateTime - std::time::Duration` is supported by the `time` crate; `Duration` here is the same `std::time::Duration` `scheduler_loop` already uses.)

- [ ] **Step 4: Tuning fields (run.rs)**

In `EngineTuning` (`run.rs:33-43`) add two fields after `scheduler_tick`:

```rust
    /// How often the reconciliation loop sweeps stranded runs
    /// (`LOOM_RECONCILE_TICK_SECS`, default 60).
    pub reconcile_tick: Duration,
    /// Minimum age of a `Running` run before it is eligible for reconciliation
    /// (`LOOM_RECONCILE_GRACE_SECS`, default 120).
    pub reconcile_grace: Duration,
```

and in `from_map` (after the `scheduler_tick` entry, ~line 62) add:

```rust
            reconcile_tick: Duration::from_secs(service_runtime::parse_var(
                vars,
                "LOOM_RECONCILE_TICK_SECS",
                60_u64,
            )?),
            reconcile_grace: Duration::from_secs(service_runtime::parse_var(
                vars,
                "LOOM_RECONCILE_GRACE_SECS",
                120_u64,
            )?),
```

- [ ] **Step 5: Spawn + cancel the loop (run.rs)**

Before the scheduler spawn (`run.rs:120`), clone another control-plane handle for the reconcile loop (mirror `sched_cp` at `run.rs:80`):

```rust
    let reconcile_cp = sched_cp.clone();
```

After the scheduler `tokio::spawn` block (~line 125), add:

```rust
    let reconcile_cancel = CancellationToken::new();
    let reconcile = tokio::spawn(scheduler::reconcile_loop(
        reconcile_cp,
        tuning.reconcile_tick,
        tuning.reconcile_grace,
        reconcile_cancel.clone(),
    ));
```

After the scheduler shutdown (`sched_cancel.cancel(); drop(sched.await);`, ~line 136-137) add:

```rust
    reconcile_cancel.cancel();
    drop(reconcile.await);
```

- [ ] **Step 6: Build + test the engine**

Run: `buck2 build --console none //src/services/engine/...`
Run: `buck2 test --console none //src/services/engine/...`
Expected: builds clean; `reconcile_sweeps_a_stranded_run_and_leaves_a_live_one` passes; existing scheduler/engine tests unaffected.

- [ ] **Step 7: Commit**

```bash
git add src/services/engine/src/scheduler.rs src/services/engine/src/run.rs src/services/engine/tests/scheduler.rs
git commit -m "feat(engine): reconcile_loop drives the stranded-run sweep on a grace-guarded tick"
```

---

### Task 3: Worker cleanup refactor (behavior-preserving)

**Files:**
- Modify: `src/services/worker/src/transform.rs` (extract `mark_running_if_tracked`; drop `handle_typed_transform_inner`'s `run_id` param; refresh a doc comment)

**Interfaces:**
- Consumes: `TransformCtx` (with `control` + `worker_tuning`); `JobFailure::retry`; `parsed.run_id` on `TypedTransformJob`.
- Produces: private `async fn mark_running_if_tracked(ctx: &TransformCtx, run_id: Option<uuid::Uuid>, attempts: i32) -> std::result::Result<(), JobFailure>`.

- [ ] **Step 1: Extract the helper and call it from both handlers**

In `src/services/worker/src/transform.rs`, add the helper (near the two handlers):

```rust
/// Mark a tracked run `Running` before execution. A failure to reach the engine
/// is retryable — the run record stays `Queued` and the retry re-marks it. A
/// no-op when the job carries no `run_id` (ad-hoc).
async fn mark_running_if_tracked(
    ctx: &TransformCtx,
    run_id: Option<uuid::Uuid>,
    attempts: i32,
) -> std::result::Result<(), JobFailure> {
    if let Some(rid) = run_id {
        ctx.control.mark_run_running(rid).await.map_err(|e| {
            JobFailure::retry(
                ctx.worker_tuning.backoff(attempts),
                format!("mark_run_running: {e}"),
            )
        })?;
    }
    Ok(())
}
```

In `handle_transform` (~line 42-51), replace the `if let Some(rid) = run_id { … }` block with:

```rust
    mark_running_if_tracked(ctx, run_id, attempts).await?;
```

In `handle_typed_transform` (~line 96-105), replace the identical block with:

```rust
    mark_running_if_tracked(ctx, run_id, attempts).await?;
```

- [ ] **Step 2: Drop the redundant `run_id` param from the inner fn**

Change `handle_typed_transform_inner`'s signature (line 111-116) to drop `run_id: Option<uuid::Uuid>`:

```rust
async fn handle_typed_transform_inner(
    ctx: &TransformCtx,
    attempts: i32,
    parsed: &TypedTransformJob,
) -> std::result::Result<(), JobFailure> {
```

Inside it, replace the two `run_id` use sites with `parsed.run_id`:
- lineage (~line 151): `run_id: RunId(parsed.run_id.unwrap_or_else(uuid::Uuid::new_v4)),`
- `WireTransform` (~line 175): `run_id: parsed.run_id,`

In `handle_typed_transform`, update the call (~line 106) to drop the argument:

```rust
    let result = handle_typed_transform_inner(ctx, attempts, &parsed).await;
```

(Keep `let run_id = parsed.run_id;` at ~line 95 — it is still used by `mark_running_if_tracked` and `report_run_failure`.)

- [ ] **Step 3: Refresh the doc comment**

Update `handle_typed_transform`'s doc comment to describe the shared mark-running helper, mirroring `handle_transform`'s doc (`transform.rs:33-36`): note it parses → marks the run Running via `mark_running_if_tracked` → runs the typed funnel → best-effort reports failure.

- [ ] **Step 4: Build + test the worker**

Run: `buck2 build --console none //src/services/worker/...`
Run: `buck2 test --console none //src/services/worker/...`
Expected: builds clean; the existing transform worker e2e's (Queued→Running→Succeeded and failing-transform→Failed) plus handler tests pass — the extraction and param drop are behavior-preserving.

- [ ] **Step 5: Commit**

```bash
git add src/services/worker/src/transform.rs
git commit -m "refactor(worker): extract mark_running_if_tracked; drop redundant run_id param"
```
