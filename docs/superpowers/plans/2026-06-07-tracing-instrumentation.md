# Tracing Instrumentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `tracing`-facade instrumentation (spans + events) to the worker and the two control-plane adapters, with no behaviour change and no subscriber/metrics (those are Step 3).

**Architecture:** Libraries emit through the `tracing` facade; the Step 3 service binaries will install a subscriber. Selective instrumentation — mutating ops + worker lifecycle + Tx commit/rollback get spans; pure reads do not. One test (worker, via `tracing-test`) proves the facade is actually wired.

**Tech Stack:** Rust, buck2, `tracing`, `async-trait`, `tracing-test` (dev-only), tokio.

---

## Conventions for this plan

- **Build/test:** `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...`. Library targets: `//src/control-plane/{worker,memory,postgres}:<name>` where `<name>` is `worker`/`memory`/`postgres`.
- **Format before every commit:** `eval "$(./tools/env.sh)"` once, then `rustfmt --edition 2024 <files>` (prek rustfmt is check-only and blocks commits otherwise).
- **Clippy:** `tools/clippy-all.sh` must be clean — in particular no `unused_imports`. We use **fully-qualified `#[tracing::instrument(...)]`** on adapter methods so no `use tracing::...` import is needed in those files; the worker uses `tracing::{info_span, debug, info, warn}` macros (path-qualified, no import) plus `use tracing::Instrument;` for the `.instrument()` combinator.
- **`#[instrument]` records every non-skipped arg via `Debug`.** If a non-skipped arg's type does not implement `Debug`, the build fails — add that arg to `skip(...)`. Always `skip(self)`.
- **No logic changes.** Only attributes and event lines are added. Return values, error propagation, and control flow stay equivalent.

---

## Task 1: Add the `tracing` dependency (+ `tracing-test` dev-dep)

**Files:**
- Modify: `src/control-plane/worker/Cargo.toml`, `src/control-plane/memory/Cargo.toml`, `src/control-plane/postgres/Cargo.toml`
- Modify (generated): `third-party/BUCK`, `Cargo.lock`
- Modify: `src/control-plane/worker/BUCK`, `src/control-plane/memory/BUCK`, `src/control-plane/postgres/BUCK`

- [ ] **Step 1: Add to each crate's `Cargo.toml`**

`src/control-plane/memory/Cargo.toml` and `src/control-plane/postgres/Cargo.toml` — add under `[dependencies]`:
```toml
# Tracing facade: libraries emit spans/events; the Step 3 service binaries install
# the subscriber. No subscriber/metrics here.
tracing = "0.1"
```

`src/control-plane/worker/Cargo.toml` — add the same line under `[dependencies]`, and under `[dev-dependencies]`:
```toml
# Capturing subscriber for the instrumentation test (handles the global-default-once
# footgun + cross-thread capture). Dev-only — never ships in the library.
tracing-test = "0.2"
```

- [ ] **Step 2: Refresh the lockfile**

Run: `buck2 run //tools:reindeer -- update`
Expected: `Cargo.lock` updated with `tracing` (and `tracing-test` + its transitive deps). (Adding a dep without refreshing the lock makes `reindeer-check` fail later — see CLAUDE.md.)

- [ ] **Step 3: Regenerate `third-party/BUCK`**

Run: `./tools/buckify.sh`
Expected: `third-party/BUCK` now has `//third-party:tracing` and `//third-party:tracing-test` aliases (plus any new transitive crates). Confirm: `grep -E 'name = "tracing(-test)?"' third-party/BUCK`.

- [ ] **Step 4: Wire the library/test BUCK deps**

In `src/control-plane/memory/BUCK` and `src/control-plane/postgres/BUCK`, add `"//third-party:tracing",` to the `rust_library`'s `deps` list (keep alphabetical-ish ordering with the other `//third-party:*` entries).

In `src/control-plane/worker/BUCK`:
- add `"//third-party:tracing",` to the `rust_library` `deps`.
- add `"//third-party:tracing-test",` to the **`rust_test`** (`worker_test`) `deps`.

- [ ] **Step 5: Build to confirm deps resolve**

Run: `buck2 build //src/control-plane/worker:worker //src/control-plane/memory:memory //src/control-plane/postgres:postgres`
Expected: builds clean. (An as-yet-unused `tracing` dep compiles fine — no usage yet.)

- [ ] **Step 6: Verify reindeer is in sync**

Run: `buck2 run //tools:prek -- run --all-files reindeer-check` (or `./tools/buckify.sh && git diff --exit-code third-party/BUCK`)
Expected: no diff — generated rules match the manifests.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/*/Cargo.toml src/control-plane/*/BUCK third-party/BUCK Cargo.lock
git commit -m "build(control-plane): add tracing dep (+ tracing-test dev-dep)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Instrument the worker + prove it with a test

**Files:**
- Modify: `src/control-plane/worker/src/lib.rs` (the `Some(job)` arm of `run`)
- Test: `src/control-plane/worker/tests/worker.rs` (add one test)

- [ ] **Step 1: Write the failing test**

Add to the top of `src/control-plane/worker/tests/worker.rs` (with the other imports):
```rust
use tracing_test::traced_test;
```

Add this test at the end of the file:
```rust
// The worker's tracing instrumentation actually fires: a contained handler panic
// emits the "handler panic contained" warn event. Proves the tracing facade is
// wired end-to-end (catches #[instrument]/event mis-wiring that compiles to nothing).
#[tokio::test]
#[traced_test]
async fn emits_tracing_event_on_contained_panic() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT);
    cp.enqueue(job("boom")).await.unwrap();

    let token = CancellationToken::new();
    let t = token.clone();
    let worker =
        Worker::new(cp.clone(), "w1", LOCK_TIMEOUT).with_poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(async move {
        worker
            .run(&["boom".to_string()], t, move |_j| async move {
                panic!("handler blew up");
                #[allow(unreachable_code)]
                Ok(())
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert!(
        logs_contain("handler panic contained"),
        "worker should emit the panic-contained tracing event"
    );
}
```
(`#[traced_test]` injects `logs_contain` into scope. The default `#[tokio::test]` is a current-thread runtime, so the spawned worker runs on the test thread and capture is reliable.)

- [ ] **Step 2: Run the test — verify it FAILS**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/worker:worker_test`
Expected: FAIL on `emits_tracing_event_on_contained_panic` — the event doesn't exist yet (`logs_contain` returns false). The other 5 tests still pass.

- [ ] **Step 3: Instrument the worker run loop**

In `src/control-plane/worker/src/lib.rs`, add to the imports (with the other `use` lines):
```rust
use tracing::Instrument;
```

Replace the entire `Some(job) => { ... }` arm (currently the block from `let id = job.id;` through the closing `}` of that arm) with:
```rust
                Some(job) => {
                    let id = job.id;
                    let span = tracing::info_span!("job", job_id = ?id, kind = %job.kind);
                    async {
                        tracing::debug!("dequeued");
                        // Run the handler while heartbeating the lease on a timer, so a
                        // handler that outlives `lock_timeout` isn't reclaimed and
                        // double-executed. The heartbeat is best-effort: a missed tick is
                        // recoverable (the next tick retries; worst case the lease lapses
                        // and reclaim does its job) — deliberately asymmetric with the
                        // `?`-propagating dequeue/complete/fail below.
                        // catch_unwind contains a panicking handler so it fails the
                        // job (Abandon) instead of tearing down the loop.
                        let fut = AssertUnwindSafe(handler(job)).catch_unwind();
                        tokio::pin!(fut);
                        let mut hb = tokio::time::interval(self.heartbeat_interval);
                        let outcome = loop {
                            tokio::select! {
                                res = &mut fut => break res,
                                _ = hb.tick() => {
                                    let _ = self.queue.heartbeat(id).await;
                                }
                            }
                        };
                        match outcome {
                            Ok(Ok(())) => {
                                tracing::info!("job completed");
                                self.queue.complete(id).await?;
                            }
                            Ok(Err(JobFailure { error, policy })) => {
                                tracing::warn!(error = %error, ?policy, "job failed");
                                self.queue.fail(id, &error, policy).await?;
                            }
                            Err(panic) => {
                                let msg = panic_message(&*panic);
                                tracing::warn!(panic = %msg, "handler panic contained");
                                self.queue
                                    .fail(id, &format!("panic: {msg}"), RetryPolicy::Abandon)
                                    .await?;
                            }
                        }
                        Ok::<(), control_plane_core::ControlPlaneError>(())
                    }
                    .instrument(span)
                    .await?;
                }
```
Notes for the implementer:
- The `info_span!` reads `job.kind` (Display) *before* the `async` block moves `job` into `handler(job)` — order is correct, no borrow conflict.
- The `async` block borrows `self` and `handler` and moves `job`; it's awaited inline via `.instrument(span).await?`, so the borrows are valid and the `?` propagates out of `run` exactly as before.
- If `control_plane_core::ControlPlaneError` is not the exact error type of `control_plane_core::Result`, adjust the turbofish to the correct error type (it's the `Err` variant of the `Result` alias the crate already imports).

- [ ] **Step 4: Run the worker tests — all pass**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/worker:worker_test`
Expected: PASS, 6 tests (the 5 existing + the new one).

- [ ] **Step 5: Format + clippy**

Run: `eval "$(./tools/env.sh)" && rustfmt --edition 2024 src/control-plane/worker/src/lib.rs src/control-plane/worker/tests/worker.rs && tools/clippy-all.sh 2>&1 | grep -i worker`
Expected: no clippy findings for worker.

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/worker/
git commit -m "feat(worker): tracing instrumentation for the job loop

Per-job span (job_id, kind) + lifecycle events (dequeued/completed/failed/
panic-contained). Test asserts the panic-contained event fires.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Instrument the memory adapter

**Files:**
- Modify: `src/control-plane/memory/src/queue.rs`, `ontology.rs`, `acl.rs`, `lineage.rs`, `transaction.rs`

No new tests — the existing contract suites prove behaviour is unchanged; the worker test proves the `#[instrument]` mechanism fires.

- [ ] **Step 1: Instrument `queue.rs`**

Add `#[tracing::instrument(skip(self), level = "debug")]` directly above each of these method signatures inside `impl Queue for MemoryControlPlane`: `enqueue`, `dequeue`, `complete`, `fail`, `heartbeat`. (Do NOT instrument `await_jobs`.) If any non-skipped arg lacks `Debug`, add it to `skip(...)`.

- [ ] **Step 2: Instrument `ontology.rs`**

Add `#[tracing::instrument(skip(self), level = "debug")]` above `define_type` and `define_link` inside `impl Ontology for MemoryControlPlane`. Leave `get_type`, `list_types`, `links`, `resolve` uninstrumented.

- [ ] **Step 3: Instrument `acl.rs`**

Add `#[tracing::instrument(skip(self), level = "debug")]` above `define_subject`, `define_role`, `assign_role`, `unassign_role`, `grant`, `revoke`, `clear_policy`. For `set_policy`, use `#[tracing::instrument(skip(self, policy), level = "debug")]`. Leave `check` and `policies_for` uninstrumented.

- [ ] **Step 4: Instrument `lineage.rs`**

Add above `emit` inside `impl Lineage for MemoryControlPlane`:
```rust
#[tracing::instrument(skip(self, event), fields(run_id = ?event.run_id, event_type = ?event.event_type), level = "debug")]
```
Leave `events_for`, `upstream`, `downstream` uninstrumented.

- [ ] **Step 5: Instrument `transaction.rs`**

Inside `impl Tx for MemoryTx`: add `#[tracing::instrument(skip(self), level = "debug")]` above `commit`, `rollback`, and `enqueue`. For `emit`, use `#[tracing::instrument(skip(self, event), fields(run_id = ?event.run_id, event_type = ?event.event_type), level = "debug")]`.
(Note: `commit`/`rollback` take `self: Box<Self>` — `skip(self)` still applies.)

- [ ] **Step 6: Format + build + clippy + test**

Run:
```bash
eval "$(./tools/env.sh)" && rustfmt --edition 2024 src/control-plane/memory/src/*.rs
buck2 build //src/control-plane/memory:memory
tools/clippy-all.sh 2>&1 | grep -i memory
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory/...
```
Expected: clean build, no clippy findings, all memory contract tests pass (counts unchanged from before — behaviour preserved).

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/memory/
git commit -m "feat(memory): tracing spans on mutating ops + Tx

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Instrument the postgres adapter

**Files:**
- Modify: `src/control-plane/postgres/src/queue.rs`, `ontology.rs`, `acl.rs`, `lineage.rs`, `transaction.rs`
- Leave untouched: `catalog.rs` (only reads), `fixture.rs`, `lib.rs`

Mirror Task 3 exactly, on the `PgControlPlane` / `PgTx` impls.

- [ ] **Step 1: Instrument `queue.rs`**

`#[tracing::instrument(skip(self), level = "debug")]` above `enqueue`, `dequeue`, `complete`, `fail`, `heartbeat` in `impl Queue for PgControlPlane`. Not `await_jobs`. Do NOT instrument the `pub(crate) async fn pg_insert` free fn (it's an internal helper; the trait methods that call it are already spanned).

- [ ] **Step 2: Instrument `ontology.rs`**

`#[tracing::instrument(skip(self), level = "debug")]` above `define_type`, `define_link` in `impl Ontology for PgControlPlane`.

- [ ] **Step 3: Instrument `acl.rs`**

`#[tracing::instrument(skip(self), level = "debug")]` above `define_subject`, `define_role`, `assign_role`, `unassign_role`, `grant`, `revoke`, `clear_policy`; `#[tracing::instrument(skip(self, policy), level = "debug")]` above `set_policy`. Not `check`/`policies_for`.

- [ ] **Step 4: Instrument `lineage.rs`**

`#[tracing::instrument(skip(self, event), fields(run_id = ?event.run_id, event_type = ?event.event_type), level = "debug")]` above `emit` in `impl Lineage for PgControlPlane`. Do NOT instrument `pg_emit`, `event_datasets`, `graph_step`. Leave `events_for`/`upstream`/`downstream` uninstrumented.

- [ ] **Step 5: Instrument `transaction.rs`**

`#[tracing::instrument(skip(self), level = "debug")]` above `commit`, `rollback`, `enqueue` in `impl Tx for PgTx`; the `event`-skipping form above `emit`.

- [ ] **Step 6: Format + build + clippy + test**

Run:
```bash
eval "$(./tools/env.sh)" && rustfmt --edition 2024 src/control-plane/postgres/src/*.rs
buck2 build //src/control-plane/postgres:postgres
tools/clippy-all.sh 2>&1 | grep -i postgres
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres/...
```
Expected: clean build, no clippy findings, all postgres contract tests pass (counts unchanged). Confirm `git diff src/control-plane/postgres/src/{catalog,lib,fixture}.rs` is empty.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/postgres/
git commit -m "feat(postgres): tracing spans on mutating ops + Tx

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: Full-tree verification

- [ ] **Step 1: Full build + test**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...`
Expected: PASS — total **Pass 21** (was 20; +1 for the new worker test). No other counts changed.

- [ ] **Step 2: Full lint**

Run: `buck2 run //tools:prek -- run --all-files`
Expected: all hooks green (rustfmt, clippy, file checks, reindeer-in-sync).

- [ ] **Step 3: Confirm scope**

Run: `git diff main --stat`
Expected: only `Cargo.lock`, `third-party/BUCK`, the three crates' `Cargo.toml`/`BUCK`, the worker `src`/`tests`, and the memory/postgres `src/{queue,ontology,acl,lineage,transaction}.rs` changed. `core/`, `testkit/`, and the postgres `catalog.rs`/`lib.rs`/`fixture.rs` untouched.
