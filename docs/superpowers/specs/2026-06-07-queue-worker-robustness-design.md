# Design: queue/worker robustness (Step 2b, group 1)

> **Status:** approved design. First Step 2b hardening group from
> `2026-06-06-control-plane-critical-review.md` (§1). Two independent items in one PR:
> a handler-panic policy in the worker, and a queue concurrency contract.

## Item A — handler-panic policy in `Worker`

### Problem
`Worker::run` runs the user handler inline; a panicking handler unwinds through
`run().await` and **kills the worker loop** (and the spawned task). No test covers it.
A buggy handler should fail its job, not take down the worker.

### Fix
Catch the panic and treat it as a job failure, keeping the loop alive.

- **Mechanism: `futures_util::FutureExt::catch_unwind`.** Wrap the handler future as
  `AssertUnwindSafe(handler(job)).catch_unwind()`; a panic during poll resolves to
  `Err(Box<dyn Any + Send>)` instead of unwinding. Behaviour-preserving — the handler
  still runs **inline** in the existing `select!`/heartbeat loop, no `'static` bound added
  to `run`. Adds a `futures-util` dependency to the `worker` crate (vendored already; the
  public `//third-party:futures-util` alias is emitted by buckify, like `serde` before it).
  `AssertUnwindSafe` is sound here: on a caught panic we only record the failure and move
  on; we don't touch the handler's interrupted state.
- **Policy: `Abandon`.** A panic is a bug, not a transient fault — retrying would re-run a
  poison job. On a caught panic the worker calls
  `self.queue.fail(id, "panic: <msg>", RetryPolicy::Abandon)`, extracting the panic message
  best-effort (`downcast_ref::<&str>()` / `::<String>()`, else `"unknown panic"`). Not
  configurable this cycle (YAGNI).
- **Caveat:** `catch_unwind` only works under `panic = "unwind"` (the default). A
  `panic = "abort"` build profile aborts the process regardless — documented, not handled.

The `run` loop's `Some(job)` arm becomes (heartbeat arm unchanged):
```rust
let fut = AssertUnwindSafe(handler(job)).catch_unwind();
tokio::pin!(fut);
let mut hb = tokio::time::interval(self.heartbeat_interval);
let outcome = loop {
    tokio::select! {
        res = &mut fut => break res,
        _ = hb.tick() => { let _ = self.queue.heartbeat(id).await; }
    }
};
match outcome {
    Ok(Ok(())) => self.queue.complete(id).await?,
    Ok(Err(JobFailure { error, policy })) => self.queue.fail(id, &error, policy).await?,
    Err(panic) => {
        let msg = panic_message(&panic);
        self.queue.fail(id, &format!("panic: {msg}"), RetryPolicy::Abandon).await?;
    }
}
```

### Test (memory)
A worker handling two kinds — `"boom"` (panics) and `"ok"` (succeeds). Enqueue one of
each; run the worker; cancel after it drains. Assert: the `"ok"` handler ran (the worker
**survived** the panic and kept processing), `run` returned `Ok` (loop not torn down), and
a probe `dequeue` of both kinds returns `None` (the panicking job was Abandoned — not
retried, not stuck running; the good job completed). The observable contract is
survival + no-poison-retry; the fake retains the abandoned job but exposes no state getter,
so we assert via dequeue-drained rather than inspecting state.

## Item B — queue concurrency contract (`SKIP LOCKED`)

### Problem
The queue's whole point is `FOR UPDATE SKIP LOCKED` fairness, but it's only ever tested
single-threaded. Nothing asserts that concurrent workers each claim a job **exactly once**.

### Fix
A new `testkit` contract:
```rust
pub async fn queue_concurrency_contract<CP>(cp: CP)
where CP: Queue + Clone + Send + Sync + 'static
```
Enqueue M jobs (e.g. 50) of one kind; spawn N tasks (e.g. 8), each looping `dequeue` →
`complete` until it gets `None`; join all and collect claimed `JobId`s. Assert
`all.len() == M` **and** all ids distinct — every job claimed exactly once (none lost, none
double-claimed).

Determinism: a worker sees `None` only when no job is *available* (remaining jobs are
already claimed-and-counted by another worker, in-flight to `complete`), so when the last
worker drains, all M are accounted for — `total == M` holds without flakiness. The test
runs fast (< the 300ms `lock_timeout`), so no lock expiry / re-claim.

Runtime: a plain `#[tokio::test]` (current-thread) suffices — `tokio::spawn`ed tasks
interleave at each `dequeue().await`, so the pg adapter issues genuinely concurrent
`SKIP LOCKED` queries (real contention via in-flight I/O); the memory adapter serializes on
its `Mutex` but still validates no-double-claim. No `rt-multi-thread` tokio feature needed
(only `rt`, already enabled).

### Wiring
Invoked from a new `#[tokio::test]` fn in each adapter's existing `tests/queue.rs`
(`memory` + `postgres`) — no new BUCK targets.

## Scope / non-goals

- One PR ("queue/worker robustness"), two items.
- **Not** configurable panic policy; **not** panic-isolation via a spawned task (we chose
  inline `catch_unwind`); **not** `rt-multi-thread`.
- The concurrency contract asserts the *exactly-once* guarantee, not throughput/fairness
  ordering.
