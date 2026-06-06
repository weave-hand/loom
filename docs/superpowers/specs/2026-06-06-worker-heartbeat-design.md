# Design: Worker lease heartbeating (Step 2a #1)

> **Status:** approved design. First hardening item from
> `2026-06-06-control-plane-critical-review.md` (§1) under the loom roadmap's Step 2a.
> A focused correctness fix in the `control-plane-worker` crate.

## Problem

`Worker::run` processes a job as `dequeue → handler → complete/fail` with **nothing
renewing the lease while `handler` runs**. The queue marks a claimed job `running`
with `locked_at = now`; `dequeue` reclaims a `running` job once `locked_at <
now - lock_timeout` (crashed-worker recovery). So any handler that runs longer than
the queue's `lock_timeout` has its lease expire mid-flight, another worker reclaims
the job, and it is **executed more than once**. `Queue::heartbeat` exists precisely to
refresh `locked_at`, but the worker never calls it — it is dead from the consumer's
side, and no test runs a handler longer than `lock_timeout`.

This is a live at-least-once-with-silent-duplication bug, not future work.

## Fix

The worker heartbeats the in-flight job on a timer derived from the lease, for the
duration of the handler. Entirely within `control-plane-worker` (`src/lib.rs` +
tests); no `core` or trait change (`Queue::heartbeat` already exists).

### API

`Worker::new` gains the lease:

```rust
/// `lease` must match the queue's configured `lock_timeout`: it is how long a
/// claimed job stays locked without a heartbeat. The worker heartbeats the
/// in-flight job every `lease / 3` so a handler running longer than the lease is
/// not reclaimed and double-executed.
pub fn new(queue: Q, worker_id: impl Into<String>, lease: Duration) -> Self
```

- Heartbeat interval = `(lease / 3).max(Duration::from_millis(1))`. Derived, not
  separately configurable (the lease is the single knob). The `1ms` floor avoids a
  zero-period `tokio::time::interval` panic if a caller passes a tiny lease.
- `with_poll_interval` is unchanged (the idle wakeup fallback is a separate concern
  from the in-flight heartbeat).
- Breaking change: the existing worker tests and the pg integration test update their
  `Worker::new(...)` calls to pass a lease (they already define a `LOCK_TIMEOUT`).

### Mechanism

Single-task, no spawn, no new trait bounds, same `Fn(Job) -> Fut` handler signature.
When a job is claimed, race the handler future against a heartbeat interval:

```rust
Some(job) => {
    let id = job.id;
    let fut = handler(job);
    tokio::pin!(fut);
    let mut hb = tokio::time::interval(self.heartbeat_interval);
    let outcome = loop {
        tokio::select! {
            res = &mut fut => break res,
            _ = hb.tick() => {
                // Best-effort: a missed heartbeat is recoverable (the next tick
                // retries; worst case the lease lapses and reclaim does its job).
                // Deliberately asymmetric with dequeue/complete/fail, which `?`.
                let _ = self.queue.heartbeat(id).await;
            }
        }
    };
    match outcome {
        Ok(()) => self.queue.complete(id).await?,
        Err(JobFailure { error, policy }) => self.queue.fail(id, &error, policy).await?,
    }
}
```

`tokio::time::interval` fires its first tick immediately, producing one heartbeat
right after the claim — harmless (an early lease refresh) and not worth suppressing.
The idle/`None` branch and shutdown handling are unchanged.

### Error handling

The periodic `heartbeat` is **best-effort**: its `Err` is dropped and the next tick
retries. Rationale (confirmed in design): losing one heartbeat is recoverable, whereas
tearing down a long-running worker on a transient blip is not what the component whose
job is to keep running should do; process-level connection handling is the worker
process's concern regardless. `dequeue`/`complete`/`fail` keep propagating via `?`
(broader transient-error resilience is out of scope here). Note: the memory adapter's
`heartbeat` is infallible, so this `Err` path only manifests against Postgres.

## Testing

Hermetic, against the in-memory adapter (memory's `dequeue` honors `locked_at`
freshness, so the lease-held property is observable without Postgres):

- **`heartbeat_keeps_long_handler_single` (the regression guard):** `lock_timeout =
  200ms`, `Worker::new(.., lease = 200ms)` → heartbeat ~66ms. Enqueue one job; the
  handler signals it has started (e.g. via a channel/flag) then sleeps ~500ms
  (> lock_timeout) before returning `Ok`. While the handler is in flight, a `dequeue`
  under a **different** worker id returns `None` (the lease is held by heartbeating).
  After the worker drains and is cancelled: the handler ran **exactly once** and the
  job is gone (completed). Without the fix, the concurrent dequeue would reclaim and
  the handler would run twice.
- **Existing tests** (`drains_jobs_then_shuts_down`, `retry_then_succeed`,
  `abandon_is_terminal`) updated only to pass the lease to `Worker::new`; behavior and
  assertions unchanged.

Assertions key off observable state (execution count, dequeue visibility), never
timing internals beyond the deliberate sleeps already used in the suite.

## Non-goals

- Retrying / tolerating transient `dequeue`/`complete`/`fail` errors (the loop's
  broader resilience is unchanged).
- Handler-panic policy (`catch_unwind`) — separate Step 2b item.
- A separately configurable heartbeat interval (derive from the lease; revisit only if
  a consumer needs it).
- A pg-specific integration test of the best-effort `Err` path — the memory test
  covers the lease-held behavior; the `Err` branch is trivial and pg-only.
