# Design: Tx isolation contract + memory atomic-commit fix (Step 2a #2)

> **Status:** approved design. Second hardening item from
> `2026-06-06-control-plane-critical-review.md` (§1) under the loom roadmap's Step 2a.
> A `testkit` contract + a surgical fix in `control-plane-memory`.

## Problem

Two related gaps the critical review flagged:

1. **No isolation contract.** `queue_contract` and `lineage_contract` test
   *commit → visible* and *rollback → invisible*, but only by committing/rolling back
   *before* reading. The unwritten property is: **while a `Tx` is open and
   uncommitted, the autocommit read path observes none of its writes**, and on commit
   all of them appear. The two adapters have entirely different isolation machinery
   (pg = a real `sqlx::Transaction`, READ COMMITTED; memory = stage-and-apply under a
   `Mutex`) and nothing verifies they agree.

2. **Memory `Tx::commit` is not atomic across concerns.** It locks `rows`, applies
   staged jobs, *releases*, then locks `lineage`, applies staged events — two separate
   critical sections. A concurrent reader can observe jobs-applied-but-events-not. The
   pg adapter gets true all-or-nothing from one `sqlx::Transaction`, so the fake does
   **not** faithfully model the atomic guarantee it exists to let downstream tests
   rely on.

## Fix part 1 — the isolation contract (`testkit`)

A new `tx_isolation_contract<CP: ControlPlane + Queue + Lineage>(cp: &CP)`, run against
both adapters. It is **deterministic** — no real threading. It exploits that an open
`Tx` holds its writes outside the autocommit read path (memory: in the `MemoryTx`
staged buffers; pg: uncommitted on the tx's own connection, invisible to a separate
pool connection under READ COMMITTED). It exercises **both** transactional ops
(`enqueue` + `emit`) in one `Tx` — the cross-concern path — so it also pins that a
commit makes the whole unit visible.

- **Open-tx invisibility (commit path):** `begin`; within the `Tx`, `enqueue` a job of
  a unique kind and `emit` a lineage event for a unique `run_id`. **Before commit**,
  via the same `cp` (autocommit): `dequeue([kind])` is `None` **and** `events_for(run)`
  is empty. Then `commit`. After commit: `dequeue([kind])` returns the job **and**
  `events_for(run)` returns the event.
- **Rollback invisibility:** `begin`; `enqueue` + `emit` (a different kind / `run_id`);
  `rollback`. The job is never dequeued and `events_for(run)` stays empty.

Determinism notes: the read happens on the same thread while the `Tx` handle is still
alive (commit consumes it: stage → read-via-`cp` → commit → read-again). For pg, the
read uses a pool connection distinct from the tx's connection; the fixture pool is
`max_connections(5)`, so holding the tx connection while reading does not deadlock.

## Fix part 2 — atomic memory commit (`control-plane-memory`)

`MemoryTx::commit` acquires **both** the `rows` and `lineage` locks and applies both
staged buffers before releasing either, so any single-lock reader (`dequeue` locks
`rows`; `events_for` locks `lineage`) observes all-or-nothing:

```rust
async fn commit(self: Box<Self>) -> Result<()> {
    let staged_jobs = !self.staged.is_empty();
    {
        // Hold BOTH locks across the whole apply so commit is atomic w.r.t. any
        // single-lock reader (dequeue/events_for): no partial-commit is observable.
        // Lock order rows-then-lineage must be consistent everywhere to stay
        // deadlock-free (readers take only one lock; no reader takes both).
        let mut rows = self.rows.lock().unwrap();
        let mut lin = self.lineage.lock().unwrap();
        for (id, job) in self.staged {
            MemoryControlPlane::insert_with_id(&mut rows, id, job);
        }
        lin.events.extend(self.staged_events);
    }
    if staged_jobs {
        self.notify.notify_waiters();
    }
    Ok(())
}
```

This is correct by construction (single critical section). Lock order `rows → lineage`
is the only ordering that takes both; readers take exactly one, so there is no deadlock
risk. The comment pins the invariant so a future edit doesn't silently reintroduce the
two-section gap.

## Testing

- New `tx_isolation_contract` in `testkit`, invoked from `memory/tests/tx.rs` and
  `postgres/tests/tx.rs` (new `tx` `rust_test` targets in each adapter's BUCK; the pg
  one carries the usual `POSTGRES_BIN_DIR`/`LD_LIBRARY_PATH`/`LOOM_MIGRATIONS_DIR`
  env, no DuckDB). Both must pass.
- The memory fix is covered behaviorally by the same contract (post-commit both ops
  visible); its atomicity is by-construction (single critical section), per the design
  decision not to add a probabilistic concurrency test.

## Non-goals

- **A probabilistic "never partial" concurrency stress test** — deliberately skipped:
  the single-critical-section fix makes partial commit structurally impossible in the
  fake, the isolation contract pins the observable semantics deterministically, and a
  timing-dependent test that can flake or pass vacuously is worse than none (it erodes
  trust in the suite). The commit-site comment guards against regression instead.
- **Stronger isolation levels** (serializable, etc.) — the contract asserts only
  uncommitted-invisibility (READ COMMITTED-ish), which is what both adapters provide.
- **Read-your-writes within a `Tx`** — `Tx` exposes no read methods (only
  `enqueue`/`emit`/`commit`/`rollback`), so this is out of scope by construction.
- **Touching the other three `Mutex`es** (catalog/ontology/acl) — `Tx` only stages
  queue + lineage writes; the commit fix holds exactly the two locks those touch.
