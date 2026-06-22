# Flush idempotency under duplicate dispatch — over-the-wire test — Design

> Closes `iss-flush-at-least-once-idempotency`. The engine-wire flush vertical
> (#108) dispatches `flush_table` jobs to workers with **at-least-once** delivery:
> a missed lease heartbeat can let a second worker re-dispatch the *same*
> `flush_table` job, so two flushes of one table can execute concurrently.
> `flush_table` is *designed* to make that a safe no-op — a transaction-scoped
> advisory lock serializes per-table flushes, and the loser re-reads `current` and
> finds the inline rows already end-capped — but **nothing asserts it**. This slice
> adds the missing regression: two concurrent `flush_table` dispatches over the
> engine wire produce exactly-once effect. Test-only; no production code changes.

## Goal

A test proves that two concurrent `flush_table` executions for the same table —
modeling at-least-once redelivery of one job to two workers — write the rows
**exactly once**: one dispatch does the work, the other is a clean no-op, and the
durable state is identical to a single flush.

## Why the guarantee holds (what the test pins)

`flush_table` (`…/postgres/src/iceberg_flush.rs:29`) takes a **transaction-scoped
advisory lock** (`pg_advisory_xact_lock`) on a per-table key *before* it reads any
state, and holds it across the whole flush (Parquet append + inline end-cap commit)
until the lock transaction ends. So only one flush is ever inside the critical
section; the second blocks, then — after the first commits — re-reads
`current_snapshot` and `inline_live_batch`, finds the inline rows already
end-capped at the new snapshot, and returns `Ok(None)` (`flush_locked`, the
"nothing live to flush" branch). Exactly-once by construction, for **any**
interleaving (fully overlapping or fully sequential) — which is what makes the
test inherently non-flaky: the invariant does not depend on the two dispatches
racing in a particular order.

The wire faithfully carries this: `EngineControlService::flush_table`
(`…/engine/src/service.rs:93`) calls the primitive and returns
`snapshot_id: Option<i64>`; `GrpcQueueClient::flush_table`
(`…/engine-wire/src/client.rs:47`) returns `Result<Option<i64>>` — so the winner's
RPC returns `Some(snap)` and the loser's returns `None`. Each RPC mints its own
`RunId` (`service.rs:106`), so the winner emits exactly one compaction lineage
event and the loser emits none (it returns before the lineage step).

## The test (over the wire)

Add one `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` to
`src/services/worker/tests/e2e.rs`, reusing that file's existing harness
(`spawn_server`, `make_catalog`, `columns`, `inline_batch`, `inline_lineage`) — no
new helpers, no new BUCK target.

Shape:

```rust
/// At-least-once redelivery: two workers dispatch the SAME flush_table job
/// concurrently. The per-table advisory lock makes the duplicate a safe no-op —
/// the rows are written exactly once. Holds under any interleaving, so non-flaky.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_dispatch_flush_is_idempotent_over_the_wire() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let (_sock_dir, sock) = spawn_server(&fx, &db).await;

    let table = TableRef { schema: "wh".into(), name: "t".into() };
    let run = RunId(uuid::Uuid::new_v4());

    // Land three inline rows directly (threshold None — we drive the duplicate
    // dispatch ourselves rather than through the queue; two flush RPCs of the same
    // table *is* the same job delivered twice).
    inline_append(&pool, &table, &columns(), &inline_batch(&[1, 2, 3]),
                  inline_lineage(run, &table), None).await.expect("inline_append");

    // Two concurrent flush_table RPCs for the same table (one cloned client,
    // tonic multiplexes; the engine runs each as its own handler task).
    let c1 = GrpcQueueClient::connect(&sock).await.expect("connect");
    let c2 = c1.clone();
    let (a, b) = tokio::join!(
        c1.flush_table("wh".into(), "t".into()),
        c2.flush_table("wh".into(), "t".into()),
    );
    let a = a.expect("rpc a ok");
    let b = b.expect("rpc b ok");

    // (1) Exactly one dispatch did the work; the other was a no-op.
    assert!(a.is_some() ^ b.is_some(), "exactly one flush wrote a snapshot (got {a:?}, {b:?})");
    assert!(a.is_none() || b.is_none(), "the duplicate dispatch is a no-op");

    // (2) Durable state == a single flush: one fileset holding exactly the 3 rows
    //     (not 6), and the inline rows retired.
    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice.current_snapshot(&table).await.expect("current");
    let files = ice.files(&table, cur.id, PageReq::unbounded()).await.expect("files");
    let rows: i64 = files.items.iter().map(|f| f.record_count).sum();
    assert_eq!(rows, 3, "rows written exactly once across both dispatches (no double-write)");
    let inline = ice.inline_parquet(&table, cur.id).await.expect("inline");
    assert!(inline.is_none(), "inline rows retired exactly once");
}
```

### Why these assertions are sufficient (and run-id-agnostic)

- **`a.is_some() ^ b.is_some()`** proves the engine executed the flush twice and
  exactly one did work — the no-op loser returned `None`, which it can only do via
  `flush_locked`'s "nothing live" branch, i.e. it wrote nothing and emitted no
  lineage. So a separate "exactly one compaction event" assertion is *implied* and
  not needed (and would require a dataset-scoped lineage read the test can't key by
  the engine-minted run-id).
- **`rows == 3`** (not 6) is the durable no-double-write proof; **`inline is_none`**
  proves the rows were end-capped exactly once. Together with the XOR, this pins
  exactly-once at both the dispatch boundary and the storage layer.

## What this does NOT do

- **No production code change** — `flush_table`, the engine service, the worker,
  and the wire are unmodified; this asserts existing behavior.
- **Does not simulate the lease-lapse machinery** (heartbeat timeout → re-dequeue)
  — two concurrent `flush_table` RPCs *are* the duplicate-execution outcome that a
  lapse produces; reproducing the timing of a lapse adds flake, not coverage. The
  property under test is idempotency of duplicate execution, which is delivery-path
  agnostic.
- **Does not touch the primitive-level `iceberg_flush.rs` tests** — they stay as
  the sequential coverage; this adds the concurrent/over-the-wire case the issue
  flags.

## Testing / validation

- The new test is the deliverable. It is a `loom_fixture_test` (the worker e2e
  target already is — Postgres + DuckDB fixture, local execution).
- Run it in isolation and confirm green:
  `buck2 test //src/services/worker:e2e` (single target — and note the fixture-boot
  throttle from `iss-fixture-boot-contention`, if landed, only matters for the
  whole-suite sweep, not a single target).
- Existing worker e2e and `iceberg_flush.rs` tests stay green unchanged.

## Files

- Modify: `src/services/worker/tests/e2e.rs` — add the one concurrent-dispatch
  test above, reusing the existing helpers. (The `worker:e2e` `rust_test`/
  `loom_fixture_test` target already depends on `engine`, `engine-wire`,
  `control-plane-postgres`, `iceberg`, `tokio`, `uuid` — **no BUCK change**.)
- Modify: `docs/ISSUES.md` — close `iss-flush-at-least-once-idempotency`
  (`[x] status:fixed pr:#<n>`); it points at this design.
- No production source, `Cargo.*`, `third-party/BUCK`, or core change.
