# In-flight MV runs vs. a deleted def Design

> **Status:** design (direction). This spec makes `iss-mv-watermark-ghost-rows` build-ready.
> The item stays in ISSUES (a defect in shipped code); a separate work agent writes the
> implementation plan from it and builds it.

## Problem

The registration→watermark reconciliation is complete on the **admin** paths:
`delete_transform` (`postgres/src/transforms.rs:532-575`) deletes the MV's
`stream.mv_watermark` rows with the def, and `define_transform` (`transforms.rs:425-468`)
deletes the *previous* output's rows when a redefinition moves an MV off it — both in the same
transaction as the def write, both certified on **both** backends by
`control_plane_testkit::transforms_contract` (`testkit/src/lib.rs:5504-5651`).

What remains is a run that commits **after** its def is gone. Its watermark CAS
(`pg_advance_mv_watermark`, `stream.rs:590-628`) neither takes the define lock nor re-checks the
registration, so it re-inserts rows under a now-defless key. `mv_floor` then floors the source at
that key's offsets with **no def to point the operator at**, and the only cure is a manual
`delete from stream.mv_watermark`.

## The research overturns the entry in both directions — read this first

### Narrower than recorded: only the `from == 0` branch can ghost

The CAS has **two** branches (`stream.rs:596-628`):

```rust
let sql = if adv.from == 0 {
    "insert into stream.mv_watermark (…) values ($1,$2,$3,$4) \
     on conflict (mv, source_table_id, bucket) do update set next_offset = $4 \
     where stream.mv_watermark.next_offset = 0"
} else {
    "update stream.mv_watermark set next_offset = $4 \
     where mv = $1 and source_table_id = $2 and bucket = $3 and next_offset = $5"
};
… if done.rows_affected() == 0 { return Err(ControlPlaneError::Conflict(…)) }
```

An in-flight run that has already consumed offsets has `from > 0` → the **UPDATE** branch → the
deleted row means `rows_affected() == 0` → `Conflict` → the whole commit tx rolls back and the
job Abandons ("superseded: watermark advanced concurrently", `worker/src/stream_mv.rs:255-258`).
**No ghost.** A mixed advance (bucket 0 virgin at `from=0`, bucket 1 at `from=5`) still rolls
back on the `from > 0` leg. Ghosts require **every** advance in the commit to be `from == 0`.

### Much wider than recorded: the ghost watermark is the *cheapest* of three symptoms

The entry calls this "bounded in practice (one micro-batch's worth of lag)". It is not.
**`delete_transform` cancels nothing** — it touches the def row and the watermark rows, and
never the queue. A `stream_mv` job **already enqueued but not yet claimed** runs *after* the
delete. By then `pg_mv_watermarks` returns **empty**, so the delta scan reads the source from
**offset 0** — every advance is `from == 0`, the INSERT branch fires, and the blast radius is
the **entire source table**, not one micro-batch. The window is *queue latency* (retries and
backoff included), not commit latency.

And the same commit tx (`inline_append_decl`, `iceberg_inline.rs:450-788`) does three more
things:

- **It re-creates the output.** `ensure_table` (`:473`) mints the output's mirror row **if
  absent**. So the operator flow "delete the def, drop the output" is undone by a queued run: the
  output table comes back (`inline_append_mv` re-declares it a Log stream table, `:836-848`) and
  gets a **full re-materialization of the source appended on top** of whatever it held — a Log
  append, so no dedup, no CoW.
- **It emits lineage** (`:771`) for a transform that no longer exists, and **fires data triggers**
  (`:779-784`) off the resurrected output.
- **It marks the run succeeded** (`:730`) — `transforms.run` deliberately has no FK and survives
  def deletion (`0031_transforms.sql:13`), so nothing aborts it.

This matters for fix selection: a def-existence guard on the CAS fixes **all three at once**,
precisely because the CAS's `Conflict` rolls back the same transaction that carries the output. A
GC sweep cures the watermark and leaves the resurrected table and the duplicate rows behind.

### There is no FK, and there structurally cannot be one

`stream.mv_watermark` (`migrations/0042_mv_watermark.sql:6-12`) declares **zero** FKs. Its key
`mv` is `mv_key(output)` = the **output table's** qualified name (`core/src/stream.rs:146-153`),
not the def name — and nothing stops **two defs from naming the same output** (N defs → 1 key).
So there is no column to reference, no `ON DELETE CASCADE` story, and the ghost INSERT cannot
fail on an FK. The entry's premise survives this check.

### The blocker for both proposed fixes: defless watermark keys are LEGITIMATE today

`POST /admin/transforms/run` (`runtime/src/admin.rs:1510-1543`) submits an **ad-hoc**
`TransformBody` with `transform: None` and **no def row** — and it deserializes *any* variant,
including `{"kind":"microbatch",…}` (no `validate_transform_def`, no variant filter; its
doc-comment only *claims* `physical|typed`). Such a run writes watermarks under a key **no def
names**. `mv_floor`'s own module doc already concedes this (`mv_floor.rs:26-27`: "every `mv`
holding a watermark row against the source (**an ad-hoc run**, or one mid-deletion)").

So a naive "defless ⇒ illegitimate" rule breaks ad-hoc MV runs, and a naive GC sweep **deletes a
live ad-hoc run's cursor**. Deleting a watermark is not a no-op — it is a "reprocess everything"
instruction.

## Design

**Recommended: (a) a def-existence guard on the CAS**, paired with a decision on ad-hoc MV runs.

It is the only option that prevents ghosts from being *created* rather than mopping them up; it is
atomic with the commit (the rollback is already the exactly-once mechanism, so no orphaned output);
it needs no new lock; and it automatically fixes the queued-job-outlives-def case, which is the big
one.

Mechanics: `transforms.transform` is in the same DB and the CAS runs on the commit tx's connection,
so the guard sees the def under normal MVCC (and blocks on `define_transform`'s `for update` row
lock if that tx is open). `TransformBody` is internally tagged (`#[serde(tag = "kind")]`, variants
`microbatch` / `microbatch_join`) and `TableRef` is `{schema,name}`, so the predicate is expressible
in SQL:

```sql
exists (select 1 from transforms.transform
        where body->>'kind' in ('microbatch','microbatch_join')
          and (body->'output'->>'schema') || '.' || (body->'output'->>'name') = $mv)
```

(Existing precedent decodes bodies in Rust instead — `pg_micro_batch_readers`,
`transforms.rs:81-112`. Either is fine; the SQL form keeps it atomic with the CAS.)

**Failure semantics — the part the entry does not mention, and it matters.** On rejection the CAS
must **error, not silently skip**. In `inline_append_mv` the error propagates via `?` and rolls the
whole tx back, discarding output *and* watermark together — clean. Silently skipping the CAS while
landing the output would be strictly worse (duplicate rows, no cursor). But it must **not** reuse
`Conflict`: the worker maps `Conflict` → `abandon("superseded: watermark advanced concurrently")`
(`stream_mv.rs:255-258`), a misleading message for "your def was deleted". Use `Validation` (also
Abandons, `stream_mv.rs:259-261`) with an honest message.

Implement in the **memory fake too** (`memory/src/stream.rs:116-142`) — it can consult
`self.transforms`; mind the documented lock order (`transforms` before `mv_watermarks`, never held
together).

**(b) A GC sweep of defless keys is NOT safe as stated** and is at best belt-and-braces. Its natural
home is `gc_locked`, which already computes both sides of the set difference (`watermark_mvs(tid)`,
`mv_floor.rs:158-166`, minus `pg_micro_batch_readers(table)`). But it would delete the cursor of (i)
a legitimately running **ad-hoc** MV run and (ii) an MV whose `define_transform` tx is open and not
yet visible (the sweep takes only the *table* advisory lock, not `TRANSFORM_DEFINE_LOCK`). Both reset
to 0 → the next run rescans from 0 → **duplicated output**. Making it safe needs at least
`TRANSFORM_DEFINE_LOCK` **and** a way to distinguish a live ad-hoc key from a dead one — which does
not exist today (nothing links a watermark key to a live run).

### What a human must decide before building

1. **Are ad-hoc (defless) MicroBatch runs supported?** They work today by accident — the admin route
   accepts the variant while its own doc-comment says `physical|typed`. If **no** (the reading the
   evidence supports), the cheapest correct move is to **reject `MicroBatch`/`MicroBatchJoin` bodies
   in `run_adhoc_route`**, after which (a) is unambiguous. If **yes**, (a) needs a carve-out (the
   guard would also have to accept a live non-terminal `transforms.run` whose frozen body names the
   key — currently unrepresentable without a schema change) and (b) is off the table entirely.
2. Whether `delete_transform` / `define_transform` should additionally **cancel the MV's queued jobs
   and runs**. Nothing does today. Without it, every torn-down MV leaves a job that will run, fail the
   new guard, and mark a run Failed — noise, but no corruption.
3. Whether the **two-defs-one-output** hazard below is in scope.

## Adjacent defect found in review (file separately if out of scope)

Because the watermark key is the **output** and `delete_transform` deletes by `mv_key(output)`
alone, if **two defs share an output**, `delete_transform(A)` wipes the watermarks def **B** is
still legitimately using — B then rescans its source from offset 0 and re-materializes everything.
Same failure class, **admin path, no race needed**. `transforms.rs:536-538` documents the
"keyed by `mv_key(output)` alone" choice but reasons only about the single-def case.

## Testing

**No concurrency needed — the race is fully sequential.** The CAS re-reads nothing and holds no
lock, so "the run's CAS lands after the delete" is observationally identical to calling the CAS
after the delete on the same connection:

```
define_transform(MicroBatch { source, output })                    // def exists
advance_mv_watermark(mv_key(output), tid, [{b:0, from:0, to:3}])   // ok
delete_transform(name)                                             // rows gone (certified today)
advance_mv_watermark(mv_key(output), tid, [{b:0, from:0, to:6}])   // ← TODAY: succeeds → GHOST
assert!(mv_watermarks(mv, tid).is_empty())                         // ← the new invariant
```

This belongs in **`transforms_contract`**, not `mv_watermarks_contract`: it needs both traits, and
`transforms_contract`'s bound is already `CP: ControlPlane + Transforms + Ontology + MvWatermarks`
(`testkit/src/lib.rs:4982-4986`) — so it runs on **both backends** for free, right after the
existing delete/redefine assertions at `:5504`. Targets: `//src/control-plane/postgres:transforms`
(`BUCK:365-379`) and `//src/control-plane/memory:transforms` (`BUCK:141-148`).

Also cover:

- **Mixed advance after delete** (`[{b:0,from:0,to:3},{b:1,from:5,to:9}]`) — pins that the `from > 0`
  branch *already* rolls back, so the new guard is not what makes that case safe.
- **The queued-job case (the big one)** — a worker e2e (`worker/tests/stream_mv_e2e.rs`): enqueue an
  MV job, delete the def, then let the job run. Assert the output table is **not** re-created, no
  duplicate rows land, and no watermark rows appear.
- **`mv_floor` returns to empty** for the source after a post-delete CAS attempt
  (`postgres/tests/mv_floor.rs`, which already has `deleting_the_mv_registration_releases_the_floor`
  at `:739-774`).
- **Ad-hoc MV run** — whichever way decision (1) goes, pin it with a test.

## Non-regression

- A run whose def still exists is unaffected (the guard's `exists` is true).
- The `from > 0` UPDATE branch keeps its existing `Conflict`-on-missing-row semantics.
- The admin-path reconciliation and its contract assertions are untouched.
- New SQL ⇒ `tools/sqlx-prepare.sh` + commit `.sqlx`. Note `stream.rs:592-595,571-572` carries a
  **stale comment** claiming "sqlx regen unavailable in-env (initdb as root); convert to `query!`
  when regenerating locally" — `tools/sqlx-prepare.sh` exists and the cache is committed, so the
  guard SQL can and should be compile-time `query!`. Delete that comment.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `#[expect(lint, reason)]` locally.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- Tests are `rust_test`/`loom_fixture_test` targets only; contracts go in `testkit` and run on both
  backends.

## Acceptance

1. A watermark CAS whose def no longer exists **fails** with a `Validation` error (not `Conflict`),
   rolling back the output with it — pinned by a `transforms_contract` case on both backends.
2. A queued MV job that outlives its def cannot re-create the output table or land duplicate rows.
3. No defless watermark key can be created by a run (given decision (1) on ad-hoc runs).
4. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `pg_advance_mv_watermark` (`postgres/src/stream.rs:590-628`) + the memory fake
  (`memory/src/stream.rs:116-142`); `define_transform` / `delete_transform`
  (`postgres/src/transforms.rs:337-575`); `mv_key` (`core/src/stream.rs:146-153`);
  `pg_micro_batch_readers` (`transforms.rs:81-112`); `inline_append_decl`'s commit tx
  (`iceberg_inline.rs:450-788`, CAS at `:724-732`; `advance_mv_watermark_only` at `:806-816`);
  the worker's error mapping (`worker/src/stream_mv.rs:253-261`); `run_adhoc_route`
  (`runtime/src/admin.rs:1510-1543`); `transforms_contract` (`testkit/src/lib.rs:4982,5504-5651`).
- Produces: the def-existence guard on the CAS (both backends) + its `Validation`; the ad-hoc
  MicroBatch decision; the `transforms_contract` ghost case + the queued-job worker e2e.
