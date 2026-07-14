# First-declare arm — kind-symmetric post-race re-read Design

> **Status:** design (direction). This spec makes `iss-stream-first-declare-race-kind`
> build-ready. The item stays in ISSUES (a defect in shipped code); a separate work agent
> writes the implementation plan from it and builds it.

## Problem

`reconcile_stream_mode` (`postgres/src/stream.rs:56-269`) dispatches on
`(requested_buckets, existing_buckets)`. PR #432 (`#iss-stream-log-vs-cdc-declare`, closed)
made the **count-equal** arm `(Some(_), Some(m))` kind-symmetric — a declare whose
`StreamDecl` kind disagrees with the stored `StreamMeta.kind` is now a `Validation` error in
both directions.

The **first-declare** arm `(Some(n), None)` (`stream.rs:197-250`) still has the hole. It
exists because two writers can race to declare a brand-new table: the declare is
`insert … on conflict (table_id) do nothing` (`pg_declare_stream`, `stream.rs:337-342`), so
the loser's insert no-ops, and it then re-reads what the winner actually recorded:

```rust
236	    let stored = pg_stream_bucket_count(&mut *conn, tid)   // <-- COUNT ONLY, NO KIND
243	    if stored != n { return Err(Conflict("stream bucket count mismatch …")); }
249	    Some(stored)
```

`pg_stream_bucket_count` compares only the **count**. A log first-declare that loses to a CDC
first-declare with the same bucket count passes `stored == n` and proceeds against a
`kind='cdc'` row — log framing stamped into CDC storage, exactly what the count-equal arm now
rejects.

**The primitive is verified, not assumed.** Booting the pinned hermetic Postgres (PG 17.9):
`on conflict do nothing` **blocks** against an *uncommitted* conflicting insert
(`wait_event_type=Lock, wait_event=transactionid`); when the winner commits, the loser's
insert returns `INSERT 0 0` and its next read — in the same still-open READ COMMITTED
transaction — sees the winner's `kind='cdc'` row. The mechanism is exactly as described.

Note the CDC sub-branch (`stream.rs:205-227`) does *extra writes* before the re-read (the
changelog mirror row + an unconditional `pg_set_changelog_table_id` UPDATE), so a CDC loser
stamps `changelog_table_id` onto the winner's log row. The caller's rollback undoes it today,
but the kind check should fire **before** those writes.

## What the register entry misses — read this before planning

**1. The fix is two lines and needs no SQL.** `pg_stream_meta` (`stream.rs:423-456`) is a
sibling free function in the same module, is **already called 100 lines above** (`stream.rs:125`),
and its query is **already in the committed `.sqlx` cache**
(`query-065612427774e533b46d428bdcdf2891c9cdeb7968f7e4a369f6c11d5426fb9b.json`). So swapping
the re-read from `pg_stream_bucket_count` to `pg_stream_meta` and comparing `kind` alongside
`bucket_count` needs **no new statement, no `tools/sqlx-prepare.sh` run, and no `.sqlx`
change**. `sqlx-cache-check` stays green. (Only inventing *new* SQL — a `select … for update`,
a conditional insert — would change that. Don't.)

**2. The real work is testability, and the arm is harder to reach than the entry claims.**

- `reconcile_stream_mode` and `StreamDecl` are **`pub(crate)`** (`stream.rs:56`, `:17`). loom
  forbids inline `#[cfg(test)]` tests, so **no `tests/` integration test can call them today.**
  The spec's first task is to widen visibility (`pub`, or `#[doc(hidden)] pub`).
- **The natural two-`land` race does NOT reach this arm.** `iceberg_mirror::ensure_table`
  (`iceberg_mirror.rs:92-169`) is a SELECT-then-INSERT under a savepoint, and
  `iceberg_mirror.table` carries a **partial unique index**
  (`iceberg_table_one_live_idx … where end_snapshot is null`, `migrations/0012`). Two writers
  creating the same brand-new table therefore **serialize at `ensure_table`**: the loser blocks,
  takes 23505, rolls back to the savepoint, re-SELECTs the winner's `table_id` — and because a
  declaring writer commits the mirror row and the `stream.stream_table` row in the *same*
  transaction, the loser resumes only once **both** are committed. Its `pg_stream_meta` read at
  `stream.rs:125` therefore returns `Some`, routing it to the **already-fixed** count-equal arm.

  The `(Some(n), None)` window is reachable only when the mirror row is committed by a
  **different, earlier** transaction than the declaring one, while the declarer's
  `pre_existing` witness is still `false`. Two such paths exist:
  1. **`land_parquet`'s out-of-transaction `pre_existing` probe** (`iceberg_landing.rs:853-862`)
     — probed on a *separate pooled connection before the tx*. A stale `false` lets a
     stream-declaring writer proceed against a pre-committed `tid` with no `ensure_table`
     serialization.
  2. **`StreamTables::declare_stream` / `declare_cdc`** (`stream.rs:479,489`) — the public
     `control_plane_core` trait methods run standalone **on the pool** (autocommit, one
     statement), with no reconcile, no kind check, no count check. Test-only today, but they
     bypass every guard in `reconcile_stream_mode`.

  **Be honest in the plan:** this fix is **defense-in-depth on a narrow window**, not a
  routinely-hit bug. That is a reason to keep it small, not a reason to skip it.

**3. Give the first-declare arm a *distinct* error message.** If it raises the byte-identical
`Validation` the count-equal arm raises, a concurrency test **cannot prove which arm fired** —
a green test for the wrong reason. Use something like "…declared concurrently with a different
stream kind". Precedent for asserting on the message: `stream_log_vs_cdc_declare.rs:133-136`.

## Design

1. Widen `reconcile_stream_mode` + `StreamDecl` visibility enough for a `tests/` target to
   drive them directly.
2. In the `(Some(n), None)` arm, replace the `pg_stream_bucket_count` re-read with
   `pg_stream_meta` and validate **both** fields: `bucket_count != n` → the existing `Conflict`;
   `meta.kind` disagreeing with `decl`'s kind → a **new, distinctly-worded** `Validation`.
3. Move the kind check **ahead of** the CDC sub-branch's changelog writes so a losing CDC
   declare never stamps `changelog_table_id` onto a log winner's row.
4. Leave `pg_stream_bucket_count` alone. It is *also* used as a deliberately kind-agnostic
   "is this a stream table?" probe (`iceberg_inline.rs:481`, `iceberg_landing.rs:858,1224`,
   `iceberg_flush.rs:169` — any kind ⇒ framing). Do **not** change those.

## Testing

A deterministic two-connection fixture test — **no sleeps**. `stream.stream_table.table_id` is
a bare `bigint primary key` with **no FK** (`migrations/0035_stream_table.sql`), and
`reconcile_stream_mode` takes `tid` and `pre_existing` as parameters, so the test controls both:

1. **conn A** (raw `sqlx` tx): `begin; insert into stream.stream_table (table_id, bucket_count,
   kind, bucket_key) values (T, 2, 'cdc', 'id')` — hold **uncommitted**.
2. **conn B** (`tokio::spawn`): `begin;` then `reconcile_stream_mode(&mut txB, T,
   &StreamDecl::Log(2), /*pre_existing*/ false, &table, at)`. Its `:125` meta read sees `None`;
   its `pg_declare_stream` **blocks** on A.
3. **Barrier** (this is the load-bearing part): poll `pg_stat_activity` until B's backend shows
   `wait_event_type='Lock' AND wait_event='transactionid'` — empirically verified to be exactly
   what appears — **then** commit A. B unblocks → `INSERT 0 0` → re-read → post-fix
   `Err(Validation)` with the distinct message.

   Without the barrier, A might commit before B's `:125` read, sending B into the
   `(Some, Some)` arm — which would pass for the wrong reason. Assert on the **distinct
   message** to prove the first-declare arm fired.
4. Symmetric case (CDC request losing to a log winner).
5. Regression: a losing CDC declare leaves **no** `changelog_table_id` on the winner's row.

Closest in-repo concurrency precedent: `postgres/tests/acl_inheritance_concurrency.rs`
(`#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`, `tokio::spawn` + join) — but it
serializes on an advisory lock, not a held-open transaction, so the barrier technique is new
here.

New `loom_fixture_test` (**not** a bare `rust_test`) in `postgres/BUCK`, mirroring
`stream-log-vs-cdc-declare` (`BUCK:1673-1704`) but with a slimmer dep set — it never lands data:
`[":postgres", "//src/control-plane/core:core", "//third-party:sqlx", "//third-party:tokio"]`.

## Non-regression

- The count-equal arm and its #432 guards are untouched.
- A first-declare that wins its race is unaffected (`pg_stream_meta` returns its own row).
- No SQL change ⇒ `.sqlx` unchanged ⇒ `sqlx-cache-check` green.
- The memory fake has its own first-wins declare pinned by a testkit contract
  (`testkit/src/lib.rs:6141-6234`); `reconcile_stream_mode` is pg-only, so the fake needs no
  change.

## Out of scope (record as new issues in the closing PR)

- **`land_parquet`'s stale `pre_existing` witness** (`iceberg_landing.rs:853-862`) — probed
  outside the tx. Besides widening this race window, it already lets a batch→stream
  **conversion** slip past the `stream.rs:198` "cannot convert existing batch table" guard.
  That is a distinct defect; file it.
- **`StreamTables::declare_stream` / `declare_cdc` are unguarded public API** — pool-level,
  autocommit, no reconcile. Decide whether to document them as internal/test-only or route them
  through `reconcile_stream_mode`.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `#[expect(lint, reason)]` locally.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- Tests are `rust_test`/`loom_fixture_test` targets only.

## Acceptance

1. A log first-declare that loses a race to a CDC first-declare with the same bucket count is
   rejected with a `Validation` error — and the test proves the **first-declare** arm fired.
2. The symmetric case (CDC losing to log) is rejected too.
3. No `.sqlx` change; existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `reconcile_stream_mode` (`postgres/src/stream.rs:56-269`, `pub(crate)` — widen);
  `StreamDecl` (`stream.rs:17-29`, `pub(crate)` — widen); `pg_stream_meta`
  (`stream.rs:423-456`); `pg_stream_bucket_count` (`stream.rs:350-362`, leave alone);
  `pg_declare_stream` / `pg_declare_cdc` (`stream.rs:332-420`); `StreamMeta` / `StreamKind`
  (`core/src/stream.rs:12-84`); `ControlPlaneError::Validation` (`core/src/error.rs:31`).
- Produces: the kind-aware first-declare re-read + its distinct `Validation`; the
  `pg_stat_activity`-barrier two-connection fixture test.
