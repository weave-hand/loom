# ensure_table first-write race — design

**Item:** `#iss-ensure-table-first-write-race`

## Problem

`ensure_table` (`src/control-plane/postgres/src/iceberg_mirror.rs:64-93`)
resolves a live `iceberg_mirror.table` row for `(ns, name)` by
SELECT-live-row-then-INSERT with no conflict handling:

1. `select table_id … where … end_snapshot is null` (`fetch_optional`,
   lines 70-81) — return it if a live row exists;
2. otherwise `insert … returning table_id` (`fetch_one`, lines 82-92).

The two steps are not atomic. Two writers issuing their **first** write to the
same brand-new `schema.table` both observe no live row at step 1, so both reach
step 2. The `iceberg_table_one_live_idx` **partial unique index** —
`(table_namespace, table_name) where end_snapshot is null`
(`migrations/0012_iceberg_mirror.sql:27-29`), which loom owns to guarantee *at
most one live row per table* (the invariant the read-path MVCC predicates
assume) — admits exactly one of the two inserts. The loser's INSERT trips a
Postgres `unique_violation` (SQLSTATE `23505`), which `map_err(backend)` wraps
into a raw `ControlPlaneError::Backend`. Instead of resolving to the winner's
`table_id` (or retrying cleanly), the loser surfaces a low-level backend error
and — because the violation aborts the statement inside the caller's
transaction — nothing it attempted commits.

This is **pre-existing** (it predates the stream engine) and **narrow**: it
requires two genuinely-concurrent *first* writes to *one new* table. Slice 1's
`stream_inline` concurrency test
(`tests/stream_inline.rs:379`,
`concurrent_first_declare_with_differing_counts_does_not_desync`) surfaced it —
that test currently *tolerates* either the `ensure_table` `23505` shape or the
stream-declare `Conflict` (`is_benign_ensure_table_race`, lines 408-419) and
pins only the real desync invariant. The race is one facet of the broader
multi-writer-ingest open question in `ARCHITECTURE.md`.

## Scope

- Make `ensure_table`'s INSERT race-safe: on a `23505` from the live-row unique
  index, roll back to a savepoint and re-SELECT the now-committed live row, so
  the losing writer returns the **winner's** `table_id` — the same value a
  serialized winner-then-loser ordering would have produced.
- Tighten the concurrency coverage that surfaced the race (see **Testing**).

**Non-goals (explicit):**

- **Not** the broader multi-writer-ingest question (`ARCHITECTURE.md`): this
  fixes one narrow first-write collision, not concurrent *appends*, lock
  strategy, or writer coordination in general.
- **No schema/index change** — `iceberg_table_one_live_idx` stays exactly as is;
  it is the mechanism the fix *relies on*, not something to relax.
- **No change to the already-live path** (step 1): a writer that finds a live
  row already returns it correctly. The fix only guards the INSERT branch.
- No change to `ensure_table`'s signature or its `&mut PgConnection` /
  return-`i64` contract; callers are untouched.

## Design

**Reuse the existing savepoint + duplicate-race helpers.** `iceberg_inline.rs`
already carries the two primitives this fix needs:

- `is_duplicate_object_race(&sqlx::Error) -> bool`
  (`iceberg_inline.rs:304-308`) — true for `42P07`/`42701`/`23505`; the `23505`
  arm is exactly the live-index unique violation here.
- `run_idempotent_ddl` (`iceberg_inline.rs:317-353`) — the SAVEPOINT wrap: open
  `savepoint loom_ddl`, run the statement, `release` on success, and on a
  duplicate-race error `rollback to savepoint` + `release` so the object "now
  exists" and the **outer transaction survives** (a raw duplicate would poison
  it).

`ensure_table` differs from `run_idempotent_ddl` in one way: it must *return the
winner's `table_id`*, not merely swallow the duplicate. So the fix applies the
same savepoint *shape* to the INSERT but, in the catch arm, re-runs step 1's
live-row SELECT and returns that id. Concretely, wrap the step-2 INSERT:

1. `savepoint loom_ensure_table`;
2. run the `insert … returning table_id`;
3. on `Ok(tid)` → `release savepoint` → return `tid`;
4. on `Err(e)` where `is_duplicate_object_race(&e)` → `rollback to savepoint`
   (undo the aborted INSERT so the outer tx is usable again) → **re-SELECT the
   live row** (the same query text as step 1) → return its `table_id`;
5. on any other `Err(e)` → `rollback to savepoint`, then `map_err(backend)` and
   surface.

**Helper reuse / visibility.** `is_duplicate_object_race` is private to
`iceberg_inline`; promote it to `pub(crate)` (or lift both it and a small
savepoint helper into a shared module) so `iceberg_mirror` can call it rather
than re-deriving the `23505` check. `run_idempotent_ddl` itself is *not* reused
verbatim — it returns `()` and cannot re-SELECT — but the catch/rollback
structure is copied deliberately (matching its comments) so the two race guards
read alike. Savepoint statements use `AssertSqlSafe` with static names, exactly
as `run_idempotent_ddl` does.

**Transaction / savepoint nesting.** `ensure_table` always runs inside the
caller's outer transaction — every production caller threads a
`conn: &mut PgConnection = &mut tx` from a `pool.begin()`
(`iceberg_inline.rs:411-423`, `iceberg_landing.rs:539`, `:740`). A savepoint is
therefore always nested within a live transaction (Postgres requires this), and
`rollback to savepoint` rewinds only the failed INSERT, leaving the rest of the
caller's in-flight work intact. `ensure_table` never owns a transaction, so no
new `begin()` is introduced.

**Liveness of the re-SELECT.** The re-SELECT is guaranteed to find exactly one
committed live row. Under Postgres's default READ COMMITTED isolation (loom sets
no other level), an INSERT that conflicts on a unique index with a *concurrent
uncommitted* row **blocks** until that transaction resolves: if the winner
commits, the loser then receives `23505`; if the winner rolls back, the loser's
INSERT succeeds instead. So by the time the loser observes `23505`, the winner's
transaction has **committed**, its row is live, and — because the partial unique
index permits exactly one live `(ns, name)` — the loser's re-SELECT (issued as a
new statement after `rollback to savepoint`, thus reading a fresh READ COMMITTED
snapshot) sees precisely that one row. A "no live row found" result at step 4
would contradict the index invariant and should map to a `backend` error, not a
second INSERT. (This liveness argument is specific to READ COMMITTED; a future
move to REPEATABLE READ/SERIALIZABLE would surface serialization failures the
caller must retry instead — worth a note but out of scope here.)

**`.sqlx` regen.** None required. The re-SELECT reuses the *existing* live-row
`query_scalar!` SQL verbatim (same text ⇒ same cache entry), and the savepoint
statements are runtime `AssertSqlSafe` (no compile-time macro, no `.sqlx`
entry). The `sqlx-cache-check` test needs no new fixtures.

## Testing

- **Tighten the existing test.** With the race fixed, the `ensure_table` stage
  can no longer emit a `23505` to a caller, so
  `concurrent_first_declare_with_differing_counts_does_not_desync`
  (`tests/stream_inline.rs:379`) should drop `is_benign_ensure_table_race` and
  assert the losing *differing-count* writer is exactly
  `ControlPlaneError::Conflict` (the stream-declare rejection) — never a raw
  backend `23505`. The desync invariant (no `loom_bucket ≥ bucket_count`) stays.
- **New focused race test.** Add a fixture test that races two first-writes with
  **identical** params to one brand-new table (two `ensure_table` calls on
  concurrent transactions, or two same-bucket-count `inline_append`s): assert
  **both** return `Ok` with the **same** `table_id`, that exactly one live
  `iceberg_mirror.table` row exists for `(ns, name)`, and that no `23505` is
  surfaced to either caller. This is the invariant the fix adds and that no
  current test asserts.
- **Determinism.** Race tests are inherently scheduling-dependent; run under the
  existing `multi_thread` fixture flavor and assert the *outcome* (same id, one
  live row) rather than which writer wins, matching the existing test's style.
