# MV/CDC declare-vs-register race Design

> **Status:** design (direction). This spec makes `iss-mv-cdc-declare-register-race` build-ready.
> The item stays in ISSUES (a defect in shipped code); a separate work agent writes the
> implementation plan from it and builds it.

## Problem

The MV/CDC mutual exclusion (shipped #443) is enforced from both sides:

- `define_transform` refuses a micro-batch MV whose source is a declared CDC table —
  `pg_refuse_mv_over_cdc_source` (`postgres/src/stream.rs:495-515`, called from
  `transforms.rs:568`).
- `reconcile_stream_mode` refuses a CDC declaration on a table a micro-batch MV already sources —
  `pg_refuse_cdc_over_mv_source` (`stream.rs:531-548`, called unconditionally at `stream.rs:259`).

That closes both **sequential** orderings. The residual race: the two guards run in **separate
transactions with no shared lock**, so a `land_cdc` committing concurrently with a
`define_transform` can each read a snapshot in which the other's row does not yet exist, both
pass, both commit — the wedged state (a micro-batch MV sourcing a declared CDC table: the MV can
never run, its watermark never advances, `mv_floor` pins the source at offset 0 forever, and the
consolidate fold permanently skips-and-re-arms).

`define_transform` already takes the per-table advisory lock for MV-source bodies
(`lock_key(&src.schema, &src.name)`, `transforms.rs:503-509`) — but that lock's documented
purpose is the GC/floor reclaim race; the ingest declaration path never contends for it, so it
cannot serialize this race on its own.

## Research findings that shape the fix

1. **`lock_key` is shared infrastructure.** `pub fn lock_key(schema, name) -> i64`
   (`iceberg_flush.rs:430`); taken as blocking, transaction-scoped `pg_advisory_xact_lock` by
   flush (`iceberg_flush.rs:66-70`), consolidate (`:463-471` via `TableLock`), GC
   (`iceberg_gc.rs:112-116`), and `define_transform` (`transforms.rs:504-508`).
2. **The declaration only happens in the first-declare arm.** `reconcile_stream_mode`
   (`stream.rs:234`) runs inside the write transaction on every `?mode=cdc`/`?mode=stream` land
   (callers: `land_parquet_stream` at `iceberg_landing.rs:982`, `inline_append` at
   `iceberg_inline.rs:595`), but the actual DECLARE fires only in the `(Some, None)` arm gated on
   `existing_meta.is_none()` (`stream.rs:271,279-327`). Steady-state appends hit the
   redeclare-validate arm (`:275-278`). **So the register's framing — "every `?mode=cdc` write
   would serialize against transform registration" — is avoidable: lock only when declaring.**
3. **The guard is a full-table scan on every CDC land today.** `pg_refuse_cdc_over_mv_source`
   is a no-op for non-CDC decls but for CDC calls `pg_micro_batch_readers` (`transforms.rs:81`),
   a full scan of `transforms.transform` decoding bodies — on **every** CDC append.
4. **Lock ordering is deadlock-safe.** `define_transform` takes the global
   `TRANSFORM_DEFINE_LOCK` (`transforms.rs:489`) *then* `lock_key(source)` (`:504`); the ingest
   side would take only `lock_key(table)` and never the global lock — no cycle. Preserve this
   property (the warning at `transforms.rs:500-502`).
5. **A deterministic race-test technique exists.** `tests/stream_first_declare_race.rs:204`
   (`await_declare_blocked`) polls `pg_stat_activity` until the losing session is observably
   blocked on the lock — no sleeps.

## Decision (operator, 2026-07-21): lock the first-declare arm only

Restructure `reconcile_stream_mode`'s declare path:

1. Read `existing_meta` (cheap, unchanged).
2. **If a declaration would be created** (`existing_meta.is_none()` and the decl declares a
   stream/CDC kind): take `pg_advisory_xact_lock(lock_key(schema, name))` on the landing
   transaction, **re-read** `existing_meta` (a concurrent first-declare may have committed while
   we waited — if now `Some`, fall through to the redeclare-validate arm), then run
   `pg_refuse_cdc_over_mv_source` **under the lock**, then declare.
3. Steady-state appends (`existing_meta` already `Some`) take **no lock** — zero hot-path cost.

With this, the CDC-declare and the MV-register mutually exclude: whichever transaction takes the
per-table lock second blocks until the first commits, then its guard read sees the winner's
committed row and refuses. Both orders converge to "exactly one of {CDC declaration, MV
registration} exists" — the wedged state becomes unreachable.

**Move the guard into the guarded arm.** Since a committed CDC declaration makes
`define_transform`'s own guard refuse any later MV, and a committed MV registration makes the
(now-locked) first-declare guard refuse the CDC declaration, the per-land
`pg_refuse_cdc_over_mv_source` call at the top of `reconcile_stream_mode` (`:259`) is only
load-bearing at first-declare. Relocate it into the first-declare arm (under the lock). This is
also a hot-path **improvement**: steady-state CDC appends stop paying a full
`transforms.transform` scan per land. The plan must verify the redeclare-validate arm cannot
transition a table's kind to CDC (kind-change is refused — `stream_log_vs_cdc_declare` coverage);
if any path could, the guard must also cover it.

Lock **any** first-declare (log or CDC), not just CDC: it is a once-per-table event, the
symmetry is simpler to reason about, and it also serializes first-declares against GC/flush/
consolidate holders of the same key (harmless, brief).

**Memory backend:** the memory fake serializes everything under its own mutexes and cannot
exhibit the race; no change beyond keeping guard placement behaviorally equivalent (the
sequential-refusal contract stays certified on both backends).

## Testing

- **Race test** in `postgres/tests/mv_source_refuse.rs` (which already has `define_mv`,
  `declare_cdc_via_land`, `ensure` helpers and both sequential cases at `:122`/`:178`), reusing
  the `pg_stat_activity` barrier technique from `stream_first_declare_race.rs`:
  - Order A: open a `define_transform`-shaped transaction holding `lock_key(source)`; start a
    concurrent first CDC-declaring land; barrier until it is blocked on the lock; commit the
    define; assert the land is refused (typed refusal, not a timeout/serialization error).
  - Order B: hold the lock from a first-declaring land transaction; run `define_transform`
    concurrently; barrier; commit the land; assert the define is refused.
  - Post-condition in both: the wedged state is absent — never both a CDC `stream_table` row and
    a micro-batch MV body naming that source.
- **Hot-path pin:** a steady-state CDC append to an already-declared table takes no table
  advisory lock (assert via `pg_locks` from the test connection mid-transaction, or by
  construction — the lock call is only reachable in the first-declare arm).
- Existing sequential guard tests (`mv_source_refuse.rs`), `stream_first_declare_race.rs`,
  `stream_log_vs_cdc_declare.rs`, `define_transform_refuse.rs` stay green.

## Non-regression

- No schema change, no wire change, no `.sqlx` change unless the guard SQL moves textually
  (if it does: `tools/sqlx-prepare.sh` + commit the cache).
- Lock ordering: ingest takes only `lock_key(table)`; never introduce `TRANSFORM_DEFINE_LOCK`
  on the ingest side.
- The exclusion (and this lock) lifts wholesale when `[[fut-mv-cdc-source]]` lands; keep the
  mechanism local to `reconcile_stream_mode` so it deletes cleanly.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `#[expect(lint, reason)]` locally.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- Fixture tests use `loom_fixture_test`; the race test is postgres-only by nature (advisory
  locks), like `stream_first_declare_race.rs`.

## Acceptance

1. A concurrent first CDC declaration and micro-batch MV registration serialize: exactly one
   commits, the loser gets the existing typed refusal — pinned by the barrier-based race test in
   both orders.
2. Steady-state CDC appends to a declared table take no per-table advisory lock and no longer
   run the `pg_micro_batch_readers` scan per land.
3. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `reconcile_stream_mode` (`postgres/src/stream.rs:234-327`),
  `pg_refuse_cdc_over_mv_source` (`stream.rs:531-548`), `pg_refuse_mv_over_cdc_source`
  (`stream.rs:495-515`), `lock_key` (`iceberg_flush.rs:430`), `define_transform`'s lock block
  (`transforms.rs:480-509`), the barrier helper pattern
  (`tests/stream_first_declare_race.rs:204`), `tests/mv_source_refuse.rs` and its helpers.
- Produces: the conditionally-locked first-declare arm (lock → re-read → guard → declare), the
  relocated guard, the two-order race test + hot-path pin.
