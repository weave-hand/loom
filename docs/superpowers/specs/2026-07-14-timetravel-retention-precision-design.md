# Time-travel retention guard — precision over proxy Design

> **Status:** design (direction). This spec makes
> `iss-timetravel-quiet-table-overconservative` build-ready. The item stays in ISSUES
> (a defect in shipped code); a separate work agent writes the implementation plan from
> it and builds it.

## Problem

`ensure_within_retention` (`query-api/src/handler.rs:452-490`) 410s a time-travel read
(`?as_of` / `?as_of_snapshot`) whenever the resolved snapshot `at` is older than the GC
retention horizon `H = max(snapshot_id) where snapshot_time < now() - gc_retention`
(`Catalog::snapshot_horizon` → `iceberg_mirror::horizon_before`, the *same* derivation
`iceberg_gc::gc_locked` reclaims under, so guard and reclaimer cannot drift). The `table`
argument is used **only for the error message** — the guard is entirely table-independent.

It is over-conservative: many tables are never GC'd at all (GC is opt-in per table via the
maintenance scheduler or the `GcTable` RPC), and an append-only table end-caps nothing, so
its old snapshots are complete *forever* — yet every read below `H` is refused.

## The register entry's framing is wrong — read this before planning

The entry proposes a per-table **"quiet-table exemption"** ("the table was never rewritten
since that snapshot"). Research against the tree says that proxy is wrong on **both** sides,
and would not even fix the motivating example.

**Too strong.** "No writes since `S`" is not what makes a read complete. An **append** never
end-caps anything, so old reads stay complete no matter how many appends follow. The
existing guard test (`query-api/tests/as_of_guards_e2e.rs:142-144`) asserts 410 for exactly
this shape, and its comment — "no exemption applies to a genuinely stale read of a table
that has since been **rewritten**" — is **factually wrong about its own fixture**:
`seed_arrays` → `fixture::iceberg_writer::append_batches` is an *append*. `main.thing` at S1
is fully readable and always will be. Under the correct fix that assertion **flips to 200**,
and the test needs a genuinely end-capping fixture (an overwrite/truncate) to retain a real
410 case.

**Too weak — and this one is unsound.** Governed delete-all →
`IcebergActionWriter::overwrite_table` with empty IPC → `overwrite_truncate`
(`iceberg_landing.rs:1260-1279`) end-caps **every** live data file and inline row and writes
**no new row of any kind**. GC then deletes those `data_file` rows, their stats, and the
Parquet objects. The surviving mirror state for the table is: a live `table` row
(`begin = S0`), its `column` rows, and *zero* data files. A "max(begin|end) over surviving
rows" query therefore reports **"quiet since S"** — and we would serve a read that silently
returns **0 rows** instead of the data that was live at `S`. The proxy converts a loud 410
into silent data loss.

**The exemption's removal was more right than recorded.** The entry says the branch "could
never independently flip a verdict" — true for *live* tables (`H ≤ tip = current_snapshot`,
since `iceberg_mirror.snapshot_seq` is one catalog-global sequence and the snapshot row names
no table). But for a **dropped-but-not-yet-reclaimed** incarnation, `current_snapshot` is the
last snapshot before the drop, which can be `< H` — there the branch was not dead, it was
**actively unsound** (it would have flipped `Gone` → `Ok` for precisely the read whose files
were end-capped at the drop). Deleting it was correct; the stated reason was incomplete.

## The precise condition

The mirror is MVCC (`begin ≤ at < end`), and GC reclaims exactly `end_snapshot IS NOT NULL
AND end_snapshot <= H` (module invariant, `iceberg_gc.rs:14-22`; live rows are never
touched). A deletion harms a read at `S` iff the deleted row was **visible** at `S`:

> A read at `S` is incomplete ⟺ ∃ a reclaimed row/file `r` of the incarnation live at `S`
> with `begin(r) ≤ S < end(r) ≤ H`.

That is the condition to enforce. It cannot be evaluated on today's schema, for one reason:
**reclaimed-ness is recorded nowhere.** `gc_locked` writes no audit row — it returns a
`GcSummary` to its RPC caller and nothing else. Any predicate computed from *surviving*
mirror rows is blind to what GC already destroyed (that is exactly the truncate
counterexample).

## Design

Make GC record what it destroyed, then test the real condition.

1. **Durable reclaim watermark (new migration, next free number `0044`).** Add
   `iceberg_mirror.table.reclaimed_through bigint not null default 0`. Inside `gc_locked`'s
   existing transaction (`iceberg_gc.rs:150-170`), set it to
   `max(reclaimed_through, max(end_snapshot) over everything reclaimed this run)`. Keying on
   `table_id` gives **per-incarnation** scoping for free, which is required (see below).
2. **The guard becomes a conjunction.** Serve `S` iff **both**:
   - `S >= reclaimed_through(tid_at_S)` — nothing visible at `S` has already been destroyed; and
   - there is **no surviving row** of `tid_at_S` (data files ∪ inline) with
     `begin ≤ S < end ≤ H` — nothing visible at `S` is *eligible* to be destroyed at any
     moment, including mid-read.

   The second clause is what closes the race with a concurrent GC, and the conjunction is
   **monotone**: a GC that reclaims such a row simultaneously bumps the watermark above `S`,
   so a verdict can never flip from "complete" to "incomplete" behind an already-served read.
3. **`at >= H` stays the cheap fast path** — unchanged semantics, no extra query, and it is
   the overwhelmingly common case.

This keeps the guard's "deterministic whether or not GC has actually run" property in the
**safe** direction: a table GC has never touched is now correctly *serveable*, because both
clauses are provable from the mirror. The quiet-table proxy is strictly worse on both axes;
**do not build it.**

### Why per-incarnation, not per `(schema, name)`

`resolve_table(table, at)` (`iceberg_catalog.rs:50-65`) picks the `table_id` live *at* `at`,
and dropped incarnations are reclaimed separately (`reclaim_dropped`, `iceberg_gc.rs:275-313`).
A read inside a dropped incarnation resolves fine until that incarnation is fully reclaimed
(`D <= H`), at which point the `table` row itself is deleted and the read degrades to a
**404** — the one case where the evidence self-destructs safely.

### Open question a human should confirm

Whether the second clause should also cover **schema** rows (`iceberg_mirror.column`
end-caps under schema evolution). Column end-caps do not destroy data, and GC does not
reclaim `column` rows except at full dropped-incarnation reclaim — so the recommendation is
**no**, data files ∪ inline rows only. Confirm before building.

## Non-regression

- `at >= H` — byte-identical (fast path, no new query).
- A table GC has reclaimed past `S` — still 410, now for a *provable* reason.
- The one intended behavior change: a below-`H` read of a table whose visible-at-`S` rows
  were never end-capped (the append-only case) now returns **200** instead of 410. This
  **flips an existing assertion** in `as_of_guards_e2e.rs:142-144` — that is the fix
  working, not a regression, and the test's prose must be rewritten.
- The floor can only ever *widen* what is served; it never serves an incomplete read.

## Testing

Mirror `query-api/tests/as_of_guards_e2e.rs` (`loom_fixture_test`, `query-api/BUCK:2114-2130`,
deps include `":e2e-support"`). Its pattern: `PgFixture::shared()` → `fx.fresh_db()` →
`fx.pool_for(&db)` → `fixture::IcebergWriter::new` → `writer.seed_arrays(...)` (returns the
loom snapshot id) → `cp.define_type(...)` → `IcebergCatalog::new(pool)` +
`e2e_support::InProcessServingEngine`; drive the guard with
`e2e_support::get_with_retention(cp, eng, uri, "alice", Duration::ZERO)` (vs `get`, which
uses the 7-day `TEST_GC_RETENTION`).

- **Append-only below H now serves** (the acceptance test) — seed S1, append S2, retention
  zero: `?as_of_snapshot=S1` → **200** with S1's rows. Rewrite the stale prose at
  `as_of_guards_e2e.rs:6-7,142-144`.
- **A genuinely end-capped read still 410s** — new fixture that actually end-caps (overwrite
  or truncate) — this is the case the current test only *claims* to cover.
- **The truncate trap (the unsoundness pin)** — seed at S0, governed delete-all (→
  `overwrite_truncate`, end-caps everything, writes nothing), run GC, then read at S0. Must
  be **410**, never 200-with-zero-rows. A quiet-table proxy fails exactly here; this test is
  what forbids anyone re-introducing it.
- **Eligible-but-not-yet-reclaimed** — end-cap below `H` but never run GC: still 410 (clause
  2), proving the guard does not depend on GC having run.
- **Watermark monotonicity** — two GC runs; `reclaimed_through` never regresses.
- **Dropped incarnation** — read inside a dropped-but-unreclaimed incarnation; after full
  reclaim it 404s (not 410, not 200).
- GC-side fixture updates in `postgres/tests/iceberg_gc.rs` for the new watermark write.

## Blast radius

- **Callers of the guard**: only `handler.rs:448` (via `resolve_read_snapshot` → `read_object`,
  `read_object_page`) and `http.rs:331` (dataset detail). `QueryError::AsOfGone` → 410 at
  `http.rs:967`; OpenAPI at `http.rs:294,519`.
- **The engine has no parallel guard.** `EngineTicket::AsOfSql` → `do_get_as_of_sql`
  (`engine/src/flight.rs:102-117`) runs with **no** retention check — query-api is the sole
  enforcement point. Fine while the Flight surface is internal-only; say so explicitly rather
  than assume symmetry (it becomes a real gap when the external SQL wire lands).
- **New SQL + trait surface.** A new `Catalog` method lands in `core/src/catalog.rs` (trait),
  `postgres/src/iceberg_catalog.rs` (impl), `memory/src/catalog.rs` (impl — the memory
  catalog also models one global snapshot list), plus a **testkit contract** next to the
  existing `snapshot_horizon` one (`testkit/src/lib.rs:699-723`, runs on both backends).
- The **inline tier is a dynamic relation** (`iceberg_mirror.inline_<table_id>`), so clause 2
  cannot be a single compile-time `sqlx::query!` — it needs a `to_regclass` probe + runtime
  `AssertSqlSafe`, exactly as `iceberg_gc::delete_end_capped_inline_rows` /
  `count_candidates` already do. Static parts stay compile-time → `tools/sqlx-prepare.sh` +
  commit `.sqlx` (enforced by `//src/control-plane/postgres:sqlx-cache-check`).
- **Docs**: `docs/system-capabilities/query-api.md:60,64,105` describes today's conservative
  behavior and names the issue id; it moves with the fix.

## Known assumption (inherited, not introduced)

`H = max(snapshot_id) where snapshot_time < cutoff` assumes snapshot-id order tracks
`snapshot_time` order. `snapshot_time` defaults to `now()` (transaction *start*) while the id
comes from `nextval` at an arbitrary point in the transaction, so ids and times can invert
under concurrent long writers. GC and the guard share the derivation, so they never disagree
with *each other* — but this fix starts reasoning about per-row `end_snapshot` vs `H` and
inherits the assumption. Not a blocker; worth one line in the plan.

## Out of scope (deferred)

- Reconciling query-api's `LOOM_GC_RETENTION_SECS` with the engine's own copy (already
  documented as a caveat in `docs/system-capabilities/query-api.md:64`).
- A retention guard on the engine's internal Flight `AsOfSql` ticket — rides the external SQL
  wire.

## Acceptance

1. A below-`H` time-travel read of a table whose visible-at-`S` rows were never end-capped
   returns **200** with the correct rows.
2. A read whose visible-at-`S` rows were end-capped and reclaimed still returns **410** —
   and the truncate case (end-cap with no new rows written) is pinned by a test.
3. A read that is merely *eligible* for reclaim (end-capped below `H`, GC never run) still
   410s — the guard does not depend on GC having run.
4. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `ensure_within_retention` (`query-api/src/handler.rs:452-490`);
  `resolve_read_snapshot` (`handler.rs:413-450`); `Catalog::snapshot_horizon`
  (`core/src/catalog.rs:90-94`) → `iceberg_mirror::horizon_before`
  (`postgres/src/iceberg_mirror.rs:277-289`); `resolve_table`
  (`postgres/src/iceberg_catalog.rs:50-65`); `gc_locked` / `reclaim_live` / `reclaim_dropped`
  (`postgres/src/iceberg_gc.rs:121-313`); the mirror MVCC columns
  (`migrations/0012_iceberg_mirror.sql`) and the dynamic inline relation
  (`postgres/src/iceberg_inline.rs:276-297`).
- Produces: migration `0044` (`iceberg_mirror.table.reclaimed_through`); the GC-side
  watermark write; a new `Catalog` method + testkit contract for the two-clause predicate; the
  rewritten `as_of_guards_e2e.rs` narrative + the truncate-trap regression test.
