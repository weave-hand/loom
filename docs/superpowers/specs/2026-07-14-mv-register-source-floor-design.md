# MV registration against a truncated source Design

> **Status:** design (direction). This spec makes `iss-mv-register-below-reclaimed-floor`
> build-ready. The item stays in ISSUES (a defect in shipped code); a separate work agent writes
> the implementation plan from it and builds it.

## Problem

A newly registered micro-batch MV has no `stream.mv_watermark` rows, so `mv_floor` defaults it to
**0** for every bucket (`mv_floor.rs:99-110`, `unwrap_or(0)`) and its first delta is defined as
"the source from offset 0" (`mv_delta.rs`, `loom_offset >= 0`). But offsets below the floor in
force *before* it registered may already be gone. Nothing validates that: `define_transform`
(`postgres/src/transforms.rs:337-487`) is **one upsert into `transforms.transform`** — it never
touches `stream.*`, never reads the source's surviving offsets, never checks the source exists.
The MV's first run silently under-reads a short prefix rather than failing loud.

The source is recorded **only inside the JSONB `body`** (`TransformBody::MicroBatch { source, … }`),
which is why "who reads table T" is a full scan + decode of every transform body
(`pg_micro_batch_readers`, `transforms.rs:81-112`).

## Corrections and additions to the register entry

**1. The harm is understated — registration is also an *over-hold*.** Because `mv_floor`'s reader
set includes registered-but-unrun MVs and defaults them to 0, **registering any new MV instantly
floors its source's GC at 0 in every bucket** until that MV first runs — pinning every end-capped
byte and inflating `held_by_mv_floor`. Bootstrapping fixes the under-read *and* the over-hold with
one write. That materially strengthens the case for bootstrap over reject.

**2. "Already gone" happens at end-cap, not at reclaim.** GC only ever reclaims **end-capped** rows,
and an end-capped row is *already* invisible to `mv_delta` (which reads live-at-current-snapshot).
So the query the fix needs must be defined over **live-at-the-current-snapshot** data — **not**
"rows still present in the mirror". The entry's fix-shape prose glosses this, and getting it wrong
would compute a floor over data the MV can never read. (Root cause of the prefix loss is
`#iss-end-cap-ignores-mv-floor`; this item is about registering *sanely* against a source that has
already lost a prefix.)

**3. The lock diagnosis is right; the implied remedy is wrong.** The entry says `mv_floor` is read
"on the pool, outside the GC transaction" — true (`iceberg_gc.rs:144` vs the `pool.begin()` at
`:153`). But **both** the floor read and the reclaim already sit inside `gc_table`'s **per-table
advisory lock** (`lock_key`, held across all of `gc_locked`, `iceberg_gc.rs:99-119`). Moving the
read inside the GC transaction fixes **nothing**. The actual hole is one-sided:
**`define_transform` takes only the global `TRANSFORM_DEFINE_LOCK`, never `lock_key(source)`** —
so a registration is free to commit inside GC's locked window. Say this explicitly, or an
implementer will fix the wrong thing.

**4. It is architecturally cheap.** `gc_locked` lives in **`postgres/src/iceberg_gc.rs`**, the same
crate as `transforms.rs`, and `lock_key` is already `pub(crate)`. `define_transform` can take
`pg_advisory_xact_lock(lock_key(source.schema, source.name))` with no new API surface. (The public
wrapper `iceberg_flush::lock_table` is what `engine-serving` already uses, so the same key already
serializes flush / consolidate / GC / MV-delta-read.)

**⚠ Deadlock hazard the plan must respect.** The commit path takes `lock_table(table)` **and then**
row-locks `transforms.transform` (`pg_fire_data_triggers`, `transforms.rs:229-235`, `for update`).
`define_transform` today row-locks its own def at `:440-446`. Acquiring `lock_key(source)` *after*
that `for update` gives a classic inversion. **`lock_key(source)` must be acquired at the top of the
transaction — immediately after `TRANSFORM_DEFINE_LOCK`, before any `transforms.transform` access.**
Global order: `TRANSFORM_DEFINE_LOCK → lock_key(table) → transform row locks`, consistent with the
commit path.

**5. `stream.mv_watermark` has no room to record a bootstrap.** Its PK is
`(mv, source_table_id, bucket)` with a single `next_offset` column (`0042_mv_watermark.sql`). A
bootstrapped row is **byte-identical** to a consumed one. "Recorded and observable" therefore means
a **migration**, not just new logic.

**6. Per-bucket `min(loom_offset)` is not obtainable from file stats.** Flush does not partition by
bucket (`iceberg_flush.rs:169` is its only bucket reference), so a flushed Parquet file can span
every bucket and its `loom_offset` min/max are **cross-bucket**. From the mirror you can get an
exact *cross-bucket* minimum over live files ∪ inline, and an exact *per-bucket* minimum only for
the **inline** tier (and for single-bucket files, where `loom_bucket` min == max). The fix must
therefore **round down, never up** — an overshoot silently skips live rows. Note the NULL fail-safe
**inverts** relative to GC: for a `min` guard, a missing stat means "I cannot prove where this file
starts" and must resolve **downward** (to 0). This collides directly with
`#iss-mv-floor-holds-pre-declaration-files`.

**7. This item shares a fix surface with `#iss-end-cap-ignores-mv-floor`.** That spec needs
`mv_floor` generalized from `&PgPool` to `&mut PgConnection` for a tx-atomic guard — which
simultaneously closes this item's floor-read race. **Sequence them together.**

## Design

**Recommended: bootstrap-and-record**, default = **earliest surviving** (rounded down), with the
start recorded and logged; reject only under an explicit per-def opt-in.

Why bootstrap over reject:

- **Reject is a time bomb.** "Refuse if the source has been truncated past 0" means a stream table
  that has ever been GC'd can **never gain a new MV again** — and under any real retention policy
  that is every stream table, eventually. It also fails a natural operator flow (add a second MV to
  a live stream) with no remedy short of recreating the source.
- **Bootstrapping is not newly lossy.** The rows the first run reads are **identical** to today
  (`loom_offset >= N` and `>= 0` select the same *live* set, since nothing below N survives). It
  converts an existing, currently-invisible gap into an **explicit, recorded** one.
- It composes with the CAS unchanged: a bootstrapped row at `next_offset = N > 0` is advanced by the
  `from > 0` UPDATE branch (which requires the row to exist at exactly N — it does); at `N = 0` by
  the `from == 0` branch. **No CAS change needed.**
- It fixes the over-hold (correction 1) for free.

At `define_transform`, for an MV body: resolve the source's surviving per-bucket
`min(loom_offset)` over **live-at-current-snapshot** data (inline exact; files rounded down to the
cross-bucket bound; missing stat ⇒ 0), then insert `stream.mv_watermark` rows at that offset and
log the start. A source that is **not yet declared / does not exist** at define time stays legal
(define-before-land is common) — there is simply nothing to bootstrap, and nothing can have been
lost.

### What a human must decide

1. **Default semantics: earliest-surviving vs latest.** "Latest" — bootstrap from
   `stream.bucket_offset.next` (the allocator high-water mark) — is **exact, per-bucket, already
   stored, and needs no scan at all**. It is Kafka's `auto.offset.reset=latest`, and for a *new* MV
   over a long-running stream it is arguably the semantics people actually want. Earliest-surviving
   preserves today's read set exactly (so no existing test's data changes) but is only coarsely
   computable. **This is a product decision, not a correctness one — and it is the single most
   important call in the item.** Note a `latest` default would break the existing
   `registered_but_unrun_mv_floors_at_zero` test (`mv_floor.rs:242`); an `earliest` default keeps it
   green.
2. Whether `TransformDef`'s MV bodies may grow a `start_at: Earliest | Latest | RequireComplete`
   field. Backward-compatible via `#[serde(default)]` (bodies are JSONB), and it makes "reject" an
   opt-in rather than a policy.
3. Whether a bootstrapped start must be **durably distinguishable** from a consumed offset (a
   migration on `stream.mv_watermark` — e.g. a `start_offset` column) or whether a log line +
   an admin-readable field on `GET /admin/transforms/{name}` suffices. The auditable option is the
   one a governed platform should want.
4. **Precision:** accept the coarse cross-bucket file bound, or require per-bucket exactness (which
   needs single-bucket-file stats, a Parquet read, or a maintained per-bucket low-watermark table —
   the natural sibling of `#iss-end-cap-ignores-mv-floor`).

## Testing

All in `//src/control-plane/postgres:mv-floor` (`loom_fixture_test`, `BUCK:581-600`) — no new
fixture machinery. `seed_source` (`tests/mv_floor.rs:139-186`) already registers MVs **after**
landing, so "register against an already-reclaimed source" just needs a GC between `land` and
`register_mv`. Helpers in place: `register_mv` (`:96-111`), `advance` (`:115-119`),
`end_cap_data_files` (`:362-372`), `age_all_snapshots` (`:348-355`), `current_snapshot_id`
(`:398-405`), `gc_table`.

- **Register against a reclaimed source** (the acceptance test) — seed with **no** MV (so the floor
  is `None` and GC is unguarded); end-cap + age + `gc_table` so a prefix is physically gone; land
  more rows so a surviving range exists; then `register_mv`. Assert the watermark rows are
  bootstrapped to the surviving min **and** that `mv_floor(...).per_bucket` is that min — i.e. the
  new MV **no longer resets the source's GC floor to 0** (correction 1).
- **Control: register against a never-GC'd source** — floor 0,
  `registered_but_unrun_mv_floors_at_zero` (`:242`) stays green. (Verify when choosing the default —
  a `latest` default would break it.)
- **First delta is not short** — a worker e2e (`stream_mv_e2e.rs`, which already has the GC
  machinery): GC first, then register, then run; the delta covers exactly the surviving range.
- **The lock race** — hold `pg_advisory_xact_lock(lock_key(source))` in a hand-rolled tx and wrap
  `define_transform` in a `tokio::time::timeout`, asserting it blocks. (The fixture tests already
  use raw `sqlx::query(AssertSqlSafe(..))` for exactly this kind of setup.)
- **Define-before-land still legal** — register against a source that does not exist yet: no error,
  no watermark rows.
- **No deadlock** — a concurrent `define_transform` + MV commit (which takes `lock_table` then
  row-locks the def) must not deadlock; pin the lock order.

## Non-regression

- Sources that have never been reclaimed bootstrap to 0 ⇒ existing floor tests unchanged.
- The watermark CAS is untouched (both branches keep working against a bootstrapped row).
- Memory backend has no advisory locks (documented mutex lock order), so the **lock half is
  postgres-only** and cannot be a testkit contract; the **bootstrap half** should be.
- New SQL ⇒ `tools/sqlx-prepare.sh` + commit `.sqlx`.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `#[expect(lint, reason)]` locally.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- Tests are `rust_test`/`loom_fixture_test` targets only.

## Acceptance

1. Registering an MV against a source whose prefix is gone **bootstraps** its watermarks to the
   surviving range (rounded down) rather than silently under-reading from 0 — and the start is
   recorded/observable.
2. Registering an MV no longer drops its source's GC floor to 0 (the over-hold is gone).
3. `define_transform` takes `lock_key(source)` at the top of its transaction, so a registration
   cannot straddle GC's floor-read/reclaim window — with no lock inversion against the commit path.
4. Define-before-land remains legal.
5. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `define_transform` (`postgres/src/transforms.rs:337-487`, upsert at `:469-484`;
  `TRANSFORM_DEFINE_LOCK` at `:17-19,347`); `TransformBody::MicroBatch/MicroBatchJoin`
  (`core/src/transforms.rs:37-83`); `pg_micro_batch_readers` (`transforms.rs:81-112`); `mv_floor` /
  `MvFloor::min_offset` (`mv_floor.rs:59-110`); `pg_advance_mv_watermark` (`stream.rs:596-628`);
  `stream.mv_watermark` (`0042_mv_watermark.sql`); `stream.bucket_offset` (`0034_stream.sql`);
  `files_with_stats` (`iceberg_catalog.rs:75-135`) + `data_file_column_stat`
  (`0016_iceberg_mirror_file_column_stats.sql`); `inline_live_batch_full`
  (`iceberg_inline.rs:1616`); `gc_table` / `lock_key` (`iceberg_gc.rs:99-119`,
  `iceberg_flush.rs:395-402`).
- Produces: the surviving-offset-range query (live-at-current-snapshot, rounded down); the
  bootstrap write at registration + its recorded start; `lock_key(source)` on `define_transform`.
