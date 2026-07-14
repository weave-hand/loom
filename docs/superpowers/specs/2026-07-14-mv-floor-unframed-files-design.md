# Unframed files held forever by the MV floor Design

> **Status:** design (direction). This spec makes `iss-mv-floor-holds-pre-declaration-files`
> build-ready. The item stays in ISSUES (a defect in shipped code); a separate work agent writes
> the implementation plan from it and builds it.

## Problem

The MV floor's file guard is deliberately fail-safe: a file whose `loom_offset` **max** stat is
missing is **held**, never reclaimed. In `victim_data_files` (`postgres/src/iceberg_gc.rs:379-389`)
the scalar subquery yields NULL, the comparison is NULL, and the row is not selected:

```sql
and ($3::bigint is null or (
      select cs.max_value from iceberg_mirror.data_file_column_stat cs
      where cs.data_file_id = df.data_file_id
        and cs.column_name = 'loom_offset')::bigint < $3::bigint)
```

So once any MV registers against a table carrying **unframed** files, those files become permanently
unreclaimable and inflate `GcSummary.held_by_mv_floor` with no laggard MV to blame (the warning names
a bucket's slowest MV; these files belong to no bucket). Bounded, visible, never lossy — but a
genuine "GC never converges for this table" case and a misleading metric.

## The register entry's premise and fix shape are both wrong — read this before planning

### 1. The premise is only reachable through a raw trait call

The entry says `declare_stream` "can be applied to a table that already has data files". Every
**supported** path already refuses exactly that. `reconcile_stream_mode` — the one seam every
production declarer passes through — rejects it (`postgres/src/stream.rs:197-203`):

```rust
(Some(n), None) => {
    if pre_existing {
        return Err(ControlPlaneError::Validation(format!(
            "cannot convert existing batch table {}.{} to a stream table", …)));
    }
```

A table with data files necessarily has a live mirror row, so `pre_existing` is true. This is
**intended and documented** (`docs/system-capabilities/stream.md:38-41` — "converting a pre-existing
batch table to `stream`/`cdc` is a `Validation` error (no retroactive conversion)") and tested e2e
(`ingest/tests/stream_declare.rs:154-176`, `model_cdc_declare.rs:99`).

The hole is that the guard lives in `reconcile_stream_mode`, **not in `pg_declare_stream` /
`pg_declare_cdc`**, which are bare `insert … on conflict do nothing` with zero checks
(`stream.rs:332-347`, `:401-420`) and are exposed publicly as `StreamTables::declare_stream` /
`declare_cdc`. **No production caller of those trait methods exists** — a tree-wide grep finds only
the core trait decl, the memory impl, the testkit contract, and ~20 test call sites.

So the entry's **option (c) ("deny declaration over a table with existing unframed files") is not
merely acceptable — it is already the policy at every user-facing seam.** The work is to make it
structural.

### 2. Option (a) — the declaring-snapshot exemption — is INERT

Through `reconcile_stream_mode`, a declaration happens only when `!pre_existing`, i.e. the declaring
write is the table's **genesis**. Therefore `declared_at_snapshot == min(begin_snapshot)` over all of
the table's files, and **`begin_snapshot < declared_at_snapshot` selects the empty set on every table
a supported workflow can produce.** It fixes nothing there — and it costs a migration, a widened
public trait (`declare_stream` has no snapshot param), a `.sqlx` refresh, and a testkit-contract
change.

### 3. …and on the tables where it *would* fire, it still doesn't converge

If you *do* raw-declare over a table that already has an Iceberg table, the table never becomes
properly framed — so **post**-declaration files are unframed too:

- Framing columns enter the Iceberg schema **only at table creation** — `ensure_iceberg_table`
  (`iceberg_landing.rs:628-651`); its own doc says "Only matters on table CREATION — a pre-existing
  table's schema is untouched".
- The direct-write stream path then allocates offsets, stamps framing… and **silently drops it**:
  `coerce_batch_to_ice` iterates the *pre-existing, framing-free* `ice_arrow.fields()` and takes
  `batch.column(i)` **positionally** (`iceberg_landing.rs:363-407`, called at `:1041-1042`), so the
  three appended framing arrays fall off the end. The commit projects user columns only →
  `SchemaPlan::Identical` → no error.

Live proof in the suite: `postgres/tests/compact_trigger.rs:334-343` lands a Parquet file, then
`cp.declare_stream(tid, 4)`, then lands **two more** — all `.expect("land")` succeed today. Offsets
are burned in `stream.bucket_offset` but the events carry none, so `mv_delta_scan` would never see
those rows: **a latent correctness bug in its own right.**

(Asymmetry worth knowing: the **inline** path *does* hard-fail on the same table —
`classify_schema_change` returns `NonNullableColumnAdded { loom_change_kind }` → `Validation`. Parquet
path: silent. Inline path: error.)

### 4. The inline tier has the identical hole, and the entry misses it

`delete_end_capped_inline_rows` (`iceberg_gc.rs:463-480`) builds its guard from per-bucket clauses; a
row with NULL `loom_bucket`/`loom_offset` matches **no** clause → **held forever**, and it is counted
by `count_candidates` → inflates `held_by_mv_floor` too. Pre-declaration inline rows keep NULL framing:
`ensure_inline_schema` adds the columns with `add column if not exists` and **no backfill**
(`iceberg_inline.rs:386-394`).

The doc comment directly above that code asserts the opposite — "an unframed row (NULL bucket/offset —
**impossible on a stream table**) is held" (`iceberg_gc.rs:445-446`) — which contradicts
`iceberg_gc.rs:360-365` ("`declare_stream` **may** be applied to a table that ALREADY has data files").
**Any fix must cover both tiers; the entry's fix shapes address only files.**

### 5. `held_by_mv_floor` never reaches the operator

`GcTableResponse` carries three fields (`engine-wire/proto/engine_control.proto:69`); the count's only
surface is an in-process `tracing::warn`. The "misleading metric" is visible in engine logs alone.

## Design

**Make the existing policy structural: refuse the declaration at the primitive, not at one caller.**

1. Move the "no retroactive conversion" guard **into `pg_declare_stream` / `pg_declare_cdc`** (or into
   a shared helper both call): refuse when the target table already has any data file **or any inline
   row**. Then the fail-safe hold becomes genuinely unreachable, `reconcile_stream_mode`'s
   `pre_existing` check becomes a redundant early-out (keep it — it gives the better error message and
   fires before any writes), and the raw `StreamTables` trait stops being a back door.
2. **Correct the two contradicting doc comments** (`iceberg_gc.rs:360-365` and `:445-446`) — they are
   the source of this issue's mistaken premise. State plainly that unframed files/rows are unreachable
   on a declared stream table, and that the NULL-stat hold is a fail-safe for corrupt/absent stats
   only.
3. Keep the fail-safe hold itself **unchanged**. It is correct as defense against genuinely missing
   stats (parquet stats disabled, an all-NULL `loom_offset` column). It just stops being routinely
   reachable.

This needs **no migration, no `.sqlx` schema change beyond the new guard query, no trait widening** —
versus option (a)'s migration + trait change + testkit change for a predicate that selects nothing.

### What a human must decide

- **Is a batch→stream conversion ever wanted?** Today it is refused and documented as such. If it is
  ever to be supported, the honest shape is a **backfill** (rewrite existing files with framing
  stamped, allocating offsets), not an exemption — and that is a separate, much larger item. This spec
  assumes "no", matching the shipped policy.
- **The ~20 test call sites that raw-declare over landed tables** (notably
  `compact_trigger.rs:334-343`, which lands *then* declares) will start failing. They must be reseeded
  to declare at genesis. That is the bulk of the diff, and it is a **feature**: those tests were
  encoding the broken state.

## Testing

`//src/control-plane/postgres:mv-floor` (`loom_fixture_test`, `BUCK:581-600`). `seed_source`
(`tests/mv_floor.rs:139-186`) already builds the topology; `buckets: Option<i32>` is what declares the
stream table.

- **Declare-over-files is refused** (the acceptance test) — land a batch table with files, then
  `cp.declare_stream(tid, 4)` → `Err(Validation)`. Mirrors the shape at `compact_trigger.rs:334-343`,
  which currently *succeeds*.
- **Declare-over-inline-rows is refused** — same, seeded inline (the tier the entry misses).
- **Declare at genesis still works** — every existing stream test stays green (this is the regression
  bar).
- **The fail-safe still holds a statless file** — synthesize a file whose `loom_offset` stat row is
  absent/NULL and assert GC holds it. This pins that we removed the *reachability*, not the safety net.
- **GC converges** for a stream table declared at genesis with an MV: nothing is held that the floor
  does not justify.
- Reseed the raw-declare test call sites; expect churn across `compact_trigger.rs`, the testkit
  contract (`testkit/src/lib.rs:6145-6193`), and the memory fake.

## Non-regression

- Tables declared at genesis (every supported workflow) are unaffected.
- The NULL-stat fail-safe is unchanged.
- `reconcile_stream_mode`'s existing `Validation` message and its e2e tests
  (`ingest/tests/stream_declare.rs:154-176`) stay green.

## Out of scope (record as new issues in the closing PR)

- **Framing is silently dropped on a raw-declared pre-existing table** (`coerce_batch_to_ice`'s
  positional take against a framing-free Iceberg schema, `iceberg_landing.rs:363-407`). Once
  declaration-over-files is refused this becomes unreachable *through that door* — but the positional
  coercion is fragile on its own and deserves an item.
- **`held_by_mv_floor` is not surfaced over the engine RPC** (`GcTableResponse` has three fields), so
  the operator's only signal is a log line. File it.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `#[expect(lint, reason)]` locally.
- `tools/sqlx-prepare.sh` + commit `.sqlx` after SQL changes.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- Tests are `rust_test`/`loom_fixture_test` targets only; the `StreamTables` contract runs on **both**
  backends, so the memory fake must refuse identically.

## Acceptance

1. `declare_stream` / `declare_cdc` refuse a table that already has data files **or** inline rows — at
   the primitive, so the raw trait is no longer a back door.
2. The unframed-file and unframed-inline-row hold-forever cases are **unreachable**, and the NULL-stat
   fail-safe still works.
3. The two contradicting doc comments are corrected.
4. Existing suites green (`buck2 test //src/...`), with the raw-declare test call sites reseeded.

## Interfaces (names the plan consumes)

- Consumes: `pg_declare_stream` / `pg_declare_cdc` (`postgres/src/stream.rs:332-420`);
  `reconcile_stream_mode`'s `pre_existing` guard (`stream.rs:197-203`); `StreamTables`
  (`core/src/stream.rs:126-140`) + the memory impl + the testkit contract
  (`testkit/src/lib.rs:6145-6193`); `victim_data_files` (`iceberg_gc.rs:373-400`) and
  `delete_end_capped_inline_rows` (`iceberg_gc.rs:452-490`); `MvFloor` (`mv_floor.rs`);
  `ensure_iceberg_table` / `framing_column_specs` (`iceberg_landing.rs:628-676`);
  `ensure_inline_schema` (`iceberg_inline.rs:386-394`).
- Produces: the declaration-time refusal at the primitive; corrected GC doc comments; reseeded
  raw-declare tests.
