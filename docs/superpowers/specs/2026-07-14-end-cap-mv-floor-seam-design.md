# End-cap seam — the MV floor at the point of harm Design

> **Status:** design (direction). This spec makes `iss-end-cap-ignores-mv-floor` build-ready.
> The item stays in ISSUES (a defect in shipped code); a separate work agent writes the
> implementation plan from it and builds it.

## Problem

`#road-mv-watermark-aware-gc` (shipped, #430) put the per-bucket MV read-position floor
(`mv_floor`, `postgres/src/mv_floor.rs`) on `gc_locked`. That tier **cannot deliver
hole-freedom**, and the register entry is right about why: GC only ever reclaims **end-capped**
rows/files (`end_snapshot <= H`), while a micro-batch MV's delta reads **live** rows at the
**current** snapshot (`files_with_stats(t, current)` ∪ `inline_live_batch_full(t, current)`).
An end-capped row is therefore *already invisible* to the MV before GC touches it. The floor on
`gc_locked` buys byte-retention defense (`GcSummary.held_by_mv_floor`) plus a reusable
primitive — not hole-freedom.

**The harm is created at end-cap time.** No downstream GC-tier guard can bring back a row that
has left the current snapshot.

## The exposure is NOT latent — two lossy paths are unguarded today

The register entry says today's end-cappers are all benign and "the lossy surfaces are coming".
**That is false**, and the plan must start from the corrected picture.

### The gate that defines "harmful"

`mv_delta_scan` accepts **log** stream sources only (`engine-serving/src/mv_delta.rs:72-82` —
"cdc sources are deferred"). So *harmful* = "can remove offsets from a declared **log** stream
table that an MV sources".

### The eight end-cap paths (five primitives, two files)

Every production write of `end_snapshot` goes through exactly five functions:
`end_cap_files_by_path` / `end_cap_live_data_files` / `mark_dropped`
(`postgres/src/iceberg_mirror.rs:335-419`) and `end_cap_live_inline_rows` /
`end_cap_inline_rows_by_id` (`postgres/src/iceberg_inline.rs:76-124`).

| Path | Tier | Verdict |
| --- | --- | --- |
| **E1** catalog drop (`mark_dropped`) | files, table/column | Lossy **by design** — bypasses the floor on purpose so drop-GC converges; warns, naming stranded MVs. |
| **E2** `compact_table` | files (subset) | **Benign, and re-projecting** — the worker refetches every row and rewrites it coalesced, so framing rides through. Both enqueue producers skip stream tables — but the guard is on the *producers*, not on `compact_table` itself. Convention, not structure. |
| **E3** **flush** | inline (targeted) | **Benign, and re-projecting** — and **missing from the register entry entirely**. See below: it is the constraint that makes the naive seam wrong. |
| **E4** transform overwrite | files + inline | Benign — structurally refused by `pg_refuse_stream_target` (`iceberg_control_plane.rs:147`) + `define_transform` (`transforms.rs:423`). |
| **E5** multi-step `write_steps` | files + inline | Benign — same structural refusal (`iceberg_landing.rs:592`). |
| **E6** **`overwrite_table` / `OverwriteTable` RPC** | files + **all** inline | **LOSSY. UNGUARDED.** |
| **E7** **`consolidate_table` — COW arm** | files + folded inline | **LOSSY. UNGUARDED.** |
| **E8** `consolidate_table` — CDC arm | files + folded inline | Lossy, but **currently gated** by `mv_delta_scan`'s log-only filter. Already built — not a "coming" surface. |

### E6 — `overwrite_table` is live and unguarded

`overwrite_with_cap` (`iceberg_landing.rs:1201-1247`) **explicitly supports** declared stream
tables (it resolves `include_framing` from `pg_stream_bucket_count`), yet carries **no**
`pg_refuse_stream_target` and no floor consult. Worse, its empty-body branch —
`overwrite_truncate` (`:1260-1279`) — end-caps every live file **and** every live inline row
with **no schema or stream check at all**. A delete-all on an MV-sourced log stream table
silently destroys the entire offset range. The RPC is live (`engine/src/service.rs:827-851`).

### E7 — the COW consolidate fold reaches log stream tables through a fallthrough

The arm dispatch (`consolidate.rs:160`) sends **only** `kind == Cdc` to the CDC arm — so
`kind == Log` falls through to the **COW** arm whenever `identity_for_table` is `Some` and
`has_shadow` is set. All three preconditions are reachable, each unguarded:

1. An ontology-bound, identity-bearing **non-CDC** table is a first-class served configuration —
   `build_serving_provider` (`serving.rs:134-151`) resolves identity and *then* filters
   `stream_meta` to `Cdc`, i.e. it explicitly handles identity-bearing non-CDC tables.
2. `bind` (`ingest/src/bind.rs`) has **no stream check** — nothing forbids binding a type to a
   declared log stream table.
3. A typed UPDATE/DELETE reaches `write_inline_delta` (`iceberg_inline.rs:1217+`), which has **no
   stream guard**: a log table takes the non-CDC branch (an unframed `+U`/`-D` inline row, NULL
   `loom_bucket`/`loom_offset`), then unconditionally `set_has_shadow` (`:1421`) and bumps the
   consolidate trigger (`:1407-1419`). query-api's `action.rs` is entirely stream-unaware.

`consolidate_cow_locked` then end-caps **every live file** plus the folded inline rows and
re-projects only the identity-fold winners, user columns only — offsets an MV has not read are
destroyed.

**So the exposure is gated only by the unenforced assumption that a declared log stream table is
never ontology-bound.** Nothing enforces it and `serving.rs` actively supports the combination.

## The insight that makes or breaks the seam

A blanket *"refuse any end-cap at or above the floor"* **breaks flush (E3)** and would wrongly
refuse compaction (E2). Both end-cap offsets above the floor **and re-project those same rows at
the same `(bucket, offset)`** — flush moves inline rows into live Parquet; compaction rewrites
small files coalesced. The rows never leave the live set, so no MV can miss them.

The question is therefore not *"may I end-cap this row?"* but:

> **"May I remove these offsets from the live set?"**

Intent must come from the **call site**; it cannot be inferred from the SQL. Any seam that tries
to infer it will either break flush or fail to catch E6/E7.

## Design

Make the intent a **required argument** of the five primitives, so the type system forces every
future retention path to decide. This is the structural option the register entry asks for; a
caller-consulted `mv_floor()` query with no signature change is exactly the by-convention scheme
that let E6 and E7 exist unnoticed.

```rust
// mv_floor.rs
pub enum EndCapIntent<'a> {
    /// Offsets survive: the same rows are re-projected into the new live set at the SAME
    /// (bucket, offset). Flush, plain-coalesce compaction. No floor consult.
    Reframing,
    /// Offsets leave the live set. Must clear the MV floor.
    Removing,
    /// Deliberate destruction; floor bypassed on purpose, reason recorded + logged.
    Destroying { reason: &'a str },
}

/// Refuse a `Removing` end-cap that would take offsets at/above any MV's read position.
/// Runs on the CALLER'S transaction, so the refusal is atomic with the write and cannot
/// straddle a concurrent `define_transform`.
pub async fn guard_end_cap(
    conn: &mut PgConnection,
    table: &TableRef,
    tid: i64,
    intent: &EndCapIntent<'_>,
) -> Result<()>;
```

**Precedent to copy:** `pg_refuse_stream_target` (`stream.rs:373-398`) — resolve `tid`, one
`exists` query, `ControlPlaneError::Validation`, called **inside the caller's commit tx** so the
refusal is atomic with the write. Already adopted by three write paths.

**Required refactor (and it pays for itself):** `mv_floor` today takes **`&PgPool`**, which makes
a tx-atomic guard impossible. Generalize it to `&mut PgConnection` — cheap, because all three of
its dependencies (`pg_stream_bucket_count`, `pg_micro_batch_readers`, `pg_mv_watermarks`) are
already `E: sqlx::PgExecutor`-generic; only `mv_floor`'s own `fetch_all(pool)` is pool-bound.
`gc_locked` then reads the floor **inside** its transaction, which **also closes the floor-read
race in `#iss-mv-register-below-reclaimed-floor` for free**. The two items share a fix surface and
should be sequenced together.

**Bounds — reuse GC's, do not invent a second pair:**

- **Files:** `MvFloor::min_offset()` vs the file's `loom_offset` **max** stat; a NULL stat is
  **held/refused** (fail-safe). A file straddling the floor cannot be partially end-capped without
  a rewrite ⇒ **refuse**, don't filter.
- **Inline rows:** per-bucket precise (`loom_bucket = b and loom_offset < floor_b`), exactly as
  `delete_end_capped_inline_rows` already does.

**Adoption per path:** E1 → `Destroying { reason: "catalog drop" }` (keep the stranded-MV warning);
E2/E3 → `Reframing` (no consult); E6 → `Removing` ⇒ refuse (`Validation` → 4xx); E7/E8 →
`Removing` ⇒ see the open question below.

### Genuinely open questions — a human must decide these before building

1. **Is "overwrite a declared stream table" ever legitimate?** The code deliberately supports it
   (`include_framing`; `tests/stream_overwrite_framing.rs` asserts it), but the *only* caller that
   supplies framed batches is the CDC consolidate fold. If the answer is "only consolidate", the
   cheaper and stronger fix is to extend `pg_refuse_stream_target` to `overwrite_parquet_snapshot`
   and give consolidate a private framed entrypoint. **This materially changes the item's shape.**
2. **What does a *correct* COW fold over a log stream table even mean?** Folding by identity is
   fundamentally incompatible with an offset-framed log an MV replays. The honest answer may be to
   **refuse the ontology-bound-identity + log-stream combination at bind time** — a different fix,
   in a different file, that *dissolves* E7 rather than guarding it.
3. **Refuse vs defer for the consolidate job.** A raw refusal inside `consolidate_table` **poisons
   the queue** (the `stream_consolidate` job retries forever). Needs `JobFailure::retry` with
   backoff, or "skip, clear the trigger, re-arm later". This is the hardest call in the item.
4. **Should `compact_table` carry the guard itself** (structure) or keep relying on its enqueue
   producers (convention)? Compaction is lossless, so this is future-proofing, not a live bug.

## Testing

The seam is testable **end-to-end today with no synthetic lossy helper**, precisely because E6/E7
are real:

- **The trip test (real path, real refusal).** Seed a declared **log** stream table (buckets=1, N
  events) via `tests/mv_floor.rs::seed_source`; register an MV (`register_mv`, `on_input_commit:
  false`); advance its watermark to 3 of 6 (`advance`); then call `overwrite_parquet_snapshot(…,
  batches = vec![])` — the truncate branch, the cleanest trip. **Today:** every file and inline row
  is end-capped and offsets 3..6 are gone. **After:** `Err(Validation)`, live set untouched.
- **The over-refusal test — equally important.** Same seed, then `flush_table` with the MV still at
  3 of 6 → **must still succeed** (`Reframing`), and `lagging_mv_holds_the_unread_tail` stays green.
  This is what proves the seam distinguishes reframing from removal, and it is the test that catches
  the naive implementation.
- **Bypass unchanged:** `dropped_source_bypasses_the_floor` (`tests/mv_floor.rs:684-733`) stays green.
- **E7 trip:** bind an ontology type with an identity to a declared log stream table, issue a typed
  UPDATE, let the consolidate trigger fire → refused (or, per open question 2, refused at bind).
- **Deletion of the synthetic helper.** `tests/mv_floor.rs:362-372` has a raw-SQL `end_cap_data_files`
  precisely because no *guarded* API existed. The seam is the opportunity to delete it and drive GC's
  file-tier tests through the real primitive.

All in `//src/control-plane/postgres:mv-floor` (`loom_fixture_test`, `BUCK:581-598`); `seed_source`
already builds exactly the topology needed.

## Non-regression

- Tables no MV sources: `mv_floor` fast-paths to `None` ⇒ every intent is a no-op ⇒ byte-identical.
- Flush and compaction keep end-capping above the floor (`Reframing`).
- Drop keeps converging (`Destroying`).
- New/changed SQL ⇒ `tools/sqlx-prepare.sh` + commit `.sqlx`.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `#[expect(lint, reason)]` locally.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- Tests are `rust_test`/`loom_fixture_test` targets only.
- GC's commit-then-delete ordering and advisory-lock discipline are unchanged — the seam guards
  *end-cap*, not reclaim.

## Corrections to carry back into `docs/ISSUES.md` on close

1. The "COW/shadow fold" is **not** part of `iceberg_compact::compact_table` — it is
   `consolidate_table`'s COW arm (`consolidate.rs:356-510`), a separate unguarded path.
2. **"All current paths are benign" is false.** E6 and E7 are unguarded lossy end-caps over declared
   log stream tables — the exact MV source class.
3. The CDC consolidate fold (E8) is **already** a built lossy end-cap, held back only by
   `mv_delta_scan`'s log-only filter.
4. **Flush is missing from the entry**, and it is the constraint that makes a naive seam wrong.
5. `mv_floor` takes a `&PgPool`, which prevents a tx-atomic guard; generalizing it to
   `&mut PgConnection` is a prerequisite here **and** closes `#iss-mv-register-below-reclaimed-floor`'s
   race. Sequence the two together.

## Acceptance

1. A `Removing` end-cap that would take offsets at/above an MV's read position is **refused**, atomically
   with the caller's write — proven on the real `overwrite_table` path, not a synthetic one.
2. Flush and compaction of unconsumed offsets **still succeed** (`Reframing`) — the over-refusal test.
3. The drop bypass still converges and still names stranded MVs.
4. Every one of the five end-cap primitives requires an explicit intent (structural, not by convention).
5. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: the five end-cap primitives (`iceberg_mirror.rs:335-419`, `iceberg_inline.rs:76-124`);
  `mv_floor` / `MvFloor::min_offset` / `stranded_mv_readers` (`mv_floor.rs`); `gc_locked` /
  `victim_data_files` / `delete_end_capped_inline_rows` (`iceberg_gc.rs:121-490`);
  `pg_refuse_stream_target` (`stream.rs:373-398`) as the refusal precedent; `overwrite_with_cap` /
  `overwrite_truncate` (`iceberg_landing.rs:1201-1279`); `consolidate_table`'s arm dispatch
  (`consolidate.rs:160`); `mv_delta_scan`'s log-only filter (`mv_delta.rs:72-82`).
- Produces: `EndCapIntent` + `guard_end_cap`; `mv_floor` on `&mut PgConnection`; intent-carrying
  signatures on all five primitives; the trip/over-refusal test pair.
