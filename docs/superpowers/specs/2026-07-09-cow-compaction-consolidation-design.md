# Scalable COW — slice 2: tombstone-aware compaction consolidation Design

> **Status:** design (direction). This spec makes `road-cow-compaction-consolidation`
> build-ready. A separate work agent writes the implementation plan from it and
> builds it. Slice 1 (`road-cow-inline-shadow`, #331) has landed; every file:line
> reference below is against the current tree.

## Goal

A background **consolidation job** for a mutated (shadow-bearing) non-CDC
identity table: fold the accumulated inline tier — plain appends, row-versions,
tombstones — together with the base Parquet into a **new base snapshot** that is
the merge-on-read view materialized (latest-per-identity wins, a tombstoned
winner drops the identity), retire exactly the inline rows it consumed at that
same snapshot, and **re-enable the byte-trigger flush** slice 1 suppressed. This
resolves the **unbounded-inline-growth** gap slice 1 deliberately left: today a
table that has taken one UPDATE/DELETE accumulates inline rows forever
(`has_shadow` suppresses its flush), and every read pays the merge over an
ever-growing Postgres inline tier.

**Design principle — the fold is the read, materialized.** Slice 1 defined
current-state as `build_merge_view` under `Precedence::Snapshot`
(`engine-serving/src/serving.rs:256`): file tier ranks `<0, false>`, inline tier
ranks `<begin_snapshot, loom_tombstone>`, rank-1 per identity wins, a tombstoned
winner is hidden. Consolidation writes exactly that set as the new base, so
reads before and after are identical by construction. This mirrors how the CDC
`consolidate_stream` fold is the `Precedence::Offset` merge materialized
(`engine-serving/src/consolidate.rs:240-247`, unified by
`road-stream-merge-engines`).

## Context — what ships today (slice 1 + the stream engine)

- **The shadow model.** A governed UPDATE/DELETE on an identity-bearing non-CDC
  type writes ONE inline delta row via `write_inline_delta`
  (`iceberg_inline.rs:1036`): a version row (`loom_tombstone=false,
  loom_change_kind='+U'`, full post-PATCH image) or a tombstone
  (`loom_tombstone=true, loom_change_kind='-D'`, data columns NULL), under a
  per-identity advisory-lock CAS on `max(begin_snapshot)`
  (`iceberg_inline.rs:1074-1088`). Inline delta rows are never end-capped by the
  write path; the commit sets the table's `has_shadow` flag
  (`iceberg_mirror.shadow_flag`, `set_has_shadow`, `iceberg_inline.rs:1232`).
- **Merge-on-read.** `build_serving_provider` (`serving.rs:74`) routes an
  identity-bearing non-CDC table with live inline rows through `build_merge_view`
  (`serving.rs:325`) with `Precedence::Snapshot`; `build_inline_provider`
  (`serving.rs:541`) exposes `begin_snapshot` (i64 non-null) and `loom_tombstone`
  (bool non-null) alongside the user columns for exactly this fold. With **no**
  live inline rows a non-CDC identity table serves the plain file provider
  (`serving.rs:209`) — which is why the consolidated base must itself be
  identity-deduped and tombstone-free.
- **The suppressed flush** is two seams, both keyed on `has_shadow`:
  1. the **enqueue gate** — `inline_append_decl`'s byte-trigger block skips
     enqueueing `flush_table` (and leaves the trigger disarmed) for a shadowed
     non-CDC table (`iceberg_inline.rs:704-735`; the CDC exemption at `:719-724`
     landed with the dual-write flush);
  2. the **flush backstop** — `flush_locked` no-ops (and resets the trigger) when
     `has_shadow` is set, because a job can be enqueued *before* the mutation
     lands (`iceberg_flush.rs:98-110`).
  The reason: the flush appends its read rows to Parquet as ordinary rows
  (`append_parquet_snapshot` + `InlineEndCap`); draining a version/tombstone
  would create a **duplicate or resurrected file row** for that identity — for
  a non-CDC table the file tier ranks `<0, false>`, so a flushed shadow delta
  loses all its precedence and tombstones stop deleting.
- **The mechanical template already exists.** The CDC `consolidate_stream`
  (`engine-serving/src/consolidate.rs:57`) is the same operation class for CDC
  bases: engine-side fold under the per-table advisory lock (`lock_table`,
  `iceberg_flush.rs:414` — the same key flush/GC take), DataFusion
  `ROW_NUMBER()` fold, `overwrite_parquet_snapshot`, then `clear_has_shadow` +
  `clear_consolidate_trigger` (`consolidate.rs:260-268`). It is driven by the
  `stream_consolidate` job (`worker/src/consolidate.rs:15` →
  `ConsolidateStream` RPC → `engine/src/service.rs:260`), auto-enqueued by the
  `consolidate_trigger` delta-row counter in `write_inline_delta` — **gated
  `if cdc &&` today** (`iceberg_inline.rs:1218`), with the threshold already
  threaded from `LOOM_CONSOLIDATE_DELTA_THRESHOLD` (default 128,
  `engine/src/run.rs:41-43`) through
  `IcebergActionWriter::with_consolidate_delta_threshold`
  (`action_writer.rs:41`). For a **non-CDC** table `consolidate_stream` returns
  `Ok(0)` — a no-op. Slice 2 fills exactly that arm.
- **The commit primitives.** `overwrite_parquet_snapshot`
  (`iceberg_landing.rs:1117`) replaces the live file set in one Postgres tx
  (pointer CAS + `end_cap_live_data_files` + project new files + lineage +
  deduped `build_vector_index` jobs via `rebuild_jobs_for`,
  `vector_index.rs:154`) — and, in overwrite mode, **blanket end-caps every live
  inline row** (`commit_mirror.rs:182-185`). The flush path instead retires
  exactly the rows it read, via `CommitExtras.end_cap` /
  `end_cap_inline_rows_by_id` (`commit_mirror.rs:59`, `iceberg_inline.rs:106`).
  No current caller passes both.

## Design

### 1. Fold semantics (tombstone-aware, latest-per-identity)

Input: the base's live Parquet files (at the current snapshot) UNION the
table's **entire live inline tier** — appends, versions, tombstones. Reference
SQL (the `Precedence::Snapshot` mapping of `build_merge_view`, verbatim):

```sql
SELECT <user_cols> FROM (
  SELECT <user_cols>, _loom_tomb,
         ROW_NUMBER() OVER (PARTITION BY <identity> ORDER BY _loom_prec DESC) AS _rn
  FROM (
    SELECT <user_cols>, 0 AS _loom_prec, false AS _loom_tomb FROM base_files
    UNION ALL
    SELECT <user_cols>, begin_snapshot AS _loom_prec, loom_tombstone AS _loom_tomb
    FROM base_inline
  ) base_input
) t WHERE _rn = 1 AND _loom_tomb = false
```

Outcomes, per identity:

- **file row only** → passes through unchanged.
- **file row + newer version(s)** → the greatest-`begin_snapshot` version's full
  image replaces it (inline always outranks the file tier's literal `0`).
- **file row + tombstone winner** → the identity is **dropped** from the new
  base (tombstone-aware: the delete becomes physical; it cannot resurrect).
- **inline append (no file row)** → folds in as its identity's winner — so
  consolidation also **drains the plain appends** a shadowed table accumulated
  while its flush was suppressed. Consolidation *is* the flush path for a
  mutated table.
- **duplicate-identity file rows** (a base landed with dup identities) collapse
  to the same arbitrary rank-1 winner the merge view already served — reads are
  unchanged; the physical dup is gone.

The output projection is the plain mirror data schema (user columns only — a
non-CDC table has no framing in its physical Parquet;
`overwrite_parquet_snapshot`'s `include_framing` check via
`pg_stream_bucket_count` stays false, `iceberg_landing.rs:1133-1141`).
`loom_tombstone`/`begin_snapshot` are fold-internal and never land in Parquet.

An **all-tombstoned** fold (every identity deleted) produces zero rows and takes
the existing `overwrite_truncate` branch (`iceberg_landing.rs:1160`) — extended
to carry the targeted inline cap (below).

### 2. Atomicity — the consuming overwrite (no lock on the hot mutation path)

The commit is ONE Postgres transaction, via a new **consuming** variant of the
overwrite primitive: `overwrite_parquet_snapshot_consuming(pool, catalog, table,
columns, batches, lineage, InlineEndCap { table_id, row_ids })`. Same tx as
today's overwrite (pointer CAS, `end_cap_live_data_files`, project new files,
lineage, `rebuild_jobs_for` jobs, data triggers) with ONE semantic change,
implemented at `write_mirror` (`commit_mirror.rs:145`): **when a targeted
`end_cap` rides an overwrite commit, the blanket `end_cap_live_inline_rows` is
skipped** — the commit retires exactly the inline rows the fold consumed
(`end_cap_inline_rows_by_id`, already ordered before lineage in
`apply_commit_extras`, `commit_mirror.rs:80-84`) and leaves every other live
inline row alone. No existing caller passes both `overwrite: true` and
`end_cap: Some(..)`, so current behavior is untouched.

Why by-id and not blanket: **a mutation committing mid-consolidation must
survive.** `write_inline_delta` takes only its per-identity advisory lock — not
`lock_table` — so a delta can commit between the fold's read and the overwrite
commit. Under the blanket cap that row would be end-capped *without ever
entering the fold*: a silently lost update. Under the consuming cap it is not in
`row_ids`, stays live, and keeps shadowing the new base exactly as merge-on-read
requires (inline `begin_snapshot >= 1` always outranks the file tier's `0`).
The hot mutation path needs **no new locking**.

Read-set determinism: the fold reads files and inline rows at the table's
current snapshot `cur`; `begin_snapshot` allocation is monotonic
(`next_snapshot`), so a delta committing after `cur` was read is invisible to
*both* the file read and the MVCC-predicated inline read — the consumed set is
exact regardless of timing. Concurrent flush/GC end-caps are excluded by holding
`lock_table` for the whole read→fold→commit window (same contention behavior as
`consolidate_stream`, `consolidate.rs:107-118`).

CAS interaction (the slice-1 guard, `iceberg_inline.rs:1081-1088`): after the
commit caps an identity's folded rows, its live `max(begin_snapshot)` drops to
`0` — the same value a fresh reader of the consolidated (file-resident) object
computes. A mutation that read its expected version *before* consolidation and
CAS-checks *after* it gets a loud `Conflict` and retries against the folded
state; one that commits before the consolidation's commit simply stays live and
shadows the new base. No interleaving loses a write; no interleaving double
applies one.

**Drive-by fix (latent CDC race):** `consolidate_stream` currently relies on
the blanket cap — a CDC inline row committing mid-consolidation is end-capped
without being folded *or* changelog-flushed (lost from base AND changelog). It
already holds the consumed `row_ids` (`inline_live_batch_full`,
`consolidate.rs:181-184`, currently ignored); switching it to the same consuming
overwrite closes that hole for free. `stream_cdc_consolidate` stays green.

### 3. What "re-enable the suppressed flush" concretely means

The suppression code itself is **correct and stays** — it is self-narrowing
(keyed on `has_shadow`), and slice 1 wrote it to "re-fire once slice-2
consolidation clears the flag" (`iceberg_inline.rs:708-712`). Re-enabling is
three post-commit steps in the consolidation (mirroring `consolidate_locked`'s
clears and `flush_locked`'s self-heal narrative — each idempotent, so a crash
between commit and clears heals on the next run):

1. **Conditionally clear `has_shadow`** — a new
   `clear_has_shadow_if_quiescent(conn, tid) -> bool`: one statement,
   `DELETE FROM iceberg_mirror.shadow_flag WHERE table_id = $1 AND NOT EXISTS
   (SELECT 1 FROM inline_<tid> WHERE end_snapshot IS NULL AND (loom_tombstone OR
   loom_change_kind IN ('+U','-D')))`. Conditional because mid-consolidation
   deltas survive by design (§2) — clearing over a live delta would re-open the
   corruption slice 1 closed. If deltas remain, the flag stays and the *next*
   consolidation (re-armed trigger, below) drains them.
2. **Reset the byte trigger** (`reset_inline_trigger`) — the consolidation
   drained the inline bytes the counter accrued; without this the first
   post-consolidation append could enqueue one spurious flush (harmless — it
   self-heals on the no-live-rows branch — but noisy).
3. **Disarm the consolidate trigger** (`clear_consolidate_trigger`) so the next
   delta accrual can re-enqueue — exactly as the CDC arm does
   (`consolidate.rs:262-268`).

After a quiescent clear, the ordinary flush lifecycle resumes: appends accrue
bytes, the gate enqueues `flush_table`, the flush drains — a consolidated table
is indistinguishable from a never-mutated one.

**Hardening the backstop (closes the conditional-clear's write-skew).** The
single-statement clear in (1) has a benign-looking but real skew: a delta tx
whose `set_has_shadow` no-ops against the still-present flag row can commit
*after* the clear's NOT-EXISTS snapshot — leaving a live delta with no flag. The
fix is not at the flag but at the flush, where the corruption would actually
happen: `flush_locked`'s guard is extended to inspect **the read set it is about
to write** — after `inline_live_batch` returns `(tid, row_ids, batch)`, one
query checks for shadow rows among exactly `loom_row_id = ANY(row_ids)`
(`loom_tombstone OR loom_change_kind IN ('+U','-D')`); on a hit it no-ops,
**re-sets `has_shadow`** (self-heal), and resets the trigger. This is race-free
by construction — a flush can only corrupt by *appending rows it read*, and a
delta committing after the read is not in `row_ids` and not written. The flag
becomes a fast-path optimization; safety derives from the data. (The existing
flag check at `iceberg_flush.rs:98-110` stays as the cheap early-out.)

### 4. Trigger / enqueue — reuse the consolidate seam wholesale

**No new job kind, RPC, wire message, or operator endpoint.** The entire
pipeline exists and is table-shaped, not CDC-shaped: `consolidate_trigger`
accrual (`bump_consolidate_trigger`/`arm_consolidate_trigger`,
`iceberg_mirror.rs:606/:637`), the deduped `stream_consolidate` job, the worker
handler (`run_wire_job`), the `ConsolidateStream` RPC, and the engine-side
entry. Slice 2:

- **drops the `cdc &&` gate** on the trigger block in `write_inline_delta`
  (`iceberg_inline.rs:1218`), counting `1` accrued delta row per non-CDC
  version/tombstone (the non-CDC branches currently leave `emitted_rows = 0`).
  The threshold is the already-wired `consolidate_delta_threshold`
  (`LOOM_CONSOLIDATE_DELTA_THRESHOLD`, default 128) — one knob for both table
  kinds, debounced by `enqueued`, re-armed by step 3 of §3.
- makes the engine entry **dispatch**: `consolidate_table(cp, catalog, pool,
  table)` routes `kind='cdc'` to the existing Offset fold, a non-CDC
  identity-bearing table with `has_shadow` to the new Snapshot fold
  (`consolidate_cow_locked`), and everything else to the `Ok(0)` no-op —
  the same shape `build_serving_provider`'s precedence dispatch uses. The
  wire names (`stream_consolidate`, `ConsolidateStream`) are kept verbatim for
  job/proto stability; the doc comments say "consolidate this table's inline
  tier into its base", which is now literally true for both kinds.

An operator-facing HTTP trigger is deliberately out (none exists for the CDC
consolidate either; a job row can be inserted manually). Note the **separate**
`road-compaction-auto-trigger` (spec `2026-07-09-compaction-auto-trigger-design`)
is about auto-enqueueing the small-file `compact_table` job off file counts —
a different job on a different counter; the two share only the
"mirror-the-inline-flush-trigger" pattern and must not collide on trigger
tables (this slice touches only `consolidate_trigger`).

### 5. Time travel, lineage, and the mirror — honest claims

- Consolidation is **one new mirror snapshot** `S`. An as-of read at `q < S`
  sees the old files (end-capped at `S`, so `end > q` → visible) and the
  consumed inline rows (same) — history is byte-identical; the new base files
  begin at `S` and are invisible to `q < S`. Reads *spanning* the consolidation
  change representation, never results.
- Lineage: a `Complete` event with the table as both input and output, payload
  `{"source": "consolidate_cow"}` — mirroring `consolidate_event`
  (`consolidate.rs:41`) with a distinct source tag so provenance distinguishes
  the two folds.
- The **raw Iceberg metadata** still references the replaced files until GC —
  the accepted `iss-iceberg-inline-visibility` gap class every overwrite carries
  (`iceberg_landing.rs:1104-1106`); consolidation adds no new claim.
- End-capped inline rows are reclaimed by the existing `gc_table`
  (`delete_end_capped_inline_rows`, `iceberg_gc.rs:137`) once past retention —
  the growth resolution is end-to-end: fold → end-cap → GC.
- **Vector indexes:** the overwrite commit enqueues one deduped
  `build_vector_index` per declared index (`rebuild_jobs_for`), so the stale
  cold-index entries #400 could only suppress at query time are now durably
  removed — the "durable cold-entry removal stays with slice-2 compaction" note
  in `docs/system-capabilities/query-api.md` closes with this slice.
- **Known wart, carried:** `overwrite_parquet_snapshot` fires data triggers
  (`data_trigger_tables = [table]`) although consolidation is data-preserving;
  the CDC consolidate already inherits this. Kept identical for both folds (a
  joint follow-up may suppress triggers on consolidate commits); a transform
  self-triggered by its own output's consolidation is already prevented by the
  at-most-one-pending queue dedup.

## Non-regression

- **Identity-less tables, vector-guarded types, CDC declare/flush paths:
  untouched.** The COW arm is gated on `identity present AND non-CDC AND
  has_shadow`.
- **LastRow/FirstRow/Versioned CDC consolidation is byte-identical** except the
  blanket→targeted inline cap (§2 drive-by), which only changes behavior in the
  raced window where the old behavior lost data. `stream_cdc_consolidate`,
  `consolidate_lock`, `merge_on_read`, `cow_inline_shadow_e2e`,
  `cow_inline_shadow_gov_e2e`, `flush_suppression` stay green.
- The mutation write path changes by exactly one relaxed gate (the consolidate
  trigger accrual) — no signature, no locking, no CAS change.

## Testing

Tests are `rust_test` / `loom_fixture_test` integration targets only — never
inline `#[cfg(test)]`; new fixture tests use the `loom_fixture_test` macro
(`src/control-plane/postgres/defs.bzl`) and wire their own BUCK target
mirroring a sibling.

1. **Consuming overwrite primitive** (postgres): an overwrite carrying
   `InlineEndCap{row_ids}` end-caps exactly those rows at the new snapshot; a
   live inline row NOT in `row_ids` survives live (the mid-consolidation
   mutation's survival, proven deterministically at the primitive level); a
   plain overwrite still blanket-caps.
2. **Shadow-aware inline read**: appends + a version + a tombstone read back
   with `begin_snapshot`/`loom_tombstone` and matching `row_ids`.
3. **The fold** (engine-serving fixture): seed a file-resident identity table;
   UPDATE one id, DELETE another, insert a new object inline; consolidate →
   the new base holds the updated image, drops the deleted id, keeps untouched
   and appended rows; consumed inline rows end-capped; `has_shadow` cleared;
   reads before/after byte-identical (merge-view equivalence); as-of a
   pre-consolidation snapshot still serves the originals.
4. **Flush re-enabled**: post-consolidation, an append + `flush_table` drains
   normally (no suppression); the byte-trigger gate enqueues again.
5. **Backstop hardening**: with a live shadow row and the flag *manually
   cleared* (simulating the skew), `flush_table` no-ops, restores the flag, and
   writes no Parquet.
6. **Trigger**: non-CDC deltas crossing `consolidate_delta_threshold` enqueue
   exactly one deduped `stream_consolidate` job; below threshold none; the
   trigger re-arms after consolidation.
7. **Residual-delta path**: consolidating while one identity's delta is
   deliberately left un-folded (insert after the fold's read is simulated by a
   second delta post-consolidation) keeps `has_shadow` set and the read
   correct; a second consolidation quiesces it.
8. **e2e** (query-api, reusing `e2e-support`): governed UPDATE + DELETE →
   consolidate (RPC via the worker job or direct engine call) → governed read
   identical, ACL masking intact, no duplicate/resurrected rows.

## Global constraints (loom-specific, carry into the plan)

- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/
  `indexing_slicing`/`panic`/`todo` in production lib/bin code;
  `#[expect(lint, reason = "...")]` for justified exceptions. Test code is
  exempt via `loom_rust_test`/`loom_fixture_test`.
- **New SQL is runtime `AssertSqlSafe`** against the dynamic `inline_<tid>` /
  standalone trigger tables (the slice-1 precedent — `set_has_shadow` et al.);
  no `.sqlx` regeneration is expected. If any compile-time `query!` changes,
  run `tools/sqlx-prepare.sh` and commit `.sqlx/`.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files`.
  Markdown ends with exactly one trailing newline, no trailing whitespace.
- **Fixture tests must use `loom_fixture_test`**, and the full local suite needs
  `-j 8` (PG boot-slot starvation).

## Out of scope (deferred)

- **Slice 3 — identity-change / upsert** stays deferred as
  [[fut-cow-identity-change]] (a PATCH may not change the identity column; no
  upsert action kind). Nothing in this slice depends on it.
- **Identity-less types** keep whole-table COW (`overwrite_table`,
  `action_writer.rs:146`) — they have no merge key and no shadow tier.
- **File-granular COW** ([[fut-cow-file-granular]]) and the Arrow-native read
  leg / vector-type COW ([[fut-cow-arrow-native]]) — different write-amplification
  attacks, unchanged here.
- **Operator HTTP trigger for consolidation** — parity with the CDC consolidate
  (none). A maintenance-schedule surface can ride
  [[road-scheduled-maintenance-jobs]].
- **Small-file compaction auto-trigger** — [[road-compaction-auto-trigger]],
  specced separately; distinct job (`compact_table`), distinct counter.
- **Suppressing data triggers on consolidate commits** (the §5 wart) — a joint
  CDC+COW follow-up if it bites.

## Interfaces (names the plan consumes)

- Consumes: `lock_table`/`TableLock` (`iceberg_flush.rs:414`);
  `read_files_as_batches` (`control_plane_postgres`); `IcebergCatalog::
  {current_snapshot, schema, files_with_stats, inline_live_batch_full}`;
  `overwrite_parquet_snapshot`/`overwrite_truncate` (`iceberg_landing.rs:1117/
  :1160`); `CommitExtras`/`InlineEndCap`/`apply_commit_extras`/`write_mirror`
  (`commit_mirror.rs:27/:59/:75/:145`); `end_cap_inline_rows_by_id`
  (`iceberg_inline.rs:106`); `has_shadow`/`set_has_shadow`/`clear_has_shadow`
  (`iceberg_inline.rs:771-806`); `identity_for_table`
  (`control_plane_postgres::ontology`); `bump_consolidate_trigger`/
  `arm_consolidate_trigger`/`clear_consolidate_trigger`/`reset_inline_trigger`
  (`iceberg_mirror.rs`); `register_batches` (`datafusion_io`); the
  `stream_consolidate` job + `ConsolidateStream` RPC + `handle_stream_consolidate`
  (`worker/src/consolidate.rs:15`, `engine/src/service.rs:260`).
- Produces (later plan tasks rely on these EXACT names/types):
  - `pub async fn overwrite_parquet_snapshot_consuming(pool: &PgPool, catalog:
    &SqlCatalog, table: &TableRef, columns: &[ColumnSpec], batches:
    Vec<RecordBatch>, lineage: Option<&LineageEvent>, consumed: InlineEndCap<'_>)
    -> Result<SnapshotId>` in `iceberg_landing` (shares its body with
    `overwrite_parquet_snapshot`; both zero-file branches route through an
    `overwrite_truncate` extended with `Option<InlineEndCap>`).
  - The `write_mirror` blanket-cap rule: `end_cap_live_inline_rows` runs only
    when `overwrite && extras.end_cap.is_none()`.
  - `IcebergCatalog::inline_live_batch_shadow(&self, table: &TableRef, at:
    SnapshotId) -> Result<Option<(i64, Vec<i64>, RecordBatch)>>` in
    `iceberg_inline.rs` — every live inline row (appends, versions, tombstones)
    with `begin_snapshot` (i64) + `loom_tombstone` (bool) appended after the
    user columns.
  - `pub async fn clear_has_shadow_if_quiescent(conn: &mut PgConnection, tid:
    i64) -> Result<bool>` in `iceberg_inline.rs` (true = cleared).
  - `pub async fn consolidate_table(cp: &PgControlPlane, catalog: &SqlCatalog,
    pool: &PgPool, table: &TableRef) -> Result<i64, EngineServingError>` in
    `engine-serving/src/consolidate.rs` — the dispatch entry the
    `ConsolidateStream` RPC handler calls; `consolidate_cow_locked` is its
    private COW arm.
  - The relaxed consolidate-trigger gate in `write_inline_delta`
    (`emitted_rows = 1` on the non-CDC version/tombstone branches; `if cdc &&`
    becomes `if let Some(threshold)`).
  - The hardened flush guard in `flush_locked`: shadow-row check over
    `loom_row_id = ANY(read row_ids)`; on hit, no-op + `set_has_shadow` +
    `reset_inline_trigger`.
