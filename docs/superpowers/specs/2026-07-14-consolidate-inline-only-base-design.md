# Consolidate over an inline-only base Design

> **Status:** design (direction). This spec makes `iss-consolidate-inline-only-base`
> build-ready. The item stays in ISSUES (a defect in shipped code); a separate work
> agent writes the implementation plan from it and builds it.

## Problem

`consolidate_locked`'s CDC arm (`engine-serving/src/consolidate.rs:180-349`) reads the
base's file tier **unconditionally**:

```rust
214	    let files = ice.files_with_stats(table, current.id).await.map_err(to_serving)?;
218	    let paths: Vec<String> = files.into_iter().map(|f| f.path).collect();
219	    let (file_schema, file_batches) = read_files_as_batches(catalog, table, &paths)
```

`read_files_as_batches` (`postgres/src/iceberg_read.rs:30-40`) calls
`catalog.load_table(&ident)` **before** iterating the (possibly empty) path list, purely
to derive an Arrow schema from `tbl.metadata().current_schema()`. For a table with no
row in the vendored `iceberg_tables` catalog, that errors — even with `paths == []`.
Its own doc comment ("an empty `files` slice returns the table schema and zero batches",
`iceberg_read.rs:23`) is true only when a catalog row exists.

This is the identical defect `#iss-mv-delta-inline-source-unflushed` (closed by #436)
fixed in `mv_delta_locked`. The **shadow/COW sibling in the same file is already
guarded** (`if has_files { read_files_as_batches(…) }`, `consolidate.rs:421-426`, with a
two-armed `union_sql`) — so the asymmetry is within one function's two arms.

### Reachability — verified, and it is a production path

Every link holds:

1. **The trigger needs no flush.** `bump_consolidate_trigger`
   (`postgres/src/iceberg_mirror.rs:621-647`) has exactly **one** production caller:
   `write_inline_delta` (`postgres/src/iceberg_inline.rs:1407-1419`), which enqueues the
   `stream_consolidate` job when `delta_count >= threshold` (production default **128**,
   `ingest/src/config.rs:67`). Nothing in that path flushes.
2. **A CDC declare pre-creates only the changelog Iceberg table.** `land_cdc`
   (`postgres/src/iceberg_landing.rs:171-197`) calls `ensure_iceberg_table` for
   `changelog_table_ref(table)` only; the **base** gets no Iceberg table. The inline
   branch (`inline_append_decl`) mints mirror rows exclusively — it never touches the
   Iceberg SQL catalog. The base's `iceberg_tables` row first appears at flush.
3. **`paths == []` really reaches the read.** `files_with_stats`
   (`postgres/src/iceberg_catalog.rs:75-101`) is a plain select over
   `iceberg_mirror.data_file` → empty vec, no error. And the function's existing
   `NotFound → Ok(0)` early return does **not** fire, because `current_snapshot` reads
   the **mirror** (which the inline path *does* populate), not the Iceberg catalog.

End-to-end: a typed insert on a CDC-declared type lands inline (default
`inline_byte_limit` 16 MiB), then ≥128 delta-rows' worth of governed `PATCH`/`DELETE`
enqueue the consolidate job before the 64 MiB byte-flush ever fires. The job then dies at
`load_table`.

### The durable secondary consequence (missed by the register entry)

`arm_consolidate_trigger` sets `enqueued = true`, and **only a successful consolidate**
clears it (`clear_consolidate_trigger`, `iceberg_mirror.rs:666-676`, called after the
overwrite at `consolidate.rs:344`). The enqueue condition is `delta_count >= effective &&
!st.enqueued` (`iceberg_inline.rs:1409`). So once this job fails and abandons, the trigger
stays **latched forever**: that table can never enqueue another `stream_consolidate`, even
after a later flush would have made it succeed. The fix must therefore also clear the
trigger on the no-op path, or the table stays wedged.

### Corrections to the register entry (fix in the closing PR)

- The entry says the reachable shape is "a small first `land` plus `write_delta`
  **inserts**". Wrong: `write_delta` is the UPDATE/DELETE path and is the *only* thing that
  bumps the trigger. Inserts (`write_object` → `land_cdc` → `inline_append_decl`) never bump
  it. The reachable shape is *inline insert(s) + ≥128 delta-rows of governed UPDATE/DELETE*.
- The entry's "same one-line `if !paths.is_empty()` guard plus an empty-tier early return;
  cheap" **under-specifies the fix** — see Design below. `mv_delta`'s early-return-an-empty-
  delta is *not* the analogue: consolidate's job is to **write**.

## Design

Consolidate is a **writer**, not a reader. The three states and their correct behavior:

| `paths` | inline tier | Behavior |
| --- | --- | --- |
| non-empty | any | **Unchanged** — today's path, byte-identical. |
| **empty** | `Some` | **Real work.** Skip the file tier; register only `base_inline` and take the inline-only arm of `union_sql`. The fold (`Precedence::Offset`, `row_number() … where _rn = 1 and loom_change_kind <> '-D'`) then commits through the existing `overwrite_parquet_snapshot_consuming` (`consolidate.rs:309-322`), whose `append_parquet_snapshot` → `ensure_iceberg_table` (`iceberg_landing.rs:267`) **creates the base's Iceberg table on the spot** and writes its first Parquet file (with framing, resolved from `pg_stream_bucket_count`). Net effect: the inline-only CDC base gets materialized — exactly what the job exists to do. |
| **empty** | `None` | **No-op — but not a bare `return Ok(0)`.** Nothing to fold, but it must still `clear_has_shadow` + `clear_consolidate_trigger`, mirroring the COW arm's stale-flag self-heal (`consolidate.rs:399-415`). Otherwise `enqueued` latches permanently (above). |

Mechanically this is the COW arm's shape, lifted into the CDC arm: a `has_files` bool, a
conditional `register_batches("base_files", …)`, and a two-armed `union_sql`.

**No schema plumbing is needed.** `file_schema` is used *only* to register `base_files`
(`consolidate.rs:234`). With the file tier skipped, the fold reads the inline batch's own
schema (`inline_batch.schema()`, `consolidate.rs:239`), which `inline_live_batch_full`
already returns fully framed (`iceberg_inline.rs:1616-1622`). So — unlike the `mv_delta`
fix — **`framed_schema` is not required**, and `mv_delta`'s private `fn framed_schema`
(`mv_delta.rs:230-240`, not `pub`) does **not** need its visibility changed.

Two invariants to preserve:

- The CDC arm today does **not** early-return on `inline == None` — it folds files-only and
  re-writes them (a legitimate post-flush fold). Keep that.
- A fold yielding **zero** rows (every identity's winner is a `-D`) already short-circuits
  to `overwrite_truncate` (`iceberg_landing.rs:1214-1215`), which is mirror-only and safe
  with no `iceberg_tables` row.

## Non-regression

- Any base with ≥1 Parquet file takes the existing path byte-identically.
- The guard can only ever *skip an empty relation* — the fold's row set is unchanged.
- No migration; no new SQL expected (all helpers exist) — so no `.sqlx` refresh.

## Testing

Mirror `engine-serving/tests/mv_delta_inline_source.rs` (the #436 regression test), whose
`land_inline` helper is the canonical inline-only idiom (`InlineLimits { inline_byte_limit:
usize::MAX, flush_byte_threshold: i64::MAX }` — forces inline, disarms the byte-flush), and
combine it with the CDC seeding from `postgres/tests/stream_cdc_consolidate_trigger.rs`
(`ensure_table` + `next_snapshot` + `declare_cdc(tid, 2, "id", MergeEngine::LastRow)` +
`inline_append` + `write_inline_delta`). Note the CDC arm needs **no ontology type** for
`LastRow` (identity comes from `meta.bucket_key`).

New `loom_fixture_test` in `//src/services/engine-serving` (mirror the `cow-consolidate` /
`mv-delta-inline-source` stanzas):

- **Inline-only CDC base consolidates** (the acceptance test) — declare CDC, inline-append,
  `write_inline_delta` a few updates, call `consolidate_table` with **no flush**: succeeds,
  the base's Iceberg table now exists, the folded Parquet holds the merged rows, folded
  inline rows are end-capped.
- **Neither tier** — declared CDC base with no files and no live inline rows: no-op, no
  error, and — the regression that matters — `clear_consolidate_trigger` ran, so a
  subsequent `write_inline_delta` past the threshold **can enqueue again** (assert on the
  `bump_consolidate_trigger` return, not just absence of a panic).
- **Files-only fold still works** (pin the preserved invariant).
- **The trigger-latch regression** — assert the wedge is gone: pre-fix, a failed consolidate
  leaves `enqueued = true` forever.
- The existing CDC e2e (`query-api/tests/stream_cdc_consolidate.rs:193`) carries an explicit
  `gov.flush_table(...)`. **Leave it** — it exercises the files-present arm on purpose; the
  new inline-only test is the missing coverage. (Contrast with #436, where deleting the
  workaround *was* the acceptance test.)
- Also note `cow_consolidate.rs` has **no inline-only COW case** either, despite the guard
  at `consolidate.rs:421` — adding one is cheap and closes an untested branch.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `#[expect(lint, reason)]` locally.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- Tests are `rust_test`/`loom_fixture_test` targets only.
- If SQL changes, `tools/sqlx-prepare.sh` + commit `.sqlx` (not expected here).

## Out of scope (record as a new issue in the closing PR)

**A second, unreported instance of the same bug:** `collect_vectors`
(`postgres/src/vector_index.rs:464`, called by `build_vector_index`, `vector_index.rs:652`)
calls `read_files_as_batches` with `paths` straight from `files_with_stats` and then unions
the **hot inline tier** — i.e. it explicitly supports inline-only data, yet dies on
`load_table` when the table has no Parquet file. Reachable via the engine's
`BuildVectorIndex` RPC (`engine/src/service.rs:477`), and note `overwrite_truncate` enqueues
rebuild jobs *without* creating an Iceberg table. File it as its own item rather than
scope-creeping this fix.

For completeness, the remaining `read_files_as_batches` call site — `engine/src/flight.rs:306`
(Flight `DoGet`) — is non-empty by construction at its only in-tree producer
(`worker/src/compact.rs:40-50` returns early unless `small.len() >= 2`), though it is not
defensively guarded.

## Acceptance

1. A CDC base that has only ever been inline-appended consolidates successfully, with no
   flush anywhere, and its Iceberg base table is created by the fold.
2. The neither-tier case is a clean no-op that **clears** the consolidate trigger (the
   table is not wedged).
3. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `consolidate_locked` + `consolidate_cow_locked`'s `has_files` shape
  (`engine-serving/src/consolidate.rs:180-510`); `read_files_as_batches` / `load_table`
  (`postgres/src/iceberg_read.rs:23-40`); `files_with_stats`
  (`postgres/src/iceberg_catalog.rs:75-101`); `inline_live_batch_full`
  (`postgres/src/iceberg_inline.rs:1616-1622`); `overwrite_parquet_snapshot_consuming` /
  `append_parquet_snapshot` / `ensure_iceberg_table` (`postgres/src/iceberg_landing.rs`);
  `clear_has_shadow` / `clear_consolidate_trigger` / `bump_consolidate_trigger`
  (`postgres/src/iceberg_mirror.rs:621-676`).
- Produces: the guarded file tier + two-armed `union_sql` in the CDC arm; the
  trigger-clearing neither-tier no-op; the inline-only CDC consolidate fixture test.
