# Multi-file DuckLake LIMIT corruption — loom-side read guard — Design

> Closes the loom-side portion of `iss-multi-file-limit-misread`. A DuckLake table
> backed by **more than one** Parquet file, read with an **unordered pushed-down
> `LIMIT`**, reconstructs column values incorrectly — an upstream DuckDB/DuckLake
> bug (observed: int64 `id` `10 → 266`, i.e. `10 + (1 << 8)` — a file/row-group
> ordinal bleeding into the value's high bits). loom already pins *small* outputs
> to a single file at write time; *large, legitimately-multi-file* tables remain
> exposed on every governed read (which always pushes `LIMIT 1000`, never ordered).
> This slice adds a loom-side read guard, proven by a reproduction test.

## Goal

A governed read of a multi-file DuckLake table returns **correct, uncorrupted
values** under the standard pushed-down `LIMIT`, on the DuckDB serving engine —
without removing the always-present safety `LIMIT`.

## Exposure (today)

- Every governed read appends `LIMIT 1000` (`DEFAULT_LIMIT`,
  `…/handler.rs`), emitted by every compiler
  (`compile_select_with`, `compile_chain_with`, `compile_chain_pairs`,
  `compile_graph_reach`, `compile_graph_reach_union`, `compile_graph_reach_tail`
  in `…/query-api/src/sql.rs`). Reads carry **no `ORDER BY`**.
- The write path already pins `minimum_parallel_output_files = estimate_partitions`
  (`…/datafusion-io/src/write.rs`), so small results land as one file (safe). Large
  results that legitimately split into N>1 files are the exposed case.

## Reproduction first (the foundation)

The slice begins with a fixture test (`loom_fixture_test`, Postgres + DuckLake)
that builds a genuinely **multi-file** table — force >1 Parquet file via a small
`target_file_size_bytes` (as `…/datafusion-io/tests/single_file_write.rs` does for
its split case) and enough distinct-`id` rows — then reads it through the serving
engine with the standard `LIMIT`.

- The work agent **first confirms the corruption reproduces without the guard**
  (returned `id`s are wrong / out of the source range), to prove the guard does
  real work.
- The durable regression assertion is that **with the guard** the read returns the
  **exact source `id` set, uncorrupted**.
- **Known risk:** the corruption was reported non-deterministic (it depended on
  executor batch ordering). If it cannot be made to fail reliably *without* the
  guard, the test still asserts correctness *with* the guard and documents the
  manual repro steps; the work agent records what it observed.

## The guard — stable `ORDER BY` barrier (primary)

Apply a stable `ORDER BY` immediately before the `LIMIT` on the DuckDB serving
path. Rationale:

- It targets the documented **unordered** trigger directly.
- `ORDER BY <cols> LIMIT n` compiles to DuckDB's **TopN** operator, which must read
  the full scan output to select the top n — so the `LIMIT` is **not pushed into
  the multi-file Parquet scan** (the corruption site), while TopN keeps it bounded
  and efficient (≈O(n log k), not a full sort). A bare nested subquery is *not*
  used as the primary because DuckDB flattens it and re-pushes the `LIMIT`.

**Order key:** the queried type's declared `identity` when it is present and
visible; otherwise the **projected columns** (always available — value integrity
needs the sort barrier, not a unique total order). The key is composed from
columns the read already projects, so no extra columns are read or leaked (a
masked column is not used as an order key).

**DuckDB-only, dialect-gated.** Add a capability to the `SqlDialect` trait, e.g.
`fn limit_needs_order_barrier(&self) -> bool` — `true` for `DuckDbDialect`,
`false` for the DataFusion/Iceberg dialect (that engine does not have the bug, so
it keeps the bare `LIMIT`). A shared helper emits `ORDER BY <order key> LIMIT n`
when the dialect requests the barrier, and `LIMIT n` otherwise. It is applied at
**every** `LIMIT` site listed above, since any of those reads can scan a multi-file
table (object reads, chains, association pairs, and the three graph reaches — the
graph reaches order the outer projection over the reachable set).

**Fallback (documented):** if the reproduction shows the `ORDER BY` barrier does
*not* fully fix the corruption, fall back to an explicit pushdown-suppression
construct (a materialization barrier so the scan reads whole row-groups before the
`LIMIT`), validated against the same repro. The chosen mechanism is whichever the
repro proves correct.

## What this does NOT change

- The always-present safety `LIMIT` stays (reads are still capped).
- The write-side single-file pinning (`estimate_partitions`) stays — it covers the
  small-output case this read guard does not need to.
- The non-DuckDB (DataFusion/Iceberg) serving path is unchanged (no barrier).
- Reads become *ordered* on the DuckDB path (a behavior change). Existing read
  tests assert row *sets*, not order; any that assume an order are updated.

## Testing

- **Multi-file serving repro → regression** (`loom_fixture_test`): the foundation
  test above — a multi-file table read under `LIMIT` returns the exact source
  values with the guard.
- **Compiler unit test**: assert the emitted SQL carries the `ORDER BY <key>`
  before `LIMIT` for `DuckDbDialect`, and carries a bare `LIMIT` (no barrier) for
  the non-barrier dialect. Cover the identity-present and identity-absent
  (projected-columns) key selection.
- **Untouched:** the existing `…/datafusion-io/tests/single_file_write.rs`
  write-side regression tests stay green.

## Out of scope → follow-on item

- **Upstream DuckDB/DuckLake repro + fix, and a version-pin bump.** File the
  upstream bug with a minimal multi-file + `LIMIT` reproduction; when loom's pinned
  DuckDB (`duckdb` crate `1.10503.1`, bundled) is bumped to a version where it is
  fixed, this workaround and its `SqlDialect` gate can be removed. Mint a `fut-`
  item (`area:query`/`devx`) so the workaround's removal is tracked. (A DuckDB pip
  `SET`/`PRAGMA` that disables the offending optimization, if one is found during
  the upstream work, is an acceptable alternative removal path.)

## Files

- Modify: `src/services/query-api/src/sql.rs` — add the `SqlDialect`
  `limit_needs_order_barrier()` capability + impls; a shared order-barrier helper;
  apply it at each `LIMIT` site (`compile_select_with`, `compile_chain_with`,
  `compile_chain_pairs`, `compile_graph_reach`, `compile_graph_reach_union`,
  `compile_graph_reach_tail`). The order key is threaded from each handler's
  already-resolved identity/projection.
- Possibly modify: `src/services/query-api/src/handler.rs` — pass the order key
  (identity-or-projection) into the compilers if not already in scope at the call
  sites.
- Create: a multi-file serving repro `loom_fixture_test` (query-api `tests/`) and a
  compiler unit `rust_test`; the `BUCK` targets.
- Modify: `docs/ISSUES.md` — close `iss-multi-file-limit-misread` (scoped) and add
  the upstream-fix / version-bump follow-on item.
- Core untouched.
