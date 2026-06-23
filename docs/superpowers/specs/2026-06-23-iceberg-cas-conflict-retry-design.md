# Multi-writer CAS-conflict retry/backoff (Iceberg) — design

_2026-06-23. Work item: [[road-iceberg-cas-conflict-retry]] (promoted from
[[fut-iceberg-cas-conflict-retry]])._

## Problem

The Iceberg catalog commits a snapshot with an optimistic compare-and-swap on the
table's `metadata_location` pointer. In
`src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs`,
`do_update_table` runs:

```sql
UPDATE iceberg_tables
SET metadata_location = ?, previous_metadata_location = ?
WHERE ... AND metadata_location = ?   -- CAS on the old location
```

When a concurrent writer commits between this writer's table load and its CAS,
the `WHERE metadata_location = <old>` matches zero rows and the commit returns
`Error(ErrorKind::CatalogCommitConflicts)` — already marked `.with_retryable(true)`
(catalog.rs ~line 451–457). The whole Postgres tx (pointer CAS + `iceberg_mirror.*`
projection + optional lineage + optional inline end-cap) rolls back atomically.

**Nothing retries this today.** Neither the catalog nor any writer-level entry
point re-drives a lost CAS, so under contention some appends simply fail instead
of eventually committing. This is why
`concurrent_appends_keep_the_mirror_consistent`
(`tests/iceberg_write_roundtrip.rs`) is pinned at `N=4`: bumping it to `N=8` turned
it red, and the bump was reverted. Hoisting the object-store reads out of the tx
(`iss-iceberg-tx-objectstore`) shortened the conflict window but did not add retry.

This is a production-readiness gap on the path to **Iceberg-default**
([[fut-replace-ducklake-decision]]): real multi-writer deployments against S3
([[road-iceberg-real-object-store]]) will hit this routinely.

## Why a bare CAS retry is wrong

A conflict means the writer's *staged metadata* was built against a now-stale
`metadata_location`. Re-running only the inner CAS would replay the same stale
metadata and conflict forever. A correct retry must **reload the table** (to pick
up the winning writer's new parent snapshot) and **re-stage the `fast_append`**
against it, then re-commit.

The already-written Parquet **data files are reused** across attempts — they are
written once, before any commit, and carry unique UUID-prefixed paths
(`write_parquet`), so re-adding them to a fresh `fast_append` against the reloaded
table is correct and collision-free. Only the metadata commit replays.

## Approach

A single bounded, backed-off retry helper in `iceberg_writer.rs`, which the
`append_batches*` family routes its commit through. `do_update_table`, the
`SqlCatalog` `Catalog` impl, and the `CommitExtrasCatalog` decorator are
**untouched** — the CAS-conflict detection already exists and is already
retryable-flagged; this slice only adds the consumer that respects it.

Considered and rejected:

- **Push retry into `SqlCatalog::update_table`.** Transparent to all callers, but
  the trait method only sees the already-staged commit — it cannot reload and
  re-stage, so it would replay stale metadata. Structurally wrong.
- **Caller-driven retry in each writer (ingest, flush, tests).** Duplicates the
  reload/re-stage logic at every call site — exactly the gap that left the test
  harness un-retried.

### The helper

```rust
// iceberg_writer.rs
const COMMIT_MAX_RETRIES: u32 = 5;
const COMMIT_BACKOFF_BASE: Duration = Duration::from_millis(5);
const COMMIT_BACKOFF_CAP:  Duration = Duration::from_millis(200);

/// Commit a `fast_append` of `data_files`, retrying on a lost pointer CAS.
/// On `CatalogCommitConflicts`, reload the table (fresh parent snapshot),
/// re-stage the same already-written `data_files`, and re-commit, with bounded
/// exponential backoff + jitter. Any other error, or a conflict past the cap,
/// propagates unchanged.
async fn commit_append_with_retry(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    table: Table,            // the already-loaded table for attempt 0
    data_files: Vec<DataFile>,
) -> Result<()>;
```

Loop:

1. Build `Transaction::new(&table).fast_append().add_data_files(data_files.clone())`,
   `apply`, `tx.commit(catalog)`.
2. `Ok(())` → return.
3. `Err(e)` with `e.kind() == ErrorKind::CatalogCommitConflicts` and
   `attempt < COMMIT_MAX_RETRIES` → sleep the backoff, then
   `table = catalog.load_table(ident).await?` and continue.
4. Any other error, or a conflict at the cap → `return Err(e)` (unchanged; still
   retryable-flagged so a higher layer such as the queue worker's `RetryPolicy`
   can re-drive it).

Attempt 0 uses the table the caller already loaded — the reload round-trip is paid
only on the retry path, so the common no-conflict case is unchanged.

`append_batches` and `append_batches_with_extras` both write Parquet once, then
delegate their commit section to this helper, passing `table.identifier()` as
`ident`. For the extras path, the `CommitExtrasCatalog` wrapper is rebuilt each
attempt (it is cheap and holds only borrows) and passed as the `&dyn Catalog`, so
the lineage event and inline end-cap **re-present per attempt** and persist only on
the winning commit — a lost CAS rolls them back with the snapshot. This matches the
promise the `append_batches_with_lineage` doc comment already makes.

### Backoff (inline, no new dependency)

`tokio::time::sleep` with `delay = min(COMMIT_BACKOFF_CAP, COMMIT_BACKOFF_BASE * 2^attempt)`,
plus a small jitter term derived from a per-writer value already in hand (a hash of
the `TableIdent` XOR the attempt number) so colliding writers de-synchronize rather
than re-collide in lockstep. No `rand` or `backoff` crate is added — the repo keeps
its dependency closure tight, and this is ~15 lines.

### Scope

`append_batches*` only — the real-Parquet append paths (ingest landing and flush's
produced append both route here), i.e. every write path that exists and races
today. Inline appends (`inline_append`) are mirror-only with no pointer CAS, so they
are out of scope by construction.

The helper is generic over "reload + re-stage + commit." When
[[road-iceberg-overwrite-mode]] lands, its overwrite commit routes through the same
helper with a one-liner — wiring it now would be speculative (the overwrite commit
shape is not in-tree), so it is a deferred follow-up owned by that slice, not built
here.

## Error contract

Unchanged surface. On exhaustion the original `CatalogCommitConflicts` propagates
(retryable-flagged). No new public error variants; no signature changes to the
`append_batches*` return types.

## Testing

- Keep `concurrent_appends_keep_the_mirror_consistent` at `N=4` as the baseline.
- **Add** a sibling `concurrent_appends_tolerate_contention` at `N=8`, sharing a
  common harness helper extracted from the existing test (no copy-paste — the
  per-writer spawn/append/assert body becomes a parameterized helper over `N`).
  It asserts the same invariants: exactly `N` snapshots, zero orphaned snapshots,
  exactly `N` files. `N=8` going green is the proof retry works — it went red
  without it.
- **No minimum-retry-count assertion.** Contention is nondeterministic; asserting
  "at least one retry occurred" would be flaky (a run where writers happen not to
  collide would spuriously fail). The invariant set under `N=8` is the robust proof.

Both tests stay `loom_fixture_test` targets under
`//src/control-plane/postgres:iceberg-write-roundtrip` (hermetic Postgres).

## Deferred follow-ups

- Env-tunable retry cap (`LOOM_ICEBERG_COMMIT_MAX_RETRIES`) — a module constant is
  fine until a deployment needs tuning.
- Wiring the overwrite path ([[road-iceberg-overwrite-mode]]) through the helper —
  owned by that slice.
- Feeding retry counts into a metrics counter — waits on [[fut-metrics-crate]].

## Out of scope

Object-store-read scoping (already done, `iss-iceberg-tx-objectstore`); DuckLake's
write path (its concurrency model is separate); changing the CAS mechanism itself.
