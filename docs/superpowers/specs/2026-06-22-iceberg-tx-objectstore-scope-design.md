# Iceberg commit: object-store reads out of the PG transaction — Design

> Closes `iss-iceberg-tx-objectstore`. The atomic Iceberg commit
> (`SqlCatalog::do_update_table`, `…/iceberg_sql_catalog/catalog.rs:394`) opens a
> Postgres transaction for the pointer CAS + mirror projection, but the mirror
> projection reads the **new snapshot's manifests from object storage**
> (`added_files_of` → `load_manifest_list`/`load_manifest` via the table's `FileIO`)
> **while that transaction is held open**. Holding a PG transaction — and its row
> locks and pooled connection — across slow, network object-store I/O caps
> multi-writer throughput. This slice hoists every object-store read out of the
> transaction, so the tx holds only fast local PG work, with no change to the
> atomicity guarantees.

## Where the object-store read sits today

`do_update_table` already does most object-store I/O *before* `begin()`:

- `load_table` (`catalog.rs:400`) — reads current metadata (pre-tx ✓);
- `staged_table.metadata().write_to(file_io, …)` (`:406`) — writes staged metadata
  (pre-tx ✓);
- `let mut tx = self.connection.begin()` (`:411`).

Then **inside** the tx it calls `project_mirror(&mut tx, …)` (`:449`), whose first
line is `let files = added_files_of(staged).await?` (`:376`) — that
`added_files_of` (`…/iceberg_mirror.rs:230`) loads the manifest list and each
manifest through `table.file_io()` (object store, lines 234-244). Everything else
in the tx is pure PG SQL (`next_snapshot`, `ensure_table`, `project_columns`,
`project_files`, the optional end-cap and lineage, then `commit`). So
`added_files_of` is the **sole** object-store read inside the transaction.

The manifests are immutable and already written (the `fast_append` wrote them, and
`write_to` wrote the staged metadata) by the time `do_update_table` runs, so they
can be read before the tx with identical results — there is no read-after-write
hazard and the CAS still guards the pointer.

## The fix — hoist the read, make the in-tx writer FileIO-free

Split the mirror projection into a **pre-tx read phase** and an **in-tx write
phase**, and restructure `do_update_table` so all object-store I/O precedes
`begin()`.

### Pre-tx (no transaction held)

```rust
// ... after write_to(staged metadata), BEFORE begin():
let mirror_files = added_files_of(&staged_table).await?;        // object store
let mirror_columns = columns_of(&staged_table);                 // in-memory (current schema)
let staged_snap = staged_table.metadata().current_snapshot().map(|s| s.snapshot_id());
```

### In-tx (pure PG, structurally incapable of object-store I/O)

Replace `project_mirror(&mut tx, ident, staged)` with a writer that takes the
**precomputed inputs** and **no `&Table` / `FileIO`**:

```rust
/// Write the mirror rows for a committed table state, in the caller's tx. Takes
/// precomputed inputs only — it holds no `FileIO`, so it CANNOT read object
/// storage (the property `iss-iceberg-tx-objectstore` requires, enforced by the
/// signature, not just by convention).
async fn write_mirror(
    &self,
    tx: &mut Transaction<'_, Postgres>,
    ident: &TableIdent,
    staged_snap: Option<i64>,
    columns: &[ProjectedColumn],   // whatever columns_of returns
    files: &[ProjectedFile],
) -> control_plane_core::Result<SnapshotId> {
    let ns = ident.namespace().join(".");
    let conn = &mut **tx;
    let at = next_snapshot(conn, staged_snap).await?;
    let tid = ensure_table(conn, &ns, ident.name(), at).await?;
    if !columns_exist(conn, tid).await? {
        project_columns(conn, tid, at, columns).await?;
    }
    project_files(conn, tid, at, files).await?;
    Ok(at)
}
```

`do_update_table` then becomes: object-store reads → `begin()` → CAS UPDATE
(unchanged) → `write_mirror(&mut tx, …, &mirror_columns, &mirror_files)` → end-cap
(unchanged) → lineage (unchanged) → `commit`. The transaction body is now **pure
local PG**; not a single object-store byte is read or written while it is open.

The snapshot-id allocation (`next_snapshot`) stays in-tx — it is a PG-side counter
that must share the commit's isolation, and it does not depend on the manifest read
(the files are keyed by content, not by `at`). Ordering is therefore safe: read
manifests pre-tx, allocate `at` in-tx, write the precomputed files at `at`.

## Why this is safe (atomicity unchanged)

- **CAS still guards the pointer.** If `rows_affected() == 0`, the tx rolls back and
  nothing is inserted — exactly as today. The pre-tx manifest read is simply
  discarded on a lost CAS (wasted I/O, never corruption).
- **Immutable inputs.** Staged metadata + manifests are content-addressed and
  already persisted before `do_update_table`; reading them before vs. during the tx
  yields identical `ProjectedFile`s. No TOCTOU.
- **End-cap + lineage** remain in the same tx as the CAS + mirror rows, so the flush
  end-cap and the landing lineage still commit/roll back atomically with the
  snapshot (the `iss-action-lineage-atomicity` / flush guarantees are untouched).
- **Multi-writer correctness** is already covered by
  `concurrent_appends_keep_the_mirror_consistent`
  (`…/tests/iceberg_write_roundtrip.rs:152`): N concurrent appends → exactly N
  snapshots, zero orphan snapshots, all N files. The shorter tx reduces lock-hold
  time and CAS-conflict retries; it does not change that asserted outcome.

## What this does NOT change

- The CAS / conflict-retry semantics, the mirror projection's *content*, the
  end-cap, and the lineage emission — all identical, just re-sequenced.
- No new locking, no advisory lock — the existing optimistic CAS is sufficient;
  this slice only shortens the tx, it does not redesign concurrency control.
- The single-writer behavior and all existing snapshots/time-travel semantics.
- `added_files_of` / `columns_of` themselves are unchanged — only *where* they are
  called moves.

## Testing

All tests are `rust_test`/`loom_fixture_test` integration targets.

- **Regression (the safety net):** the full iceberg suite stays green unchanged —
  especially `concurrent_appends_keep_the_mirror_consistent` (multi-writer
  integrity), the append/round-trip (`iceberg_write_roundtrip.rs`), append-with-
  lineage, flush time-travel + idempotency (`iceberg_flush.rs`), landing
  (`iceberg_landing.rs`), and the snapshot-sequence (`iceberg_snapshot_seq.rs`)
  tests. These prove the re-sequencing preserved atomicity, rollback-on-conflict,
  and mirror content.
- **The structural guarantee is by construction, not by a runtime probe:**
  `write_mirror` takes precomputed `&[ProjectedFile]`/`&[ProjectedColumn]` and a
  `&mut Transaction` — it has **no `&Table`/`FileIO`**, so it is *type-level*
  impossible for it to read object storage inside the tx. The reviewer verifies the
  signature; no flaky timing assertion is attempted.
- **Strengthen the multi-writer test (optional, low-risk):** bump
  `concurrent_appends_keep_the_mirror_consistent`'s `N` (e.g. 4 → 8) to exercise the
  shorter tx under more contention, keeping the same invariants. Only if it stays
  fast and non-flaky; otherwise leave `N` as is.

## Out of scope

- **Multi-writer *latency* benchmarking / a perf regression gate** — this slice is a
  structural fix; quantifying the throughput gain is not a goal (no bench harness
  exists).
- **Holding the tx across the *metadata write*** — `write_to` is already pre-tx; no
  change.
- **Conflict-retry tuning / backoff** — unchanged; the issue is tx duration, not
  retry policy.

## Files

- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` — in
  `do_update_table`, hoist `added_files_of` + `columns_of` + the staged snapshot id
  before `begin()`; replace `project_mirror` with the FileIO-free `write_mirror`
  taking precomputed inputs; call it inside the existing tx.
- Possibly modify: `src/control-plane/postgres/src/iceberg_mirror.rs` — only if a
  small signature/visibility tweak is needed so `write_mirror` can pass precomputed
  `ProjectedColumn`/`ProjectedFile` slices (the `project_columns`/`project_files`
  helpers already take slices). `added_files_of`/`columns_of` logic is unchanged.
- Possibly modify: `src/control-plane/postgres/tests/iceberg_write_roundtrip.rs` —
  the optional `N` bump only.
- Modify: `docs/ISSUES.md` — close `iss-iceberg-tx-objectstore`
  (`[x] status:fixed pr:#<n>`); it points at this design.
- Core (`src/control-plane/core/`) untouched; no dependency/lockfile change.
