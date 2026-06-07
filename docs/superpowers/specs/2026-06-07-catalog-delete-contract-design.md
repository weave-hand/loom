# Design: Catalog MVCC delete/before-existence contract (Step 2a #3)

> **Status:** approved design. Third hardening item from
> `2026-06-06-control-plane-critical-review.md` (§1) under the loom roadmap's Step 2a.
> Extends the `CatalogSeed` test seam and adds a `catalog_delete_contract`.

## Problem

The catalog's MVCC predicate — `begin <= s AND (end IS NULL OR end > s)`, reimplemented
independently in both adapters — is only exercised on the **append path**: every seeded
row has `end = NULL`, so the `end > s` branch and the `begin <= s` false branch are
untested in both backends. The riskiest duplicated logic runs only on its happy half.

### The subtlety that shapes the test

`current_snapshot` (and the memory `latest_live`) return the **latest snapshot at which
the table is live** — they search history. So after a `DROP` at snapshot `D` with no data
added after the last batch `s1`, `current_snapshot(T)` returns **`s1`**, not `NotFound`.
`NotFound` is reserved for a table that **never existed**. "Dropped" is therefore
observable not as "current_snapshot fails" but as **time-travel boundaries**:

- `files(T, D)` / `schema(T, D)` → `NotFound` — the table is not live *at the drop
  snapshot* (`end > s` evaluates **false**; previously-untested branch).
- `files(T, s1)` → still returns the files — live at `s1 < D` (`end > s` **true with a
  non-null `end`**; previously-untested — the append path only ever had `end = NULL`).
- `snapshots(T)` excludes `D`; `current_snapshot(T)` still returns `s1` (time-travel into
  the live past survives the drop).

And **query-before-existence** exercises the `begin <= s` false branch: at a snapshot that
predates the table's `begin`, the table is not live → `NotFound`.

## Seam change (`testkit`)

Add one method to the `CatalogSeed` trait:

```rust
#[async_trait]
pub trait CatalogSeed {
    async fn seed(&self, spec: SeedSpec) -> Vec<SeededSnapshot>;
    /// Drop `table`. Returns the snapshot `D` at which it was dropped: the table and
    /// its files/columns gain `D` as their `end_snapshot`, so the table is not live at
    /// `D` or later, but remains live (time-travellable) at any snapshot `< D`.
    async fn drop_table(&self, table: &TableRef) -> SnapshotId;
}
```

Adding a method breaks the two existing impls until updated; both are in the adapters'
`tests/catalog.rs` and are updated in the same change. (Chosen over generalizing
`SeedSpec` into an op-list — one method is surgical and the only new op we need.)

### memory seeder

A new inherent method on `MemoryControlPlane` (mirroring `seed_catalog`), wrapped by
`MemSeeder`:

```rust
/// Test-support: drop `table` at a fresh snapshot, setting `end` on the table and its
/// still-open files/columns so the MVCC `end`-bound is exercised at the file/column
/// level (not just short-circuited by the table-liveness gate).
pub fn drop_table_catalog(&self, table: &TableRef) -> SnapshotId {
    let mut cat = self.catalog.lock().unwrap();
    let key = (table.schema.clone(), table.name.clone());
    let d = cat.new_snapshot();
    if let Some(t) = cat.tables.get_mut(&key) {
        t.end = Some(d);
    }
    for f in cat.files.get_mut(&key).into_iter().flatten() {
        if f.end.is_none() { f.end = Some(d); }
    }
    for c in cat.columns.get_mut(&key).into_iter().flatten() {
        if c.end.is_none() { c.end = Some(d); }
    }
    SnapshotId(d)
}
```

Setting `end` on files+columns (not just the table) matters: it makes memory exercise the
**non-null `end > s`** branch at the file/column level, the same branch pg runs — rather
than letting the table-liveness gate short-circuit it (which would leave memory's
file/column `end` permanently `NULL` and the branch untested there).

### pg seeder (real DuckLake)

A new `DuckLakeWriter::drop_table` issues `DROP TABLE lake.<schema>.<table>` through the
pinned DuckDB CLI (same ATTACH as `seed`: same db, socket, data dir), then reads back the
drop snapshot from the catalog:

```rust
pub async fn drop_table(&self, schema: &str, table: &str) -> i64 {
    // ... same ATTACH preamble as seed() ...
    // DROP TABLE lake.{schema}.{table};
    // run via Command::new(duckdb_bin).arg("-c").arg(sql)
    // then read back the end_snapshot DuckLake set on the table row:
    //   select t.end_snapshot from ducklake_table t
    //     join ducklake_schema s on t.schema_id = s.schema_id
    //   where s.schema_name=$1 and t.table_name=$2 and t.end_snapshot is not null
    //   order by t.end_snapshot desc limit 1
}
```

`PgSeeder::drop_table` wraps it (`SnapshotId(writer.drop_table(...).await)`). The same
`DuckLakeWriter` instance seeds and drops, so both tables share one DuckLake catalog.

## Contract (`testkit`)

A new `catalog_delete_contract<C: Catalog, S: CatalogSeed>(catalog, seeder)`, called from
a second `#[tokio::test]` in each adapter's existing `tests/catalog.rs` (no new BUCK
target). Sketch:

1. **Pre-seed an unrelated table** (`main.other`, one batch) to advance the global
   snapshot counter; record `before = that snapshot` (predates the target's existence).
2. **Seed the target** `main.events` (cols `id BIGINT NOT NULL`, `name VARCHAR`; batches
   `[10, 20]`) → `s1` = the second batch's snapshot. Sanity: `current_snapshot == s1`,
   `files(s1).len() == 2`.
3. **Before-existence** (`begin <= s` false): `files(T, before)` and `schema(T, before)`
   → `NotFound`.
4. **Drop**: `d = drop_table(T)`; assert `d > s1`.
5. **`end > s` false**: `files(T, d)` and `schema(T, d)` → `NotFound`.
6. **`end > s` true (non-null end)**: `files(T, s1).len() == 2`, `schema(T, s1).columns.len() == 2`
   (time-travel into the live past still works).
7. **History/current drop-aware**: `snapshots(T)` all `< d`; `current_snapshot(T).id == s1`.
8. **Never-existed stays distinct**: `current_snapshot(main.nope)` → `NotFound`.

`SnapshotId` already derives `Ord`, so `d > s1` / `sn.id < d` compile. Assertions are on
variants/ids, never on backend messages.

## Testing

- New `catalog_delete_contract` in `testkit`, run from a second test fn in
  `memory/tests/catalog.rs` (fresh `MemoryControlPlane`) and
  `postgres/tests/catalog.rs` (fresh `fixture.fresh_db()` + `PgSeeder`). Both existing
  catalog targets gain a test; no BUCK changes.
- The pg path drives a real `DROP TABLE` and reads back the end-snapshot, validating that
  DuckLake's `end_snapshot` semantics match the adapter's range queries.

## Non-goals

- **Schema evolution** (`ALTER TABLE` add/drop column, asserting `schema(T, old)` vs
  `schema(T, new)`) — deferred and recorded in `docs/FUTURE.md`. `DROP` already exercises
  the column `end`-bound; evolution is about *which* columns change, a lower-risk path
  that adds `ALTER`/CLI surface.
- **File supersession / compaction** (files replaced rather than the table dropped) —
  not deterministically CLI-drivable here; deferred.
- Changing the append-path `catalog_contract` — left as-is; the new contract is additive.
