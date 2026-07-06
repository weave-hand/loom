# Time-travel reads (`AS OF`) — design

**Item:** new (`road-timetravel-reads`, to be registered) · **Relates:** `fut-iceberg-time-travel-schema`

## Problem

loom snapshots every write atomically and retains history under an MVCC mirror:
every `iceberg_mirror` row (`data_file`, `column`, `table`) carries
`begin_snapshot`/`end_snapshot`, retiring rows are end-capped rather than
deleted, and the core `Catalog` trait already exposes historical reads —
`snapshots(table)`, `files(table, at)`, `schema(table, at)`. Age-based GC
(`iceberg_gc.rs`) is proven safe against in-window time-travel reads.

**But there is no user-facing way to read data as of a past snapshot.** The
serving path always resolves `catalog.current_snapshot(table)`
(`engine-serving/src/serving.rs:94`) and the HTTP read endpoints expose no
snapshot/timestamp selector. The plumbing to resolve an old snapshot exists;
the last mile — a selector on the read endpoints, threaded through the Flight
`CommandStatementQuery` wire to the resolution point — is unbuilt.

This slice builds that last mile for the two GET read paths.

## Scope

Add an `AS OF` selector to the two coherent GET read paths:

- `GET /objects/{type}` — typed object reads.
- `GET /datasets/{schema}/{table}` — dataset detail (snapshot id + column schema).

Two mutually-exclusive query params:

- `?as_of_snapshot=<i64>` — an exact mirror snapshot id.
- `?as_of=<rfc3339>` — resolves to the latest snapshot with
  `snapshot_time <= ts` **at which that table is live**.

Absent ⇒ current behavior (live snapshot), byte-identical to today.

**Non-goals (explicit, stay live-only / deferred):**

- Link traversal (`GET /objects/{from}/links/{link}`) — multi-table
  same-snapshot pinning is its own design.
- Mutations (insert / update / delete actions).
- SQL-syntax `AS OF` / `FOR SYSTEM_TIME AS OF` on any external SQL surface.
- **Schema-as-of resolution.** As-of reads project with the *current* schema.
  Since schema-evolution (`road-iceberg-schema-evolution`) is not built, table
  schemas do not vary across snapshots today, so this has no observable gap.
  `fut-iceberg-time-travel-schema` stays deferred.
- Hard rejection of reads outside the GC retention window (see *Retention
  caveat*) — documented limitation + a new FUTURE item, not enforced here.

## Design

### Resolution lives in query-api

query-api holds a control-plane catalog handle (`st.cp.catalog()`, already used
for metadata reads in `http.rs`). It resolves the selector to a **concrete
`SnapshotId` before the wire**, so the engine's contract stays "read table at
snapshot id X" and no timestamp logic crosses the wire:

- `as_of_snapshot=<id>`: validate the table is live at that id → else `404`.
- `as_of=<rfc3339>`: parse → `snapshot_as_of(table, ts)` → `404` if the table
  has no snapshot at/before `ts`; `400` on an unparseable timestamp.
- both params present → `400`.

### New catalog method

Add to the core `Catalog` trait (`control-plane/core/src/catalog.rs`):

```rust
/// The latest snapshot at or before `ts` at which `table` is live, or `None`
/// if the table has no such snapshot (no data at/before that instant).
async fn snapshot_as_of(&self, table: &TableRef, ts: OffsetDateTime)
    -> Result<Option<Snapshot>>;
```

- **postgres** (`iceberg_mirror.rs`): one indexed query — the max `snapshot_id`
  among rows where the table is live (`begin_snapshot <= s AND (end_snapshot IS
  NULL OR end_snapshot > s)`) and `snapshot_time <= ts`. Add a committed `.sqlx`
  entry via `tools/sqlx-prepare.sh`.
- **memory** (`memory` fake): equivalent scan over recorded snapshots.
- **testkit contract** (`control-plane/testkit`): shared test asserting
  semantics (exact ts hit, between-snapshots resolves to the earlier, before
  first snapshot ⇒ `None`, dropped/re-created table liveness) run against both
  adapters.

`Snapshot` already carries `id` and `time` (the mirror's `snapshot_time`
column); no new type.

### Wire

Add one optional field to `GovernedStatementQuery`
(`engine-wire/src/flight.rs`):

```rust
#[serde(default)]
pub as_of_snapshot: Option<i64>,
```

`#[serde(default)]` keeps it backward-compatible under the struct's existing
`deny_unknown_fields`. `None` ⇒ live read (today's path, byte-identical). A
single query-level selector is applied to the (single) table the object/dataset
read registers — matching the read shape; multi-table selection is out of scope.

### Engine

`execute_governed_sql_stream(…, at: Option<SnapshotId>)`
(`engine-serving/src/serving.rs`) threads `at` to `build_serving_provider`.
When `Some`, it replaces `catalog.current_snapshot(table)` with `at` and feeds
it to the existing `files_with_stats(table, at)` — the mirror provider already
takes an arbitrary `SnapshotId`. When `None`, the current-snapshot branch is
untouched. Schema is derived from the current schema (per scope). The Flight
`do_get` governed-SQL handler (`engine/src/flight.rs`) decodes the new field and
passes it through.

### query-api HTTP + client

- `query_params.rs` / `params.rs`: parse `as_of` / `as_of_snapshot`, enforce
  mutual exclusion, map parse/validation failures to `400`.
- Object-read handler: resolve → set `as_of_snapshot` on the
  `GovernedStatementQuery` the engine client sends.
- Dataset-detail handler (`http.rs`): when a selector is present, resolve it and
  report that snapshot (id + time) and its schema instead of `current_snapshot`.
- OpenAPI (`openapi.rs`): document both params on the two operations.

### Retention caveat

A selector resolving to a snapshot aged past the GC horizon `H` may have had its
file rows reclaimed → the read returns partial/empty data. The `snapshot` rows
themselves are not GC'd, so the id/timestamp still *resolves* but under-reads.
**Documented as a known limitation**; a new `docs/FUTURE.md` item
(`fut-timetravel-retention-guard`, "reject as-of reads that resolve outside the
retention window with a clear `410`/`404`") records the follow-up. Not enforced
in this slice.

## Testing

- **`engine-serving`** integration (fixture): land a table twice (snapshot S1
  then S2); `build_serving_provider(table, Some(S1))` yields only the
  first-write rows; `None` yields the S2 rows. Empty/absent-at-snapshot table
  returns no provider (existing contract).
- **`control-plane/testkit`** contract: `snapshot_as_of` semantics across memory
  + postgres (exact hit, between, before-first ⇒ `None`).
- **`postgres`** `sqlx-cache-check`: the new `query!` has a committed `.sqlx`
  entry (freshness enforced by the existing test).
- **query-api e2e** (`e2e-support`): object read `?as_of_snapshot=` and
  `?as_of=` return the historical row set; both-params ⇒ `400`; unknown/not-live
  id ⇒ `404`; unparseable ts ⇒ `400`; no selector ⇒ live rows (unchanged).
  Dataset-detail as-of returns the historical snapshot id/time + schema.
- All new tests are `rust_test` integration targets (no inline `#[cfg(test)]`),
  fixture tests via `loom_fixture_test`.

## Rollout / docs

- Update `docs/system-capabilities/` (query-api read path) to document the
  as-of selector.
- Register the new capability item and close-out via `loom-docs-update`:
  add `road-timetravel-reads` (this slice) and `fut-timetravel-retention-guard`
  (deferred follow-up); note `fut-iceberg-time-travel-schema` remains the
  schema-as-of follow-up.
