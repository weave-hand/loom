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

### Wire — a dedicated as-of read plane

The object read runs on the **plain** Flight SQL plane
(`EngineServingClient::fetch_rows` → `FlightSqlClient::execute` → standard
`CommandStatementQuery`/`TicketStatementQuery` → engine `do_get_sql` →
`execute_query_stream`). That standard command struct is loom's future *external*
SQL interop surface and carries only a query string — it must not be polluted
with a loom-specific field. `GovernedStatementQuery` is a *different* plane, used
only by `flight_export.rs`, not by the object read.

So as-of rides a **new loom-native JSON ticket**, parallel to the existing
`VectorSearchTicket`/`FlightTicket` (which bypass `CommandStatementQuery` and go
single-hop straight to `do_get`):

```rust
// engine-wire/src/flight.rs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AsOfStatementQuery {
    pub sql: String,
    pub as_of_snapshot: i64,
}
// + a new EngineTicket::AsOfSql(AsOfStatementQuery) variant, decoded after the
//   protobuf ticket and disjoint from the other JSON shapes by deny_unknown_fields.
```

The no-as-of hot path is **byte-identical** to today — the new ticket is used
only when a selector is present.

### Engine

- `build_serving_provider`, `register_iceberg_table`, and `execute_query_stream`
  (`engine-serving/src/serving.rs`) each gain `at: Option<SnapshotId>`.
  `execute_query_stream`'s existing caller (`do_get_sql`) passes `None`
  (unchanged). In `build_serving_provider`, `None` ⇒ `current_snapshot(table).id`
  (today's path); `Some(id)` ⇒ use `id` directly and thread it into the existing
  `catalog.schema(table, id)` / `files_with_stats(table, id)` /
  `build_inline_provider(…, id, …)`. Because `execute_query_stream` registers
  **every** live table, a table not live at `at` (created after the requested
  snapshot) must be **skipped** (`catalog.schema` returns `NotFound` ⇒ return
  `Ok(None)` when `at.is_some()`), never error — a query referencing it then
  surfaces as a plan-time unknown table (`400`).
- `engine/src/flight.rs`: a new `do_get` dispatch arm for `EngineTicket::AsOfSql`
  → `do_get_as_of_sql(q)` → `execute_query_stream(catalog, &q.sql,
  Some(SnapshotId(q.as_of_snapshot)), store)`. `do_get_sql` is unchanged.
- Schema is the mirror schema *at the resolved snapshot* (which today equals the
  current schema — no schema evolution yet), consistent for files + inline.

### query-api serving seam + client

- `ServingEngine::fetch_rows(&self, sql, params, at: Option<SnapshotId>)`
  (`serving.rs`): add the `at` param. Every existing call site passes `None`
  except the object read. `EngineServingClient::fetch_rows` sends the new
  `AsOfStatementQuery` ticket (single-hop `do_get`) when `at.is_some()`, else the
  unchanged `FlightSqlClient::execute`.
- `QueryDeps` (`handler.rs`) gains `catalog: &dyn Catalog` (from `st.cp.catalog()`
  in `st.deps()`), so the read path can resolve/validate the selector once the
  type's backing table is known.

### query-api HTTP + resolution

- `AsOfSelector` enum (`{ Snapshot(i64), Time(OffsetDateTime) }`): a *parsed but
  unresolved* selector. `ObjectQuery` gains `as_of: Option<AsOfSelector>`.
- `get_object` (`http.rs`): `as_of` / `as_of_snapshot` become reserved query
  keys; parse to `AsOfSelector`; both-present ⇒ `400`; non-integer id or
  unparseable RFC3339 ⇒ `400`.
- Resolution (in the read path, where `g.otype.table` and `deps.catalog` are
  available): `Time(ts)` → `catalog.snapshot_as_of(table, ts)` (`None` ⇒ `404`);
  `Snapshot(id)` → validate liveness via `catalog.schema(table, id)` (`NotFound`
  ⇒ `404`). The resolved `SnapshotId` flows into `fetch_rows(…, Some(id))`.
- Dataset-detail (`get_dataset`, catalog-only — no engine): parse the same
  selector, resolve it (same rules), and report that snapshot's id/time + its
  schema instead of `current_snapshot`.
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
