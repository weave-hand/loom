# Flight `do_get` must reject ticket files outside the table's live snapshot

- **Date:** 2026-06-29
- **Area:** iceberg
- **Register item:** [[iss-flight-ticket-path-unchecked]]
- **Status:** spec (ready for a work agent to plan + build)

## Problem

The engine's Arrow Flight data plane ([[road-engine-wire-flight]]) decodes a
`FlightTicket { schema, name, files }` and streams the named files back. In
`do_get` (`src/services/engine/src/flight.rs`, the file-ticket branch ~line 135):

```rust
let req = FlightTicketReq::decode(ticket)?;
let table = TableRef { schema: req.schema, name: req.name };
let (_schema, batches) = read_files_as_batches(&self.catalog, &table, &req.files).await?;
```

`read_files_as_batches` (`src/control-plane/postgres/src/iceberg_read.rs`) loads the
table **only for its Arrow schema**, then reads each ticket-named `path` verbatim via
`tbl.file_io().new_input(path)`. **Nothing checks that those paths belong to the loaded
table's live snapshot.** The mirror stores absolute `file://` paths, so a crafted ticket
could name a data file from another table — or any path the warehouse `FileIO` can
resolve — and the engine would stream it.

Today this is low-risk and **not** a live exploit: the plane is an internal-only UDS
with no untrusted clients, `FileIO` is scoped to the warehouse storage factory, and the
sole consumer ([[road-compaction-job]]) derives `files` from the live snapshot itself.
This spec is **defense-in-depth**: make the engine enforce the invariant its callers
already honor, so the boundary cannot regress into a real cross-table read if a future
caller (or bug) supplies an unchecked path.

## Design

Cross-check every ticket-named path against the table's **live-snapshot** file set
before reading any bytes, and reject an unknown path with `Status::invalid_argument` —
the failure mode already used throughout `do_get` for malformed tickets.

### Where the live file set comes from

The Flight service already holds both catalogs:

```rust
pub catalog: SqlCatalog,            // used by read_files_as_batches (byte reader)
pub serving_catalog: IcebergCatalog // the mirror read-adapter
```

`IcebergCatalog` exposes exactly the needed pair (already used in `service.rs:148-150`):

- `current_snapshot(&table)` (the `core::Catalog` trait method) → the live `SnapshotId`;
- `files_with_stats(&table, snap.id)` → `Vec<FileWithStats>`, each carrying
  `path: String`.

Both `files_with_stats(...).path` and `ticket.files` are mirror path strings in the
**same encoding** (absolute, as stored), so a `HashSet<String>` membership compare on the
exact strings is correct — no normalization needed. (Worth a one-line code comment
recording that the two share the mirror's encoding.)

### The guard

In `do_get`'s file-ticket branch, after decoding `req` and before
`read_files_as_batches`:

1. `snap = serving_catalog.current_snapshot(&table)` — map a `NotFound` table to
   `Status::invalid_argument` (an unknown table is a bad ticket, and stays no-leak —
   it never reveals existence beyond "rejected").
2. `live: HashSet<String> = serving_catalog.files_with_stats(&table, snap.id)` mapped to
   `.path`.
3. For each `p` in `req.files`, if `!live.contains(p)` → return
   `Status::invalid_argument("flight ticket names a file outside the table's live snapshot")`
   (do **not** echo the offending path back, to avoid confirming what paths exist).
4. Otherwise proceed to `read_files_as_batches` unchanged.

Empty `req.files` stays valid (vacuously passes; returns the table schema and zero
batches, as today).

### Testable seam

Factor the set check into a small pure helper so the core decision is unit-testable
without a live wire, e.g.:

```rust
/// Err(()) if any requested path is not in the live set. Pure; no I/O.
fn all_in_live_set(live: &HashSet<String>, requested: &[String]) -> Result<(), ()>;
```

`do_get` maps its `Err` to `Status::invalid_argument`. The catalog round-trip
(`current_snapshot` + `files_with_stats`) stays in `do_get`, which already does
async catalog work.

`read_files_as_batches` is left unchanged — it remains a pure byte reader; authorization
lives at the wire boundary, mirroring how the SQL plane authorizes before executing.

## Scope

In scope:

- The membership guard in `do_get`'s file-ticket branch (`engine/src/flight.rs`) using
  `serving_catalog`'s `current_snapshot` + `files_with_stats`.
- The pure `all_in_live_set` helper + its unit test.
- The integration test below.

Out of scope:

- The k-NN ticket branch (`FlightTicketReq` is disjoint from the vector-search ticket;
  the vector plane derives its own files engine-side and is not path-driven by the
  client).
- Any change to `FlightTicket`'s shape, the compaction caller, or `read_files_as_batches`.
- Snapshot-isolation / time-travel tickets (the live snapshot is the only valid set
  here; a ticket cannot request a historical snapshot's files).

## Testing

Two levels, per loom's testing rules (`rust_test` integration targets, no inline
`#[cfg(test)]`):

1. **Pure unit test** (a plain `rust_test`, no fixture) for `all_in_live_set`: a subset
   passes; a superset / disjoint path fails; empty-requested passes.
2. **Live-engine integration test** (`loom_fixture_test`, hermetic Postgres + warehouse)
   driving `do_get` over the UDS. Reuse the live-engine harness from
   `src/services/engine/tests/flight_sql.rs` (which already boots the engine's Flight
   server) — or a sibling `flight_ticket_membership.rs`:
   - Land table **A** (capture its real file paths from `files_with_stats`) and a second
     table **B**.
   - **Positive:** a `FlightTicket` for A naming A's live files streams the expected rows
     (regression guard — the happy path is unbroken).
   - **Negative:** a `FlightTicket` for A naming **B's** file path (or a bogus path)
     fails with `Status::invalid_argument` and streams nothing.

The negative case is the one that fails on `main` today.

## Risk

- Behavior is unchanged for the only real caller (compaction derives `files` from the
  live snapshot, so every path is in-set); the guard is invisible to it.
- One extra catalog round-trip per `do_get` (`current_snapshot` + `files_with_stats`) —
  the same pair the serving read path already issues per query, negligible against the
  Parquet reads that follow.
- The reused failure mode (`Status::invalid_argument`) and the no-path-echo rejection
  keep the change small and leak-free; no new error kinds, no proto/wire change.
