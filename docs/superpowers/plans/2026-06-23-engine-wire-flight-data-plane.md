# Arrow Flight Data Plane (engine-wire) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a generic Arrow Flight data plane to loom's `engine` process so a zero-pool worker can stream a table's Parquet file rows out as Arrow record batches — no Postgres connection and no object-store credentials on the worker.

**Architecture:** The `engine` (sole Postgres + object-store owner) gains a second tonic service — an Arrow `FlightService` — served on the **same** Unix-domain socket as the existing `EngineControl` gRPC. A Flight `Ticket` names `{schema, name, files}`; the engine resolves the file set against its iceberg catalog, reads the Parquet via its `FileIO`, and streams the rows back as `FlightData`. A new zero-pool `FlightTableClient` in the shared `engine-wire` crate is the consumer: it dials the same socket, sends the ticket, and reconstructs the exact `RecordBatch`es. This is the foundational data plane (`road-engine-wire-flight`); the queue-driven compaction job that is its first real consumer is a **separate** roadmap item (`road-compaction-job`) and is **out of scope here**.

**Tech Stack:** Rust (edition 2024), tonic 0.14.6, prost 0.14.4, `arrow-flight` 57.3.1 (new dep), the **arrow-57.3.1** stack (the iceberg data chain), `parquet-57.3.1`, the `iceberg` crate's `FileIO`/`SqlCatalog`, buck2 + reindeer for third-party vendoring, `loom_fixture_test` for hermetic Postgres + object-store tests.

## Global Constraints

- **Scope is Layer 1 only (the Flight data plane).** Do NOT add `COMPACT_JOB_KIND`/`CompactJob`, the `EngineControl::CompactTable` RPC, the operator `POST …/compact` endpoint, or any worker compaction dispatch branch — those belong to `road-compaction-job`, a separate item that depends on this one. Spec: `docs/superpowers/specs/2026-06-22-engine-wire-compaction-flight-design.md` §"Layer 1 — Arrow Flight data plane".
- **Arrow version MUST be 57.3.1, not 58.** The engine reads/writes Parquet through the `iceberg` crate, which is pinned to the **arrow-57.3.1** stack (`//third-party:arrow-array` aliases to `:arrow-array-57`; `iceberg` depends on `arrow-array 57.3.1`, `parquet 57.3.1`). The Flight plane carries iceberg-produced `RecordBatch`es, so `arrow-flight` and every arrow/parquet target it touches MUST be the **57** line. Using arrow-58 types will not unify with the iceberg batches and will not compile.
- **Use the explicit `//third-party:parquet-57` target**, NOT `//third-party:parquet` (the default alias points to `:parquet-58`). Likewise prefer the unsuffixed arrow targets only where they already alias to `-57` (`//third-party:arrow-array` → `:arrow-array-57`, `//third-party:arrow-schema` → `:arrow-schema-57` — verify each alias before use; when in doubt use the explicit `-57` suffix).
- **Tests are `rust_test` integration targets only.** No inline `#[cfg(test)]`/`#[test]` in `src/**.rs` (the `no-inline-tests` prek hook fails the build otherwise). Fixture-backed tests (hermetic Postgres/DuckDB/object-store) MUST use the `loom_fixture_test` macro (`src/control-plane/postgres/defs.bzl`), never a bare `rust_test`, or they route to remote execution and fail as root.
- **Worker stays zero-pool.** The Flight client lives in `engine-wire` (already in the worker's dep closure) and MUST NOT pull Postgres into the worker binary's dependency closure. Do not add a `//src/control-plane/postgres` dep to `//src/services/worker:worker-bin`.
- **Minimal lockfile drift.** When adding `arrow-flight`, do NOT run `cargo generate-lockfile` (it re-resolves the whole graph and has twice downgraded `duckdb` 1.10503.1 → 1.10501.0, breaking unrelated DuckLake tests). Use `cargo add … --manifest-path …`, then verify `duckdb` stayed at `1.10503.1` (re-pin with `cargo update -p duckdb --precise 1.10503.1` if it moved), then `./tools/buckify.sh`, then run the **full** `buck2 test //src/...` before finishing.
- **Don't pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to a file and grep it (`buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`).
- **Build with RE available:** `BUILDBUDDY_API_KEY` drives remote execution; fixture tests still run their test command locally via `loom_fixture_test`.

---

## File Structure

| File | Responsibility | Action |
|------|----------------|--------|
| `src/services/engine-wire/Cargo.toml` | Declare `arrow-flight` 57.3.1 (client side) | Modify |
| `src/services/engine/Cargo.toml` | Declare `arrow-flight` 57.3.1 (server side) | Modify |
| `third-party/BUCK` | Generated `arrow-flight` rules (via `buckify.sh`) | Modify (generated) |
| `third-party/fixups/arrow-flight/fixups.toml` | Buildscript decision, only if reindeer warns | Create (contingent) |
| `src/services/engine-wire/src/flight.rs` | `FlightTicket` wire type + `FlightTableClient` (zero-pool consumer) | Create |
| `src/services/engine-wire/src/lib.rs` | `pub mod flight;` + a shared `uds_channel` connector helper | Modify |
| `src/services/engine-wire/src/client.rs` | Reuse the shared `uds_channel` helper (DRY) | Modify |
| `src/services/engine-wire/BUCK` | Add `arrow-flight`, arrow-57 deps to `:engine-wire`; new `flight` unit-test target | Modify |
| `src/control-plane/postgres/src/iceberg_read.rs` | `read_files_as_batches` — read a file set → `(SchemaRef, Vec<RecordBatch>)` via `FileIO` + parquet-57 | Create |
| `src/control-plane/postgres/src/lib.rs` | `pub mod iceberg_read;` + re-export `read_files_as_batches` | Modify |
| `src/control-plane/postgres/BUCK` | Add `parquet-57` / arrow-57 deps if missing; new `iceberg-read` fixture-test target | Modify |
| `src/control-plane/postgres/tests/iceberg_read.rs` | Fixture test: land a file, read it back to exact rows | Create |
| `src/services/engine/src/flight.rs` | `FlightDataService` (`do_get` streams the file set) | Create |
| `src/services/engine/src/lib.rs` | `pub mod flight;` | Modify |
| `src/services/engine/src/main.rs` | Register `FlightServiceServer` on the same tonic server/socket | Modify |
| `src/services/engine/BUCK` | Add `arrow-flight`, arrow-57 deps to `:engine` | Modify |
| `src/services/worker/tests/flight_roundtrip.rs` | e2e: zero-pool client streams a known file set, reconstructs exact rows | Create |
| `src/services/worker/tests/e2e.rs` *(or shared helper)* | `spawn_server` also adds the Flight service | Modify |
| `src/services/worker/BUCK` | New `flight-roundtrip` fixture-test target | Modify |

---

## Task 1: Vendor `arrow-flight` 57.3.1

**Files:**
- Modify: `src/services/engine-wire/Cargo.toml`, `src/services/engine/Cargo.toml`
- Modify (generated): `third-party/BUCK`, `Cargo.lock`
- Create (contingent): `third-party/fixups/arrow-flight/fixups.toml`

**Interfaces:**
- Produces: the buck target `//third-party:arrow-flight` (an alias resolving to the 57.3.1 `cargo.rust_library`), depended on by later tasks.

- [ ] **Step 1: Activate the hermetic toolchain**

```bash
eval "$(./tools/env.sh)"
```

- [ ] **Step 2: Add the dependency to both crates with a minimal lock update**

`arrow-flight` 57.3.1 matches the vendored arrow-57.3.1 / tonic-0.14 / prost-0.14 stack. Use `cargo add` (minimal drift), not `cargo generate-lockfile`:

```bash
cargo add arrow-flight@=57.3.1 --manifest-path src/services/engine-wire/Cargo.toml
cargo add arrow-flight@=57.3.1 --manifest-path src/services/engine/Cargo.toml
```

- [ ] **Step 3: Guard the duckdb pin (CRITICAL)**

```bash
grep -A1 '^name = "duckdb"$' Cargo.lock
```

Expected: `version = "1.10503.1"`. If it moved (e.g. to `1.10501.0`), re-pin:

```bash
cargo update -p duckdb --precise 1.10503.1
```

Also confirm no second major of `tonic`/`prost` entered the lock (arrow-flight 57 must share tonic 0.14 / prost 0.14):

```bash
grep -A1 '^name = "tonic"$' Cargo.lock; grep -A1 '^name = "prost"$' Cargo.lock
```

Expected: only `tonic 0.14.x` and `prost 0.14.x`. If a second version appears, stop — the arrow-flight version is wrong; do not proceed.

- [ ] **Step 4: Regenerate the buck rules**

```bash
./tools/buckify.sh
git diff --stat third-party/BUCK
```

Expected: `third-party/BUCK` gains `arrow-flight` rules (an `http_archive`, a `cargo.rust_library` named `arrow-flight-57` or `arrow-flight`, and an `alias` named `arrow-flight`). If `buckify.sh` warns about an `arrow-flight` build script, create the fixup and re-run:

```bash
mkdir -p third-party/fixups/arrow-flight
printf '[buildscript]\nrun = false\n' > third-party/fixups/arrow-flight/fixups.toml
./tools/buckify.sh
```

(arrow-flight ships pre-generated protocol code by default; the build script only regenerates under the `flight-sql-experimental` feature, which we do not enable — so `run = false` is correct.)

- [ ] **Step 5: Verify the target builds**

```bash
buck2 build //third-party:arrow-flight > /tmp/flight_dep.log 2>&1; grep -E "BUILD SUCCEEDED|BUILD FAILED|error" /tmp/flight_dep.log
```

Expected: `BUILD SUCCEEDED`.

- [ ] **Step 6: Commit**

```bash
git add src/services/engine-wire/Cargo.toml src/services/engine/Cargo.toml Cargo.lock third-party/BUCK third-party/fixups
git commit -m "build(third-party): vendor arrow-flight 57.3.1 for the engine-wire data plane"
```

---

## Task 2: `FlightTicket` wire type

**Files:**
- Create: `src/services/engine-wire/src/flight.rs`
- Modify: `src/services/engine-wire/src/lib.rs`
- Modify: `src/services/engine-wire/BUCK`
- Test: `src/services/engine-wire/tests/flight_ticket.rs` (create)

**Interfaces:**
- Produces: `FlightTicket { schema: String, name: String, files: Vec<String> }` with `encode(&self) -> Vec<u8>` and `decode(bytes: &[u8]) -> Result<FlightTicket, serde_json::Error>`. Consumed by the engine Flight server (Task 4) to decode the request and by the `FlightTableClient` (Task 5) to encode it. `files` holds the data-file path strings exactly as stored in the iceberg mirror / returned by `files_with_stats` (passed verbatim to `FileIO::new_input`).

- [ ] **Step 1: Write the failing test**

Create `src/services/engine-wire/tests/flight_ticket.rs`:

```rust
use engine_wire::flight::FlightTicket;

#[test]
fn ticket_json_round_trips() {
    let t = FlightTicket {
        schema: "wh".into(),
        name: "orders".into(),
        files: vec!["data/loom-abc.parquet".into(), "data/loom-def.parquet".into()],
    };
    let bytes = t.encode();
    let back = FlightTicket::decode(&bytes).expect("decode");
    assert_eq!(t, back);
}

#[test]
fn decode_rejects_garbage() {
    assert!(FlightTicket::decode(b"not json").is_err());
}
```

- [ ] **Step 2: Wire the test target and run it to confirm it fails**

In `src/services/engine-wire/BUCK`, add a `rust_test` target mirroring an existing pure-logic test (e.g. `//src/control-plane/core:page`):

```python
rust_test(
    name = "flight-ticket",
    crate = "flight_ticket",
    srcs = ["tests/flight_ticket.rs"],
    crate_root = "tests/flight_ticket.rs",
    edition = "2024",
    deps = [":engine-wire", "//third-party:serde_json"],
)
```

```bash
buck2 test //src/services/engine-wire:flight-ticket > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```

Expected: build failure / FAIL — `engine_wire::flight` does not exist yet.

- [ ] **Step 3: Implement `FlightTicket`**

Create `src/services/engine-wire/src/flight.rs`:

```rust
//! Arrow Flight wire types and the zero-pool table-stream client.

use serde::{Deserialize, Serialize};

/// What a Flight `Ticket` names: an explicit set of a table's data files to
/// stream. `files` are the data-file path strings exactly as stored in the
/// iceberg mirror (passed verbatim to the engine's `FileIO::new_input`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlightTicket {
    pub schema: String,
    pub name: String,
    pub files: Vec<String>,
}

impl FlightTicket {
    /// JSON-encode for the `Ticket.ticket` bytes.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("FlightTicket is always serializable")
    }

    /// Decode from `Ticket.ticket` bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}
```

Add to `src/services/engine-wire/src/lib.rs` (alongside the existing `pub mod client;`):

```rust
pub mod flight;
```

Ensure `engine-wire`'s library deps include `//third-party:serde` (with derive) and `//third-party:serde_json` in `src/services/engine-wire/BUCK` (add if absent).

- [ ] **Step 4: Run the test to verify it passes**

```bash
buck2 test //src/services/engine-wire:flight-ticket > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: `Tests finished: Pass 2. Fail 0.`

- [ ] **Step 5: Commit**

```bash
git add src/services/engine-wire/src/flight.rs src/services/engine-wire/src/lib.rs src/services/engine-wire/BUCK src/services/engine-wire/tests/flight_ticket.rs
git commit -m "feat(engine-wire): add FlightTicket wire type for the Flight data plane"
```

---

## Task 3: `read_files_as_batches` (engine-side file read)

**Files:**
- Create: `src/control-plane/postgres/src/iceberg_read.rs`
- Modify: `src/control-plane/postgres/src/lib.rs`
- Modify: `src/control-plane/postgres/BUCK`
- Test: `src/control-plane/postgres/tests/iceberg_read.rs` (create)

**Interfaces:**
- Consumes: `SqlCatalog` (the iceberg `Catalog` impl, `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs`), `control_plane_core::TableRef`, the file path strings from a `FlightTicket`.
- Produces:
  ```rust
  pub async fn read_files_as_batches(
      catalog: &SqlCatalog,
      table: &control_plane_core::TableRef,
      files: &[String],
  ) -> control_plane_core::Result<(arrow_schema::SchemaRef, Vec<arrow_array::RecordBatch>)>
  ```
  Loads the iceberg `Table`, reads each listed file's bytes via the table's `FileIO`, decodes Parquet (parquet-57) into `RecordBatch`es, and returns the Arrow schema (the table's current Arrow schema) plus all batches in file order. Consumed by the Flight server (Task 4). An empty `files` slice returns the table schema and zero batches. A path not resolvable by `FileIO` is an error.

- [ ] **Step 1: Write the failing fixture test**

Create `src/control-plane/postgres/tests/iceberg_read.rs`. Mirror the fixture setup of an existing iceberg fixture test (e.g. `tests/iceberg_overwrite.rs`) for booting Postgres + a `SqlCatalog` over a temp warehouse and landing a Parquet snapshot. Land one known batch, capture the resulting file path(s) from `files_with_stats` (or the append result), then read them back:

```rust
// imports + fixture boot mirror tests/iceberg_overwrite.rs
// NOTE: `current_snapshot` is a method of the `control_plane_core::Catalog`
// TRAIT (impl'd on IcebergCatalog), so the test MUST `use control_plane_core::Catalog;`
// for the call below to resolve. `files_with_stats` is inherent and needs no trait import.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_landed_file_back_to_exact_rows() {
    // boot fixture: pool, SqlCatalog `catalog`, TableRef `table`
    // land a known batch (e.g. ids [1,2,3]) via the same append helper
    // iceberg_overwrite.rs uses (append_parquet_snapshot / land_parquet)

    // resolve the live file paths at the current snapshot
    let ice = control_plane_postgres::iceberg_catalog::IcebergCatalog::new(pool.clone());
    let snap = ice.current_snapshot(&table).await.expect("snapshot");
    let files: Vec<String> = ice
        .files_with_stats(&table, snap.id)
        .await
        .expect("files")
        .into_iter()
        .map(|f| f.path)
        .collect();
    assert!(!files.is_empty());

    let (schema, batches) =
        control_plane_postgres::read_files_as_batches(&catalog, &table, &files)
            .await
            .expect("read");

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
    // assert the schema field names match the landed columns
    assert!(schema.fields().iter().any(|f| f.name() == "id"));
}
```

(Use the exact landing helper and column/batch builders that `tests/iceberg_overwrite.rs` already uses — repeat them here; do not invent new ones. Confirm the field name asserted matches that test's seeded schema.)

- [ ] **Step 2: Wire the fixture-test target and confirm it fails**

In `src/control-plane/postgres/BUCK`, add (using `loom_fixture_test`, mirroring the `iceberg-overwrite` target):

```python
loom_fixture_test(
    name = "iceberg-read",
    crate = "iceberg_read",
    srcs = ["tests/iceberg_read.rs"],
    crate_root = "tests/iceberg_read.rs",
    edition = "2024",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:iceberg",
        "//third-party:tokio",
        "//third-party:uuid",
        # plus whatever tests/iceberg_overwrite.rs depends on for landing
    ],
)
```

```bash
buck2 test //src/control-plane/postgres:iceberg-read > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```

Expected: build failure — `read_files_as_batches` does not exist.

- [ ] **Step 3: Implement `read_files_as_batches`**

Create `src/control-plane/postgres/src/iceberg_read.rs`:

```rust
//! Read an explicit set of a table's data files into Arrow record batches.
//!
//! The engine is the data source for the Flight data plane: it owns the
//! object store via the iceberg `FileIO`, so it resolves the file set and
//! decodes the Parquet here. The bytes never touch the worker except as the
//! streamed Arrow batches.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use control_plane_core::{ControlPlaneError, Result, TableRef};
use iceberg::{Catalog, TableIdent};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::iceberg_sql_catalog::catalog::SqlCatalog;

/// Read `files` (data-file paths) of `table` into Arrow batches via the
/// catalog's `FileIO`. Returns the table's current Arrow schema and the
/// batches in file order.
pub async fn read_files_as_batches(
    catalog: &SqlCatalog,
    table: &TableRef,
    files: &[String],
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let ident = TableIdent::from_strs([table.schema.as_str(), table.name.as_str()])
        .map_err(iceberg_err)?;
    let tbl = catalog.load_table(&ident).await.map_err(iceberg_err)?;

    // The table's current Arrow schema — the fidelity contract for the stream.
    let arrow_schema: SchemaRef = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(tbl.metadata().current_schema())
            .map_err(iceberg_err)?,
    );

    let mut batches = Vec::new();
    for path in files {
        let bytes: Bytes = tbl
            .file_io()
            .new_input(path)
            .map_err(iceberg_err)?
            .read()
            .await
            .map_err(iceberg_err)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(parquet_err)?
            .build()
            .map_err(parquet_err)?;
        for b in reader {
            batches.push(b.map_err(arrow_err)?);
        }
    }
    Ok((arrow_schema, batches))
}

fn iceberg_err(e: iceberg::Error) -> ControlPlaneError {
    ControlPlaneError::Backend(e.to_string())
}
fn parquet_err(e: parquet::errors::ParquetError) -> ControlPlaneError {
    ControlPlaneError::Backend(e.to_string())
}
fn arrow_err(e: arrow_schema::ArrowError) -> ControlPlaneError {
    ControlPlaneError::Backend(e.to_string())
}
```

Notes for the implementer:
- Verify the exact `ControlPlaneError` backend variant name and constructor used elsewhere in this crate (e.g. how `iceberg_mirror.rs` maps `iceberg::Error`); reuse the existing `backend(...)`/`iceberg_err(...)` helper rather than re-deriving if one exists in scope.
- Verify `iceberg::arrow::schema_to_arrow_schema` is the correct path in the vendored `iceberg` version; if the helper differs, use the table's stored Arrow schema source that `iceberg_writer.rs` / `iceberg_mirror.rs` already use to obtain a schema. If a schema helper is unavailable, derive the `SchemaRef` from the first batch (`batches[0].schema()`), and for the empty-`files` case build it from the iceberg current schema.
- `bytes::Bytes` — confirm `//third-party:bytes` is a dep (the iceberg `FileIO::read()` returns `Bytes`); add if missing.

Add to `src/control-plane/postgres/src/lib.rs`:

```rust
pub mod iceberg_read;
pub use iceberg_read::read_files_as_batches;
```

In `src/control-plane/postgres/BUCK`, ensure the `:postgres` library deps include `//third-party:parquet-57` (the arrow-57 parquet, NOT `//third-party:parquet`), `//third-party:arrow-array` (→ arrow-57), `//third-party:arrow-schema` (→ arrow-57), and `//third-party:bytes`. Add any missing.

- [ ] **Step 4: Run the fixture test to verify it passes**

```bash
buck2 test //src/control-plane/postgres:iceberg-read > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```

Expected: `Tests finished: Pass 1. Fail 0.`

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_read.rs src/control-plane/postgres/src/lib.rs src/control-plane/postgres/BUCK src/control-plane/postgres/tests/iceberg_read.rs
git commit -m "feat(postgres): read_files_as_batches — decode a data-file set to Arrow batches via FileIO"
```

---

## Task 4: Flight server (`FlightDataService`) on the engine

**Files:**
- Create: `src/services/engine/src/flight.rs`
- Modify: `src/services/engine/src/lib.rs`, `src/services/engine/src/main.rs`
- Modify: `src/services/engine/BUCK`

**Interfaces:**
- Consumes: `FlightTicket` (Task 2), `read_files_as_batches` (Task 3), `SqlCatalog` + `PgPool` (already held by the engine).
- Produces: `FlightDataService { catalog: SqlCatalog, pool: PgPool }` implementing `arrow_flight::flight_service_server::FlightService`; `do_get` decodes the ticket, calls `read_files_as_batches`, and streams the batches (schema-first) as `FlightData`. All other Flight methods return `Status::unimplemented`. Registered on the engine's tonic server via `FlightServiceServer::new(service)`. Consumed over the wire by Task 5's client and exercised by Task 6's e2e.

- [ ] **Step 1: Implement the Flight service**

Create `src/services/engine/src/flight.rs`:

```rust
//! Arrow Flight data plane: stream a table's data-file rows out as Arrow
//! batches. The engine owns Postgres + object store, so it is the data source;
//! the worker is a pure compute client over the wire.

use std::pin::Pin;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use control_plane_core::TableRef;
use control_plane_postgres::read_files_as_batches;
use control_plane_postgres::iceberg_sql_catalog::catalog::SqlCatalog;
use futures::{StreamExt, TryStreamExt};
use sqlx::PgPool;
use tonic::{Request, Response, Status, Streaming};

pub struct FlightDataService {
    pub catalog: SqlCatalog,
    pub pool: PgPool,
}

#[tonic::async_trait]
impl FlightService for FlightDataService {
    type HandshakeStream =
        Pin<Box<dyn futures::Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
    type ListFlightsStream =
        Pin<Box<dyn futures::Stream<Item = Result<FlightInfo, Status>> + Send>>;
    type DoGetStream =
        Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoPutStream =
        Pin<Box<dyn futures::Stream<Item = Result<PutResult, Status>> + Send>>;
    type DoExchangeStream =
        Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoActionStream =
        Pin<Box<dyn futures::Stream<Item = Result<arrow_flight::Result, Status>> + Send>>;
    type ListActionsStream =
        Pin<Box<dyn futures::Stream<Item = Result<ActionType, Status>> + Send>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = FlightTicketReq::decode(request.into_inner())?;
        let table = TableRef {
            schema: ticket.schema,
            name: ticket.name,
        };
        let (_schema, batches) = read_files_as_batches(&self.catalog, &table, &ticket.files)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // FlightDataEncoderBuilder emits a schema message first, then the data —
        // satisfying the stream's schema-fidelity contract.
        let input = futures::stream::iter(batches.into_iter().map(Ok));
        let stream = FlightDataEncoderBuilder::new()
            .build(input)
            .map_err(|e| Status::internal(e.to_string()));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }
    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }
    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }
    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }
    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }
    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }
    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

/// Decode a `FlightTicket` from the request, mapping a bad ticket to
/// `invalid_argument`.
struct FlightTicketReq {
    schema: String,
    name: String,
    files: Vec<String>,
}
impl FlightTicketReq {
    fn decode(t: Ticket) -> Result<Self, Status> {
        let ft = engine_wire::flight::FlightTicket::decode(&t.ticket)
            .map_err(|e| Status::invalid_argument(format!("bad flight ticket: {e}")))?;
        Ok(Self {
            schema: ft.schema,
            name: ft.name,
            files: ft.files,
        })
    }
}
```

Notes for the implementer:
- The exact set of associated `Stream` types and method signatures MUST match the `FlightService` trait in the vendored arrow-flight 57.3.1. Generate the skeleton by letting the compiler tell you the required items (implement `do_get`, stub the rest with `Status::unimplemented`). The list above is the arrow-flight 57 surface; adjust names (`arrow_flight::Result`, `PollInfo`, etc.) to the exact re-exports if they differ.
- Confirm `control_plane_postgres::iceberg_sql_catalog::catalog::SqlCatalog` is the correct public path (it is how `main.rs` imports it today); if `SqlCatalog` is re-exported at the crate root, prefer that.
- Add `pub mod flight;` to `src/services/engine/src/lib.rs`.

- [ ] **Step 2: Register the Flight service on the engine's tonic server**

In `src/services/engine/src/main.rs`, add the Flight service alongside `EngineControlServer` on the **same** server/socket. **`SqlCatalog` is NOT `Clone`** (verified — `catalog.rs:193` derives only `Debug`) and `EngineControlService` consumes its `catalog` by value, so build a **second** `SqlCatalog` for the Flight service from the same props (the same `SqlCatalogBuilder` call). `PgPool` IS `Clone`. Concretely:

```rust
use arrow_flight::flight_service_server::FlightServiceServer;
use engine::flight::FlightDataService;

// ... after building `cp`, the first `catalog`, `pool` ...
let control = EngineControlService {
    cp,
    catalog,                 // moved into the control service
    pool: pool.clone(),
};
// Build a second SqlCatalog for the Flight service (SqlCatalog is not Clone).
let flight_catalog = SqlCatalogBuilder::default()
    .with_storage_factory(Arc::new(LocalFsStorageFactory))
    .load("loom", props.clone())     // same props used for the first catalog
    .await?;
let flight = FlightDataService { catalog: flight_catalog, pool };

Server::builder()
    .add_service(EngineControlServer::new(control))
    .add_service(FlightServiceServer::new(flight))
    .serve_with_incoming_shutdown(incoming, async {
        tokio::signal::ctrl_c().await.ok();
    })
    .await?;
```

Notes:
- Confirm `props` (the catalog config map) is still in scope / cloneable at this point; if the existing code consumed it building the first catalog, hoist a `props.clone()` or rebuild the map. Do NOT add a Postgres connection the Flight service doesn't need beyond the one `PgPool` clone.

- [ ] **Step 3: Update the engine BUCK deps**

In `src/services/engine/BUCK`, add to the `:engine` library and/or `engine-bin` deps: `//third-party:arrow-flight`, `//third-party:arrow-array` (→ 57), `//third-party:futures`, and confirm `//third-party:tonic`. Keep `//src/control-plane/postgres:postgres` (already present — `read_files_as_batches` lives there).

- [ ] **Step 4: Build the engine to verify the service compiles**

```bash
buck2 build //src/services/engine:engine //src/services/engine:engine-bin > /tmp/eng.log 2>&1; grep -E "BUILD SUCCEEDED|BUILD FAILED|error\[" /tmp/eng.log
```

Expected: `BUILD SUCCEEDED`. (Behaviour is proven end-to-end in Task 6; this step is a compile gate.)

- [ ] **Step 5: Commit**

```bash
git add src/services/engine/src/flight.rs src/services/engine/src/lib.rs src/services/engine/src/main.rs src/services/engine/BUCK
git commit -m "feat(engine): serve an Arrow Flight do_get data plane alongside EngineControl"
```

---

## Task 5: Zero-pool `FlightTableClient`

**Files:**
- Modify: `src/services/engine-wire/src/flight.rs` (extend with the client)
- Modify: `src/services/engine-wire/src/lib.rs` (shared `uds_channel` helper)
- Modify: `src/services/engine-wire/src/client.rs` (reuse the helper — DRY)
- Modify: `src/services/engine-wire/BUCK`

**Interfaces:**
- Consumes: `FlightTicket` (Task 2), the engine's `do_get` (Task 4).
- Produces:
  ```rust
  impl FlightTableClient {
      pub async fn connect(socket: impl Into<String>) -> Result<Self>;
      pub async fn fetch(&self, ticket: FlightTicket)
          -> Result<Vec<arrow_array::RecordBatch>>;
  }
  ```
  Dials the engine's UDS, sends the ticket via `do_get`, and reconstructs the `RecordBatch`es from the `FlightData` stream. No Postgres. Consumed by Task 6's e2e.
- Also produces a crate-internal helper `pub(crate) async fn uds_channel(socket: String) -> Result<tonic::transport::Channel>` reused by both `GrpcQueueClient` and `FlightTableClient`.

- [ ] **Step 1: Extract the shared UDS connector**

In `src/services/engine-wire/src/lib.rs` (or a small `conn.rs` module), add:

```rust
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

/// Dial the engine's Unix-domain socket and return a tonic `Channel`.
/// The URI is ignored by the connector; the connector dials the UDS.
pub(crate) async fn uds_channel(socket: String) -> control_plane_core::Result<Channel> {
    Endpoint::try_from("http://[::]:50051")
        .map_err(be)?
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket = socket.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(socket).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .map_err(be)
}
```

**Known repo facts (verified — do not re-derive):**
- There is **no** crate-level `Result` alias in `engine-wire`; modules import `Result` from `control_plane_core`. So `uds_channel` returns `control_plane_core::Result<Channel>` (as above), and so does `FlightTableClient` in Task 5 Step 2.
- `be` is currently a **private** `fn be(...)` inside `src/services/engine-wire/src/client.rs:12` (the error-boxing helper). To call it from `lib.rs`, **promote it to `pub(crate) fn be(...)`** and either move it into `lib.rs` or `use crate::client::be;` from `lib.rs`. Do this as part of this step (it is required, not optional).

Refactor `GrpcQueueClient::connect` in `src/services/engine-wire/src/client.rs` to call `crate::uds_channel(socket).await?` and wrap it in `EngineControlClient::new(channel)` (behaviour-preserving; the existing `e2e` flush test is the regression guard).

- [ ] **Step 2: Write the failing client (compile-gate via Task 6)**

The client has no standalone unit test — it needs a live engine, so it is exercised by the Task 6 e2e. Implement it now; Task 6 is its test.

Extend `src/services/engine-wire/src/flight.rs`:

```rust
use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use futures::TryStreamExt;
use tonic::transport::Channel;

use crate::Result;

/// Zero-pool client for the engine's Arrow Flight data plane. Holds no
/// Postgres connection: it streams a file set's rows from the engine.
#[derive(Clone)]
pub struct FlightTableClient {
    inner: FlightServiceClient<Channel>,
}

impl FlightTableClient {
    /// Connect over the engine's Unix-domain socket.
    pub async fn connect(socket: impl Into<String>) -> Result<Self> {
        let channel = crate::uds_channel(socket.into()).await?;
        Ok(Self {
            inner: FlightServiceClient::new(channel),
        })
    }

    /// Stream the rows of the ticket's file set and collect the batches.
    pub async fn fetch(&self, ticket: FlightTicket) -> Result<Vec<RecordBatch>> {
        let resp = self
            .inner
            .clone()
            .do_get(Ticket {
                ticket: ticket.encode().into(),
            })
            .await
            .map_err(crate::be)?;
        let stream = FlightRecordBatchStream::new_from_flight_data(
            resp.into_inner().map_err(|e| e.into()),
        );
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(|e| crate::be(e))?;
        Ok(batches)
    }
}
```

Notes for the implementer:
- The exact constructor for decoding (`FlightRecordBatchStream::new_from_flight_data` vs `FlightDataDecoder`) and the error mapping for the inbound `tonic::Status` stream MUST match arrow-flight 57.3.1. The decoder expects a stream of `Result<FlightData, FlightError>`; map the inbound `Result<FlightData, tonic::Status>` accordingly (`Status` → `FlightError::Tonic`/`FlightError::ExternalError` per the vendored API). Let the compiler/types guide the exact `.map_err`.
- Map `arrow_flight::error::FlightError` into the engine-wire `Result` error type via the existing `be(...)` helper (it accepts `Box<dyn Error>` / `impl Display`); confirm its signature.

- [ ] **Step 3: Update engine-wire BUCK deps**

In `src/services/engine-wire/BUCK`, add to `:engine-wire`: `//third-party:arrow-flight`, `//third-party:arrow-array` (→ 57), `//third-party:futures`, `//third-party:tower` (for `service_fn`, if not already pulled), and confirm `//third-party:hyper-util`, `//third-party:tokio`, `//third-party:tonic` are present (they are, for the existing client).

- [ ] **Step 4: Build engine-wire and re-run the flush e2e (refactor regression guard)**

```bash
buck2 build //src/services/engine-wire:engine-wire > /tmp/ew.log 2>&1; grep -E "BUILD SUCCEEDED|BUILD FAILED|error\[" /tmp/ew.log
buck2 test //src/services/worker:e2e > /tmp/e2e.log 2>&1; grep -E "Tests finished|FAIL" /tmp/e2e.log
```

Expected: `BUILD SUCCEEDED`; the existing flush e2e still `Pass`es (proves the `uds_channel` refactor preserved `GrpcQueueClient` behaviour).

- [ ] **Step 5: Commit**

```bash
git add src/services/engine-wire/src/flight.rs src/services/engine-wire/src/lib.rs src/services/engine-wire/src/client.rs src/services/engine-wire/BUCK
git commit -m "feat(engine-wire): zero-pool FlightTableClient; share the UDS connector"
```

---

## Task 6: Flight round-trip e2e (acceptance test)

**Files:**
- Create: `src/services/worker/tests/flight_roundtrip.rs`
- Modify: `src/services/worker/tests/e2e.rs` (extend `spawn_server` to also add the Flight service) — or factor a shared `spawn_server` if cleaner
- Modify: `src/services/worker/BUCK`

**Interfaces:**
- Consumes: `FlightTableClient` (Task 5), the engine Flight service (Task 4), the postgres landing helpers (test harness only).
- Produces: the spec's Layer-1 acceptance test — "a worker streams a known file set from the engine over Flight and reconstructs the exact rows (schema + values), no Postgres on the worker."

- [ ] **Step 1: Make the test engine serve the Flight service**

In `src/services/worker/tests/e2e.rs`, find `spawn_server` (it builds an `EngineControlService` and serves it on a UDS). The test already has a `make_catalog(...)` helper that builds a `SqlCatalog` from the fixture DSN — use it to build a **second** catalog for the Flight service (`SqlCatalog` is not `Clone`; see Task 4). Add the Flight service to the same server so both this test and the new one get it:

```rust
// inside spawn_server, where the tonic Server is built. `make_catalog`/the
// existing builder produces each SqlCatalog; PgPool is Clone.
let control_catalog = make_catalog(fx, db).await;   // existing first catalog
let flight_catalog  = make_catalog(fx, db).await;   // second, for Flight
let control = EngineControlService { cp, catalog: control_catalog, pool: pool.clone() };
Server::builder()
    .add_service(EngineControlServer::new(control))
    .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(
        engine::flight::FlightDataService { catalog: flight_catalog, pool },
    ))
    .serve_with_incoming_shutdown(incoming, shutdown_fut)
```

(Match the exact `make_catalog`/builder call already in `e2e.rs`. If `spawn_server` is private to `e2e.rs`, make the new test reuse it by moving it into a shared `tests/support`-style module wired as a `rust_library` dep — preferred — otherwise a focused duplicate of the minimal spawn in the new test is acceptable and should be noted in the commit.)

- [ ] **Step 2: Write the round-trip test**

Create `src/services/worker/tests/flight_roundtrip.rs`:

```rust
// Mirror e2e.rs fixture boot: PgFixture, fresh_db, pool_for, spawn_server.
// MUST `use control_plane_core::Catalog;` — `current_snapshot` is a trait method.
use control_plane_core::Catalog;
use engine_wire::flight::{FlightTableClient, FlightTicket};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_streams_a_file_set_and_reconstructs_exact_rows() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let (_sock_dir, sock) = spawn_server(&fx, &db).await;

    let table = TableRef { schema: "wh".into(), name: "t".into() };

    // Land several small files of known rows via the postgres helpers
    // (test harness has Postgres). Reuse the landing helper e2e.rs / the
    // iceberg fixture tests use; land ids [1,2,3] then [4,5] in two files.
    // ... land ...

    // Resolve the live file paths (test-side, has Postgres).
    let ice = control_plane_postgres::iceberg_catalog::IcebergCatalog::new(pool.clone());
    let snap = ice.current_snapshot(&table).await.expect("snapshot");
    let files: Vec<String> = ice
        .files_with_stats(&table, snap.id)
        .await
        .expect("files")
        .into_iter()
        .map(|f| f.path)
        .collect();
    assert!(files.len() >= 1);

    // Zero-pool client (no Postgres) streams the rows over Flight.
    let client = FlightTableClient::connect(&sock).await.expect("connect");
    let batches = client
        .fetch(FlightTicket {
            schema: table.schema.clone(),
            name: table.name.clone(),
            files,
        })
        .await
        .expect("flight fetch");

    // Reconstruct: exact row count and schema fidelity.
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 5);
    assert!(batches[0].schema().fields().iter().any(|f| f.name() == "id"));
    // Optionally assert the concatenated `id` column equals [1,2,3,4,5].
}
```

(Use the exact landing helper, `TableRef`, column builders, and `PgFixture`/`spawn_server` imports that `e2e.rs` already uses — repeat them; do not invent new fixtures. Match the seeded schema's column name in the assertion.)

- [ ] **Step 3: Wire the fixture-test target**

In `src/services/worker/BUCK`, add a `loom_fixture_test` mirroring the existing `e2e` target, adding `//third-party:arrow-array` (→ 57) for the batch assertions and keeping `//src/services/engine:engine`, `//src/services/engine-wire:engine-wire`, `//src/control-plane/postgres:postgres`, `//third-party:arrow-flight` if referenced:

```python
loom_fixture_test(
    name = "flight-roundtrip",
    crate = "flight_roundtrip",
    srcs = ["tests/flight_roundtrip.rs"],   # plus the shared support module if factored
    crate_root = "tests/flight_roundtrip.rs",
    edition = "2024",
    deps = [
        ":worker",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/control-plane/worker:worker",
        "//src/services/engine:engine",
        "//src/services/engine-wire:engine-wire",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:iceberg",
        "//third-party:tempfile",
        "//third-party:tokio",
        "//third-party:tokio-stream",
        "//third-party:tonic",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 4: Run the e2e to verify it passes**

```bash
buck2 test //src/services/worker:flight-roundtrip > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|panicked" /tmp/t.log
```

Expected: `Tests finished: Pass 1. Fail 0.`

- [ ] **Step 5: Full suite + commit**

Run the whole suite (the dependency change in Task 1 makes a per-crate green insufficient — see Global Constraints):

```bash
buck2 test //src/... > /tmp/all.log 2>&1; grep -E "Tests finished|FAIL" /tmp/all.log
```

Expected: all `Pass`, `Fail 0` — in particular the `query-api`/`worker` DuckLake fixture tests (the canary for an accidental duckdb downgrade) stay green.

```bash
git add src/services/worker/tests/flight_roundtrip.rs src/services/worker/tests/e2e.rs src/services/worker/BUCK
git commit -m "test(worker): Flight round-trip e2e — zero-pool client reconstructs a streamed file set"
```

---

## Acceptance Criteria (from the spec, Layer-1 subset)

This plan delivers the Layer-1 (Flight data plane) portion of the spec's acceptance criteria:

- **AC4:** The Arrow Flight data plane is a reusable engine-wire component (generic `FlightTicket`/stream), introduced here — Tasks 2, 4, 5.
- **AC2 (data-plane half):** The consumer holds **no** Postgres connection and streams file data over Arrow Flight from the engine — Tasks 5, 6 (the e2e proves the client path is zero-pool).
- **AC5:** `buck2 test //src/...` is green; the existing flush vertical is unchanged — Task 5 Step 4 (flush e2e regression) and Task 6 Step 5 (full suite).

Criteria AC1 (operator enqueue → compaction), AC2's commit half (`CompactTable`), and AC3 (conflict/retry) belong to `road-compaction-job` and are explicitly **out of scope** here.

## Self-Review

**1. Spec coverage (Layer 1):** Ticket (`{schema,name,files}`) → Task 2. Server resolves file set + reads object store + streams Arrow → Tasks 3+4. Schema fidelity (stream carries the table's Arrow schema) → Task 4 (`FlightDataEncoderBuilder` emits schema first) + Task 3 (returns the table schema). Reusable/generic data plane → Tasks 2/4/5 (no compaction coupling). Worker as zero-pool Flight client → Task 5. Flight round-trip test → Task 6. Layer 2 (compaction job, operator endpoint, CompactTable RPC) correctly excluded per Global Constraints.

**2. Placeholder scan:** No "TBD"/"add error handling"/"similar to Task N" — each task carries concrete code, exact buck targets, and exact commands. The few "verify the exact API name against the vendored crate" notes are deliberate guardrails for version-specific arrow-flight/iceberg surface, each with a concrete fallback, not placeholders for missing design.

**3. Type consistency:** `FlightTicket { schema, name, files: Vec<String> }` is identical across Tasks 2/4/5/6. `read_files_as_batches(&SqlCatalog, &TableRef, &[String]) -> Result<(SchemaRef, Vec<RecordBatch>)>` is defined in Task 3 and consumed verbatim in Task 4. `FlightTableClient::{connect, fetch}` defined in Task 5 and used in Task 6. `uds_channel(String) -> Result<Channel>` defined and consumed within Task 5. Arrow stack is 57 throughout (Global Constraints); `parquet-57` not `parquet`.
