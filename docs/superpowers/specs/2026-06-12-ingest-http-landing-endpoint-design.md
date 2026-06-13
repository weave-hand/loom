# Design: ingest HTTP landing endpoint

> **Status:** approved design (2026-06-12). First sub-slice of the **ingest service shell**
> (the networked surface over the in-process land pipeline). Makes loom's landing path
> reachable over an HTTP wire contract: a client POSTs Arrow data for a target table, the
> server decodes it and drives the existing `materialize` orchestrator (gate →
> schema-select → write Parquet → put → atomic snapshot+lineage commit), and returns the
> new snapshot id. **Defers:** the runnable binary + runtime wiring (slice 2), the
> DataFusion ingestion compute path (slice 3), the Quack endpoint (slice 4), and write-path
> ACL governance (its own slice).

## Goal

`ingest` is a pure library today: `materialize(cp, store, req) -> SnapshotId` orchestrates
the whole land pipeline in-process, but nothing exposes it over a wire. This slice adds the
**HTTP landing endpoint** — an axum `router()` + `land` handler + `AppState` — that decodes
an Arrow IPC request into what `materialize` consumes and returns a structured response. It
mirrors `query-api`'s already-built `http.rs`/`http_smoke` exactly (router + handler tested
in-process via tower `oneshot`, no socket, no binary). After this slice, "land data into a
governed DuckLake table" is reachable through an HTTP contract instead of only a Rust call.

## Why this shape

- **`materialize` is already the seam.** Its signature —
  `materialize(cp: &dyn ControlPlane, store: &dyn ObjectStore, req: MaterializeRequest) ->
  Result<SnapshotId, IngestError>` — is exactly a request handler's body. The shell's only
  job is decode-request → build `MaterializeRequest` → map `Result` to HTTP.
- **The facade earns its keep.** `AppState` holds one `Arc<dyn ControlPlane>` (the object-safe
  facade from the prior slice) plus `Arc<dyn ObjectStore>` — the adapter-agnostic holder the
  facade was built for.
- **Hermetic by construction.** `MemoryControlPlane` + `object_store::memory::InMemory` drive
  the entire pipeline with no Postgres/DuckDB, so the smoke test is a pure in-process
  `oneshot` (the memory adapter already proves `create_table`+`append_files`+`commit` via the
  testkit snapshot contract).

## Module layout

```
src/services/ingest/src/
  http.rs        # NEW: AppState, router(), the land handler, wire DTOs, error mapping
  lib.rs         # add `pub mod http;`
src/services/ingest/tests/
  http_land.rs   # NEW: hermetic land smoke (memory CP + in-memory object store)
```

No binary and no `main.rs` in this slice — `router()` is the deliverable, tested via
`oneshot`. (This matches `query-api`'s built state: real `http.rs`, stub `main.rs`.)

## The wire contract

### Request

```
POST /datasets/:schema/:table
Content-Type: application/vnd.apache.arrow.stream   (advisory; body is read as Arrow IPC regardless)
Body: an Arrow IPC *stream* (schema message + record-batch messages)

Optional headers:
  X-Loom-Model:  JSON {"columns":[{"name":"id","ty":"int64","required":true}, ...]}
  X-Loom-Run-Id: a UUID for the lineage run (absent → server generates one; present-but-not-a-UUID → 400)
```

- **Path** `:schema`/`:table` → `TableRef { schema, name }`. (Same two-segment identity the
  catalog uses; loom identifiers contain no `.`.)
- **Body** is decoded with `arrow::ipc::reader::StreamReader`: `reader.schema()` gives the
  `Arc<Schema>`, iterating yields `Vec<RecordBatch>`. Both feed `MaterializeRequest`.
- **`X-Loom-Model`** (optional) is parsed by an http-owned DTO into a `ModelShape` gate. The
  gate types (`ModelShape`/`ColumnShape`) deliberately do **not** derive serde — the HTTP
  layer owns its wire representation and converts, keeping the library domain types
  serde-free (the same separation `query-api` keeps between core types and `render`).

  ```rust
  // http.rs — inbound wire DTO, converts into the library's ModelShape
  #[derive(serde::Deserialize)]
  struct LandModel { columns: Vec<LandColumn> }
  #[derive(serde::Deserialize)]
  struct LandColumn { name: String, ty: String, required: bool }
  impl From<LandModel> for ModelShape { /* map columns -> ColumnShape */ }
  ```

  Absent header → un-modeled landing (schema inferred). Present-but-invalid JSON → `400`.

### Server-derived values (the client does not send these)

- **File name:** `part-<uuid>.parquet` (the client must not dictate storage keys).
- **`LineageEvent`** (built in the handler; `materialize` requires a caller-built event):
  - `run_id`: from `X-Loom-Run-Id` if present, else `RunId(Uuid::new_v4())`.
  - `event_type`: `EventType::Complete` (a completed landing).
  - `event_time`: `OffsetDateTime::now_utc()`.
  - `outputs`: `vec![DatasetId::from(&table).dataset_ref()]` — the canonical loom identity
    for the landed table (reuses the `DatasetId` newtype).
  - `inputs`: `vec![]` — the HTTP client is not a catalogued source. Naming external
    upstreams is deferred (consistent with `DatasetRef`'s "datasource-derived namespace,
    owned by services" design); the landed output is still recorded.
  - `payload`: `serde_json::json!({"source": "http-land"})`.

### Response

- **`200 OK`**, `application/json`:
  ```json
  { "snapshot_id": 3, "dataset": "main.customer" }
  ```
  (`snapshot_id` is the `SnapshotId(i64)` inner value; `dataset` is `"<schema>.<table>"`.)
- **Errors** (map decode failures + `IngestError`):

  | Condition | Status | Body | Rationale |
  |---|---|---|---|
  | Body is not valid Arrow IPC | `400` | `"invalid arrow ipc stream"` | Client error; safe, no internals |
  | `X-Loom-Model` present but not valid JSON | `400` | `"invalid X-Loom-Model"` | Client error |
  | `X-Loom-Run-Id` present but not a UUID | `400` | `"invalid X-Loom-Run-Id"` | Client error |
  | `IngestError::DoesNotConform(violations)` | `422` | `{"violations":[{"column":..,"reason":..}]}` | Client's data/model mismatch — safe and useful to echo |
  | `IngestError::Infer` (Arrow type loom can't land) | `400` | `"unsupported column type"` | Client's schema; safe generic |
  | `IngestError::{Write,Store,ControlPlane,NoSnapshot}` | `500` | `"internal error"` | **Opaque** — never echo backend detail (mirrors `query-api`'s policy) |

  The `422` body is built inline from `ViolationReason` (no serde on the domain enum):
  `MissingRequired` → `{"column":c,"reason":"missing_required"}`;
  `TypeMismatch{expected,found}` → `{"column":c,"reason":"type_mismatch","expected":..,"found":..}`;
  `Unsupported` → `{"column":c,"reason":"unsupported"}`.

## `AppState`

```rust
#[derive(Clone)]
pub struct AppState {
    pub cp: Arc<dyn ControlPlane>,        // the object-safe facade
    pub store: Arc<dyn ObjectStore>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/datasets/:schema/:table", axum::routing::post(land))
        .with_state(state)
}
```

The handler builds `MaterializeRequest { table: &table, schema, batches: &batches, file_name:
&fname, gate: model.as_ref(), lineage }` and calls
`materialize(st.cp.as_ref(), st.store.as_ref(), req).await`, then maps the result per the
table above.

## Governance (explicitly out of scope this slice)

No ACL check on the landing path in v1. A write-governance slice (subject plumbing + an
`Action::Write` policy model + `acl().check(...)` before landing, via the facade) is the
natural next governance step and is noted here as the known gap. The endpoint is otherwise
unauthenticated in v1 — acceptable because no binary binds it to a socket yet.

## Testing

`src/services/ingest/tests/http_land.rs` — fully hermetic (`MemoryControlPlane::new(...)` +
`object_store::memory::InMemory`, `tower::ServiceExt::oneshot`, no socket/Postgres/DuckDB).
A small helper encodes an Arrow `RecordBatch` to IPC stream bytes via
`arrow::ipc::writer::StreamWriter`.

1. **Un-modeled land succeeds end-to-end.** Build a batch (`id: Int64, name: Utf8`), POST to
   `/datasets/main/customer` with no model header. Assert `200`; parse the JSON and assert
   `dataset == "main.customer"` and `snapshot_id` is present (an integer). Then **read the
   memory catalog back through the same `cp`** — `cp.catalog().current_snapshot(&table)` (or
   `files(..)`) is non-empty — proving `materialize` actually ran the full
   create_table+append_files+commit through the endpoint, not just returned 200.
2. **Modeled land succeeds.** Same batch + `X-Loom-Model` matching the columns
   (`id: int64 required`, `name: varchar`) → `200`.
3. **Non-conforming model → 422.** `X-Loom-Model` requires a column absent from the batch
   (or a mismatched `ty`) → `422`; body contains a `violations` array naming the offending
   column. (No catalog rows written — assert the table has no current snapshot.)
4. **Garbage body → 400.** POST non-Arrow bytes → `400`.
5. **Bad model header → 400.** Valid Arrow body + `X-Loom-Model: not json` → `400`.

## Dependencies / BUCK

- `//src/services/ingest:ingest` lib deps gain: `//third-party:axum`, `//third-party:tokio`,
  `//third-party:time`, `//third-party:serde_json`, `//third-party:uuid`, `//third-party:serde`
  (for the `Deserialize` DTO), and `//src/control-plane/core:core` (for `ControlPlane`,
  `DatasetId`, `LineageEvent`, etc. — confirm it isn't already inherited).
- **Arrow IPC:** the handler uses `arrow::ipc::reader::StreamReader` and the test uses
  `arrow::ipc::writer::StreamWriter`. Confirm the `//third-party:arrow` alias exposes the
  `ipc` module (the arrow meta-crate enables `ipc` by default); if it does not compile, add
  the `ipc` feature to the arrow dep and re-buckify.
- Test target `http-land` deps: `:ingest`, `//src/control-plane/memory:memory`,
  `//third-party:{arrow,object_store,axum,http-body-util,tower,serde_json,tokio}` (and
  `//src/control-plane/core:core` if the test names core types directly).
- `materialize` runs in-process (no DataFusion), so no new heavy deps.

## Scope / non-goals

- **In:** `http.rs` (`AppState`, `router`, `land` handler, wire DTOs, error mapping); the
  `pub mod http` export; the hermetic `http_land` smoke test; BUCK wiring.
- **Out:**
  - **Binary + runtime wiring** (PG pool, S3/local store config, bind address, `serve`) —
    slice 2; shared with query-api's stub binary.
  - **DataFusion ingestion compute path** — slice 3; `materialize` keeps using the direct
    Arrow→Parquet `write.rs`.
  - **Quack endpoint** — slice 4.
  - **Write-path ACL** — own governance slice (noted above).
  - **Naming external lineage inputs** — `inputs: []` for now; an upstream-source contract is
    a later refinement.
  - **Very wide models** — `X-Loom-Model` is a header; very large column lists could hit
    header-size limits. Documented caveat; a multipart/body-framed model is the upgrade path.
  - **Multiple files / partitioning per request** — one Arrow stream → one Parquet file →
    one append per call.

## Open risks

- **Arrow `ipc` feature.** If `//third-party:arrow` omits `ipc`, the build fails to resolve
  `arrow::ipc`; the fix is a one-line fixup (enable the feature) + re-buckify. Flagged so it's
  a known first-build check, not a surprise.
- **Header-encoded model size** (above) — acceptable for v1's narrow tables.
- **`now_utc()` in the handler** — the lineage `event_time` is wall-clock; fine for a service
  handler (unlike workflow scripts, there is no determinism constraint here).
