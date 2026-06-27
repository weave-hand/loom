# Governed Arrow Flight Export Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Give query-api a second listener — an external, authenticated Arrow Flight
server — that streams a **governed typed-object slice** out as columnar Arrow (vectors
carried natively as `List<Float32>`), bypassing the `LIMIT 1000` JSON / `SqlValue` path.

**Architecture:** query-api hosts a **governing Flight proxy**. The Flight ticket/command
carries a loom `ExportCommand` (JSON: `{type, filters?, ids?}`) — **not SQL**. `get_flight_info`
authenticates, governs, and returns the projected Arrow schema + a ticket. `do_get`
authenticates, **re-derives the governed SQL for that subject** (reusing the exact governance
the HTTP read path uses), runs it over the engine's existing internal Flight-SQL (UDS) via a
new **streaming** client method, and re-encodes the `RecordBatch` stream straight out. Re-governing
per `do_get` (the ticket is a command, not SQL) is the load-bearing security property: a forged or
replayed ticket is just another export request, governed for *that call's* authenticated subject.

**Tech Stack:** Rust 2024, arrow-flight 58, tonic, axum (existing HTTP), the loom
`compile_select_with` SQL governance, bearer-token auth (`service_runtime::auth`), buck2.

**Spec:** `docs/superpowers/specs/2026-06-26-governed-flight-export-design.md`
(register item `road-governed-flight-export`).

---

## ⚠️ Build/verify reality for this repo

**This macOS arm64 host CANNOT compile loom locally** (the cpython toolchain in
`toolchains/BUCK` is linux-only — buck2 fails at target-graph config). **CI is the only
compiler.** Every "run the test" step below means: commit, push the `work/road-governed-flight-export`
branch, and read the BuildBuddy `affected` lane (`gh pr checks`). The TDD ordering (test
first) is preserved, but the fail→pass loop is observed in CI, not locally. Keep pushes small
so a red lane points at one change. The **stricter clippy** gate (`#200`: pedantic + restriction
tree-wide) applies to all new code — no `unwrap`/`expect`/`panic`/`indexing` in production paths;
use `#[expect(lint, reason = "…")]` locally or the test-lint allowlist in test files.

---

## Architecture decisions baked into this plan

1. **The export server holds a raw `FlightSqlClient`, not a `ServingEngine`.** The HTTP read
   path flattens batches to scalar `Rows` (`EngineServingClient` → `batches_to_rows`), which has
   no list variant and would stringify a vector. The export path must keep `RecordBatch`es, so it
   talks to the engine through a new streaming method on `engine_wire::flight::FlightSqlClient`
   and never touches `ServingEngine`/`SqlValue`.

2. **Governance is extracted, not duplicated.** `handler::read_object` is split into
   `compile_object_read` (auth-subject + ACL + ontology → governed `(sql, params, columns,
   logical_types)`) and a thin `read_object` that executes + assembles `ObjectRows`. The export
   path calls `compile_object_read` with a different `limit`, then streams instead of flattening.
   Identical SQL, identical ACL — no second governance implementation to drift.

3. **The row cap is an explicit error, never a silent truncation.** `compile_object_read` is
   called with `limit = max_rows + 1`; the outgoing stream is wrapped in a row counter that emits
   a stream error once cumulative rows exceed `max_rows`. So an export of ≤ `max_rows` streams in
   full; a larger slice ends with an explicit error after the cap — the consumer never mistakes a
   truncated result for a complete one. (Spec error-handling §.)

4. **The arrow schema mapping lives in query-api, not engine-serving.** query-api is a
   zero-DataFusion crate by design; it must not depend on `engine-serving`. The `BaseType →
   DataType` mapping (including `Vector(N) → List<Float32>`) is reproduced locally in
   `flight_export.rs`, mirroring `engine-serving`'s `base_to_arrow` exactly so the advertised
   schema agrees with the engine's streamed schema.

---

## Task 1: Streaming Flight-SQL client (`execute_stream`)

Add a non-buffering counterpart to `FlightSqlClient::execute` that returns the decoded
`do_get` stream so query-api can forward engine→consumer back-pressured.

**Files:**
- Modify: `src/services/engine-wire/src/flight.rs` (add `execute_stream` to `impl FlightSqlClient`; add imports)
- Verified by: the Task 6 governed-export e2e (this is the path it exercises end-to-end);
  no standalone fixture harness is added in engine-wire to avoid duplicating the live-engine setup.

**Step 1: Add imports**

At the top of `src/services/engine-wire/src/flight.rs`, alongside the existing `use` lines,
ensure these are present:

```rust
use std::pin::Pin;
use futures::Stream; // for the boxed return type; TryStreamExt is already imported
```

**Step 2: Add the method**

Inside `impl FlightSqlClient` (just after `execute`), add:

```rust
    /// Like [`execute`](Self::execute) but returns the decoded `do_get` result as a
    /// **stream** of `RecordBatch`es instead of buffering them into a `Vec`. The caller
    /// (query-api's governed Flight export) re-encodes this stream straight out, so the
    /// engine→consumer path stays back-pressured and a large export never materialises
    /// in query-api's memory. Performs the same `CommandStatementQuery` get_flight_info →
    /// do_get dance as `execute`.
    pub async fn execute_stream(
        &self,
        sql: String,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> {
        let cmd = CommandStatementQuery {
            query: sql,
            transaction_id: None,
        };
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());

        let info = self
            .inner
            .clone()
            .get_flight_info(descriptor)
            .await
            .map_err(crate::client::be)?
            .into_inner();
        let ticket = info
            .endpoint
            .into_iter()
            .next()
            .and_then(|e| e.ticket)
            .ok_or_else(|| crate::client::be("flight info carried no ticket"))?;

        let resp = self
            .inner
            .clone()
            .do_get(ticket)
            .await
            .map_err(crate::client::be)?;
        // Decode the schema-first FlightData stream into RecordBatches, mapping the
        // stream's FlightError items to control-plane errors (same `be` mapping the
        // buffered path uses). The stream owns the (cloned) response, so it is 'static.
        let stream = FlightRecordBatchStream::new_from_flight_data(
            resp.into_inner()
                .map_err(arrow_flight::error::FlightError::from),
        )
        .map_err(crate::client::be);
        Ok(Box::pin(stream))
    }
```

Notes for the implementer:
- `FlightRecordBatchStream` and `FlightDescriptor`, `CommandStatementQuery`, `ProstMessageExt`,
  `Message` are already imported in this file.
- `.map_err` on the stream is `futures::TryStreamExt` (already imported).
- `Result` here is `control_plane_core::Result` (already imported as `Result`).

**Step 3: Verify (CI)**

```bash
git add src/services/engine-wire/src/flight.rs
git commit -m "feat(engine-wire): streaming FlightSqlClient::execute_stream for export"
git push
gh pr checks  # affected lane must compile engine-wire (no behavior change to execute)
```

Expected: `affected` builds engine-wire clean. No test asserts on it yet — Task 6 does.

---

## Task 2: Extract `compile_object_read` from `read_object`

Split the governance-and-compile half of `read_object` out of its execution, so the export
path can reuse the *exact* governed SQL build with a different row limit and without flattening.
Pure refactor: `read_object`'s observable behavior is unchanged (all existing read tests pass).

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (`read_object` → `compile_object_read` + thin `read_object`)
- Tests: existing `governed_read`, `typed_filter_e2e`, `derived_properties_e2e`, `object_set_e2e`,
  `sql_compile` cover behavior preservation (no new test needed for the refactor itself).

**Step 1: Add the `GovernedRead` struct**

Near `ObjectRows` in `handler.rs`, add:

```rust
/// The governed, compiled-but-not-yet-executed form of an object read: the SQL string +
/// positional params, plus the projected output columns and their logical types (in SELECT
/// order), and which of those output columns were **masked**. Shared by `read_object`
/// (executes → `ObjectRows`) and the Flight export path (streams the engine result + builds
/// the Arrow schema from `columns`/`logical_types`/`masked_columns`).
///
/// `masked_columns` is load-bearing for the export schema: a masked column is SELECTed as the
/// constant `'***'` (`sql.rs` `MASK_MARKER`), so the engine streams it back as **Utf8**, not as
/// its declared logical type. The export schema builder must therefore advertise masked columns
/// as `Utf8` — otherwise `get_flight_info`'s schema (e.g. `Float64`/`List<Float32>`) disagrees
/// with the `do_get` data schema and a strict Flight client errors. (See reviewer finding C2.)
#[derive(Debug)]
pub struct GovernedRead {
    pub sql: String,
    pub params: Vec<SqlValue>,
    pub columns: Vec<String>,
    pub logical_types: Vec<String>,
    pub masked_columns: Vec<String>,
}
```

**Step 2: Introduce `compile_object_read`**

Add this function. It is the body of the current `read_object` **up to and including**
`compile_select_with`, plus building `columns`/`logical_types`, but it does **not** call
`fetch_rows`. It takes `ontology`/`acl`/`dialect` explicitly (not a `QueryDeps`) so the
export server — which holds no `ServingEngine` — can call it. It takes an explicit `limit`.

```rust
use control_plane_core::{Aggregation /* if needed */};
use crate::sql::SqlDialect;

/// Govern + compile an object read without executing it. Resolves the type, applies the
/// deny-by-default Read gate, loads row/column policy, projects allowed columns, resolves
/// governed derived (aggregate-over-link) columns, and compiles the SELECT (with `limit`).
/// Returns the SQL + params + projected `columns`/`logical_types` (SELECT order). Shared by
/// the HTTP read path and the Flight export path so governance lives in exactly one place.
pub async fn compile_object_read(
    q: &ObjectQuery,
    subject: &Subject,
    ontology: &(dyn Ontology + Send + Sync),
    acl: &(dyn Acl + Send + Sync),
    dialect: &dyn SqlDialect,
    limit: u32,
) -> Result<GovernedRead, QueryError> {
    // … move the existing read_object body here, replacing:
    //   deps.ontology  -> ontology
    //   deps.acl       -> acl
    //   deps.serving.dialect() -> dialect
    //   DEFAULT_LIMIT (in the compile_select_with call) -> limit
    // … and instead of `deps.serving.fetch_rows(...)` + ObjectRows, end with:
    //   compute masked output columns = output columns that are in the policy `masked` set
    //   (covers both physical masked columns and masked derived columns):
    let masked_columns: Vec<String> =
        columns.iter().filter(|c| masked.contains(*c)).cloned().collect();
    Ok(GovernedRead { sql, params, columns, logical_types, masked_columns })
}
```

Concretely, the moved body is lines `handler.rs:181`–`328` (everything from `let type_name = …`
through building `logical_types`), with the four substitutions above. The block that derives
`columns` (`let mut columns = allowed.clone(); columns.extend(derived_names…)`) and
`logical_types` moves in too. Drop the `let served = …` line (currently `handler.rs:308`,
interleaved before the `columns`/`logical_types` builds — `columns`/`logical_types` have no data
dependency on `served`, so dropping it is sound) and the `debug_assert_eq!`/`ObjectRows`
construction — those stay in `read_object`. The `masked` `HashSet` is already in scope from
`load_policy` earlier in the moved body — reuse it for `masked_columns`.

**Step 3: Reduce `read_object` to compile + execute**

```rust
pub async fn read_object(
    q: &ObjectQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let g = compile_object_read(
        q,
        subject,
        deps.ontology,
        deps.acl,
        deps.serving.dialect(),
        DEFAULT_LIMIT,
    )
    .await?;
    let served = deps.serving.fetch_rows(&g.sql, &g.params).await?;
    // The serving engine must echo the projected columns in SELECT order — the contract
    // that lets the renderer zip logical_types/columns onto each row's cells by position.
    debug_assert_eq!(
        served.columns, g.columns,
        "serving engine returned columns out of the projected order"
    );
    Ok(ObjectRows {
        columns: g.columns,
        logical_types: g.logical_types,
        rows: served.rows,
    })
}
```

**Step 4: Verify (CI)**

```bash
git add src/services/query-api/src/handler.rs
git commit -m "refactor(query-api): split compile_object_read out of read_object"
git push
gh pr checks  # affected runs governed_read + typed_filter_e2e + derived_properties_e2e etc.
```

Expected: all existing query-api read tests stay green (behavior identical).

---

## Task 3: `ExportCommand` + the export Arrow schema mapping

The wire command and the projected-schema builder. Pure logic — unit-testable with a plain
`rust_test` (no fixture).

**Files:**
- Create: `src/services/query-api/src/flight_export.rs`
- Modify: `src/services/query-api/src/lib.rs` (add `pub mod flight_export;`)
- Create: `src/services/query-api/tests/export_command.rs` (unit test)
- Modify: `src/services/query-api/BUCK` (new `rust_test` target `export-command`; library deps — see Task 5)

**Step 1: Write the failing test** — `tests/export_command.rs`

```rust
use arrow::datatypes::DataType;
use query_api::flight_export::{export_arrow_schema, ExportCommand};

#[test]
fn export_command_json_round_trips() {
    let cmd = ExportCommand {
        type_name: "Chunk".to_string(),
        filters: vec![("sourcebook".to_string(), "PHB".to_string())],
        ids: vec!["c1".to_string(), "c2".to_string()],
    };
    let bytes = cmd.encode();
    let back = ExportCommand::decode(&bytes).expect("decode");
    assert_eq!(back, cmd);
}

#[test]
fn export_command_decodes_minimal() {
    // Only `type` is required; filters/ids default to empty.
    let bytes = br#"{"type":"Chunk"}"#;
    let cmd = ExportCommand::decode(bytes).expect("decode");
    assert_eq!(cmd.type_name, "Chunk");
    assert!(cmd.filters.is_empty());
    assert!(cmd.ids.is_empty());
}

#[test]
fn export_schema_maps_scalars_and_vector() {
    let cols = vec!["id".to_string(), "embedding".to_string()];
    let types = vec!["string".to_string(), "vector(4)".to_string()];
    let schema = export_arrow_schema(&cols, &types, &[]).expect("schema");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    // A vector column is list<float32> with a non-null `element` field.
    match schema.field(1).data_type() {
        DataType::List(f) => {
            assert_eq!(f.data_type(), &DataType::Float32);
            assert!(!f.is_nullable());
        }
        other => panic!("expected List<Float32>, got {other:?}"),
    }
}

#[test]
fn export_schema_advertises_masked_columns_as_utf8() {
    // A masked column streams back as the constant '***' (Utf8), so the advertised schema
    // must say Utf8 too — even though the column's declared type is a vector.
    let cols = vec!["embedding".to_string()];
    let types = vec!["vector(4)".to_string()];
    let schema = export_arrow_schema(&cols, &types, &["embedding".to_string()]).expect("schema");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
}

#[test]
fn export_schema_rejects_unknown_logical_type() {
    let err = export_arrow_schema(&["x".to_string()], &["nonsense".to_string()], &[]);
    assert!(err.is_err());
}
```

**Step 2: Implement** — `src/services/query-api/src/flight_export.rs` (this step)

Add the command type and the schema mapping. (The `FlightService` impl is Task 4 — keep this
file compiling on its own first.)

```rust
//! query-api's governed Arrow Flight **export** surface. A second listener (TCP) that
//! authenticates a bearer-token caller, governs a typed-object slice exactly as the HTTP
//! read path does, and streams the engine's `RecordBatch` result straight out columnar —
//! vectors carried natively as `List<Float32>`, never flattened through `SqlValue`. The
//! Flight ticket/command carries a loom `ExportCommand`, NOT SQL; loom compiles the ACL'd
//! SQL server-side per `do_get`, so a forged/replayed ticket is still a governed request.
//! See docs/superpowers/specs/2026-06-26-governed-flight-export-design.md.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use control_plane_core::{resolve_logical, BaseType};
use serde::{Deserialize, Serialize};

/// The governed slice to export — mirrors `GET /objects/{type}` params. JSON in the Flight
/// descriptor `cmd` (get_flight_info) and the `Ticket` bytes (do_get), mirroring
/// `FlightTicket`'s JSON convention.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportCommand {
    /// The ontology type to export (e.g. "Chunk").
    #[serde(rename = "type")]
    pub type_name: String,
    /// Optional equality filters on allowed columns (validated server-side).
    #[serde(default)]
    pub filters: Vec<(String, String)>,
    /// Optional object-set identity values → an `In` predicate on the declared identity.
    #[serde(default)]
    pub ids: Vec<String>,
}

impl ExportCommand {
    /// JSON-encode for the descriptor `cmd` / `Ticket` bytes.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("ExportCommand is always serializable")
    }
    /// Decode from descriptor `cmd` / `Ticket` bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// Map a loom logical `BaseType` to the canonical Arrow `DataType` — an exact mirror of
/// `engine-serving`'s `base_to_arrow`, reproduced here because query-api must not depend on
/// the DataFusion serving crate. Keeping the two in lockstep ensures the schema advertised
/// by `get_flight_info` matches the schema the engine streams in `do_get`.
fn base_to_arrow(b: BaseType) -> DataType {
    match b {
        BaseType::Integer => DataType::Int32,
        BaseType::Long => DataType::Int64,
        BaseType::Double => DataType::Float64,
        BaseType::Boolean => DataType::Boolean,
        BaseType::String => DataType::Utf8,
        BaseType::Date => DataType::Date32,
        BaseType::Timestamp => DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        BaseType::Vector(_) => {
            DataType::List(Arc::new(Field::new("element", DataType::Float32, false)))
        }
    }
}

/// Build the projected Arrow schema for an export from the governed output columns, their loom
/// logical types (positionally aligned, in SELECT order), and the set of **masked** output
/// columns. Columns are nullable (the read projection does not assert non-null). A **masked**
/// column is advertised as `Utf8` — it is SELECTed as the `'***'` constant, so the engine
/// streams it as Utf8 regardless of its declared type; advertising the declared type would make
/// `get_flight_info`'s schema disagree with the `do_get` data schema. An unknown logical type is
/// an error — the ontology should never hold one.
pub fn export_arrow_schema(
    columns: &[String],
    logical_types: &[String],
    masked_columns: &[String],
) -> Result<SchemaRef, String> {
    if columns.len() != logical_types.len() {
        return Err(format!(
            "export schema: {} columns / {} types (must match)",
            columns.len(),
            logical_types.len()
        ));
    }
    let mut fields = Vec::with_capacity(columns.len());
    for (name, lt) in columns.iter().zip(logical_types) {
        let dt = if masked_columns.iter().any(|m| m == name) {
            DataType::Utf8 // masked → '***' constant streams as Utf8
        } else {
            let base =
                resolve_logical(lt).ok_or_else(|| format!("unknown logical type `{lt}`"))?;
            base_to_arrow(base)
        };
        fields.push(Field::new(name, dt, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}
```

**Step 3: Wire the module + test target**

`lib.rs`: add `pub mod flight_export;` (keep alphabetical: after `filter`, before `handler`).

`BUCK`: add a unit-test target near the other `rust_test`s:

```python
rust_test(
    name = "export-command",
    crate = "export_command",
    srcs = ["tests/export_command.rs"],
    crate_root = "tests/export_command.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//third-party:arrow",
    ],
)
```

**`serde` is a required library dep in this task** (reviewer D2): query-api's library currently
deps only `serde_json`, which does **not** put the `serde` crate in the extern prelude — so
`use serde::{Deserialize, Serialize}` and `#[derive(Serialize, Deserialize)]` on `ExportCommand`
will not resolve. Add `"//third-party:serde",` (it carries the `derive` feature — engine-wire
already deps it for the identical `FlightTicket` derive) to the `query-api` `rust_library` `deps`
in **this** task (the `arrow-flight`/`tonic`/`futures` library deps still land in Task 5 with the
`FlightService` impl). Keep the list sorted.

**Step 4: Verify (CI)**

```bash
git add src/services/query-api/src/flight_export.rs src/services/query-api/src/lib.rs \
        src/services/query-api/tests/export_command.rs src/services/query-api/BUCK
git commit -m "feat(query-api): ExportCommand + export Arrow schema (vector->list<float>)"
git push
gh pr checks  # `export-command` test must pass; library still compiles
```

Expected: 4 assertions green, including `vector(4) → List<Float32>`.

---

## Task 4: `FlightExportService` — the governing Flight server

The `FlightService` impl: bearer-token auth from gRPC metadata, `get_flight_info` (govern →
schema + ticket), `do_get` (govern → stream with row cap), everything else `unimplemented`.

**Files:**
- Modify: `src/services/query-api/src/flight_export.rs` (add the service + auth helper + error mapping)
- Modify: `src/services/query-api/BUCK` (library deps: `arrow-flight`, `tonic`, `futures` — see Task 5)

**Step 1: Add imports to `flight_export.rs`**

```rust
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use control_plane_core::{Auth, ControlPlane, SubjectId};
use engine_wire::flight::FlightSqlClient;
use futures::StreamExt;
use service_runtime::{token_sha256, Subject};
use std::pin::Pin;
use time::OffsetDateTime;
use tonic::{Request, Response, Status, Streaming};

use crate::handler::{compile_object_read, ObjectQuery, QueryError};
use crate::serving::inline_params;
use crate::sql::DataFusionDialect;
```

**Step 2: The service struct + constructor**

```rust
/// The governed Flight export server. Holds the auth seam, the control plane (ACL +
/// ontology), a streaming client to the engine's internal Flight-SQL plane, and the
/// per-export row cap. Spawned only when `LOOM_FLIGHT_BIND_ADDR` is set.
pub struct FlightExportService {
    auth: Arc<dyn Auth + Send + Sync>,
    cp: Arc<dyn ControlPlane>,
    engine: FlightSqlClient,
    /// Hard cap on rows per export (`LOOM_EXPORT_MAX_ROWS`). The governed SQL is compiled
    /// with `LIMIT max_rows + 1`; the outgoing stream errors once it exceeds `max_rows`, so
    /// an over-cap slice fails explicitly rather than truncating silently.
    max_rows: u32,
}

impl FlightExportService {
    pub fn new(
        auth: Arc<dyn Auth + Send + Sync>,
        cp: Arc<dyn ControlPlane>,
        engine: FlightSqlClient,
        max_rows: u32,
    ) -> Self {
        Self { auth, cp, engine, max_rows }
    }
}
```

**Step 3: Auth helper + error mapping (free functions in the module)**

```rust
/// Resolve the bearer token in the gRPC `authorization` metadata to a verified subject.
/// Missing/invalid/expired → `Unauthenticated`; an auth-store fault → `Internal`.
async fn authenticate(
    auth: &(dyn Auth + Send + Sync),
    md: &tonic::metadata::MetadataMap,
) -> Result<SubjectId, Status> {
    let token = md
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
    let hash = token_sha256(token);
    match auth.resolve_session(&hash, OffsetDateTime::now_utc()).await {
        Ok(Some(sid)) => Ok(sid),
        Ok(None) => Err(Status::unauthenticated("invalid or expired token")),
        Err(_) => Err(Status::internal("auth error")),
    }
}

/// Map a governance error to a gRPC status. Mirrors the HTTP mapping but in Flight terms:
/// a denied type is `PermissionDenied` (before existence is revealed); unknown type / bad
/// filter is `InvalidArgument` (same validation as `read_object`); backend faults are
/// `Internal` (no internal detail leaked to the client).
fn map_query_err(e: QueryError) -> Status {
    match e {
        QueryError::Forbidden => Status::permission_denied("forbidden"),
        QueryError::UnknownType(t) => Status::invalid_argument(format!("unknown type: {t}")),
        QueryError::UnknownLink(l) => Status::invalid_argument(format!("unknown link: {l}")),
        QueryError::BadFilter(c) => Status::invalid_argument(format!("filter not permitted: {c}")),
        QueryError::BadFilterValue(e) => Status::invalid_argument(e.to_string()),
        QueryError::NoIdentity(t) => Status::invalid_argument(format!("type has no identity: {t}")),
        // Backend / SQL faults: opaque internal (logged server-side by the caller if needed).
        other => Status::internal(other.to_string()),
    }
}
```

Implementer note: confirm `QueryError` variants referenced exist (they do — see `handler.rs`).
`AmbiguousLink`/`BadChain`/graph variants can't arise from an `ObjectQuery` export, so the
catch-all `other => Internal` covers them; that's acceptable (they're unreachable here).

**Step 4: The `FlightService` impl**

```rust
#[tonic::async_trait]
impl FlightService for FlightExportService {
    type HandshakeStream =
        Pin<Box<dyn futures::Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
    type ListFlightsStream =
        Pin<Box<dyn futures::Stream<Item = Result<FlightInfo, Status>> + Send>>;
    type DoGetStream = Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoPutStream = Pin<Box<dyn futures::Stream<Item = Result<PutResult, Status>> + Send>>;
    type DoExchangeStream = Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoActionStream =
        Pin<Box<dyn futures::Stream<Item = Result<arrow_flight::Result, Status>> + Send>>;
    type ListActionsStream =
        Pin<Box<dyn futures::Stream<Item = Result<ActionType, Status>> + Send>>;

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let subject = authenticate(self.auth.as_ref(), request.metadata()).await?;
        let descriptor = request.into_inner();
        let cmd = ExportCommand::decode(&descriptor.cmd)
            .map_err(|e| Status::invalid_argument(format!("bad export command: {e}")))?;
        let governed = compile_object_read(
            &ObjectQuery {
                type_name: cmd.type_name.clone(),
                eq_filters: cmd.filters.clone(),
                ids: cmd.ids.clone(),
            },
            &Subject(subject),
            self.cp.ontology(),
            self.cp.acl(),
            &DataFusionDialect,
            self.max_rows.saturating_add(1),
        )
        .await
        .map_err(map_query_err)?;
        let schema = export_arrow_schema(
            &governed.columns,
            &governed.logical_types,
            &governed.masked_columns,
        )
        .map_err(Status::internal)?;
        let endpoint = FlightEndpoint::new().with_ticket(Ticket { ticket: cmd.encode().into() });
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema encode: {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);
        Ok(Response::new(info))
    }

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let subject = authenticate(self.auth.as_ref(), request.metadata()).await?;
        let ticket = request.into_inner();
        let cmd = ExportCommand::decode(&ticket.ticket)
            .map_err(|e| Status::invalid_argument(format!("bad export ticket: {e}")))?;
        // Re-derive the governed SQL for THIS authenticated subject (compile with the cap+1
        // so an over-cap slice can be detected, not silently truncated).
        let governed = compile_object_read(
            &ObjectQuery {
                type_name: cmd.type_name,
                eq_filters: cmd.filters,
                ids: cmd.ids,
            },
            &Subject(subject),
            self.cp.ontology(),
            self.cp.acl(),
            &DataFusionDialect,
            self.max_rows.saturating_add(1),
        )
        .await
        .map_err(map_query_err)?;
        let sql = inline_params(&governed.sql, &governed.params);
        let batches = self
            .engine
            .execute_stream(sql)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Row cap: count rows as they stream; once cumulative rows exceed `max_rows`, emit a
        // stream error so the export fails explicitly instead of truncating. Engine/stream
        // faults are surfaced as stream errors too (the consumer sees a failed stream).
        let max = u64::from(self.max_rows);
        let mut seen: u64 = 0;
        let capped = batches.map(move |item| match item {
            Ok(batch) => {
                // num_rows() is usize; widen losslessly to u64 (no truncation possible on any
                // target loom builds for). try_from keeps the restriction-clippy gate happy.
                seen += u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
                if seen > max {
                    Err(FlightError::from_external_error(Box::new(std::io::Error::other(
                        format!("export exceeded LOOM_EXPORT_MAX_ROWS ({max})"),
                    ))))
                } else {
                    Ok(batch)
                }
            }
            Err(e) => Err(FlightError::from_external_error(Box::new(e))),
        });
        let out = FlightDataEncoderBuilder::new()
            .build(capped)
            .map_err(|e| Status::internal(e.to_string()));
        Ok(Response::new(Box::pin(out)))
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
```

Implementer notes:
- `(batch.num_rows() as u64)` will trip the `cast_possible_truncation`/`cast_lossless` clippy
  lints. Prefer `u64::try_from(batch.num_rows()).unwrap_or(u64::MAX)` or
  `#[expect(clippy::cast_possible_truncation, reason = "row count fits u64")]`.
- `std::io::Error::other` is stable; if the pinned toolchain rejects it, use
  `std::io::Error::new(std::io::ErrorKind::Other, msg)`.
- **`FlightInfo::try_with_schema` is the one arrow-flight API this codebase does not already
  use** (the engine's `get_flight_info` attaches no schema). It is a real builder method in
  recent arrow-flight (`try_with_schema(self, &Schema) -> Result<Self, ArrowError>`, IPC-encoding
  the schema), but **confirm it on the first CI compile of this task**. Fallback if the vendored
  arrow-flight 58 lacks it: drop the `.try_with_schema(&schema)?` line and return the `FlightInfo`
  without an embedded schema — Flight clients then read the schema from the `do_get` stream's
  first message (the data schema, which is authoritative anyway). If you take the fallback, note
  it as a `fut-` deferral in Task 7 and adjust the Task 6 `get_flight_info` schema assertion to
  read the schema from `do_get` instead.
- `self.cp.ontology()` / `self.cp.acl()` return `&(dyn Ontology …)` / `&(dyn Acl …)` — exactly
  `compile_object_read`'s params.

**Step 5: Verify (CI)** — compiles only; behavior under test in Task 6.

```bash
git add src/services/query-api/src/flight_export.rs src/services/query-api/BUCK
git commit -m "feat(query-api): FlightExportService — governed Flight export server"
git push
gh pr checks
```

---

## Task 5: Library/binary BUCK deps + `main.rs` wiring

Add the third-party deps the new code needs, and spawn the Flight server from `main.rs`
when `LOOM_FLIGHT_BIND_ADDR` is set (default off → today's HTTP-only behavior).

**Files:**
- Modify: `src/services/query-api/BUCK` (library + binary deps)
- Modify: `src/services/query-api/src/main.rs` (config + spawn)

**Step 1: BUCK deps**

Add to the `query-api` `rust_library` `deps` (keep sorted):

```python
        "//third-party:arrow-flight",
        "//third-party:futures",
        "//third-party:tonic",
```

(If the `serde` derive on `ExportCommand` needs it, also add `"//third-party:serde",`.)

Add to the `query-api-bin` `rust_binary` `deps`:

```python
        "//third-party:arrow-flight",
        "//third-party:tonic",
```

**Step 2: `main.rs` — config constants**

Near the other `DEFAULT_*` consts:

```rust
/// Default per-export row cap (`LOOM_EXPORT_MAX_ROWS`). Bounds a runaway governed export; an
/// operator hydrating a large working set raises it.
const DEFAULT_EXPORT_MAX_ROWS: u32 = 1_000_000;
```

**Step 3: `main.rs` — keep the engine socket reusable**

The engine socket is currently consumed building `EngineServingClient`. Bind it to a named
`String` first so both the HTTP serving client and the Flight export client can use it:

```rust
    let engine_socket =
        std::env::var("LOOM_ENGINE_SOCKET").map_err(|e| -> Box<dyn std::error::Error> {
            format!("LOOM_ENGINE_SOCKET must be set for the Iceberg serving backend: {e}").into()
        })?;
```

Move that read out of the `(serving, action_engine)` block so `engine_socket` is in scope for
the spawn below, and pass `engine_socket.clone()` to `EngineServingClient::connect`.

**Step 3b: `main.rs` — clone `cp` BEFORE it is moved into `AppState`** (reviewer D1 — blocker)

`cp` (`Arc<dyn ControlPlane>`) is **moved** into `AppState { cp, … }` (currently `main.rs:78`),
so a `cp.clone()` in the spawn below would be a use-after-move (E0382). Bind a clone for the
Flight server *before* building `app`:

```rust
    // Clone for the optional Flight export server before `cp` is moved into AppState.
    let cp_flight = cp.clone();
    let auth_flight: std::sync::Arc<dyn control_plane_core::Auth + Send + Sync> = pg.clone();
```

(`pg` is `Arc<PgControlPlane>`, still in scope after `app`; the explicit `auth_flight` binding
makes the `Arc<dyn Auth>` unsize-coercion unambiguous.)

**Step 4: `main.rs` — spawn the Flight export server**

After the `app` is built and **before** `service_runtime::serve(...)`, add:

```rust
    // Optional external Arrow Flight export listener (opt-in via LOOM_FLIGHT_BIND_ADDR).
    if let Ok(bind) = std::env::var("LOOM_FLIGHT_BIND_ADDR") {
        use arrow_flight::flight_service_server::FlightServiceServer;
        use query_api::flight_export::FlightExportService;

        let max_rows = std::env::var("LOOM_EXPORT_MAX_ROWS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(DEFAULT_EXPORT_MAX_ROWS);
        let addr: std::net::SocketAddr = bind
            .parse()
            .map_err(|e| -> Box<dyn std::error::Error> {
                format!("LOOM_FLIGHT_BIND_ADDR `{bind}` is not a valid socket address: {e}").into()
            })?;
        let flight_engine =
            engine_wire::flight::FlightSqlClient::connect(engine_socket.clone()).await?;
        // cp_flight / auth_flight were cloned in Step 3b, before `cp` moved into AppState.
        let export = FlightExportService::new(auth_flight, cp_flight, flight_engine, max_rows);

        tokio::spawn(async move {
            tracing::info!(%addr, "starting governed Flight export server");
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(export))
                .serve(addr)
                .await
            {
                tracing::error!(error = %e, "Flight export server exited");
            }
        });
    }

    service_runtime::serve(cfg.bind_addr, app).await?;
```

Implementer notes:
- `auth_flight` / `cp_flight` come from Step 3b (cloned before `cp` moves into `AppState`). Do
  **not** write `cp.clone()` here — `cp` is already moved (E0382).
- The binary needs `engine_wire` on its dep list — it is **not** currently there. Add
  `"//src/services/engine-wire:engine-wire",` to the `query-api-bin` deps (the library re-exports
  nothing of it). Alternatively re-export `FlightSqlClient` from `query_api` and use that; prefer
  adding the engine-wire dep to the binary to keep it explicit.
- Spawn-and-log (not `try_join!`) is deliberate for this MVP: the HTTP path is the primary
  server; a Flight bind failure is logged and the HTTP server still runs. (A future hardening
  could make a Flight bind error fatal.)

**Step 5: Verify (CI)**

```bash
git add src/services/query-api/BUCK src/services/query-api/src/main.rs
git commit -m "feat(query-api): wire LOOM_FLIGHT_BIND_ADDR Flight export server in main"
git push
gh pr checks  # affected builds query-api-bin; HTTP path + all existing tests unchanged
```

---

## Task 6: Governed export end-to-end test (fixture)

The acceptance test: a live engine over UDS + hermetic Postgres/object-store, an in-process
`FlightExportService` over a TCP port, a real Flight client `do_get`. Asserts vectors stream
value-exact as `List<Float32>`, ACL is applied, auth is enforced, and no `LIMIT 1000` clips.

**Files:**
- Create: `src/services/query-api/tests/governed_flight_export_e2e.rs`
- Modify: `src/services/query-api/BUCK` (new `loom_fixture_test` target)

**Step 1: BUCK target** (mirror `engine-wire-serving-e2e`, which stands up the live engine):

```python
loom_fixture_test(
    name = "governed-flight-export-e2e",
    crate = "governed_flight_export_e2e",
    srcs = ["tests/governed_flight_export_e2e.rs"],
    crate_root = "tests/governed_flight_export_e2e.rs",
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/services/engine:engine",
        "//src/services/engine-wire:engine-wire",
        "//src/services/ingest:ingest",
        "//src/services/runtime:runtime",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow",
        "//third-party:arrow-array",
        "//third-party:arrow-flight",
        "//third-party:futures",
        "//third-party:iceberg",
        "//third-party:object_store",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:tokio",
        "//third-party:tokio-stream",
        "//third-party:tonic",
        "//third-party:uuid",
    ],
)
```

Dep rationale: `runtime` (mint a bearer token via `service_runtime::control_plane` +
`Auth::create_session` / `token_sha256`, and build the `PgControlPlane`); `futures` (drive the
Flight client + decode the `do_get` stream incrementally for the streaming-smoke case);
`ingest` (seed the `vector(4)` column through the real landing path — see Step 2 note).

**Step 2: Two prerequisite mechanisms — read these before writing the test (reviewer D4):**

- **Booting the live engine over UDS:** mirror `tests/engine_wire_serving_e2e.rs` exactly — it
  builds an Iceberg `SqlCatalog` + `IcebergCatalog`, constructs `engine::flight::FlightDataService`,
  binds a `tokio::net::UnixListener` at a temp socket path, and serves it with
  `tonic::transport::Server::builder().add_service(FlightServiceServer::new(flight))` over a
  `UnixListenerStream` in a spawned task (see `engine/src/main.rs:42-69` for the exact construction).
  Set `LOOM_ENGINE_SOCKET` to that path. Then `FlightSqlClient::connect(socket)` for the export
  service. Copy that boot block; do not invent a new one.
- **Seeding a `vector(4)` column:** you **cannot** seed vectors through the action writer —
  `serving.rs:177-181` makes `build_object_batch` return an error for `BaseType::Vector` (mirror-side
  vector serving is deferred). Seed through the **landing materializer** (the ingest path that
  A3a's `tests/vector_landing.rs` exercises): build an Arrow batch with a `List<Float32>` column,
  land it as Parquet + an Iceberg snapshot via the ingest materializer, and register the type in
  the ontology with the `vector(4)` logical type. Reuse `vector_landing.rs`'s seed helper as the
  template (it already lands a vector column end-to-end).

**Step 3: Test structure** (the implementer fleshes out the helpers from `e2e_support` +
`engine_wire_serving_e2e`'s engine-boot pattern):

```rust
//! End-to-end: governed Arrow Flight export. Boots a live engine (UDS) over hermetic
//! Postgres + object store, seeds a typed object with a vector(4) column, stands up an
//! in-process FlightExportService on a TCP port, and exercises do_get with a real Flight
//! client. Verifies native vector carriage, ACL application, auth, and the no-LIMIT export.

// Helpers reused: e2e_support seed (`tref`/`land`/`prop`), the engine-boot from
// engine-wire-serving-e2e (build SqlCatalog + FlightDataService over a UnixListener),
// and a bearer-token mint via service_runtime auth routes / create_session.

#[tokio::test]
async fn governed_export_streams_vectors_value_exact() {
    // 1. boot postgres fixture + object store + engine over UDS (LOOM_ENGINE_SOCKET).
    // 2. seed type `Chunk` with columns: id string (identity), embedding vector(4),
    //    over >1000 rows (to prove no LIMIT 1000) — or a smaller set for the value-exact
    //    assertion and a separate >1000 case.
    // 3. grant the subject Read on `Chunk`; mint a bearer token (create_session).
    // 4. start FlightExportService::new(auth, cp, FlightSqlClient::connect(socket), max_rows)
    //    on 127.0.0.1:0 (ephemeral port) via tonic Server in a spawned task.
    // 5. Flight client: get_flight_info(ExportCommand{type:"Chunk"}) with the bearer token in
    //    the `authorization` metadata → assert the schema has embedding: List<Float32>.
    // 6. do_get(ticket) → collect RecordBatches → assert:
    //    - the embedding column downcasts to ListArray of Float32Array, value-exact vs seed
    //      (NOT a Utf8 stringification);
    //    - row count == seeded count (no truncation at 1000).
}

#[tokio::test]
async fn export_applies_acl_row_filter_and_mask() {
    // A row-filtered subject's export returns only permitted rows; a column-masked subject's
    // export projects the mask constant; a type-denied subject → PermissionDenied.
}

#[tokio::test]
async fn export_requires_bearer_token() {
    // get_flight_info / do_get with no (or a bogus) `authorization` metadata → Unauthenticated.
}
```

**Step 4: Key assertions to encode** (spec acceptance criteria):
- **Native vector**: assert on the **`do_get` data stream** schema (authoritative), not the
  advertised `get_flight_info` schema. Downcast `batch.column(embedding_idx)` to
  `arrow::array::ListArray`; its values to `Float32Array`; compare to the seeded vectors
  element-by-element. A regression that routed through `SqlValue` would yield `Utf8` — assert the
  type is `List<Float32>`, not `Utf8`. (For the `get_flight_info` advertised-schema check, assert
  only the outer `DataType::List(Float32)`; do not assert on field nullability or the inner
  field's name — those are not part of the contract.)
- **ACL**: seed two subjects; one with a row filter (`sourcebook = 'PHB'`), assert its export
  omits the other rows. One with `embedding` masked, assert the masked projection. One with no
  Read grant, assert `tonic::Status::code() == Code::PermissionDenied`.
- **Auth**: no token → `Code::Unauthenticated`.
- **No LIMIT**: seed 1001 rows (or set `max_rows` high), assert the export returns all of them.
- **Streaming smoke**: assert more than one `FlightData`/`RecordBatch` message arrives for a
  multi-batch result (decode the stream incrementally rather than `try_collect` for this case).
- **Default off**: (covered structurally — `LOOM_FLIGHT_BIND_ADDR` unset means `main` never
  spawns the server; the existing HTTP tests already prove the HTTP path is unchanged. No new
  assertion needed, but the test must not set that env var globally in a way that leaks.)

**Step 5: Verify (CI)**

```bash
git add src/services/query-api/tests/governed_flight_export_e2e.rs src/services/query-api/BUCK
git commit -m "test(query-api): governed Flight export e2e (vectors, ACL, auth, no-limit)"
git push
gh pr checks  # the fixture lane runs the e2e local-with-postgres
```

Expected: all e2e cases green. Iterate via CI on any compile/runtime gap (the fixture boots
`initdb`/`postgres` + the engine; mirror `engine-wire-serving-e2e` exactly for the boot).

---

## Task 7: Close the register item + open the PR

**Files:**
- Modify: `docs/ROADMAP.md` (via `loom-docs-update`: `- [ ]`→`- [x]`, `status:done`, `pr:#<N>`)

**Steps:**
1. Run the `loom-docs-update` skill to close `road-governed-flight-export` (mark done, add the PR
   number) and record any new deferral surfaced during the build (e.g. if `try_with_schema`
   mismatch handling or a Flight bind-fatal hardening is punted, add a `fut-…` item).
2. `bash tools/docs.sh validate` → OK.
3. Open the PR with head `work/road-governed-flight-export` (binds the claim):
   `superpowers:finishing-a-development-branch`. Title:
   `feat(query): governed Arrow Flight export (close road-governed-flight-export)`.
4. Confirm CI green (`affected` + `lint` + the fixture lane). The claim is reaped on merge.

---

## Verification checklist (acceptance criteria → where proven)

| Spec acceptance criterion | Proven by |
|---|---|
| 1. Authed `get_flight_info`/`do_get` of `ExportCommand` → columnar Arrow, `vector(N)` as `List<Float32>` value-exact | Task 6 `governed_export_streams_vectors_value_exact` |
| 2. Governance identical to `GET /objects/{type}` (deny-by-default, row filters, masking), re-derived per `do_get`, no SQL on the wire | Task 2 (shared `compile_object_read`) + Task 6 `export_applies_acl_row_filter_and_mask` |
| 3. Full slice (no `LIMIT 1000`), bounded by `LOOM_EXPORT_MAX_ROWS`, streamed (not buffered) | Task 1 (`execute_stream`) + Task 4 (cap) + Task 6 no-limit & streaming-smoke cases |
| 4. Missing/invalid token → `Unauthenticated`; type-denied → `PermissionDenied` | Task 4 (`authenticate`, `map_query_err`) + Task 6 `export_requires_bearer_token` |
| 5. `buck2 test //src/...` green; `LOOM_FLIGHT_BIND_ADDR` unset leaves HTTP path + defaults unchanged | Task 5 (opt-in spawn) + full CI sweep |
```
