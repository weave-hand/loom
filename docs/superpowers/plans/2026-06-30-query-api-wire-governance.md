# Wire-backed ControlPlane for query-api — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** query-api reads its governance metadata (ACL policy + ontology) over the engine gRPC wire instead of from its own Postgres connection, by injecting a wire-backed `ControlPlane` whose `acl()`/`ontology()` issue RPCs to the engine while `queue()` stays the direct Postgres plane.

**Architecture:** Extend the existing `EngineControl` protobuf service with nine unary governance-read RPCs whose requests/responses carry serde-JSON-encoded `control_plane_core` domain payloads. Engine-side handlers delegate straight to the engine's own `cp.acl()`/`cp.ontology()`. On the query-api side, `WireAcl` + `WireOntology` implement the read methods by calling those RPCs through the engine-wire client; they compose into a `WireControlPlane` (read-only governance client) that delegates `queue()` to the direct Postgres plane and guards every write/define/catalog/lineage path. Because every query-api handler already reads governance *only* through the `ControlPlane` trait accessors (`st.cp.acl()`, `st.cp.ontology()`, `st.cp.queue()`, `deps.cp` in `action.rs`), the relocation is purely a `main.rs` construction swap — **no handler code changes**.

**Tech Stack:** Rust 2024, buck2, tonic/prost gRPC over UDS (protox + tonic-prost-build codegen, no protoc), serde/serde_json, `#[async_trait]`, sqlx (Postgres), hermetic Postgres fixture (`PgFixture`) + Iceberg `SqlCatalog` for e2e.

## Global Constraints

- **Tests are `rust_test` integration targets only — never inline `#[cfg(test)]`.** Each test is a sibling `tests/<name>.rs` file wired as its own target in the crate's `BUCK`, via the `loom_rust_test` wrapper (or `loom_fixture_test` for Postgres-backed tests). Inline `#[test]` is rejected by the `no-inline-tests` prek hook.
- **Postgres-backed tests must use `loom_fixture_test`** (sets `remote_execution = "disabled"`), not bare `rust_test`, or they route to RE and fail as root.
- **Clippy is strict (pedantic + restriction).** Production code must not trip `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `indexing_slicing`, `map_err_ignore`, etc. Where a `panic!`/`unreachable!` guard is genuinely required (the read-only `catalog()`/`lineage()` accessors), gate it with `#[expect(clippy::panic, reason = "…")]` (or the specific lint) — every `#[expect]`/`#[allow]` needs a `reason`. Prefer returning a loud `Err` over panicking wherever the method returns `Result`.
- **The query-api library must NOT gain a direct dependency on `control_plane_postgres`.** A prior slice ([[road-engine-serving-write-relocation]]) added a guard enforcing this (`tools/check-query-api-postgres-free.sh`). `WireControlPlane` lives in the query-api *library* and must hold the direct Postgres plane only as an `Arc<dyn ControlPlane>` (injected by the binary), never naming the concrete postgres type. The pool/`PgControlPlane` is constructed only in `main.rs` (the binary) and in tests (which already depend on `control-plane/postgres`).
- **No new `local_only`/`uses_local_*` buck actions on the common build path.**
- **Markdown lint:** any `.md` edit ends with exactly one trailing newline, no trailing whitespace.
- **serde derives are additive** — never change an existing `#[derive(...)]`'s behavior, only append `serde::Serialize, serde::Deserialize`.
- **Conventional Commits** on every commit message (enforced by the `conventional-commit` commit-msg hook).

---

## File Structure

**Core domain types (add serde derives):**
- `src/control-plane/core/src/acl.rs` — add serde to `SubjectId`, `Action`, `PolicyTarget`, `Decision`, `Policy`.
- `src/control-plane/core/src/ontology.rs` — add serde to `TypeName`, `PropertyDef`, `ObjectType`, `Cardinality`, `LinkBacking`, `LinkDef`, `Aggregation`, `DerivedPropertyDef`, `ActionName`, `ActionKind`, `ParamDef`, `VectorIndexDef`, `ActionDef`.
- `src/control-plane/core/src/catalog.rs` — add serde to `TableRef`.
- `src/control-plane/core/src/vector_index.rs` — add serde to `Metric`, `IndexSpec`.
- `src/control-plane/core/src/page.rs` — add serde to `PageReq`, `Page<T>`.
- `src/control-plane/core/tests/serde_roundtrip.rs` — **new** round-trip test (+ target in `core/BUCK`).

**Proto + codegen:**
- `src/services/engine-wire/proto/engine_control.proto` — add 9 RPCs + their request/response messages.

**Engine-wire client (governance read methods + error mapping):**
- `src/services/engine-wire/src/client.rs` — add `cp_status` helper + nine governance-read methods on `GrpcQueueClient`.
- `src/services/engine-wire/tests/cp_status.rs` — **new** unit test for the status mapper (+ target in `engine-wire/BUCK`).

**Engine-side handlers:**
- `src/services/engine/src/service.rs` — implement the 9 new RPC handler methods on `EngineControlService`.

**query-api wire control plane:**
- `src/services/query-api/src/wire_control_plane.rs` — **new** module: `WireAcl`, `WireOntology`, `WireControlPlane`.
- `src/services/query-api/src/lib.rs` — declare `pub mod wire_control_plane;`.
- `src/services/query-api/src/main.rs` — swap the injected governance plane to `WireControlPlane`.
- `src/services/query-api/BUCK` — register the two new test targets; confirm `engine-wire` dep on the library (already present).

**Tests (query-api e2e):**
- `src/services/query-api/tests/e2e_support.rs` — extract a `spawn_engine` helper that returns the socket path; add a `connect_gov_client` / `wire_cp` helper.
- `src/services/query-api/tests/wire_governance_e2e.rs` — **new**: RPC round-trip + parity + read-only-guard tests.

---

## Task 1: serde derives on the core governance domain types

**Files:**
- Modify: `src/control-plane/core/src/acl.rs`, `src/control-plane/core/src/ontology.rs`, `src/control-plane/core/src/catalog.rs`, `src/control-plane/core/src/vector_index.rs`, `src/control-plane/core/src/page.rs`
- Create: `src/control-plane/core/tests/serde_roundtrip.rs`
- Modify: `src/control-plane/core/BUCK`

**Interfaces:**
- Produces: every type in the transitive closure of the nine governance reads gains `serde::Serialize + serde::Deserialize`. The wire-relevant set is exactly: `SubjectId`, `Action`, `PolicyTarget`, `Decision`, `Policy` (acl.rs); `TypeName`, `PropertyDef`, `ObjectType`, `Cardinality`, `LinkBacking`, `LinkDef`, `Aggregation`, `DerivedPropertyDef`, `ActionName`, `ActionKind`, `ParamDef`, `VectorIndexDef`, `ActionDef` (ontology.rs); `TableRef` (catalog.rs); `Metric`, `IndexSpec` (vector_index.rs); `PageReq`, `Page<T>` (page.rs). (`RowFilter`, `CompareOp`, `ScalarValue`, `Cursor` already derive serde — do not touch.)

- [ ] **Step 1: Write the failing round-trip test**

Create `src/control-plane/core/tests/serde_roundtrip.rs`:

```rust
//! Every governance-read domain payload must round-trip through serde_json so it
//! can cross the engine-wire RPC boundary losslessly. This pins the serde derives
//! the wire-backed ControlPlane depends on.

use control_plane_core::{
    Action, ActionDef, ActionKind, ActionName, Cardinality, Decision, DerivedPropertyDef,
    Aggregation, IndexSpec, LinkBacking, LinkDef, Metric, ObjectType, Page, PageReq, Policy,
    PolicyTarget, PropertyDef, RowFilter, ScalarValue, CompareOp, SubjectId, TableRef, TypeName,
    VectorIndexDef,
};

fn roundtrip<T>(v: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let json = serde_json::to_string(v).expect("serialize");
    let back: T = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(v, &back, "round-trip mismatch for {json}");
}

#[test]
fn acl_payloads_roundtrip() {
    roundtrip(&SubjectId("alice".into()));
    roundtrip(&Action::Read);
    roundtrip(&Action::Write);
    roundtrip(&Decision::Allow);
    roundtrip(&Decision::Deny);
    roundtrip(&PolicyTarget::Type(TypeName("customer".into())));
    roundtrip(&PolicyTarget::Table(TableRef {
        schema: "main".into(),
        name: "orders".into(),
    }));
    let policy = Policy {
        target: PolicyTarget::Type(TypeName("customer".into())),
        row_filter: Some(RowFilter::Compare {
            property: "region".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Text("emea".into()),
        }),
        deny_columns: vec!["ssn".into()],
        mask_columns: vec!["email".into()],
    };
    roundtrip(&policy);
    roundtrip(&Page {
        items: vec![policy],
        next: None,
    });
}

#[test]
fn ontology_payloads_roundtrip() {
    let ty = ObjectType {
        name: TypeName("customer".into()),
        properties: vec![PropertyDef {
            name: "id".into(),
            ty: "int".into(),
            required: true,
        }],
        derived: vec![DerivedPropertyDef {
            name: "order_count".into(),
            ty: "int".into(),
            link: "orders".into(),
            agg: Aggregation::Count,
        }],
        table: TableRef {
            schema: "main".into(),
            name: "customer".into(),
        },
        identity: Some("id".into()),
    };
    roundtrip(&ty);
    roundtrip(&Page {
        items: vec![LinkDef {
            name: "orders".into(),
            from: TypeName("customer".into()),
            to: TypeName("order".into()),
            cardinality: Cardinality::Many,
            backing: LinkBacking::ForeignKey {
                from_column: "id".into(),
                to_column: "customer_id".into(),
            },
        }],
        next: None,
    });
    roundtrip(&ActionDef {
        name: ActionName("create_customer".into()),
        target: TypeName("customer".into()),
        parameters: vec![],
        kind: ActionKind::Insert,
    });
    roundtrip(&VectorIndexDef {
        name: "emb_idx".into(),
        type_name: TypeName("customer".into()),
        property: "embedding".into(),
        metric: Metric::Cosine,
        spec: IndexSpec::Flat,
    });
    roundtrip(&PageReq::default());
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `buck2 test //src/control-plane/core:serde-roundtrip > /tmp/t.log 2>&1; grep -E "error\[|cannot|Tests finished|FAIL" /tmp/t.log`
Expected: build failure — the target does not exist yet AND the types do not implement `Serialize`/`Deserialize`.

- [ ] **Step 3: Add the BUCK target**

In `src/control-plane/core/BUCK`, mirror the existing `page` test target (look for `loom_rust_test(name = "page", …)`), adding:

```python
loom_rust_test(
    name = "serde-roundtrip",
    srcs = ["tests/serde_roundtrip.rs"],
    crate = "serde_roundtrip",
    crate_root = "tests/serde_roundtrip.rs",
    deps = [
        ":core",
        "//third-party:serde",
        "//third-party:serde_json",
    ],
)
```

(Match the exact `deps`/attr style of the sibling tests; `:core` is this crate's library target. If the existing tests reference serde via a different label, copy theirs.)

- [ ] **Step 4: Add serde derives to acl.rs**

In `src/control-plane/core/src/acl.rs`, append `, serde::Serialize, serde::Deserialize` to the `#[derive(...)]` of each of these (keep existing derives intact):

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SubjectId(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Action {
    Read,
    Write,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PolicyTarget {
    Type(TypeName),
    Table(TableRef),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Decision {
    Allow,
    Deny,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Policy {
    pub target: PolicyTarget,
    pub row_filter: Option<RowFilter>,
    pub deny_columns: Vec<String>,
    pub mask_columns: Vec<String>,
}
```

(Use the fully-qualified `serde::Serialize`/`serde::Deserialize` path so no `use` import is needed. Do not modify doc comments or field bodies.)

- [ ] **Step 5: Add serde derives to ontology.rs**

In `src/control-plane/core/src/ontology.rs`, append `, serde::Serialize, serde::Deserialize` to the `#[derive(...)]` of: `TypeName`, `PropertyDef`, `ObjectType`, `Cardinality`, `LinkBacking`, `LinkDef`, `Aggregation`, `DerivedPropertyDef`, `ActionName`, `ActionKind`, `ParamDef`, `VectorIndexDef`, `ActionDef`. Example for two of them (apply the same pattern to all 13):

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TypeName(pub String);

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObjectType {
    pub name: TypeName,
    pub properties: Vec<PropertyDef>,
    pub derived: Vec<DerivedPropertyDef>,
    pub table: TableRef,
    pub identity: Option<String>,
}
```

Note `ActionKind` carries `#[derive(... Default)]` and a `#[default]` attribute on `Insert` — keep those; just append the two serde traits.

- [ ] **Step 6: Add serde derives to catalog.rs, vector_index.rs, page.rs**

`src/control-plane/core/src/catalog.rs`:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TableRef {
    pub schema: String,
    pub name: String,
}
```

`src/control-plane/core/src/vector_index.rs` — `Metric` (keep its `Copy, Default` + `#[default]`) and `IndexSpec`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum Metric {
    #[default]
    Cosine,
    L2,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IndexSpec {
    Flat,
    IvfFlat { nlist: Option<u32> },
    Hnsw { m: Option<u32>, ef_construction: Option<u32> },
}
```

`src/control-plane/core/src/page.rs` — `PageReq` and the generic `Page<T>` (serde derives the `T: Serialize`/`T: Deserialize` bounds automatically):

```rust
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PageReq {
    pub after: Option<Cursor>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next: Option<Cursor>,
}
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `buck2 test //src/control-plane/core:serde-roundtrip > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: `Tests finished: Pass …`. Then confirm the crate still builds clean and clippy-clean:
`buck2 build '//src/control-plane/core:core[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean).

- [ ] **Step 8: Commit**

```bash
git add src/control-plane/core
git commit -m "feat(core): serde derives on governance-read domain types

Adds Serialize/Deserialize to the ACL/ontology/catalog/vector-index/page
types carried by the upcoming EngineControl governance-read RPCs."
```

---

## Task 2: EngineControl governance-read RPCs (proto + codegen)

**Files:**
- Modify: `src/services/engine-wire/proto/engine_control.proto`

**Interfaces:**
- Produces: nine new RPCs on `service EngineControl` — `Check`, `PoliciesFor`, `GetType`, `Resolve`, `Links`, `LinksTo`, `GetAction`, `VectorIndexesFor`, `GetVectorIndex` — and their request/response message types. **Convention:** scalar names (`TypeName`/`ActionName`, both newtypes over `String`, and the vector-index `name`) ride as plain proto `string` fields; every compound argument (`SubjectId`, `Action`, `PolicyTarget`, `PageReq`) and every result payload rides as a serde-JSON `string` field — consistent with the existing `WriteObject`/`OverwriteTable` RPCs' `columns_json`/`lineage_json`. This keeps the proto thin and the `core` types the sole contract.

- [ ] **Step 1: Add the RPCs and messages to the proto**

In `src/services/engine-wire/proto/engine_control.proto`, add the nine RPCs to the `service EngineControl { … }` block (after `OverwriteTable`):

```protobuf
  // Governance reads (query-api reads ACL + ontology over the wire).
  rpc Check            (CheckRequest)            returns (CheckResponse);
  rpc PoliciesFor      (PoliciesForRequest)      returns (PoliciesForResponse);
  rpc GetType          (GetTypeRequest)          returns (GetTypeResponse);
  rpc Resolve          (ResolveRequest)          returns (ResolveResponse);
  rpc Links            (LinksRequest)            returns (LinksResponse);
  rpc LinksTo          (LinksToRequest)          returns (LinksToResponse);
  rpc GetAction        (GetActionRequest)        returns (GetActionResponse);
  rpc VectorIndexesFor (VectorIndexesForRequest) returns (VectorIndexesForResponse);
  rpc GetVectorIndex   (GetVectorIndexRequest)   returns (GetVectorIndexResponse);
```

Then add the message definitions at the end of the file:

```protobuf
// ---- Governance-read messages. *_json fields are serde_json of the named core type. ----

message CheckRequest  { string subject_json = 1; string action_json = 2; string target_json = 3; }
message CheckResponse { string decision_json = 1; }   // Decision

message PoliciesForRequest {
  string subject_json = 1;
  string action_json = 2;
  string target_json = 3;
  string page_json = 4;     // PageReq
}
message PoliciesForResponse { string page_json = 1; }  // Page<Policy>

message GetTypeRequest  { string type_name = 1; }
message GetTypeResponse { string object_type_json = 1; }   // ObjectType

message ResolveRequest  { string type_name = 1; }
message ResolveResponse { string table_ref_json = 1; }     // TableRef

message LinksRequest  { string type_name = 1; string page_json = 2; }
message LinksResponse { string page_json = 1; }            // Page<LinkDef>

message LinksToRequest  { string type_name = 1; string page_json = 2; }
message LinksToResponse { string page_json = 1; }          // Page<LinkDef>

message GetActionRequest  { string action_name = 1; }
message GetActionResponse { string action_def_json = 1; }  // ActionDef

message VectorIndexesForRequest  { string type_name = 1; }
message VectorIndexesForResponse { string indexes_json = 1; }  // Vec<VectorIndexDef>

message GetVectorIndexRequest  { string type_name = 1; string name = 2; }
message GetVectorIndexResponse { string index_json = 1; }      // Option<VectorIndexDef>
```

- [ ] **Step 2: Build the engine-wire library to run codegen and verify the stubs compile**

Run: `buck2 build //src/services/engine-wire:engine-wire > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error|FAIL" /tmp/b.log`
Expected: `BUILD SUCCEEDED` — protox/tonic regenerate `loom.engine.v1.rs` with the new `pb::CheckRequest`, `pb::engine_control_client::EngineControlClient::check`, the server-trait methods, etc. (The library has no handler for them yet; the server trait now has unimplemented-by-default methods only on the *server* side, which `EngineControlService` will implement in Task 4 — until then `engine` will fail to build. That is expected and addressed in Task 4; this task's gate is only that `engine-wire` itself compiles.)

- [ ] **Step 3: Commit**

```bash
git add src/services/engine-wire/proto/engine_control.proto
git commit -m "feat(engine-wire): EngineControl governance-read RPCs

Adds Check/PoliciesFor + GetType/Resolve/Links/LinksTo/GetAction/
VectorIndexesFor/GetVectorIndex RPCs carrying serde-JSON core payloads."
```

---

## Task 3: engine-wire client governance-read methods + error mapping

**Files:**
- Modify: `src/services/engine-wire/src/client.rs`
- Create: `src/services/engine-wire/tests/cp_status.rs`
- Modify: `src/services/engine-wire/BUCK`

**Interfaces:**
- Consumes: `pb::*Request`/`*Response` from Task 2; `control_plane_core` domain types with serde from Task 1.
- Produces: on `GrpcQueueClient`, nine async methods returning `core` domain types mapped to `ControlPlaneError`:
  - `gov_check(&self, subject: &SubjectId, action: Action, target: &PolicyTarget) -> Result<Decision, ControlPlaneError>`
  - `gov_policies_for(&self, subject: &SubjectId, action: Action, target: &PolicyTarget, page: &PageReq) -> Result<Page<Policy>, ControlPlaneError>`
  - `gov_get_type(&self, name: &TypeName) -> Result<ObjectType, ControlPlaneError>`
  - `gov_resolve(&self, name: &TypeName) -> Result<TableRef, ControlPlaneError>`
  - `gov_links(&self, name: &TypeName, page: &PageReq) -> Result<Page<LinkDef>, ControlPlaneError>`
  - `gov_links_to(&self, name: &TypeName, page: &PageReq) -> Result<Page<LinkDef>, ControlPlaneError>`
  - `gov_get_action(&self, name: &ActionName) -> Result<ActionDef, ControlPlaneError>`
  - `gov_vector_indexes_for(&self, type_name: &TypeName) -> Result<Vec<VectorIndexDef>, ControlPlaneError>`
  - `gov_get_vector_index(&self, type_name: &TypeName, name: &str) -> Result<Option<VectorIndexDef>, ControlPlaneError>`
  - a free fn `pub fn cp_status(s: tonic::Status) -> ControlPlaneError`.

- [ ] **Step 1: Write the failing test for the status mapper**

Create `src/services/engine-wire/tests/cp_status.rs`:

```rust
//! The client maps tonic Status codes back to ControlPlaneError variants so that
//! query-api's error handling (e.g. NotFound -> 404) behaves identically whether
//! governance is read direct or over the wire.

use control_plane_core::ControlPlaneError;
use engine_wire::client::cp_status;
use tonic::Status;

#[test]
fn not_found_maps_to_not_found() {
    let e = cp_status(Status::not_found("no such type: customer"));
    assert!(matches!(e, ControlPlaneError::NotFound(m) if m == "no such type: customer"));
}

#[test]
fn aborted_maps_to_conflict() {
    let e = cp_status(Status::aborted("conflict"));
    assert!(matches!(e, ControlPlaneError::Conflict(_)));
}

#[test]
fn other_codes_map_to_backend() {
    let e = cp_status(Status::internal("boom"));
    assert!(matches!(e, ControlPlaneError::Backend(_)));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/services/engine-wire:cp-status > /tmp/t.log 2>&1; grep -E "error|cannot|Tests finished|FAIL" /tmp/t.log`
Expected: build failure (target/`cp_status` do not exist).

- [ ] **Step 3: Add the test target to engine-wire/BUCK**

In `src/services/engine-wire/BUCK`, mirror an existing `loom_rust_test` (or add one if there are none), adding:

```python
loom_rust_test(
    name = "cp-status",
    srcs = ["tests/cp_status.rs"],
    crate = "cp_status",
    crate_root = "tests/cp_status.rs",
    deps = [
        ":engine-wire",
        "//src/control-plane/core:core",
        "//third-party:tonic",
    ],
)
```

(Match the crate's existing dep label style. `control_plane_core` must already be a dep of `:engine-wire` — `convert.rs` uses it; if `tonic` is not yet exposed to tests, add `//third-party:tonic`.)

- [ ] **Step 4: Implement `cp_status` and the governance methods**

In `src/services/engine-wire/src/client.rs`, first ensure the needed imports are present (add what's missing near the top):

```rust
use control_plane_core::{
    Action, ActionDef, ActionName, ControlPlaneError, Decision, LinkDef, ObjectType, Page,
    PageReq, Policy, PolicyTarget, SubjectId, TableRef, TypeName, VectorIndexDef,
};
```

Add the status mapper (mirror of the engine-side `status` fn in `engine/src/service.rs`):

```rust
/// Map a tonic [`Status`] from an `EngineControl` governance RPC back to a
/// [`ControlPlaneError`], inverting the engine-side `status` mapping so the error
/// *kind* (notably `NotFound`) survives the wire round-trip.
#[must_use]
pub fn cp_status(s: tonic::Status) -> ControlPlaneError {
    use tonic::Code;
    match s.code() {
        Code::NotFound => ControlPlaneError::NotFound(s.message().to_string()),
        Code::Aborted => ControlPlaneError::Conflict(s.message().to_string()),
        other => ControlPlaneError::Backend(
            format!("engine governance RPC failed ({other:?}): {}", s.message()).into(),
        ),
    }
}

/// Decode a serde-JSON governance payload, mapping decode failure to `Serialization`.
fn de<T: serde::de::DeserializeOwned>(json: &str) -> Result<T, ControlPlaneError> {
    serde_json::from_str(json).map_err(|e| ControlPlaneError::Serialization(e.to_string()))
}

/// Encode a governance argument to serde-JSON, mapping failure to `Serialization`.
fn se<T: serde::Serialize>(v: &T) -> Result<String, ControlPlaneError> {
    serde_json::to_string(v).map_err(|e| ControlPlaneError::Serialization(e.to_string()))
}
```

Then add the methods inside the `impl GrpcQueueClient { … }` block (mirror the existing `flush_table` call shape — clone the inner client, build the request, await, `.into_inner()`):

```rust
pub async fn gov_check(
    &self,
    subject: &SubjectId,
    action: Action,
    target: &PolicyTarget,
) -> Result<Decision, ControlPlaneError> {
    let resp = self
        .inner
        .clone()
        .check(pb::CheckRequest {
            subject_json: se(subject)?,
            action_json: se(&action)?,
            target_json: se(target)?,
        })
        .await
        .map_err(cp_status)?
        .into_inner();
    de(&resp.decision_json)
}

pub async fn gov_policies_for(
    &self,
    subject: &SubjectId,
    action: Action,
    target: &PolicyTarget,
    page: &PageReq,
) -> Result<Page<Policy>, ControlPlaneError> {
    let resp = self
        .inner
        .clone()
        .policies_for(pb::PoliciesForRequest {
            subject_json: se(subject)?,
            action_json: se(&action)?,
            target_json: se(target)?,
            page_json: se(page)?,
        })
        .await
        .map_err(cp_status)?
        .into_inner();
    de(&resp.page_json)
}

pub async fn gov_get_type(&self, name: &TypeName) -> Result<ObjectType, ControlPlaneError> {
    let resp = self
        .inner
        .clone()
        .get_type(pb::GetTypeRequest { type_name: name.0.clone() })
        .await
        .map_err(cp_status)?
        .into_inner();
    de(&resp.object_type_json)
}

pub async fn gov_resolve(&self, name: &TypeName) -> Result<TableRef, ControlPlaneError> {
    let resp = self
        .inner
        .clone()
        .resolve(pb::ResolveRequest { type_name: name.0.clone() })
        .await
        .map_err(cp_status)?
        .into_inner();
    de(&resp.table_ref_json)
}

pub async fn gov_links(
    &self,
    name: &TypeName,
    page: &PageReq,
) -> Result<Page<LinkDef>, ControlPlaneError> {
    let resp = self
        .inner
        .clone()
        .links(pb::LinksRequest { type_name: name.0.clone(), page_json: se(page)? })
        .await
        .map_err(cp_status)?
        .into_inner();
    de(&resp.page_json)
}

pub async fn gov_links_to(
    &self,
    name: &TypeName,
    page: &PageReq,
) -> Result<Page<LinkDef>, ControlPlaneError> {
    let resp = self
        .inner
        .clone()
        .links_to(pb::LinksToRequest { type_name: name.0.clone(), page_json: se(page)? })
        .await
        .map_err(cp_status)?
        .into_inner();
    de(&resp.page_json)
}

pub async fn gov_get_action(&self, name: &ActionName) -> Result<ActionDef, ControlPlaneError> {
    let resp = self
        .inner
        .clone()
        .get_action(pb::GetActionRequest { action_name: name.0.clone() })
        .await
        .map_err(cp_status)?
        .into_inner();
    de(&resp.action_def_json)
}

pub async fn gov_vector_indexes_for(
    &self,
    type_name: &TypeName,
) -> Result<Vec<VectorIndexDef>, ControlPlaneError> {
    let resp = self
        .inner
        .clone()
        .vector_indexes_for(pb::VectorIndexesForRequest { type_name: type_name.0.clone() })
        .await
        .map_err(cp_status)?
        .into_inner();
    de(&resp.indexes_json)
}

pub async fn gov_get_vector_index(
    &self,
    type_name: &TypeName,
    name: &str,
) -> Result<Option<VectorIndexDef>, ControlPlaneError> {
    let resp = self
        .inner
        .clone()
        .get_vector_index(pb::GetVectorIndexRequest {
            type_name: type_name.0.clone(),
            name: name.to_string(),
        })
        .await
        .map_err(cp_status)?
        .into_inner();
    de(&resp.index_json)
}
```

(If `serde`/`serde_json` are not yet deps of `:engine-wire`, add `//third-party:serde` + `//third-party:serde_json` to the library's `deps` in `engine-wire/BUCK` — `serde_json` is already used by the write-path client code, so it should be present.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test //src/services/engine-wire:cp-status > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass`. Confirm clippy: `buck2 build '//src/services/engine-wire:engine-wire[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean).

- [ ] **Step 6: Commit**

```bash
git add src/services/engine-wire
git commit -m "feat(engine-wire): client governance-read methods + status mapper

GrpcQueueClient gains gov_* methods issuing the new RPCs and decoding serde
core payloads; cp_status inverts the engine status mapping (NotFound preserved)."
```

---

## Task 4: engine-side RPC handlers + round-trip integration test

**Files:**
- Modify: `src/services/engine/src/service.rs`
- Modify: `src/services/query-api/tests/e2e_support.rs` (extract `spawn_engine`, add `connect_gov_client`)
- Create: `src/services/query-api/tests/wire_governance_e2e.rs`
- Modify: `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `GrpcQueueClient::gov_*` (Task 3); the existing `spawn_engine_writer` harness.
- Produces: nine `impl pb::engine_control_server::EngineControl` handler methods on `EngineControlService` that decode the request JSON, reconstruct `core` types, delegate to `self.cp.acl()`/`self.cp.ontology()`, serde-encode the result, and map errors via the existing `status`. Also `e2e_support::spawn_engine(...) -> (String, EngineGuard)` and `e2e_support::connect_gov_client(sock: &str) -> GrpcQueueClient`.

- [ ] **Step 1: Write the failing round-trip integration test**

Create `src/services/query-api/tests/wire_governance_e2e.rs`. It builds the Postgres fixture manually (the `setup_widget_writer` pattern in `action_e2e.rs:35-112` — `setup_iceberg` consumes `db`/`warehouse` internally and does NOT return them, but `spawn_engine` needs them, so do NOT use `setup_iceberg` here), seeds an ontology directly on the `cp`, boots the engine over a UDS, connects a `GrpcQueueClient`, and asserts each governance RPC round-trips the same payload the direct `PgControlPlane` returns, including NotFound parity:

```rust
//! Each EngineControl governance RPC round-trips its core payload engine<->client,
//! identically to a direct PgControlPlane read, and a missing type surfaces NotFound
//! over the wire just as it does direct.

use control_plane_core::{
    Acl, Action, Cardinality, ControlPlaneError, LinkBacking, LinkDef, ObjectType, Ontology,
    PageReq, PolicyTarget, PropertyDef, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{connect_gov_client, spawn_engine};

/// Boot a fresh db + warehouse and define a `customer` type, an `order` type, and an
/// `orders` link customer->order, so get_type/resolve/links all resolve.
async fn seed(fx: &PgFixture) -> (control_plane_postgres::PgControlPlane, String, tempfile::TempDir) {
    let (cp, db) = fx.fresh_db().await;
    let warehouse = tempfile::tempdir().expect("warehouse");
    let customer = TypeName("customer".into());
    let order = TypeName("order".into());
    for (name, table) in [(&customer, "customer"), (&order, "order")] {
        cp.ontology()
            .define_type(ObjectType {
                name: name.clone(),
                properties: vec![PropertyDef { name: "id".into(), ty: "long".into(), required: true }],
                derived: vec![],
                table: TableRef { schema: "main".into(), name: table.into() },
                identity: Some("id".into()),
            })
            .await
            .expect("define_type");
    }
    cp.ontology()
        .define_link(LinkDef {
            name: "orders".into(),
            from: customer.clone(),
            to: order.clone(),
            cardinality: Cardinality::Many,
            backing: LinkBacking::ForeignKey {
                from_column: "id".into(),
                to_column: "customer_id".into(),
            },
        })
        .await
        .expect("define_link");
    (cp, db, warehouse)
}

#[tokio::test]
async fn rpc_roundtrips_match_direct_reads() {
    let fx = PgFixture::start();
    let (cp, db, warehouse) = seed(&fx).await;
    let (sock, _guard) =
        spawn_engine(&fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let client = connect_gov_client(&sock).await;

    // get_type parity
    let name = TypeName("customer".into());
    let direct = cp.ontology().get_type(&name).await.expect("direct get_type");
    let wire = client.gov_get_type(&name).await.expect("wire get_type");
    assert_eq!(direct, wire);

    // resolve parity
    assert_eq!(
        cp.ontology().resolve(&name).await.expect("direct resolve"),
        client.gov_resolve(&name).await.expect("wire resolve"),
    );

    // links parity
    let dl = cp.ontology().links(&name, PageReq::unbounded()).await.expect("direct links");
    let wl = client.gov_links(&name, &PageReq::unbounded()).await.expect("wire links");
    assert_eq!(dl.items, wl.items);

    // check parity (deny-by-default: unknown subject -> Deny on both)
    let subject = SubjectId("nobody".into());
    let target = PolicyTarget::Type(name.clone());
    assert_eq!(
        cp.acl().check(&subject, Action::Read, &target).await.expect("direct check"),
        client.gov_check(&subject, Action::Read, &target).await.expect("wire check"),
    );

    // NotFound parity
    let missing = TypeName("does_not_exist".into());
    let direct_err = cp.ontology().get_type(&missing).await.expect_err("direct missing");
    let wire_err = client.gov_get_type(&missing).await.expect_err("wire missing");
    assert!(matches!(direct_err, ControlPlaneError::NotFound(_)));
    assert!(matches!(wire_err, ControlPlaneError::NotFound(_)));
}
```

> Confirm `PageReq::unbounded()` exists (it is used throughout `handler.rs`). `PgFixture::start()` is synchronous and returns `Self`; `fresh_db()` returns `(PgControlPlane, String)` (verified at `e2e_support.rs`/`fixture.rs:288`). The `seed` helper is local to this test file; if a later test needs the same topology, promote it to `e2e_support`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:wire-governance-e2e > /tmp/t.log 2>&1; grep -E "error|cannot|Tests finished|FAIL" /tmp/t.log`
Expected: build failure — `spawn_engine`/`connect_gov_client` and the `engine` crate handlers don't exist yet (the `engine` crate won't compile until the handlers are added).

- [ ] **Step 3: Implement the engine-side handlers**

In `src/services/engine/src/service.rs`, add `Ontology` and `Acl` to the `use control_plane_core::{…}` import line (it currently imports `Catalog, Queue, RetryPolicy, RunId, TableRef`). Then add these methods inside the `impl pb::engine_control_server::EngineControl for EngineControlService` block (mirror the decode→delegate→encode shape of `write_object`). Use small JSON helpers:

```rust
async fn check(
    &self,
    req: Request<pb::CheckRequest>,
) -> std::result::Result<Response<pb::CheckResponse>, Status> {
    let r = req.into_inner();
    let subject: control_plane_core::SubjectId = de_arg(&r.subject_json, "subject")?;
    let action: control_plane_core::Action = de_arg(&r.action_json, "action")?;
    let target: control_plane_core::PolicyTarget = de_arg(&r.target_json, "target")?;
    let decision = self.cp.acl().check(&subject, action, &target).await.map_err(status)?;
    Ok(Response::new(pb::CheckResponse { decision_json: se_out(&decision)? }))
}

async fn policies_for(
    &self,
    req: Request<pb::PoliciesForRequest>,
) -> std::result::Result<Response<pb::PoliciesForResponse>, Status> {
    let r = req.into_inner();
    let subject: control_plane_core::SubjectId = de_arg(&r.subject_json, "subject")?;
    let action: control_plane_core::Action = de_arg(&r.action_json, "action")?;
    let target: control_plane_core::PolicyTarget = de_arg(&r.target_json, "target")?;
    let page: control_plane_core::PageReq = de_arg(&r.page_json, "page")?;
    let policies = self
        .cp
        .acl()
        .policies_for(&subject, action, &target, page)
        .await
        .map_err(status)?;
    Ok(Response::new(pb::PoliciesForResponse { page_json: se_out(&policies)? }))
}

async fn get_type(
    &self,
    req: Request<pb::GetTypeRequest>,
) -> std::result::Result<Response<pb::GetTypeResponse>, Status> {
    let name = control_plane_core::TypeName(req.into_inner().type_name);
    let ty = self.cp.ontology().get_type(&name).await.map_err(status)?;
    Ok(Response::new(pb::GetTypeResponse { object_type_json: se_out(&ty)? }))
}

async fn resolve(
    &self,
    req: Request<pb::ResolveRequest>,
) -> std::result::Result<Response<pb::ResolveResponse>, Status> {
    let name = control_plane_core::TypeName(req.into_inner().type_name);
    let table = self.cp.ontology().resolve(&name).await.map_err(status)?;
    Ok(Response::new(pb::ResolveResponse { table_ref_json: se_out(&table)? }))
}

async fn links(
    &self,
    req: Request<pb::LinksRequest>,
) -> std::result::Result<Response<pb::LinksResponse>, Status> {
    let r = req.into_inner();
    let name = control_plane_core::TypeName(r.type_name);
    let page: control_plane_core::PageReq = de_arg(&r.page_json, "page")?;
    let links = self.cp.ontology().links(&name, page).await.map_err(status)?;
    Ok(Response::new(pb::LinksResponse { page_json: se_out(&links)? }))
}

async fn links_to(
    &self,
    req: Request<pb::LinksToRequest>,
) -> std::result::Result<Response<pb::LinksToResponse>, Status> {
    let r = req.into_inner();
    let name = control_plane_core::TypeName(r.type_name);
    let page: control_plane_core::PageReq = de_arg(&r.page_json, "page")?;
    let links = self.cp.ontology().links_to(&name, page).await.map_err(status)?;
    Ok(Response::new(pb::LinksToResponse { page_json: se_out(&links)? }))
}

async fn get_action(
    &self,
    req: Request<pb::GetActionRequest>,
) -> std::result::Result<Response<pb::GetActionResponse>, Status> {
    let name = control_plane_core::ActionName(req.into_inner().action_name);
    let action = self.cp.ontology().get_action(&name).await.map_err(status)?;
    Ok(Response::new(pb::GetActionResponse { action_def_json: se_out(&action)? }))
}

async fn vector_indexes_for(
    &self,
    req: Request<pb::VectorIndexesForRequest>,
) -> std::result::Result<Response<pb::VectorIndexesForResponse>, Status> {
    let name = control_plane_core::TypeName(req.into_inner().type_name);
    let indexes = self.cp.ontology().vector_indexes_for(&name).await.map_err(status)?;
    Ok(Response::new(pb::VectorIndexesForResponse { indexes_json: se_out(&indexes)? }))
}

async fn get_vector_index(
    &self,
    req: Request<pb::GetVectorIndexRequest>,
) -> std::result::Result<Response<pb::GetVectorIndexResponse>, Status> {
    let r = req.into_inner();
    let name = control_plane_core::TypeName(r.type_name);
    let index = self
        .cp
        .ontology()
        .get_vector_index(&name, &r.name)
        .await
        .map_err(status)?;
    Ok(Response::new(pb::GetVectorIndexResponse { index_json: se_out(&index)? }))
}
```

Add the two JSON helpers near the existing `status`/`parse_id` free fns in `service.rs`:

```rust
fn de_arg<T: serde::de::DeserializeOwned>(json: &str, what: &str) -> std::result::Result<T, Status> {
    serde_json::from_str(json).map_err(|e| Status::invalid_argument(format!("bad {what}_json: {e}")))
}

fn se_out<T: serde::Serialize>(v: &T) -> std::result::Result<String, Status> {
    serde_json::to_string(v).map_err(|e| Status::internal(format!("encode failed: {e}")))
}
```

(`serde`/`serde_json` are already deps of `engine` via the existing `*_json` write handlers; if not, add them to `engine/BUCK`.)

- [ ] **Step 4: Add `spawn_engine` + `connect_gov_client` to e2e_support**

In `src/services/query-api/tests/e2e_support.rs`, refactor `spawn_engine_writer` (lines 868–947) to delegate to a new `spawn_engine` that returns the socket path, so both helpers share the server boot (avoids the duplication the `loom-duplication` routine flags). Replace the body from the `let pool = …` line onward so that:

```rust
/// Spawn an `EngineControlService` on a UDS and return its socket path + keep-alive guard.
pub async fn spawn_engine(
    fx: &control_plane_postgres::fixture::PgFixture,
    db: &str,
    warehouse: &std::path::Path,
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
) -> (String, EngineGuard) {
    // ... move the existing SqlCatalog/IcebergActionWriter/EngineControlService construction
    //     and UnixListener+Server::builder spawn here (lines 878-936), returning:
    (sock_str, EngineGuard { _sock_dir: sock_dir, handle })
}

/// Connect a raw governance/queue client to a spawned engine socket.
pub async fn connect_gov_client(sock: &str) -> engine_wire::client::GrpcQueueClient {
    engine_wire::client::GrpcQueueClient::connect(sock.to_string())
        .await
        .expect("connect GrpcQueueClient")
}
```

Then rewrite `spawn_engine_writer` to call `spawn_engine` and connect the existing `EngineActionClient`:

```rust
pub async fn spawn_engine_writer(
    fx: &control_plane_postgres::fixture::PgFixture,
    db: &str,
    warehouse: &std::path::Path,
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
) -> (query_api::engine_action_client::EngineActionClient, EngineGuard) {
    let (sock, guard) =
        spawn_engine(fx, db, warehouse, inline_byte_limit, flush_byte_threshold).await;
    let client = query_api::engine_action_client::EngineActionClient::connect(sock)
        .await
        .expect("connect EngineActionClient");
    (client, guard)
}
```

(Confirm the exact `engine_wire::client::GrpcQueueClient` path — if the crate re-exports it as `engine_wire::GrpcQueueClient`, use that. Add `//src/services/engine-wire:engine-wire` to the `e2e-support` `deps` in `BUCK` if not already present.)

- [ ] **Step 5: Add the test target to query-api/BUCK**

In `src/services/query-api/BUCK`, mirror an existing fixture-backed e2e target (e.g. `action_e2e`) using `loom_fixture_test`:

```python
loom_fixture_test(
    name = "wire-governance-e2e",
    srcs = ["tests/wire_governance_e2e.rs"],
    crate = "wire_governance_e2e",
    crate_root = "tests/wire_governance_e2e.rs",
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/services/engine-wire:engine-wire",
        "//third-party:tokio",
        # ... match the deps of the sibling action_e2e target
    ],
)
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:wire-governance-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: `Tests finished: Pass`. Also rebuild the engine to confirm it compiles with the new handlers: `buck2 build //src/services/engine:engine > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error" /tmp/b.log`. Clippy: `buck2 build '//src/services/engine:engine[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log`.

- [ ] **Step 7: Commit**

```bash
git add src/services/engine/src/service.rs src/services/query-api/tests/e2e_support.rs \
        src/services/query-api/tests/wire_governance_e2e.rs src/services/query-api/BUCK
git commit -m "feat(engine): governance-read RPC handlers + wire round-trip e2e

EngineControlService delegates the new RPCs to its own cp.acl()/ontology();
e2e proves each RPC round-trips the core payload and preserves NotFound."
```

---

## Task 5: WireAcl + WireOntology + WireControlPlane (read-only governance client)

**Files:**
- Create: `src/services/query-api/src/wire_control_plane.rs`
- Modify: `src/services/query-api/src/lib.rs`
- Append to: `src/services/query-api/tests/wire_governance_e2e.rs` (guard tests)

**Interfaces:**
- Consumes: `GrpcQueueClient::gov_*` (Task 3); the `Acl`/`Ontology`/`ControlPlane` traits and `ControlPlaneError`.
- Produces:
  - `pub struct WireAcl { client: GrpcQueueClient }` implementing `Acl` — `check`/`policies_for` delegate; all 10 write/define methods return `Err(read_only("…"))`.
  - `pub struct WireOntology { client: GrpcQueueClient }` implementing `Ontology` — `get_type`/`resolve`/`links`/`links_to`/`get_action`/`vector_indexes_for`/`get_vector_index` delegate; `list_types` + all 4 define methods return `Err(read_only("…"))`.
  - `pub struct WireControlPlane { acl: WireAcl, ontology: WireOntology, direct: Arc<dyn ControlPlane> }` implementing `ControlPlane` — `acl()`/`ontology()` return the wire adapters; `queue()` delegates to `direct.queue()`; `catalog()`/`lineage()` panic (guarded); `begin()` returns `Err`.
  - `pub fn new(client: GrpcQueueClient, direct: Arc<dyn ControlPlane>) -> WireControlPlane`.

- [ ] **Step 1: Write the failing read-only-guard tests**

Append to `src/services/query-api/tests/wire_governance_e2e.rs` (reuse the local `seed` helper + the same imports added in Task 4 Step 1):

```rust
use std::sync::Arc;

use control_plane_core::{ControlPlane, ControlPlaneError as CPE, RoleId};
use query_api::wire_control_plane::WireControlPlane;

#[tokio::test]
async fn wire_acl_is_read_only() {
    let fx = PgFixture::start();
    let (cp, db, warehouse) = seed(&fx).await;
    let (sock, _guard) =
        spawn_engine(&fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let client = connect_gov_client(&sock).await;
    let cp = Arc::new(cp);
    let wire = WireControlPlane::new(client, cp.clone() as Arc<dyn ControlPlane>);

    // Read methods work over the wire (acl()/ontology() resolve).
    assert!(wire.ontology().get_type(&TypeName("customer".into())).await.is_ok());

    // Write/define methods fail loudly rather than silently no-op.
    let err = wire.acl().define_role(&RoleId("x".into())).await.expect_err("define_role");
    assert!(matches!(err, CPE::Backend(_)));
    let customer = cp.ontology().get_type(&TypeName("customer".into())).await.unwrap();
    let err = wire.ontology().define_type(customer).await.expect_err("define_type");
    assert!(matches!(err, CPE::Backend(_)));

    // queue() delegates to the direct plane (still usable for GC enqueue).
    let _ = wire.queue(); // does not panic
}

#[tokio::test]
#[should_panic(expected = "read-only")]
async fn wire_catalog_is_guarded() {
    let fx = PgFixture::start();
    let (cp, db, warehouse) = seed(&fx).await;
    let (sock, _guard) =
        spawn_engine(&fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let client = connect_gov_client(&sock).await;
    let wire = WireControlPlane::new(client, Arc::new(cp) as Arc<dyn ControlPlane>);
    let _ = wire.catalog(); // must panic: query-api never reads catalog over this plane
}
```

(`RoleId` derives no serde and isn't sent over the wire — `define_role` short-circuits to `Err` before any RPC. `fresh_db()` returns an owned `PgControlPlane`; wrap it in `Arc` to satisfy `WireControlPlane::new`'s `Arc<dyn ControlPlane>` parameter.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test //src/services/query-api:wire-governance-e2e > /tmp/t.log 2>&1; grep -E "error|cannot|Tests finished|FAIL" /tmp/t.log`
Expected: build failure — `query_api::wire_control_plane::WireControlPlane` does not exist.

- [ ] **Step 3: Create the wire_control_plane module**

Create `src/services/query-api/src/wire_control_plane.rs`:

```rust
//! A read-only governance `ControlPlane` for query-api: `acl()`/`ontology()` read
//! over the engine wire; `queue()` delegates to the direct Postgres plane (still used
//! for the GC enqueue); `catalog()`/`lineage()`/`begin()` are guarded because query-api
//! never uses them through this plane. Write/define governance methods fail loudly —
//! query-api authorizes reads here and sends pre-authorized writes via the engine's
//! write RPCs; it never defines governance.

use std::sync::Arc;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, ActionDef, ActionName, Catalog, ControlPlane, ControlPlaneError, Decision,
    Effect, LinkDef, Lineage, ObjectType, Ontology, Page, PageReq, Policy, PolicyTarget, Queue,
    RoleId, SubjectId, TableRef, Tx, TypeName, VectorIndexDef,
};
use engine_wire::client::GrpcQueueClient;

type Result<T> = std::result::Result<T, ControlPlaneError>;

fn read_only(method: &str) -> ControlPlaneError {
    ControlPlaneError::Backend(
        format!("WireControlPlane is a read-only governance client: {method} is not supported")
            .into(),
    )
}

/// ACL reads over the engine wire; write/define methods rejected.
pub struct WireAcl {
    client: GrpcQueueClient,
}

#[async_trait]
impl Acl for WireAcl {
    async fn check(&self, subject: &SubjectId, action: Action, target: &PolicyTarget) -> Result<Decision> {
        self.client.gov_check(subject, action, target).await
    }
    async fn policies_for(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
        page: PageReq,
    ) -> Result<Page<Policy>> {
        self.client.gov_policies_for(subject, action, target, &page).await
    }

    async fn define_subject(&self, _id: &SubjectId) -> Result<()> { Err(read_only("define_subject")) }
    async fn define_role(&self, _id: &RoleId) -> Result<()> { Err(read_only("define_role")) }
    async fn assign_role(&self, _s: &SubjectId, _r: &RoleId) -> Result<()> { Err(read_only("assign_role")) }
    async fn unassign_role(&self, _s: &SubjectId, _r: &RoleId) -> Result<()> { Err(read_only("unassign_role")) }
    async fn add_role_inheritance(&self, _r: &RoleId, _i: &RoleId) -> Result<()> { Err(read_only("add_role_inheritance")) }
    async fn remove_role_inheritance(&self, _r: &RoleId, _i: &RoleId) -> Result<()> { Err(read_only("remove_role_inheritance")) }
    async fn grant(&self, _r: &RoleId, _a: Action, _t: PolicyTarget, _e: Effect) -> Result<()> { Err(read_only("grant")) }
    async fn revoke(&self, _r: &RoleId, _a: Action, _t: &PolicyTarget) -> Result<()> { Err(read_only("revoke")) }
    async fn set_policy(&self, _r: &RoleId, _a: Action, _p: Policy) -> Result<()> { Err(read_only("set_policy")) }
    async fn clear_policy(&self, _r: &RoleId, _a: Action, _t: &PolicyTarget) -> Result<()> { Err(read_only("clear_policy")) }
}

/// Ontology reads over the engine wire; write/define methods rejected.
pub struct WireOntology {
    client: GrpcQueueClient,
}

#[async_trait]
impl Ontology for WireOntology {
    async fn get_type(&self, name: &TypeName) -> Result<ObjectType> {
        self.client.gov_get_type(name).await
    }
    async fn resolve(&self, name: &TypeName) -> Result<TableRef> {
        self.client.gov_resolve(name).await
    }
    async fn links(&self, name: &TypeName, page: PageReq) -> Result<Page<LinkDef>> {
        self.client.gov_links(name, &page).await
    }
    async fn links_to(&self, name: &TypeName, page: PageReq) -> Result<Page<LinkDef>> {
        self.client.gov_links_to(name, &page).await
    }
    async fn get_action(&self, name: &ActionName) -> Result<ActionDef> {
        self.client.gov_get_action(name).await
    }
    async fn vector_indexes_for(&self, type_name: &TypeName) -> Result<Vec<VectorIndexDef>> {
        self.client.gov_vector_indexes_for(type_name).await
    }
    async fn get_vector_index(&self, type_name: &TypeName, name: &str) -> Result<Option<VectorIndexDef>> {
        self.client.gov_get_vector_index(type_name, name).await
    }

    async fn list_types(&self, _page: PageReq) -> Result<Page<ObjectType>> { Err(read_only("list_types")) }
    async fn define_type(&self, _ty: ObjectType) -> Result<()> { Err(read_only("define_type")) }
    async fn define_link(&self, _link: LinkDef) -> Result<()> { Err(read_only("define_link")) }
    async fn define_action(&self, _action: ActionDef) -> Result<()> { Err(read_only("define_action")) }
    async fn define_vector_index(&self, _def: VectorIndexDef) -> Result<()> { Err(read_only("define_vector_index")) }
}

/// Composite read-only governance plane. `queue()` delegates to `direct`.
pub struct WireControlPlane {
    acl: WireAcl,
    ontology: WireOntology,
    direct: Arc<dyn ControlPlane>,
}

impl WireControlPlane {
    #[must_use]
    pub fn new(client: GrpcQueueClient, direct: Arc<dyn ControlPlane>) -> Self {
        Self {
            acl: WireAcl { client: client.clone() },
            ontology: WireOntology { client },
            direct,
        }
    }
}

#[async_trait]
impl ControlPlane for WireControlPlane {
    fn acl(&self) -> &(dyn Acl + Send + Sync) { &self.acl }
    fn ontology(&self) -> &(dyn Ontology + Send + Sync) { &self.ontology }
    fn queue(&self) -> &(dyn Queue + Send + Sync) { self.direct.queue() }

    #[expect(clippy::panic, reason = "read-only governance client: query-api never reads the catalog through this plane")]
    fn catalog(&self) -> &(dyn Catalog + Send + Sync) {
        panic!("WireControlPlane is a read-only governance client: catalog() is not supported")
    }
    #[expect(clippy::panic, reason = "read-only governance client: query-api never reads lineage through this plane")]
    fn lineage(&self) -> &(dyn Lineage + Send + Sync) {
        panic!("WireControlPlane is a read-only governance client: lineage() is not supported")
    }
    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        Err(read_only("begin"))
    }
}
```

> **Verification before coding:** open `src/control-plane/core/src/transaction.rs`, `acl.rs`, `ontology.rs` and confirm every trait method signature above matches **exactly** (param names may differ; types/order must not). In particular confirm `WireAcl`/`WireOntology` cover **every** method of `Acl`/`Ontology` — a missing method fails to compile, an extra one too. Confirm `GrpcQueueClient` is `Clone` (it derives `Clone` per `client.rs`).

- [ ] **Step 4: Declare the module in lib.rs**

In `src/services/query-api/src/lib.rs`, add alongside the other `pub mod` declarations:

```rust
pub mod wire_control_plane;
```

Confirm `async_trait` is a dep of the query-api library (the handlers/serving use trait objects; if `async_trait` isn't already a `deps` entry in `query-api/BUCK`'s library target, add `//third-party:async-trait`). `engine-wire` is already a library dep.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `buck2 test //src/services/query-api:wire-governance-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: `Tests finished: Pass` (all round-trip + guard tests). Clippy on the library: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean).

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/wire_control_plane.rs src/services/query-api/src/lib.rs \
        src/services/query-api/tests/wire_governance_e2e.rs src/services/query-api/BUCK
git commit -m "feat(query-api): WireControlPlane read-only governance client

WireAcl/WireOntology read ACL+ontology over the engine wire; queue() delegates
to the direct plane; catalog()/lineage()/begin() and all write/define paths are
guarded. Tested for read-only contract + queue delegation."
```

---

## Task 6: Inject WireControlPlane in main.rs + governed-read parity e2e

**Files:**
- Modify: `src/services/query-api/src/main.rs`
- Create: `src/services/query-api/tests/wire_governed_read_e2e.rs`
- Modify: `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `WireControlPlane::new` (Task 5); the existing `governed_read.rs` e2e scenario helpers in `e2e_support` (`grant_read_columns`, `grant_read_filtered`, `get`, `setup_iceberg`).
- Produces: the query-api binary injects `WireControlPlane` as the `cp: Arc<dyn ControlPlane>` used by both the HTTP router and the Flight export; the Postgres pool/`pg` is retained only for `Auth`, `bootstrap_admin`, and the GC enqueue (via `WireControlPlane::queue()` → `direct`).

- [ ] **Step 1: Write the failing parity e2e**

Create `src/services/query-api/tests/wire_governed_read_e2e.rs`. It reproduces the `governed_read.rs` deny-column + row-filter scenario, then runs `read_object` twice over the **same** in-process serving engine — once with governance read directly from `cp`, once with governance read over the wire via `WireControlPlane` — and asserts the governed `ObjectRows` are identical. Governance crosses the wire (the spawned engine reads the same Postgres db); only the data path stays in-process, which is exactly what this slice changes.

```rust
//! A governed object read (deny-column + row-filter) returns IDENTICAL ObjectRows
//! whether governance (ontology + ACL) is read direct from PgControlPlane or over the
//! engine wire via WireControlPlane. The serving engine (data) is the same in-process
//! Iceberg engine in both legs; only the governance transport differs.

use std::sync::Arc;

use control_plane_core::{
    Acl, Action, CompareOp, ControlPlane, Effect, ObjectType, Ontology, Policy, PolicyTarget,
    PropertyDef, RoleId, RowFilter, ScalarValue, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{InProcessServingEngine, connect_gov_client, spawn_engine};
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::serving::SqlValue;
use query_api::wire_control_plane::WireControlPlane;

#[tokio::test(flavor = "multi_thread")]
async fn governed_read_parity_over_wire() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");

    // 1. Seed orders(id, status, secret): (1,'open','s1'),(2,'closed','s2'),(3,'open','s3').
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("status".to_string(), "string".to_string(), true),
        ("secret".to_string(), "string".to_string(), true),
    ];
    writer
        .seed_arrays(
            "main",
            "orders",
            &cols,
            &[
                SeedCol::Long(vec![1, 2, 3]),
                SeedCol::Str(vec!["open", "closed", "open"]),
                SeedCol::Str(vec!["s1", "s2", "s3"]),
            ],
        )
        .await;

    // 2. Ontology: type Order -> main.orders.
    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![
            PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
            PropertyDef { name: "status".into(), ty: "String".into(), required: false },
            PropertyDef { name: "secret".into(), ty: "String".into(), required: false },
        ],
        derived: vec![],
        table: TableRef { schema: "main".into(), name: "orders".into() },
        identity: None,
    })
    .await
    .unwrap();

    // 3. ACL: analyst can Read Order; policy denies `secret`, restricts rows to status='open'.
    let subj = SubjectId("analyst".into());
    let role = RoleId("analysts".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(&role, Action::Read, PolicyTarget::Type(TypeName("Order".into())), Effect::Allow)
        .await
        .unwrap();
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: Some(RowFilter::Compare {
                property: "status".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("open".into()),
            }),
            deny_columns: vec!["secret".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // 4. One in-process serving engine over the seeded catalog (data path, both legs).
    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);

    // 5. Spawn the engine for governance-over-wire (it reads the SAME Postgres db);
    //    build WireControlPlane. The warehouse it gets is unused by governance reads.
    let (sock, _guard) =
        spawn_engine(&fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let cp = Arc::new(cp);
    let wire = WireControlPlane::new(connect_gov_client(&sock).await, cp.clone() as Arc<dyn ControlPlane>);

    // 6. Same query + subject, governance direct vs over the wire.
    let q = ObjectQuery { type_name: "Order".into(), eq_filters: vec![], ids: vec![] };
    let s = Subject(subj.clone());
    let direct = read_object(
        &q,
        &s,
        &QueryDeps { ontology: cp.ontology(), acl: cp.acl(), serving: &eng, default_limit: 1000 },
    )
    .await
    .expect("direct governed read");
    let over_wire = read_object(
        &q,
        &s,
        &QueryDeps { ontology: wire.ontology(), acl: wire.acl(), serving: &eng, default_limit: 1000 },
    )
    .await
    .expect("wire governed read");

    // Parity: identical projection, logical types, and rows.
    assert_eq!(direct.columns, over_wire.columns);
    assert_eq!(direct.logical_types, over_wire.logical_types);
    assert_eq!(direct.rows, over_wire.rows);

    // And the governance actually applied over the wire: `secret` dropped, only open rows.
    assert_eq!(over_wire.columns, vec!["id".to_string(), "status".to_string()]);
    assert_eq!(over_wire.rows.len(), 2, "row filter kept only status=open");
    let ids: Vec<&SqlValue> = over_wire.rows.iter().map(|r| &r[0]).collect();
    assert!(ids.contains(&&SqlValue::Int(1)) && ids.contains(&&SqlValue::Int(3)));

    drop(writer);
}
```

> This faithfully reuses the `governed_read.rs` oracle (lines 17-140) — same seed, same grants/policy, same `read_object`/`QueryDeps`/`Subject` API — so it needs no new `e2e_support` driver. `wire.ontology()`/`wire.acl()` return `&(dyn Ontology + Send + Sync)` / `&(dyn Acl + Send + Sync)`, which are exactly the `QueryDeps` field types. If `read_object`'s `ObjectRows` field names differ, match the assertions in `governed_read.rs`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:wire-governed-read-e2e > /tmp/t.log 2>&1; grep -E "error|cannot|Tests finished|FAIL" /tmp/t.log`
Expected: build failure or assertion gap until the helper/driver and (if needed) `get`-over-`dyn ControlPlane` exist. (The production swap in Step 3 is independent — this test constructs `WireControlPlane` directly — but keep it here to prove parity end-to-end.)

- [ ] **Step 3: Swap the injected governance plane in main.rs**

In `src/services/query-api/src/main.rs`, after `engine_socket` is resolved (it is needed for the client) and after `pg` is built, replace the `let cp: Arc<dyn ControlPlane> = pg.clone();` line (currently line 31) with a `WireControlPlane`. Because the client needs `engine_socket`, move the `cp` construction below the `engine_socket` resolution (lines 33–36). Concretely:

```rust
    // Concrete PgControlPlane: retained for Auth, bootstrap_admin, and the GC enqueue
    // (via WireControlPlane::queue()). Governance reads (ACL + ontology) go over the wire.
    let pg = Arc::new(service_runtime::control_plane(pool.clone(), cfg.lock_timeout));

    let engine_socket =
        std::env::var("LOOM_ENGINE_SOCKET").map_err(|e| -> Box<dyn std::error::Error> {
            format!("LOOM_ENGINE_SOCKET must be set for the Iceberg serving backend: {e}").into()
        })?;

    // Governance reads relocate to the engine wire; queue() still delegates to `pg`.
    let gov_client = engine_wire::client::GrpcQueueClient::connect(engine_socket.clone()).await?;
    let cp: Arc<dyn ControlPlane> = Arc::new(query_api::wire_control_plane::WireControlPlane::new(
        gov_client,
        pg.clone(),
    ));
```

Leave the rest unchanged: `auth_state.auth = pg.clone()`, `bootstrap_admin(pg.as_ref(), …)`, `cp_flight = cp.clone()` (the Flight export now reads governance over the wire too — correct and intended), and `AppState { cp, … }`. Confirm `engine_wire` is a dep of the query-api **binary** target in `BUCK` (it is, transitively via the library; add an explicit `//src/services/engine-wire:engine-wire` to the binary `deps` if the `engine_wire::client::GrpcQueueClient` path doesn't resolve in `main.rs`).

- [ ] **Step 4: Add the test target and run the full query-api suite**

Add a `loom_fixture_test` target `wire-governed-read-e2e` to `src/services/query-api/BUCK` (mirror `wire-governance-e2e` deps). Then run the **entire** query-api test suite to prove behaviour preservation (the existing direct-cp suites must stay green, and the new wire suites pass):

Run: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all pass. Build the binary too: `buck2 build //src/services/query-api:query-api > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error" /tmp/b.log`. Clippy: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log`.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/main.rs src/services/query-api/tests/wire_governed_read_e2e.rs \
        src/services/query-api/BUCK
git commit -m "feat(query-api): read governance over the engine wire

main.rs injects WireControlPlane as the governance ControlPlane for both the HTTP
router and Flight export; the Postgres pool is retained only for Auth, bootstrap,
and the GC enqueue. Governed-read parity proven over the wire."
```

---

## Task 7: Full-tree verification + docs register update

**Files:**
- Modify: `docs/ROADMAP.md` / `docs/FUTURE.md` (via `loom-docs-update`)

- [ ] **Step 1: Run the full first-party suite**

Run: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all pass (the engine, engine-wire, core, and query-api crates all build and test green). If any fixture test flakes on resource contention (a known cloud issue), re-run that target alone to confirm.

- [ ] **Step 2: Run all clippy + prek hooks**

Run: `./tools/clippy-all.sh > /tmp/c.log 2>&1; tail -5 /tmp/c.log` (expect clean) and `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -iE "failed|error" /tmp/p.log` (commit any hook fixes).

- [ ] **Step 3: Update the documentation registers**

Invoke `loom-docs-update`: mark `road-query-api-wire-governance` `- [x]` with terminal status and `pr:#<N>`; confirm the three follow-on FUTURE items the spec records exist and stay open — `fut-auth-wire-resolve`, `fut-queue-wire-enqueue`, `fut-wire-governance-cache` (the credential-free-binary + caching follow-ons). Stage the register edits alongside the work.

- [ ] **Step 4: Commit the docs**

```bash
git add docs
git commit -m "docs(registers): close road-query-api-wire-governance"
```

---

## Self-Review

**1. Spec coverage:**
- *New EngineControl governance-read RPCs (ACL PoliciesFor/Check; ontology GetType/Resolve/Links/LinksTo/GetAction/VectorIndexesFor/GetVectorIndex) + engine-side handlers* → Task 2 (proto) + Task 4 (handlers). ✓
- *serde derives on the ontology domain types the RPCs carry* → Task 1 (ontology + the ACL/catalog/vector/page types in the closure). ✓
- *query-api WireAcl + WireOntology + WireControlPlane composite; flipping governance reads onto it; main.rs injection* → Task 5 (adapters) + Task 6 (main.rs). The "flip" is the construction swap only — handlers read through `cp.acl()`/`cp.ontology()` already, so no handler edits (documented). ✓
- *queue() stays direct Postgres; catalog()/lineage() guarded; write/define unimplemented-guarded; read-only client* → Task 5. Write/define return loud `Err`; `catalog()`/`lineage()` panic-guard (they return `&dyn` so can't return `Err`); `begin()` returns `Err`. ✓
- *Testing 1 (behaviour-preserving e2e)* → Task 6 Step 4 runs the full existing suite (direct-cp, unchanged) + the new wire parity test. ✓
- *Testing 2 (RPC round-trips)* → Task 4 round-trip test. ✓
- *Testing 3 (governance parity incl. NotFound)* → Task 4 (NotFound parity) + Task 6 (masked/filtered parity). ✓
- *Testing 4 (read-only guard fails loudly)* → Task 5 guard tests. ✓
- *Out of scope (auth over wire, GC enqueue over wire, caching, catalog/lineage over wire, TLS)* → none implemented; queue/auth stay direct; follow-on FUTURE items reaffirmed in Task 7. ✓

**2. Placeholder scan:** Code steps carry full code. Two deliberate "verify the real signature" notes (`setup_iceberg`'s return shape; the `governed_read.rs` scenario body) are inspection instructions, not placeholders — the surrounding harness and the assertion contract are fully specified, and the exact values live in files the implementer reads. The trait-method coverage note in Task 5 is a compile-enforced checklist, not a gap.

**3. Type consistency:** `gov_*` client method names and signatures in Task 3's Interfaces match their call sites in Task 5. `cp_status`/`de`/`se` (client) vs `status`/`de_arg`/`se_out` (engine) are deliberately distinct (different error targets: `ControlPlaneError` vs `Status`). `WireControlPlane::new(client, direct)` arg order matches Tasks 5 and 6. `PageReq` is passed by value to the trait methods (`policies_for(… page: PageReq)`, `links(… page: PageReq)`) and by reference to the client (`gov_*(… page: &PageReq)`) — the adapter bridges `&page`. `read_only(&str) -> ControlPlaneError::Backend` is used uniformly for every rejected method.
