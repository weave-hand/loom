# Engine-Wire Flush Vertical Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A new `engine` process owns Postgres and serves a tonic `EngineControl` over a unix socket; a new zero-pool `worker` process drains `flush_table` jobs and runs the flush, all over the wire.

**Architecture:** Three crates — `engine-wire` (shared proto + generated tonic + `core↔proto` translation + `GrpcQueueClient`, NO postgres dep), `engine` (binary: `EngineControlService` over `PgControlPlane` + `SqlCatalog`), `worker` (binary: the unchanged `Worker<GrpcQueueClient>` loop). Codegen is pure-Rust via `protox` (no `protoc`) through a buck2 genrule.

**Tech Stack:** Rust 2024, buck2, tonic/prost/protox, reindeer, Postgres, `loom_fixture_test`, `Worker<Q>`.

## Global Constraints

- **No inline `#[cfg(test)]`** — tests are `rust_test`/`loom_fixture_test` targets in `tests/<name>.rs`; a prek hook fails on `#[test]` in `src/**`.
- **Fixture tests use `loom_fixture_test`** (`src/control-plane/postgres/defs.bzl`), not bare `rust_test`.
- **Codegen via `protox` — NO `protoc`/no external compiler.** `protox::compile(...)` → `FileDescriptorSet` → `tonic_prost_build::configure().compile_fds(...)`.
- **Zero-pool is structural:** the `worker` crate's BUCK target must NOT depend on `//src/control-plane/postgres`.
- **`enqueue` is not on the wire** — the producer enqueues in PG directly; `GrpcQueueClient::enqueue` returns an unsupported error.
- **`reindeer update` can silently downgrade `duckdb`** — after any dep change run the **full** `buck2 test //src/...` and diff `Cargo.lock` vs the merge-base for native crates (`libduckdb-sys`); fix via `cargo update -p duckdb --precise 1.10503.1` + `./tools/buckify.sh` if it moved (CLAUDE.md).
- **buck2 command forms (for implementers):** run BARE — no `>` redirect, no `|` pipe, no `;`/`&&` compound (those auto-deny non-interactively). `buck2 test //target` prints to stdout (no pipe = no stall). Format with `buck2 run //tools:rustfmt -- <files>` (not `cargo fmt`). Commit with plain `git commit -m … -m …`. Run `tools/buckify.sh`, `tools/sqlx-prepare.sh`, the full suite, and any redirected/compound command from the CONTROLLER (main loop), not a subagent.
- **Commit trailer:** `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- **Branch:** `spec/engine-wire-flush-vertical` (already created off `main`; the spec is committed on it).

---

### Task 1: Codegen pipe de-risk — vendor the tonic stack + a `Ping` RPC end to end

The riskiest unknown (loom's first proto codegen in buck2) proven on ONE trivial RPC before any real surface. Deliverable: a generated `pb` module compiles and a unit test round-trips a `Ping` message.

**Files:**
- Create: `src/services/engine-wire/Cargo.toml`, `src/services/engine-wire/proto/ping.proto`, `src/services/engine-wire/codegen/main.rs`, `src/services/engine-wire/src/lib.rs`, `src/services/engine-wire/tests/pb_roundtrip.rs`, `src/services/engine-wire/BUCK`
- Modify: `Cargo.toml` (workspace members), `third-party/BUCK` (regenerated)

**Interfaces:**
- Produces: a buck2 target `//src/services/engine-wire:engine-wire` exposing a `pb` module with the generated tonic types; the proven codegen pattern (codegen `rust_binary` + `genrule` + `env`-location include) that Task 3 reuses for the real proto.

- [ ] **Step 1: Create the engine-wire crate manifest with the tonic stack**

`src/services/engine-wire/Cargo.toml`:

```toml
[package]
name = "engine-wire"
version = "0.1.0"
edition = "2024"

[dependencies]
tonic = "0.13"
prost = "0.13"
prost-types = "0.13"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net"] }
tokio-stream = { version = "0.1", features = ["net"] }
tower = "0.5"
hyper-util = "0.1"
control-plane-core = { path = "../../control-plane/core" }

[build-dependencies]
protox = "0.7"
prost-build = "0.13"
tonic-prost-build = "0.13"
```

> Versions are a starting point — the de-risk is partly *which* versions resolve cleanly. Pin whatever `cargo` resolves; record the resolved versions in your report. `tonic`/`prost`/`protox`/`tonic-prost-build` are all pure-Rust (per `docs/spike/engine-wire-transport.md`) — no `protoc`, no `links` crates.

- [ ] **Step 2: Add the crate to the workspace and vendor the deps**

Add `"src/services/engine-wire"` to `members` in the root `Cargo.toml`. Then (CONTROLLER runs these — they redirect/chain and touch the lock):

```
eval "$(./tools/env.sh)" && cargo generate-lockfile
./tools/buckify.sh
```

Then the **full** suite as the dep-graph guard, and diff the lock for native crates:

```
buck2 test //src/...
git diff origin/main -- Cargo.lock | grep -iE "libduckdb-sys|duckdb|zstd-sys|ring" | head
```

Expected: suite green; no `duckdb` version movement. If `duckdb` moved, `cargo update -p duckdb --precise 1.10503.1` + `./tools/buckify.sh`.

- [ ] **Step 3: Write the throwaway `ping.proto`**

`src/services/engine-wire/proto/ping.proto`:

```protobuf
syntax = "proto3";
package loom.ping.v1;

message PingRequest { string note = 1; }
message PingResponse { string note = 1; }

service Ping {
  rpc Ping(PingRequest) returns (PingResponse);
}
```

- [ ] **Step 4: Write the codegen binary**

`src/services/engine-wire/codegen/main.rs` — argv: `<proto_file> <out_dir>`. Uses protox (no protoc) → FileDescriptorSet → tonic codegen:

```rust
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let proto = PathBuf::from(args.next().expect("usage: codegen <proto> <out_dir>"));
    let out_dir = PathBuf::from(args.next().expect("usage: codegen <proto> <out_dir>"));
    let include = proto.parent().unwrap().to_path_buf();

    // protox: pure-Rust protobuf compiler — emits a FileDescriptorSet, no protoc.
    let fds = protox::compile([&proto], [&include])?;

    // tonic-prost-build consumes the FDS directly (no protoc invocation).
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir(&out_dir)
        .compile_fds(fds)?;
    Ok(())
}
```

> The exact `tonic_prost_build` API (`configure()`, `compile_fds`) is what to verify in this de-risk. If the resolved crate version exposes a different entry point (e.g. `compile_fds` lives on a different builder, or `protox::compile` returns a typed wrapper needing `.file_descriptor_set()`), adjust here — that discovery is the point of Task 1. Record the working form in your report.

- [ ] **Step 5: Write the lib that includes the generated module**

`src/services/engine-wire/src/lib.rs`:

```rust
//! loom engine-wire: the shared tonic contract between the engine (server) and
//! clients (the worker). Generated code is included from the codegen genrule via
//! the ENGINE_PB env-location (mirrors the postgres crate's SQLX_OFFLINE_DIR).

/// Generated protobuf + tonic client/server stubs.
pub mod pb {
    include!(concat!(env!("ENGINE_PB"), "/loom.ping.v1.rs"));
}
```

> The generated file name follows the proto `package` (`loom.ping.v1` → `loom.ping.v1.rs`) by default with `out_dir`. If the resolved tonic version names it differently (e.g. `ping.rs`), match the actual emitted filename — check the genrule output dir.

- [ ] **Step 6: Write the BUCK file (codegen binary + genrule + library + test)**

`src/services/engine-wire/BUCK`. Mirror the genrule/`$(exe)`/`$(location)` and `env`-location patterns from `src/control-plane/postgres/BUCK` (`:sqlx-cache` + the `libxml2`/`duckdb-cli` genrules):

```python
load("@prelude//rust:cargo_package.bzl", "cargo")

# Pure-Rust codegen tool: protox + tonic-prost-build, no protoc.
cargo.rust_binary(
    name = "codegen",
    crate = "engine_wire_codegen",
    srcs = ["codegen/main.rs"],
    crate_root = "codegen/main.rs",
    edition = "2024",
    deps = [
        "//third-party:protox",
        "//third-party:prost-build",
        "//third-party:tonic-prost-build",
    ],
)

# Run the codegen tool over the proto, emitting generated .rs into $OUT.
genrule(
    name = "pb-gen",
    out = "gen",
    cmd = "mkdir -p $OUT && $(exe :codegen) $(location proto/ping.proto) $OUT",
)

cargo.rust_library(
    name = "engine-wire",
    crate = "engine_wire",
    srcs = ["src/lib.rs"],
    crate_root = "src/lib.rs",
    edition = "2024",
    env = {"ENGINE_PB": "$(location :pb-gen)"},
    deps = [
        "//third-party:tonic",
        "//third-party:prost",
        "//third-party:prost-types",
        "//third-party:tokio",
        "//third-party:tokio-stream",
        "//third-party:tower",
        "//third-party:hyper-util",
        "//src/control-plane/core:core",
    ],
    visibility = ["PUBLIC"],
)

cargo.rust_test(
    name = "pb-roundtrip",
    crate = "pb_roundtrip",
    srcs = ["tests/pb_roundtrip.rs"],
    crate_root = "tests/pb_roundtrip.rs",
    edition = "2024",
    deps = [":engine-wire", "//third-party:prost"],
)
```

> Verify the third-party alias names match what `buckify.sh` generated (e.g. `//third-party:tonic-prost-build` vs `//third-party:tonic_prost_build`). `grep -n "name = " third-party/BUCK | grep -iE "tonic|prost|protox|tower|hyper-util"` to confirm the exact aliases, and fix the deps lists accordingly.

- [ ] **Step 7: Write the round-trip test**

`src/services/engine-wire/tests/pb_roundtrip.rs`:

```rust
//! Proves the codegen pipe: the generated prost types encode/decode.
use engine_wire::pb::{PingRequest, PingResponse};
use prost::Message;

#[test]
fn ping_request_roundtrips() {
    let req = PingRequest { note: "hi".into() };
    let bytes = req.encode_to_vec();
    let back = PingRequest::decode(bytes.as_slice()).unwrap();
    assert_eq!(back.note, "hi");
}

#[test]
fn ping_response_roundtrips() {
    let resp = PingResponse { note: "ok".into() };
    let back = PingResponse::decode(resp.encode_to_vec().as_slice()).unwrap();
    assert_eq!(back.note, "ok");
}
```

- [ ] **Step 8: Build + test, format, commit**

```
buck2 test //src/services/engine-wire:pb-roundtrip
buck2 run //tools:rustfmt -- src/services/engine-wire/src/lib.rs src/services/engine-wire/codegen/main.rs src/services/engine-wire/tests/pb_roundtrip.rs
```

Expected: `pb-roundtrip` Pass 2. Then commit:

```
git add src/services/engine-wire Cargo.toml Cargo.lock third-party/BUCK
git commit -m "feat(engine-wire): vendor tonic stack + prove the protox codegen pipe (Ping RPC)" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

> If the buck2 codegen wiring fights you after a genuine effort (the genrule can't run the binary, the env-include path is wrong, the alias names mismatch), STOP and report BLOCKED with the exact error — this is the de-risk task; the controller adjusts the approach rather than the implementer thrashing.

---

### Task 2: Move the flush-job contract to `core`

So the zero-pool worker can read `FlushJob` without depending on `control-plane-postgres`. Small, independent refactor of Spec 1 code.

**Files:**
- Modify: `src/control-plane/core/src/lib.rs` (or a new `src/control-plane/core/src/flush.rs`), `src/control-plane/postgres/src/iceberg_flush.rs`, `src/control-plane/core/BUCK` (serde dep if missing)
- Test: `src/control-plane/core/tests/flush_job.rs`

**Interfaces:**
- Produces: `control_plane_core::{FlushJob, FLUSH_JOB_KIND}` with the same shape as Spec 1.
- Consumes: nothing.

- [ ] **Step 1: Add `FlushJob` + `FLUSH_JOB_KIND` to core**

Create `src/control-plane/core/src/flush.rs`:

```rust
//! The flush-table job contract, shared by the producer (postgres `inline_append`)
//! and the consumer (the worker). Lives in core so a zero-pool worker can read it
//! without depending on the postgres adapter.

/// The queue `kind` for an inline-flush job.
pub const FLUSH_JOB_KIND: &str = "flush_table";

/// The payload of a `flush_table` job: which table to flush.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct FlushJob {
    pub schema: String,
    pub name: String,
}
```

Add `pub mod flush;` and re-export in `src/control-plane/core/src/lib.rs` (`pub use flush::{FlushJob, FLUSH_JOB_KIND};`). Ensure `core`'s BUCK target depends on `//third-party:serde` (with derive); add it if absent.

- [ ] **Step 2: Remove the duplicates from postgres and re-import**

In `src/control-plane/postgres/src/iceberg_flush.rs`, delete the local `FLUSH_JOB_KIND` const and `FlushJob` struct (Spec 1 added them near the top). Replace their uses with the core ones: add `use control_plane_core::{FlushJob, FLUSH_JOB_KIND};` (or reference via the existing `control_plane_core` import) and update any `crate::iceberg_flush::FLUSH_JOB_KIND` references (e.g. in `iceberg_inline.rs`) to `control_plane_core::FLUSH_JOB_KIND`.

- [ ] **Step 3: Write a contract test**

`src/control-plane/core/tests/flush_job.rs`:

```rust
use control_plane_core::{FlushJob, FLUSH_JOB_KIND};

#[test]
fn flush_job_serde_roundtrip_and_kind() {
    assert_eq!(FLUSH_JOB_KIND, "flush_table");
    let j = FlushJob { schema: "wh".into(), name: "t".into() };
    let v = serde_json::to_value(&j).unwrap();
    assert_eq!(v["schema"], "wh");
    assert_eq!(v["name"], "t");
    let back: FlushJob = serde_json::from_value(v).unwrap();
    assert_eq!(back.schema, "wh");
    assert_eq!(back.name, "t");
}
```

Wire a `flush-job` `rust_test` target in `src/control-plane/core/BUCK` (mirror an existing core test target; deps `:core`, `//third-party:serde_json`).

- [ ] **Step 4: Build, test, full inline-flush regression, commit**

```
buck2 test //src/control-plane/core:flush-job //src/control-plane/postgres:inline-flush-trigger
buck2 run //tools:rustfmt -- src/control-plane/core/src/flush.rs src/control-plane/core/src/lib.rs src/control-plane/postgres/src/iceberg_flush.rs
```

Expected: both green (the trigger still enqueues with the re-homed contract). Commit:

```
git add src/control-plane/core src/control-plane/postgres/src/iceberg_flush.rs src/control-plane/postgres/src/iceberg_inline.rs
git commit -m "refactor(core): move FlushJob + FLUSH_JOB_KIND from postgres to core" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: The real `engine_control.proto` + `core↔proto` translation

Replace `ping.proto` with the real surface and add the pure translation layer (unit-tested without any server).

**Files:**
- Delete: `src/services/engine-wire/proto/ping.proto`, `src/services/engine-wire/tests/pb_roundtrip.rs`
- Create: `src/services/engine-wire/proto/engine_control.proto`, `src/services/engine-wire/src/convert.rs`, `src/services/engine-wire/tests/convert.rs`
- Modify: `src/services/engine-wire/src/lib.rs` (pb module path), `src/services/engine-wire/BUCK` (genrule proto path, test target)

**Interfaces:**
- Consumes: the codegen pattern from Task 1; `control_plane_core::{Job, JobId, RetryPolicy}`.
- Produces: `engine_wire::pb` (the 6-RPC generated stubs); `engine_wire::convert` with `job_to_pb(&Job) -> pb::Job`, `job_from_pb(pb::Job) -> Result<Job, ConvertError>`, `retry_policy_to_pb`/`_from_pb`, and `ConvertError`.

- [ ] **Step 1: Write `engine_control.proto`**

`src/services/engine-wire/proto/engine_control.proto` — exactly the surface from the spec (`docs/superpowers/specs/2026-06-20-engine-wire-flush-vertical-design.md`, "The `EngineControl` surface"). Copy that proto block verbatim.

- [ ] **Step 2: Point the genrule + lib at the new proto**

In `src/services/engine-wire/BUCK`, change the `:pb-gen` genrule's `$(location proto/ping.proto)` → `$(location proto/engine_control.proto)`. In `src/lib.rs`, change the include to `concat!(env!("ENGINE_PB"), "/loom.engine.v1.rs")` (matching the new package; verify the emitted filename). Remove the `:pb-roundtrip` target.

- [ ] **Step 3: Write the failing translation test**

`src/services/engine-wire/tests/convert.rs`:

```rust
use control_plane_core::{Job, JobId, RetryPolicy};
use engine_wire::convert::{job_from_pb, job_to_pb, retry_policy_from_pb, retry_policy_to_pb};
use std::time::Duration;

#[test]
fn job_roundtrips_through_pb() {
    let id = uuid::Uuid::new_v4();
    let job = Job {
        id: JobId(id),
        kind: "flush_table".into(),
        payload: serde_json::json!({"schema":"wh","name":"t"}),
        attempts: 2,
        run_at: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
    };
    let back = job_from_pb(job_to_pb(&job)).unwrap();
    assert_eq!(back.id.0, id);
    assert_eq!(back.kind, "flush_table");
    assert_eq!(back.payload["schema"], "wh");
    assert_eq!(back.attempts, 2);
    assert_eq!(back.run_at.unix_timestamp(), 1_700_000_000);
}

#[test]
fn retry_policy_roundtrips() {
    let r = retry_policy_from_pb(retry_policy_to_pb(&RetryPolicy::Retry { delay: Duration::from_millis(250) }));
    assert!(matches!(r, Ok(RetryPolicy::Retry { delay }) if delay == Duration::from_millis(250)));
    let a = retry_policy_from_pb(retry_policy_to_pb(&RetryPolicy::Abandon));
    assert!(matches!(a, Ok(RetryPolicy::Abandon)));
}
```

Add a `convert` `rust_test` target in BUCK (deps `:engine-wire`, `//third-party:control-plane-core`… i.e. `//src/control-plane/core:core`, `//third-party:serde_json`, `//third-party:uuid`, `//third-party:time`).

- [ ] **Step 4: Run to verify it fails**

```
buck2 test //src/services/engine-wire:convert
```

Expected: FAIL to compile — `engine_wire::convert` doesn't exist yet.

- [ ] **Step 5: Implement `convert.rs`**

`src/services/engine-wire/src/convert.rs`:

```rust
//! core <-> pb translation. Pure, no I/O. Payload crosses as a JSON string;
//! JobId as a Uuid string; run_at as unix microseconds; RetryPolicy as a oneof.
use control_plane_core::{Job, JobId, RetryPolicy};
use crate::pb;

#[derive(Debug)]
pub struct ConvertError(pub String);

pub fn job_to_pb(j: &Job) -> pb::Job {
    pb::Job {
        id: j.id.0.to_string(),
        kind: j.kind.clone(),
        payload: j.payload.to_string(),
        attempts: j.attempts,
        run_at: (j.run_at.unix_timestamp_nanos() / 1_000) as i64, // micros
    }
}

pub fn job_from_pb(p: pb::Job) -> Result<Job, ConvertError> {
    Ok(Job {
        id: JobId(p.id.parse().map_err(|e| ConvertError(format!("job id: {e}")))?),
        kind: p.kind,
        payload: serde_json::from_str(&p.payload).map_err(|e| ConvertError(format!("payload: {e}")))?,
        attempts: p.attempts,
        run_at: time::OffsetDateTime::from_unix_timestamp_nanos((p.run_at as i128) * 1_000)
            .map_err(|e| ConvertError(format!("run_at: {e}")))?,
    })
}

pub fn retry_policy_to_pb(r: &RetryPolicy) -> pb::RetryPolicy {
    use pb::retry_policy::Kind;
    pb::RetryPolicy {
        kind: Some(match r {
            RetryPolicy::Retry { delay } => Kind::RetryDelayMs(delay.as_millis() as i64),
            RetryPolicy::Abandon => Kind::Abandon(pb::Abandon {}),
        }),
    }
}

pub fn retry_policy_from_pb(p: pb::RetryPolicy) -> Result<RetryPolicy, ConvertError> {
    use pb::retry_policy::Kind;
    match p.kind {
        Some(Kind::RetryDelayMs(ms)) => Ok(RetryPolicy::Retry { delay: std::time::Duration::from_millis(ms.max(0) as u64) }),
        Some(Kind::Abandon(_)) => Ok(RetryPolicy::Abandon),
        None => Err(ConvertError("retry policy: empty oneof".into())),
    }
}
```

Add `pub mod convert;` to `src/lib.rs`.

> The generated names (`pb::retry_policy::Kind::RetryDelayMs`, `pb::Abandon`) follow prost's conventions from the proto field names. If they differ, match the generated module — inspect the genrule output.

- [ ] **Step 6: Build, test, format, commit**

```
buck2 test //src/services/engine-wire:convert
buck2 run //tools:rustfmt -- src/services/engine-wire/src/lib.rs src/services/engine-wire/src/convert.rs src/services/engine-wire/tests/convert.rs
```

Expected: `convert` Pass 2. Commit:

```
git add src/services/engine-wire
git commit -m "feat(engine-wire): real EngineControl proto + core<->pb translation" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: The engine binary + `EngineControlService` + `GrpcQueueClient` (the working wire)

Both halves of the wire and an integration test proving queue ops + flush round-trip over a UDS.

**Files:**
- Create: `src/services/engine-wire/src/client.rs` (`GrpcQueueClient`), `src/services/engine/Cargo.toml`, `src/services/engine/src/service.rs` (`EngineControlService`), `src/services/engine/src/main.rs`, `src/services/engine/BUCK`, `src/services/engine/tests/wire.rs`
- Modify: `src/services/engine-wire/src/lib.rs` (`pub mod client;`), `src/services/engine-wire/BUCK` (client deps), `Cargo.toml` (workspace members)

**Interfaces:**
- Consumes: `engine_wire::{pb, convert}`; `control_plane_core::Queue` + types; `control_plane_postgres::{PgControlPlane, iceberg_sql_catalog::SqlCatalog, iceberg_flush::flush_table}`; `control_plane_core::{TableRef, RunId}`; `service_runtime`.
- Produces: `engine_wire::client::GrpcQueueClient` (impl `Queue` + `async fn flush_table(&self, schema, name) -> Result<Option<i64>>`); `engine::service::EngineControlService`.

- [ ] **Step 1: Implement `GrpcQueueClient` (client adapter)**

`src/services/engine-wire/src/client.rs`. Wraps `pb::engine_control_client::EngineControlClient<tonic::transport::Channel>`; connects over a UDS; implements `Queue` (mapping `tonic::Status` → `ControlPlaneError::Backend`); `enqueue` returns an unsupported error; adds `flush_table`. Connect pattern (tonic UDS):

```rust
use control_plane_core::{ControlPlaneError, Job, JobId, NewJob, Queue, Result, RetryPolicy};
use crate::{convert, pb};
use crate::pb::engine_control_client::EngineControlClient;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

#[derive(Clone)]
pub struct GrpcQueueClient {
    inner: EngineControlClient<Channel>,
}

fn be<E: std::fmt::Display>(e: E) -> ControlPlaneError {
    ControlPlaneError::Backend(e.to_string().into())
}

impl GrpcQueueClient {
    pub async fn connect(socket: impl Into<String>) -> Result<Self> {
        let socket = socket.into();
        // The URI is ignored by the connector; the connector dials the UDS.
        let channel = Endpoint::try_from("http://[::]:50051")
            .map_err(be)?
            .connect_with_connector(service_fn(move |_: Uri| {
                let socket = socket.clone();
                async move {
                    let stream = tokio::net::UnixStream::connect(socket).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await
            .map_err(be)?;
        Ok(Self { inner: EngineControlClient::new(channel) })
    }

    pub async fn flush_table(&self, schema: String, name: String) -> Result<Option<i64>> {
        let resp = self.inner.clone()
            .flush_table(pb::FlushTableRequest { schema, name }).await
            .map_err(be)?.into_inner();
        Ok(resp.snapshot_id)
    }
}

#[async_trait::async_trait]
impl Queue for GrpcQueueClient {
    async fn enqueue(&self, _job: NewJob) -> Result<JobId> {
        Err(ControlPlaneError::Backend("enqueue is not available over the engine-wire".into()))
    }
    async fn dequeue(&self, kinds: &[String], worker: &str) -> Result<Option<Job>> {
        let resp = self.inner.clone()
            .dequeue(pb::DequeueRequest { kinds: kinds.to_vec(), worker: worker.into() }).await
            .map_err(be)?.into_inner();
        resp.job.map(convert::job_from_pb).transpose().map_err(|e| be(e.0))
    }
    async fn complete(&self, id: JobId) -> Result<()> {
        self.inner.clone().complete(pb::CompleteRequest { id: id.0.to_string() }).await.map_err(be)?;
        Ok(())
    }
    async fn fail(&self, id: JobId, error: &str, policy: RetryPolicy) -> Result<()> {
        self.inner.clone().fail(pb::FailRequest {
            id: id.0.to_string(), error: error.into(),
            policy: Some(convert::retry_policy_to_pb(&policy)),
        }).await.map_err(be)?;
        Ok(())
    }
    async fn heartbeat(&self, id: JobId) -> Result<()> {
        self.inner.clone().heartbeat(pb::HeartbeatRequest { id: id.0.to_string() }).await.map_err(be)?;
        Ok(())
    }
    async fn await_jobs(&self, kinds: &[String], timeout: std::time::Duration) -> Result<()> {
        let mut req = tonic::Request::new(pb::AwaitJobsRequest {
            kinds: kinds.to_vec(), timeout_ms: timeout.as_millis() as u64,
        });
        req.set_timeout(timeout + std::time::Duration::from_secs(2)); // client deadline > server long-poll
        let _ = self.inner.clone().await_jobs(req).await.map_err(be)?;
        Ok(())
    }
}
```

Add `pub mod client;` to `src/lib.rs`; add `//third-party:async-trait` to the engine-wire BUCK deps. Verify the generated client path `pb::engine_control_client::EngineControlClient`.

- [ ] **Step 2: Implement `EngineControlService` (server)**

`src/services/engine/src/service.rs`. Holds `PgControlPlane` (Queue) + `SqlCatalog` + `PgPool`; implements `pb::engine_control_server::EngineControl`. Each handler delegates and maps `ControlPlaneError` → `tonic::Status`:

```rust
use control_plane_core::{Catalog as _, Queue, RetryPolicy, RunId, TableRef};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use engine_wire::{convert, pb};
use sqlx::PgPool;
use tonic::{Request, Response, Status};

pub struct EngineControlService {
    pub cp: PgControlPlane,
    pub catalog: SqlCatalog,
    pub pool: PgPool,
}

fn status(e: control_plane_core::ControlPlaneError) -> Status {
    match e {
        control_plane_core::ControlPlaneError::NotFound(m) => Status::not_found(m.to_string()),
        other => Status::internal(other.to_string()),
    }
}

#[tonic::async_trait]
impl pb::engine_control_server::EngineControl for EngineControlService {
    async fn dequeue(&self, req: Request<pb::DequeueRequest>) -> std::result::Result<Response<pb::DequeueResponse>, Status> {
        let r = req.into_inner();
        let job = self.cp.dequeue(&r.kinds, &r.worker).await.map_err(status)?;
        Ok(Response::new(pb::DequeueResponse { job: job.as_ref().map(convert::job_to_pb) }))
    }
    async fn complete(&self, req: Request<pb::CompleteRequest>) -> std::result::Result<Response<pb::CompleteResponse>, Status> {
        let id = parse_id(&req.into_inner().id)?;
        self.cp.complete(id).await.map_err(status)?;
        Ok(Response::new(pb::CompleteResponse {}))
    }
    async fn fail(&self, req: Request<pb::FailRequest>) -> std::result::Result<Response<pb::FailResponse>, Status> {
        let r = req.into_inner();
        let id = parse_id(&r.id)?;
        let policy: RetryPolicy = convert::retry_policy_from_pb(r.policy.ok_or_else(|| Status::invalid_argument("missing policy"))?)
            .map_err(|e| Status::invalid_argument(e.0))?;
        self.cp.fail(id, &r.error, policy).await.map_err(status)?;
        Ok(Response::new(pb::FailResponse {}))
    }
    async fn heartbeat(&self, req: Request<pb::HeartbeatRequest>) -> std::result::Result<Response<pb::HeartbeatResponse>, Status> {
        let id = parse_id(&req.into_inner().id)?;
        self.cp.heartbeat(id).await.map_err(status)?;
        Ok(Response::new(pb::HeartbeatResponse {}))
    }
    async fn await_jobs(&self, req: Request<pb::AwaitJobsRequest>) -> std::result::Result<Response<pb::AwaitJobsResponse>, Status> {
        let r = req.into_inner();
        self.cp.await_jobs(&r.kinds, std::time::Duration::from_millis(r.timeout_ms)).await.map_err(status)?;
        Ok(Response::new(pb::AwaitJobsResponse {}))
    }
    async fn flush_table(&self, req: Request<pb::FlushTableRequest>) -> std::result::Result<Response<pb::FlushTableResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef { schema: r.schema, name: r.name };
        let snap = flush_table(&self.catalog, &self.pool, &table, RunId(uuid::Uuid::new_v4())).await.map_err(status)?;
        Ok(Response::new(pb::FlushTableResponse { snapshot_id: snap.map(|s| s.0) }))
    }
}

fn parse_id(s: &str) -> std::result::Result<control_plane_core::JobId, Status> {
    Ok(control_plane_core::JobId(s.parse().map_err(|_| Status::invalid_argument("bad job id"))?))
}
```

- [ ] **Step 3: Write the engine `main.rs`**

`src/services/engine/src/main.rs` — per the spec's engine section. Build `Config::from_env`, pool, `control_plane`, `SqlCatalog` (reuse the `make_catalog` shape from `postgres/tests/iceberg_flush.rs` — `SqlCatalogBuilder` over `cfg.db.pg_url()` + `file://{data_path}`), bind `LOOM_ENGINE_SOCKET` UnixListener (remove a stale file first), serve `EngineControlServer` with `serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)`. `shutdown` = `tokio::signal::ctrl_c()`.

- [ ] **Step 4: Write the integration test (the wire works)**

`src/services/engine/tests/wire.rs` — a `loom_fixture_test`. Spawn `EngineControlService` (over a fixture `PgControlPlane` + a `make_catalog` `SqlCatalog`) on a tempdir UDS via `tokio::spawn(Server::builder()...serve_with_incoming(...))`; connect a `GrpcQueueClient::connect(sock)`. Cases:
  1. **dequeue/complete:** enqueue a job directly via the fixture's `PgControlPlane`, `client.dequeue(["flush_table"], "w")` returns it over the wire, `client.complete(id)`, then a second dequeue returns `None`.
  2. **fail Retry/Abandon:** `client.fail(id, "boom", Retry{..})` then `Abandon` — assert no panic and (for Abandon) the job is terminal.
  3. **await_jobs wakes on notify:** start `client.await_jobs(["flush_table"], 5s)` in a task; after ~100ms enqueue a job via the fixture; assert `await_jobs` returns well before the 5s timeout.
  4. **flush over the wire:** `inline_append` rows via the fixture, `client.flush_table("wh","t")` returns `Some(_)`, and `IcebergCatalog::files` shows the table is now file-backed.

> Reuse the fixture/catalog setup from `postgres/tests/iceberg_flush.rs` (`PgFixture::start()`, `fresh_db`, `pool_for`, `make_catalog`). For the server-in-test, bind the UDS in a `tempfile::tempdir()`.

- [ ] **Step 5: BUCK targets**

Add `engine-wire`'s `client` deps (`//third-party:async-trait`, `//third-party:tower`, `//third-party:hyper-util`). Create `src/services/engine/Cargo.toml` + `BUCK`: a `rust_library` (`engine`) and `rust_binary` (`engine-bin`) and a `loom_fixture_test` (`wire`). Deps: `:engine-wire`, `//src/control-plane/postgres:postgres`, `//src/control-plane/core:core`, `//src/services/runtime:runtime`, `//third-party:{tonic,tokio,tokio-stream,uuid,async-trait,sqlx,iceberg,tempfile}`. Add `src/services/engine` to workspace members + `./tools/buckify.sh` (CONTROLLER, since deps changed).

- [ ] **Step 6: Build, test, format, commit**

```
buck2 test //src/services/engine:wire
buck2 run //tools:rustfmt -- <changed .rs files>
```

Expected: `wire` green (4 cases). Commit engine-wire client + engine crate.

```
git add src/services/engine-wire src/services/engine Cargo.toml Cargo.lock third-party/BUCK
git commit -m "feat(engine): EngineControl tonic server + GrpcQueueClient over a unix socket" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: The worker binary + full e2e + structural guard

The zero-pool consumer and the money test.

**Files:**
- Create: `src/services/worker/Cargo.toml`, `src/services/worker/src/main.rs`, `src/services/worker/src/handler.rs`, `src/services/worker/BUCK`, `src/services/worker/tests/e2e.rs`
- Modify: `Cargo.toml` (workspace members), `docs/spike/{ICEBERG_ROADMAP,engine-wire-transport}.md`

**Interfaces:**
- Consumes: `engine_wire::client::GrpcQueueClient`; `control_plane_worker::Worker`; `control_plane_core::{FlushJob, JobFailure, RetryPolicy, Job}`.

- [ ] **Step 1: Write the flush handler**

`src/services/worker/src/handler.rs`:

```rust
//! The worker's job handler: parse a flush_table job and run it over the wire.
use control_plane_core::{FlushJob, Job, JobFailure, RetryPolicy};
use engine_wire::client::GrpcQueueClient;
use std::time::Duration;

pub async fn handle_flush(flush: GrpcQueueClient, job: Job) -> std::result::Result<(), JobFailure> {
    let FlushJob { schema, name } = serde_json::from_value(job.payload)
        .map_err(|e| JobFailure { error: format!("bad flush payload: {e}"), policy: RetryPolicy::Abandon })?;
    flush.flush_table(schema, name).await.map_err(|e| JobFailure {
        error: e.to_string(),
        policy: RetryPolicy::Retry { delay: backoff(job.attempts) },
    })?;
    Ok(())
}

fn backoff(attempts: i32) -> Duration {
    // simple capped exponential: 1s, 2s, 4s, … max 60s
    let secs = 1u64.checked_shl(attempts.clamp(0, 6) as u32).unwrap_or(64).min(60);
    Duration::from_secs(secs)
}
```

- [ ] **Step 2: Write the worker `main.rs`**

`src/services/worker/src/main.rs` — own DB-free config (`LOOM_ENGINE_SOCKET`, `LOOM_WORKER_ID` default hostname/uuid, `LOOM_LOCK_TIMEOUT_MS` default 5000):

```rust
use std::time::Duration;
use control_plane_worker::Worker;
use engine_wire::client::GrpcQueueClient;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = std::env::var("LOOM_ENGINE_SOCKET")?;
    let worker_id = std::env::var("LOOM_WORKER_ID").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());
    let lease = Duration::from_millis(
        std::env::var("LOOM_LOCK_TIMEOUT_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(5000));

    let client = GrpcQueueClient::connect(socket).await?;
    let flush = client.clone();
    let worker = Worker::new(client, worker_id, lease);

    let shutdown = CancellationToken::new();
    let sig = shutdown.clone();
    tokio::spawn(async move { let _ = tokio::signal::ctrl_c().await; sig.cancel(); });

    worker.run(
        &[control_plane_core::FLUSH_JOB_KIND.to_string()],
        shutdown,
        move |job| { let flush = flush.clone(); async move { worker::handler::handle_flush(flush, job).await } },
    ).await?;
    Ok(())
}
```

(Expose `pub mod handler;` via `src/lib.rs` or keep `handler` a module of the binary; adjust the path accordingly.)

- [ ] **Step 3: Write the e2e test**

`src/services/worker/tests/e2e.rs` — a `loom_fixture_test`. Stand up the engine `EngineControlService` on a tempdir UDS (as in Task 4's test), `inline_append` rows past the threshold so Spec 1 enqueues a real `flush_table` job (via the fixture `PgControlPlane`/landing with `Some(1)`), construct `Worker::new(GrpcQueueClient::connect(sock), "w", 5s)`, and run ONE drain (run the loop with a cancellation token that you cancel after the first completion, or call a single dequeue→handle→complete cycle). Assert: the job is gone from `queue.jobs` AND the table is file-backed (reuse the flush assertions).

> If driving the full `Worker::run` for exactly one job is awkward in a test, drive the cycle directly through the `GrpcQueueClient` (`dequeue` → `handle_flush` → `complete`) — that still exercises the whole wire (producer→queue→client→engine→flush) end to end. Note which form you used.

- [ ] **Step 4: BUCK + structural guard**

Create `src/services/worker/Cargo.toml` + `BUCK`: `rust_binary` (`worker-bin`) + a `loom_fixture_test` (`e2e`). Deps: `:engine-wire`, `//src/control-plane/worker:worker`, `//src/control-plane/core:core`, `//third-party:{tokio,tokio-util,uuid,serde_json}`. The e2e test additionally needs the engine + postgres fixture, so put it in a target that depends on `//src/services/engine:engine` + `//src/control-plane/postgres:postgres` — **but keep that on the TEST target, not the `worker-bin` target.** The `worker-bin` target's deps must NOT include `//src/control-plane/postgres`.

Structural guard — add to the worker BUCK a comment and verify (CONTROLLER):

```
buck2 uquery "deps(//src/services/worker:worker-bin)" | grep -i "control-plane/postgres" | head
```

Expected: empty (zero-pool proven — the binary has no postgres in its dep closure). Record this in the report.

- [ ] **Step 5: Roadmap docs**

Update `docs/spike/engine-wire-transport.md` ("Architecture that falls out" / open questions) and `docs/spike/ICEBERG_ROADMAP.md` (#3) to mark the flush vertical / consumer **landed**: engine + worker over UDS, control-plane only; Arrow Flight data-plane still deferred. One trailing newline, no trailing whitespace.

- [ ] **Step 6: Full suite, lint, commit**

CONTROLLER runs:

```
buck2 test //src/...
buck2 run //tools:prek -- run --all-files
```

Expected: all green. Commit:

```
git add src/services/worker Cargo.toml Cargo.lock third-party/BUCK docs/spike
git commit -m "feat(worker): zero-pool flush worker draining flush_table jobs over the engine-wire" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Out of scope (this plan)

Arrow Flight / the data plane, `enqueue` over the wire, a persistent-stream `AwaitJobs`, deploy (apko/Helm), and multi-engine/TLS/auth — all deferred per the spec.
