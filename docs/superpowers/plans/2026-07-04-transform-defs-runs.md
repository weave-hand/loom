# Transform ergonomics slice 1 — defs, runs, HTTP surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Named `TransformDef`s + durable `TransformRun` records as a sixth control-plane concern, with run lifecycle over engine-wire and eight admin HTTP routes (spec: `docs/superpowers/specs/2026-07-04-transform-ergonomics-design.md`, item `road-transform-defs-runs`).

**Architecture:** New `Transforms` trait in `control-plane/core` implemented by memory + postgres adapters (testkit-contracted); runs ride the existing `transform`/`typed-transform` job kinds via a back-compatible `run_id` payload field; the worker reports lifecycle over new `MarkRunRunning`/`FinishRunFailed` RPCs and `CommitTransform` marks the run Succeeded **inside the commit transaction** via a new `TableTx::mark_run_succeeded` seam; eight `require_admin` routes on the runtime admin router with OpenAPI fragments.

**Tech Stack:** Rust (buck2, no cargo builds), axum + utoipa, sqlx compile-time queries, tonic/prost gRPC over UDS, protox codegen.

## Global Constraints

- **Slice-1 rejections:** `define_transform` rejects `schedule: Some(_)` and `on_input_commit: true` with `Validation` (slices 2/3 relax this). No inert-but-stored schedule strings.
- **202 for both run submissions** (`/run` routes); 201 for define; 200 idempotent deletes with `{deleted: …}` echo.
- **A Succeeded run always carries a concrete `snapshot_id`** — the no-snapshot commit is the worker's deterministic-abandon path and ends `Failed`.
- **Run states:** `Queued → Running → Succeeded | Failed`, with `Running → Queued` on retryable failure (error retained).
- **`run_id` doubles as the lineage `run_id`** — the worker reuses the payload `run_id` for its `LineageEvent` when present.
- Tests are `rust_test` integration targets only (never inline `#[cfg(test)]`); postgres fixture tests use `loom_fixture_test`.
- After ANY SQL change: `bash tools/sqlx-prepare.sh` and commit the `.sqlx` diff.
- Before every commit: `buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -c Failed /tmp/prek.log` must print `0`. rustfmt is check-only — fix formatting manually.
- Never pipe `buck2 test` through `head`/`tail`: redirect to a file, then grep.
- Clippy pedantic+restriction applies to production code (no `unwrap`/`expect`/indexing; `#[expect(lint, reason = "…")]` for local silences). Test targets are exempt from panic-safety lints via `loom_rust_test`/`loom_fixture_test`.
- Every commit message ends with:
  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01KrF8cSY7q1odAy9AW6aAnd
  ```

**Plan-level refinements of the spec (record in the PR body):**
1. The spec's "`create_run` (Queued) + `queue().enqueue(...)` in one control-plane transaction" is delivered as a single concern method `submit_run(run, job)` — same atomicity guarantee, one seam, no `Tx`-trait growth. (Slice 3 adds its own commit-seam entry point.)
2. `TransformRun` does not derive serde (it never crosses a serde boundary: postgres stores typed columns, HTTP responses map to explicit DTOs). `TransformDef`/`TransformBody` are serde as specced.
3. The in-commit success mark is `TableTx::mark_run_succeeded(run_id)` (staged, applied at commit with the allocated snapshot) — the engine cannot know the snapshot id before `commit()` allocates it.

---

### Task 1: Core `transforms` module — types, trait, validation

**Files:**
- Create: `src/control-plane/core/src/transforms.rs`
- Modify: `src/control-plane/core/src/lib.rs` (module + re-exports)
- Create: `src/control-plane/core/tests/transforms.rs`
- Modify: `src/control-plane/core/BUCK` (new test target)

**Interfaces:**
- Produces (later tasks depend on these exact names):
  - `TransformName(pub String)`; `TransformDef { name, body, schedule: Option<String>, on_input_commit: bool }`
  - `TransformBody::{Physical{inputs: Vec<TableRef>, output: TableRef, sql: String, output_mode: OutputMode}, Typed{inputs: Vec<String>, output: String, sql: String, output_mode: OutputMode}}`
  - `RunTrigger::{Manual, Schedule, DataTrigger, AdHoc}`; `RunState::{Queued, Running, Succeeded, Failed}` — both with `as_str()` + `FromStr`
  - `TransformRun { run_id: Uuid, transform: Option<TransformName>, trigger, state, body, queued_at, started_at, finished_at, snapshot_id: Option<i64>, error: Option<String> }`
  - `RunOutcome::{Succeeded{snapshot_id: i64}, RetryQueued{error: String}, Failed{error: String}}`
  - `trait Transforms` (methods below); `TransformBody::to_job(&self, run_id: Uuid) -> NewJob`; `validate_transform_def(&TransformDef) -> Result<()>`

- [ ] **Step 1: Write the failing test** — `src/control-plane/core/tests/transforms.rs`:

```rust
//! Serde shapes and helpers of the transforms concern types.

use control_plane_core::{
    OutputMode, RunState, RunTrigger, TRANSFORM_JOB_KIND, TYPED_TRANSFORM_JOB_KIND, TableRef,
    TransformBody, TransformDef, TransformName, validate_transform_def,
};
use uuid::Uuid;

fn physical_body() -> TransformBody {
    TransformBody::Physical {
        inputs: vec![TableRef { schema: "main".into(), name: "src".into() }],
        output: TableRef { schema: "main".into(), name: "dst".into() },
        sql: "select * from src".into(),
        output_mode: OutputMode::Append,
    }
}

#[test]
fn body_serde_is_internally_tagged() {
    let v = serde_json::to_value(physical_body()).unwrap();
    assert_eq!(v["kind"], "physical");
    assert_eq!(v["output"]["name"], "dst");
    let back: TransformBody = serde_json::from_value(v).unwrap();
    assert_eq!(back, physical_body());

    let typed: TransformBody = serde_json::from_value(serde_json::json!({
        "kind": "typed", "inputs": ["Order"], "output": "OrderSummary",
        "sql": "select * from Order",
    }))
    .unwrap();
    match typed {
        TransformBody::Typed { output_mode, .. } => assert_eq!(output_mode, OutputMode::Append),
        TransformBody::Physical { .. } => panic!("wrong variant"),
    }
}

#[test]
fn def_serde_defaults_schedule_and_trigger() {
    let def: TransformDef = serde_json::from_value(serde_json::json!({
        "name": "t1",
        "body": {"kind": "typed", "inputs": ["A"], "output": "B", "sql": "select 1"},
    }))
    .unwrap();
    assert_eq!(def.name, TransformName("t1".into()));
    assert_eq!(def.schedule, None);
    assert!(!def.on_input_commit);
}

#[test]
fn to_job_maps_kind_and_threads_run_id() {
    let rid = Uuid::new_v4();
    let job = physical_body().to_job(rid);
    assert_eq!(job.kind, TRANSFORM_JOB_KIND);
    assert_eq!(job.payload["run_id"], serde_json::json!(rid.to_string()));
    assert_eq!(job.payload["output"]["name"], "dst");
    assert!(job.run_at.is_none());
    assert_eq!(job.priority, 0);

    let typed = TransformBody::Typed {
        inputs: vec!["A".into()],
        output: "B".into(),
        sql: "select 1".into(),
        output_mode: OutputMode::Overwrite,
    };
    assert_eq!(typed.to_job(rid).kind, TYPED_TRANSFORM_JOB_KIND);
}

#[test]
fn slice1_rejects_schedule_and_data_trigger() {
    let mut def = TransformDef {
        name: TransformName("t".into()),
        body: physical_body(),
        schedule: Some("* * * * *".into()),
        on_input_commit: false,
    };
    assert!(validate_transform_def(&def).is_err(), "schedule rejected until slice 2");
    def.schedule = None;
    def.on_input_commit = true;
    assert!(validate_transform_def(&def).is_err(), "data trigger rejected until slice 3");
    def.on_input_commit = false;
    assert!(validate_transform_def(&def).is_ok());
}

#[test]
fn run_enums_round_trip_strings() {
    for (t, s) in [
        (RunTrigger::Manual, "manual"),
        (RunTrigger::Schedule, "schedule"),
        (RunTrigger::DataTrigger, "data-trigger"),
        (RunTrigger::AdHoc, "ad-hoc"),
    ] {
        assert_eq!(t.as_str(), s);
        assert_eq!(s.parse::<RunTrigger>().unwrap(), t);
    }
    for (t, s) in [
        (RunState::Queued, "queued"),
        (RunState::Running, "running"),
        (RunState::Succeeded, "succeeded"),
        (RunState::Failed, "failed"),
    ] {
        assert_eq!(t.as_str(), s);
        assert_eq!(s.parse::<RunState>().unwrap(), t);
    }
    assert!("bogus".parse::<RunTrigger>().is_err());
    assert!("bogus".parse::<RunState>().is_err());
}
```

- [ ] **Step 2: Add the BUCK test target** (mirror the existing `page` target in `src/control-plane/core/BUCK`; note core's BUCK loads `loom_rust_test` — mirror whichever wrapper the `page` target uses):

```python
loom_rust_test(
    name = "transforms",
    crate = "transforms",
    srcs = ["tests/transforms.rs"],
    crate_root = "tests/transforms.rs",
    edition = "2024",
    deps = [":core", "//third-party:serde_json", "//third-party:uuid"],
)
```

- [ ] **Step 3: Run to verify it fails**

Run: `buck2 test //src/control-plane/core:transforms > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t1.log`
Expected: compile error — `TransformBody` not found.

- [ ] **Step 4: Implement `src/control-plane/core/src/transforms.rs`:**

```rust
//! The transforms concern: named transform definitions and their runs.
//!
//! A [`TransformDef`] is the authored artifact — SQL over physical tables
//! (`Physical`) or ontology types (`Typed`), mirroring the two queue job
//! payloads. A [`TransformRun`] is the durable record of one execution; its
//! `run_id` doubles as the lineage `run_id`, so a run's lineage events are
//! queryable with no extra linkage. Runs freeze the body they executed:
//! redefinition never rewrites history.
//!
//! Slice 1 delivers definitions + runs only: [`validate_transform_def`]
//! rejects `schedule`/`on_input_commit` until slices 2/3 make them live.

use std::str::FromStr;

use async_trait::async_trait;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::error::{ControlPlaneError, Result};
use crate::page::{Page, PageReq};
use crate::queue::NewJob;
use crate::snapshot::TableRef;
use crate::transform_job::{
    OutputMode, TRANSFORM_JOB_KIND, TYPED_TRANSFORM_JOB_KIND, TransformJob, TypedTransformJob,
};

/// A transform's name (unique key of the definition).
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TransformName(pub String);

/// The executable body of a transform — mirrors the queue job payloads.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum TransformBody {
    /// SQL over physical tables.
    Physical {
        inputs: Vec<TableRef>,
        output: TableRef,
        sql: String,
        #[serde(default)]
        output_mode: OutputMode,
    },
    /// SQL in ontology-type vocabulary.
    Typed {
        inputs: Vec<String>,
        output: String,
        sql: String,
        #[serde(default)]
        output_mode: OutputMode,
    },
}

impl TransformBody {
    /// Build the queue job for one run of this body, threading `run_id` into
    /// the payload. Serialization of the payload structs cannot fail (plain
    /// data), so this is infallible.
    #[must_use]
    pub fn to_job(&self, run_id: Uuid) -> NewJob {
        let (kind, payload) = match self {
            Self::Physical { inputs, output, sql, output_mode } => (
                TRANSFORM_JOB_KIND,
                serde_json::to_value(TransformJob {
                    inputs: inputs.clone(),
                    output: output.clone(),
                    sql: sql.clone(),
                    output_mode: *output_mode,
                    run_id: Some(run_id),
                }),
            ),
            Self::Typed { inputs, output, sql, output_mode } => (
                TYPED_TRANSFORM_JOB_KIND,
                serde_json::to_value(TypedTransformJob {
                    inputs: inputs.clone(),
                    output: output.clone(),
                    sql: sql.clone(),
                    output_mode: *output_mode,
                    run_id: Some(run_id),
                }),
            ),
        };
        NewJob {
            kind: kind.to_string(),
            payload: payload.unwrap_or(serde_json::Value::Null),
            run_at: None,
            priority: 0,
        }
    }
}

/// A named transform definition. `schedule` (slice 2) and `on_input_commit`
/// (slice 3) are carried in the shape but rejected by
/// [`validate_transform_def`] until their slices land.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TransformDef {
    pub name: TransformName,
    pub body: TransformBody,
    #[serde(default)]
    pub schedule: Option<String>,
    #[serde(default)]
    pub on_input_commit: bool,
}

/// What started a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunTrigger {
    Manual,
    Schedule,
    DataTrigger,
    AdHoc,
}

/// A run's lifecycle state. `Queued → Running → Succeeded | Failed`, with
/// `Running → Queued` on a retryable failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunState {
    Queued,
    Running,
    Succeeded,
    Failed,
}

impl RunTrigger {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Schedule => "schedule",
            Self::DataTrigger => "data-trigger",
            Self::AdHoc => "ad-hoc",
        }
    }
}

impl FromStr for RunTrigger {
    type Err = ControlPlaneError;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "manual" => Ok(Self::Manual),
            "schedule" => Ok(Self::Schedule),
            "data-trigger" => Ok(Self::DataTrigger),
            "ad-hoc" => Ok(Self::AdHoc),
            other => Err(ControlPlaneError::Validation(format!(
                "unknown run trigger: {other}"
            ))),
        }
    }
}

impl RunState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

impl FromStr for RunState {
    type Err = ControlPlaneError;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            other => Err(ControlPlaneError::Validation(format!(
                "unknown run state: {other}"
            ))),
        }
    }
}

/// The durable record of one transform execution. `body` is frozen at submit
/// time. Deliberately NOT serde: postgres stores typed columns and HTTP maps
/// to explicit DTOs, so this never crosses a serde boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct TransformRun {
    /// Doubles as the lineage `run_id`.
    pub run_id: Uuid,
    /// `None` for ad-hoc runs.
    pub transform: Option<TransformName>,
    pub trigger: RunTrigger,
    pub state: RunState,
    pub body: TransformBody,
    pub queued_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub snapshot_id: Option<i64>,
    pub error: Option<String>,
}

/// Terminal-or-retry outcome reported for a run.
#[derive(Clone, Debug, PartialEq)]
pub enum RunOutcome {
    /// The commit produced this snapshot. (A no-snapshot commit is the
    /// worker's deterministic-abandon path and ends `Failed`.)
    Succeeded { snapshot_id: i64 },
    /// Retryable failure: state back to `Queued`, error retained.
    RetryQueued { error: String },
    /// Terminal failure (abandon).
    Failed { error: String },
}

/// Slice-1 definition validation, shared by both adapters: `schedule` and
/// `on_input_commit` are carried in the shape but not yet live.
pub fn validate_transform_def(def: &TransformDef) -> Result<()> {
    if def.schedule.is_some() {
        return Err(ControlPlaneError::Validation(
            "transform schedules are not supported yet (slice 2)".into(),
        ));
    }
    if def.on_input_commit {
        return Err(ControlPlaneError::Validation(
            "data-triggered transforms are not supported yet (slice 3)".into(),
        ));
    }
    Ok(())
}

/// The transforms concern: named definitions and their runs.
#[async_trait]
pub trait Transforms {
    /// Define or redefine (upsert) a transform. Typed bodies validate that
    /// input/output type names exist in the ontology (`Validation` otherwise).
    async fn define_transform(&self, def: TransformDef) -> Result<()>;
    /// `NotFound` if undefined.
    async fn get_transform(&self, name: &TransformName) -> Result<TransformDef>;
    /// All definitions, name-ordered, single full page (`page` accepted for
    /// future use like the other list reads).
    async fn list_transforms(&self, page: PageReq) -> Result<Page<TransformDef>>;
    /// Idempotent: deleting an unknown name is `Ok(())`. Runs keep their
    /// frozen body and name (plain text, no FK) — history survives deletion.
    async fn delete_transform(&self, name: &TransformName) -> Result<()>;

    /// Record `run` (must be `Queued`) and enqueue `job` atomically: the job
    /// is visible to a worker iff the run row exists.
    async fn submit_run(&self, run: TransformRun, job: NewJob) -> Result<crate::JobId>;
    /// `Queued → Running`, stamping `started_at`. `NotFound` if unknown.
    async fn mark_run_running(&self, run_id: Uuid) -> Result<()>;
    /// Apply a [`RunOutcome`]. `NotFound` if unknown.
    async fn finish_run(&self, run_id: Uuid, outcome: RunOutcome) -> Result<()>;
    /// `NotFound` if unknown.
    async fn get_run(&self, run_id: Uuid) -> Result<TransformRun>;
    /// Runs (optionally of one transform), newest first (`queued_at` desc,
    /// `run_id` desc tiebreak), single full page.
    async fn list_runs(
        &self,
        transform: Option<&TransformName>,
        page: PageReq,
    ) -> Result<Page<TransformRun>>;
}
```

Wire into `src/control-plane/core/src/lib.rs` (alphabetical among the existing `mod`/`pub use` lists):

```rust
mod transforms;
pub use transforms::{
    RunOutcome, RunState, RunTrigger, TransformBody, TransformDef, TransformName, TransformRun,
    Transforms, validate_transform_def,
};
```

NOTE: this task does NOT yet add `run_id` to `TransformJob`/`TypedTransformJob` — Task 2 does. To keep Task 1 compiling standalone, implement Tasks 1 and 2 in the SAME commit if the implementer prefers, or temporarily write `to_job` without the `run_id` field and let Task 2 finish it. Preferred: do Task 2's payload change first within this task's Step 4 (it is four lines), then `to_job` compiles as written. The test in Step 1 already assumes the payload carries `run_id`.

- [ ] **Step 5: Apply Task 2's payload change now** (see Task 2 Step 1 for the exact diff), then run the test:

Run: `buck2 test //src/control-plane/core:transforms > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS (`Tests finished: Pass 5. Fail 0.`)

- [ ] **Step 6: Run the existing core tests** (payload change must not break them):

Run: `buck2 test //src/control-plane/core: > /tmp/t1b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1b.log`
Expected: all pass.

- [ ] **Step 7: prek, then commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -c Failed /tmp/prek.log
git add src/control-plane/core docs/superpowers/plans/2026-07-04-transform-defs-runs.md
git commit -m "feat(control-plane): transforms concern types + trait (defs, runs, outcomes)" -m "..."
```

---

### Task 2: `run_id` on the job payloads (back-compatible)

**Files:**
- Modify: `src/control-plane/core/src/transform_job.rs`
- Modify: `src/control-plane/core/tests/transforms.rs` (add back-compat test)
- Modify: any `TransformJob {`/`TypedTransformJob {` construction sites (find with `grep -rn "TransformJob {" src/ --include=*.rs` — expected: `src/services/worker/tests/transform_e2e.rs`, `src/services/worker/tests/typed_transform_e2e.rs`; add `run_id: None`)

**Interfaces:**
- Produces: `TransformJob.run_id: Option<Uuid>` and `TypedTransformJob.run_id: Option<Uuid>`, both `#[serde(default)]`.

*(If folded into Task 1's commit per the note above, just verify the steps below were all done.)*

- [ ] **Step 1: Add the fields** in `src/control-plane/core/src/transform_job.rs` — to BOTH structs, after `output_mode`:

```rust
    /// When present, the id of the `TransformRun` this job executes (and the
    /// lineage `run_id` the worker will emit). Absent on legacy/direct
    /// enqueues, which run without a run record.
    #[serde(default)]
    pub run_id: Option<uuid::Uuid>,
```

(`uuid` is already a core dep with the `serde` feature enabled graph-wide.)

- [ ] **Step 2: Back-compat test** — append to `src/control-plane/core/tests/transforms.rs`:

```rust
#[test]
fn legacy_payload_without_run_id_still_deserializes() {
    let legacy = serde_json::json!({
        "inputs": [{"schema": "main", "name": "src"}],
        "output": {"schema": "main", "name": "dst"},
        "sql": "select 1",
    });
    let job: control_plane_core::TransformJob = serde_json::from_value(legacy).unwrap();
    assert_eq!(job.run_id, None);
    let typed = serde_json::json!({ "inputs": ["A"], "output": "B", "sql": "select 1" });
    let job: control_plane_core::TypedTransformJob = serde_json::from_value(typed).unwrap();
    assert_eq!(job.run_id, None);
}
```

- [ ] **Step 3: Fix construction sites** — `grep -rn "TransformJob {" src/ --include=*.rs` and add `run_id: None,` to each struct literal (worker e2e tests).

- [ ] **Step 4: Run core tests + the two worker e2e targets' build**

Run: `buck2 test //src/control-plane/core: > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Run: `buck2 build -M none //src/services/worker/... > /tmp/t2b.log 2>&1; tail -3 /tmp/t2b.log`
Expected: PASS / BUILD SUCCEEDED.

- [ ] **Step 5: prek + commit** (`feat(control-plane): thread optional run_id through transform job payloads`)

---

### Task 3: Memory adapter + testkit contract

**Files:**
- Create: `src/control-plane/memory/src/transforms.rs`
- Modify: `src/control-plane/memory/src/lib.rs` (state field + module)
- Modify: `src/control-plane/testkit/src/lib.rs` (new `transforms_contract`)
- Create: `src/control-plane/memory/tests/transforms.rs`
- Modify: `src/control-plane/memory/BUCK`

**Interfaces:**
- Consumes: Task 1's types/trait.
- Produces: `control_plane_testkit::transforms_contract<CP: ControlPlane + Transforms + Ontology>(cp: &CP)` (Task 4 reuses it); `MemoryControlPlane: Transforms`.

- [ ] **Step 1: Write the contract** — append to `src/control-plane/testkit/src/lib.rs` (follow the existing `#![allow]` posture of the file; helpers may `unwrap`):

```rust
/// Contract for the transforms concern. Seeds two ontology types (`Widget`,
/// `Gadget`) for typed-body validation; callers pass a fresh plane.
pub async fn transforms_contract<CP>(cp: &CP)
where
    CP: ControlPlane + Transforms + Ontology,
{
    use control_plane_core::{
        OutputMode, RunOutcome, RunState, RunTrigger, TransformBody, TransformDef, TransformName,
        TransformRun,
    };

    // Seed types for typed-body validation (mirror the ObjectType seeding the
    // ontology_contract uses — same table/property shapes).
    seed_type(cp, "Widget", "main", "widget").await;
    seed_type(cp, "Gadget", "main", "gadget").await;

    let phys = TransformBody::Physical {
        inputs: vec![tref("main", "src")],
        output: tref("main", "dst"),
        sql: "select * from src".into(),
        output_mode: OutputMode::Append,
    };
    let def = TransformDef {
        name: TransformName("daily".into()),
        body: phys.clone(),
        schedule: None,
        on_input_commit: false,
    };

    // define + get + upsert redefine
    cp.define_transform(def.clone()).await.unwrap();
    assert_eq!(cp.get_transform(&def.name).await.unwrap(), def);
    let redefined = TransformDef { body: TransformBody::Physical {
        inputs: vec![tref("main", "src2")],
        output: tref("main", "dst"),
        sql: "select * from src2".into(),
        output_mode: OutputMode::Overwrite,
    }, ..def.clone() };
    cp.define_transform(redefined.clone()).await.unwrap();
    assert_eq!(cp.get_transform(&def.name).await.unwrap(), redefined, "define is an upsert");

    // slice-1 rejections + typed validation
    let sched = TransformDef { schedule: Some("* * * * *".into()), ..def.clone() };
    assert!(matches!(
        cp.define_transform(sched).await,
        Err(ControlPlaneError::Validation(_))
    ), "schedule rejected until slice 2");
    let trig = TransformDef { on_input_commit: true, ..def.clone() };
    assert!(matches!(cp.define_transform(trig).await, Err(ControlPlaneError::Validation(_))));
    let bad_typed = TransformDef {
        name: TransformName("typed".into()),
        body: TransformBody::Typed {
            inputs: vec!["Widget".into(), "Nope".into()],
            output: "Gadget".into(),
            sql: "select 1".into(),
            output_mode: OutputMode::Append,
        },
        schedule: None,
        on_input_commit: false,
    };
    assert!(matches!(
        cp.define_transform(bad_typed.clone()).await,
        Err(ControlPlaneError::Validation(_))
    ), "unknown typed input rejected");
    let good_typed = TransformDef { body: TransformBody::Typed {
        inputs: vec!["Widget".into()],
        output: "Gadget".into(),
        sql: "select 1".into(),
        output_mode: OutputMode::Append,
    }, ..bad_typed };
    cp.define_transform(good_typed.clone()).await.unwrap();

    // list: name-ordered, full page
    let page = cp.list_transforms(PageReq::default()).await.unwrap();
    assert_eq!(
        page.items.iter().map(|d| d.name.0.as_str()).collect::<Vec<_>>(),
        vec!["daily", "typed"]
    );
    assert!(page.next.is_none());

    // submit_run: run recorded Queued + job dequeueable
    let rid = uuid::Uuid::new_v4();
    let run = TransformRun {
        run_id: rid,
        transform: Some(TransformName("daily".into())),
        trigger: RunTrigger::Manual,
        state: RunState::Queued,
        body: redefined.body.clone(),
        queued_at: time::OffsetDateTime::now_utc(),
        started_at: None,
        finished_at: None,
        snapshot_id: None,
        error: None,
    };
    let job = redefined.body.to_job(rid);
    let kinds = vec![job.kind.clone()];
    cp.submit_run(run, job).await.unwrap();
    let got = cp.get_run(rid).await.unwrap();
    assert_eq!(got.state, RunState::Queued);
    assert_eq!(got.transform, Some(TransformName("daily".into())));
    assert_eq!(got.body, redefined.body, "body frozen at submit");
    let j = cp.queue().dequeue(&kinds, "w").await.unwrap().expect("submitted job");
    assert_eq!(j.payload["run_id"], serde_json::json!(rid.to_string()));

    // lifecycle: running -> retry-queued (error retained) -> running -> succeeded
    cp.mark_run_running(rid).await.unwrap();
    let r = cp.get_run(rid).await.unwrap();
    assert_eq!(r.state, RunState::Running);
    assert!(r.started_at.is_some());
    cp.finish_run(rid, RunOutcome::RetryQueued { error: "flaky".into() }).await.unwrap();
    let r = cp.get_run(rid).await.unwrap();
    assert_eq!(r.state, RunState::Queued);
    assert_eq!(r.error.as_deref(), Some("flaky"));
    cp.mark_run_running(rid).await.unwrap();
    cp.finish_run(rid, RunOutcome::Succeeded { snapshot_id: 41 }).await.unwrap();
    let r = cp.get_run(rid).await.unwrap();
    assert_eq!(r.state, RunState::Succeeded);
    assert_eq!(r.snapshot_id, Some(41));
    assert!(r.finished_at.is_some());

    // a second, ad-hoc failed run; list newest-first + filter
    let rid2 = uuid::Uuid::new_v4();
    let run2 = TransformRun {
        run_id: rid2,
        transform: None,
        trigger: RunTrigger::AdHoc,
        state: RunState::Queued,
        body: good_typed.body.clone(),
        queued_at: time::OffsetDateTime::now_utc(),
        started_at: None,
        finished_at: None,
        snapshot_id: None,
        error: None,
    };
    cp.submit_run(run2, good_typed.body.to_job(rid2)).await.unwrap();
    cp.finish_run(rid2, RunOutcome::Failed { error: "bad sql".into() }).await.unwrap();
    let all = cp.list_runs(None, PageReq::default()).await.unwrap();
    assert_eq!(all.items.first().map(|r| r.run_id), Some(rid2), "newest first");
    assert_eq!(all.items.len(), 2);
    let named = cp.list_runs(Some(&TransformName("daily".into())), PageReq::default()).await.unwrap();
    assert_eq!(named.items.len(), 1);
    assert_eq!(named.items[0].run_id, rid);

    // unknown-id errors
    let nope = uuid::Uuid::new_v4();
    assert!(matches!(cp.get_run(nope).await, Err(ControlPlaneError::NotFound(_))));
    assert!(matches!(cp.mark_run_running(nope).await, Err(ControlPlaneError::NotFound(_))));
    assert!(matches!(
        cp.finish_run(nope, RunOutcome::Failed { error: "e".into() }).await,
        Err(ControlPlaneError::NotFound(_))
    ));

    // delete: idempotent; runs survive with the frozen name
    cp.delete_transform(&TransformName("daily".into())).await.unwrap();
    cp.delete_transform(&TransformName("daily".into())).await.unwrap();
    assert!(matches!(
        cp.get_transform(&TransformName("daily".into())).await,
        Err(ControlPlaneError::NotFound(_))
    ));
    let named = cp.list_runs(Some(&TransformName("daily".into())), PageReq::default()).await.unwrap();
    assert_eq!(named.items.len(), 1, "runs survive definition deletion");
}
```

Where `seed_type`/`tref` — reuse the testkit's existing helpers if present (grep for `fn tref`/how `ontology_contract` seeds `ObjectType`); otherwise add small private helpers next to the contract mirroring `ontology_contract`'s seeding (an `ObjectType` with `name`, `table: TableRef`, `identity`, empty properties — copy the exact struct literal shape from the existing contract).

- [ ] **Step 2: Memory test file** — `src/control-plane/memory/tests/transforms.rs`:

```rust
use std::time::Duration;

use control_plane_memory::MemoryControlPlane;

#[tokio::test]
async fn memory_passes_transforms_contract() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    control_plane_testkit::transforms_contract(&cp).await;
}
```

BUCK target in `src/control-plane/memory/BUCK` (mirror the `queue` target):

```python
loom_rust_test(
    name = "transforms",
    crate = "transforms",
    srcs = ["tests/transforms.rs"],
    crate_root = "tests/transforms.rs",
    edition = "2024",
    deps = [":memory", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

(Use whichever wrapper — `rust_test` vs `loom_rust_test` — the sibling targets in that BUCK use; testkit's BUCK needs no dep change since core is already there.)

- [ ] **Step 3: Run to verify it fails**

Run: `buck2 test //src/control-plane/memory:transforms > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t3.log`
Expected: compile error — `MemoryControlPlane: Transforms` not satisfied.

- [ ] **Step 4: Implement the memory adapter.** In `src/control-plane/memory/src/lib.rs` add the state field (next to `ontology`):

```rust
    transforms: Arc<Mutex<TransformsState>>,
```

initialized `Arc::default()` in `new()`, plus `mod transforms;`. New file `src/control-plane/memory/src/transforms.rs`:

```rust
//! Memory transforms concern: definitions and runs in two maps.

use std::collections::HashMap;

use async_trait::async_trait;
use control_plane_core::{
    ControlPlaneError, JobId, NewJob, PageReq, Page, Result, RunOutcome, RunState, TransformBody,
    TransformDef, TransformName, TransformRun, Transforms, validate_transform_def,
};
use uuid::Uuid;

use crate::MemoryControlPlane;

#[derive(Default)]
pub(crate) struct TransformsState {
    pub(crate) defs: HashMap<String, TransformDef>,
    pub(crate) runs: HashMap<Uuid, TransformRun>,
}

#[async_trait]
impl Transforms for MemoryControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_transform(&self, def: TransformDef) -> Result<()> {
        validate_transform_def(&def)?;
        if let TransformBody::Typed { inputs, output, .. } = &def.body {
            let ont = self.ontology.lock();
            for ty in inputs.iter().chain(std::iter::once(output)) {
                if !ont.types.contains_key(ty) {
                    return Err(ControlPlaneError::Validation(format!(
                        "unknown ontology type in typed transform: {ty}"
                    )));
                }
            }
        }
        self.transforms.lock().defs.insert(def.name.0.clone(), def);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_transform(&self, name: &TransformName) -> Result<TransformDef> {
        self.transforms
            .lock()
            .defs
            .get(&name.0)
            .cloned()
            .ok_or_else(|| ControlPlaneError::NotFound(format!("transform {}", name.0)))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_transforms(&self, _page: PageReq) -> Result<Page<TransformDef>> {
        let mut items: Vec<_> = self.transforms.lock().defs.values().cloned().collect();
        items.sort_by(|a, b| a.name.0.cmp(&b.name.0));
        Ok(Page::from_full(items))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn delete_transform(&self, name: &TransformName) -> Result<()> {
        self.transforms.lock().defs.remove(&name.0);
        Ok(())
    }

    #[tracing::instrument(skip(self, run, job), level = "debug")]
    async fn submit_run(&self, run: TransformRun, job: NewJob) -> Result<JobId> {
        // Insert the run and the job under the transforms lock so no reader
        // observes the job without its run (mirrors the postgres transaction).
        let mut st = self.transforms.lock();
        let id = self.enqueue_locked(job);
        st.runs.insert(run.run_id, run);
        Ok(id)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn mark_run_running(&self, run_id: Uuid) -> Result<()> {
        let mut st = self.transforms.lock();
        let run = st.runs.get_mut(&run_id)
            .ok_or_else(|| ControlPlaneError::NotFound(format!("run {run_id}")))?;
        run.state = RunState::Running;
        run.started_at = Some(time::OffsetDateTime::now_utc());
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn finish_run(&self, run_id: Uuid, outcome: RunOutcome) -> Result<()> {
        let mut st = self.transforms.lock();
        let run = st.runs.get_mut(&run_id)
            .ok_or_else(|| ControlPlaneError::NotFound(format!("run {run_id}")))?;
        apply_outcome(run, outcome);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_run(&self, run_id: Uuid) -> Result<TransformRun> {
        self.transforms
            .lock()
            .runs
            .get(&run_id)
            .cloned()
            .ok_or_else(|| ControlPlaneError::NotFound(format!("run {run_id}")))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_runs(
        &self,
        transform: Option<&TransformName>,
        _page: PageReq,
    ) -> Result<Page<TransformRun>> {
        let mut items: Vec<_> = self
            .transforms
            .lock()
            .runs
            .values()
            .filter(|r| transform.is_none_or(|t| r.transform.as_ref() == Some(t)))
            .cloned()
            .collect();
        items.sort_by(|a, b| {
            b.queued_at.cmp(&a.queued_at).then(b.run_id.cmp(&a.run_id))
        });
        Ok(Page::from_full(items))
    }
}

/// Shared state transition for [`RunOutcome`] — also used by the memory
/// `TableTx` success path (Task 6).
pub(crate) fn apply_outcome(run: &mut TransformRun, outcome: RunOutcome) {
    match outcome {
        RunOutcome::Succeeded { snapshot_id } => {
            run.state = RunState::Succeeded;
            run.snapshot_id = Some(snapshot_id);
            run.finished_at = Some(time::OffsetDateTime::now_utc());
        }
        RunOutcome::RetryQueued { error } => {
            run.state = RunState::Queued;
            run.error = Some(error);
        }
        RunOutcome::Failed { error } => {
            run.state = RunState::Failed;
            run.error = Some(error);
            run.finished_at = Some(time::OffsetDateTime::now_utc());
        }
    }
}
```

`enqueue_locked(job) -> JobId`: the memory queue's insert seam. Look at `src/control-plane/memory/src/queue.rs` — `MemoryControlPlane` has `insert_with_id(&mut rows, id, job)` (used by `MemoryTx::commit`). Add a small `pub(crate) fn enqueue_locked(&self, job: NewJob) -> JobId` on `MemoryControlPlane` that locks `rows`, inserts with a fresh `Uuid::new_v4()`, calls `self.notify.notify_waiters()`, and returns the id — matching whatever the direct `Queue::enqueue` impl does (read it and reuse its body; if it is already a thin wrapper over such a helper, call that). NOTE the lock order comment in `transaction.rs` (`rows` before `lineage` before `catalog`): here we hold `transforms` then take `rows` — no existing reader takes `transforms` + another lock, so no deadlock; add a comment saying so.

- [ ] **Step 5: Run the contract**

Run: `buck2 test //src/control-plane/memory:transforms > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS.

- [ ] **Step 6: Full memory + testkit build** — `buck2 build -M none //src/control-plane/... > /tmp/t3b.log 2>&1; tail -3 /tmp/t3b.log` (postgres does not implement the trait yet, but nothing forces it to — the trait is standalone until Task 5 touches `ControlPlane`).

- [ ] **Step 7: prek + commit** (`feat(control-plane): memory transforms adapter + testkit contract`)

---

### Task 4: Postgres adapter — migration, impl, `.sqlx`

**Files:**
- Create: `src/control-plane/postgres/migrations/0031_transforms.sql`
- Create: `src/control-plane/postgres/src/transforms.rs`
- Modify: `src/control-plane/postgres/src/lib.rs` (`mod transforms;`)
- Create: `src/control-plane/postgres/tests/transforms.rs`
- Modify: `src/control-plane/postgres/BUCK`
- Regenerate: `src/control-plane/postgres/.sqlx/`

**Interfaces:**
- Consumes: Task 1's trait, Task 3's `transforms_contract`.
- Produces: `PgControlPlane: Transforms`; `pub(crate) async fn pg_mark_run_succeeded<'e, E: sqlx::PgExecutor<'e>>(ex: E, run_id: Uuid, snapshot_id: i64) -> Result<()>` (Task 6 uses it from `IcebergTx::commit`).

- [ ] **Step 1: Migration** `src/control-plane/postgres/migrations/0031_transforms.sql`:

```sql
-- The transforms concern: named definitions and their durable runs.
create schema if not exists transforms;

create table transforms.transform (
    name            text primary key,
    body            jsonb   not null,
    schedule        text,
    on_input_commit boolean not null default false
);

create table transforms.run (
    run_id      uuid primary key,
    -- Plain text, no FK: runs survive definition deletion (frozen history).
    transform   text,
    trigger     text not null,
    state       text not null,
    body        jsonb not null,
    queued_at   timestamptz not null,
    started_at  timestamptz,
    finished_at timestamptz,
    snapshot_id bigint,
    error       text
);

create index run_by_transform on transforms.run (transform, queued_at desc, run_id desc);
```

- [ ] **Step 2: Fixture test** `src/control-plane/postgres/tests/transforms.rs`:

```rust
use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_transforms_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::transforms_contract(&cp).await;
}
```

BUCK (mirror the existing contract fixture targets, e.g. the acl one — MUST be `loom_fixture_test`):

```python
loom_fixture_test(
    name = "transforms",
    crate = "transforms",
    srcs = ["tests/transforms.rs"],
    crate_root = "tests/transforms.rs",
    deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

- [ ] **Step 3: Run to verify it fails** — `buck2 test //src/control-plane/postgres:transforms > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t4.log` → compile error (trait not implemented).

- [ ] **Step 4: Implement** `src/control-plane/postgres/src/transforms.rs`:

```rust
//! Postgres transforms concern over the `transforms` schema (migration 0031).

use async_trait::async_trait;
use control_plane_core::{
    ControlPlaneError, JobId, NewJob, Page, PageReq, Result, RunOutcome, RunState, RunTrigger,
    TransformBody, TransformDef, TransformName, TransformRun, Transforms, validate_transform_def,
};
use uuid::Uuid;

use crate::queue::pg_insert;
use crate::{PgControlPlane, backend};

fn ser(v: &impl serde::Serialize) -> Result<serde_json::Value> {
    serde_json::to_value(v).map_err(|e| ControlPlaneError::Serialization(e.to_string()))
}

fn de_body(v: serde_json::Value) -> Result<TransformBody> {
    serde_json::from_value(v).map_err(|e| ControlPlaneError::Serialization(e.to_string()))
}

/// Mark `run_id` succeeded at `snapshot_id` on any executor — callable from
/// inside `IcebergTx::commit`'s held transaction (Task 6).
pub(crate) async fn pg_mark_run_succeeded<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    run_id: Uuid,
    snapshot_id: i64,
) -> Result<()> {
    let res = sqlx::query!(
        "update transforms.run \
         set state = 'succeeded', snapshot_id = $2, finished_at = now() \
         where run_id = $1",
        run_id,
        snapshot_id,
    )
    .execute(ex)
    .await
    .map_err(backend)?;
    if res.rows_affected() == 0 {
        return Err(ControlPlaneError::NotFound(format!("run {run_id}")));
    }
    Ok(())
}

#[async_trait]
impl Transforms for PgControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_transform(&self, def: TransformDef) -> Result<()> {
        validate_transform_def(&def)?;
        if let TransformBody::Typed { inputs, output, .. } = &def.body {
            let mut names: Vec<String> = inputs.clone();
            names.push(output.clone());
            names.sort();
            names.dedup();
            let known = sqlx::query_scalar!(
                "select count(*) from ontology.object_type where name = any($1)",
                &names,
            )
            .fetch_one(self.pool())
            .await
            .map_err(backend)?
            .unwrap_or(0);
            if known != i64::try_from(names.len()).unwrap_or(i64::MAX) {
                return Err(ControlPlaneError::Validation(
                    "typed transform references unknown ontology types".into(),
                ));
            }
        }
        let body = ser(&def.body)?;
        sqlx::query!(
            "insert into transforms.transform (name, body, schedule, on_input_commit) \
             values ($1, $2, $3, $4) \
             on conflict (name) do update set \
                 body = excluded.body, schedule = excluded.schedule, \
                 on_input_commit = excluded.on_input_commit",
            def.name.0,
            body,
            def.schedule,
            def.on_input_commit,
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_transform(&self, name: &TransformName) -> Result<TransformDef> {
        let row = sqlx::query!(
            "select name, body, schedule, on_input_commit \
             from transforms.transform where name = $1",
            name.0,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(format!("transform {}", name.0)))?;
        Ok(TransformDef {
            name: TransformName(row.name),
            body: de_body(row.body)?,
            schedule: row.schedule,
            on_input_commit: row.on_input_commit,
        })
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_transforms(&self, _page: PageReq) -> Result<Page<TransformDef>> {
        let rows = sqlx::query!(
            "select name, body, schedule, on_input_commit \
             from transforms.transform order by name",
        )
        .fetch_all(self.pool())
        .await
        .map_err(backend)?;
        let items = rows
            .into_iter()
            .map(|r| {
                Ok(TransformDef {
                    name: TransformName(r.name),
                    body: de_body(r.body)?,
                    schedule: r.schedule,
                    on_input_commit: r.on_input_commit,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Page::from_full(items))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn delete_transform(&self, name: &TransformName) -> Result<()> {
        sqlx::query!("delete from transforms.transform where name = $1", name.0)
            .execute(self.pool())
            .await
            .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, run, job), level = "debug")]
    async fn submit_run(&self, run: TransformRun, job: NewJob) -> Result<JobId> {
        let body = ser(&run.body)?;
        let mut tx = self.pool().begin().await.map_err(backend)?;
        sqlx::query!(
            "insert into transforms.run \
                 (run_id, transform, trigger, state, body, queued_at) \
             values ($1, $2, $3, $4, $5, $6)",
            run.run_id,
            run.transform.as_ref().map(|t| t.0.clone()),
            run.trigger.as_str(),
            run.state.as_str(),
            body,
            run.queued_at,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        let id = pg_insert(&mut *tx, &job).await?;
        tx.commit().await.map_err(backend)?;
        Ok(id)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn mark_run_running(&self, run_id: Uuid) -> Result<()> {
        let res = sqlx::query!(
            "update transforms.run set state = 'running', started_at = now() \
             where run_id = $1",
            run_id,
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        if res.rows_affected() == 0 {
            return Err(ControlPlaneError::NotFound(format!("run {run_id}")));
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn finish_run(&self, run_id: Uuid, outcome: RunOutcome) -> Result<()> {
        let res = match outcome {
            RunOutcome::Succeeded { snapshot_id } => {
                return pg_mark_run_succeeded(self.pool(), run_id, snapshot_id).await;
            }
            RunOutcome::RetryQueued { error } => sqlx::query!(
                "update transforms.run set state = 'queued', error = $2 where run_id = $1",
                run_id,
                error,
            )
            .execute(self.pool())
            .await
            .map_err(backend)?,
            RunOutcome::Failed { error } => sqlx::query!(
                "update transforms.run \
                 set state = 'failed', error = $2, finished_at = now() where run_id = $1",
                run_id,
                error,
            )
            .execute(self.pool())
            .await
            .map_err(backend)?,
        };
        if res.rows_affected() == 0 {
            return Err(ControlPlaneError::NotFound(format!("run {run_id}")));
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_run(&self, run_id: Uuid) -> Result<TransformRun> {
        let r = sqlx::query!(
            "select run_id, transform, trigger, state, body, queued_at, \
                    started_at, finished_at, snapshot_id, error \
             from transforms.run where run_id = $1",
            run_id,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(format!("run {run_id}")))?;
        Ok(TransformRun {
            run_id: r.run_id,
            transform: r.transform.map(TransformName),
            trigger: r.trigger.parse::<RunTrigger>()?,
            state: r.state.parse::<RunState>()?,
            body: de_body(r.body)?,
            queued_at: r.queued_at,
            started_at: r.started_at,
            finished_at: r.finished_at,
            snapshot_id: r.snapshot_id,
            error: r.error,
        })
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_runs(
        &self,
        transform: Option<&TransformName>,
        _page: PageReq,
    ) -> Result<Page<TransformRun>> {
        let rows = sqlx::query!(
            "select run_id, transform, trigger, state, body, queued_at, \
                    started_at, finished_at, snapshot_id, error \
             from transforms.run \
             where ($1::text is null or transform = $1) \
             order by queued_at desc, run_id desc",
            transform.map(|t| t.0.clone()),
        )
        .fetch_all(self.pool())
        .await
        .map_err(backend)?;
        let items = rows
            .into_iter()
            .map(|r| {
                Ok(TransformRun {
                    run_id: r.run_id,
                    transform: r.transform.map(TransformName),
                    trigger: r.trigger.parse::<RunTrigger>()?,
                    state: r.state.parse::<RunState>()?,
                    body: de_body(r.body)?,
                    queued_at: r.queued_at,
                    started_at: r.started_at,
                    finished_at: r.finished_at,
                    snapshot_id: r.snapshot_id,
                    error: r.error,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Page::from_full(items))
    }
}
```

(Column-nullability details — e.g. whether `query!` types `count(*)` and nullable columns as `Option` — will surface when the `.sqlx` cache is generated; adjust the decode lines accordingly. If a duplicated run-row decoder between `get_run`/`list_runs` bothers the reviewer, factor a `fn decode_run(row) -> Result<TransformRun>` — preferred.)

- [ ] **Step 5: Regenerate the sqlx cache** — `bash tools/sqlx-prepare.sh` (expect new `query-*.json` files under `src/control-plane/postgres/.sqlx/`; commit them).

- [ ] **Step 6: Run the contract** — `buck2 test //src/control-plane/postgres:transforms --unstable-allow-all-tests-on-re > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log` → PASS. Also run the cache-freshness gate: `buck2 test //src/control-plane/postgres:sqlx-cache-check --unstable-allow-all-tests-on-re > /tmp/t4b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4b.log` → PASS.

- [ ] **Step 7: prek + commit** (`feat(control-plane): postgres transforms adapter (migration 0031 + .sqlx)`)

---

### Task 5: `ControlPlane::transforms()` accessor — all four implementors

**Files:**
- Modify: `src/control-plane/core/src/transaction.rs` (trait method)
- Modify: `src/control-plane/memory/src/lib.rs:189` (`impl ControlPlane`)
- Modify: `src/control-plane/postgres/src/lib.rs:94` (`impl ControlPlane`)
- Modify: `src/control-plane/postgres/src/iceberg_control_plane.rs:58` (`impl ControlPlane`)
- Modify: `src/services/query-api/src/wire_control_plane.rs:221` (`impl ControlPlane`)
- Modify: `src/services/query-api/tests/wire_governance_e2e.rs` (delegation assert)

**Interfaces:**
- Consumes: Tasks 3–4 (`Transforms` impls on both planes).
- Produces: `fn transforms(&self) -> &(dyn Transforms + Send + Sync)` on `ControlPlane` — Task 7 (engine) and Task 10 (admin routes) call `cp.transforms()`.

- [ ] **Step 1: Add to the trait** in `src/control-plane/core/src/transaction.rs` (after `queue()`):

```rust
    fn transforms(&self) -> &(dyn Transforms + Send + Sync);
```

(plus `use crate::transforms::Transforms;` in that file's imports).

- [ ] **Step 2: Implement in all four implementors:**

```rust
// memory/src/lib.rs and postgres/src/lib.rs (both planes implement the trait directly):
    fn transforms(&self) -> &(dyn Transforms + Send + Sync) {
        self
    }
// postgres/src/iceberg_control_plane.rs (delegates like the other concerns):
    fn transforms(&self) -> &(dyn Transforms + Send + Sync) {
        self.pg.transforms()
    }
// query-api/src/wire_control_plane.rs (management surface is served direct, like catalog()):
    fn transforms(&self) -> &(dyn Transforms + Send + Sync) {
        self.direct.transforms()
    }
```

Add the `Transforms` import to each file's `control_plane_core` use list. Update the `wire_control_plane.rs` module doc's concern table/prose to mention transforms delegating direct.

- [ ] **Step 3: Extend the wire delegation test** — in `src/services/query-api/tests/wire_governance_e2e.rs`, find `wire_catalog_delegates_direct` and add alongside (inside it or as a sibling assert following its pattern):

```rust
    let transforms = wire.transforms().list_transforms(PageReq::default()).await
        .expect("wire transforms() delegates to the direct plane");
    assert!(transforms.next.is_none());
```

- [ ] **Step 4: Whole-tree build + affected tests**

Run: `buck2 build -M none //src/... > /tmp/t5.log 2>&1; tail -3 /tmp/t5.log` → SUCCEEDED (this catches any fifth implementor the grep missed).
Run: `buck2 test //src/control-plane/... --unstable-allow-all-tests-on-re > /tmp/t5b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5b.log` → PASS.
Run: `buck2 test //src/services/query-api:wire-governance-e2e --unstable-allow-all-tests-on-re > /tmp/t5c.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5c.log` (use the target name that owns `wire_governance_e2e.rs` — check the BUCK file) → PASS.

- [ ] **Step 5: prek + commit** (`feat(control-plane): ControlPlane::transforms() accessor across all planes`)

---

### Task 6: `TableTx::mark_run_succeeded` — in-commit success marking

**Files:**
- Modify: `src/control-plane/core/src/transaction.rs` (`TableTx` trait)
- Modify: `src/control-plane/postgres/src/iceberg_control_plane.rs` (`IcebergTx`)
- Modify: `src/control-plane/memory/src/transaction.rs` (`MemoryTx`)
- Modify: `src/control-plane/testkit/src/lib.rs` (contract)
- Modify: `src/control-plane/memory/tests/transforms.rs`, `src/control-plane/postgres/tests/` (invoke it)

**Interfaces:**
- Consumes: Task 4's `pg_mark_run_succeeded`, Task 3's `apply_outcome`.
- Produces: `TableTx::mark_run_succeeded(&mut self, run_id: Uuid) -> Result<()>` — staged; applied at `commit()` with the snapshot that commit allocates. Task 7's engine handler calls it.

- [ ] **Step 1: Contract first** — append to testkit:

```rust
/// The success mark rides the table-commit transaction: the run flips to
/// Succeeded with the very snapshot the commit allocates, atomically.
pub async fn transform_run_commit_success_contract<CP>(cp: &CP)
where
    CP: ControlPlane + Transforms + TableControlPlane,
{
    use control_plane_core::{
        ColumnSpec, DataFile, OutputMode, RunState, RunTrigger, TransformBody, TransformRun,
    };

    let rid = uuid::Uuid::new_v4();
    let body = TransformBody::Physical {
        inputs: vec![tref("main", "src")],
        output: tref("main", "run_dst"),
        sql: "select 1".into(),
        output_mode: OutputMode::Append,
    };
    let run = TransformRun {
        run_id: rid,
        transform: None,
        trigger: RunTrigger::AdHoc,
        state: RunState::Queued,
        body: body.clone(),
        queued_at: time::OffsetDateTime::now_utc(),
        started_at: None,
        finished_at: None,
        snapshot_id: None,
        error: None,
    };
    cp.submit_run(run, body.to_job(rid)).await.unwrap();

    let out = tref("main", "run_dst");
    let cols = vec![ColumnSpec { name: "id".into(), ty: "long".into(), nullable: true }];
    let files = vec![DataFile { path: "f1.parquet".into(), record_count: 1, file_size_bytes: 10 }];

    // Rollback leaves the run untouched.
    let mut tx = cp.begin_table().await.unwrap();
    tx.create_table(&out, &cols).await.unwrap();
    tx.append_files(&out, &files).await.unwrap();
    tx.mark_run_succeeded(rid).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(cp.get_run(rid).await.unwrap().state, RunState::Queued);

    // Commit marks Succeeded at the allocated snapshot.
    let mut tx = cp.begin_table().await.unwrap();
    tx.create_table(&out, &cols).await.unwrap();
    tx.append_files(&out, &files).await.unwrap();
    tx.mark_run_succeeded(rid).await.unwrap();
    let snap = tx.commit().await.unwrap().expect("a snapshot");
    let r = cp.get_run(rid).await.unwrap();
    assert_eq!(r.state, RunState::Succeeded);
    assert_eq!(r.snapshot_id, Some(snap.0));
}
```

(Match `ColumnSpec`/`DataFile` field names to their actual core definitions — check `src/control-plane/core/src/snapshot.rs`; the existing `queue_contract`/catalog contracts already construct them, copy that shape.)

Invoke from `src/control-plane/memory/tests/transforms.rs` (add a second `#[tokio::test]` calling it with a fresh `MemoryControlPlane`). For postgres: find the existing fixture test that exercises `IcebergControlPlane`/`begin_table` (`grep -rln "begin_table\|IcebergControlPlane" src/control-plane/postgres/tests/`) and mirror its setup (it constructs `IcebergControlPlane::new(cp, catalog)` over the fixture's `SqlCatalog`); add a `#[tokio::test]` in `src/control-plane/postgres/tests/transforms.rs` invoking the contract with that plane, reusing the same setup helper.

- [ ] **Step 2: Run to verify failure** — memory transforms test target → compile error (`mark_run_succeeded` not on `TableTx`).

- [ ] **Step 3: Implement.** Core `TableTx` (in `transaction.rs`, after `compact_files`):

```rust
    /// Stage a transform-run success mark: at `commit()`, the run flips to
    /// `Succeeded` carrying the snapshot that commit allocates — in the same
    /// unit of work as the data. Staging twice is an error; committing with a
    /// staged mark but no snapshot-producing write is an error (a success
    /// mark without a snapshot is meaningless — see `RunOutcome::Succeeded`).
    async fn mark_run_succeeded(&mut self, run_id: Uuid) -> Result<()>;
```

`IcebergTx` (`iceberg_control_plane.rs`): add field `staged_run_success: Option<Uuid>` (init `None` in `begin_table`), impl:

```rust
    async fn mark_run_succeeded(&mut self, run_id: Uuid) -> Result<()> {
        if self.staged_run_success.is_some() {
            return Err(ControlPlaneError::Validation(
                "a run success mark is already staged on this transaction".into(),
            ));
        }
        self.staged_run_success = Some(run_id);
        Ok(())
    }
```

In `IcebergTx::commit`, destructure the new field, and (a) in the early-return branch (nothing staged): if `staged_run_success.is_some()`, return a `Validation` error before committing; (b) after `register_files`/compaction loops, before `tx.commit()`:

```rust
        if let Some(rid) = staged_run_success {
            crate::transforms::pg_mark_run_succeeded(&mut *tx, rid, at.0).await?;
        }
```

`MemoryTx` (`memory/src/transaction.rs`): add fields `staged_run_success: Option<Uuid>` and `transforms: Arc<Mutex<TransformsState>>` (thread it from `MemoryControlPlane::begin_table` — find where `MemoryTx` is constructed, `grep -n "MemoryTx {" src/control-plane/memory/src/`). Same staging guard. In `commit`, after the catalog-writes section computes `last_snapshot` (still inside or right after the locked block — take the `transforms` lock AFTER the three existing locks; note in a comment that no reader takes `transforms` plus another lock, so lock order is safe):

```rust
        if let Some(rid) = staged_run_success {
            let Some(s) = last_snapshot else {
                return Err(control_plane_core::ControlPlaneError::Validation(
                    "mark_run_succeeded staged without a snapshot-producing write".into(),
                ));
            };
            let mut st = transforms.lock();
            let run = st.runs.get_mut(&rid).ok_or_else(|| {
                control_plane_core::ControlPlaneError::NotFound(format!("run {rid}"))
            })?;
            crate::transforms::apply_outcome(run, RunOutcome::Succeeded { snapshot_id: s });
        }
```

CAREFUL: the validation-before-mutation discipline in `MemoryTx::commit` (see its comment block) — do the `last_snapshot.is_none()`/unknown-run checks in a way that cannot leave partial state: the run mutation is the LAST state change, and the unknown-run check must happen BEFORE the queue/lineage/catalog sections apply. Easiest correct order: at the top of the locked block, if a run success is staged, verify the run exists in `transforms` (early-return `NotFound` before anything mutates); the snapshot check + mutation stay at the end. (`last_snapshot` is only known at the end — a staged success with no staged writes fails there; accept that this particular misuse aborts after queue/lineage applied, and prevent it structurally: the engine only stages the mark alongside `create_table`+files, and the guard is for programmer error. Mirror the postgres behaviour, where the error likewise aborts the whole sqlx transaction — for memory, do the no-writes check FIRST: `if staged_run_success.is_some() && self.staged_tables.is_empty() && self.staged_writes.is_empty() && self.staged_compactions.is_empty() { return Err(...) }` before any mutation, which restores strict validate-before-mutate.)

- [ ] **Step 4: Run** memory + postgres transforms targets + `buck2 build -M none //src/...` (the trait change breaks any other `TableTx` impl — grep confirmed only these two).
- [ ] **Step 5: prek + commit** (`feat(control-plane): TableTx::mark_run_succeeded — run success in the commit tx`)

---

### Task 7: Engine-wire RPCs + engine handlers

**Files:**
- Modify: `src/services/engine-wire/proto/engine_control.proto`
- Modify: `src/services/engine-wire/src/client.rs`
- Modify: `src/services/engine/src/service.rs`
- Modify: every `commit_transform(` caller (worker `src/services/worker/src/transform.rs` — signature gains an arg)

**Interfaces:**
- Consumes: Task 5 (`cp.transforms()`), Task 6 (`TableTx::mark_run_succeeded`).
- Produces (Task 8 calls these):
  - `EngineControlClient::mark_run_running(&self, run_id: Uuid) -> Result<()>`
  - `EngineControlClient::finish_run_failed(&self, run_id: Uuid, error: &str, terminal: bool) -> Result<()>`
  - `commit_transform(..., run_id: Option<Uuid>)` — new trailing parameter.

- [ ] **Step 1: Proto** — in `engine_control.proto`, add to the service block:

```protobuf
  rpc MarkRunRunning  (MarkRunRunningRequest)  returns (MarkRunRunningResponse);
  rpc FinishRunFailed (FinishRunFailedRequest) returns (FinishRunFailedResponse);
```

messages (near the transform messages):

```protobuf
message MarkRunRunningRequest  { string run_id = 1; }
message MarkRunRunningResponse {}
message FinishRunFailedRequest {
  string run_id = 1;
  string error = 2;
  bool terminal = 3;  // true => Failed; false => back to Queued (retry)
}
message FinishRunFailedResponse {}
```

and extend `CommitTransformRequest` with `optional string run_id = 7;`.

- [ ] **Step 2: Client** (`client.rs`) — hand-rolled like `commit_transform` (the `gov_rpc!` macro is for JSON-page reads; these are fire-and-forget):

```rust
    /// Mark a transform run Running (dequeued by a worker).
    pub async fn mark_run_running(&self, run_id: uuid::Uuid) -> Result<()> {
        self.inner
            .clone()
            .mark_run_running(pb::MarkRunRunningRequest { run_id: run_id.to_string() })
            .await
            .map_err(cp_status)?;
        Ok(())
    }

    /// Report a run failure: `terminal` abandons (Failed); otherwise the run
    /// goes back to Queued with the error retained.
    pub async fn finish_run_failed(
        &self,
        run_id: uuid::Uuid,
        error: &str,
        terminal: bool,
    ) -> Result<()> {
        self.inner
            .clone()
            .finish_run_failed(pb::FinishRunFailedRequest {
                run_id: run_id.to_string(),
                error: error.to_string(),
                terminal,
            })
            .await
            .map_err(cp_status)?;
        Ok(())
    }
```

(Use whichever error-mapping helper `commit_transform` uses — it maps with `be`; the gov reads use `cp_status`. Mirror `commit_transform`'s `be` if `cp_status` is read-specific — check both helpers and pick the one that preserves NotFound, which matters for `mark_run_running` on a vanished run. If `be` collapses everything to Backend, use `cp_status`.)

`commit_transform` signature gains `run_id: Option<uuid::Uuid>` as the last parameter and sets `run_id: run_id.map(|u| u.to_string())` in the request.

- [ ] **Step 3: Engine handlers** (`service.rs`) — new trait methods on the `EngineControl` impl:

```rust
    async fn mark_run_running(
        &self,
        req: Request<pb::MarkRunRunningRequest>,
    ) -> std::result::Result<Response<pb::MarkRunRunningResponse>, Status> {
        let rid = parse_run_id(&req.into_inner().run_id)?;
        self.cp.transforms().mark_run_running(rid).await.map_err(status)?;
        Ok(Response::new(pb::MarkRunRunningResponse {}))
    }

    async fn finish_run_failed(
        &self,
        req: Request<pb::FinishRunFailedRequest>,
    ) -> std::result::Result<Response<pb::FinishRunFailedResponse>, Status> {
        let r = req.into_inner();
        let rid = parse_run_id(&r.run_id)?;
        let outcome = if r.terminal {
            control_plane_core::RunOutcome::Failed { error: r.error }
        } else {
            control_plane_core::RunOutcome::RetryQueued { error: r.error }
        };
        self.cp.transforms().finish_run(rid, outcome).await.map_err(status)?;
        Ok(Response::new(pb::FinishRunFailedResponse {}))
    }
```

with a small helper near the other decode helpers:

```rust
fn parse_run_id(s: &str) -> std::result::Result<uuid::Uuid, Status> {
    uuid::Uuid::parse_str(s).map_err(|e| Status::invalid_argument(format!("bad run_id: {e}")))
}
```

In the existing `commit_transform` handler, after the tx is built and files staged (right before `tx.commit()` — i.e. after the `append_files`/`replace_files` call and `tx.emit(lineage)`), add:

```rust
    if let Some(rid) = r.run_id.as_deref() {
        let rid = parse_run_id(rid)?;
        tx.mark_run_succeeded(rid).await.map_err(status)?;
    }
```

- [ ] **Step 4: Update `commit_transform` callers** — `grep -rn "commit_transform(" src/ --include=*.rs`; the worker's `run_wire_transform` call passes `None` for now (Task 8 threads the real id).

- [ ] **Step 5: Build the affected services**

Run: `buck2 build -M none //src/services/engine-wire/... //src/services/engine/... //src/services/worker/... > /tmp/t7.log 2>&1; tail -3 /tmp/t7.log` → SUCCEEDED.

- [ ] **Step 6: prek + commit** (`feat(engine): run-lifecycle RPCs — MarkRunRunning, FinishRunFailed, CommitTransform{run_id}`)

---

### Task 8: Worker lifecycle threading

**Files:**
- Modify: `src/services/worker/src/transform.rs`

**Interfaces:**
- Consumes: Task 7's client methods; payload `run_id` from Task 2.
- Produces: worker behaviour — on a `run_id` payload: MarkRunRunning after parse; lineage `run_id` reuses it; commit passes it; every failure path reports `finish_run_failed` (terminal iff Abandon) before returning.

- [ ] **Step 1: Restructure `handle_transform` and `handle_typed_transform`** into thin lifecycle wrappers. Pattern (apply to both; the bodies differ only in the inner fn):

```rust
pub async fn handle_transform(ctx: &TransformCtx, job: Job) -> std::result::Result<(), JobFailure> {
    let attempts = job.attempts;
    let parsed: TransformJob = serde_json::from_value(job.payload)
        .map_err(|e| JobFailure::abandon(format!("bad transform payload: {e}")))?;
    let run_id = parsed.run_id;
    if let Some(rid) = run_id {
        // Failure to reach the engine is retryable — the run record stays
        // Queued and the retry re-marks it.
        ctx.control.mark_run_running(rid).await.map_err(|e| {
            JobFailure::retry(ctx.worker_tuning.backoff(attempts), format!("mark_run_running: {e}"))
        })?;
    }
    let result = transform_inner(ctx, attempts, parsed).await;
    report_run_failure(ctx, run_id, &result).await;
    result
}

/// Best-effort failure reporting: the run record must reflect the failure,
/// but a reporting error must not mask the original failure (the queue's
/// retry/abandon decision stands either way).
async fn report_run_failure(
    ctx: &TransformCtx,
    run_id: Option<uuid::Uuid>,
    result: &std::result::Result<(), JobFailure>,
) {
    let (Some(rid), Err(f)) = (run_id, result) else { return };
    let terminal = matches!(f.policy, RetryPolicy::Abandon);
    if let Err(e) = ctx.control.finish_run_failed(rid, &f.error, terminal).await {
        tracing::warn!(run_id = %rid, error = %e, "failed to report run failure");
    }
}
```

`transform_inner` is the existing body from payload-parse onward (the current `handle_transform` minus the parse), with two changes:
1. The lineage event uses `RunId(run_id.unwrap_or_else(uuid::Uuid::new_v4))` instead of always-fresh (thread `run_id`/`parsed.run_id` in — the parsed struct is already available).
2. The `commit_transform(...)` call passes `parsed.run_id` as the new trailing argument.

(Check `JobFailure`'s field names in `src/control-plane/core/src/queue.rs` — `{ error: String, policy: RetryPolicy }`; adjust the match accordingly. If `handle_typed_transform` funnels into the shared `run_wire_transform`, put the lifecycle wrapper around that shared funnel instead of duplicating it twice — `WireTransform` likely needs a `run_id: Option<Uuid>` field so the shared path can commit with it; prefer that over duplication.)

- [ ] **Step 2: Build + existing e2e still green (legacy no-run path)**

Run: `buck2 build -M none //src/services/worker/... > /tmp/t8.log 2>&1; tail -3 /tmp/t8.log` → SUCCEEDED.
Run: `buck2 test //src/services/worker:transform-e2e //src/services/worker:typed-transform-e2e --unstable-allow-all-tests-on-re > /tmp/t8b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t8b.log` (use the actual target names from `src/services/worker/BUCK`) → PASS.

- [ ] **Step 3: prek + commit** (`feat(worker): thread run lifecycle through transform execution`)

---

### Task 9: Worker e2e — run lifecycle end to end

**Files:**
- Modify: `src/services/worker/tests/transform_e2e.rs`

**Interfaces:**
- Consumes: everything above. This is the slice's integration proof.

- [ ] **Step 1: Add two tests** to `transform_e2e.rs`, reusing its existing fixture/bootstrap helpers (`PgFixture::shared`, `spawn_engine_uds`, `build_ctx`, seeding of `main.src` — read the file first and reuse its `setup`/seed sections verbatim):

```rust
#[tokio::test]
async fn run_lifecycle_succeeds_with_commit_snapshot() {
    // ... existing-style setup: fixture, engine, seeded main.src ...
    let def = TransformDef {
        name: TransformName("daily".into()),
        body: TransformBody::Physical {
            inputs: vec![src.clone()],
            output: dst.clone(),
            sql: "select * from src where id > 1".into(),
            output_mode: OutputMode::Append,
        },
        schedule: None,
        on_input_commit: false,
    };
    cp.transforms().define_transform(def.clone()).await.expect("define");
    let rid = uuid::Uuid::new_v4();
    let run = TransformRun { run_id: rid, transform: Some(def.name.clone()),
        trigger: RunTrigger::Manual, state: RunState::Queued, body: def.body.clone(),
        queued_at: time::OffsetDateTime::now_utc(), started_at: None, finished_at: None,
        snapshot_id: None, error: None };
    cp.transforms().submit_run(run, def.body.to_job(rid)).await.expect("submit");

    let ctx = build_ctx(&eng.sock, &wh_str).await;
    let job = ctx.control.dequeue(&[TRANSFORM_JOB_KIND.to_string()], "e2e-worker")
        .await.expect("dequeue").expect("job");
    handle_transform(&ctx, job).await.expect("transform");

    let r = cp.transforms().get_run(rid).await.expect("run");
    assert_eq!(r.state, RunState::Succeeded);
    assert!(r.started_at.is_some() && r.finished_at.is_some());
    // The run's snapshot is the table's committed snapshot.
    let ice = IcebergCatalog::new(pool.clone());
    let snap = ice.current_snapshot(&dst).await.expect("output snapshot");
    assert_eq!(r.snapshot_id, Some(snap.id));
    // The lineage run_id IS the transform run_id.
    let lineage_run: uuid::Uuid = sqlx::query_scalar(
        "select e.run_id from lineage.event e \
         join lineage.event_dataset d on d.event_id = e.event_id and d.direction = 'output' \
         where d.name = $1",
    )
    .bind("main.dst")
    .fetch_one(&pool)
    .await
    .expect("lineage run id");
    assert_eq!(lineage_run, rid);
}

#[tokio::test]
async fn run_lifecycle_fails_terminally_on_bad_sql() {
    // ... same setup ...
    let rid = uuid::Uuid::new_v4();
    let body = TransformBody::Physical {
        inputs: vec![src.clone()],
        output: dst.clone(),
        sql: "select definitely_not_a_column from src".into(),
        output_mode: OutputMode::Append,
    };
    let run = TransformRun { run_id: rid, transform: None, trigger: RunTrigger::AdHoc,
        state: RunState::Queued, body: body.clone(),
        queued_at: time::OffsetDateTime::now_utc(), started_at: None, finished_at: None,
        snapshot_id: None, error: None };
    cp.transforms().submit_run(run, body.to_job(rid)).await.expect("submit");
    let ctx = build_ctx(&eng.sock, &wh_str).await;
    let job = ctx.control.dequeue(&[TRANSFORM_JOB_KIND.to_string()], "e2e-worker")
        .await.expect("dequeue").expect("job");
    let err = handle_transform(&ctx, job).await.expect_err("bad sql must fail");
    assert!(matches!(err.policy, RetryPolicy::Abandon), "bad SQL is deterministic");
    let r = cp.transforms().get_run(rid).await.expect("run");
    assert_eq!(r.state, RunState::Failed);
    assert!(r.error.as_deref().unwrap_or_default().contains("definitely_not_a_column") 
        || r.error.is_some(), "error text recorded");
}
```

(Verify the lineage `run_id` column name/type against migration files — `grep -n run_id src/control-plane/postgres/migrations/*lineage*` — and adjust the query if it is stored as text. Confirm whether bad-SQL is Abandon in the worker's error mapping; if the DataFusion plan error maps to Retry instead, pick a deterministic-abandon input — e.g. a typed transform with a conformance mismatch, or an unknown input table — and assert THAT shape. The point is: terminal failure ⇒ run Failed with error text.)

- [ ] **Step 2: Run**

Run: `buck2 test //src/services/worker:transform-e2e --unstable-allow-all-tests-on-re > /tmp/t9.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t9.log` → PASS (all, incl. the pre-existing tests).

- [ ] **Step 3: prek + commit** (`test(worker): transform run lifecycle e2e — succeeded snapshot + terminal failure`)

---

### Task 10: Admin HTTP surface + OpenAPI

**Files:**
- Modify: `src/services/runtime/src/admin.rs` (8 handlers, DTOs, router, `AdminApiDoc`)
- Modify: `src/services/runtime/tests/openapi_fragments.rs` (19 → 27)
- Modify: `src/services/runtime/tests/admin_management.rs` (transform tests)
- Modify: `src/services/query-api/tests/openapi.rs` (`expected()` +8)
- Modify: `src/services/runtime/BUCK` if new deps needed (`uuid` — check; `time` already present)

**Interfaces:**
- Consumes: Task 5's `st.cp.transforms()`.
- Produces: routes exactly:
  `POST /admin/transforms` (201/400), `GET /admin/transforms` (200), `GET /admin/transforms/{name}` (200/404), `DELETE /admin/transforms/{name}` (200), `POST /admin/transforms/{name}/run` (202/404), `POST /admin/transforms/run` (202/400), `GET /admin/transforms/{name}/runs` (200/404), `GET /admin/runs/{run_id}` (200/400/404).

- [ ] **Step 1: Extend the fragment drift test FIRST** — in `openapi_fragments.rs`, add to the expected set:

```rust
        ("post", "/admin/transforms"),
        ("get", "/admin/transforms"),
        ("get", "/admin/transforms/{name}"),
        ("delete", "/admin/transforms/{name}"),
        ("post", "/admin/transforms/{name}/run"),
        ("post", "/admin/transforms/run"),
        ("get", "/admin/transforms/{name}/runs"),
        ("get", "/admin/runs/{run_id}"),
```

Run it → FAIL (routes not documented yet).

- [ ] **Step 2: DTOs + handlers in `admin.rs`.** DTOs (next to the existing ones):

```rust
/// A transform definition, echoed in its serde shape.
#[derive(serde::Serialize, utoipa::ToSchema)]
struct TransformDefView {
    name: String,
    /// The `TransformBody` serde shape (`{"kind": "physical"|"typed", ...}`).
    #[schema(value_type = Object)]
    body: serde_json::Value,
    schedule: Option<String>,
    on_input_commit: bool,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct ListTransformsResp { transforms: Vec<TransformDefView> }

/// One transform run.
#[derive(serde::Serialize, utoipa::ToSchema)]
struct TransformRunView {
    run_id: String,
    /// Absent for ad-hoc runs.
    transform: Option<String>,
    trigger: String,
    state: String,
    #[schema(value_type = Object)]
    body: serde_json::Value,
    queued_at: String,
    started_at: Option<String>,
    finished_at: Option<String>,
    snapshot_id: Option<i64>,
    error: Option<String>,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct ListRunsResp { runs: Vec<TransformRunView> }

/// Acknowledgement of an accepted (asynchronous) run submission.
#[derive(serde::Serialize, utoipa::ToSchema)]
struct RunSubmittedResp { run_id: String }
```

Conversion helpers (plain fns near the DTOs; if the codebase already formats timestamps somewhere — grep `Rfc3339` in runtime/query-api — mirror that; otherwise use the well-known formatter as below):

```rust
use time::format_description::well_known::Rfc3339;

fn rfc3339(t: time::OffsetDateTime) -> String {
    t.format(&Rfc3339).unwrap_or_default()
}

fn def_view(d: &TransformDef) -> TransformDefView {
    TransformDefView {
        name: d.name.0.clone(),
        body: serde_json::to_value(&d.body).unwrap_or(serde_json::Value::Null),
        schedule: d.schedule.clone(),
        on_input_commit: d.on_input_commit,
    }
}

fn run_view(r: &TransformRun) -> TransformRunView {
    TransformRunView {
        run_id: r.run_id.to_string(),
        transform: r.transform.as_ref().map(|t| t.0.clone()),
        trigger: r.trigger.as_str().to_string(),
        state: r.state.as_str().to_string(),
        body: serde_json::to_value(&r.body).unwrap_or(serde_json::Value::Null),
        queued_at: rfc3339(r.queued_at),
        started_at: r.started_at.map(rfc3339),
        finished_at: r.finished_at.map(rfc3339),
        snapshot_id: r.snapshot_id,
        error: r.error.clone(),
    }
}
```

The shared submit path (used by run-now and ad-hoc):

```rust
async fn submit_new_run(
    st: &AdminState,
    transform: Option<TransformName>,
    trigger: RunTrigger,
    body: TransformBody,
) -> Response {
    let run_id = uuid::Uuid::new_v4();
    let run = TransformRun {
        run_id,
        transform,
        trigger,
        state: RunState::Queued,
        body: body.clone(),
        queued_at: time::OffsetDateTime::now_utc(),
        started_at: None,
        finished_at: None,
        snapshot_id: None,
        error: None,
    };
    match st.cp.transforms().submit_run(run, body.to_job(run_id)).await {
        Ok(_) => (
            StatusCode::ACCEPTED,
            Json(RunSubmittedResp { run_id: run_id.to_string() }),
        )
            .into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}
```

The eight handlers (each with a `#[utoipa::path]` mirroring the existing style — `tag = "admin"`, `security(("bearer_auth" = []))`, documented statuses per the route table above; POST bodies are the open `Json<serde_json::Value>` → `serde_json::from_value` → 400 `"invalid TransformDef: {e}"` / `"invalid TransformBody: {e}"` pattern from `define_link_route`):

```rust
async fn define_transform_route(State(st): State<AdminState>, Json(body): Json<serde_json::Value>) -> Response {
    let def: TransformDef = match serde_json::from_value(body) {
        Ok(d) => d,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("invalid TransformDef: {e}")).into_response(),
    };
    match st.cp.transforms().define_transform(def).await {
        Ok(()) => (StatusCode::CREATED, "defined").into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

async fn list_transforms_route(State(st): State<AdminState>) -> Response {
    match st.cp.transforms().list_transforms(PageReq::default()).await {
        Ok(page) => {
            let transforms = page.items.iter().map(def_view).collect();
            (StatusCode::OK, Json(ListTransformsResp { transforms })).into_response()
        }
        Err(e) => status_for(&e).into_response(),
    }
}

async fn get_transform_route(State(st): State<AdminState>, Path(name): Path<String>) -> Response {
    match st.cp.transforms().get_transform(&TransformName(name)).await {
        Ok(def) => (StatusCode::OK, Json(def_view(&def))).into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

async fn delete_transform_route(State(st): State<AdminState>, Path(name): Path<String>) -> Response {
    match st.cp.transforms().delete_transform(&TransformName(name.clone())).await {
        Ok(()) => Json(serde_json::json!({ "deleted": name })).into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

async fn run_transform_route(State(st): State<AdminState>, Path(name): Path<String>) -> Response {
    let name = TransformName(name);
    let def = match st.cp.transforms().get_transform(&name).await {
        Ok(d) => d,
        Err(e) => return status_for(&e).into_response(),
    };
    submit_new_run(&st, Some(name), RunTrigger::Manual, def.body).await
}

async fn run_adhoc_route(State(st): State<AdminState>, Json(body): Json<serde_json::Value>) -> Response {
    let body: TransformBody = match serde_json::from_value(body) {
        Ok(b) => b,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("invalid TransformBody: {e}")).into_response(),
    };
    submit_new_run(&st, None, RunTrigger::AdHoc, body).await
}

async fn list_transform_runs(State(st): State<AdminState>, Path(name): Path<String>) -> Response {
    let name = TransformName(name);
    // 404 for an unknown transform name (distinguish "no runs" from "no such transform").
    if let Err(e) = st.cp.transforms().get_transform(&name).await {
        return status_for(&e).into_response();
    }
    match st.cp.transforms().list_runs(Some(&name), PageReq::default()).await {
        Ok(page) => {
            let runs = page.items.iter().map(run_view).collect();
            (StatusCode::OK, Json(ListRunsResp { runs })).into_response()
        }
        Err(e) => status_for(&e).into_response(),
    }
}

async fn get_run_route(State(st): State<AdminState>, Path(run_id): Path<String>) -> Response {
    let rid = match uuid::Uuid::parse_str(&run_id) {
        Ok(u) => u,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("invalid run id: {e}")).into_response(),
    };
    match st.cp.transforms().get_run(rid).await {
        Ok(run) => (StatusCode::OK, Json(run_view(&run))).into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}
```

Router additions (order matters for the static `run` segment — register `/admin/transforms/run` BEFORE the `{name}` routes is NOT required with axum's matchit (static wins), but keep them adjacent for readability):

```rust
        .route("/admin/transforms", post(define_transform_route).get(list_transforms_route))
        .route("/admin/transforms/run", post(run_adhoc_route))
        .route(
            "/admin/transforms/:name",
            get(get_transform_route).delete(delete_transform_route),
        )
        .route("/admin/transforms/:name/run", post(run_transform_route))
        .route("/admin/transforms/:name/runs", get(list_transform_runs))
        .route("/admin/runs/:run_id", get(get_run_route))
```

`AdminApiDoc`: add the eight handler fn names to `paths(...)` and `TransformDefView, ListTransformsResp, TransformRunView, ListRunsResp, RunSubmittedResp` to `components(schemas(...))`. Imports: add `TransformBody, TransformDef, TransformName, TransformRun, RunState, RunTrigger, PageReq` to the `control_plane_core` use list; check `src/services/runtime/BUCK` has `//third-party:uuid` (add if missing) — `time` is already a dep.

- [ ] **Step 3: Route tests** — extend `src/services/runtime/tests/admin_management.rs` (reuse its `states`/`app`/`seed_admin_session`/`seed_types`/`send`/`req_json`/`req_empty` helpers):

```rust
const TRANSFORM_BODY: &str = r#"{
    "name": "daily",
    "body": {"kind": "physical",
             "inputs": [{"schema": "main", "name": "src"}],
             "output": {"schema": "main", "name": "dst"},
             "sql": "select * from src"}
}"#;

#[tokio::test]
async fn define_get_delete_transform() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    let (status, _) = send(app(cp.clone()), req_json("POST", "/admin/transforms", &token, TRANSFORM_BODY)).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = send(app(cp.clone()), req_empty("GET", "/admin/transforms/daily", &token)).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["body"]["kind"], "physical");
    // list
    let (status, body) = send(app(cp.clone()), req_empty("GET", "/admin/transforms", &token)).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["transforms"].as_array().unwrap().len(), 1);
    // delete twice — idempotent
    let (status, body) = send(app(cp.clone()), req_empty("DELETE", "/admin/transforms/daily", &token)).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["deleted"], "daily");
    let (status, _) = send(app(cp.clone()), req_empty("DELETE", "/admin/transforms/daily", &token)).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(app(cp), req_empty("GET", "/admin/transforms/daily", &token)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn define_transform_rejects_bad_shapes() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    // not a TransformDef
    let (status, _) = send(app(cp.clone()), req_json("POST", "/admin/transforms", &token, r#"{"nope": 1}"#)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // schedule not yet supported (slice 2)
    let scheduled = TRANSFORM_BODY.replace(r#""name": "daily""#, r#""name": "daily", "schedule": "* * * * *""#);
    let (status, _) = send(app(cp.clone()), req_json("POST", "/admin/transforms", &token, &scheduled)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // typed body referencing unknown types
    let (status, _) = send(app(cp), req_json("POST", "/admin/transforms", &token, r#"{
        "name": "t", "body": {"kind": "typed", "inputs": ["Nope"], "output": "AlsoNope", "sql": "select 1"}
    }"#)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn run_now_and_adhoc_submit_runs() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    send(app(cp.clone()), req_json("POST", "/admin/transforms", &token, TRANSFORM_BODY)).await;

    let (status, body) = send(app(cp.clone()), req_empty_post("/admin/transforms/daily/run", &token)).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let rid = v["run_id"].as_str().unwrap().to_string();

    // run visible: by id, in the transform's history, newest first
    let (status, body) = send(app(cp.clone()), req_empty("GET", &format!("/admin/runs/{rid}"), &token)).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["state"], "queued");
    assert_eq!(v["trigger"], "manual");
    assert_eq!(v["transform"], "daily");
    let (_, body) = send(app(cp.clone()), req_empty("GET", "/admin/transforms/daily/runs", &token)).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["runs"][0]["run_id"], rid.as_str());

    // the queue job exists and carries the run id
    let job = cp.queue().dequeue(&["transform".to_string()], "t").await.unwrap().expect("job");
    assert_eq!(job.payload["run_id"], serde_json::json!(rid));

    // ad-hoc: body only, no definition
    let (status, body) = send(app(cp.clone()), req_json("POST", "/admin/transforms/run", &token, r#"
        {"kind": "physical", "inputs": [{"schema": "main", "name": "a"}],
         "output": {"schema": "main", "name": "b"}, "sql": "select 1"}"#)).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let (_, body) = send(app(cp.clone()), req_empty("GET", &format!("/admin/runs/{}", v["run_id"].as_str().unwrap()), &token)).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["trigger"], "ad-hoc");
    assert!(v["transform"].is_null());

    // 404s + 400s
    let (status, _) = send(app(cp.clone()), req_empty_post("/admin/transforms/nope/run", &token)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(app(cp.clone()), req_empty("GET", "/admin/transforms/nope/runs", &token)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(app(cp.clone()), req_empty("GET", "/admin/runs/not-a-uuid", &token)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = send(app(cp), req_empty("GET", &format!("/admin/runs/{}", uuid::Uuid::new_v4()), &token)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
```

(Add a `req_empty_post` helper mirroring `req_empty` with method POST, or reuse `req_empty("POST", …)` if it already takes a method. Add `//third-party:uuid` to the test target deps. If `seed_types` seeds different type names than the typed-400 test assumes, keep the typed test's names unknown ones — the point is the 400. Also add one non-admin 403 spot-check: a plain-user token against `GET /admin/transforms` → 403, mirroring the existing pattern.)

- [ ] **Step 4: query-api drift** — add the same 8 `(method, path)` pairs to `expected()` in `src/services/query-api/tests/openapi.rs`.

- [ ] **Step 5: Run**

Run: `buck2 test //src/services/runtime:... > /tmp/t10.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t10.log` (the runtime test targets: fragments + admin-management + any others in its BUCK) → PASS.
Run: `buck2 test //src/services/query-api:openapi --unstable-allow-all-tests-on-re > /tmp/t10b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t10b.log` (actual target name from BUCK) → PASS.

- [ ] **Step 6: prek + commit** (`feat(api): transform management routes — define/run/history under /admin`)

---

### Task 11: Docs, register close, full sweep

**Files:**
- Modify: `docs/system-capabilities/transform.md` (defs/runs/HTTP capability paragraphs; trim the "programmatic transforms / no HTTP surface" gap line)
- Modify: `docs/system-capabilities/control-plane.md` (sixth concern)
- Modify: `docs/ROADMAP.md` (remove the `road-transform-defs-runs` entry; in the slice-2 and slice-3 items, replace `[[road-transform-defs-runs]]-blocked` with prose: "`#road-transform-defs-runs` landed (PR #NN) — unblocked.")
- Modify: `docs/FUTURE.md` ONLY if a review deferral was minted during implementation.

**Steps:**

- [ ] **Step 1:** Make the docs edits above. `bash tools/docs.sh validate` → OK.
- [ ] **Step 2:** Full-tree gate:

```bash
buck2 build -M none //src/... > /tmp/t11.log 2>&1; tail -3 /tmp/t11.log
buck2 test //src/... --unstable-allow-all-tests-on-re > /tmp/t11b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t11b.log
```

Expected: build SUCCEEDED; all tests pass (fixture flakes: re-run the individual target once before investigating).

- [ ] **Step 3:** prek + commit (`docs: close road-transform-defs-runs — transform defs/runs capability`). The `#NN` PR number is patched into branch-owned files only AFTER the PR exists (targeted `sed` on the docs files this branch touches, never a blanket grep).

---

## Final review gate (per loom-work-checkout)

After all tasks: dispatch the final code-review subagent AND run the metric gates —
`loom-complexity diff` + `loom-duplication diff` (changed files only, print-only). Any NEW hotspot (cc > 15, cog > 15, MI < 20, SLOC > 100) or NEW ≥20-line duplication pair must be fixed or justified in the PR body. Expected flags to pre-justify: the testkit contract fn will exceed SLOC 100 (test scaffolding, matches the concern-contract precedent); the two run-row decoders in postgres if not factored (factor them).
