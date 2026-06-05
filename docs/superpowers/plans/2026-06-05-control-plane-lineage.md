# Control-Plane Lineage (Phase 5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add loom's lineage concern — OpenLineage events with a typed envelope + opaque payload, a one-hop upstream/downstream graph, and an audit read — as a `Lineage` trait in `core`, a contract, an in-memory fake, and a Postgres adapter + `lineage` migration. Extend the cross-concern `Tx` seam with `emit` so `emit + enqueue` is one atomic unit.

**Architecture:** Ports-and-adapters, identical to the queue/ontology/acl cycles. `core` defines the runtime-free `Lineage` trait + value types and adds one method (`emit`) to the existing `Tx` trait. `testkit` defines one self-seeding contract that also tests cross-concern atomicity. `control-plane-memory` and `control-plane-postgres` each satisfy it. Lineage stores and serves; one-hop graph via per-event input/output co-membership; store-don't-validate.

**Tech Stack:** Rust (edition 2024), `async-trait`, `uuid`, `time`, `serde_json`, `sqlx` 0.8 (runtime query API, `json` + `uuid` + `time` features), buck2.

**Spec:** `docs/superpowers/specs/2026-06-05-control-plane-lineage-design.md` — read it; this plan implements it exactly. Deferred items are recorded in `docs/FUTURE.md`.

---

## File Structure

- **Create** `src/control-plane/core/src/lineage.rs` — `Lineage` trait + types (`RunId`, `DatasetRef`, `EventType`, `LineageEvent`).
- **Modify** `src/control-plane/core/src/lib.rs` — `mod lineage;` + `pub use`.
- **Modify** `src/control-plane/core/src/transaction.rs` — add `emit` to the `Tx` trait (imports `LineageEvent`).
- **Modify** `src/control-plane/testkit/src/lib.rs` — add `lineage_contract`; **modify** `testkit/Cargo.toml` + `testkit/BUCK` (add `uuid`).
- **Modify** `src/control-plane/memory/src/lib.rs` — `LineageState` + `impl Lineage` + `MemoryTx::emit`.
- **Create** `src/control-plane/memory/tests/lineage.rs`; **modify** `src/control-plane/memory/BUCK` (add `lineage` test target).
- **Create** `src/control-plane/postgres/migrations/0004_lineage.sql`.
- **Modify** `src/control-plane/postgres/src/lib.rs` — `impl Lineage` + `pg_emit` helper + `event_type_*` helpers + `PgTx::emit`.
- **Create** `src/control-plane/postgres/tests/lineage.rs`; **modify** `src/control-plane/postgres/BUCK` (add `lineage` test target).

The branch is created by the executor before Task 1 — do **not** implement on `main`.

**Sequencing note (important):** Task 1 adds `emit` to the `Tx` trait. That leaves the existing `impl Tx for MemoryTx` and `impl Tx for PgTx` incomplete, so the **memory and postgres crates will not build until Tasks 3 and 4** add their `emit` impls. This is expected. Each task verifies only its own crate; the full `//src/...` suite is green again after Task 4.

**Test invocation:** pg tests run local-only — `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:lineage`.

**Formatting (avoids a commit loop):** the prek `rustfmt` hook is **check-only** — it fails the commit with a diff but does not rewrite. Before every commit, format the changed files yourself, then add + commit:
```bash
eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 <changed .rs files>
```
(The dev shell builds the toolchain on first run — slow once, cached after.)

---

### Task 1: `Lineage` trait + types in `core`, and `Tx::emit`

**Files:**
- Create: `src/control-plane/core/src/lineage.rs`
- Modify: `src/control-plane/core/src/lib.rs`
- Modify: `src/control-plane/core/src/transaction.rs`

- [ ] **Step 1: Write `core/src/lineage.rs`** verbatim:

```rust
//! The lineage concern: loom's provenance record. Every snapshot-producing run
//! emits an OpenLineage event with its input and output datasets; the `lineage`
//! schema is loom-owned (this trait reads AND writes it). Lineage stores and
//! serves provenance — it does not enforce anything.
//!
//! A `DatasetRef` is OpenLineage's own `{namespace, name}` identity, deliberately
//! decoupled from [`crate::TableRef`]/[`crate::TypeName`] so the graph can span
//! physical tables, ontology types, and external datasets alike. The graph
//! ([`Lineage::upstream`]/[`Lineage::downstream`]) is one hop, computed from each
//! event's own input/output co-membership.

use async_trait::async_trait;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::error::Result;

/// An OpenLineage run identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RunId(pub Uuid);

/// OpenLineage dataset identity. Decoupled from `TableRef`/`TypeName` so lineage
/// can reference physical tables, ontology types, and external datasets uniformly.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DatasetRef {
    pub namespace: String,
    pub name: String,
}

/// OpenLineage run-lifecycle event type. Stored; opaque to loom's own logic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventType {
    Start,
    Running,
    Complete,
    Abort,
    Fail,
}

/// A lineage event: a typed envelope (the fields loom indexes/queries) plus the
/// full OpenLineage event stored opaquely in `payload`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LineageEvent {
    pub run_id: RunId,
    pub event_type: EventType,
    pub event_time: OffsetDateTime,
    pub inputs: Vec<DatasetRef>,
    pub outputs: Vec<DatasetRef>,
    pub payload: serde_json::Value,
}

#[async_trait]
pub trait Lineage {
    /// Record an event (append-only). Its own transaction.
    async fn emit(&self, event: LineageEvent) -> Result<()>;
    /// All events for a run, in emit order. Empty if the run is unknown.
    async fn events_for(&self, run: &RunId) -> Result<Vec<LineageEvent>>;
    /// One hop: datasets that fed directly into a run that produced `dataset`
    /// (order unspecified). Empty if `dataset` is unknown.
    async fn upstream(&self, dataset: &DatasetRef) -> Result<Vec<DatasetRef>>;
    /// One hop: datasets produced directly by a run that consumed `dataset`
    /// (order unspecified). Empty if `dataset` is unknown.
    async fn downstream(&self, dataset: &DatasetRef) -> Result<Vec<DatasetRef>>;
}
```

- [ ] **Step 2: Wire into `core/src/lib.rs`.** Add `mod lineage;` (after `mod error;`, before `mod ontology;`) and the re-export (after the `error` re-export, before `ontology`):

```rust
pub use lineage::{DatasetRef, EventType, Lineage, LineageEvent, RunId};
```

- [ ] **Step 3: Add `emit` to the `Tx` trait in `core/src/transaction.rs`.**

Change the import line to also bring in `LineageEvent`:
```rust
use crate::lineage::LineageEvent;
use crate::queue::{JobId, NewJob};
```
Add the method to the `Tx` trait (after `enqueue`):
```rust
    /// Emit a lineage event within this unit of work: persisted only if the
    /// transaction commits. Makes "record lineage AND enqueue downstream work"
    /// atomic.
    async fn emit(&mut self, event: LineageEvent) -> Result<()>;
```

- [ ] **Step 4: Build core.**
```bash
env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/core:core
```
Expected: builds clean. (The `memory`/`postgres` crates will NOT build now — they'll gain `Tx::emit` in Tasks 3–4. Do not build them in this task.)

- [ ] **Step 5: Format + commit.**
```bash
eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 src/control-plane/core/src/lineage.rs src/control-plane/core/src/lib.rs src/control-plane/core/src/transaction.rs
git add src/control-plane/core
git commit -m "feat(control-plane): add Lineage trait + types and Tx::emit to core"
```

---

### Task 2: `lineage_contract` in testkit (+ `uuid` dep)

**Files:**
- Modify: `src/control-plane/testkit/Cargo.toml`
- Modify: `src/control-plane/testkit/BUCK`
- Modify: `src/control-plane/testkit/src/lib.rs`

- [ ] **Step 1: Add `uuid` to `testkit/Cargo.toml`** under `[dependencies]` (needed to construct `RunId`s):
```toml
uuid = { version = "1", features = ["v4"] }
```

- [ ] **Step 2: Add `//third-party:uuid` to `testkit/BUCK`** in the `testkit` `rust_library` `deps` (alphabetical; the alias already exists — other crates use it, so no buckify is needed):
```python
    deps = [
        "//third-party:async-trait",
        "//src/control-plane/core:core",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
```

- [ ] **Step 3: Extend the testkit imports.** Add the lineage types, plus `HashSet` for set comparisons, to `testkit/src/lib.rs`. Update the `use control_plane_core::{…}` block to add `DatasetRef, EventType, Lineage, LineageEvent, RunId`, and add at the top of the file (with the other `use`s):
```rust
use std::collections::HashSet;
```

- [ ] **Step 4: Append `lineage_contract`** to the end of `testkit/src/lib.rs`:

```rust
/// Contract for the `Lineage` ops, including the first cross-concern atomic unit
/// (`emit` + `enqueue` in one `Tx`). `cp` must be freshly empty.
pub async fn lineage_contract<CP: ControlPlane + Lineage + Queue>(cp: &CP) {
    let ds = |ns: &str, n: &str| DatasetRef {
        namespace: ns.to_string(),
        name: n.to_string(),
    };
    // A fixed whole-second timestamp so the pg `timestamptz` round-trip is exact.
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let set = |v: Vec<DatasetRef>| v.into_iter().collect::<HashSet<_>>();

    // --- emit -> events_for round-trips the envelope + opaque payload ---
    let run = RunId(uuid::Uuid::new_v4());
    let event = LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: ts,
        inputs: vec![ds("ducklake", "main.a"), ds("ducklake", "main.b")],
        outputs: vec![ds("ducklake", "main.c")],
        payload: serde_json::json!({"eventType": "COMPLETE", "run": {"runId": run.0.to_string()}}),
    };
    cp.emit(event.clone()).await.expect("emit");

    let got = cp.events_for(&run).await.expect("events_for");
    assert_eq!(got, vec![event.clone()], "envelope + payload round-trip intact");
    assert!(
        cp.events_for(&RunId(uuid::Uuid::new_v4()))
            .await
            .unwrap()
            .is_empty(),
        "unknown run -> empty"
    );

    // --- one-hop graph via per-event co-membership ---
    assert_eq!(set(cp.upstream(&ds("ducklake", "main.c")).await.unwrap()), set(vec![ds("ducklake", "main.a"), ds("ducklake", "main.b")]));
    assert_eq!(set(cp.downstream(&ds("ducklake", "main.a")).await.unwrap()), set(vec![ds("ducklake", "main.c")]));
    assert_eq!(set(cp.downstream(&ds("ducklake", "main.b")).await.unwrap()), set(vec![ds("ducklake", "main.c")]));
    assert!(
        cp.downstream(&ds("ducklake", "main.c")).await.unwrap().is_empty(),
        "nothing consumes c -> no downstream"
    );
    assert!(
        cp.upstream(&ds("ducklake", "main.a")).await.unwrap().is_empty(),
        "nothing produces a -> no upstream"
    );
    assert!(
        cp.upstream(&ds("ducklake", "main.missing")).await.unwrap().is_empty(),
        "unknown dataset -> empty"
    );

    // multiple events for one run come back in emit order
    let run2 = RunId(uuid::Uuid::new_v4());
    let start = LineageEvent {
        run_id: run2,
        event_type: EventType::Start,
        event_time: ts,
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({"eventType": "START"}),
    };
    let complete = LineageEvent {
        run_id: run2,
        event_type: EventType::Complete,
        event_time: ts,
        inputs: vec![ds("ducklake", "main.c")],
        outputs: vec![ds("ontology", "Customer")],
        payload: serde_json::json!({"eventType": "COMPLETE"}),
    };
    cp.emit(start.clone()).await.unwrap();
    cp.emit(complete.clone()).await.unwrap();
    assert_eq!(
        cp.events_for(&run2).await.unwrap(),
        vec![start, complete],
        "events returned in emit order"
    );
    // graph spans namespaces (physical -> ontology)
    assert_eq!(set(cp.downstream(&ds("ducklake", "main.c")).await.unwrap()), set(vec![ds("ontology", "Customer")]));

    // --- the headline cross-concern atomicity test: emit + enqueue in one Tx ---
    let kinds = vec!["lineage-test".to_string()];

    // rollback -> neither the event nor the job is visible
    let rolled = RunId(uuid::Uuid::new_v4());
    {
        let mut tx = cp.begin().await.expect("begin");
        tx.emit(LineageEvent {
            run_id: rolled,
            event_type: EventType::Complete,
            event_time: ts,
            inputs: vec![],
            outputs: vec![ds("ducklake", "main.rolled")],
            payload: serde_json::json!({}),
        })
        .await
        .unwrap();
        tx.enqueue(NewJob {
            kind: "lineage-test".into(),
            payload: serde_json::json!({}),
            run_at: None,
            priority: 0,
        })
        .await
        .unwrap();
        tx.rollback().await.expect("rollback");
    }
    assert!(
        cp.events_for(&rolled).await.unwrap().is_empty(),
        "rolled-back emit is not visible"
    );
    assert!(
        cp.dequeue(&kinds, "w").await.unwrap().is_none(),
        "rolled-back enqueue is not visible"
    );

    // commit -> both the event and the job are visible
    let committed = RunId(uuid::Uuid::new_v4());
    {
        let mut tx = cp.begin().await.expect("begin");
        tx.emit(LineageEvent {
            run_id: committed,
            event_type: EventType::Complete,
            event_time: ts,
            inputs: vec![],
            outputs: vec![ds("ducklake", "main.committed")],
            payload: serde_json::json!({}),
        })
        .await
        .unwrap();
        tx.enqueue(NewJob {
            kind: "lineage-test".into(),
            payload: serde_json::json!({}),
            run_at: None,
            priority: 0,
        })
        .await
        .unwrap();
        tx.commit().await.expect("commit");
    }
    assert_eq!(
        cp.events_for(&committed).await.unwrap().len(),
        1,
        "committed emit is visible"
    );
    assert!(
        cp.dequeue(&kinds, "w").await.unwrap().is_some(),
        "committed enqueue is visible"
    );
}
```

- [ ] **Step 5: Format + build.**
```bash
eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 src/control-plane/testkit/src/lib.rs
env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/testkit:testkit
```
Expected: builds clean. If the commit's `reindeer-check` hook later flags `third-party/BUCK` drift (it should not, since the `uuid` alias already exists), run `./tools/buckify.sh`, `git add third-party/BUCK`, and retry.

- [ ] **Step 6: Commit.**
```bash
git add src/control-plane/testkit
git commit -m "feat(control-plane): add lineage_contract to testkit"
```

---

### Task 3: In-memory `Lineage` fake + `MemoryTx::emit`

**Files:**
- Modify: `src/control-plane/memory/src/lib.rs`
- Create: `src/control-plane/memory/tests/lineage.rs`
- Modify: `src/control-plane/memory/BUCK`

- [ ] **Step 1: Failing test wiring.** Create `src/control-plane/memory/tests/lineage.rs`:
```rust
#[tokio::test]
async fn memory_passes_lineage_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::lineage_contract(&cp).await;
}
```
Add the test target to `src/control-plane/memory/BUCK` (after the `acl` target):
```python
rust_test(
    name = "lineage",
    crate = "lineage",
    srcs = ["tests/lineage.rs"],
    crate_root = "tests/lineage.rs",
    edition = "2024",
    deps = [
        ":memory",
        "//src/control-plane/testkit:testkit",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Confirm the crate currently fails to build** (missing `Tx::emit` from Task 1 + no `Lineage` impl):
```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:lineage
```
Expected: build error (`MemoryTx: Tx` incomplete / `MemoryControlPlane: Lineage` not satisfied).

- [ ] **Step 3: Edit `src/control-plane/memory/src/lib.rs`.**

(a) Add lineage types to the `use control_plane_core::{…}` block: `DatasetRef, Lineage, LineageEvent, RunId`.

(b) Add the state struct near `OntologyState`:
```rust
#[derive(Default)]
struct LineageState {
    events: Vec<LineageEvent>,
}
```

(c) Add a field to `MemoryControlPlane` and initialize it in `new` (alongside `acl`):
```rust
    lineage: Arc<Mutex<LineageState>>,
```
```rust
            lineage: Arc::new(Mutex::new(LineageState::default())),
```

(d) Add `impl Lineage` after `impl Acl for MemoryControlPlane`:
```rust
#[async_trait]
impl Lineage for MemoryControlPlane {
    async fn emit(&self, event: LineageEvent) -> Result<()> {
        self.lineage.lock().unwrap().events.push(event);
        Ok(())
    }

    async fn events_for(&self, run: &RunId) -> Result<Vec<LineageEvent>> {
        Ok(self
            .lineage
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|e| e.run_id == *run)
            .cloned()
            .collect())
    }

    async fn upstream(&self, dataset: &DatasetRef) -> Result<Vec<DatasetRef>> {
        let lin = self.lineage.lock().unwrap();
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for e in lin.events.iter().filter(|e| e.outputs.contains(dataset)) {
            for d in &e.inputs {
                if seen.insert(d.clone()) {
                    out.push(d.clone());
                }
            }
        }
        Ok(out)
    }

    async fn downstream(&self, dataset: &DatasetRef) -> Result<Vec<DatasetRef>> {
        let lin = self.lineage.lock().unwrap();
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for e in lin.events.iter().filter(|e| e.inputs.contains(dataset)) {
            for d in &e.outputs {
                if seen.insert(d.clone()) {
                    out.push(d.clone());
                }
            }
        }
        Ok(out)
    }
}
```

(e) Extend `MemoryTx` and its `begin`/`commit`/add `emit`. In `begin`, pass the lineage handle and a staged-events buffer:
```rust
        Ok(Box::new(MemoryTx {
            rows: self.rows.clone(),
            notify: self.notify.clone(),
            lineage: self.lineage.clone(),
            staged: Vec::new(),
            staged_events: Vec::new(),
        }))
```
Add the fields to the struct:
```rust
struct MemoryTx {
    rows: Arc<Mutex<Vec<Row>>>,
    notify: Arc<Notify>,
    lineage: Arc<Mutex<LineageState>>,
    staged: Vec<(Uuid, NewJob)>,
    staged_events: Vec<LineageEvent>,
}
```
In `commit`, apply the staged events (after applying staged jobs, before the notify):
```rust
    async fn commit(self: Box<Self>) -> Result<()> {
        let staged_any = !self.staged.is_empty();
        {
            let mut rows = self.rows.lock().unwrap();
            for (id, job) in self.staged {
                MemoryControlPlane::insert_with_id(&mut rows, id, job);
            }
        }
        if !self.staged_events.is_empty() {
            self.lineage.lock().unwrap().events.extend(self.staged_events);
        }
        if staged_any {
            self.notify.notify_waiters();
        }
        Ok(())
    }
```
Add the `emit` method to `impl Tx for MemoryTx` (after `enqueue`):
```rust
    async fn emit(&mut self, event: LineageEvent) -> Result<()> {
        self.staged_events.push(event);
        Ok(())
    }
```
(`rollback` stays a no-op — staged events are dropped with the `MemoryTx`.)

- [ ] **Step 4: Format, run the contract, clippy.**
```bash
eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 src/control-plane/memory/src/lib.rs src/control-plane/memory/tests/lineage.rs
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:lineage
env -u BUCK_PREFER_REMOTE buck2 build '//src/control-plane/memory:memory[clippy.txt]'
```
Expected: test PASS (1); clippy `[clippy.txt]` empty. Fix the fake (not the contract/core) if anything fails.

- [ ] **Step 5: Commit.**
```bash
git add src/control-plane/memory
git commit -m "feat(control-plane): in-memory Lineage fake + MemoryTx::emit passing the contract"
```

---

### Task 4: Postgres `Lineage` adapter + `PgTx::emit` + `0004_lineage.sql`

**Files:**
- Create: `src/control-plane/postgres/migrations/0004_lineage.sql`
- Modify: `src/control-plane/postgres/src/lib.rs`
- Create: `src/control-plane/postgres/tests/lineage.rs`
- Modify: `src/control-plane/postgres/BUCK`

- [ ] **Step 1: Write the migration** `src/control-plane/postgres/migrations/0004_lineage.sql`:
```sql
create schema if not exists lineage;

create table lineage.event (
    event_id   bigserial   primary key,
    run_id     uuid        not null,
    event_type text        not null, -- 'start'|'running'|'complete'|'abort'|'fail'
    event_time timestamptz not null,
    payload    jsonb       not null
);

create index on lineage.event (run_id);

-- Per-event inputs/outputs; powers the one-hop graph. Ordinal preserves emit order.
create table lineage.event_dataset (
    event_id  bigint not null references lineage.event (event_id) on delete cascade,
    direction text   not null, -- 'input' | 'output'
    ordinal   int    not null,
    namespace text   not null,
    name      text   not null,
    primary key (event_id, direction, ordinal)
);

create index on lineage.event_dataset (direction, namespace, name);
```

- [ ] **Step 2: Failing test wiring.** Create `src/control-plane/postgres/tests/lineage.rs`:
```rust
use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_lineage_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::lineage_contract(&cp).await;
}
```
Add the test target to `src/control-plane/postgres/BUCK` (after the `acl` target):
```python
rust_test(
    name = "lineage",
    crate = "lineage",
    srcs = ["tests/lineage.rs"],
    crate_root = "tests/lineage.rs",
    edition = "2024",
    env = {
        "POSTGRES_BIN_DIR": "$(location :postgres-bin)/bin",
        "POSTGRES_LD_LIBRARY_PATH": "$(location :postgres-bin)/lib:$(location :libxml2)",
        "LOOM_MIGRATIONS_DIR": "$(location :migrations)/migrations",
    },
    deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

- [ ] **Step 3: Confirm it fails to build:**
```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:lineage
```
Expected: build error (`PgTx: Tx` incomplete / `PgControlPlane: Lineage` not satisfied).

- [ ] **Step 4: Edit `src/control-plane/postgres/src/lib.rs`.**

(a) Add lineage types to the `use control_plane_core::{…}` block: `DatasetRef, EventType, Lineage, LineageEvent, RunId`. (`Uuid` is already imported.)

(b) Add the event-type codec helpers near `cardinality_to_str`/`_from_str`:
```rust
fn event_type_to_str(t: EventType) -> &'static str {
    match t {
        EventType::Start => "start",
        EventType::Running => "running",
        EventType::Complete => "complete",
        EventType::Abort => "abort",
        EventType::Fail => "fail",
    }
}

fn event_type_from_str(s: &str) -> EventType {
    match s {
        "running" => EventType::Running,
        "complete" => EventType::Complete,
        "abort" => EventType::Abort,
        "fail" => EventType::Fail,
        _ => EventType::Start,
    }
}
```

(c) Add a free `pg_emit` helper (executor-generic, like `pg_insert`) near `pg_insert`:
```rust
async fn pg_emit<'e, E: sqlx::PgExecutor<'e>>(ex: E, event: &LineageEvent) -> Result<()> {
    // One round-trip: insert the event and all its input/output rows via unnest,
    // returning nothing. Ordinals come from WITH ORDINALITY (1-based; the read
    // path orders by `ordinal`, so the absolute base is irrelevant).
    sqlx::query(
        "with e as ( \
             insert into lineage.event (run_id, event_type, event_time, payload) \
             values ($1, $2, $3, $4) returning event_id) \
         insert into lineage.event_dataset (event_id, direction, ordinal, namespace, name) \
         select e.event_id, d.direction, d.ordinal, d.namespace, d.name \
         from e, ( \
             select 'input' as direction, ord as ordinal, ns as namespace, nm as name \
             from unnest($5::text[], $6::text[]) with ordinality as t(ns, nm, ord) \
             union all \
             select 'output', ord, ns, nm \
             from unnest($7::text[], $8::text[]) with ordinality as t(ns, nm, ord)) d",
    )
    .bind(event.run_id.0)
    .bind(event_type_to_str(event.event_type))
    .bind(event.event_time)
    .bind(&event.payload)
    .bind(event.inputs.iter().map(|d| d.namespace.clone()).collect::<Vec<_>>())
    .bind(event.inputs.iter().map(|d| d.name.clone()).collect::<Vec<_>>())
    .bind(event.outputs.iter().map(|d| d.namespace.clone()).collect::<Vec<_>>())
    .bind(event.outputs.iter().map(|d| d.name.clone()).collect::<Vec<_>>())
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(())
}
```

(d) Add `emit` to `impl Tx for PgTx` (after `enqueue`):
```rust
    async fn emit(&mut self, event: LineageEvent) -> Result<()> {
        pg_emit(&mut *self.tx, &event).await
    }
```

(e) Add `impl Lineage for PgControlPlane` after `impl Ontology for PgControlPlane`:
```rust
#[async_trait]
impl Lineage for PgControlPlane {
    async fn emit(&self, event: LineageEvent) -> Result<()> {
        pg_emit(&self.pool, &event).await
    }

    async fn events_for(&self, run: &RunId) -> Result<Vec<LineageEvent>> {
        let rows = sqlx::query(
            "select event_id, event_type, event_time, payload from lineage.event \
             where run_id = $1 order by event_id",
        )
        .bind(run.0)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let event_id: i64 = r.get("event_id");
            out.push(LineageEvent {
                run_id: *run,
                event_type: event_type_from_str(r.get::<String, _>("event_type").as_str()),
                event_time: r.get("event_time"),
                inputs: self.event_datasets(event_id, "input").await?,
                outputs: self.event_datasets(event_id, "output").await?,
                payload: r.get("payload"),
            });
        }
        Ok(out)
    }

    async fn upstream(&self, dataset: &DatasetRef) -> Result<Vec<DatasetRef>> {
        self.graph_step(dataset, "output", "input").await
    }

    async fn downstream(&self, dataset: &DatasetRef) -> Result<Vec<DatasetRef>> {
        self.graph_step(dataset, "input", "output").await
    }
}
```

(f) Add the two private helpers in an `impl PgControlPlane { … }` block (place it next to the existing private `resolve_table` helper block):
```rust
    /// The datasets of one event in one direction, ordered by ordinal.
    async fn event_datasets(&self, event_id: i64, direction: &str) -> Result<Vec<DatasetRef>> {
        let rows = sqlx::query(
            "select namespace, name from lineage.event_dataset \
             where event_id = $1 and direction = $2 order by ordinal",
        )
        .bind(event_id)
        .bind(direction)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| DatasetRef {
                namespace: r.get("namespace"),
                name: r.get("name"),
            })
            .collect())
    }

    /// One-hop graph: distinct datasets on `to_dir` of any event that has
    /// `dataset` on `from_dir`. `upstream` = (output -> input); `downstream` =
    /// (input -> output).
    async fn graph_step(
        &self,
        dataset: &DatasetRef,
        from_dir: &str,
        to_dir: &str,
    ) -> Result<Vec<DatasetRef>> {
        let rows = sqlx::query(
            "select distinct b.namespace, b.name \
             from lineage.event_dataset a \
             join lineage.event_dataset b on b.event_id = a.event_id and b.direction = $4 \
             where a.direction = $3 and a.namespace = $1 and a.name = $2",
        )
        .bind(&dataset.namespace)
        .bind(&dataset.name)
        .bind(from_dir)
        .bind(to_dir)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| DatasetRef {
                namespace: r.get("namespace"),
                name: r.get("name"),
            })
            .collect())
    }
```

- [ ] **Step 5: Format, run the contract, clippy.**
```bash
eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 src/control-plane/postgres/src/lib.rs src/control-plane/postgres/tests/lineage.rs
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:lineage
env -u BUCK_PREFER_REMOTE buck2 build '//src/control-plane/postgres:postgres[clippy.txt]'
```
Expected: test PASS (1); clippy empty. This exercises the migration, the `unnest … with ordinality` insert, the `jsonb` payload + `timestamptz` + `uuid` round-trip, the one-hop graph self-join, and the cross-concern `Tx` atomicity (emit+enqueue commit/rollback).

Debugging notes if it fails (fix the adapter/migration, never the contract):
- `unnest($5::text[], $6::text[]) with ordinality as t(ns, nm, ord)` — column order is the unnested columns first, then the ordinality column last; the alias `t(ns, nm, ord)` names them in that order.
- empty `inputs`/`outputs` → `unnest` of two empty arrays yields zero rows (correct: a START event contributes no edges).
- `event_time` round-trip: the contract uses a whole-second timestamp, so `timestamptz` precision is not an issue.
- `Vec<String>` ↔ `text[]` and `serde_json::Value` ↔ `jsonb` are supported by the enabled sqlx features.

- [ ] **Step 6: Commit.**
```bash
git add src/control-plane/postgres
git commit -m "feat(control-plane): Postgres Lineage adapter + PgTx::emit + lineage migration"
```

---

## Final Verification

- [ ] **Branch + commits** (per the `verify-branch-after-subagents` lesson):
```bash
git branch --show-current     # expect the feature branch, NOT main
git log --oneline -6          # expect the 4 task commits on the lineage design-spec commit
```

- [ ] **Full suite + tally:**
```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...
```
Expected: all pass, with **two more** passing targets than before this branch (`//src/control-plane/memory:lineage` and `//src/control-plane/postgres:lineage`). If the count is off, a target silently fell out of scope — investigate before finishing.

- [ ] **Lint** (CI's `lint` job):
```bash
buck2 run //tools:prek -- run --all-files
```
Expected: rustfmt, clippy, file checks, and `reindeer-check` all pass.

- [ ] Hand off to **superpowers:finishing-a-development-branch**.

---

## Self-Review Notes (for the implementer)

- **Spec coverage:** `Lineage` trait (`emit`/`events_for`/`upstream`/`downstream`); generic `DatasetRef`; full `EventType`; opaque `jsonb` payload; one-hop per-event co-membership graph; `events_for` audit read-back; `Tx::emit` flat method; the cross-concern atomicity test; store-don't-validate; single-tenant. All across Tasks 1–4.
- **Type consistency:** `RunId(Uuid)` (Copy), `DatasetRef` derives `Hash`+`Eq` (used in `HashSet` dedup in both the fake and the contract), `EventType` Copy. `event_type_to_str`/`_from_str` round-trip the lifecycle; `events_for` reads `event_type` back so `from_str` is needed. The pg `pg_emit` helper is shared by `Lineage::emit` and `PgTx::emit` (mirrors `pg_insert`).
- **Sequencing:** adding `Tx::emit` in Task 1 breaks memory/postgres until Tasks 3/4 — expected; each task builds only its own crate; full suite green after Task 4.
- **No unused imports:** memory imports `DatasetRef`/`Lineage`/`LineageEvent`/`RunId`; pg additionally imports `EventType` (used by the codec helpers).
