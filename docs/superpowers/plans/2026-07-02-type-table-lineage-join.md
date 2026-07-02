# Type↔table lineage layer-join Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit a binding lineage edge inside `define_type` so a provenance walk automatically crosses the type↔table seam (`upstream(type)` reaches its backing table and that table's ingest ancestry; `downstream(table)` reaches its typed descendants).

**Architecture:** Reuse an ordinary `LineageEvent` (no new relation, no new `EventType`) whose `inputs = [backing-table ref]` and `outputs = [type ref]`, carrying a marker payload `{"loom.kind": "type-table-binding"}`. A pure `core` constructor builds the identical event for both adapters. Each adapter's `define_type` emits it **atomically with the type upsert**, guarded by a source-read so an unchanged re-`define_type` emits nothing. The transitive-closure reads are **unchanged** — the edge is just another `event_dataset` row pair.

**Tech Stack:** Rust 2024, buck2, `async-trait`, sqlx (postgres, compile-time `query!` with a committed `.sqlx` offline cache), `parking_lot::Mutex` (memory fake), the `control_plane_testkit` contract library.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)] mod tests` / `#[test]` in `src/**.rs`. The `no-inline-tests` prek hook fails otherwise. Put every test in a `tests/<name>.rs` file wired as a `rust_test` (or `loom_fixture_test`) target.
- **New fixture (postgres) tests must use `loom_fixture_test`**, not a bare `rust_test`. Existing postgres test targets already do — reuse them; do not add a bare `rust_test` that boots postgres.
- **No `core` trait signature changes.** `Ontology`/`Lineage` trait method signatures are untouched. The work is a new free function in `core` plus adapter-internal `define_type` changes plus a testkit contract.
- **Reuse the EXACT SQL string** `select table_schema, table_name from ontology.object_type where name = $1` for the postgres source-guard pre-read. That byte-identical string already has a committed `.sqlx` cache entry (`query-15ad3006…json`, shared with `resolve`). Reusing it means **no `.sqlx` regeneration** — which is mandatory here because this cloud session runs as **root** and `initdb`/`tools/sqlx-prepare.sh` refuse to run as root. Do NOT introduce any other new `query!`/`query_scalar!` SQL string in the postgres library.
- **Clippy is strict** (pedantic + restriction) on production lib/bin code: no `unwrap`/`expect`/`panic`/`indexing_slicing`/`todo`/`dbg!`; carry source errors. Test code is exempt from the panic-safety lints (the `loom_rust_test`/`loom_fixture_test` wrappers inject the allows; the testkit lib already has a crate-level `#![allow(...)]`).
- **Cloud build/verify discipline:** build with `buck2 build -M none //src/...` (or scope to touched targets) to stay under the ~38 GiB disk cap; `buck2 clean` between heavy phases. Postgres **fixture tests cannot run locally as root** — verify them via CI (BuildBuddy `affected` action on the PR). Memory + core + testkit-via-memory tests DO run locally.
- **Markdown lint:** any `.md` you touch must end with exactly one trailing newline and have no trailing whitespace (`end-of-file-fixer`, `trim trailing whitespace`).
- **Conventional Commits** on every commit message (the `conventional-commit` commit-msg hook enforces it locally).

---

### Task 1: Core — `type_table_binding_event` constructor + marker constant

**Files:**
- Modify: `src/control-plane/core/src/identity.rs` (add the marker const + the constructor)
- Modify: `src/control-plane/core/src/lib.rs:41` (re-export the two new symbols)
- Test: `src/control-plane/core/tests/identity.rs` (add unit tests)
- Modify: `src/control-plane/core/BUCK` (add `//third-party:serde_json` to the `identity` test target's deps)

**Interfaces:**
- Consumes: `crate::catalog::TableRef`, `crate::ontology::{ObjectType, TypeName}`, `crate::lineage::{LineageEvent, EventType, RunId, DatasetRef}`, the existing `DatasetId::from(&TableRef)` / `TypeId::from(&TypeName)` + their `.dataset_ref()`.
- Produces (used by Tasks 2 & 3):
  - `pub const TYPE_TABLE_BINDING_KIND: &str = "type-table-binding";`
  - `pub fn type_table_binding_event(ty: &ObjectType) -> LineageEvent` — `inputs = [DatasetId::from(&ty.table).dataset_ref()]`, `outputs = [TypeId::from(&ty.name).dataset_ref()]`, `event_type = EventType::Complete`, fresh `RunId(Uuid::new_v4())`, `event_time = OffsetDateTime::now_utc()`, `payload = {"loom.kind": TYPE_TABLE_BINDING_KIND}`.

- [ ] **Step 1: Write the failing test** — append to `src/control-plane/core/tests/identity.rs`:

```rust
use control_plane_core::{
    EventType, ObjectType, TYPE_TABLE_BINDING_KIND, type_table_binding_event,
};

fn otype(name: &str, schema: &str, table: &str) -> ObjectType {
    ObjectType {
        name: TypeName(name.into()),
        properties: vec![],
        derived: vec![],
        table: tref(schema, table),
        identity: None,
    }
}

#[test]
fn binding_event_points_table_to_type() {
    let ev = type_table_binding_event(&otype("Customer", "main", "customers"));
    // The table is consumed to constitute the type: table is INPUT, type is OUTPUT.
    assert_eq!(
        ev.inputs,
        vec![DatasetRef {
            namespace: "loom".into(),
            name: "main.customers".into(),
        }],
        "backing table is the input (upstream) node"
    );
    assert_eq!(
        ev.outputs,
        vec![DatasetRef {
            namespace: "loom:type".into(),
            name: "Customer".into(),
        }],
        "the type is the output (downstream) node"
    );
    assert_eq!(ev.event_type, EventType::Complete, "a completed fact");
    assert_eq!(
        ev.payload,
        serde_json::json!({ "loom.kind": TYPE_TABLE_BINDING_KIND }),
        "carries the binding marker"
    );
    assert_eq!(TYPE_TABLE_BINDING_KIND, "type-table-binding");
}

#[test]
fn binding_events_have_distinct_fresh_run_ids() {
    let a = type_table_binding_event(&otype("Customer", "main", "customers"));
    let b = type_table_binding_event(&otype("Customer", "main", "customers"));
    assert_ne!(a.run_id.0, b.run_id.0, "each binding event gets a fresh RunId");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 build //src/control-plane/core:identity 2>&1 | tail -20`
Expected: FAIL — `cannot find function type_table_binding_event` / `TYPE_TABLE_BINDING_KIND` (and the `serde_json` dep is missing).

- [ ] **Step 3: Add the `serde_json` dep to the identity test target** — in `src/control-plane/core/BUCK`, change the `identity` test target's `deps` from `deps = [":core"],` to:

```python
rust_test(
    name = "identity",
    crate = "identity",
    srcs = ["tests/identity.rs"],
    crate_root = "tests/identity.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [
        ":core",
        "//third-party:serde_json",
    ],
)
```

- [ ] **Step 4: Implement the constructor** — in `src/control-plane/core/src/identity.rs`, extend the imports and append the const + function.

At the top, change the imports block to add the lineage + ontology-type symbols:

```rust
use crate::catalog::TableRef;
use crate::lineage::{DatasetRef, EventType, LineageEvent, RunId};
use crate::ontology::{ObjectType, TypeName};
```

At the end of the file (after the `From<&TypeName> for DatasetRef` impl), add:

```rust
/// The `payload."loom.kind"` marker distinguishing a type↔table binding event from
/// any other lineage event. Lets a future filter/GC recognize the seam edge without
/// affecting closure traversal (the closure ignores payload entirely).
pub const TYPE_TABLE_BINDING_KIND: &str = "type-table-binding";

/// Build the lineage *binding edge* for a type and its backing table. The physical
/// rows of the table are consumed to constitute the type, so the table is the **input**
/// (upstream) node and the type is the **output** (downstream) node — the direction that
/// makes `upstream(type)` reach the table and `downstream(table)` reach the type. Both
/// control-plane adapters call this one constructor so their binding events cannot drift
/// (marker payload, direction, `EventType::Complete`). A fresh `RunId` per event is
/// intentional (see the design's *RunId determinism* open question); the source-guard in
/// each adapter's `define_type` — not run-id identity — prevents duplicate edges.
#[must_use]
pub fn type_table_binding_event(ty: &ObjectType) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![DatasetId::from(&ty.table).dataset_ref()],
        outputs: vec![TypeId::from(&ty.name).dataset_ref()],
        payload: serde_json::json!({ "loom.kind": TYPE_TABLE_BINDING_KIND }),
    }
}
```

- [ ] **Step 5: Re-export from the crate root** — in `src/control-plane/core/src/lib.rs`, update the `identity` re-export line (currently line 41):

```rust
pub use identity::{
    DatasetId, LOOM_DATASET_NAMESPACE, LOOM_TYPE_NAMESPACE, TYPE_TABLE_BINDING_KIND, TypeId,
    type_table_binding_event,
};
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `buck2 test //src/control-plane/core:identity 2>&1 | grep -E "Tests finished|FAIL|PASS"`
Expected: PASS (all identity tests, including the two new ones).

- [ ] **Step 7: Lint the changed crate**

Run: `buck2 build '//src/control-plane/core:core[clippy.txt]' 2>&1 | tail -5` then confirm the emitted `clippy.txt` is empty.
Expected: no clippy findings (empty `clippy.txt`).

- [ ] **Step 8: Commit**

```bash
git add src/control-plane/core/src/identity.rs src/control-plane/core/src/lib.rs \
        src/control-plane/core/tests/identity.rs src/control-plane/core/BUCK
git commit -m "feat(control-plane-core): type_table_binding_event constructor + marker"
```

---

### Task 2: Testkit contract + memory adapter emits the binding edge

This task delivers the shared behavioral contract AND the memory implementation together, because a testkit contract is only exercised through an adapter — the memory adapter is where we can run it locally red→green.

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs` (add `type_table_binding_contract`)
- Modify: `src/control-plane/memory/src/ontology.rs:22-28` (`define_type` emits the guarded edge)
- Test: `src/control-plane/memory/tests/lineage.rs` (wire a test that calls the new contract)

**Interfaces:**
- Consumes: the in-scope testkit imports (`ObjectType`, `TableRef`, `TypeName`, `DatasetRef`, `LineageEvent`, `EventType`, `RunId`, `Page`, `PageReq`, `Ontology`, `Lineage`, `HashSet`, `OffsetDateTime`, `uuid`). The contract exercises the binding edge indirectly — it calls `define_type` (which emits via Task 1's constructor) and asserts only on `upstream`/`downstream`; it does NOT call `type_table_binding_event` itself. The memory `define_type` (Step 4) is what consumes Task 1's constructor.
- Produces (used by Task 3): `pub async fn type_table_binding_contract<CP: Ontology + Lineage>(cp: &CP)`.

- [ ] **Step 1: Write the contract (the failing test body)** — append to `src/control-plane/testkit/src/lib.rs`:

```rust
/// Contract: `define_type` emits a type↔table *binding edge* so a provenance walk
/// crosses the type↔table seam. The physical table is upstream of the type. The edge
/// is an ordinary `LineageEvent`, so the closure needs no changes. Run against every
/// adapter implementing `Ontology + Lineage`.
pub async fn type_table_binding_contract<CP: Ontology + Lineage>(cp: &CP) {
    let ds = |ns: &str, n: &str| DatasetRef {
        namespace: ns.to_string(),
        name: n.to_string(),
    };
    let set = |p: Page<DatasetRef>| p.into_iter().collect::<HashSet<_>>();
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let edge = |inp: DatasetRef, out: DatasetRef| LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: ts,
        inputs: vec![inp],
        outputs: vec![out],
        payload: serde_json::json!({}),
    };
    let otype = |name: &str, schema: &str, table: &str| ObjectType {
        name: TypeName(name.into()),
        properties: vec![],
        derived: vec![],
        table: TableRef {
            schema: schema.into(),
            name: table.into(),
        },
        identity: None,
    };

    // define type X bound to main.customers -> emits binding edge {customers -> type/X}
    cp.define_type(otype("Bnd_X", "main", "bnd_customers"))
        .await
        .unwrap();

    let table_ref = ds("loom", "main.bnd_customers");
    let type_x = ds("loom:type", "Bnd_X");
    let type_y = ds("loom:type", "Bnd_Y");
    let s3 = ds("s3://raw", "bnd_src");

    // ingest edge s3 -> loom/main.bnd_customers ; transform edge type/X -> type/Y
    cp.emit(edge(s3.clone(), table_ref.clone())).await.unwrap();
    cp.emit(edge(type_x.clone(), type_y.clone())).await.unwrap();

    // 1. crosses the seam: upstream(Y, depth=3) reaches X, the backing table, and s3.
    let up_y3 = set(cp.upstream(&type_y, 3, PageReq::unbounded()).await.unwrap());
    assert!(up_y3.contains(&type_x), "upstream(Y) reaches X");
    assert!(
        up_y3.contains(&table_ref),
        "upstream(Y) crosses the binding to the backing table"
    );
    assert!(
        up_y3.contains(&s3),
        "upstream(Y) reaches the table's ingest source"
    );
    // downstream(table, depth=2) reaches the type and its typed descendant.
    let down_t2 = set(cp.downstream(&table_ref, 2, PageReq::unbounded()).await.unwrap());
    assert!(down_t2.contains(&type_x), "downstream(table) reaches the type");
    assert!(down_t2.contains(&type_y), "downstream(table) reaches Y");

    // 4. Direction: table is upstream of type; type is downstream of table (one hop).
    let up_x1 = set(cp.upstream(&type_x, 1, PageReq::unbounded()).await.unwrap());
    assert_eq!(
        up_x1,
        [table_ref.clone()].into_iter().collect(),
        "table is the sole one-hop upstream of the type"
    );
    let down_t1 = set(cp.downstream(&table_ref, 1, PageReq::unbounded()).await.unwrap());
    assert!(
        down_t1.contains(&type_x),
        "type is downstream of the table (one hop)"
    );

    // 5. Depth accounting: the seam costs one hop.
    let up_y1 = set(cp.upstream(&type_y, 1, PageReq::unbounded()).await.unwrap());
    assert_eq!(
        up_y1,
        [type_x.clone()].into_iter().collect(),
        "depth=1 reaches only X — the seam is not yet crossed"
    );
    let up_y2 = set(cp.upstream(&type_y, 2, PageReq::unbounded()).await.unwrap());
    assert!(
        up_y2.contains(&table_ref),
        "depth=2 crosses the seam to the backing table"
    );

    // 2. Idempotent re-define: re-defining X->same table adds no spurious upstream;
    //    the type stays single-sourced to exactly its backing table. (Row-level
    //    exactly-once is pinned by the postgres guard-count test in Task 3, since the
    //    read is set-deduplicated and cannot count duplicate identical edges.)
    cp.define_type(otype("Bnd_X", "main", "bnd_customers"))
        .await
        .unwrap();
    let up_x_redef = set(cp.upstream(&type_x, 1, PageReq::unbounded()).await.unwrap());
    assert_eq!(
        up_x_redef,
        [table_ref.clone()].into_iter().collect(),
        "redundant re-define leaves the type single-sourced to its table"
    );

    // 3. Rebind to a NEW table appends a second edge (append-only history); the old
    //    binding is retained.
    cp.define_type(otype("Bnd_X", "main", "bnd_customers_v2"))
        .await
        .unwrap();
    let table_ref_v2 = ds("loom", "main.bnd_customers_v2");
    let up_x_rebind = set(cp.upstream(&type_x, 1, PageReq::unbounded()).await.unwrap());
    assert!(
        up_x_rebind.contains(&table_ref),
        "rebind retains the original binding (append-only)"
    );
    assert!(
        up_x_rebind.contains(&table_ref_v2),
        "rebind adds the new backing table as an upstream"
    );
}
```

- [ ] **Step 2: Wire the memory test** — append to `src/control-plane/memory/tests/lineage.rs`:

```rust
#[tokio::test]
async fn memory_passes_type_table_binding_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::type_table_binding_contract(&cp).await;
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `buck2 test //src/control-plane/memory:lineage 2>&1 > /tmp/t2.log; grep -E "Tests finished|FAIL|panicked|assertion" /tmp/t2.log | head`
Expected: FAIL — the new test panics on the first `upstream(Y)` seam assertion (`memory` `define_type` does not emit a binding edge yet).

- [ ] **Step 4: Implement the guarded emit in memory `define_type`** — replace the body of `define_type` in `src/control-plane/memory/src/ontology.rs` (lines 21-28) with:

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_type(&self, ty: ObjectType) -> Result<()> {
        // Reject malformed constraint declarations at define time — same gate as the
        // postgres adapter, so both reject identically (the testkit contract pins this).
        control_plane_core::validate_constraints(&ty.properties)?;
        // Emit the type↔table binding edge iff the type is new or its backing table
        // changed (source guard: an unchanged re-define emits nothing). Lock order is
        // ontology-then-lineage; no other memory path holds both, so nesting is safe.
        let mut ont = self.ontology.lock();
        let changed = ont.types.get(&ty.name.0).is_none_or(|prev| prev.table != ty.table);
        let event = changed.then(|| control_plane_core::type_table_binding_event(&ty));
        ont.types.insert(ty.name.0.clone(), ty);
        if let Some(event) = event {
            self.lineage.lock().events.push(event);
        }
        Ok(())
    }
```

Note: `is_none_or` is stable on this toolchain (already used in `lib.rs:57` `is_none_or`). The `event` is built from `&ty` **before** the `insert` moves `ty`.

- [ ] **Step 5: No import change** — the `validate_constraints` call was already present in the original `define_type` (the replacement in Step 4 preserves it) and `type_table_binding_event` is referenced via the fully-qualified `control_plane_core::` path, so the existing `use control_plane_core::{...}` list at the top of `src/control-plane/memory/src/ontology.rs` is left **unchanged**. No new `use` is needed.

- [ ] **Step 6: Run the memory test to verify it passes**

Run: `buck2 test //src/control-plane/memory:lineage 2>&1 > /tmp/t2.log; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS (all memory lineage tests, including the new binding contract).

- [ ] **Step 7: Run the full memory + testkit-touching suite to catch additive regressions**

Run: `buck2 test //src/control-plane/memory/... 2>&1 > /tmp/t2b.log; grep -E "Tests finished|FAIL" /tmp/t2b.log`
Expected: PASS. In particular `//src/control-plane/memory:ontology`, `:acl`, `:existence-validation` (all call `define_type`) still pass — the binding emit is additive and none assert on lineage.

- [ ] **Step 8: Lint the two changed crates**

Run:
```bash
buck2 build '//src/control-plane/memory:memory[clippy.txt]' '//src/control-plane/testkit:testkit[clippy.txt]' 2>&1 | tail -5
```
Expected: both emitted `clippy.txt` files empty.

- [ ] **Step 9: Commit**

```bash
git add src/control-plane/testkit/src/lib.rs src/control-plane/memory/src/ontology.rs \
        src/control-plane/memory/tests/lineage.rs
git commit -m "feat(control-plane-memory): emit type-table binding edge in define_type"
```

---

### Task 3: Postgres adapter emits the binding edge (atomic, source-guarded) + guard-count test

**Files:**
- Modify: `src/control-plane/postgres/src/ontology.rs:13-90` (`define_type`: source-guard pre-read + conditional in-tx `pg_emit`)
- Test: `src/control-plane/postgres/tests/lineage.rs` (wire the shared contract + add a postgres-only guard-count test)
- Modify: `src/control-plane/postgres/BUCK` (add `//third-party:sqlx` to the `lineage` test target's deps — the guard-count test uses `sqlx::query_scalar` directly)

**Interfaces:**
- Consumes: `crate::lineage::pg_emit` (`pub(crate) async fn pg_emit<'e, E: sqlx::PgExecutor<'e>>(ex: E, event: &LineageEvent) -> Result<()>`); `control_plane_core::{type_table_binding_event, TYPE_TABLE_BINDING_KIND}`; the `PgFixture` API (`start`, `fresh_db() -> (PgControlPlane, String)`, `pool_for(&db) -> PgPool`).
- Produces: no new public API.

- [ ] **Step 1: Wire the shared contract test (postgres)** — append to `src/control-plane/postgres/tests/lineage.rs`:

```rust
#[tokio::test]
async fn postgres_passes_type_table_binding_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::type_table_binding_contract(&cp).await;
}
```

- [ ] **Step 2: Write the postgres-only guard-count test (the row-level exactly-once check)** — also append to `src/control-plane/postgres/tests/lineage.rs`. This is the only place the source guard's *row-level* exactly-once behavior is observable (the closure reads dedupe identical edges). It uses a runtime `query_scalar` with a static string literal (no `.sqlx` cache needed — same runtime pattern as `fixture.rs:614`):

```rust
#[tokio::test]
async fn postgres_binding_edge_is_source_guarded() {
    use control_plane_core::{ObjectType, Ontology, TableRef, TypeName};

    let fixture = PgFixture::start();
    let (cp, db) = fixture.fresh_db().await;
    let pool = fixture.pool_for(&db).await;

    let otype = |schema: &str, table: &str| ObjectType {
        name: TypeName("GuardX".into()),
        properties: vec![],
        derived: vec![],
        table: TableRef {
            schema: schema.into(),
            name: table.into(),
        },
        identity: None,
    };

    // Count binding events whose output node is loom:type/GuardX.
    let count = || async {
        sqlx::query_scalar::<_, i64>(
            "select count(*) from lineage.event e \
             join lineage.event_dataset d on d.event_id = e.event_id \
             where e.payload->>'loom.kind' = 'type-table-binding' \
               and d.direction = 'output' and d.namespace = 'loom:type' and d.name = $1",
        )
        .bind("GuardX")
        .fetch_one(&pool)
        .await
        .unwrap()
    };

    // First define + an identical re-define: exactly ONE binding edge (guard suppresses
    // the redundant one).
    cp.define_type(otype("main", "guard_a")).await.unwrap();
    cp.define_type(otype("main", "guard_a")).await.unwrap();
    assert_eq!(count().await, 1, "redundant re-define emits no second edge");

    // Rebind to a different table: a SECOND edge is appended (append-only history).
    cp.define_type(otype("main", "guard_b")).await.unwrap();
    assert_eq!(count().await, 2, "rebind appends a new edge; the old one is retained");
}
```

- [ ] **Step 3: Add the `core` + `sqlx` deps to the `lineage` test target** — in `src/control-plane/postgres/BUCK`, the `lineage` `loom_fixture_test` (line ~301) currently has:

```python
    deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
```

Change it to add BOTH `//src/control-plane/core:core` (the guard-count test does `use control_plane_core::{ObjectType, Ontology, TableRef, TypeName}` — buck2 requires a crate to be a **direct** dep to `use` it; the existing `lineage` target omits core only because its sources never named it) AND `//third-party:sqlx` (the guard-count test's `sqlx::query_scalar`):

```python
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//src/control-plane/testkit:testkit",
        "//third-party:sqlx",
        "//third-party:tokio",
    ],
```

- [ ] **Step 4: Confirm the postgres tests BUILD (pre-implementation) — runtime is CI only**

These are `loom_fixture_test` targets that boot postgres, which **refuses to run as root**, so they cannot run in this cloud session. Confirm they at least *build*:

Run: `buck2 build //src/control-plane/postgres:lineage 2>&1 | tail -5`
Expected: builds. (The red→green proof happens on CI; the new assertions fail there until the implementation below lands, then pass.)

- [ ] **Step 5: Implement the source-guard pre-read + conditional emit in postgres `define_type`** — in `src/control-plane/postgres/src/ontology.rs`, inside `define_type`, after `let mut tx = self.pool.begin().await.map_err(backend)?;` and BEFORE the `object_type` upsert, add the pre-read:

```rust
        let mut tx = self.pool.begin().await.map_err(backend)?;
        // Source guard: read the type's PRIOR backing table (if any) before the upsert so
        // we emit the binding edge only when the type is new or its table changed. Reuses
        // `resolve`'s exact SQL string, so it shares that query's committed `.sqlx` cache
        // entry — no cache regeneration.
        let prior = sqlx::query!(
            "select table_schema, table_name from ontology.object_type where name = $1",
            ty.name.0,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?;
        let binding_changed = match &prior {
            Some(r) => r.table_schema != ty.table.schema || r.table_name != ty.table.name,
            None => true,
        };
```

Then, after the derived-property loop and BEFORE `tx.commit().await.map_err(backend)?;`, add the conditional emit (in the SAME `tx`, so the edge commits atomically with the type):

```rust
        if binding_changed {
            crate::lineage::pg_emit(&mut *tx, &control_plane_core::type_table_binding_event(&ty))
                .await?;
        }
        tx.commit().await.map_err(backend)?;
        Ok(())
```

Leave the `use control_plane_core::{...}` import list as-is (the two new symbols are referenced by fully-qualified path: `control_plane_core::type_table_binding_event` and `crate::lineage::pg_emit`).

- [ ] **Step 6: Confirm the offline build still works with NO `.sqlx` change**

Run:
```bash
buck2 build //src/control-plane/postgres:postgres 2>&1 | tail -5
git status --porcelain src/control-plane/postgres/.sqlx
```
Expected: the library builds offline; `git status` shows **no** change under `.sqlx/` (the pre-read reused the existing cached query string). If `.sqlx` shows a diff or the build complains about a missing query, the SQL string was not byte-identical to `resolve`'s — fix the string to match exactly.

- [ ] **Step 7: Lint the postgres crate**

Run: `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' 2>&1 | tail -5`
Expected: empty `clippy.txt`.

- [ ] **Step 8: Commit**

```bash
git add src/control-plane/postgres/src/ontology.rs src/control-plane/postgres/tests/lineage.rs \
        src/control-plane/postgres/BUCK
git commit -m "feat(control-plane-postgres): emit atomic type-table binding edge in define_type"
```

---

### Task 4: Update the typed-transform e2e assertion (expected behavior change)

The new binding edge intentionally makes a type's backing table appear in `upstream(type)`. The `typed_transform_e2e` test's step **5b** asserts `upstream(OrderEnriched, 1) == {Customer, Order}` and that every node is `loom:type`-namespaced — both now change because `define_type(OrderEnriched)` emits `{loom/main.order_enriched → loom:type/OrderEnriched}`. Rewrite 5b to validate the NEW correct behavior: the type-layer ancestry is still `{Customer, Order}`, AND the backing table is now a one-hop upstream via the binding edge.

**Files:**
- Modify: `src/services/transform/tests/typed_transform_e2e.rs:286-302`

**Interfaces:**
- Consumes: the binding behavior from Tasks 1–3. No new symbols.

- [ ] **Step 1: Rewrite step 5b** — replace lines 286-302 of `src/services/transform/tests/typed_transform_e2e.rs` (the block from the `// 5b.` comment through the closing of the `"lineage nodes are type-namespaced"` assert) with:

```rust
    // 5b. TYPE-named lineage plus the type↔table binding edge. The typed transform gives
    //     the type-layer ancestry (Customer, Order); `define_type(OrderEnriched)` emits a
    //     binding edge so the backing table is now a one-hop upstream of the type too.
    let out_ds: DatasetRef = (&TypeName("OrderEnriched".into())).into();
    let ups = pg
        .lineage()
        .upstream(&out_ds, 1, PageReq::unbounded())
        .await
        .unwrap();
    let type_up: std::collections::HashSet<String> = ups
        .items
        .iter()
        .filter(|d| d.namespace == "loom:type")
        .map(|d| d.name.clone())
        .collect();
    assert_eq!(
        type_up,
        std::collections::HashSet::from(["Customer".to_string(), "Order".to_string()]),
        "type-named transform ancestry, got {type_up:?}"
    );
    assert!(
        ups.items
            .iter()
            .any(|d| d.namespace == "loom" && d.name == "main.order_enriched"),
        "binding edge: the backing table is upstream of the type, got {:?}",
        ups.items
    );
```

- [ ] **Step 2: Build the test target (cannot run locally as root; CI verifies)**

Run: `buck2 build //src/services/transform:typed_transform_e2e 2>&1 | tail -5`
Expected: builds. (Runtime verification is on CI — this is a postgres fixture e2e.)

- [ ] **Step 3: Commit**

```bash
git add src/services/transform/tests/typed_transform_e2e.rs
git commit -m "test(transform): typed e2e upstream now includes the type-table binding edge"
```

---

### Task 5: Close the register item

**Files:**
- Modify: `docs/ROADMAP.md` (the `road-type-table-lineage-join` entry)

- [ ] **Step 1: Update the register entry via the loom-docs-update skill** — mark `road-type-table-lineage-join` done: flip `- [ ]` → `- [x]`, set `status:done`, and set `pr:#<N>` once the PR number is known. Use the `loom-docs-update` skill (it validates grammar/ids/links). The entry currently reads:

```
- [ ] **Type↔table lineage layer-join** `{#road-type-table-lineage-join area:lineage status:planned from:2026-06-15-typed-transforms-part1-design pr:- spec:2026-07-01-type-table-lineage-join-design}`
```

becomes (fill the PR number after opening the PR):

```
- [x] **Type↔table lineage layer-join** `{#road-type-table-lineage-join area:lineage status:done from:2026-06-15-typed-transforms-part1-design pr:#<N> spec:2026-07-01-type-table-lineage-join-design}`
```

- [ ] **Step 2: Validate the registers**

Run: `bash tools/docs.sh validate 2>&1 | tail -5`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add docs/ROADMAP.md
git commit -m "docs(roadmap): close road-type-table-lineage-join"
```

---

## Testing summary (what proves the feature)

| Layer | Target | Runs where |
|-------|--------|-----------|
| Core constructor (direction, marker, fresh RunId) | `//src/control-plane/core:identity` | local + CI |
| Shared behavior (crosses seam, direction, depth, idempotent re-define read-level, rebind appends) | `type_table_binding_contract` via `//src/control-plane/memory:lineage` | local + CI |
| Shared behavior on postgres | `type_table_binding_contract` via `//src/control-plane/postgres:lineage` | **CI only** (fixture) |
| Source-guard row-level exactly-once + rebind append | `postgres_binding_edge_is_source_guarded` | **CI only** (fixture) |
| No regression in the type-lineage e2e (now validates the binding) | `//src/services/transform:typed_transform_e2e` | **CI only** (fixture) |

## Notes / rationale carried from the spec

- **Why reuse an ordinary `LineageEvent`:** the transitive-closure reads (`WITH RECURSIVE` in postgres, visited-set BFS in memory) walk `event_dataset` co-membership; a binding event is just another row pair, so the closure crosses it with zero CTE/BFS changes. A dedicated relation/`EventType` was rejected (would force a `UNION` in every closure query).
- **Why emit inside `define_type` (not `bind()`):** `ObjectType.table` is always present, so `define_type` always knows the backing table; `bind()` delegates to `define_type`, so the dataset→model promotion path gets the edge with no signature change. And `define_type` already runs its own transaction, giving atomicity for free (postgres) / a two-lock critical section (memory).
- **Append-only rebind:** re-binding a type to a new table appends a new edge without retracting the old — historically accurate provenance. Edge retraction/tombstoning is out of scope.
- **Dangling refs are legal:** `define_type` does not verify the table exists in the catalog (lineage refs are deliberately opaque, exactly like external datasets). No catalog read is added.
