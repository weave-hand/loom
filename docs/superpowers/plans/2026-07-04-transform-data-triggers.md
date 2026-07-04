# Transform Data Triggers (slice 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `TransformDef.on_input_commit` goes live — the snapshot-commit seam in the postgres adapter (and the memory fake's `MemoryTx::commit`) matches committed tables against data-triggered defs and enqueues `trigger: DataTrigger` runs in the same commit transaction, with queued-run debounce, define-time DAG (cycle) validation, and runtime self-trigger suppression.

**Architecture:** One shared pure core helper (`TriggerNode` + `validate_no_trigger_cycle`) backs define-time cycle rejection in both adapters. On postgres, one executor-generic hook (`pg_fire_data_triggers`) is called inside every new-data commit transaction: directly by `IcebergTx::commit`, `inline_append`, `write_inline_delta`, `write_steps`, and `overwrite_truncate`; and via a new `CommitExtras.data_trigger_tables` field for the `apply_commit_extras` paths (`land_parquet`, `overwrite_parquet_snapshot` → `do_update_table` and `land_additive`). Data-preserving rewrites (inline flush, compaction) do NOT fire. The memory fake mirrors the hook inside `MemoryTx::commit` under its existing lock discipline.

**Tech Stack:** Rust, buck2, sqlx compile-time queries (postgres), parking_lot (memory), testkit cross-adapter contracts.

**Spec:** `docs/superpowers/specs/2026-07-04-transform-ergonomics-design.md` §Data triggers. Register item: `road-transform-data-triggers`.

## Global Constraints

- **No inline `#[test]`** — tests are sibling `tests/<name>.rs` files wired as `rust_test`/`loom_fixture_test` targets in the crate's BUCK (`tools/check-inline-tests.sh` enforces).
- **Postgres-touching tests use `loom_fixture_test`** (`src/control-plane/postgres/defs.bzl`), never bare `rust_test`.
- **After any SQL change** (new `query!` sites, migration 0033): run `bash tools/sqlx-prepare.sh` and commit the `.sqlx` delta with the code.
- **Clippy pedantic+restriction on prod code**: no `unwrap`/`expect`/indexing in `src/**` lib code (`#[expect(lint, reason = "...")]` where unavoidable); tests are exempt from panic-safety lints via the test macros.
- **Before every commit:** `buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -c Failed /tmp/prek.log` must print `0` (grep exits 1 on 0 matches — do not chain with `&&`). prek's clippy hook BUILDS every first-party target, so the whole tree must compile at every commit.
- **Commits:** conventional style, `--no-verify`, each with trailers:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01KrF8cSY7q1odAy9AW6aAnd`
- **NO `git push` until Task 6 is green** (pre-push hooks run full tests; the branch must land as one reviewed unit).
- **buck2 in cloud sessions:** build with `-M none`; run fixture tests with `--unstable-allow-all-tests-on-re` (the shim injects it); never pipe `buck2 test`/`bxl` through `head`/`tail` — redirect to a file and grep.
- **Semantics fixed by the spec** (copy verbatim into any judgment call):
  - Trigger runs are created **inside the same commit transaction** — no polling.
  - **Debounce:** skip enqueue when the transform already has a `Queued` run (at-most-one-pending). A `Running` run does NOT suppress.
  - **Define time:** when `on_input_commit` is set, build the edge set over data-triggered defs (X → Y where Y reads X's output, resolved to physical tables) and reject a define that creates a cycle → `Validation` → 400.
  - **Run time (defense in depth):** a commit produced by a transform run never triggers the same transform — the seam resolves the committing `run_id` to its transform name and skips self-matches.
  - Trigger fires per **def** (not per matched table); the run freezes the def's body at enqueue; `trigger: DataTrigger`, `state: Queued`.
  - Typed inputs resolve through the ontology **at eval time**; an unresolvable type name contributes no match and no edge.
  - Only **new-data commits** fire: appends, replaces, truncates, inline deltas. Data-preserving rewrites (inline flush, compaction) and pure table creations do not.

---

### Task 1: Core — cycle validation helper, lift the `on_input_commit` rejection, `data_triggered_defs()` trait method

**Files:**
- Modify: `src/control-plane/core/src/transforms.rs`
- Modify: `src/control-plane/core/tests/transforms.rs` (existing `rust_test` target)
- Modify: `src/control-plane/memory/src/transforms.rs`
- Modify: `src/control-plane/postgres/src/transforms.rs`

**Interfaces:**
- Produces (later tasks consume verbatim):
  ```rust
  pub struct TriggerNode {
      pub name: String,
      pub inputs: Vec<TableRef>,
      pub output: Option<TableRef>,
  }
  impl TriggerNode {
      pub fn resolve(
          name: &TransformName,
          body: &TransformBody,
          types: &std::collections::HashMap<String, TableRef>,
      ) -> Self;
  }
  pub fn validate_no_trigger_cycle(nodes: &[TriggerNode]) -> Result<()>;
  // Transforms trait gains:
  async fn data_triggered_defs(&self) -> Result<Vec<TransformDef>>;
  ```
- `validate_transform_def` no longer rejects `on_input_commit` (cycle validation is adapter-side, Task 2 — it needs the existing def set + ontology).

**Steps:**

- [ ] **Step 1: Write failing core tests** — append to `src/control-plane/core/tests/transforms.rs`:

```rust
// -- slice 3: trigger-cycle validation --

fn node(name: &str, inputs: &[(&str, &str)], output: Option<(&str, &str)>) -> TriggerNode {
    TriggerNode {
        name: name.to_string(),
        inputs: inputs.iter().map(|(s, t)| tref(s, t)).collect(),
        output: output.map(|(s, t)| tref(s, t)),
    }
}

#[test]
fn trigger_cycle_accepts_a_dag() {
    // a -> b -> c is acyclic; unrelated d has no edges.
    let nodes = vec![
        node("a", &[("main", "t0")], Some(("main", "t1"))),
        node("b", &[("main", "t1")], Some(("main", "t2"))),
        node("c", &[("main", "t2")], Some(("main", "t3"))),
        node("d", &[("main", "x")], Some(("main", "y"))),
    ];
    validate_no_trigger_cycle(&nodes).unwrap();
}

#[test]
fn trigger_cycle_rejects_a_two_cycle() {
    let nodes = vec![
        node("a", &[("main", "t1")], Some(("main", "t2"))),
        node("b", &[("main", "t2")], Some(("main", "t1"))),
    ];
    let err = validate_no_trigger_cycle(&nodes).unwrap_err();
    assert!(matches!(err, ControlPlaneError::Validation(_)));
    let msg = err.to_string();
    assert!(msg.contains('a') && msg.contains('b'), "cycle members named: {msg}");
}

#[test]
fn trigger_cycle_rejects_a_self_loop() {
    let nodes = vec![node("a", &[("main", "t1")], Some(("main", "t1")))];
    assert!(validate_no_trigger_cycle(&nodes).is_err());
}

#[test]
fn trigger_cycle_unresolvable_output_forms_no_edge() {
    // "a" would close the loop but its output no longer resolves.
    let nodes = vec![
        node("a", &[("main", "t2")], None),
        node("b", &[("main", "t1")], Some(("main", "t2"))),
    ];
    validate_no_trigger_cycle(&nodes).unwrap();
}

#[test]
fn trigger_node_resolves_typed_and_physical_bodies() {
    let mut types = std::collections::HashMap::new();
    types.insert("Widget".to_string(), tref("main", "widgets"));
    // Typed: known input resolves, unknown input drops, unknown output -> None.
    let typed = TransformBody::Typed {
        inputs: vec!["Widget".into(), "Ghost".into()],
        output: "Phantom".into(),
        sql: "select 1".into(),
        output_mode: OutputMode::Append,
    };
    let n = TriggerNode::resolve(&TransformName("t".into()), &typed, &types);
    assert_eq!(n.inputs, vec![tref("main", "widgets")]);
    assert_eq!(n.output, None);
    // Physical passes through untouched.
    let phys = TransformBody::Physical {
        inputs: vec![tref("main", "src")],
        output: tref("main", "dst"),
        sql: "select 1".into(),
        output_mode: OutputMode::Append,
    };
    let n = TriggerNode::resolve(&TransformName("p".into()), &phys, &types);
    assert_eq!(n.inputs, vec![tref("main", "src")]);
    assert_eq!(n.output, Some(tref("main", "dst")));
}

#[test]
fn validate_accepts_a_data_triggered_def() {
    let def = TransformDef {
        name: TransformName("dt".into()),
        body: TransformBody::Physical {
            inputs: vec![tref("main", "src")],
            output: tref("main", "dst"),
            sql: "select 1".into(),
            output_mode: OutputMode::Append,
        },
        schedule: None,
        on_input_commit: true,
    };
    validate_transform_def(&def).unwrap();
}
```

Reuse the file's existing `tref` helper and imports (extend the `use` list with `TriggerNode`, `validate_no_trigger_cycle` as needed). If the existing file asserts the old rejection (a test that `on_input_commit: true` fails validation), DELETE that assertion — its behavior is the thing this slice changes.

- [ ] **Step 2: Run to verify failure**

```bash
buck2 test //src/control-plane/core:transforms > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t1.log
```
Expected: compile FAIL (`TriggerNode` not found). (If the core transforms test target has a different name, find it: `grep -n 'tests/transforms' src/control-plane/core/BUCK`.)

- [ ] **Step 3: Implement in `src/control-plane/core/src/transforms.rs`**

Remove the `on_input_commit` rejection block from `validate_transform_def` (lines 268–272) and update its doc comment plus the module doc (lines 10–13) — `on_input_commit` is live; adapter-side define-time validation rejects trigger cycles. Then add:

```rust
/// A data-triggered def's physical io, resolved for cycle validation and
/// commit-seam matching.
#[derive(Clone, Debug, PartialEq)]
pub struct TriggerNode {
    pub name: String,
    pub inputs: Vec<TableRef>,
    /// `None` when the def's output does not resolve (e.g. a deleted
    /// ontology type): an unresolvable output can never match a commit, so
    /// it contributes no edge.
    pub output: Option<TableRef>,
}

impl TriggerNode {
    /// Resolve `body` to physical io given `types` (ontology type name →
    /// backing table). Typed names absent from `types` contribute nothing:
    /// a vanished binding cannot match a commit, so it forms no edge.
    #[must_use]
    pub fn resolve(
        name: &TransformName,
        body: &TransformBody,
        types: &std::collections::HashMap<String, TableRef>,
    ) -> Self {
        let (inputs, output) = match body {
            TransformBody::Physical { inputs, output, .. } => {
                (inputs.clone(), Some(output.clone()))
            }
            TransformBody::Typed { inputs, output, .. } => (
                inputs.iter().filter_map(|t| types.get(t).cloned()).collect(),
                types.get(output).cloned(),
            ),
        };
        Self {
            name: name.0.clone(),
            inputs,
            output,
        }
    }
}

/// Reject a firing cycle among data-triggered defs. Edge X → Y iff Y reads
/// X's resolved output (a def reading its own output is a self-cycle).
/// `nodes` is the complete data-triggered set INCLUDING the candidate being
/// defined. Kahn's algorithm: repeatedly remove zero-in-degree nodes; any
/// remainder is cyclic and is named in the error.
pub fn validate_no_trigger_cycle(nodes: &[TriggerNode]) -> Result<()> {
    use std::collections::HashMap;
    let mut indegree: HashMap<&str, usize> =
        nodes.iter().map(|n| (n.name.as_str(), 0)).collect();
    let mut succs: HashMap<&str, Vec<&str>> = HashMap::new();
    for from in nodes {
        let Some(out) = &from.output else { continue };
        for to in nodes.iter().filter(|to| to.inputs.contains(out)) {
            succs
                .entry(from.name.as_str())
                .or_default()
                .push(to.name.as_str());
            if let Some(d) = indegree.get_mut(to.name.as_str()) {
                *d += 1;
            }
        }
    }
    let mut ready: Vec<&str> = indegree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(n, _)| *n)
        .collect();
    let mut removed = 0usize;
    while let Some(n) = ready.pop() {
        removed += 1;
        for s in succs.get(n).map(Vec::as_slice).unwrap_or_default() {
            if let Some(d) = indegree.get_mut(s) {
                *d -= 1;
                if *d == 0 {
                    ready.push(s);
                }
            }
        }
    }
    if removed == nodes.len() {
        return Ok(());
    }
    let mut cyclic: Vec<&str> = indegree
        .iter()
        .filter(|(_, d)| **d > 0)
        .map(|(n, _)| *n)
        .collect();
    cyclic.sort_unstable();
    Err(ControlPlaneError::Validation(format!(
        "data-trigger cycle among transforms: {}",
        cyclic.join(", ")
    )))
}
```

Add to the `Transforms` trait (after `next_run_at`):

```rust
    /// Every definition with `on_input_commit` set, name-ordered — the
    /// commit-seam matcher's candidate set.
    async fn data_triggered_defs(&self) -> Result<Vec<TransformDef>>;
```

Re-export `TriggerNode` / `validate_no_trigger_cycle` from the crate root the same way `validate_transform_def` is (check `src/control-plane/core/src/lib.rs`'s existing `pub use` for the transforms module and extend it).

- [ ] **Step 4: Implement `data_triggered_defs` on both adapters** (the trait addition breaks their builds — same task keeps the tree green).

`src/control-plane/memory/src/transforms.rs` (inside `impl Transforms for MemoryControlPlane`):

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn data_triggered_defs(&self) -> Result<Vec<TransformDef>> {
        let mut defs: Vec<TransformDef> = self
            .transforms
            .lock()
            .defs
            .values()
            .filter(|d| d.on_input_commit)
            .cloned()
            .collect();
        defs.sort_by(|a, b| a.name.0.cmp(&b.name.0));
        Ok(defs)
    }
```

`src/control-plane/postgres/src/transforms.rs` (inside the `impl Transforms`; mirror the existing `list_transforms` decode style — it selects the same columns and builds `TransformDef` via `de_body`):

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn data_triggered_defs(&self) -> Result<Vec<TransformDef>> {
        let rows = sqlx::query!(
            "select name, body, schedule, on_input_commit from transforms.transform \
             where on_input_commit order by name",
        )
        .fetch_all(self.pool())
        .await
        .map_err(backend)?;
        rows.into_iter()
            .map(|r| {
                Ok(TransformDef {
                    name: TransformName(r.name),
                    body: de_body(r.body)?,
                    schedule: r.schedule,
                    on_input_commit: r.on_input_commit,
                })
            })
            .collect()
    }
```

- [ ] **Step 5: sqlx cache, tests green, whole-tree build**

```bash
bash tools/sqlx-prepare.sh
buck2 test //src/control-plane/core:transforms > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log
buck2 build -M none //src/... > /tmp/b1.log 2>&1; tail -3 /tmp/b1.log
```
Expected: core tests PASS; build green.

- [ ] **Step 6: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -c Failed /tmp/prek.log
git add -A
git commit --no-verify -m "feat(transform): trigger-cycle validation helper + data_triggered_defs; on_input_commit accepted"
```
(with the standard trailers)

---

### Task 2: Define-time DAG validation in both adapters + testkit contract legs

**Files:**
- Modify: `src/control-plane/memory/src/transforms.rs` (`define_transform`)
- Modify: `src/control-plane/postgres/src/transforms.rs` (`define_transform`)
- Modify: `src/control-plane/testkit/src/lib.rs` (`transforms_contract` gains legs)
- Modify: `src/control-plane/postgres/.sqlx/` (regenerated)

**Interfaces:**
- Consumes: `TriggerNode::resolve`, `validate_no_trigger_cycle` (Task 1).
- Produces: `define_transform` rejects cycle-creating data-triggered defs with `ControlPlaneError::Validation` on BOTH adapters; postgres serializes concurrent defines with `pg_advisory_xact_lock`.

**Steps:**

- [ ] **Step 1: Write failing contract legs** in `transforms_contract` (`src/control-plane/testkit/src/lib.rs`, after the existing typed-validation leg). The contract already binds `CP: ControlPlane + Transforms + Ontology` and has `tref` + `seed_type` helpers, and has seeded types `Widget`→(main, widgets) and `Gadget`→(main, gadgets):

```rust
    // -- data triggers: define-time cycle validation (slice 3) --

    let phys = |name: &str, input: (&str, &str), output: (&str, &str)| TransformDef {
        name: TransformName(name.to_string()),
        body: TransformBody::Physical {
            inputs: vec![tref(input.0, input.1)],
            output: tref(output.0, output.1),
            sql: "select 1".to_string(),
            output_mode: OutputMode::Append,
        },
        schedule: None,
        on_input_commit: true,
    };

    // A data-triggered def is accepted and round-trips its flag.
    cp.define_transform(phys("dt-a", ("main", "t1"), ("main", "t2")))
        .await
        .unwrap();
    assert!(cp.get_transform(&TransformName("dt-a".into())).await.unwrap().on_input_commit);

    // A self-loop (reads its own output) is rejected.
    let err = cp
        .define_transform(phys("dt-self", ("main", "s1"), ("main", "s1")))
        .await
        .unwrap_err();
    assert!(matches!(err, ControlPlaneError::Validation(_)), "{err}");

    // Closing a two-cycle against dt-a is rejected...
    let err = cp
        .define_transform(phys("dt-b", ("main", "t2"), ("main", "t1")))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("cycle"), "{err}");
    // ...and the rejected def was not stored.
    assert!(matches!(
        cp.get_transform(&TransformName("dt-b".into())).await,
        Err(ControlPlaneError::NotFound(_))
    ));

    // Redefining dt-a does not collide with its own previous edges.
    cp.define_transform(phys("dt-a", ("main", "t1"), ("main", "t2")))
        .await
        .unwrap();

    // A typed cycle resolves through the ontology: Widget->(main,widgets),
    // Gadget->(main,gadgets). dt-t1 reads Widget writes Gadget; dt-t2
    // (reads Gadget writes Widget) closes the loop -> rejected.
    let typed = |name: &str, input: &str, output: &str| TransformDef {
        name: TransformName(name.to_string()),
        body: TransformBody::Typed {
            inputs: vec![input.to_string()],
            output: output.to_string(),
            sql: "select 1".to_string(),
            output_mode: OutputMode::Append,
        },
        schedule: None,
        on_input_commit: true,
    };
    cp.define_transform(typed("dt-t1", "Widget", "Gadget")).await.unwrap();
    let err = cp.define_transform(typed("dt-t2", "Gadget", "Widget")).await.unwrap_err();
    assert!(err.to_string().contains("cycle"), "{err}");

    // data_triggered_defs returns exactly the flagged defs, name-ordered.
    let dt = cp.data_triggered_defs().await.unwrap();
    let names: Vec<&str> = dt.iter().map(|d| d.name.0.as_str()).collect();
    assert_eq!(names, vec!["dt-a", "dt-t1"]);

    // Cleanup so later legs' def counts are unaffected.
    cp.delete_transform(&TransformName("dt-a".into())).await.unwrap();
    cp.delete_transform(&TransformName("dt-t1".into())).await.unwrap();
```

Place this block BEFORE any leg that asserts an exact `list_transforms` count, or adjust such assertions; read the surrounding contract first and reuse its local helper style (it may already have a `def` helper — adapt rather than duplicate).

- [ ] **Step 2: Run to verify failure** (memory first — fast):

```bash
buck2 test //src/control-plane/memory:transforms > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|panicked" /tmp/t2.log
```
Expected: FAIL — self-loop/two-cycle defines are accepted (no validation yet).

- [ ] **Step 3: Memory implementation** — restructure `define_transform` in `src/control-plane/memory/src/transforms.rs` so cycle validation runs under the `transforms` lock (atomic with the insert), with the ontology binding snapshot taken BEFORE that lock (the ontology mutex is never held together with the transforms mutex — preserve that):

```rust
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
        // Ontology snapshot for trigger-cycle resolution, cloned OUTSIDE the
        // transforms lock: the ontology mutex is never held together with
        // rows/lineage/catalog/transforms anywhere — keep it that way.
        let type_tables: std::collections::HashMap<String, control_plane_core::TableRef> = {
            self.ontology
                .lock()
                .types
                .iter()
                .map(|(n, t)| (n.clone(), t.table.clone()))
                .collect()
        };
        // Pure (no lock held) — computed before taking `transforms` below.
        let next = def
            .schedule
            .as_deref()
            .map(|e| next_cron_occurrence(e, OffsetDateTime::now_utc()))
            .transpose()?;
        let mut st = self.transforms.lock();
        if def.on_input_commit {
            // Edge set over data-triggered defs (the candidate replaces any
            // same-name predecessor), validated atomically with the insert.
            let mut nodes: Vec<TriggerNode> = st
                .defs
                .values()
                .filter(|d| d.on_input_commit && d.name.0 != def.name.0)
                .map(|d| TriggerNode::resolve(&d.name, &d.body, &type_tables))
                .collect();
            nodes.push(TriggerNode::resolve(&def.name, &def.body, &type_tables));
            validate_no_trigger_cycle(&nodes)?;
        }
        match next {
            Some(n) => {
                st.next_run_at.insert(def.name.0.clone(), n);
            }
            None => {
                st.next_run_at.remove(&def.name.0);
            }
        }
        st.defs.insert(def.name.0.clone(), def);
        Ok(())
    }
```

Add `TriggerNode`, `validate_no_trigger_cycle` (and `TableRef` if the plain path is awkward) to the file's `use control_plane_core::{...}` list.

- [ ] **Step 4: Memory tests pass**

```bash
buck2 test //src/control-plane/memory:transforms > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log
```

- [ ] **Step 5: Postgres implementation** — restructure `define_transform` in `src/control-plane/postgres/src/transforms.rs`: move the existing typed-existence check and upsert into ONE transaction, guarded by an advisory lock so two racing defines cannot jointly create a cycle:

```rust
/// Serializes trigger-DAG validation against concurrent defines: two racing
/// `define_transform`s could each see the other absent and jointly commit a
/// cycle. Arbitrary constant, unique within loom's advisory-lock usage.
const TRANSFORM_DEFINE_LOCK: i64 = 0x6c6f_6f6d_7472; // "loomtr"
```

```rust
    async fn define_transform(&self, def: TransformDef) -> Result<()> {
        validate_transform_def(&def)?;
        let body = ser(&def.body)?;
        let next_run_at = def
            .schedule
            .as_deref()
            .map(|e| next_cron_occurrence(e, OffsetDateTime::now_utc()))
            .transpose()?;
        let mut tx = self.pool().begin().await.map_err(backend)?;
        sqlx::query!("select pg_advisory_xact_lock($1)", TRANSFORM_DEFINE_LOCK)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        // (existing typed-existence check moves here verbatim, executing on
        //  `&mut *tx` instead of `self.pool()` — same SQL, same error)
        if def.on_input_commit {
            let existing = sqlx::query!(
                "select name, body from transforms.transform \
                 where on_input_commit and name <> $1",
                def.name.0,
            )
            .fetch_all(&mut *tx)
            .await
            .map_err(backend)?;
            let mut bodies: Vec<(TransformName, TransformBody)> = existing
                .into_iter()
                .map(|r| Ok((TransformName(r.name), de_body(r.body)?)))
                .collect::<Result<_>>()?;
            bodies.push((def.name.clone(), def.body.clone()));
            let types = pg_type_tables(&mut *tx, &bodies).await?;
            let nodes: Vec<TriggerNode> = bodies
                .iter()
                .map(|(n, b)| TriggerNode::resolve(n, b, &types))
                .collect();
            validate_no_trigger_cycle(&nodes)?;
        }
        // (existing upsert moves here verbatim, executing on `&mut *tx`)
        tx.commit().await.map_err(backend)?;
        Ok(())
    }
```

with the shared resolution helper (also used by Task 3's hook):

```rust
/// Resolve every typed name appearing in `bodies` to its backing table, in
/// one query. Missing names are simply absent from the map (an unresolvable
/// type cannot match a commit and forms no edge).
async fn pg_type_tables<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    bodies: &[(TransformName, TransformBody)],
) -> Result<std::collections::HashMap<String, TableRef>> {
    let mut names: Vec<String> = bodies
        .iter()
        .flat_map(|(_, b)| match b {
            TransformBody::Typed { inputs, output, .. } => inputs
                .iter()
                .chain(std::iter::once(output))
                .cloned()
                .collect::<Vec<_>>(),
            TransformBody::Physical { .. } => Vec::new(),
        })
        .collect();
    names.sort_unstable();
    names.dedup();
    if names.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let rows = sqlx::query!(
        "select name, table_schema, table_name from ontology.object_type \
         where name = any($1)",
        &names,
    )
    .fetch_all(ex)
    .await
    .map_err(backend)?;
    Ok(rows
        .into_iter()
        .map(|r| {
            (
                r.name,
                TableRef {
                    schema: r.table_schema,
                    name: r.table_name,
                },
            )
        })
        .collect())
}
```

Notes for the implementer: `TableRef` and the new core names need importing; the existing typed-existence check and upsert move INTO the tx unchanged (executor swap only — the SQL strings must stay byte-identical so their `.sqlx` entries are reused). If `sqlx::query!` chokes on `pg_advisory_xact_lock`'s `void` return during `sqlx-prepare`, use `sqlx::query!("select pg_advisory_xact_lock($1) as \"lock: ()\"", ...)` — but try the plain form first.

- [ ] **Step 6: sqlx cache + postgres tests pass**

```bash
bash tools/sqlx-prepare.sh
buck2 test //src/control-plane/postgres:transforms //src/control-plane/postgres:sqlx-cache-check --unstable-allow-all-tests-on-re > /tmp/t2p.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2p.log
```

- [ ] **Step 7: Whole-tree build, prek, commit**

```bash
buck2 build -M none //src/... > /tmp/b2.log 2>&1; tail -3 /tmp/b2.log
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -c Failed /tmp/prek.log
git add -A
git commit --no-verify -m "feat(transform): define-time trigger-DAG validation on both adapters"
```

---

### Task 3: Postgres commit-seam hook — `pg_fire_data_triggers`, migration 0033, wiring into every new-data commit path

**Files:**
- Create: `src/control-plane/postgres/migrations/0033_transform_data_triggers.sql`
- Modify: `src/control-plane/postgres/src/transforms.rs` (hook + `pg_insert_run` extraction)
- Modify: `src/control-plane/postgres/src/iceberg_control_plane.rs` (`IcebergTx::commit`)
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs` (`CommitExtras` + `apply_commit_extras`)
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (`land_parquet`, `overwrite_parquet_snapshot`, `overwrite_truncate`, `write_steps`)
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (`inline_append`, `write_inline_delta`)
- Create: `src/control-plane/postgres/tests/data_triggers.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test` target)

**Interfaces:**
- Consumes: `TriggerNode::resolve`, `pg_type_tables` (Task 2), `crate::queue::pg_insert` (`<'e, E: sqlx::PgExecutor<'e>>(ex, &NewJob) -> Result<JobId>`), `de_body`/`ser`, `TransformBody::to_job(run_id)`.
- Produces:
  ```rust
  pub(crate) async fn pg_fire_data_triggers(
      conn: &mut sqlx::PgConnection,
      committed: &[TableRef],
      committing_run: Option<Uuid>,
  ) -> Result<usize>;
  pub(crate) async fn pg_insert_run<'e, E: sqlx::PgExecutor<'e>>(ex: E, run: &TransformRun) -> Result<()>;
  // CommitExtras gains:
  pub data_trigger_tables: &'a [TableRef],
  ```

**Steps:**

- [ ] **Step 1: Migration** — `src/control-plane/postgres/migrations/0033_transform_data_triggers.sql`:

```sql
-- Debounce probe for data triggers: "does this transform already have a
-- queued run" is checked once per matched def per commit.
create index run_queued_by_transform on transforms.run (transform)
    where state = 'queued';
```

- [ ] **Step 2: Write the failing fixture test** — `src/control-plane/postgres/tests/data_triggers.rs`. Mirror the fixture/catalog setup used by `tests/transforms.rs` (`iceberg_cp`) and the landing tests (`tests/iceberg_landing.rs` shows how `land` is driven — copy its batch/schema helper style rather than inventing new plumbing). Legs, each on a fresh db (`fx.fresh_db()`/`fresh_control_plane` per the file you mirror):

```rust
//! Data triggers fire inside the landing/commit transactions (slice 3).
//! Each leg seeds a physical data-triggered def whose input is the table
//! being written, performs one commit path, and asserts on the run rows.

use control_plane_core::{
    ControlPlane, OutputMode, PageReq, RunState, RunTrigger, TableRef, TransformBody,
    TransformDef, TransformName, Transforms,
};

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef { schema: schema.into(), name: name.into() }
}

fn dt_def(name: &str, input: &TableRef, output: (&str, &str)) -> TransformDef {
    TransformDef {
        name: TransformName(name.into()),
        body: TransformBody::Physical {
            inputs: vec![input.clone()],
            output: tref(output.0, output.1),
            sql: "select 1".into(),
            output_mode: OutputMode::Append,
        },
        schedule: None,
        on_input_commit: true,
    }
}
```

Legs:

1. **`inline_land_fires_a_data_trigger_run`** — define `dt_def("dep", &input, ("main","out"))`; `land()` a small batch into `input` (inline branch); `list_runs(Some(&name), PageReq::default())` → exactly one run: `state == Queued`, `trigger == DataTrigger`, `transform == Some("dep")`, body == the def's body.
2. **`parquet_land_fires_and_debounces`** — land a batch large enough for the Parquet branch (see `InlineLimits` handling in the landing tests; passing `InlineLimits { inline_byte_limit: 0, .. }` forces Parquet) → one queued run; land again → STILL exactly one queued run (debounce). Then `mark_run_running(run_id)` and land a third time → a second run exists (Running does not suppress).
3. **`write_steps_fires_per_matched_def`** — two defs on two different input tables; one `write_steps` call writing both tables → each def has one queued run.
4. **`unmatched_table_does_not_fire`** — define def on `main.other`; land into `main.input` → `list_runs(Some(&other_def))` empty.
5. **`overwrite_and_truncate_fire`** — def on `main.input`; `overwrite_parquet_snapshot` into it → run queued; (delete that run's def or use a second def) `overwrite_truncate` path via a zero-row overwrite → fires.
6. **`icebergtx_fires_downstream_and_marks_committing_run`** — defs A (input `main.src`, output `main.dst`) and B (input `main.dst`, output `main.sink`). Submit a run R for A via `cp.transforms().submit_run(...)`, `mark_run_running(R)`. Then over `begin_table()`: `create_table(main.dst)` + `append_files(main.dst)` + `mark_run_succeeded(R)` + `commit()`. Assert: B fired exactly one queued `DataTrigger` run; `get_run(R)` is `Succeeded`; `list_runs(Some(A))` still contains only R. (The airtight self-skip scenario — the committing def's input rebound onto its own output — needs ontology rebinding and lives in Task 4's testkit contract; this leg pins the `committing_run` plumbing through `IcebergTx`.)
7. **`flush_does_not_refire`** — def on `main.input`; land inline with a low `flush_byte_threshold` and land again so the flush job enqueues/runs (mirror how `tests/iceberg_flush.rs` drives a flush); after the flush commit, still exactly ONE queued run for the def.
8. **`broken_def_body_is_skipped`** — define a valid def, then corrupt its body directly (`sqlx::query("update transforms.transform set body = '\"nonsense\"'::jsonb where name = $1")` via the fixture pool — raw runtime SQL is fine in tests); land into a table matching a SECOND healthy def → healthy def fires, commit succeeds (the poison row was skipped, not fatal).

BUCK target (mirror an existing `loom_fixture_test` entry in `src/control-plane/postgres/BUCK`, e.g. the `transforms` one, adding `:loom-test-seed` dep if the file uses `local_sql_catalog`):

```python
loom_fixture_test(
    name = "data-triggers",
    srcs = ["tests/data_triggers.rs"],
    crate_root = "tests/data_triggers.rs",
    # deps: copy from the transforms/iceberg_landing test targets
)
```

- [ ] **Step 3: Run to verify failure**

```bash
buck2 test //src/control-plane/postgres:data-triggers --unstable-allow-all-tests-on-re > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log
```
Expected: FAIL (no runs appear — hook absent).

- [ ] **Step 4: Implement the hook** in `src/control-plane/postgres/src/transforms.rs`:

Extract `pg_insert_run` from `submit_run` (byte-identical SQL so `.sqlx` reuses the entry):

```rust
/// Insert a run row on any executor — callable from inside a commit
/// transaction (the data-trigger seam) as well as `submit_run`'s own tx.
pub(crate) async fn pg_insert_run<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    run: &TransformRun,
) -> Result<()> {
    let body = ser(&run.body)?;
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
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(())
}
```

then the hook:

```rust
/// Fire data-triggered transforms for `committed` tables, inside the
/// caller's open commit transaction — the slice-3 seam. For each def with
/// `on_input_commit` whose resolved inputs intersect `committed`:
/// lock the def row (name order; serializes the debounce against concurrent
/// commits without deadlock), skip if a `Queued` run already exists
/// (at-most-one-pending; `Running` does not suppress), then insert a
/// `DataTrigger` run and its queue job atomically with the commit.
///
/// `committing_run` is the run performing this commit (self-trigger
/// suppression): its transform, if any, never re-fires from its own write.
/// Undecodable def bodies are skipped with a warning — a poisoned admin
/// artifact must not fail unrelated ingest commits. Typed names that no
/// longer resolve contribute no match.
pub(crate) async fn pg_fire_data_triggers(
    conn: &mut sqlx::PgConnection,
    committed: &[TableRef],
    committing_run: Option<Uuid>,
) -> Result<usize> {
    if committed.is_empty() {
        return Ok(0);
    }
    let rows = sqlx::query!(
        "select name, body from transforms.transform where on_input_commit order by name",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(backend)?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut bodies: Vec<(TransformName, TransformBody)> = Vec::with_capacity(rows.len());
    for r in rows {
        match de_body(r.body) {
            Ok(b) => bodies.push((TransformName(r.name), b)),
            Err(e) => {
                tracing::warn!(transform = %r.name, error = %e,
                    "data trigger: undecodable body skipped");
            }
        }
    }
    let skip: Option<String> = match committing_run {
        Some(rid) => sqlx::query_scalar!(
            "select transform from transforms.run where run_id = $1",
            rid,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(backend)?
        .flatten(),
        None => None,
    };
    let types = pg_type_tables(&mut *conn, &bodies).await?;
    let mut fired = 0usize;
    for (name, body) in bodies {
        if skip.as_deref() == Some(name.0.as_str()) {
            continue;
        }
        let node = TriggerNode::resolve(&name, &body, &types);
        if !node.inputs.iter().any(|t| committed.contains(t)) {
            continue;
        }
        let live = sqlx::query_scalar!(
            "select name from transforms.transform where name = $1 for update",
            name.0,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(backend)?;
        if live.is_none() {
            continue; // deleted since the candidate query
        }
        let pending = sqlx::query_scalar!(
            r#"select exists(
                   select 1 from transforms.run where transform = $1 and state = 'queued'
               ) as "pending!""#,
            name.0,
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
        if pending {
            continue;
        }
        let run_id = Uuid::new_v4();
        let run = TransformRun {
            run_id,
            transform: Some(name),
            trigger: RunTrigger::DataTrigger,
            state: RunState::Queued,
            body: body.clone(),
            queued_at: OffsetDateTime::now_utc(),
            started_at: None,
            finished_at: None,
            snapshot_id: None,
            error: None,
        };
        pg_insert_run(&mut *conn, &run).await?;
        crate::queue::pg_insert(&mut *conn, &body.to_job(run_id)).await?;
        fired += 1;
    }
    Ok(fired)
}
```

- [ ] **Step 5: Wire the call sites.**

`commit_mirror.rs` — `CommitExtras` gains (after `jobs`):

```rust
    /// Fire data-triggered transforms for these committed tables inside the
    /// commit tx (slice 3). NEW-DATA commits only: data-preserving rewrites
    /// (the inline flush) leave this empty so already-fired data cannot
    /// re-fire on its own flush. Empty slice (the `Default`) fires nothing.
    pub data_trigger_tables: &'a [TableRef],
```

(add `use control_plane_core::TableRef;`) and `apply_commit_extras` gains, after the jobs loop:

```rust
    crate::transforms::pg_fire_data_triggers(
        &mut *conn,
        extras.data_trigger_tables,
        extras.lineage.map(|ev| ev.run_id.0),
    )
    .await?;
```

`iceberg_landing.rs`:
- `land_parquet`: `data_trigger_tables: std::slice::from_ref(table),` in its `CommitExtras { .. }`.
- `overwrite_parquet_snapshot`: same field in its `CommitExtras { .. }`.
- `overwrite_truncate`: before `tx.commit()`:
  ```rust
  crate::transforms::pg_fire_data_triggers(
      &mut tx,
      std::slice::from_ref(table),
      lineage.map(|ev| ev.run_id.0),
  )
  .await?;
  ```
- `write_steps`: before `tx.commit()` (duplicate targets are harmless — matching is per def):
  ```rust
  let written: Vec<TableRef> = staged.iter().map(|s| s.table.clone()).collect();
  crate::transforms::pg_fire_data_triggers(&mut tx, &written, Some(lineage.run_id.0)).await?;
  ```
  (the `pg_emit(&mut *tx, &lineage)` above already borrows `lineage` — order the fire call after it and only pass the copied `run_id`.)

`iceberg_inline.rs`:
- `inline_append`: capture `let committing = lineage.run_id.0;` near the top (before `lineage` is consumed), and before `tx.commit()`:
  ```rust
  crate::transforms::pg_fire_data_triggers(&mut tx, std::slice::from_ref(table), Some(committing)).await?;
  ```
- `write_inline_delta`: identical pattern.
- The flush function (the other tx in this file, ~lines 745+ if it is NOT `write_inline_delta` — the implementer confirms which functions own transactions here) gets NO call: flush is a data-preserving rewrite.

`iceberg_control_plane.rs` — `IcebergTx::commit`, after the `pg_mark_run_succeeded` block, before `tx.commit()`:

```rust
        // Data triggers (slice 3): fire for the tables this commit wrote new
        // data into. Compaction-only commits rewrite existing data and fire
        // nothing. `staged_run_success` is the committing run — its own
        // transform is suppressed inside the hook.
        let mut written: Vec<TableRef> = staged_files.iter().map(|(t, _, _)| t.clone()).collect();
        written.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
        written.dedup();
        crate::transforms::pg_fire_data_triggers(&mut tx, &written, staged_run_success).await?;
```

- [ ] **Step 6: sqlx + tests green**

```bash
bash tools/sqlx-prepare.sh
buck2 test //src/control-plane/postgres:data-triggers //src/control-plane/postgres:transforms //src/control-plane/postgres:sqlx-cache-check //src/control-plane/postgres:iceberg_landing //src/control-plane/postgres:iceberg_inline //src/control-plane/postgres:iceberg_flush --unstable-allow-all-tests-on-re > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log
```
(Adjust target names to what `src/control-plane/postgres/BUCK` actually calls the landing/inline/flush tests.) Expected: all PASS.

- [ ] **Step 7: Whole-tree build, prek, commit**

```bash
buck2 build -M none //src/... > /tmp/b3.log 2>&1; tail -3 /tmp/b3.log
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -c Failed /tmp/prek.log
git add -A
git commit --no-verify -m "feat(transform): postgres commit-seam data triggers — same-tx enqueue, debounce, self-skip"
```

---

### Task 4: Memory commit-seam hook + testkit `transform_data_trigger_contract` on both adapters

**Files:**
- Modify: `src/control-plane/memory/src/transaction.rs` (`MemoryTx` + commit hook)
- Modify: `src/control-plane/memory/src/lib.rs` (MemoryTx construction sites gain the `ontology` field)
- Modify: `src/control-plane/testkit/src/lib.rs` (new contract)
- Modify: `src/control-plane/memory/tests/transforms.rs`, `src/control-plane/postgres/tests/transforms.rs` (wire the contract)

**Interfaces:**
- Consumes: `TriggerNode::resolve` (Task 1); postgres hook (Task 3) — the contract must pass on BOTH adapters.
- Produces: `pub async fn transform_data_trigger_contract<CP>(cp: &CP) where CP: ControlPlane + TableControlPlane` — like `transform_run_commit_success_contract`, transforms/ontology access goes through `cp.transforms()` / `cp.ontology()` (IcebergControlPlane has no direct concern impls — the slice-1 lesson).

**Steps:**

- [ ] **Step 1: Write the failing contract** in `src/control-plane/testkit/src/lib.rs` (after `transform_run_commit_success_contract`; reuse its `DataFile` literal style — copy an existing 7-field `DataFile` from that contract verbatim):

```rust
/// Data triggers (slice 3): a table-format commit into a def's input table
/// enqueues a `DataTrigger` run in the same commit; debounce, self-skip via
/// ontology rebinding, and compaction non-firing.
pub async fn transform_data_trigger_contract<CP>(cp: &CP)
where
    CP: ControlPlane + TableControlPlane,
{
    let t = cp.transforms();
    let src = tref("main", "dt_src");
    let dst = tref("main", "dt_dst");

    let commit_into = |table: TableRef, path: &'static str| async move {
        // one create+append+commit unit of work into `table`
        // (build cols: Vec<ColumnSpec> and a 1-file Vec<DataFile> exactly as
        //  transform_run_commit_success_contract does)
        let mut tx = cp.begin_table().await.unwrap();
        tx.create_table(&table, &cols).await.unwrap();
        tx.append_files(&table, &files).await.unwrap();
        tx.commit().await.unwrap()
    };

    // 1. Physical def fires: commit into src -> one Queued DataTrigger run,
    //    frozen body.
    let def = /* physical body: inputs [src], output dst, on_input_commit */;
    t.define_transform(def.clone()).await.unwrap();
    commit_into(src.clone(), "f1").await;
    let runs = t.list_runs(Some(&def.name), PageReq::default()).await.unwrap().items;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].trigger, RunTrigger::DataTrigger);
    assert_eq!(runs[0].state, RunState::Queued);
    assert_eq!(runs[0].body, def.body);

    // 2. Debounce: a second commit does not enqueue a second run.
    commit_into(src.clone(), "f2").await;
    assert_eq!(t.list_runs(Some(&def.name), PageReq::default()).await.unwrap().items.len(), 1);

    // 3. Running does NOT suppress: mark it running, commit again -> 2 runs.
    t.mark_run_running(runs[0].run_id).await.unwrap();
    commit_into(src.clone(), "f3").await;
    assert_eq!(t.list_runs(Some(&def.name), PageReq::default()).await.unwrap().items.len(), 2);

    // 4. Typed def resolves through the ontology at eval time.
    cp.ontology().define_type(/* Widget -> (main, dt_widgets), as seed_type does */).await.unwrap();
    let typed = /* typed body: inputs ["Widget"], output "Widget"?? NO — output
                   must differ; define a second type Gadget -> (main, dt_gadgets)
                   and use inputs ["Widget"], output "Gadget" */;
    t.define_transform(typed.clone()).await.unwrap();
    commit_into(tref("main", "dt_widgets"), "f4").await;
    assert_eq!(t.list_runs(Some(&typed.name), PageReq::default()).await.unwrap().items.len(), 1);

    // 5. Self-skip (defense in depth): rebind Gadget so `typed`'s input NOW
    //    resolves to its own output table, then have a run of `typed` commit
    //    into that table with mark_run_succeeded. Without suppression this
    //    would re-trigger `typed`.
    //    Rebind: redefine type Widget -> (main, dt_gadgets).
    cp.ontology().define_type(/* Widget -> (main, dt_gadgets) */).await.unwrap();
    let rid = uuid::Uuid::new_v4();
    t.submit_run(/* Queued Manual run of `typed`, run_id rid, body typed.body */, typed.body.to_job(rid)).await.unwrap();
    t.mark_run_running(rid).await.unwrap();
    let before = t.list_runs(Some(&typed.name), PageReq::default()).await.unwrap().items.len();
    {
        let mut tx = cp.begin_table().await.unwrap();
        tx.create_table(&tref("main", "dt_gadgets"), &cols).await.unwrap();
        tx.append_files(&tref("main", "dt_gadgets"), &files_named("f5")).await.unwrap();
        tx.mark_run_succeeded(rid).await.unwrap();
        tx.commit().await.unwrap();
    }
    let after = t.list_runs(Some(&typed.name), PageReq::default()).await.unwrap().items;
    assert_eq!(after.len(), before, "self-commit must not re-trigger the committing transform");
    assert_eq!(t.get_run(rid).await.unwrap().state, RunState::Succeeded);

    // 6. Compaction does not fire. Pick real live file paths from the
    //    catalog (`cp.catalog().files(...)` or reuse the paths appended in
    //    leg 1-3) and compact src; run counts unchanged.
    let n = t.list_runs(Some(&def.name), PageReq::default()).await.unwrap().items.len();
    {
        let mut tx = cp.begin_table().await.unwrap();
        tx.compact_files(&src, &["f1-path"], &files_named("f6")).await.unwrap();
        tx.commit().await.unwrap();
    }
    assert_eq!(t.list_runs(Some(&def.name), PageReq::default()).await.unwrap().items.len(), n);
}
```

The pseudocode comments above are for you, the implementer — replace them with concrete code copied/adapted from `transform_run_commit_success_contract` and `seed_type` (the exact `ColumnSpec`/`DataFile` construction, the `ObjectType` literal). A closure capturing `cp` across `.await` may fight the borrow checker — an `async fn`-style local helper or plain repetition per leg is fine; the LEGS are the requirement, not the closure.

Wire it up:
- `src/control-plane/memory/tests/transforms.rs`:
  ```rust
  #[tokio::test]
  async fn memory_passes_transform_data_trigger_contract() {
      let cp = control_plane_memory::MemoryControlPlane::new();
      control_plane_testkit::transform_data_trigger_contract(&cp).await;
  }
  ```
  (mirror how the file constructs `cp` for the other contracts)
- `src/control-plane/postgres/tests/transforms.rs`:
  ```rust
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn postgres_passes_transform_data_trigger_contract() {
      let fx = PgFixture::shared();
      let (cp, _wh) = iceberg_cp(fx).await;
      control_plane_testkit::transform_data_trigger_contract(&cp).await;
  }
  ```

- [ ] **Step 2: Run to verify failure/pass split**

```bash
buck2 test //src/control-plane/postgres:transforms --unstable-allow-all-tests-on-re > /tmp/t4p.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4p.log
buck2 test //src/control-plane/memory:transforms > /tmp/t4m.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4m.log
```
Expected: postgres PASSES already (Task 3's hook); memory FAILS (no hook yet). If postgres fails, the contract found a real Task-3 bug — fix the hook, not the contract.

- [ ] **Step 3: Memory hook** in `src/control-plane/memory/src/transaction.rs`:

1. `MemoryTx` gains `pub(crate) ontology: Arc<Mutex<crate::ontology::OntologyState>>,` — and every construction site (`grep -rn "MemoryTx {" src/control-plane/memory/src/`) gains `ontology: self.ontology.clone(),`.
2. In `commit`, BEFORE the four-lock block:

```rust
        // Ontology snapshot for data-trigger resolution, cloned BEFORE the
        // commit locks: the ontology mutex is never held together with
        // rows/lineage/catalog/transforms anywhere (verified: every
        // `ontology.lock()` site is single-lock or drops before taking
        // another) — keeping it out of the held set preserves that
        // deadlock-freedom by construction. Staleness across this instant
        // is immaterial (postgres reads per-statement inside its tx).
        let type_tables: std::collections::HashMap<String, TableRef> = self
            .ontology
            .lock()
            .types
            .iter()
            .map(|(n, t)| (n.clone(), t.table.clone()))
            .collect();
        // The committed new-data table set (appends + replaces; compactions
        // and bare creates fire nothing), captured before the apply loop
        // consumes `staged_writes`.
        let written: Vec<TableRef> = self
            .staged_writes
            .iter()
            .map(|w| match w {
                StagedWrite::Append(t, _) | StagedWrite::Replace(t, _) => t.clone(),
            })
            .collect();
        let mut fired = false;
```

(the implementer must verify the "never held together" claim: `grep -rn "ontology.lock()" src/control-plane/memory/src/` and check each site's guard scope; record the finding in the comment if it differs.)

3. Inside the lock block, AFTER the staged-run-success section (transforms lock already held):

```rust
            // --- data triggers (slice 3): mirror pg_fire_data_triggers ---
            if !written.is_empty() {
                let skip: Option<String> = self
                    .staged_run_success
                    .and_then(|rid| transforms.runs.get(&rid))
                    .and_then(|r| r.transform.as_ref().map(|t| t.0.clone()));
                let mut names: Vec<String> = transforms
                    .defs
                    .iter()
                    .filter(|(_, d)| d.on_input_commit)
                    .map(|(n, _)| n.clone())
                    .collect();
                names.sort_unstable();
                for name in names {
                    if skip.as_deref() == Some(name.as_str()) {
                        continue;
                    }
                    let Some((body, inputs)) = transforms.defs.get(&name).map(|def| {
                        let node = control_plane_core::TriggerNode::resolve(
                            &def.name,
                            &def.body,
                            &type_tables,
                        );
                        (def.body.clone(), node.inputs)
                    }) else {
                        continue;
                    };
                    if !inputs.iter().any(|t| written.contains(t)) {
                        continue;
                    }
                    let pending = transforms.runs.values().any(|r| {
                        r.state == control_plane_core::RunState::Queued
                            && r.transform.as_ref().is_some_and(|t| t.0 == name)
                    });
                    if pending {
                        continue;
                    }
                    let run_id = Uuid::new_v4();
                    MemoryControlPlane::insert(&mut rows, body.to_job(run_id));
                    transforms.runs.insert(
                        run_id,
                        control_plane_core::TransformRun {
                            run_id,
                            transform: Some(control_plane_core::TransformName(name)),
                            trigger: control_plane_core::RunTrigger::DataTrigger,
                            state: control_plane_core::RunState::Queued,
                            body,
                            queued_at: time::OffsetDateTime::now_utc(),
                            started_at: None,
                            finished_at: None,
                            snapshot_id: None,
                            error: None,
                        },
                    );
                    fired = true;
                }
            }
```

(Prefer plain `use` imports at the top of the file over the fully-qualified paths shown; match the file's existing style. `self.staged_run_success` is `Copy` — but by this point earlier code may have moved other `self` fields; if the borrow checker objects, bind `let staged_run_success = self.staged_run_success;` up top with the other captures.)

4. Notify on fired jobs: replace `if staged_any {` with `if staged_any || fired {`.
5. Extend the lock-order comment (lines 66–75) to mention the trigger step and the pre-block ontology snapshot.

- [ ] **Step 4: All green**

```bash
buck2 test //src/control-plane/memory:transforms //src/control-plane/memory:tx //src/control-plane/postgres:transforms --unstable-allow-all-tests-on-re > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log
```

- [ ] **Step 5: Whole-tree build, prek, commit**

```bash
buck2 build -M none //src/... > /tmp/b4.log 2>&1; tail -3 /tmp/b4.log
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -c Failed /tmp/prek.log
git add -A
git commit --no-verify -m "feat(transform): memory fake mirrors the commit-seam data trigger + cross-adapter contract"
```

---

### Task 5: Engine wire e2e + runtime admin route tests + doc-string accuracy

**Files:**
- Modify: `src/services/engine/tests/transform_wire.rs` (or a sibling e2e file — read the existing tests first and extend the one that drives a worker transform through `CommitTransform`)
- Modify: the runtime admin transform route tests (find them: `grep -rln "admin/transforms" src/services/runtime/tests/`)
- Modify: `src/services/runtime/src/admin.rs` — ONLY if its `#[utoipa::path]` descriptions or DTO doc comments say data triggers are unsupported/rejected (check `grep -n "on_input_commit\|slice 3\|data.trigger" src/services/runtime/src/admin.rs` and `src/services/runtime/src/openapi*.rs` if present)

**Interfaces:**
- Consumes: everything landed in Tasks 1–4. No new interfaces.

**Steps:**

- [ ] **Step 1: Engine e2e (failing first only if trivial to stage; this is integration glue — write it, run it, expect PASS since the seam already fires).** In the engine's transform wire e2e: after the existing test's worker transform commits via `CommitTransform` into table `T_out`, extend (or clone into a new `#[tokio::test]`) so that BEFORE running the transform, a downstream data-triggered def is defined whose input is `T_out`:

```rust
    // Downstream data-triggered def: fires when the transform commits T_out.
    cp.transforms()
        .define_transform(TransformDef {
            name: TransformName("downstream".into()),
            body: TransformBody::Physical {
                inputs: vec![t_out.clone()],
                output: tref("main", "downstream_out"),
                sql: "select 1".into(),
                output_mode: OutputMode::Append,
            },
            schedule: None,
            on_input_commit: true,
        })
        .await
        .unwrap();
```

and after the transform run is asserted `Succeeded`, assert the downstream run exists:

```rust
    let runs = cp
        .transforms()
        .list_runs(Some(&TransformName("downstream".into())), PageReq::default())
        .await
        .unwrap()
        .items;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].trigger, RunTrigger::DataTrigger);
    assert_eq!(runs[0].state, RunState::Queued);
```

Adapt names (`cp`, `t_out`, helper fns) to the existing test file's vocabulary — read it first; do not build new harness plumbing.

- [ ] **Step 2: Runtime admin tests.** In the admin transform route test file, add two tests mirroring the existing define-route tests' style (router driver, admin subject):

```rust
#[tokio::test]
async fn define_data_triggered_transform_is_accepted() {
    // POST /admin/transforms with on_input_commit: true -> 201; GET shows the flag.
    let body = serde_json::json!({
        "name": "dt",
        "body": {"kind": "physical",
                 "inputs": [{"schema": "main", "name": "src"}],
                 "output": {"schema": "main", "name": "dst"},
                 "sql": "select 1"},
        "on_input_commit": true
    });
    // ...post, assert 201; get /admin/transforms/dt, assert json.on_input_commit == true
}

#[tokio::test]
async fn define_trigger_cycle_is_rejected_400() {
    // dt-a: src->dst accepted; dt-b: dst->src -> 400, body mentions "cycle".
}
```

(Concrete request plumbing: copy the file's existing 201/400 define tests.)

- [ ] **Step 3: Doc-string sweep.** Fix any admin.rs/openapi description that still says `on_input_commit` is rejected/unsupported; ditto stale comments found by `grep -rn "slice 3" src/ | grep -v tests`. Core's `transforms.rs` module docs were fixed in Task 1 — this step catches the rest (worker/engine/runtime mentions).

- [ ] **Step 4: Run the touched targets**

```bash
buck2 test //src/services/engine:transform_wire //src/services/runtime:... --unstable-allow-all-tests-on-re > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log
```
(Fill in the real target names from the crates' BUCK files.)

- [ ] **Step 5: Whole-tree build, prek, commit**

```bash
buck2 build -M none //src/... > /tmp/b5.log 2>&1; tail -3 /tmp/b5.log
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -c Failed /tmp/prek.log
git add -A
git commit --no-verify -m "test(transform): data-trigger e2e over the engine wire + admin 201/400 routes"
```

---

### Task 6: Docs, register close, full sweep

**Files:**
- Modify: `docs/system-capabilities/transform.md` (data-trigger capability section)
- Modify: `docs/system-capabilities/control-plane.md` (commit-seam hook, migration 0033, `data_triggered_defs`)
- Modify: `docs/system-capabilities/ingest.md` (one line: landing commits fire data triggers in-tx) — check the file exists/its name under `docs/system-capabilities/`
- Modify: `docs/ROADMAP.md` (REMOVE the `road-transform-data-triggers` entry — registers carry open work only)
- Modify: `docs/FUTURE.md` (trim `#fut-transform-followups`' DAG bullet: define-time DAG validation landed; the bullet's remaining scope is watermark/incremental, Ballista, streaming scans)

**Steps:**

- [ ] **Step 1: Capability docs.** Follow `docs/system-capabilities/README.md` conventions. transform.md gains a "Data triggers" section: `on_input_commit` semantics, same-tx firing (list the seams: IcebergTx, inline append/delta, parquet/additive landing, multi-step writes, overwrite/truncate; flush + compaction excluded), debounce (at-most-one-pending; Running does not suppress; concurrent-commit serialization via the def-row lock), define-time cycle rejection (Kahn over resolved edges; advisory-lock serialized on pg), runtime self-skip, poison-body skip-and-warn. Cite `PR #NN` where the register convention wants a PR reference (patched post-PR).

- [ ] **Step 2: Register edits.** Remove the ROADMAP entry block (title line + prose). In FUTURE, edit `#fut-transform-followups`: delete/trim the DAG-validation bullet, keep watermark/incremental + Ballista + streaming; mention slice 3 landed it via `` `road-transform-data-triggers` `` as a code span (never `[[…]]` to a removed id). Validate:

```bash
bash tools/docs.sh validate
```

- [ ] **Step 3: Full sweep** (all first-party tests — this is the branch gate, cloud-safe form):

```bash
buck2 build -M none //src/... > /tmp/b6.log 2>&1; tail -3 /tmp/b6.log
buck2 test //src/... --unstable-allow-all-tests-on-re > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6.log
```
Expected: `Tests finished` with zero failures (≈340 tests).

- [ ] **Step 4: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -c Failed /tmp/prek.log
git add -A
git commit --no-verify -m "docs(transform): data triggers capability; close road-transform-data-triggers"
```

---

## Post-plan pipeline (controller, not a task)

Final whole-branch review (most capable model) + metric gate (`loom-complexity diff`, `loom-duplication diff`) → fix wave → PR from `work/road-transform-data-triggers` (lease-check before push) → patch `PR #NN` placeholders via targeted sed → CI via commit-status polling → squash-merge on green.
