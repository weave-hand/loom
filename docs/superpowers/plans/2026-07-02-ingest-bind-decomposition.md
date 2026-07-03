# Ingest bind/gate decomposition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decompose the ingest `bind`/`validate_derived`/`gate` hotspots into pure,
unit-testable collectors and a cohesive `Aggregation` typing unit; add core
`LineageEvent` completion constructors and adopt them at the empty-inputs emitters;
and split `extract_pg` behind guard clauses — all behavior-preserving.

**Architecture:** Five independent, additive-then-adopt refactors. New pure functions
and core methods are introduced first (additive, no signature breakage), then the
call sites are rewritten to consume them and the old inline code is deleted. Every
refactor is guarded by existing integration tests plus new unit tests over the
extracted pure surfaces.

**Tech Stack:** Rust 2024, buck2 (`rust_test` targets only — no inline `#[cfg(test)]`),
arrow 58 (`AsArray`), `control_plane_core` domain types.

## Global Constraints

- **Tests are `rust_test` integration targets only.** Put every new test in a sibling
  `tests/<name>.rs` file wired as its own `rust_test` in the crate's `BUCK` (mirror an
  existing target). Never add an inline `#[cfg(test)] mod tests` — the `no-inline-tests`
  prek hook fails the build, and buck2 never runs inline tests anyway.
- **Behavior-preserving.** Every extraction must produce byte-identical results to the
  code it replaces. Where the old code had a semantic quirk (e.g. `is_ordered` treats
  every non-`Boolean` base type — including `Vector`/`String`/`Date` — as ordered),
  preserve it exactly; any improvement is out of scope.
- **Panic-safety clippy is enforced on production code** (`unwrap_used`, `expect_used`,
  `indexing_slicing`, `panic`, `todo`). Use `?`/`match`/iterator combinators, not
  `unwrap`/indexing. Test code is exempt via the `loom_rust_test` wrapper.
- **Build with `-M none`** in this cloud session; scope `buck2 test` to the named
  targets only — never a bare whole-tree `buck2 test //src/...`. Remote execution is
  configured, so pure-logic tests run on RE.
- **Verification command shape:** `buck2 test <targets> > /tmp/t.log 2>&1; grep -E
  "Tests finished|FAIL" /tmp/t.log` — never pipe `buck2 test` through `tail`/`head`.
- **No `.sqlx` change** is required by this plan (none of these paths touch compile-time
  SQL).

---

## File Structure

- `src/control-plane/core/src/lineage.rs` — add `LineageEvent::completed` /
  `completed_with_run` constructors (Task 1).
- `src/control-plane/core/src/logical_type.rs` — add `BaseType::is_numeric` /
  `is_ordered` (Task 2).
- `src/control-plane/core/src/ontology.rs` — add the `Aggregation` typing unit
  (`column`/`label`/`column_applicable`) and a `ResultExpectation` enum with
  `Aggregation::result_expectation` (Task 2).
- `src/control-plane/core/BUCK` — wire new core test targets (Tasks 1, 2).
- `src/control-plane/core/tests/lineage_completed.rs` — new (Task 1).
- `src/control-plane/core/tests/aggregation_typing.rs` — new (Task 2).
- `src/control-plane/core/tests/logical_type.rs` — extend for `is_numeric`/`is_ordered`
  (Task 2).
- `src/services/ingest/src/bind.rs` — rewrite `validate_derived` over the core typing
  unit (Task 3); extract pure `structural_violations` collectors and reuse
  `schema_of_table` for `bind`'s steps 1–2 (Task 4).
- `src/services/ingest/src/http.rs` — adopt `LineageEvent::completed` /
  `completed_with_run` (Task 1).
- `src/services/query-api/src/action.rs` — adopt `LineageEvent::completed` at the
  create-from-params site (Task 1).
- `src/services/ingest/src/gate.rs` — rewrite `validate_values` over `AsArray` (Task 5).
- `src/services/ingest/tests/bind_structural.rs` — new unit tests for the pure
  collectors (Task 4).
- `src/services/ingest/BUCK` — wire the new `bind-structural` test target (Task 4).
- `src/services/managed-postgres-embed/src/lib.rs` — guard-clause inversion +
  `unpack_to_temp`/`publish` split of `extract_pg` (Task 6).

Task order: Task 1 and Task 2 are core-only additive prerequisites; Task 3 consumes
Task 2; Tasks 4, 5, 6 are independent. Implement in the numbered order.

---

### Task 1: `LineageEvent` completion constructors + adopt at empty-inputs sites

**Files:**
- Modify: `src/control-plane/core/src/lineage.rs` (add `impl LineageEvent` after the
  struct at line 109)
- Modify: `src/control-plane/core/BUCK` (add `lineage-completed` test target)
- Create: `src/control-plane/core/tests/lineage_completed.rs`
- Modify: `src/services/ingest/src/http.rs:337-344` and `:421-428`
- Modify: `src/services/query-api/src/action.rs:488-495`

**Interfaces:**
- Produces:
  - `LineageEvent::completed(outputs: Vec<DatasetRef>, payload: serde_json::Value) -> LineageEvent`
    — `event_type: Complete`, fresh `run_id`, `event_time: now_utc()`, `inputs: []`.
  - `LineageEvent::completed_with_run(run_id: RunId, outputs: Vec<DatasetRef>, payload: serde_json::Value) -> LineageEvent`
    — same, with a caller-supplied `run_id`.
- Consumes: nothing (first task).

Note: `lineage.rs` already imports `time::OffsetDateTime` and `uuid::Uuid`; `RunId`,
`DatasetRef`, `EventType` are all in-module. No new imports.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/core/tests/lineage_completed.rs`:

```rust
//! The `LineageEvent::completed` / `completed_with_run` core constructors: the
//! five-field "completed event" ritual (Complete + empty inputs + now) built once,
//! so the ingest and action emitters stop hand-assembling it.

use control_plane_core::{DatasetRef, EventType, LineageEvent, RunId};
use uuid::Uuid;

fn dref(name: &str) -> DatasetRef {
    DatasetRef {
        namespace: "loom".into(),
        name: name.into(),
    }
}

#[test]
fn completed_sets_complete_empty_inputs_and_carries_outputs_payload() {
    let payload = serde_json::json!({ "source": "test" });
    let ev = LineageEvent::completed(vec![dref("t")], payload.clone());
    assert_eq!(ev.event_type, EventType::Complete);
    assert!(ev.inputs.is_empty());
    assert_eq!(ev.outputs, vec![dref("t")]);
    assert_eq!(ev.payload, payload);
}

#[test]
fn completed_mints_a_distinct_run_id_each_call() {
    let a = LineageEvent::completed(vec![], serde_json::Value::Null);
    let b = LineageEvent::completed(vec![], serde_json::Value::Null);
    assert_ne!(a.run_id, b.run_id, "each completed() mints a fresh run id");
}

#[test]
fn completed_with_run_preserves_the_supplied_run_id() {
    let run = RunId(Uuid::from_u128(42));
    let ev = LineageEvent::completed_with_run(run, vec![dref("out")], serde_json::Value::Null);
    assert_eq!(ev.run_id, run);
    assert_eq!(ev.event_type, EventType::Complete);
    assert!(ev.inputs.is_empty());
    assert_eq!(ev.outputs, vec![dref("out")]);
}
```

Wire it in `src/control-plane/core/BUCK` (add after the `lineage-cursor` block):

```python
rust_test(
    name = "lineage-completed",
    crate = "lineage_completed",
    srcs = ["tests/lineage_completed.rs"],
    crate_root = "tests/lineage_completed.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [
        ":core",
        "//third-party:serde_json",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test //src/control-plane/core:lineage-completed > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|no function|no associated" /tmp/t.log`
Expected: FAIL — `no function or associated item named 'completed'`.

- [ ] **Step 3: Write minimal implementation**

In `src/control-plane/core/src/lineage.rs`, immediately after the `LineageEvent`
struct (after line 109), add:

```rust
impl LineageEvent {
    /// A completed (`EventType::Complete`) event with a freshly-minted `run_id`, the
    /// current UTC time, and no inputs — the shape emitted by the ingest land/model
    /// paths and query-api's create-from-params action. `outputs` are the datasets the
    /// run produced; `payload` is the opaque OpenLineage body.
    #[must_use]
    pub fn completed(outputs: Vec<DatasetRef>, payload: serde_json::Value) -> Self {
        Self::completed_with_run(RunId(Uuid::new_v4()), outputs, payload)
    }

    /// [`LineageEvent::completed`] against a caller-supplied `run_id` — used where the
    /// caller owns the run id (hands it to the engine to commit row + event atomically,
    /// or threads an `X-Loom-Run-Id` header through).
    #[must_use]
    pub fn completed_with_run(
        run_id: RunId,
        outputs: Vec<DatasetRef>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            run_id,
            event_type: EventType::Complete,
            event_time: OffsetDateTime::now_utc(),
            inputs: Vec::new(),
            outputs,
            payload,
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `buck2 test //src/control-plane/core:lineage-completed > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (`Tests finished: Pass 3`).

- [ ] **Step 5: Adopt the constructors at the empty-inputs emitters**

In `src/services/ingest/src/http.rs`, replace the struct literal at lines 337–344
(the `model` handler) with:

```rust
    let lineage = LineageEvent::completed(
        vec![DatasetRef::from(&type_name)],
        serde_json::json!({ "source": "http-model", "type": type_label }),
    );
```

Replace the struct literal at lines 421–428 (the `land` handler) with:

```rust
    let lineage = LineageEvent::completed_with_run(
        run_id,
        vec![DatasetId::from(&table).dataset_ref()],
        serde_json::json!({ "source": "http-land" }),
    );
```

In `src/services/query-api/src/action.rs`, replace the struct literal at lines
488–495 (the create-from-params site) with:

```rust
    let run_id = RunId(Uuid::new_v4());
    let event = LineageEvent::completed_with_run(
        run_id,
        vec![DatasetRef::from(&action.target)],
        serde_json::json!({ "action": action_name }),
    );
```

**This site is `completed_with_run`, not `completed`.** The original literal set
`run_id: run_id`, and that same local `run_id` is handed to the engine at the
`write_object` call below so the row and the event commit under one correlated id.
Keep the `let run_id = RunId(Uuid::new_v4());` line and pass it into
`completed_with_run` — this stays byte-identical. (The only bare-`completed()` adopter
is the ingest `model` handler above, which mints its run id inline and uses it nowhere
else.)

After editing, fix imports under the strict unused-imports gate — the outcome is
deterministic (grep to confirm, but this is what you will find):
- **`http.rs`**: `EventType` and `use time::OffsetDateTime;` become unused (both literals
  were the only users) — **remove both** from the import block.
- **`action.rs`**: **keep `EventType`** — it is still used at the unchanged `action.rs:733`
  emitter (`event_type: EventType::Complete`). `OffsetDateTime` there is inline-qualified
  (`time::OffsetDateTime::now_utc()`), so there is no import line to remove.

Confirm with `buck2 build '//src/services/ingest:ingest[clippy.txt]'` and
`'//src/services/query-api:query-api[clippy.txt]'` (both must be empty).

- [ ] **Step 6: Verify the adopting crates still pass**

Run: `buck2 test //src/services/ingest:http-land //src/services/ingest:http-model //src/services/query-api:iceberg-action-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS. (If `iceberg-action-e2e` is a fixture test that is slow/needs pg, also
acceptable to rely on the build: `buck2 build -M none //src/services/query-api:query-api > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED|error\[" /tmp/b.log`.)

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/core/src/lineage.rs src/control-plane/core/BUCK \
        src/control-plane/core/tests/lineage_completed.rs \
        src/services/ingest/src/http.rs src/services/query-api/src/action.rs
git commit -m "refactor(core): LineageEvent completion constructors; adopt at empty-inputs emitters"
```

---

### Task 2: `BaseType` numeric/ordered predicates + `Aggregation` typing unit

**Files:**
- Modify: `src/control-plane/core/src/logical_type.rs` (add methods to the `impl
  BaseType` block, ~line 58)
- Modify: `src/control-plane/core/src/ontology.rs` (add `impl Aggregation` + a
  `ResultExpectation` enum after the `Aggregation` def at line 229)
- Modify: `src/control-plane/core/BUCK` (add `aggregation-typing` test target)
- Create: `src/control-plane/core/tests/aggregation_typing.rs`
- Modify: `src/control-plane/core/tests/logical_type.rs` (extend)

**Interfaces:**
- Produces (in `logical_type.rs`):
  - `BaseType::is_numeric(self) -> bool` — `matches!(self, Integer | Long | Double)`.
  - `BaseType::is_ordered(self) -> bool` — `!matches!(self, Boolean)` (preserve the
    "everything except Boolean" quirk exactly).
- Produces (in `ontology.rs`):
  - `Aggregation::column(&self) -> Option<&str>` — the target column, `None` for `Count`.
  - `Aggregation::label(&self) -> &'static str` — `"Count"`/`"Sum"`/`"Avg"`/`"Min"`/`"Max"`.
  - `Aggregation::column_applicable(&self, col: Option<BaseType>) -> bool` — `Sum`/`Avg`
    need `col.is_some_and(BaseType::is_numeric)`; `Min`/`Max` need
    `col.is_some_and(BaseType::is_ordered)`; `Count` is always `true`.
  - `pub enum ResultExpectation { IntegerOrLong, Numeric, ExactColumn(Option<BaseType>) }`
    with `ResultExpectation::accepts(&self, declared: Option<BaseType>) -> bool` and
    `ResultExpectation::description(&self) -> String`.
  - `Aggregation::result_expectation(&self, col: Option<BaseType>) -> ResultExpectation` —
    `Count => IntegerOrLong`, `Sum|Avg => Numeric`, `Min|Max => ExactColumn(col)`.
- Consumes: nothing from Task 1.

`ontology.rs` must import `BaseType` from `crate::logical_type` for
`ResultExpectation::accepts`/`description`. Check the file's existing `use` block and add
`use crate::logical_type::BaseType;` if absent. Do NOT import `resolve_logical` here — it
is not used by the added code (`accepts` takes `Option<BaseType>`, not a string); an
unused import would redden the strict clippy gate.

The behavior these methods must reproduce is the current `bind.rs` logic:
`is_numeric`/`is_ordered`/`agg_column`/`agg_label` (bind.rs lines 178–207) and the
`applicable` + `(ok, expected)` matches in `validate_derived` (bind.rs lines 266–304).
`ResultExpectation::description()` must return exactly: `"integer or long"` (Count),
`"numeric"` (Sum/Avg), and for `ExactColumn(Some(b))` → `b.canonical_name()`, for
`ExactColumn(None)` → `"the target column's type"`. `ResultExpectation::accepts(declared)`
must return: Count → `matches!(declared, Some(Integer | Long))`; Numeric →
`declared.is_some_and(is_numeric)`; `ExactColumn(col)` → `declared.is_some() && declared
== col`.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/core/tests/aggregation_typing.rs`:

```rust
//! The `Aggregation` typing unit: column/label/applicability/result-category — the
//! cohesive home the bind derived-property validator delegates to (and the single
//! landing site for the future coercion taxonomy).

use control_plane_core::{Aggregation, BaseType, ResultExpectation};

#[test]
fn column_is_none_only_for_count() {
    assert_eq!(Aggregation::Count.column(), None);
    assert_eq!(Aggregation::Sum("amount".into()).column(), Some("amount"));
    assert_eq!(Aggregation::Avg("amount".into()).column(), Some("amount"));
    assert_eq!(Aggregation::Min("t".into()).column(), Some("t"));
    assert_eq!(Aggregation::Max("t".into()).column(), Some("t"));
}

#[test]
fn labels_match_variants() {
    assert_eq!(Aggregation::Count.label(), "Count");
    assert_eq!(Aggregation::Sum(String::new()).label(), "Sum");
    assert_eq!(Aggregation::Avg(String::new()).label(), "Avg");
    assert_eq!(Aggregation::Min(String::new()).label(), "Min");
    assert_eq!(Aggregation::Max(String::new()).label(), "Max");
}

#[test]
fn column_applicable_matches_numeric_ordered_rules() {
    // Sum/Avg require numeric.
    assert!(Aggregation::Sum(String::new()).column_applicable(Some(BaseType::Double)));
    assert!(!Aggregation::Sum(String::new()).column_applicable(Some(BaseType::String)));
    assert!(!Aggregation::Avg(String::new()).column_applicable(Some(BaseType::Boolean)));
    assert!(!Aggregation::Sum(String::new()).column_applicable(None));
    // Min/Max require ordered (everything except Boolean).
    assert!(Aggregation::Min(String::new()).column_applicable(Some(BaseType::String)));
    assert!(Aggregation::Max(String::new()).column_applicable(Some(BaseType::Date)));
    assert!(!Aggregation::Min(String::new()).column_applicable(Some(BaseType::Boolean)));
    assert!(!Aggregation::Max(String::new()).column_applicable(None));
    // Count is always applicable (no column).
    assert!(Aggregation::Count.column_applicable(None));
}

#[test]
fn result_expectation_accepts_and_describes() {
    // Count -> integer or long.
    let c = Aggregation::Count.result_expectation(None);
    assert!(c.accepts(Some(BaseType::Integer)));
    assert!(c.accepts(Some(BaseType::Long)));
    assert!(!c.accepts(Some(BaseType::Double)));
    assert_eq!(c.description(), "integer or long");

    // Sum/Avg -> numeric.
    let s = Aggregation::Sum("a".into()).result_expectation(Some(BaseType::Double));
    assert!(s.accepts(Some(BaseType::Long)));
    assert!(!s.accepts(Some(BaseType::String)));
    assert!(!s.accepts(None));
    assert_eq!(s.description(), "numeric");

    // Min/Max -> exact column type.
    let m = Aggregation::Max("t".into()).result_expectation(Some(BaseType::Timestamp));
    assert!(m.accepts(Some(BaseType::Timestamp)));
    assert!(!m.accepts(Some(BaseType::Date)));
    assert!(!m.accepts(None));
    assert_eq!(m.description(), BaseType::Timestamp.canonical_name());

    // Min/Max with an unresolved column type describes the fallback.
    let unknown = Aggregation::Min("t".into()).result_expectation(None);
    assert!(!unknown.accepts(Some(BaseType::Long)));
    assert_eq!(unknown.description(), "the target column's type");
}
```

Add `ResultExpectation` to the crate's `is_numeric`/`is_ordered` coverage in
`src/control-plane/core/tests/logical_type.rs` (append these two tests at the end of
the file):

```rust
#[test]
fn is_numeric_covers_only_integer_long_double() {
    use control_plane_core::BaseType::*;
    for b in [Integer, Long, Double] {
        assert!(b.is_numeric(), "{b:?} should be numeric");
    }
    for b in [Boolean, String, Date, Timestamp] {
        assert!(!b.is_numeric(), "{b:?} should not be numeric");
    }
    assert!(!control_plane_core::BaseType::Vector(3).is_numeric());
}

#[test]
fn is_ordered_is_everything_except_boolean() {
    use control_plane_core::BaseType::*;
    assert!(!Boolean.is_ordered());
    for b in [Integer, Long, Double, String, Date, Timestamp] {
        assert!(b.is_ordered(), "{b:?} should be ordered");
    }
    assert!(control_plane_core::BaseType::Vector(3).is_ordered());
}
```

Wire the new target in `src/control-plane/core/BUCK`:

```python
rust_test(
    name = "aggregation-typing",
    crate = "aggregation_typing",
    srcs = ["tests/aggregation_typing.rs"],
    crate_root = "tests/aggregation_typing.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [":core"],
)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test //src/control-plane/core:aggregation-typing //src/control-plane/core:logical-type > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|no method|cannot find" /tmp/t.log`
Expected: FAIL — `cannot find type 'ResultExpectation'` / `no method named 'column'`.

- [ ] **Step 3: Write minimal implementation**

In `src/control-plane/core/src/logical_type.rs`, inside the existing `impl BaseType`
block (before its closing `}` near line 105), add:

```rust
    /// `Sum`/`Avg` apply only to numeric base types.
    #[must_use]
    pub fn is_numeric(self) -> bool {
        matches!(self, BaseType::Integer | BaseType::Long | BaseType::Double)
    }

    /// `Min`/`Max` apply to any totally-ordered base type — every base type except
    /// `Boolean`. (Preserves the pre-decomposition bind.rs semantics: `Vector`,
    /// `String`, `Date`, `Timestamp` are all treated as ordered here.)
    #[must_use]
    pub fn is_ordered(self) -> bool {
        !matches!(self, BaseType::Boolean)
    }
```

In `src/control-plane/core/src/ontology.rs`, after the `Aggregation` enum (line 229),
add (adding `use crate::logical_type::BaseType;` to the file's imports if not present):

```rust
/// The result-type expectation of an aggregation, resolved against the target
/// column's base type. Pairs the acceptance predicate ([`ResultExpectation::accepts`])
/// with the human description ([`ResultExpectation::description`]) used in violation
/// messages, so the derived-property validator carries neither inline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultExpectation {
    /// A `Count`: an int64 in disguise — accept `Integer` or `Long`.
    IntegerOrLong,
    /// A `Sum`/`Avg`: any numeric base type.
    Numeric,
    /// A `Min`/`Max`: exactly the target column's own base type (`None` if that type
    /// could not be resolved, which nothing then satisfies).
    ExactColumn(Option<BaseType>),
}

impl ResultExpectation {
    /// Whether a declared result type (resolved to a `BaseType`, or `None` if unknown)
    /// is consistent with this category.
    #[must_use]
    pub fn accepts(&self, declared: Option<BaseType>) -> bool {
        match self {
            ResultExpectation::IntegerOrLong => {
                matches!(declared, Some(BaseType::Integer | BaseType::Long))
            }
            ResultExpectation::Numeric => declared.is_some_and(BaseType::is_numeric),
            ResultExpectation::ExactColumn(col) => declared.is_some() && declared == *col,
        }
    }

    /// A human label for the expected result type, used in violation messages.
    #[must_use]
    pub fn description(&self) -> String {
        match self {
            ResultExpectation::IntegerOrLong => "integer or long".to_string(),
            ResultExpectation::Numeric => "numeric".to_string(),
            ResultExpectation::ExactColumn(Some(b)) => b.canonical_name(),
            ResultExpectation::ExactColumn(None) => "the target column's type".to_string(),
        }
    }
}

impl Aggregation {
    /// The target-type column this aggregation reads, or `None` for `Count` (which
    /// aggregates rows, not a column).
    #[must_use]
    pub fn column(&self) -> Option<&str> {
        match self {
            Aggregation::Count => None,
            Aggregation::Sum(c)
            | Aggregation::Avg(c)
            | Aggregation::Min(c)
            | Aggregation::Max(c) => Some(c),
        }
    }

    /// A human label for this aggregation, used in violation messages.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Aggregation::Count => "Count",
            Aggregation::Sum(_) => "Sum",
            Aggregation::Avg(_) => "Avg",
            Aggregation::Min(_) => "Min",
            Aggregation::Max(_) => "Max",
        }
    }

    /// Whether this aggregation is applicable to a column of base type `col`
    /// (`None` = unresolved). `Sum`/`Avg` need numeric; `Min`/`Max` need ordered;
    /// `Count` takes no column and is always applicable.
    #[must_use]
    pub fn column_applicable(&self, col: Option<BaseType>) -> bool {
        match self {
            Aggregation::Sum(_) | Aggregation::Avg(_) => col.is_some_and(BaseType::is_numeric),
            Aggregation::Min(_) | Aggregation::Max(_) => col.is_some_and(BaseType::is_ordered),
            Aggregation::Count => true,
        }
    }

    /// The result-type expectation for this aggregation given its target column's base
    /// type `col` (used only by `Min`/`Max`).
    #[must_use]
    pub fn result_expectation(&self, col: Option<BaseType>) -> ResultExpectation {
        match self {
            Aggregation::Count => ResultExpectation::IntegerOrLong,
            Aggregation::Sum(_) | Aggregation::Avg(_) => ResultExpectation::Numeric,
            Aggregation::Min(_) | Aggregation::Max(_) => ResultExpectation::ExactColumn(col),
        }
    }
}
```

Export `ResultExpectation` from the crate root: in `src/control-plane/core/src/lib.rs`,
find the `pub use ...ontology::{...}` re-export and add `ResultExpectation` to it (check
the exact `pub use` line for the ontology module and append the name). If ontology
types are re-exported via a `pub use crate::ontology::*;` glob, no change is needed —
verify which pattern the file uses.

- [ ] **Step 4: Run test to verify it passes**

Run: `buck2 test //src/control-plane/core:aggregation-typing //src/control-plane/core:logical-type > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/core/src/logical_type.rs src/control-plane/core/src/ontology.rs \
        src/control-plane/core/src/lib.rs src/control-plane/core/BUCK \
        src/control-plane/core/tests/aggregation_typing.rs \
        src/control-plane/core/tests/logical_type.rs
git commit -m "refactor(core): Aggregation typing unit + BaseType numeric/ordered predicates"
```

---

### Task 3: Rewrite `bind::validate_derived` over the core typing unit

**Files:**
- Modify: `src/services/ingest/src/bind.rs` (delete `agg_column`/`agg_label`/
  `is_numeric`/`is_ordered` at lines 176–207; rewrite `validate_derived` lines
  213–315; add a `target_column_type` helper)
- Test: `src/services/ingest/tests/bind_validation.rs` (existing — the exhaustive
  derived-property violation matrix; must stay green unchanged)

**Interfaces:**
- Consumes (from Task 2): `Aggregation::{column, label, column_applicable,
  result_expectation}`, `ResultExpectation`, `BaseType`, `resolve_logical`.
- Produces (private to `bind.rs`):
  - `enum ColumnLookup { Missing, Present(Option<BaseType>) }` — the result of
    resolving an aggregation's target column. `Missing` means the target type, its live
    snapshot, or the column itself is absent (the three collapsed `NotFound →
    MissingAggColumn` cases); `Present(base)` means the column exists, with `base =
    resolve_logical(col.ty)` (which may itself be `None` for an unresolved type — a
    present column whose type is unknown, which the old code did NOT treat as
    `MissingAggColumn`).
  - `async fn target_column_type(catalog: &dyn Catalog, ontology: &dyn Ontology, link:
    &LinkDef, col_name: &str) -> Result<ColumnLookup, BindError>` — resolves the link
    target type → table → snapshot → column base type into a `ColumnLookup`; propagates
    any non-`NotFound` `ControlPlaneError` as `Err`.

  **Why the enum, not `Option<BaseType>`:** the old code distinguished "column absent"
  (push `MissingAggColumn`, return) from "column present but its `ty` did not
  `resolve_logical`" (`col_base = None`, keep going — no `MissingAggColumn`). Collapsing
  both to `Ok(None)` would change behavior for the present-but-unresolved case. The enum
  keeps them distinct.

- [ ] **Step 1: Confirm the guarding tests exist and pass first (characterization)**

Run: `buck2 test //src/services/ingest:bind-validation > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (this is the behavior contract — it must still pass verbatim after the
rewrite). If it does not already exercise: an unknown-derived-link, a missing agg
column, a bad-agg-type (e.g. `Sum` on a string column), and a bad-derived-result-type,
add those cases now as failing-then-passing tests **against the current code** so the
rewrite is fully covered. (Read the file first; the matrix is described as exhaustive,
so most/all already exist.)

- [ ] **Step 2: Update imports and delete the free helpers**

In `src/services/ingest/src/bind.rs`, change the `use control_plane_core::{...}` block
to drop `BaseType` only if it becomes unused (it is still used — keep it) and ensure
`ResultExpectation` is available (it is referenced only transitively via
`result_expectation(...)`, so import is optional; `resolve_logical` stays). Delete the four
free functions `agg_column` (176–185), `agg_label` (187–196), `is_numeric` (198–201),
`is_ordered` (203–207).

- [ ] **Step 3: Add the `ColumnLookup` enum + `target_column_type` helper**

Insert (near `validate_derived`):

```rust
/// The result of resolving an aggregation's target column. `Missing` is the three
/// absent cases (target type, its live snapshot, or the column) the caller reports as
/// a single [`BindViolationReason::MissingAggColumn`]; `Present` carries the column's
/// resolved base type (`None` if its logical type is unknown — a present column, so
/// NOT a `MissingAggColumn`).
enum ColumnLookup {
    Missing,
    Present(Option<BaseType>),
}

/// Resolve `col_name` on `link`'s target type's table into a [`ColumnLookup`]. Any
/// non-`NotFound` control-plane error propagates.
async fn target_column_type(
    catalog: &dyn Catalog,
    ontology: &dyn Ontology,
    link: &LinkDef,
    col_name: &str,
) -> Result<ColumnLookup, BindError> {
    let target_table = match ontology.resolve(&link.to).await {
        Ok(t) => t,
        Err(ControlPlaneError::NotFound(_)) => return Ok(ColumnLookup::Missing),
        Err(e) => return Err(BindError::ControlPlane(e)),
    };
    let snap = match catalog.current_snapshot(&target_table).await {
        Ok(s) => s,
        Err(ControlPlaneError::NotFound(_)) => return Ok(ColumnLookup::Missing),
        Err(e) => return Err(BindError::ControlPlane(e)),
    };
    let schema = catalog.schema(&target_table, snap.id).await?;
    match schema.columns.iter().find(|c| c.name == col_name) {
        None => Ok(ColumnLookup::Missing),
        Some(col) => Ok(ColumnLookup::Present(resolve_logical(&col.ty))),
    }
}
```

- [ ] **Step 4: Rewrite `validate_derived` over the helper + typing unit**

Replace the body of `validate_derived` (keep its signature) with:

```rust
async fn validate_derived(
    catalog: &dyn Catalog,
    ontology: &dyn Ontology,
    links: &[LinkDef],
    d: &DerivedPropertyDef,
    violations: &mut Vec<BindViolation>,
) -> Result<(), BindError> {
    // 1. The named link must be defined (outbound) on this type.
    let Some(link) = links.iter().find(|l| l.name == d.link) else {
        violations.push(BindViolation {
            property: d.name.clone(),
            reason: BindViolationReason::UnknownDerivedLink(d.link.clone()),
        });
        return Ok(());
    };

    // 2. For column-bearing aggregations, resolve the target column and check the
    //    aggregation is applicable to its logical type. `Count` takes no column.
    let mut col_base: Option<BaseType> = None;
    if let Some(col_name) = d.agg.column() {
        match target_column_type(catalog, ontology, link, col_name).await? {
            ColumnLookup::Missing => {
                violations.push(BindViolation {
                    property: d.name.clone(),
                    reason: BindViolationReason::MissingAggColumn,
                });
                return Ok(());
            }
            ColumnLookup::Present(base) => {
                col_base = base;
                if !d.agg.column_applicable(col_base) {
                    // Collect-all: keep going to also report a result-type mismatch.
                    violations.push(BindViolation {
                        property: d.name.clone(),
                        reason: BindViolationReason::BadAggType {
                            agg: d.agg.label().to_string(),
                            column: col_name.to_string(),
                        },
                    });
                }
            }
        }
    }

    // 3. The declared result type must be a known logical type and consistent with the
    //    aggregation's result category. (Existence + category only; the full coercion
    //    lattice is deferred — see fut-coercion-taxonomy.)
    let declared = resolve_logical(&d.ty);
    let category = d.agg.result_expectation(col_base);
    if !category.accepts(declared) {
        violations.push(BindViolation {
            property: d.name.clone(),
            reason: BindViolationReason::BadDerivedResultType {
                declared: d.ty.clone(),
                expected: category.description(),
            },
        });
    }
    Ok(())
}
```

Cross-check the `MissingAggColumn`-then-return path: the OLD code `return Ok(())` on a
missing column (never reaching the result-type check). The new `ColumnLookup::Missing`
arm does the same `return Ok(())`. Good — behavior preserved.

- [ ] **Step 5: Run the guarding test**

Run: `buck2 test //src/services/ingest:bind-validation //src/services/ingest:bind > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS, unchanged from Step 1.

- [ ] **Step 6: Clippy-check the crate**

Run: `buck2 build '//src/services/ingest:ingest[clippy.txt]' > /tmp/c.log 2>&1; cat $(buck2 build --show-output '//src/services/ingest:ingest[clippy.txt]' 2>/dev/null | awk '{print $2}') 2>/dev/null; grep -E "error|warning" /tmp/c.log || echo "clippy clean"`
Expected: clean (empty `clippy.txt`).

- [ ] **Step 7: Commit**

```bash
git add src/services/ingest/src/bind.rs src/services/ingest/tests/bind_validation.rs
git commit -m "refactor(ingest): validate_derived over the core Aggregation typing unit"
```

---

### Task 4: Extract `bind`'s pure violation collectors + reuse `schema_of_table`

**Files:**
- Modify: `src/services/ingest/src/bind.rs` (rewrite `bind` lines 58–174; add pure
  `pub fn` collectors)
- Create: `src/services/ingest/tests/bind_structural.rs`
- Modify: `src/services/ingest/BUCK` (add `bind-structural` test target)
- Test guard: `src/services/ingest/tests/bind.rs` + `bind_validation.rs` (existing —
  must stay green)

**Interfaces:**
- Produces (`pub fn` in the `bind` module, so a sibling `tests/` file can call them):
  - `structural_violations(ty: &ObjectType, schema: &TableSchema) -> Vec<BindViolation>`
    — the three pure passes composed, in the original push order (property checks,
    then identity, then reserved names).
  - `property_violations(ty: &ObjectType, schema: &TableSchema) -> Vec<BindViolation>`
    — per-property MissingColumn / type / nullability checks (bind.rs lines 82–111).
  - `identity_violation(ty: &ObjectType) -> Option<BindViolation>` — the identity
    check (bind.rs lines 115–127).
  - `reserved_name_violations(ty: &ObjectType) -> Vec<BindViolation>` — the two `_`
    loops (properties then derived) as one iterator chain (bind.rs lines 132–147).
- Consumes: `schema_of_table` (already in the file, lines 387–397) for `bind`'s steps
  1–2.

- [ ] **Step 1: Write the failing unit test for the pure collectors**

Create `src/services/ingest/tests/bind_structural.rs`:

```rust
//! Pure, fake-free unit tests for `bind`'s structural violation collectors
//! (property type/nullability, identity, reserved names). No catalog/ontology —
//! just an `ObjectType` and a `TableSchema`.

use control_plane_core::{ColumnDef, ObjectType, PropertyDef, TableRef, TableSchema, TypeName};
use ingest::bind::{
    BindViolationReason, identity_violation, property_violations, reserved_name_violations,
    structural_violations,
};

fn col(name: &str, ty: &str, nullable: bool) -> ColumnDef {
    ColumnDef {
        order: 0,
        name: name.into(),
        ty: ty.into(),
        nullable,
    }
}

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
        constraints: control_plane_core::PropertyConstraints::default(),
    }
}

fn otype(properties: Vec<PropertyDef>, identity: Option<&str>) -> ObjectType {
    ObjectType {
        name: TypeName("T".into()),
        properties,
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "t".into(),
        },
        identity: identity.map(str::to_string),
    }
}

#[test]
fn property_missing_column_is_reported() {
    let ty = otype(vec![prop("id", "Long", true)], None);
    let schema = TableSchema { columns: vec![] };
    let v = property_violations(&ty, &schema);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].property, "id");
    assert_eq!(v[0].reason, BindViolationReason::MissingColumn);
}

#[test]
fn required_but_nullable_column_is_a_violation() {
    let ty = otype(vec![prop("id", "Long", true)], None);
    let schema = TableSchema {
        columns: vec![col("id", "long", true)],
    };
    let v = property_violations(&ty, &schema);
    assert!(v
        .iter()
        .any(|x| x.reason == BindViolationReason::NullabilityViolation));
}

#[test]
fn conforming_property_yields_no_violation() {
    let ty = otype(vec![prop("id", "Long", true)], None);
    let schema = TableSchema {
        columns: vec![col("id", "long", false)],
    };
    assert!(property_violations(&ty, &schema).is_empty());
}

#[test]
fn identity_naming_no_property_is_reported() {
    let ty = otype(vec![prop("id", "Long", true)], Some("missing"));
    let v = identity_violation(&ty).expect("violation");
    assert_eq!(v.property, "missing");
    assert!(matches!(v.reason, BindViolationReason::BadIdentity(_)));
}

#[test]
fn identity_naming_non_required_property_is_reported() {
    let ty = otype(vec![prop("id", "Long", false)], Some("id"));
    let v = identity_violation(&ty).expect("violation");
    assert!(matches!(v.reason, BindViolationReason::BadIdentity(_)));
}

#[test]
fn identity_naming_required_property_is_ok() {
    let ty = otype(vec![prop("id", "Long", true)], Some("id"));
    assert!(identity_violation(&ty).is_none());
}

#[test]
fn underscore_property_and_derived_names_are_reserved() {
    let ty = otype(vec![prop("_secret", "Long", true)], None);
    let v = reserved_name_violations(&ty);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].property, "_secret");
    assert_eq!(v[0].reason, BindViolationReason::ReservedName);
}

#[test]
fn structural_violations_composes_all_three_passes() {
    // A reserved property name AND a bad identity in one type.
    let ty = otype(vec![prop("_x", "Long", true)], Some("missing"));
    let schema = TableSchema {
        columns: vec![col("_x", "long", false)],
    };
    let v = structural_violations(&ty, &schema);
    assert!(v
        .iter()
        .any(|x| x.reason == BindViolationReason::ReservedName));
    assert!(v
        .iter()
        .any(|x| matches!(x.reason, BindViolationReason::BadIdentity(_))));
}
```

Wire the target in `src/services/ingest/BUCK` (mirror the `bind-validation` block, but
pure — deps are just `:ingest` + core):

```python
# Pure structural-violation collectors — no fakes, RE-eligible.
rust_test(
    name = "bind-structural",
    crate = "bind_structural",
    srcs = ["tests/bind_structural.rs"],
    crate_root = "tests/bind_structural.rs",
    edition = "2024",
    deps = [
        ":ingest",
        "//src/control-plane/core:core",
    ],
)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test //src/services/ingest:bind-structural > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|unresolved import|cannot find" /tmp/t.log`
Expected: FAIL — `unresolved import ingest::bind::property_violations`.

- [ ] **Step 3: Add the pure collectors and rewrite `bind`**

In `src/services/ingest/src/bind.rs`, add the three pure `pub fn`s (place them after
`bind`, before `agg_column`'s old location). `property_violations` is the loop from
lines 82–111 lifted verbatim (operating on `ty.properties` + `schema.columns`):

```rust
/// Per-property structural checks against the physical schema: a `MissingColumn`
/// (short-circuits), else an independent type check (`satisfies`) and nullability
/// check. A single property may yield up to two violations. Pure — no catalog.
#[must_use]
pub fn property_violations(ty: &ObjectType, schema: &TableSchema) -> Vec<BindViolation> {
    let mut violations = Vec::new();
    for p in &ty.properties {
        let Some(col) = schema.columns.iter().find(|c| c.name == p.name) else {
            violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::MissingColumn,
            });
            continue;
        };
        match satisfies(&p.ty, &col.ty) {
            Err(UnknownLogicalType(t)) => violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::UnknownLogicalType(t),
            }),
            Ok(false) => violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::TypeMismatch {
                    logical: p.ty.clone(),
                    physical: col.ty.clone(),
                },
            }),
            Ok(true) => {}
        }
        if p.required && col.nullable {
            violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::NullabilityViolation,
            });
        }
    }
    violations
}

/// The identity check: a declared identity must name a declared, required property
/// (a primary key cannot be nullable). `None` if there is no identity or it is valid.
#[must_use]
pub fn identity_violation(ty: &ObjectType) -> Option<BindViolation> {
    let id = ty.identity.as_ref()?;
    match ty.properties.iter().find(|p| &p.name == id) {
        None => Some(BindViolation {
            property: id.clone(),
            reason: BindViolationReason::BadIdentity("names no declared property".into()),
        }),
        Some(p) if !p.required => Some(BindViolation {
            property: id.clone(),
            reason: BindViolationReason::BadIdentity("names a non-required property".into()),
        }),
        Some(_) => None,
    }
}

/// Property and derived-property names beginning with `_` are reserved (the query
/// surface prefixes control params with `_`). Properties first, then derived — the
/// original push order.
#[must_use]
pub fn reserved_name_violations(ty: &ObjectType) -> Vec<BindViolation> {
    ty.properties
        .iter()
        .map(|p| p.name.as_str())
        .chain(ty.derived.iter().map(|d| d.name.as_str()))
        .filter(|name| name.starts_with('_'))
        .map(|name| BindViolation {
            property: name.to_string(),
            reason: BindViolationReason::ReservedName,
        })
        .collect()
}

/// The three pure structural passes composed, in the original bind push order:
/// property checks, then identity, then reserved names.
#[must_use]
pub fn structural_violations(ty: &ObjectType, schema: &TableSchema) -> Vec<BindViolation> {
    let mut violations = property_violations(ty, schema);
    violations.extend(identity_violation(ty));
    violations.extend(reserved_name_violations(ty));
    violations
}
```

Then rewrite `bind` (lines 58–174) so steps 1–2 reuse `schema_of_table` and step 3
delegates to `structural_violations`, leaving only the async derived pass inline:

```rust
pub async fn bind(
    catalog: &dyn Catalog,
    ontology: &dyn Ontology,
    type_def: ObjectType,
) -> Result<(), BindError> {
    // 1-2. The table must be live; take its physical schema at the current snapshot.
    let schema = schema_of_table(catalog, &type_def.table).await?;

    // 3. Pure structural validation (property type/nullability, identity, reserved
    //    names). Extra physical columns are fine — a type is a view over the table.
    let mut violations = structural_violations(&type_def, &schema);

    // 4. Derived properties: validate each against the ontology + the link target's
    //    physical schema (async — front-runs the read-time omission in query-api).
    if !type_def.derived.is_empty() {
        let links = match ontology.links(&type_def.name, PageReq::unbounded()).await {
            Ok(p) => p.items,
            Err(ControlPlaneError::NotFound(_)) => Vec::new(),
            Err(e) => return Err(BindError::ControlPlane(e)),
        };
        for d in &type_def.derived {
            validate_derived(catalog, ontology, &links, d, &mut violations).await?;
        }
    }

    if !violations.is_empty() {
        return Err(BindError::DoesNotConform(violations));
    }

    // 5. Persist — the type is now serveable by the governed read path.
    ontology.define_type(type_def).await?;
    Ok(())
}
```

Preserve the load-bearing doc comments from the original (the "type check and
nullability check are INDEPENDENT" note moves onto `property_violations`; the derived
front-runs note stays on the step-4 block). Confirm `schema_of_table` maps a missing
table to `BindError::TableNotFound` exactly as `bind`'s old inline steps 1–2 did — it
does (lines 387–397).

- [ ] **Step 4: Run test to verify it passes**

Run: `buck2 test //src/services/ingest:bind-structural //src/services/ingest:bind //src/services/ingest:bind-validation > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS across all three (the new unit tests + both existing integration
suites).

- [ ] **Step 5: Clippy-check**

Run: `buck2 build '//src/services/ingest:ingest[clippy.txt]' > /tmp/c.log 2>&1; grep -E "error|warning" /tmp/c.log || echo "clippy build ok"`
Expected: build succeeds, `clippy.txt` empty.

- [ ] **Step 6: Commit**

```bash
git add src/services/ingest/src/bind.rs src/services/ingest/tests/bind_structural.rs \
        src/services/ingest/BUCK
git commit -m "refactor(ingest): extract bind's pure structural violation collectors"
```

---

### Task 5: `gate::validate_values` → `AsArray` dispatch

**Files:**
- Modify: `src/services/ingest/src/gate.rs:102-190` (the five downcast loops)
- Test guard: `src/services/ingest/tests/gate.rs` (existing — must stay green)

**Interfaces:**
- No public signature change. Internal rewrite of the per-array dispatch only.

- [ ] **Step 1: Confirm the guarding test passes and covers each array type**

Run: `buck2 test //src/services/ingest:gate > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS. Read `tests/gate.rs` — confirm `validate_values` is exercised for a
string constraint (Utf8), an int64 range, and a float64 range. If a case is missing
(e.g. no LargeUtf8 or Int32 constraint test), add one now against the current code so
the rewrite is covered. At minimum add an Int32-column range test and a LargeUtf8
length test, since those two arms are easy to break in the rewrite.

- [ ] **Step 2: Rewrite the dispatch over `AsArray`**

In `src/services/ingest/src/gate.rs`, replace the `use arrow::array::{...}` import with:

```rust
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Int32Type, Int64Type, Float64Type, Schema};
```

Replace the `match array.data_type() { ... }` block (lines 122–174) with the
`AsArray` + `.iter().flatten()` form (`.flatten()` drops the `None`s = nulls, matching
the old `if !a.is_null(i)` guards):

```rust
            match array.data_type() {
                DataType::Utf8 => {
                    for s in array.as_string::<i32>().iter().flatten() {
                        validator.check_str(s, &mut cv);
                    }
                }
                DataType::LargeUtf8 => {
                    for s in array.as_string::<i64>().iter().flatten() {
                        validator.check_str(s, &mut cv);
                    }
                }
                DataType::Int32 => {
                    for v in array.as_primitive::<Int32Type>().iter().flatten() {
                        validator.check_num(f64::from(v), &mut cv);
                    }
                }
                DataType::Int64 => {
                    for v in array.as_primitive::<Int64Type>().iter().flatten() {
                        #[expect(
                            clippy::cast_precision_loss,
                            reason = "i64->f64 acceptable for range validation"
                        )]
                        let f = v as f64;
                        validator.check_num(f, &mut cv);
                    }
                }
                DataType::Float64 => {
                    for v in array.as_primitive::<Float64Type>().iter().flatten() {
                        validator.check_num(v, &mut cv);
                    }
                }
                _ => {}
            }
```

Notes:
- `as_string::<i32>()` / `as_string::<i64>()` and `as_primitive::<T>()` are `AsArray`
  methods; they *panic* on a type mismatch, but each is called only inside its matching
  `DataType` arm, so the type is guaranteed — this mirrors the old `downcast_ref`
  under the same arm (the old code silently skipped a failed downcast; here the arm
  guard makes the cast infallible). This is acceptable and clippy-clean (no
  `unwrap`/`expect`).
- `.iter()` on a `PrimitiveArray<Int32Type>` yields `Option<i32>`; `.flatten()` yields
  `i32`. On a `GenericStringArray` it yields `Option<&str>` → `&str`.

- [ ] **Step 3: Run test to verify it still passes**

Run: `buck2 test //src/services/ingest:gate > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS, unchanged.

- [ ] **Step 4: Clippy-check**

Run: `buck2 build '//src/services/ingest:ingest[clippy.txt]' > /tmp/c.log 2>&1; grep -E "error|warning" /tmp/c.log || echo "clippy build ok"`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add src/services/ingest/src/gate.rs src/services/ingest/tests/gate.rs
git commit -m "refactor(ingest): validate_values over AsArray dispatch"
```

---

### Task 6: `extract_pg` guard-clause inversion + `unpack_to_temp`/`publish` split

**Files:**
- Modify: `src/services/managed-postgres-embed/src/lib.rs:44-85`
- Test guard: `src/services/managed-postgres-embed/tests/extract.rs` (existing — extract
  + reuse idempotency; must stay green)

**Interfaces:**
- No public signature change. `extract_pg(cache_root: &Path) -> Result<ExtractedPg,
  EmbedError>` is unchanged. Two new private helpers:
  - `fn unpack_to_temp(cache_root: &Path) -> Result<PathBuf, EmbedError>` — creates a
    per-pid temp dir, unpacks the embedded tarball into it (stripping the wrapper
    component), returns the temp path.
  - `fn publish(tmp: &Path, final_dir: &Path) -> Result<(), EmbedError>` — the atomic
    `rename`, treating a lost race (final dir already exists) as success.

- [ ] **Step 1: Confirm the guarding test passes**

Run: `buck2 test //src/services/managed-postgres-embed:extract > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (extract-then-reuse idempotency).

- [ ] **Step 2: Rewrite with guard-clause inversion + split**

Replace `extract_pg` (lines 44–85) with:

```rust
pub fn extract_pg(cache_root: &Path) -> Result<ExtractedPg, EmbedError> {
    let final_dir = cache_root.join(format!("pg-{PG_VERSION}"));
    // Already extracted (the common path): reuse it, no unpack.
    if final_dir.exists() {
        return Ok(ExtractedPg {
            bin_dir: final_dir.join("bin"),
            lib_dir: final_dir.join("lib"),
        });
    }

    let tmp = unpack_to_temp(cache_root)?;
    publish(&tmp, &final_dir)?;

    Ok(ExtractedPg {
        bin_dir: final_dir.join("bin"),
        lib_dir: final_dir.join("lib"),
    })
}

/// Unpack the embedded PG distribution into a fresh per-pid temp dir under
/// `cache_root`, stripping the single wrapping `postgresql-<ver>-<triple>/`
/// component. Returns the temp dir; the caller publishes it atomically.
fn unpack_to_temp(cache_root: &Path) -> Result<PathBuf, EmbedError> {
    std::fs::create_dir_all(cache_root)?;
    let tmp = cache_root.join(format!("pg-{}.tmp.{}", PG_VERSION, std::process::id()));
    // Re-run-safe: clear any stale temp from a previous crashed pid-reuse.
    if tmp.exists() {
        std::fs::remove_dir_all(&tmp)?;
    }
    std::fs::create_dir_all(&tmp)?;

    let gz = flate2::read::GzDecoder::new(PG_TARBALL);
    let mut archive = tar::Archive::new(gz);
    archive.set_preserve_permissions(true);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        // Strip the single wrapping `postgresql-<ver>-<triple>/` component.
        let stripped: PathBuf = path.components().skip(1).collect();
        if stripped.as_os_str().is_empty() {
            continue;
        }
        // Trusted input: the tarball is sha256-pinned and compile-time-embedded, so the
        // per-entry unpack (which does not sanitize `..`) is safe here.
        entry.unpack(tmp.join(stripped))?;
    }
    Ok(tmp)
}

/// Publish `tmp` to `final_dir` atomically. If we lost a race (another process
/// already published a complete dir), discard ours and use theirs.
fn publish(tmp: &Path, final_dir: &Path) -> Result<(), EmbedError> {
    match std::fs::rename(tmp, final_dir) {
        Ok(()) => Ok(()),
        Err(_) if final_dir.exists() => {
            drop(std::fs::remove_dir_all(tmp));
            Ok(())
        }
        Err(e) => Err(EmbedError::Io(e)),
    }
}
```

The `ExtractedPg` construction would otherwise be duplicated across the reuse path and
the freshly-published path (both build it from `final_dir`). Factor the tiny helper and
return `Ok(extracted(&final_dir))` in both places:

```rust
fn extracted(final_dir: &Path) -> ExtractedPg {
    ExtractedPg {
        bin_dir: final_dir.join("bin"),
        lib_dir: final_dir.join("lib"),
    }
}
```

So `extract_pg`'s early-return becomes `return Ok(extracted(&final_dir));` and its final
line `Ok(extracted(&final_dir))`.

- [ ] **Step 3: Run the guarding test**

Run: `buck2 test //src/services/managed-postgres-embed:extract > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS, unchanged.

- [ ] **Step 4: Clippy-check**

Run: `buck2 build '//src/services/managed-postgres-embed:managed-postgres-embed[clippy.txt]' > /tmp/c.log 2>&1; grep -E "error|warning" /tmp/c.log || echo "clippy build ok"`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add src/services/managed-postgres-embed/src/lib.rs
git commit -m "refactor(embed): guard-clause invert extract_pg; split unpack/publish"
```

---

## Final Verification (after all tasks)

- [ ] **Build the touched crates (`-M none`):**

Run: `buck2 build -M none //src/control-plane/core:core //src/services/ingest:ingest //src/services/query-api:query-api //src/services/managed-postgres-embed:managed-postgres-embed > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED|error\[" /tmp/b.log`
Expected: BUILD SUCCEEDED.

- [ ] **Test the touched crates' pure-logic suites:**

Run: `buck2 test //src/control-plane/core/... //src/services/ingest/... //src/services/managed-postgres-embed:extract //src/services/managed-postgres-embed:embed-present > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS. (Fixture tests — e.g. `bind` if it boots pg, `boot-from-embedded` — run
local; if the cloud disk is tight, `buck2 clean` first and scope to the pure targets.)

- [ ] **Clippy over all first-party Rust:**

Run: `./tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -5 /tmp/clippy.log`
Expected: no findings.

- [ ] **Run the prek hooks and commit any fixups (markdown/format):**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; tail -20 /tmp/prek.log`
Then `git add -A && git commit` if hooks changed files.

- [ ] **Close the register item via loom-docs-update** (`- [ ]`→`- [x]`, terminal
  status, add `pr:#N`) in the same PR.
