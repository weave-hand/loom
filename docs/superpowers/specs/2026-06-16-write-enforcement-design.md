# Design: fine-grained write enforcement (slice 2 of fine-grained write governance)

> **Status:** approved design (2026-06-16). The service half of fine-grained write governance.
> Slice 1 ([PR #69](https://github.com/weave-hand/loom/pull/69), merged) made `acl.policy`
> action-scoped, so a subject can hold an independent `Write` policy (`row_filter` +
> `deny_columns`) on a type. This slice makes that `Write` policy **load-bearing**: `run_action`
> loads it and rejects an insert that sets a denied column or produces a row that fails the
> policy's `RowFilter`. The read path already enforces the symmetric read-side ACL (row-filter
> pushdown + column projection); this brings the `POST /actions/{name}` write front door to parity.

## Goal

After the existing coarse `check(Write)` gate, `run_action` enforces the **fine-grained** `Write`
policy against the concrete row it is about to insert:

- **deny-write-column** — reject the action if any parameter it sets is in the policy's
  `deny_columns`.
- **row-filter-on-insert** — reject the action if the row being inserted does not satisfy the
  policy's `row_filter`, evaluated purely in-memory (no SQL round-trip).

The write filter behaves **identically to the same filter on a read** (full type-coercion parity).
Enforcement is **fail-closed**: a row is inserted only if the filter is *definitely TRUE*.

## Scope

**In scope:**
- A new pure, I/O-free evaluator module `src/services/query-api/src/write_filter.rs`:
  `compare_cell` (typed leaf compare → three-valued `Option<bool>`), `eval` (SQL three-valued-logic
  tree walk), and `check_write_policy` (the gate, returning a `WriteVerdict`).
- Wiring it into `run_action` (`src/services/query-api/src/action.rs`): load
  `policies_for(subject, Action::Write, target)`, run the gate, map a denial to the existing
  `ActionError::Forbidden`.
- A pure-logic `rust_test` for the evaluator and a `loom_fixture_test` e2e proving write-policy
  enforcement end-to-end.

**NOT in scope (follow-ups / later):**
- **Structured denial detail.** The HTTP response stays the existing generic 403; *why* a write was
  denied (which column, or row-filter failure) is logged via `tracing` only. A structured 403 body
  is a deferred follow-up.
- **`mask_columns` on writes.** Masking is a read-render concept; a `Write` policy may carry
  `mask_columns` but this slice ignores it (as anticipated by the slice-1 follow-up).
- **Action-param ⟷ property-column conformance.** This slice assumes an action's parameter names
  are the target type's property names (so a `RowFilter.property` resolves to an inserted column
  by-name) — which holds today. Validating that conformance is a pre-existing deferred follow-up.
- **`Policy` / trait changes.** No control-plane change; this slice is purely a query-api consumer.

## Background: the type mismatch

The inserted row's cells are `serving::SqlValue` — `Text | Int | Bool | Double | Date | Timestamp |
Null` (`src/services/query-api/src/serving.rs`). A `RowFilter::Compare` leaf's operand is
`control_plane_core::ScalarValue` — `Text | Int | Bool | List` only (no `Double`/temporal, so the
type derives `Eq`). The evaluator must compare a `SqlValue` cell against a `ScalarValue` operand,
coercing the way SQL does so a write filter matches its read-side twin.

## Design

### 1. The pure evaluator — `src/services/query-api/src/write_filter.rs` (new)

Declared `pub mod write_filter;` in `lib.rs`. Auto-included by the library's
`glob(["src/**/*.rs"])`. Depends only on `control_plane_core` (`RowFilter`, `CompareOp`,
`ScalarValue`, `Policy`) and `crate::serving::SqlValue` + `time` (already library deps).

**`compare_cell(cell: &SqlValue, op: CompareOp, operand: &ScalarValue) -> Option<bool>`** — the
typed leaf compare. `Some(b)` = known truth value; `None` = unknown (fail-closed at the gate).

- `IsNull` → `Some(matches!(cell, SqlValue::Null))`; `IsNotNull` → its negation. (Operand ignored.)
- For every other op, a `SqlValue::Null` cell → `None` (SQL: `NULL <op> x` is UNKNOWN).
- `In` → three-valued OR of `compare_cell(cell, Eq, elem)` over the operand `List`'s elements
  (a non-`List` operand → `None`). `NotIn` → three-valued `Not` of the `In` result.
- `Eq` / `Ne` → `Ne` is the three-valued `Not` of `Eq`. `Eq` by cell type (below).
- Ordering ops `Lt | Le | Gt | Ge` and `Eq` resolve by **cell type**, coercing the operand:
  - `Text`   cell ↔ `Text`   operand → `str` `Ord` compare; else `None`.
  - `Int`    cell ↔ `Int`    operand → `i64` compare; else `None`.
  - `Double` cell ↔ `Int`    operand → compare as `f64` (operand cast to `f64`); else `None`.
  - `Bool`   cell ↔ `Bool`   operand → `Eq`/`Ne` only; any ordering op on bool → `None`.
  - `Date`   cell ↔ `Text`   operand → parse operand as ISO `YYYY-MM-DD`; on success `Date` `Ord`
    compare; parse failure or non-`Text` operand → `None`.
  - `Timestamp` cell ↔ `Text` operand → parse operand as ISO `YYYY-MM-DDTHH:MM:SS`; on success
    `PrimitiveDateTime` `Ord` compare; parse failure or non-`Text` operand → `None`.
  - Any other cell/operand pairing → `None`.

  (Date/Timestamp parsing reuses the exact `time` format descriptors already used by `params.rs`
  and `filter.rs`, so the write path accepts precisely the operands the read path does.)

**`eval(filter: &RowFilter, row: &BTreeMap<&str, &SqlValue>) -> Option<bool>`** — walk the tree with
**SQL three-valued logic**:

- `Compare { property, op, value }` → look up `property` in `row`; an **absent** key reads as an
  unset cell = `&SqlValue::Null` (so a filter on a column the action does not set is evaluated as
  NULL → fail-closed). Delegate to `compare_cell`.
- `Not(x)` → `None` if `eval(x)` is `None`, else `Some(!b)`.
- `And(xs)` → `Some(false)` if any child is `Some(false)`; else `None` if any child is `None`; else
  `Some(true)`. (Empty `And` → `Some(true)`.)
- `Or(xs)` → the dual: `Some(true)` if any `Some(true)`; else `None` if any `None`; else
  `Some(false)`. (Empty `Or` → `Some(false)`.)

**`check_write_policy(policies: &[Policy], columns: &[String], values: &[SqlValue]) -> WriteVerdict`**
— the gate. `pub enum WriteVerdict { Allow, DenyColumn(String), DenyRow }`.

1. **deny-column**: for each `col` in `columns`, if `col` is in the **union** of every policy's
   `deny_columns`, return `DenyColumn(col)`. (Checked first; an inserted denied column is rejected
   regardless of the row filter.)
2. **row-filter**: build the `row` map `name → &SqlValue` from `columns.iter().zip(values)`. The row
   is allowed iff **every** policy whose `row_filter` is `Some(f)` has `eval(f, &row) == Some(true)`.
   A policy with `row_filter: None` adds no row constraint. The first policy whose filter is not
   `Some(true)` → `DenyRow`. (Requiring each filter to be `Some(true)` is equivalent to the read
   side's single ANDed `WHERE` being TRUE — same allow set.)
3. Otherwise `Allow`.

`mask_columns` is never read here.

### 2. Wiring into `run_action` (`src/services/query-api/src/action.rs`)

The current flow (verbatim from the file): resolve action → resolve target → coarse
`check(subject, Write, target) == Deny → Forbidden` → `parse_params` → `insert_row` → best-effort
lineage → return the created object.

Insert one new step **after** `parse_params` (so `columns`/`values` exist) and **before**
`insert_row`:

```rust
// Fine-grained Write policy: deny-write-column + row-filter-on-insert.
let write_policies = deps
    .cp
    .acl()
    .policies_for(subject, Action::Write, &policy_target, PageReq::unbounded())
    .await?;
match write_filter::check_write_policy(&write_policies.items, &columns, &values) {
    WriteVerdict::Allow => {}
    WriteVerdict::DenyColumn(col) => {
        tracing::info!(action = action_name, column = %col, "write denied: policy denies column");
        return Err(ActionError::Forbidden);
    }
    WriteVerdict::DenyRow => {
        tracing::info!(action = action_name, "write denied: row fails write policy filter");
        return Err(ActionError::Forbidden);
    }
}
```

`policy_target` is the `PolicyTarget::Type(action.target.clone())` already built for the coarse
check. No new `ActionError` variant — both denials reuse `Forbidden` (same generic 403 as the coarse
gate). Imports added: `PageReq` (from `control_plane_core`) and `crate::write_filter::{self,
WriteVerdict}`.

### 3. Semantics locked in

- **Fail-closed**: a row is inserted only if its filter is *definitely TRUE*. Unknown — an unset
  column, a type mismatch, or a parse failure — denies. Mirrors "an UNKNOWN `WHERE` row is excluded
  from a read."
- **Multi-policy** (unioned across the subject's effective roles, as `policies_for` returns): AND
  the row_filters, union the deny_columns — exactly the read side's `load_policy`.
- **No Write policy** (only the coarse grant) → no fine-grained constraint; the insert proceeds.
  Mirrors read.
- **Read/Write independence**: a `Read` policy never affects a write (different keyed
  `acl.policy` row — slice 1), and vice-versa.

## File structure

- **Create:** `src/services/query-api/src/write_filter.rs` (the evaluator + gate).
- **Modify:** `src/services/query-api/src/lib.rs` (`pub mod write_filter;`).
- **Modify:** `src/services/query-api/src/action.rs` (load Write policy + gate + imports).
- **Create:** `src/services/query-api/tests/write_filter.rs` (pure `rust_test`).
- **Modify:** `src/services/query-api/tests/action_e2e.rs` (add a write-policy enforcement e2e).
- **Modify:** `src/services/query-api/BUCK` (a `rust_test` target `write-filter`; the existing
  `action-e2e` target already covers the modified e2e file).

## Testing

- **Pure `rust_test` `write-filter`** (`tests/write_filter.rs`, no fixture; deps `[":query-api",
  "//src/control-plane/core:core"]`, mirroring `filter-coerce`/`sql-compile`):
  - `compare_cell` coercion matrix: each op × each cell type, including `Double` vs `Int`,
    `Date`/`Timestamp` vs ISO-`Text` (parse hit and parse miss), `Bool` ordering → `None`, and
    cross-type mismatches → `None`.
  - NULL/unknown: `Null` cell under value ops → `None`; `IsNull`/`IsNotNull` truth; an absent
    property in the row map reads as `Null`.
  - Three-valued `And`/`Or`/`Not` (incl. `Some(false)` short-circuit beating `None`, and the
    empty-`And`/`Or` identities); `In`/`NotIn` membership incl. an unknown element.
  - `check_write_policy`: deny-column hit (single + union across policies), `DenyRow` when one of
    several policies' filters fails (AND), `Allow` when all pass, `Allow` with no policies, a
    `None`-`row_filter` policy adding no constraint.
- **`loom_fixture_test` e2e** (extend `tests/action_e2e.rs`, real Postgres + DuckDB): on the
  governed `Widget` type, set a `Write` policy with a `row_filter` (e.g. `name = "gadget"`) and a
  `deny_columns` (e.g. a column the action can set) — assert an action whose row violates the filter
  → `Forbidden` and writes nothing; an action setting a denied column → `Forbidden` and writes
  nothing; a conforming action → succeeds and reads back. Plus: a `Read`-only policy on the type
  does **not** block a write (independence). Reuse the existing fixture/scaffolding.
- **Full `buck2 test //src/...`** stays green — existing action e2e (no Write policy set) is
  unaffected: with no fine-grained Write policy, `check_write_policy` returns `Allow`.

## Decisions

- The evaluator is a **new pure module** (`write_filter.rs`), not folded into `filter.rs` (which
  coerces read query-param strings — a different job). Pure and fully unit-testable.
- **Full type-coercion parity** with the read side: numeric `Int↔Double`, temporal `Date`/
  `Timestamp` vs ISO-`Text`, reusing `params.rs`/`filter.rs` format descriptors.
- **Fail-closed, SQL three-valued logic**: allow iff the filter is `Some(true)`.
- **Deny reuses `ActionError::Forbidden`** (generic 403); the reason is `tracing`-logged only.
- `mask_columns` **ignored** for writes.
- **Deny-column counts only actually-set columns.** `parse_params` materializes an omitted optional
  parameter as an explicit `SqlValue::Null` pair, so `run_action` gates `check_write_policy` on the
  **non-null** parsed pairs — an omitted optional is not "setting" the column. The insert still
  writes the full pair list (NULL for omitted optionals). Row-filter results are unchanged by this:
  in the evaluator an absent property and a present-`Null` cell both resolve to a `Null` cell.

## Follow-ups (later slices)

- **Structured denial detail** — a 403 body (or header) naming the denied column / row-filter
  failure, instead of logs-only.
- **`mask_columns` for writes** — formally close out (expected: permanently ignored; masking is
  read-only).
- **Action-param ⟷ property-column conformance** — the pre-existing deferred follow-up; this slice
  relies on the by-name assumption it would validate.

## Roadmap

Completes **fine-grained write governance** (slice 2 of 2). With slice 1's action-scoped policies
and this slice's enforcement, loom's write front door (`POST /actions/{name}`) reaches parity with
the read-side ACL: a `Write` policy's `row_filter` constrains which rows an action may insert and its
`deny_columns` blocks columns an action may set — independent of the subject's read scope. Lands
under Step 3 governance hardening.
