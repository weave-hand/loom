# ACL Filter Robustness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A malformed `RowFilter` can never panic a read, and `set_policy` rejects malformed/invalid filters at write time — with one shared validator defining "well-formed."

**Architecture:** A pure `validate_row_filter` in `control-plane-core` is the single definition of filter validity (structural + optional property-existence). `compile_select` (query-api) becomes fallible and validates up front (→ HTTP 500, no panic). `set_policy` (both adapters) validates at write time: structural always; for a Type target, strict (the ontology type must exist) + leaf properties must exist on it. New `ControlPlaneError::Validation` variant.

**Tech Stack:** Rust, buck2, sqlx compile-time queries (postgres), hermetic Postgres via `PgFixture`, shared `control-plane-testkit` contract.

**Reference spec:** `docs/superpowers/specs/2026-06-11-acl-filter-robustness-design.md`.

**Conventions (do not deviate):**
- Tests: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...`. **Do NOT pipe `buck2 test` through `tail`** (stalls) — redirect to a file: `buck2 test ... > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL|error\[|panicked" /tmp/t.log`.
- **rustfmt is a check-only hook** — run `buck2 run //tools:rustfmt -- <files>` before each commit or it silently aborts; confirm the SHA changed.
- After changing any `query!`, run `tools/sqlx-prepare.sh` and commit `.sqlx/`; `//src/control-plane/postgres:sqlx-cache-check` gates it.
- Lint: `tools/clippy-all.sh` + `buck2 run //tools:prek -- run --all-files`. NO `--no-verify`.
- Conventional Commits; end each commit with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Never weaken a test assertion.

---

## File Structure

- `src/control-plane/core/src/acl.rs` — **Modify:** add `validate_row_filter`.
- `src/control-plane/core/src/error.rs` — **Modify:** add `ControlPlaneError::Validation`.
- `src/control-plane/core/src/lib.rs` — **Modify:** re-export `validate_row_filter`.
- `src/services/query-api/src/sql.rs` — **Modify:** `CompileError`; `compile_select -> Result`; validate up front.
- `src/services/query-api/src/handler.rs` — **Modify:** `QueryError::Malformed`; `compile_select(...)?`.
- `src/services/query-api/tests/sql_compile.rs` — **Modify:** `.unwrap()` the result; add a malformed-filter test.
- `src/control-plane/memory/src/acl.rs` — **Modify:** `set_policy` validates (reads ontology state for the type's properties).
- `src/control-plane/postgres/src/acl.rs` — **Modify:** `set_policy` validates (queries the `ontology` schema).
- `src/control-plane/postgres/.sqlx/` — **Regenerate.**
- `src/control-plane/testkit/src/lib.rs` — **Modify:** widen `acl_contract` bound to `Acl + Ontology`; define `Customer` before the policy round-trips; add validation assertions.

---

## Task 1: Core — `validate_row_filter` + `Validation` error variant

Pure, self-contained core additions. Nothing calls `validate_row_filter` yet (Tasks 2–3 do), so behavior elsewhere is unchanged; core unit tests cover it.

**Files:**
- Modify: `src/control-plane/core/src/acl.rs`, `src/control-plane/core/src/error.rs`, `src/control-plane/core/src/lib.rs`

- [ ] **Step 1: Add the `Validation` error variant.**

In `src/control-plane/core/src/error.rs`, add to the `ControlPlaneError` enum (after `Serialization`):
```rust
    /// A request or stored value failed validation (e.g. a malformed or
    /// invalid-property RowFilter at `set_policy`). Distinct from `NotFound`
    /// (missing entity) and `Conflict` (uniqueness/concurrency).
    #[error("validation error: {0}")]
    Validation(String),
```
(The enum is `#[non_exhaustive]`, so this is additive.)

- [ ] **Step 2: Write the failing `validate_row_filter` unit tests.**

In `src/control-plane/core/src/acl.rs`, inside the existing `#[cfg(test)] mod tests` block (it already has `row_filter_json_round_trips`), add:
```rust
    use std::collections::HashSet;

    fn props(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn validate_structural_ok_and_err() {
        // In/NotIn require a list; scalar ops require a non-list; IsNull ignores value.
        let in_list = RowFilter::Compare {
            property: "r".into(),
            op: CompareOp::In,
            value: ScalarValue::List(vec![ScalarValue::Text("EU".into())]),
        };
        assert!(validate_row_filter(&in_list, None).is_ok());

        let in_scalar = RowFilter::Compare {
            property: "r".into(),
            op: CompareOp::In,
            value: ScalarValue::Text("EU".into()),
        };
        assert!(validate_row_filter(&in_scalar, None).is_err());

        let eq_list = RowFilter::Compare {
            property: "r".into(),
            op: CompareOp::Eq,
            value: ScalarValue::List(vec![]),
        };
        assert!(validate_row_filter(&eq_list, None).is_err());

        let eq_scalar = RowFilter::Compare {
            property: "r".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        };
        assert!(validate_row_filter(&eq_scalar, None).is_ok());

        let is_null = RowFilter::Compare {
            property: "r".into(),
            op: CompareOp::IsNull,
            value: ScalarValue::List(vec![]), // ignored for IsNull
        };
        assert!(validate_row_filter(&is_null, None).is_ok());
    }

    #[test]
    fn validate_property_existence() {
        let f = RowFilter::Compare {
            property: "known".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        };
        assert!(validate_row_filter(&f, Some(&props(&["known", "other"]))).is_ok());
        assert!(validate_row_filter(&f, Some(&props(&["other"]))).is_err());
        // None = structural only, property not checked.
        assert!(validate_row_filter(&f, None).is_ok());
    }

    #[test]
    fn validate_recurses_into_and_or_not() {
        // A malformed leaf deep in the tree is caught.
        let bad = RowFilter::And(vec![
            RowFilter::Compare {
                property: "a".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Int(1),
            },
            RowFilter::Or(vec![RowFilter::Not(Box::new(RowFilter::Compare {
                property: "b".into(),
                op: CompareOp::In,
                value: ScalarValue::Int(2), // In with non-list -> err
            }))]),
        ]);
        assert!(validate_row_filter(&bad, None).is_err());
        // unknown property deep in the tree, with a property set.
        assert!(validate_row_filter(&bad, Some(&props(&["a", "b"]))).is_err()); // structural err first is fine
    }
```

- [ ] **Step 3: Run — verify it fails.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/core:core > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL to compile (`validate_row_filter` undefined).

- [ ] **Step 4: Implement `validate_row_filter`.**

In `src/control-plane/core/src/acl.rs`, add at module scope (after the `Policy` struct / before the trait), and `use std::collections::HashSet;` at the top of the file if not present:
```rust
/// Validate a [`RowFilter`]'s well-formedness. Structural rules are always enforced;
/// when `properties` is `Some`, every `Compare` leaf's `property` must be a member.
/// Returns a human-readable reason on the first failure.
///
/// Structural (the CompareOp <-> ScalarValue invariant):
/// - `In` / `NotIn`         => value MUST be `ScalarValue::List`
/// - `Eq/Ne/Lt/Le/Gt/Ge`    => value must NOT be a `ScalarValue::List`
/// - `IsNull` / `IsNotNull`  => value ignored
pub fn validate_row_filter(
    f: &RowFilter,
    properties: Option<&HashSet<String>>,
) -> std::result::Result<(), String> {
    match f {
        RowFilter::Compare {
            property,
            op,
            value,
        } => {
            if let Some(props) = properties {
                if !props.contains(property) {
                    return Err(format!("unknown property: {property}"));
                }
            }
            match op {
                CompareOp::In | CompareOp::NotIn => {
                    if !matches!(value, ScalarValue::List(_)) {
                        return Err(format!("{op:?} requires a list value"));
                    }
                }
                CompareOp::IsNull | CompareOp::IsNotNull => {}
                _ => {
                    if matches!(value, ScalarValue::List(_)) {
                        return Err(format!("{op:?} requires a non-list value"));
                    }
                }
            }
            Ok(())
        }
        RowFilter::And(xs) | RowFilter::Or(xs) => {
            for x in xs {
                validate_row_filter(x, properties)?;
            }
            Ok(())
        }
        RowFilter::Not(x) => validate_row_filter(x, properties),
    }
}
```
(Return type uses `std::result::Result<(), String>` to avoid clashing with the crate's `Result<T>` alias.)

- [ ] **Step 5: Re-export from lib.rs.**

In `src/control-plane/core/src/lib.rs`, add `validate_row_filter` to the `pub use acl::{...}` list.

- [ ] **Step 6: Run — verify pass.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/core:core > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 7: Commit.**
```bash
buck2 run //tools:rustfmt -- src/control-plane/core/src/acl.rs src/control-plane/core/src/error.rs src/control-plane/core/src/lib.rs
git add -A
git commit -m "feat(core): validate_row_filter + ControlPlaneError::Validation

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Read backstop — fallible `compile_select`

`compile_select` returns `Result` and validates each filter up front, so the previously panicking arms are unreachable; `read_object` surfaces a malformed filter as a 500 (no panic).

**Files:**
- Modify: `src/services/query-api/src/sql.rs`, `src/services/query-api/src/handler.rs`, `src/services/query-api/tests/sql_compile.rs`

- [ ] **Step 1: Update sql_compile call sites + add the failing malformed test.**

In `src/services/query-api/tests/sql_compile.rs`:
- Every `let (sql, params) = compile_select(...)` / `let (sql, _params) = compile_select(...)` becomes the same with `.unwrap()` appended (the function now returns `Result`). The asserted SQL strings are unchanged.
- Add:
```rust
use control_plane_core::{CompareOp, RowFilter, ScalarValue};
// (CompareOp/RowFilter/ScalarValue are already imported at the top; don't duplicate.)

#[test]
fn malformed_filter_is_an_error_not_a_panic() {
    // `In` with a non-list value is malformed; compile_select must return Err, not panic.
    let bad = RowFilter::Compare {
        property: "region".into(),
        op: CompareOp::In,
        value: ScalarValue::Text("EU".into()),
    };
    let res = compile_select(&t(), &["id".into()], &[], std::slice::from_ref(&bad), &[], 10);
    assert!(res.is_err(), "malformed filter -> Err(CompileError), no panic");
}
```

- [ ] **Step 2: Run — verify it fails.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/services/query-api:sql-compile > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL to compile (compile_select returns a tuple, not Result; `.unwrap()`/`.is_err()` invalid).

- [ ] **Step 3: Make `compile_select` fallible.**

In `src/services/query-api/src/sql.rs`:
- Add to the imports: `use control_plane_core::validate_row_filter;`
- Add the error type (after `MASK_MARKER`):
```rust
/// A row filter that violated the CompareOp<->ScalarValue invariant (e.g. malformed
/// persisted policy data). Surfaced by the query API as an opaque 500, never a panic.
#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("malformed row filter: {0}")]
    MalformedFilter(String),
}
```
- Remove the long `// NOTE: the panic!/unreachable!...` comment block above `filter_sql` (the fallibility it described is now implemented). Keep the `scalar`/`op_sql`/`filter_sql` `unreachable!` arms but update their messages to note the input is pre-validated, e.g. `unreachable!("validated by validate_row_filter (In/NotIn handled above)")`, and the `panic!("In/NotIn requires a list value")` becomes `unreachable!("validated by validate_row_filter")`.
- Change `compile_select` to validate up front and return `Result`:
```rust
pub fn compile_select(
    table: &TableRef,
    allowed_cols: &[String],
    mask_cols: &[String],
    row_filters: &[RowFilter],
    eq_filters: &[(String, SqlValue)],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    // Validate every ACL filter's shape up front; after this the SQL-building arms
    // below cannot hit a CompareOp<->ScalarValue mismatch.
    for f in row_filters {
        validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
    }
    let mut params = Vec::new();
    // ... (existing body unchanged: build cols, from, conjuncts, sql) ...
    Ok((sql, params))
}
```
(The body that builds `cols`/`from`/`conjuncts`/`sql` is unchanged except wrapping the final return in `Ok((sql, params))`.)

- [ ] **Step 4: Thread the error through the handler.**

In `src/services/query-api/src/handler.rs`:
- Add a `QueryError` variant:
```rust
    #[error(transparent)]
    Malformed(#[from] crate::sql::CompileError),
```
- Change the `compile_select` call to propagate: `let (sql, params) = compile_select(&object_type.table, &allowed, &mask_cols, &row_filters, &q.eq_filters, DEFAULT_LIMIT)?;`
- `http.rs` needs no change: its `match` has explicit arms for `UnknownType`/`Forbidden`/`BadFilter` and a catch-all `Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error")`, which now also covers `Malformed` (a server-side data-integrity fault → opaque 500). Confirm that catch-all exists; if `http.rs` matches QueryError variants exhaustively instead, add a `QueryError::Malformed(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()` arm.

- [ ] **Step 5: Run — verify pass.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/services/query-api:sql-compile //src/services/query-api:governed-read //src/services/query-api:http-smoke > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (sql-compile incl. the new malformed test; governed-read + http-smoke still pass — their filters are well-formed and `compile_select(...)?` propagates fine).

- [ ] **Step 6: Commit.**
```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/sql.rs src/services/query-api/src/handler.rs src/services/query-api/tests/sql_compile.rs
git add -A
git commit -m "feat(query-api): fallible compile_select (malformed filter -> 500, no panic)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Write validation — `set_policy` (both adapters) + contract

`set_policy` validates the filter at write time; the shared contract proves it on both adapters. Because validation is now strict, the contract's existing Type-target policy round-trips must define their type first.

**Files:**
- Modify: `src/control-plane/memory/src/acl.rs`, `src/control-plane/postgres/src/acl.rs`, `src/control-plane/testkit/src/lib.rs`
- Regenerate: `src/control-plane/postgres/.sqlx/`

- [ ] **Step 1: Widen the contract, define the type, add the failing validation assertions.**

In `src/control-plane/testkit/src/lib.rs`:
- Widen the contract bound: `pub async fn acl_contract<A: Acl + Ontology>(a: &A) {`. Ensure `Ontology`, `ObjectType`, `PropertyDef`, `TypeName`, `TableRef` are imported in the file (the testkit already exercises ontology elsewhere; add to the `use control_plane_core::{...}` if missing).
- **Before** the existing `// --- policies: nested filter + deny columns round-trip ---` block (the `let filter = ...; let pol = Policy { target: ttype("Customer"), ... }` around line 795), define the `Customer` type with the properties its filters use (`tenant`, `is_public`, `owner`, `region`, `active`):
```rust
    // Define the Customer type so the strict Type-target policy validation below passes.
    a.define_type(ObjectType {
        name: TypeName("Customer".into()),
        properties: ["tenant", "is_public", "owner", "region", "active"]
            .iter()
            .map(|n| PropertyDef { name: (*n).into(), ty: "String".into(), required: false })
            .collect(),
        table: TableRef { schema: "main".into(), name: "customer".into() },
    })
    .await
    .expect("define Customer type");
```
- After the existing policy round-trips (near where the role-hierarchy/mask blocks were appended — find the end region), add the validation assertions:
```rust
    // --- set_policy write-time validation ---
    // malformed filter (In + scalar value) on a defined type -> Validation
    let malformed = Policy {
        target: ttype("Customer"),
        row_filter: Some(RowFilter::Compare {
            property: "region".into(),
            op: CompareOp::In,
            value: ScalarValue::Text("EU".into()),
        }),
        deny_columns: vec![],
        mask_columns: vec![],
    };
    assert!(matches!(
        a.set_policy(&rid("reader"), malformed).await,
        Err(ControlPlaneError::Validation(_)),
    ));
    // unknown leaf property on a defined type -> Validation
    let unknown_prop = Policy {
        target: ttype("Customer"),
        row_filter: Some(RowFilter::Compare {
            property: "not_a_property".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        }),
        deny_columns: vec![],
        mask_columns: vec![],
    };
    assert!(matches!(
        a.set_policy(&rid("reader"), unknown_prop).await,
        Err(ControlPlaneError::Validation(_)),
    ));
    // a filter on an UNDEFINED type -> Validation (strict)
    let undefined_type = Policy {
        target: ttype("NoSuchType"),
        row_filter: Some(RowFilter::Compare {
            property: "x".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        }),
        deny_columns: vec![],
        mask_columns: vec![],
    };
    assert!(matches!(
        a.set_policy(&rid("reader"), undefined_type).await,
        Err(ControlPlaneError::Validation(_)),
    ));
    // well-formed filter on a Table target -> Ok (structural only, no property check)
    let table_ok = Policy {
        target: ttable("main", "raw"),
        row_filter: Some(RowFilter::Compare {
            property: "anything".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        }),
        deny_columns: vec![],
        mask_columns: vec![],
    };
    a.set_policy(&rid("reader"), table_ok).await.expect("table-target structural ok");
```
(Read the contract first to confirm `ttable` exists and `reader` is defined/usable at that point — it is, used throughout.)

- [ ] **Step 2: Run — verify it fails.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:acl //src/control-plane/postgres:acl > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: FAIL — `set_policy` doesn't validate yet (the malformed/unknown/undefined assertions expecting `Validation` fail; and the existing Customer round-trips may now even succeed-without-validation, but the new asserts drive the failure).

- [ ] **Step 3: Memory — validate in `set_policy`.**

In `src/control-plane/memory/src/acl.rs`, add a private helper to read the ontology type's properties from the in-memory ontology state (match the actual field name for the ontology store — likely `self.ontology` holding object types; read the file's `Ontology` impl to find how `get_type`/`define_type` access it):
```rust
    /// Property names of a defined ontology type, or `None` if the type is undefined.
    fn type_properties(&self, name: &str) -> Option<Vec<String>> {
        let ont = self.ontology.lock().unwrap();
        ont.types
            .get(name)
            .map(|t| t.properties.iter().map(|p| p.name.clone()).collect())
    }
```
(Adapt `self.ontology` / `ont.types` / the stored type shape to the real memory ontology state — the file's `define_type`/`get_type` show the exact field/struct.)

Then in `set_policy`, after the role-exists check and before the insert:
```rust
        if let Some(f) = &policy.row_filter {
            match &policy.target {
                PolicyTarget::Type(name) => {
                    let props = self
                        .type_properties(&name.0)
                        .ok_or_else(|| ControlPlaneError::Validation(
                            format!("policy references unknown type {}", name.0),
                        ))?;
                    let set: std::collections::HashSet<String> = props.into_iter().collect();
                    validate_row_filter(f, Some(&set)).map_err(ControlPlaneError::Validation)?;
                }
                PolicyTarget::Table(_) => {
                    validate_row_filter(f, None).map_err(ControlPlaneError::Validation)?;
                }
            }
        }
```
Add `validate_row_filter` (and `PolicyTarget` if not already) to the `use control_plane_core::{...}` imports. NOTE the borrow: `set_policy` takes `policy: Policy` by value; reference its fields (`&policy.row_filter`, `&policy.target`) before the `acl.policies.insert(key, policy)` move — keep the validation block above the insert. Also ensure you don't hold the `acl` lock while taking the `ontology` lock in a way that deadlocks: do the ontology read (`type_properties`) BEFORE locking `acl` for the insert, or use separate short locks. Simplest: run the whole validation block (which locks `ontology` via `type_properties`) before `let mut acl = self.acl.lock()...`. Restructure `set_policy` so role-existence + validation happen, then the insert under the `acl` lock — and DO NOT hold `acl` while calling `type_properties` (which locks `ontology`).

- [ ] **Step 4: Postgres — validate in `set_policy`.**

In `src/control-plane/postgres/src/acl.rs` `set_policy`, after the role-exists `NotFound` check and before the insert, add (using `control_plane_core::validate_row_filter` + `PolicyTarget`):
```rust
        if let Some(f) = &policy.row_filter {
            match &policy.target {
                PolicyTarget::Type(name) => {
                    let type_exists = sqlx::query_scalar!(
                        "select exists (select 1 from ontology.object_type where name = $1)",
                        &name.0,
                    )
                    .fetch_one(&self.pool)
                    .await
                    .map_err(backend)?
                    .unwrap_or(false);
                    if !type_exists {
                        return Err(ControlPlaneError::Validation(format!(
                            "policy references unknown type {}",
                            name.0
                        )));
                    }
                    let names = sqlx::query_scalar!(
                        "select name from ontology.property where type_name = $1",
                        &name.0,
                    )
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                    let set: std::collections::HashSet<String> = names.into_iter().collect();
                    validate_row_filter(f, Some(&set)).map_err(ControlPlaneError::Validation)?;
                }
                PolicyTarget::Table(_) => {
                    validate_row_filter(f, None).map_err(ControlPlaneError::Validation)?;
                }
            }
        }
```
Add `validate_row_filter` to the `use control_plane_core::{...}` imports (`PolicyTarget` is already imported). Place this BEFORE the existing `insert into acl.policy ...` query.

- [ ] **Step 5: Regenerate sqlx + run.**

Run `tools/sqlx-prepare.sh` (the two new ontology queries enter the cache). Then:
`env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:acl //src/control-plane/postgres:acl > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS on both (the new validation assertions + the existing Customer round-trips now that Customer is defined). Then the broad sweep:
`env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/... > /tmp/s.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL" /tmp/s.log`
Expected: all PASS (governed-read's Order policy already defines Order before set_policy → its filter validates; deny-override/masking/hierarchy assertions unaffected).

- [ ] **Step 6: Commit.**
```bash
buck2 run //tools:rustfmt -- src/control-plane/memory/src/acl.rs src/control-plane/postgres/src/acl.rs src/control-plane/testkit/src/lib.rs
git add -A
git commit -m "feat(acl): set_policy validates the RowFilter (structural + type/property)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Full sweep, lint, final review, finish branch

**Files:** none (verification + finish).

- [ ] **Step 1: Full sweep.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/... > /tmp/sweep.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL" /tmp/sweep.log`
Expected: all PASS (core validate_row_filter tests, sql-compile malformed test, acl contract validation on both adapters, governed-read/http-smoke, sqlx-cache-check, everything else).

- [ ] **Step 2: Lint.**

Run:
```bash
tools/clippy-all.sh > /tmp/clippy.log 2>&1; echo "clippy warnings: $(grep -cE 'warning|error' /tmp/clippy.log)"
buck2 run //tools:prek -- run --all-files
```
Expected: clippy 0 warnings; prek all PASS.

- [ ] **Step 3: Confirm against the spec.** Spot-check: shared `validate_row_filter` (structural + property) used by BOTH `compile_select` and `set_policy`; `compile_select` fallible → `Malformed` → opaque 500, no panic path; `ControlPlaneError::Validation`; `set_policy` structural-always + strict Type (unknown type → Validation) + property existence + Table structural-only; the testkit defines `Customer` so existing round-trips pass. Out-of-scope (value-type matching, grant target validation, Table column existence) not implemented — correct.

- [ ] **Step 4: Finish the branch.** Use superpowers:finishing-a-development-branch (verify tests pass → present options). Branch: `feat/acl-filter-robustness` (already carries the spec commit).

---

## Self-review notes

- **Spec coverage:** `validate_row_filter` in core (Task 1) · `ControlPlaneError::Validation` (Task 1) · fallible `compile_select` + `Malformed`→500 (Task 2) · `set_policy` structural + strict-Type + property + Table-structural-only on both adapters (Task 3) · contract validation cases + the strict-fallout fix (define Customer) (Task 3) · core unit tests + sql malformed test + contract cases (Tasks 1–3). Non-goals (value-type matching, grant target validation, Table column existence, non-set_policy filters) untasked — correct.
- **Type consistency:** `validate_row_filter(&RowFilter, Option<&HashSet<String>>) -> Result<(), String>` identical in core/compile_select/both adapters; `CompileError::MalformedFilter(String)` + `QueryError::Malformed(#[from] CompileError)`; `ControlPlaneError::Validation(String)`; `compile_select` now returns `Result<(String, Vec<SqlValue>), CompileError>` — all 9 sql-compile call sites + the 1 handler call site updated (`.unwrap()` / `?`).
- **Strict-fallout (the load-bearing migration detail):** the testkit contract sets row-filter policies on `ttype("Customer")` (`pol` uses tenant/is_public/owner/region; `pol_w` uses active). Task 3 Step 1 defines `Customer` with exactly those properties BEFORE those round-trips, so they pass strict validation. `pol2` (None filter) and the Invoice mask policy (None filter) skip validation. The governed-read oracle defines `Order` before its `status` filter — already fine.
- **Lock-ordering (memory):** `set_policy` must read the ontology lock (`type_properties`) and the acl lock at different times — do validation (ontology read) before taking the acl lock for the insert, never nested, to avoid deadlock.
- **sqlx regen:** Task 3 adds two `ontology` queries to postgres `set_policy` — run `tools/sqlx-prepare.sh` and commit `.sqlx/`, or `sqlx-cache-check` fails. Tasks 1–2 touch no SQL macros.
- **http.rs:** the `Malformed` variant maps to the existing opaque-500 catch-all; only add an explicit arm if `http.rs` matches QueryError exhaustively (it uses a catch-all today).
