# Object-Set Inputs + Reserved Control-Param Namespace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `?_ids=` query param that scopes a read to a set of source objects by their declared identity, and migrate all control params to a reserved `_` prefix guarded by a bind-time check so they can never collide with column names.

**Architecture:** `?_ids=` lowers, in the handler, to an `In` `CallerPredicate` on the source type's identity column (shared `identity_in_predicate`), wired into `read_object` and the shared `resolve_chain` so plain reads, traversals, and association all get it. All control params (`_path`/`_direction`/`_shape`/`_ids`) use a `_` prefix; bind rejects `_`-prefixed property/derived names, making collisions impossible by construction.

**Tech Stack:** Rust, buck2, axum HTTP, DuckDB serving engine, hermetic Postgres/DuckDB fixture tests.

**Design:** `docs/superpowers/specs/2026-06-18-object-set-inputs-design.md`

## Global Constraints

- Never run two `buck2` commands concurrently. One at a time.
- Never pipe `buck2 test` through `tail`/`head` — redirect and grep:
  `buck2 test //target > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|error\[|panicked" /tmp/t.log`. Fixture tests boot hermetic Postgres+DuckDB and take minutes — allow up to 600000ms.
- Tests are integration `rust_test`/`loom_fixture_test` targets only — never inline `#[test]` in `src/**`.
- If the rustfmt pre-commit hook fails, run `buck2 run //tools:rustfmt -- <files>`, re-stage, re-commit.
- Control params use a `_` prefix verbatim: `_path`, `_direction`, `_shape`, `_ids`.
- Property names equal physical column names; an identity property name is used directly as a SQL column identifier.
- Commit trailer: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

---

## Task 1: Ontology reserved-name guard at bind

**Files:**
- Modify: `src/services/ingest/src/bind.rs` (`ReservedName` variant + `_`-prefix check)
- Modify: `src/services/ingest/tests/bind.rs` (guard matrix)

**Interfaces:**
- Produces: `BindViolationReason::ReservedName` — raised when a property or derived-property name begins with `_`.

- [ ] **Step 1: Write the failing tests**

In `src/services/ingest/tests/bind.rs`, add (mirror the file's existing harness — it lands a table and calls `bind`; reuse its fixture/landing helpers and a conforming base `ObjectType`):

```rust
#[tokio::test]
async fn bind_rejects_a_property_name_starting_with_underscore() {
    // ... land a table that also has a physical column named "_x" (so the only issue is
    // the reserved name, not a missing column) ...
    // build an ObjectType whose properties include PropertyDef { name: "_x".into(),
    //   ty: "long".into(), required: false }, identity: None
    // assert bind(...).await is Err(BindError::DoesNotConform(v)) where v contains a
    //   BindViolation { property: "_x", reason: BindViolationReason::ReservedName }
}

#[tokio::test]
async fn bind_rejects_a_derived_property_name_starting_with_underscore() {
    // base conforming type, but derived: vec![DerivedPropertyDef { name: "_y".into(),
    //   ty: "long".into(), link: "whatever".into(), agg: Aggregation::Count }]
    // assert a BindViolationReason::ReservedName violation for property "_y"
}
```

Use `matches!(v.reason, BindViolationReason::ReservedName)` and check `v.property`. Adapt the table/landing helpers to the file's existing ones (the `_x` test needs the physical column to exist so the only violation is the reserved name; or assert the violation set *contains* a `ReservedName` for `_x` alongside any MissingColumn — simplest is to make the column exist).

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/ingest:bind > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[|panicked|ReservedName" /tmp/t.log`
Expected: FAIL to compile (`ReservedName` undefined).

- [ ] **Step 3: Add the variant**

In `src/services/ingest/src/bind.rs`, add to `BindViolationReason`:

```rust
    ReservedName, // a property/derived name begins with `_`, reserved for control params
```

- [ ] **Step 4: Add the check**

In `bind`, after the existing property-conformance loop and before the identity check (or right after it — order does not matter, both append to `violations`), add:

```rust
    // Property and derived-property names beginning with `_` are reserved: the query
    // surface prefixes control params with `_` (e.g. `_ids`, `_path`), so a `_`-named
    // property would be unaddressable as a filter and could shadow a control param.
    for p in &type_def.properties {
        if p.name.starts_with('_') {
            violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::ReservedName,
            });
        }
    }
    for d in &type_def.derived {
        if d.name.starts_with('_') {
            violations.push(BindViolation {
                property: d.name.clone(),
                reason: BindViolationReason::ReservedName,
            });
        }
    }
```

- [ ] **Step 5: Run to verify pass**

Run: `buck2 test //src/services/ingest:bind > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t.log`
Expected: PASS (all bind tests, including the two new ones).

- [ ] **Step 6: Clippy + commit**

Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.

```bash
git add src/services/ingest/ docs/superpowers/plans/2026-06-18-object-set-inputs.md
git commit -m "feat(ingest): reject _-prefixed property/derived names at bind

The query surface reserves a _ prefix for control params (_ids, _path, …); a
_-prefixed property would shadow one and be unfilterable. New
BindViolationReason::ReservedName, collected like the other violations.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Migrate control params to the `_` prefix

**Files:**
- Modify: `src/services/query-api/src/http.rs` (key-match strings)
- Modify: `src/services/query-api/tests/association_e2e.rs` (query strings)

**Interfaces:**
- Produces: HTTP control-param keys are now `_path`, `_direction`, `_shape` (and `_ids` is added in Task 3).

- [ ] **Step 1: Rename the key matches in `http.rs`**

In `src/services/query-api/src/http.rs`, `get_linked`'s param loop — change the match arms:

```rust
    for (k, v) in params {
        match k.as_str() {
            "_direction" => direction_raw = Some(v),
            "_shape" => shape = Some(v),
            _ => filter_params.push((k, v)),
        }
    }
```

And `get_linked_chain`'s param loop:

```rust
    for (k, v) in params {
        match k.as_str() {
            "_path" => hops = parse_path_hops(&v),
            "_shape" => shape = Some(v),
            _ => filter_params.push((k, v)),
        }
    }
```

(These are the only two control-param loops; `get_object` has none yet — it gets `_ids` in Task 3.)

- [ ] **Step 2: Update the e2e query strings**

In `src/services/query-api/tests/association_e2e.rs`, replace every `?shape=` with `?_shape=` and every `path=` (in a query string) with `_path=`. The occurrences (verify with `grep -nE "shape=|path=" src/services/query-api/tests/association_e2e.rs`):

- `"/objects/Customer/links/orders?shape=association"` → `?_shape=association`
- `"/objects/Customer/links?path=orders,lineItems&shape=association"` → `?_path=orders,lineItems&_shape=association` (both occurrences)
- `"/objects/Customer/links/directItems?shape=association"` → `?_shape=association`
- `"/objects/Region/links/regionOrders?shape=association"` → `?_shape=association`
- `"/objects/Customer/links/asRegion?shape=association"` → `?_shape=association`

(Use `grep` to find them all; replace each `?shape=`→`?_shape=`, `?path=`→`?_path=`, `&shape=`→`&_shape=`.)

- [ ] **Step 3: Run the query-api suite to confirm the migration is consistent**

Run: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked|error\[" /tmp/t.log`
Expected: all pass. (The handler-driven traversal e2es — `multi_hop_traversal_e2e.rs`, `inverse_hops_e2e.rs` — construct `ChainQuery` in Rust and use no query keys, so they are unaffected; `path_parse.rs` tests the value parsers directly and is unaffected. If `association_e2e` fails, a query string was missed in Step 2.)

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api/
git commit -m "refactor(query): reserve a _ prefix for control params

Migrate path/direction/shape query keys to _path/_direction/_shape so control
params live in a reserved namespace that cannot collide with bare column filter
keys. Value parsers unchanged; association e2e query strings updated.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Object-set input (`?_ids=`)

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (`ids` fields, `identity_in_predicate`, wiring)
- Modify: `src/services/query-api/src/http.rs` (`_ids` parsing on all read routes; `NoIdentity` arm on `get_object`)
- Create: `src/services/query-api/tests/identity_in_predicate.rs` (unit) + BUCK target
- Create: `src/services/query-api/tests/object_set_e2e.rs` (e2e) + BUCK target

**Interfaces:**
- Consumes: `ObjectType.identity` (prior slice), `CallerPredicate`/`coerce_filter` (`crate::filter`), `project_allowed`, `resolve_chain`, `QueryError::{NoIdentity, BadFilter}`.
- Produces:
  - `fn identity_in_predicate(otype: &ObjectType, denied: &HashSet<String>, masked: &HashSet<String>, ids: &[String]) -> Result<Option<crate::filter::CallerPredicate>, QueryError>`
  - `ObjectQuery.ids: Vec<String>` and `ChainQuery.ids: Vec<String>`.

- [ ] **Step 1: Write the helper unit test (failing)**

Create `src/services/query-api/tests/identity_in_predicate.rs`:

```rust
//! identity_in_predicate lowers a set of object ids to an In predicate on the type's
//! declared identity column, governed like any caller filter.

use std::collections::HashSet;

use control_plane_core::{CompareOp, ObjectType, PropertyDef, TableRef, TypeName};
use query_api::handler::{QueryError, identity_in_predicate};
use query_api::serving::SqlValue;

fn customer(identity: Option<String>) -> ObjectType {
    ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![
            PropertyDef { name: "id".into(), ty: "long".into(), required: true },
            PropertyDef { name: "name".into(), ty: "string".into(), required: false },
        ],
        derived: vec![],
        table: TableRef { schema: "main".into(), name: "customer".into() },
        identity,
    }
}

fn empty() -> HashSet<String> {
    HashSet::new()
}

#[test]
fn lowers_ids_to_an_in_predicate_on_identity() {
    let pred = identity_in_predicate(
        &customer(Some("id".into())),
        &empty(),
        &empty(),
        &["1".to_string(), "2".to_string()],
    )
    .unwrap()
    .expect("a predicate for a non-empty id set");
    assert_eq!(pred.column, "id");
    assert_eq!(pred.op, CompareOp::In);
    assert_eq!(pred.values, vec![SqlValue::Int(1), SqlValue::Int(2)]);
}

#[test]
fn empty_ids_yields_no_predicate() {
    let pred = identity_in_predicate(&customer(Some("id".into())), &empty(), &empty(), &[]).unwrap();
    assert!(pred.is_none());
}

#[test]
fn no_declared_identity_is_no_identity_error() {
    let err = identity_in_predicate(&customer(None), &empty(), &empty(), &["1".to_string()])
        .unwrap_err();
    assert!(matches!(err, QueryError::NoIdentity(t) if t == "Customer"));
}

#[test]
fn denied_identity_is_bad_filter() {
    let denied: HashSet<String> = ["id".to_string()].into_iter().collect();
    let err = identity_in_predicate(&customer(Some("id".into())), &denied, &empty(), &["1".to_string()])
        .unwrap_err();
    assert!(matches!(err, QueryError::BadFilter(c) if c == "id"));
}

#[test]
fn masked_identity_is_bad_filter() {
    let masked: HashSet<String> = ["id".to_string()].into_iter().collect();
    let err = identity_in_predicate(&customer(Some("id".into())), &empty(), &masked, &["1".to_string()])
        .unwrap_err();
    assert!(matches!(err, QueryError::BadFilter(c) if c == "id"));
}

#[test]
fn uncoercible_value_is_bad_filter() {
    let err = identity_in_predicate(
        &customer(Some("id".into())),
        &empty(),
        &empty(),
        &["notanumber".to_string()],
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::BadFilter(c) if c == "id"));
}
```

Add the BUCK target in `src/services/query-api/BUCK` (mirror the `compile-chain-pairs` pure-logic target):

```python
rust_test(
    name = "identity-in-predicate",
    crate = "identity_in_predicate",
    srcs = ["tests/identity_in_predicate.rs"],
    crate_root = "tests/identity_in_predicate.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
    ],
)
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/query-api:identity-in-predicate > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL (compile error — `identity_in_predicate` undefined).

- [ ] **Step 3: Add `identity_in_predicate` + `ids` fields**

In `src/services/query-api/src/handler.rs`:

Add `ids` to `ObjectQuery`:

```rust
pub struct ObjectQuery {
    pub type_name: String,
    pub eq_filters: Vec<(String, String)>,
    /// Object-set input: scope the read to these identity values (an `In` predicate on
    /// the declared identity). Empty = no scoping.
    pub ids: Vec<String>,
}
```

Add `ids` to `ChainQuery`:

```rust
pub struct ChainQuery {
    pub from_type: String,
    pub path: Vec<Hop>,
    pub filters: Vec<ChainFilter>,
    /// Object-set input: scope the SOURCE to these identity values. Empty = no scoping.
    pub ids: Vec<String>,
}
```

Add the helper (place it near `project_allowed`):

```rust
/// Lower an object-set input (`ids`) to an `In` predicate on `otype`'s declared identity
/// column, governed like any caller filter. `None` when `ids` is empty. Errors: no
/// declared identity (`NoIdentity`); the identity column is denied or masked, so it is not
/// a permitted filter column (`BadFilter`); or a value does not coerce (`BadFilter`).
pub fn identity_in_predicate(
    otype: &ObjectType,
    denied: &std::collections::HashSet<String>,
    masked: &std::collections::HashSet<String>,
    ids: &[String],
) -> Result<Option<crate::filter::CallerPredicate>, QueryError> {
    if ids.is_empty() {
        return Ok(None);
    }
    let identity = otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(otype.name.0.clone()))?;
    let allowed = project_allowed(&otype.properties, denied);
    if !allowed.contains(&identity) || masked.contains(&identity) {
        return Err(QueryError::BadFilter(identity));
    }
    let ty = otype
        .properties
        .iter()
        .find(|p| p.name == identity)
        .map(|p| p.ty.as_str())
        .unwrap_or("");
    let mut values = Vec::with_capacity(ids.len());
    for raw in ids {
        values.push(
            crate::filter::coerce_filter(&identity, ty, raw)
                .map_err(|_| QueryError::BadFilter(identity.clone()))?,
        );
    }
    Ok(Some(crate::filter::CallerPredicate {
        column: identity,
        op: control_plane_core::CompareOp::In,
        values,
    }))
}
```

Confirm `ObjectType` is imported in `handler.rs` (it is — used by `HopMeta`/`read_object`). Confirm `CompareOp` is reachable as `control_plane_core::CompareOp` (no new import needed).

- [ ] **Step 4: Run the unit test to verify pass**

Run: `buck2 test //src/services/query-api:identity-in-predicate > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t.log`
Expected: PASS (all 6 cases).

- [ ] **Step 5: Wire into `read_object`**

In `read_object` (`handler.rs`), after the `eq_filters` predicate-building loop (the `for (col, raw) in &q.eq_filters { … }` block that fills `predicates`) and before `compile_select_with`, append the identity predicate:

```rust
    // Object-set input: scope to the given identities (an In predicate on the identity).
    if let Some(p) = identity_in_predicate(&object_type, &denied, &masked, &q.ids)? {
        predicates.push(p);
    }
```

- [ ] **Step 6: Wire into `resolve_chain`**

In `resolve_chain` (`handler.rs`), after the caller-filter loop (`for f in &q.filters { … }`) and before `Ok((metas, ctypes, hops))`, append the identity predicate to the SOURCE position:

```rust
    // Object-set input: scope the SOURCE (position 0) to the given identities.
    let source = &metas[0];
    if let Some(p) = identity_in_predicate(&source.otype, &source.denied, &source.masked, &q.ids)? {
        ctypes[0].predicates.push(p);
    }
```

- [ ] **Step 7: Fix the `ChainQuery`/`ObjectQuery` construction sites**

Adding the `ids` field breaks every `ObjectQuery { … }` and `ChainQuery { … }` literal. Find and fix them with `ids: vec![]` (or a real value at the HTTP sites in Step 8):

Run: `git grep -n "ObjectQuery {\|ChainQuery {" -- 'src/services/query-api/**/*.rs'`

Add `ids: vec![]` to each literal in `handler.rs` (`read_linked_objects` builds a `ChainQuery`), `http.rs` (handled in Step 8), and any test that constructs these (e.g. `associations.rs`, `multi_hop_traversal_e2e.rs`, `inverse_hops_e2e.rs`, `governed_read.rs`, `http_smoke.rs`, `typed_filter_e2e.rs` — whatever the grep lists). Build to confirm:

Run: `buck2 build //src/services/query-api:query-api > /tmp/b.log 2>&1; grep -nE "BUILD SUCCEEDED|error\[" /tmp/b.log`
Expected: BUILD SUCCEEDED.

- [ ] **Step 8: Parse `?_ids=` on all read routes + add `NoIdentity` arm to `get_object`**

In `src/services/query-api/src/http.rs`:

`get_object` — split `_ids` out of the params, parse it, set `ids`:

```rust
    let mut ids: Vec<String> = Vec::new();
    let mut eq_filters: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        if k == "_ids" {
            ids = v.split(',').filter(|s| !s.is_empty()).map(String::from).collect();
            if ids.is_empty() {
                return (StatusCode::BAD_REQUEST, "_ids requires at least one value").into_response();
            }
        } else {
            eq_filters.push((k, v));
        }
    }
```

and pass `ids` into the `ObjectQuery { type_name, eq_filters, ids }`. Add the missing `NoIdentity` arm to `get_object`'s error match (it currently lacks it):

```rust
        Err(QueryError::NoIdentity(t)) => (StatusCode::BAD_REQUEST, t).into_response(),
```

`get_linked` and `get_linked_chain` — add `_ids` to their existing control-param match loops and a non-empty guard, then set `ids` on the `ChainQuery`. In each loop add the arm:

```rust
            "_ids" => {
                ids = v.split(',').filter(|s| !s.is_empty()).map(String::from).collect();
            }
```

(declare `let mut ids: Vec<String> = Vec::new();` before the loop, and after the loop: `if !raw_ids_present_but_empty` — simpler: track presence. Use this pattern in each chain handler:)

```rust
    let mut ids: Vec<String> = Vec::new();
    let mut ids_present = false;
    // inside the loop:
    //   "_ids" => { ids_present = true; ids = v.split(',').filter(|s| !s.is_empty()).map(String::from).collect(); }
    // after the loop:
    if ids_present && ids.is_empty() {
        return (StatusCode::BAD_REQUEST, "_ids requires at least one value").into_response();
    }
```

Set `ids` on the constructed `ChainQuery` in both chain handlers. (`chain_error` already maps `NoIdentity`→400 and `BadFilter`→400, so the chain routes need no new arms.)

- [ ] **Step 9: Write the e2e (failing)**

Create `src/services/query-api/tests/object_set_e2e.rs`. Mirror the `association_e2e.rs` HTTP-router harness (PgFixture, DuckLakeWriter, AppState/router, X-Loom-Subject, ACL Read grants, type+link definitions with identities). Seed a `Customer` (identity `id`) with rows id ∈ {1,2,3} and an `Order` (identity `order_id`) FK-linked. Assertions (drive the real router via tower `oneshot`, as `association_e2e` does):

- `GET /objects/Customer?_ids=1,2` → `{"objects":[…]}` with exactly the id-1 and id-2 customers.
- `GET /objects/Customer/links/orders?_ids=1` → only object 1's orders (source scoped before the hop).
- `GET /objects/Customer/links/orders?_ids=1&_shape=association` → association pairs only from source 1.
- `GET /objects/Customer?_ids=1` on a Customer type defined WITHOUT an identity → HTTP 400. (Define a second fixture type with `identity: None`, or redefine; simplest: a separate type `Plain` with no identity and assert `/objects/Plain?_ids=1` → 400.)
- `GET /objects/Customer?_ids=` (present but empty) → HTTP 400.

Add the BUCK target (mirror `association-e2e`'s `loom_fixture_test` with `duckdb = True`, copying its `deps` — including `axum`/`tower`/`http-body-util`/`async-trait` for the router `oneshot`).

- [ ] **Step 10: Run the e2e + full query-api suite + clippy**

Run: `buck2 test //src/services/query-api:object-set-e2e > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked|error\[" /tmp/t.log`
Expected: PASS.
Run: `buck2 test //src/services/query-api/... > /tmp/t2.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t2.log`
Expected: all pass.
Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.

- [ ] **Step 11: Commit**

```bash
git add src/services/query-api/
git commit -m "feat(query): object-set inputs via ?_ids=

?_ids=1,2,3 scopes a read to a set of source objects by their declared identity
(an In predicate, via identity_in_predicate), across plain reads, traversals, and
association (one path through read_object + resolve_chain). No identity -> 400;
denied/masked identity or bad value -> 400; empty _ids -> 400.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Docs

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`
- Modify: `docs/FUTURE.md`

- [ ] **Step 1: Roadmap**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, add a "Where we are" paragraph: object-set inputs (`?_ids=`) deliver source-by-identity scoping across reads/traversals/association, completing the graph-query arc; control params now use a reserved `_` prefix guarded by a bind-time `ReservedName` check. If a "remaining" sentence still lists object-set inputs as deferred, remove it.

- [ ] **Step 2: FUTURE.md**

In `docs/FUTURE.md`, read it first, then record (editing the relevant query/reads area in place): `?_ids=` object-set inputs delivered; the reserved `_` control-param namespace + bind guard; note the still-open `/graph` surface (cyclic/self-link traversal) as the remaining graph follow-up.

- [ ] **Step 3: Lint + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -nE "Failed|Passed|error" /tmp/p.log | tail -20`
Expected: all hooks pass (fix any markdown whitespace/EOF the hooks flag, then re-run).

```bash
git add docs/
git commit -m "docs(query): mark object-set inputs + reserved control-param namespace delivered

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification

- [ ] **Whole first-party suite**

Run: `buck2 test //src/... > /tmp/all.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|error\[" /tmp/all.log`
Expected: all pass, zero failures.

- [ ] **Clippy across all first-party Rust**

Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.
