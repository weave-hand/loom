# Inverse hops inside a `/graph` path-cycle — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a `GET /objects/:type/graph?path=…` path-cycle mix forward and backward hops (e.g. `memberOf,~memberOf`), so a shared-membership cycle resolves with only the forward link declared.

**Architecture:** Lift the already-shipped relational-chain inverse machinery (`Hop`/`Direction`/`LinkBacking::reversed()`/`Ontology::links_to`/`parse_path_hops`) into the `/graph` path-cycle. An inverse hop is fully expressed at *resolution* time by two substitutions — land on the link's origin (`link.from`, resolved via inbound adjacency `links_to`) and reverse its backing (`backing.reversed()`) — so `compile_graph_reach`/`reach_joins`/`link_join` are **unchanged** (direction-agnostic). All change lives in `GraphQuery.path`'s type (`Vec<String>` → `Vec<Hop>`), the path-cycle resolution loop in `read_graph_reach`, and the HTTP path parse.

**Tech Stack:** Rust, buck2, axum, DataFusion (in-process serving for e2e), hermetic Postgres fixtures (`loom_fixture_test`).

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-07-01-inverse-in-path-design.md`. Register item `road-inverse-in-path` (area: query).
- **Tests are `rust_test` integration targets ONLY** — no inline `#[cfg(test)]`. Each new test file is a sibling `tests/<name>.rs` wired as its own target in `src/services/query-api/BUCK`. The `no-inline-tests` prek hook fails on any `#[test]`/`#[tokio::test]` in `src/**.rs`.
- **e2e/fixture tests use the `loom_fixture_test` macro**, not bare `rust_test` (they boot hermetic Postgres which refuses to run as root on RE). Pure-logic/handler tests use `loom_rust_test`.
- **Reuse `//src/services/query-api:e2e-support`** — `use e2e_support::{get, subject_with_role, grant_read, ids_i64, tref, prop, InProcessServingEngine, ...}`; add `":e2e-support"` to the new e2e target's `deps`.
- **Clippy is strict** (pedantic + restriction; `unwrap_used`/`expect_used`/`indexing_slicing`/`panic` enforced on production code). Test code is exempted from panic-safety lints via the `loom_rust_test`/`loom_fixture_test` wrappers.
- **No new SQL / no `.sqlx` change** — `compile_graph_reach` and the recursive-CTE SQL shape are unchanged; `reversed()`/`links_to` already exist across `memory`/`postgres`/`testkit`. No migration.
- **Don't pipe `buck2 test`/`bxl` through `tail`/`head`** — redirect to a file and grep: `buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **Markdown lint:** any edited `.md` must end with exactly one trailing newline and no trailing whitespace (`end-of-file-fixer`/`trim trailing whitespace` police `.md` in the `lint` CI job).
- **Commits:** Conventional Commits (a `commit-msg` prek hook enforces it). Do NOT put the model identifier in commits/PR.

---

## File Structure

- `src/services/query-api/src/handler.rs` — **Modify.** `GraphQuery.path: Vec<String>` → `Vec<Hop>`; add the `Direction::Inverse` branch to the path-cycle resolution loop in `read_graph_reach` (mirroring `resolve_chain`'s inverse arm at `handler.rs:742-763`); a `hop_path_string` helper to re-serialize `&[Hop]` for the `NotCyclicPath` message.
- `src/services/query-api/src/http.rs` — **Modify.** `get_graph_path` parses `?path=` via `parse_path_hops` → `Vec<Hop>`; the `*`-tail detection considers only forward hops; `graph_respond` takes `Vec<Hop>`; `get_graph` (single-hop route) call site becomes `vec![link_name.into()]`; add `AmbiguousLink → 400` to `graph_error`.
- `src/services/query-api/src/path_parse.rs` — **Unchanged** (`parse_path_hops` already parses the `~` sigil).
- `src/services/query-api/src/sql.rs` — **Unchanged** (`compile_graph_reach`/`reach_joins`/`link_join` are direction-agnostic).
- `src/control-plane/core/src/ontology.rs` — **Unchanged** (`reversed()`/`links_to` exist).
- `src/services/query-api/tests/path_parse.rs` — **Modify.** Add a `/graph`-grammar-inheritance assertion for `memberOf,~memberOf`.
- `src/services/query-api/tests/graph_reach.rs` — **Modify.** `graph_query` helper builds `Vec<Hop>` from `&[&str]` (forward, via `From<&str>`) so existing forward tests pass unchanged; add a directed helper + inverse handler tests.
- `src/services/query-api/tests/graph_inverse_e2e.rs` — **Create.** `loom_fixture_test` e2e over a `memberOf`-only membership graph.
- `src/services/query-api/BUCK` — **Modify.** Wire the new `graph_inverse_e2e` target (mirror `graph_path_e2e`).
- `docs/ROADMAP.md`, `docs/FUTURE.md` — **Modify** (final task, via `loom-docs-update`).

---

## Task 1: `GraphQuery.path: Vec<Hop>` + inverse branch in the path-cycle resolver

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (`GraphQuery` struct ~line 644; `read_graph_reach` loop ~lines 1016-1058; add `hop_path_string` helper)
- Modify: `src/services/query-api/src/http.rs` (`get_graph` call site ~line 432-435 and `graph_respond` ~line 594 to keep the crate compiling — see Task 2 for the full HTTP parse; here do the **minimal** compile-fix so `handler` tests build)
- Test: `src/services/query-api/tests/graph_reach.rs` (modify)

**Interfaces:**
- Consumes: `Hop { link: String, direction: Direction }` and `Direction::{Forward, Inverse}` (existing, `handler.rs:544-558`); `Hop: From<&str>`/`From<String>` yield `Forward` (existing). `Ontology::links_to(&TypeName, PageReq) -> Result<Page<LinkDef>>` (existing, `ontology.rs:239`). `LinkBacking::reversed() -> LinkBacking` (existing, `ontology.rs:83`). `QueryError::{AmbiguousLink(String), UnknownLink(String), NotCyclicPath(String), Forbidden, NoIdentity(String), UnknownType(String)}` (existing, `handler.rs:52-95`).
- Produces: `pub struct GraphQuery { pub type_name: String, pub path: Vec<Hop>, pub depth: u32, pub filters: Vec<(String, String)>, pub ids: Vec<String> }`. `fn hop_path_string(path: &[Hop]) -> String` (private in `handler.rs`) re-emitting `~` for inverse hops, comma-joined. `read_graph_reach` keeps its signature `pub async fn read_graph_reach(q: &GraphQuery, subject: &Subject, deps: &QueryDeps<'_>) -> Result<ObjectRows, QueryError>`.

- [ ] **Step 1: Update the `graph_query` test helper to build `Vec<Hop>` (keeps existing forward tests as the safety net)**

In `src/services/query-api/tests/graph_reach.rs`, change the helper so `&[&str]` maps to forward hops via `From<&str>` (no behavior change to the 5 existing tests):

```rust
fn graph_query(path: &[&str]) -> GraphQuery {
    GraphQuery {
        type_name: "Person".into(),
        path: path.iter().map(|s| (*s).into()).collect(),
        depth: 3,
        filters: vec![],
        ids: vec![],
    }
}
```

Add a directed helper + import `Direction`/`Hop` for the new inverse tests:

```rust
use query_api::handler::{Direction, GraphQuery, Hop, QueryDeps, QueryError, Subject, read_graph_reach};

/// Build a GraphQuery from explicit (name, direction) hops.
fn graph_query_hops(hops: Vec<Hop>) -> GraphQuery {
    GraphQuery {
        type_name: "Person".into(),
        path: hops,
        depth: 3,
        filters: vec![],
        ids: vec![],
    }
}

fn fwd(name: &str) -> Hop {
    Hop { link: name.into(), direction: Direction::Forward }
}
fn inv(name: &str) -> Hop {
    Hop { link: name.into(), direction: Direction::Inverse }
}
```

- [ ] **Step 2: Write the failing inverse handler tests**

The `seeded` fixture already declares `memberOf` (Person→Team) and `hasMember` (Team→Person). For the inverse tests we exercise `memberOf,~memberOf` (a valid mixed cycle Person→Team→Person using only `memberOf`) and the inverse error arms. Add to `graph_reach.rs`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn resolves_a_mixed_forward_inverse_cycle() {
    // Person --memberOf--> Team --~memberOf--> Person: hop 2 is memberOf reversed,
    // landing back on Person => a valid cycle with only `memberOf` declared.
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing {
        rows: vec![
            vec![SqlValue::Int(2), SqlValue::Text("Bob".into())],
            vec![SqlValue::Int(4), SqlValue::Text("Dana".into())],
        ],
    };
    let deps = QueryDeps { ontology: &cp, acl: &cp, serving: &serving, default_limit: 1000 };
    let rows = read_graph_reach(
        &graph_query_hops(vec![fwd("memberOf"), inv("memberOf")]),
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        rows.rows,
        vec![
            vec![SqlValue::Int(2), SqlValue::Text("Bob".into())],
            vec![SqlValue::Int(4), SqlValue::Text("Dana".into())],
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn inverse_hop_absent_inbound_is_unknown_link() {
    // `~employer` on Person: links_to(Person) has no `employer` (employer targets Company).
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps { ontology: &cp, acl: &cp, serving: &serving, default_limit: 1000 };
    let err = read_graph_reach(&graph_query_hops(vec![inv("employer")]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::UnknownLink(l) if l == "employer"),
        "expected UnknownLink(employer), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn non_cyclic_mixed_path_reserializes_with_tilde() {
    // `memberOf,~worksAt`: hop 1 Person->Team; hop 2 ~worksAt resolves via links_to(Team)?
    // worksAt is Team->Company, so its `to` is Company, NOT Team => ~worksAt is not inbound
    // to Team => UnknownLink. To exercise NotCyclicPath re-serialization instead, use a path
    // that resolves but does not close: `~hasMember` on Person lands (hasMember: Team->Person,
    // to==Person) on Team; Team != Person => NotCyclicPath("~hasMember").
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps { ontology: &cp, acl: &cp, serving: &serving, default_limit: 1000 };
    let err = read_graph_reach(&graph_query_hops(vec![inv("hasMember")]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::NotCyclicPath(p) if p == "~hasMember"),
        "expected NotCyclicPath(~hasMember), got {err:?}"
    );
}
```

Note for the implementer: `~hasMember` on Person — `hasMember` is `Team --hasMember--> Person` (its `to` is Person), so it is inbound to Person; the inverse hop lands on `link.from` = Team. `current` becomes Team ≠ Person ⇒ `NotCyclicPath`, and the message must re-serialize the inverse hop as `~hasMember`. This pins both inbound resolution and the `~`-re-serialized message.

- [ ] **Step 3: Run the new tests to verify they fail to compile / fail**

Run: `buck2 test //src/services/query-api:graph_reach > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|does not" /tmp/t.log`
Expected: FAIL — `GraphQuery.path` is still `Vec<String>` so `graph_query_hops`/`fwd`/`inv` don't compile, and the inverse branch doesn't exist. (The BUCK target name mirrors the file stem; confirm with `grep -n graph_reach src/services/query-api/BUCK`.)

- [ ] **Step 4: Change `GraphQuery.path` to `Vec<Hop>`**

In `src/services/query-api/src/handler.rs`, the `GraphQuery` struct (~line 644):

```rust
pub struct GraphQuery {
    pub type_name: String,
    pub path: Vec<Hop>,
    pub depth: u32,
    pub filters: Vec<(String, String)>,
    pub ids: Vec<String>,
}
```

- [ ] **Step 5: Add the `Direction::Inverse` branch to the path-cycle resolution loop**

Replace the forward-only loop body in `read_graph_reach` (~lines 1022-1055). The current loop is `for (i, link_name) in q.path.iter().enumerate()`; it becomes `for (i, hop) in q.path.iter().enumerate()` with a per-direction `(landed, backing)` resolution mirroring `resolve_chain` (`handler.rs:722-764`):

```rust
    let mut steps: Vec<crate::sql::GraphStep> = Vec::with_capacity(q.path.len());
    let mut current = type_name.clone();
    let last = q.path.len() - 1;
    for (i, hop) in q.path.iter().enumerate() {
        let (landed, backing) = match hop.direction {
            Direction::Forward => {
                let links = deps.ontology.links(&current, PageReq::unbounded()).await?;
                let link = links
                    .items
                    .into_iter()
                    .find(|l| l.name == hop.link)
                    .ok_or_else(|| QueryError::UnknownLink(hop.link.clone()))?;
                (link.to.clone(), link.backing.clone())
            }
            Direction::Inverse => {
                let links = deps.ontology.links_to(&current, PageReq::unbounded()).await?;
                let mut matches = links.items.into_iter().filter(|l| l.name == hop.link);
                let link = matches
                    .next()
                    .ok_or_else(|| QueryError::UnknownLink(hop.link.clone()))?;
                if matches.next().is_some() {
                    return Err(QueryError::AmbiguousLink(hop.link.clone()));
                }
                // Inverse: land on the origin, backing reversed so the symmetric join
                // reaches `current` back to `link.from`.
                (link.from.clone(), link.backing.reversed())
            }
        };
        let landed_target = PolicyTarget::Type(landed.clone());
        // Read on every reached type (intermediate + final), forward or inverse.
        if deps
            .acl
            .check(&subject.0, Action::Read, &landed_target)
            .await?
            == Decision::Deny
        {
            return Err(QueryError::Forbidden);
        }
        let landed_type = deps.ontology.get_type(&landed).await?;
        let (landed_filters, _ld, _lm) = load_policy(deps.acl, &subject.0, &landed_target).await?;
        // Intermediates carry their own row-filters; the FINAL landing is the start type, whose
        // filters are rendered at `nxt` by the compiler -> pass empty here (no double-render).
        let next_filters = if i == last { Vec::new() } else { landed_filters };
        steps.push(crate::sql::GraphStep {
            backing,
            next_table: landed_type.table.clone(),
            next_filters,
        });
        current = landed;
    }
    if current != type_name {
        return Err(QueryError::NotCyclicPath(hop_path_string(&q.path)));
    }
```

Note: the empty-path guard just above (`if q.path.is_empty() { return Err(NotCyclicPath(String::new())) }`) is unchanged. `PageReq`, `Direction`, `Decision`, `Action`, `PolicyTarget`, `load_policy` are already in scope in `handler.rs`.

- [ ] **Step 6: Add the `hop_path_string` helper**

Add near `read_graph_reach` in `handler.rs` (private):

```rust
/// Re-serialize a resolved path for error messages, re-emitting the `~` sigil for inverse
/// hops so the rendered path round-trips the request (`memberOf,~memberOf`).
fn hop_path_string(path: &[Hop]) -> String {
    path.iter()
        .map(|h| match h.direction {
            Direction::Forward => h.link.clone(),
            Direction::Inverse => format!("~{}", h.link),
        })
        .collect::<Vec<_>>()
        .join(",")
}
```

- [ ] **Step 7: Minimal HTTP compile-fix so the crate builds**

In `src/services/query-api/src/http.rs`, `graph_respond` still takes `path: Vec<String>` after this task — to keep the crate compiling, temporarily convert at the two call sites into `Vec<Hop>` right where `GraphQuery` is built. The clean parse lands in Task 2; here do the smallest change. Change `graph_respond`'s signature to accept `Vec<Hop>` and update its two callers:
- `get_graph` (~line 432): `vec![link_name]` → `vec![link_name.into()]`.
- `get_graph_path` (~line 574): wrap the existing `Vec<String> path` as `path.into_iter().map(Hop::from).collect()` at the call (Task 2 replaces this with `parse_path_hops`).

Add `Hop` to the `use crate::handler::{…}` import if not already present (it is — `http.rs:8`). `graph_respond` body builds `GraphQuery { … path, … }` unchanged (now `Vec<Hop>`).

- [ ] **Step 8: Run the handler tests to verify they pass**

Run: `buck2 test //src/services/query-api:graph_reach > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS — the 5 pre-existing forward tests plus the 3 new inverse tests.

- [ ] **Step 9: Confirm the crate + its immediate targets build (clippy clean)**

Run: `buck2 build //src/services/query-api:query-api '//src/services/query-api:query-api[clippy.txt]' > /tmp/b.log 2>&1; grep -E "BUILD|FAIL|error|warning" /tmp/b.log; cat buck-out/**/clippy.txt 2>/dev/null | head` (or use `tools/clippy-all.sh` scoped mentally to this crate). Expected: BUILD SUCCEEDED, empty clippy.

- [ ] **Step 10: Commit**

```bash
git add src/services/query-api/src/handler.rs src/services/query-api/src/http.rs src/services/query-api/tests/graph_reach.rs
git commit -m "feat(query-api): resolve inverse hops in /graph path-cycle"
```

---

## Task 2: Route `?path=` through `parse_path_hops`; `AmbiguousLink → 400` in `graph_error`

**Files:**
- Modify: `src/services/query-api/src/http.rs` (`get_graph_path` ~lines 459-575; `graph_error` ~lines 578-590)
- Test: `src/services/query-api/tests/path_parse.rs` (modify)

**Interfaces:**
- Consumes: `parse_path_hops(&str) -> Vec<Hop>` (existing, `path_parse.rs:14`). `graph_respond(st, type_name, path: Vec<Hop>, depth, filters, ids, subject)` (from Task 1). `Direction::Forward` for the `*`-tail gate. `QueryError::AmbiguousLink` (existing).
- Produces: `get_graph_path` parses the `?path=` value into `Vec<Hop>`; the `*`-recursive-core detection considers only forward hops (so a `~foo*` element stays a path-cycle inverse hop → `UnknownLink`, per spec Non-goals); `graph_error` maps `AmbiguousLink → 400`.

- [ ] **Step 1: Add the grammar-inheritance parse test**

In `src/services/query-api/tests/path_parse.rs`, add (pins that `/graph` inherits the identical `~` grammar the chain uses):

```rust
#[test]
fn graph_path_inherits_the_inverse_grammar() {
    assert_eq!(
        parse_path_hops("memberOf,~memberOf"),
        vec![
            Hop { link: "memberOf".into(), direction: Direction::Forward },
            Hop { link: "memberOf".into(), direction: Direction::Inverse },
        ]
    );
}
```

- [ ] **Step 2: Run it to verify it passes (parser already supports `~`)**

Run: `buck2 test //src/services/query-api:path_parse > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (this test documents inherited behavior; it should pass immediately since `parse_path_hops` already parses `~`). If the target name differs, `grep -n path_parse src/services/query-api/BUCK`.

- [ ] **Step 3: Route `?path=` through `parse_path_hops` and gate `*` to forward hops**

In `get_graph_path` (`http.rs`), change the `path` accumulator from `Vec<String>` to `Vec<Hop>` and the `"path"` arm to use the shared parser. Replace:

```rust
    let mut path: Vec<String> = Vec::new();
```
with
```rust
    let mut path: Vec<Hop> = Vec::new();
```
and the `"path"` match arm (~lines 473-479):
```rust
            "path" => path = parse_path_hops(&v),
```

The union guard (`!path.is_empty()`), the both-specified guard, and the empty-path guard read `path.is_empty()` — unchanged (works on `Vec<Hop>`).

The `*`-recursive-core detection (~lines 536-573) currently inspects raw strings with `s.ends_with('*')`. It must now inspect `hop.link` and fire **only for forward hops**, so a `~foo*` element is left as a path-cycle inverse hop (spec Non-goals: `~`+`*` combined → `UnknownLink`, not a silent tail). Replace the `starred` computation and the core/tail extraction:

```rust
    // Part B: a `*`-suffixed FORWARD segment marks a recursive core + relational tail. `~foo*`
    // is NOT a tail — it stays a path-cycle inverse hop whose name ends in `*` (=> UnknownLink).
    let starred: Vec<usize> = path
        .iter()
        .enumerate()
        .filter(|(_, h)| h.direction == Direction::Forward && h.link.ends_with('*'))
        .map(|(i, _)| i)
        .collect();
    if !starred.is_empty() {
        if starred.len() > 1 {
            return (
                StatusCode::BAD_REQUEST,
                "at most one path segment may be marked recursive with `*`",
            )
                .into_response();
        }
        if starred.first().copied().unwrap_or(0) != 0 {
            return (
                StatusCode::BAD_REQUEST,
                "the recursive `*` segment must be the first path segment",
            )
                .into_response();
        }
        let Some(first_hop) = path.first() else {
            return (StatusCode::BAD_REQUEST, "empty path").into_response();
        };
        let core_link = first_hop.link.trim_end_matches('*').to_string();
        if core_link.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                "recursive core link name must not be empty",
            )
                .into_response();
        }
        // Tail is forward-only (part-B non-goal defers inverse tails); re-emit each remaining
        // hop's name, re-attaching `~` for any inverse hop so it resolves as an (absent)
        // forward link rather than silently dropping the sigil.
        let tail_links: Vec<String> = path
            .get(1..)
            .unwrap_or_default()
            .iter()
            .map(|h| match h.direction {
                Direction::Forward => h.link.clone(),
                Direction::Inverse => format!("~{}", h.link),
            })
            .collect();
        return graph_tail_respond(
            &st, type_name, core_link, tail_links, depth, filters, ids, &subject,
        )
        .await;
    }
    graph_respond(&st, type_name, path, depth, filters, ids, &subject).await
```

Then revert the Task 1 temporary `.into()`/`map(Hop::from)` shim at the `get_graph_path` call to `graph_respond` — now `path` is already `Vec<Hop>`, so the call is `graph_respond(&st, type_name, path, …)` directly (shown above). `Direction` is imported in `http.rs` via `use crate::handler::{…, Hop, …}` — add `Direction` to that import list.

- [ ] **Step 4: Add `AmbiguousLink → 400` to `graph_error`**

In `graph_error` (`http.rs:578`), add the arm (mirrors `chain_error`, `http.rs:359`):

```rust
        QueryError::AmbiguousLink(l) => (StatusCode::BAD_REQUEST, l).into_response(),
```

- [ ] **Step 5: Build the crate + clippy**

Run: `buck2 build //src/services/query-api:query-api '//src/services/query-api:query-api[clippy.txt]' > /tmp/b.log 2>&1; grep -E "BUILD|FAIL|error|warning" /tmp/b.log`
Expected: BUILD SUCCEEDED, no clippy findings.

- [ ] **Step 6: Re-run the handler + parse tests**

Run: `buck2 test //src/services/query-api:graph_reach //src/services/query-api:path_parse > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/http.rs src/services/query-api/tests/path_parse.rs
git commit -m "feat(query-api): parse ~ inverse hops in /graph ?path= and map AmbiguousLink"
```

---

## Task 3: End-to-end over a `memberOf`-only membership graph

**Files:**
- Create: `src/services/query-api/tests/graph_inverse_e2e.rs`
- Modify: `src/services/query-api/BUCK` (wire the target via `loom_fixture_test`, mirroring `graph_path_e2e`)
- Reference: `src/services/query-api/tests/graph_path_e2e.rs` (the part-2 fixture to adapt), `src/services/query-api/tests/inverse_hops_e2e.rs` (the chain inverse e2e), `tests/e2e_support.rs` (helpers)

**Interfaces:**
- Consumes: `e2e_support::{InProcessServingEngine, get, grant_read, ids_i64, prop, subject_with_role, tref}`; `PgFixture`/`IcebergWriter`/`SeedCol`/`IcebergCatalog` (as in `graph_path_e2e.rs`). The router driver `get(router, subject, uri) -> (StatusCode, serde_json::Value)` (check the exact signature in `e2e_support.rs`).
- Produces: a `loom_fixture_test` target `graph_inverse_e2e` proving the `?path=memberOf,~memberOf` inverse cycle equals part-2's `memberOf,hasMember` result with only `memberOf` declared.

- [ ] **Step 1: Read the reference fixtures**

Read `src/services/query-api/tests/graph_path_e2e.rs` in full (its `setup` seeds `person`/`team`/`membership(person_id, team_id)`/`company`, declares `memberOf` Person→Team + `hasMember` Team→Person over `membership`, and `worksAt`). Read the `graph_path_e2e` target block in `src/services/query-api/BUCK`. Read `inverse_hops_e2e.rs` for the chain's inverse assertions and `e2e_support.rs` for `get`/`grant_read`/`subject_with_role`/`ids_i64`/`tref`/`prop` exact signatures. **Do not copy blindly** — match the current helper signatures.

- [ ] **Step 2: Write `graph_inverse_e2e.rs` — adapt the part-2 fixture, declaring only `memberOf`**

Create `src/services/query-api/tests/graph_inverse_e2e.rs`. Seed the identical `person`/`team`/`membership(person_id, team_id)` graph as `graph_path_e2e.rs` (Teams T1{1,2}, T2{3,4,5}, T3{5,6}; T3 inactive; person 5 bridges T2/T3), **but declare only `memberOf`** (Person→Team over `membership`) — drop `hasMember`. Grant Read on Person and Team to the subject. The module doc comment must state the shape and each assertion (mirror `graph_path_e2e.rs`'s header). Cover, per spec Testing:

1. **Equivalence + depth:** `GET /objects/Person/graph?path=memberOf,~memberOf&depth=1` from the seed set returns the same shared-team Persons that part-2's `memberOf,hasMember&depth=1` returns (e.g. from person 3: `{3,4,5}`); `depth=2` reaches further via person 5's T3 bridge (`{3,4,5,6}`). Assert distinct depths differ. Use `ids_i64` to extract + sort the `id`s.
2. **Cycle terminates + dedups (load-bearing):** the pattern inherently revisits the seed; assert the result set is finite and distinct (no duplicate ids) at `depth=3`.
3. **Intermediate governance:** a Read row-filter `active=true` on the intermediate `Team` prunes Persons reachable only through the inactive T3 (person 6 drops out). Seed via a `RowFilter`/`Policy` grant as `graph_path_e2e.rs` does.
4. **Error arms:**
   - `?path=~memberOf` queried on a type where the single inverse hop does not close (e.g. `GET /objects/Team/graph?path=~memberOf` lands on Person ≠ Team) → 400 `NotCyclicPath`. (Confirm against the fixture which query type makes it non-cyclic; the spec bullet's "lands on Person from Team" describes querying **Team**.)
   - an unknown inverse link (`?path=~nope`) → 404.
   - (Ambiguous inverse is exercised at the handler level in Task 1's follow-up if the fixture cannot cheaply declare two identically-named inbound links; if it can, add a `?path=~dup` → 400 `AmbiguousLink` case here. Otherwise add a handler test `inverse_hop_ambiguous_is_ambiguous_link` to `graph_reach.rs`: define a second link named `hasMember` from Company→Person so `links_to(Person)` matches two `hasMember`, and assert `AmbiguousLink`.)

Skeleton (fill in real column/id values from the fixture — no placeholders in the committed file):

```rust
//! Inverse-in-path e2e: GET /objects/:type/graph?path=memberOf,~memberOf over the real HTTP
//! router + in-process Iceberg/DataFusion serving, declaring ONLY `memberOf` (Person->Team).
//! Proves the inverse hop needs no declared return link: the mixed cycle equals part-2's
//! `memberOf,hasMember`; the cycle terminates + dedups; an intermediate Team row-filter prunes;
//! and the ~-inverse error arms (non-cyclic 400, unknown 404[, ambiguous 400]).
// … use e2e_support::{...}; async setup(fx) declaring only memberOf; #[tokio::test] cases.
```

- [ ] **Step 3: Wire the BUCK target (mirror `graph_path_e2e`)**

In `src/services/query-api/BUCK`, add a `loom_fixture_test` target for `graph_inverse_e2e` copying the `graph_path_e2e` block's `deps`/`env`/`srcs` (it needs `":e2e-support"`, the postgres fixture, iceberg, datafusion serving deps). Confirm the exact macro/deps by reading the `graph_path_e2e` block first.

- [ ] **Step 4: Run the e2e (fixture routes local automatically via `loom_fixture_test`)**

Run: `buck2 test //src/services/query-api:graph_inverse_e2e > /tmp/e.log 2>&1; grep -E "Tests finished|FAIL|error\[|panicked" /tmp/e.log`
Expected: PASS. If it fails on the exact reachable-id sets, verify against `graph_path_e2e.rs`'s asserted sets (the two must agree by construction) rather than adjusting the fixture.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/tests/graph_inverse_e2e.rs src/services/query-api/BUCK
git commit -m "test(query-api): e2e inverse hops in /graph over memberOf-only graph"
```

---

## Task 4: Docs — deliver the roadmap item; record deferred inverse follow-ons

**Files:**
- Modify: `docs/ROADMAP.md` (`road-inverse-in-path` → done)
- Modify: `docs/FUTURE.md` (record deferred inverse-in-union / inverse-in-tail follow-ons if not already present)

**Interfaces:**
- Consumes: the register grammar (`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`) and `loom-docs-update`.
- Produces: `road-inverse-in-path` checkbox `[x]`, `status:done`, `pr:#N` (added at PR time); FUTURE entries for the two deferred inverse axes.

- [ ] **Step 1: Run `loom-docs-update` (or edit directly following its rules)**

Flip `road-inverse-in-path` in `docs/ROADMAP.md` from `- [ ]`/`status:planned` to `- [x]`/`status:done`. The `pr:` field is set to `#N` once the PR number is known (in the finishing task). Add/confirm FUTURE entries (spec Non-goals): inverse hops in the union axis (`?links=`) and inverse hops in the recursive-core/relational-tail (`*`). Use existing `fut-…` ids if present; otherwise create per grammar.

- [ ] **Step 2: Validate the registers**

Run: `bash tools/docs.sh validate > /tmp/d.log 2>&1; cat /tmp/d.log`
Expected: no grammar/id/vocab/link errors.

- [ ] **Step 3: Fix markdown lint (trailing newline / whitespace)**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -E "Passed|Failed|Fixing" /tmp/p.log` and commit any hook-applied fixes.

- [ ] **Step 4: Commit**

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs: mark road-inverse-in-path done; defer inverse-in-union/tail"
```

---

## Final verification (before PR)

- [ ] Whole-crate test sweep: `buck2 test //src/services/query-api/... > /tmp/all.log 2>&1; grep -E "Tests finished|FAIL" /tmp/all.log` — all green (forward graph/chain/e2e regressions included).
- [ ] Clippy across first-party Rust for the touched crate: `tools/clippy-all.sh > /tmp/cl.log 2>&1; grep -E "clippy|clean|FAIL" /tmp/cl.log` (or the scoped `[clippy.txt]` sub-targets).
- [ ] `buck2 run //tools:prek -- run --all-files` clean (rustfmt, clippy hook, file checks, markdown).
- [ ] Confirm **no** change to `src/services/query-api/src/sql.rs`, `path_parse.rs`, or `src/control-plane/**` (the spec's "Unchanged" invariant — the compiler stays direction-agnostic).
