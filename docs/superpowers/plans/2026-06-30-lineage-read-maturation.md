# Lineage Read Maturation — Transitive Closure + Pagination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mature the `Lineage` read surface so `upstream`/`downstream` return the full depth-bounded transitive closure (not one hop) and all three reads honor `PageReq` (cursor + limit), on both the postgres and memory adapters, verified by an extended testkit contract.

**Architecture:** Add a `depth: u32` parameter to `upstream`/`downstream` (default behavior = `depth == 1`, today's one hop), capped by a new `LINEAGE_MAX_DEPTH` constant (over-cap → `Validation` error). Postgres implements closure as a depth-bounded `WITH RECURSIVE` CTE over `lineage.event_dataset` (mirroring query-api's `/graph` reachability); the memory fake mirrors it with a BFS + visited set. Pagination is the established keyset convention (`ORDER BY` stable key, `WHERE key > after`, `LIMIT limit + 1` to detect a next page) applied to all three reads via a new shared `Page::from_keyset` primitive and a small opaque cursor codec in `core`.

**Tech Stack:** Rust (edition 2024), buck2, sqlx compile-time `query!` (postgres), `async-trait`, `serde_json` (cursor codec — already a `core` dep), hermetic postgres fixture (`loom_fixture_test`).

## Global Constraints

- **Tests are `rust_test`/`loom_fixture_test` integration targets only — NEVER inline `#[cfg(test)]`.** The `no-inline-tests` prek hook fails the build if any `src/**.rs` file contains `#[test]`/`#[tokio::test]`. Put tests in `tests/<name>.rs` wired as a target in the crate's `BUCK`.
- **Fixture (postgres) tests use the `loom_fixture_test` macro**, never a bare `rust_test`, or they route to RE and fail as root.
- **Clippy is strict** (`pedantic` + `restriction`). On production lib/bin code: NO `unwrap()`/`expect()`/`panic!`/`todo!`/indexing-slicing/`dbg!`. Prefer `?`, `map_or`, `unwrap_or`, `usize::try_from(..).unwrap_or(..)`, `matches!`. Test code is exempted from the panic-safety lints by the `loom_rust_test`/`loom_fixture_test` wrappers, so `.unwrap()` in tests is fine.
- **After changing any postgres SQL, refresh `.sqlx`:** run `tools/sqlx-prepare.sh` and commit the `src/control-plane/postgres/.sqlx/` change. The `//src/control-plane/postgres:sqlx-cache-check` test enforces freshness in the normal test sweep.
- **Do NOT pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to a file and grep it: `buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **Commit messages must follow Conventional Commits** (`feat:`/`test:`/`refactor:`/`docs:` …) — the `conventional-commit` commit-msg hook enforces it.
- **Cursor encoding is adapter-defined and NOT part of the contract** (per `core/src/page.rs` module docs) — callers round-trip it verbatim. We use a `serde_json`-based opaque encoding (no new third-party dependency); this is explicitly compliant with the "conventionally base64 of a keyset" note ("conventionally", not required).
- **The hermetic fixture runs `initdb --no-locale`** (C collation = bytewise ordering), so postgres `ORDER BY namespace, name` matches Rust's `String`/`DatasetRef` `Ord`. No `COLLATE` clause is needed; test data still uses collation-stable lowercase-ASCII / zero-padded names for clarity.

---

## File Structure

| File | Responsibility | Task |
|------|----------------|------|
| `src/control-plane/core/src/lineage.rs` | Trait sig (`depth` param), `LINEAGE_MAX_DEPTH`, `check_depth`, cursor codec, `DatasetRef` `Ord` derive, doc updates | 1 |
| `src/control-plane/core/src/page.rs` | New `Page::from_keyset` keyset-page primitive | 1 |
| `src/control-plane/core/src/lib.rs` | Re-export the new public items | 1 |
| `src/control-plane/core/tests/lineage_cursor.rs` | Unit tests for codec + `check_depth` | 1 |
| `src/control-plane/core/tests/page.rs` | Extend with `from_keyset` unit tests | 1 |
| `src/control-plane/core/BUCK` | Wire the new `lineage_cursor` test target | 1 |
| `src/control-plane/memory/src/lineage.rs` | Memory closure (BFS + visited) + pagination | 2 |
| `src/control-plane/testkit/src/lib.rs` | Update `lineage_contract` sigs; add `lineage_closure_contract` + `lineage_pagination_contract` | 2 |
| `src/control-plane/memory/tests/lineage.rs` | Wire the two new contracts as `#[tokio::test]`s | 2 |
| `src/control-plane/postgres/src/lineage.rs` | Recursive-CTE closure + paginated `events_for` | 3 |
| `src/control-plane/postgres/.sqlx/` | Regenerated query cache | 3 |
| `src/control-plane/postgres/tests/lineage.rs` | Wire the two new contracts as `#[tokio::test]`s | 3 |
| `src/services/transform/tests/transform_e2e.rs` | Fix `.upstream(..)` call (add `depth` arg) | 4 |
| `src/services/transform/tests/typed_transform_e2e.rs` | Fix `.upstream(..)` call (add `depth` arg) | 4 |

**Build-order note:** Task 1 changes the `Lineage` trait signature, so `memory`/`postgres`/`testkit` stop compiling until their tasks update them. This is expected for a single logical refactor. Each task builds & tests only its own crate's targets; the **full** `buck2 test //src/...` is run once, in the final-review task, after all four tasks land. `memory` tests (Task 2) do not depend on `postgres`, so they build & pass independently; `transform` tests (Task 4) depend on `postgres` (Task 3) **and** the fixed call sites, so Task 4 comes last.

---

### Task 1: Core — `depth` param, depth cap, cursor codec, keyset page primitive

**Files:**
- Modify: `src/control-plane/core/src/lineage.rs`
- Modify: `src/control-plane/core/src/page.rs`
- Modify: `src/control-plane/core/src/lib.rs:33,41`
- Create: `src/control-plane/core/tests/lineage_cursor.rs`
- Modify: `src/control-plane/core/tests/page.rs`
- Modify: `src/control-plane/core/BUCK`

**Interfaces:**
- Produces (consumed by Tasks 2, 3):
  - `pub const LINEAGE_MAX_DEPTH: u32 = 32;`
  - `pub fn check_depth(depth: u32) -> Result<()>` — `Err(ControlPlaneError::Validation(_))` unless `1 <= depth <= LINEAGE_MAX_DEPTH`.
  - `pub fn encode_dataset_cursor(d: &DatasetRef) -> Cursor`
  - `pub fn decode_dataset_cursor(c: &Cursor) -> Result<DatasetRef>`
  - `pub fn encode_event_cursor(seq: i64) -> Cursor`
  - `pub fn decode_event_cursor(c: &Cursor) -> Result<i64>`
  - `impl<T> Page<T> { pub fn from_keyset(items: Vec<T>, limit: Option<u32>, cursor: impl Fn(&T) -> Cursor) -> Self }`
  - `DatasetRef` now derives `PartialOrd, Ord` (lexicographic on `(namespace, name)`).
  - Trait methods: `async fn upstream(&self, dataset: &DatasetRef, depth: u32, page: PageReq) -> Result<Page<DatasetRef>>;` and the same for `downstream`. `events_for` signature is unchanged (`page` now honored).

- [ ] **Step 1: Write the failing codec/cap unit test**

Create `src/control-plane/core/tests/lineage_cursor.rs`:

```rust
use control_plane_core::{
    Cursor, ControlPlaneError, DatasetRef, LINEAGE_MAX_DEPTH, check_depth, decode_dataset_cursor,
    decode_event_cursor, encode_dataset_cursor, encode_event_cursor,
};

fn ds(ns: &str, name: &str) -> DatasetRef {
    DatasetRef { namespace: ns.into(), name: name.into() }
}

#[test]
fn dataset_cursor_round_trips() {
    let d = ds("warehouse", "main.orders");
    let c = encode_dataset_cursor(&d);
    assert_eq!(decode_dataset_cursor(&c).unwrap(), d);
}

#[test]
fn dataset_cursor_handles_special_chars() {
    // namespace/name may contain commas, quotes, brackets — the encoding must round-trip them.
    let d = ds("s3://b,x", r#"weird"]name"#);
    let c = encode_dataset_cursor(&d);
    assert_eq!(decode_dataset_cursor(&c).unwrap(), d);
}

#[test]
fn event_cursor_round_trips() {
    let c = encode_event_cursor(42);
    assert_eq!(decode_event_cursor(&c).unwrap(), 42);
}

#[test]
fn malformed_cursor_is_validation_error() {
    let bad = Cursor("not-a-valid-cursor".into());
    assert!(matches!(decode_dataset_cursor(&bad), Err(ControlPlaneError::Validation(_))));
    assert!(matches!(decode_event_cursor(&bad), Err(ControlPlaneError::Validation(_))));
}

#[test]
fn check_depth_bounds() {
    assert!(check_depth(1).is_ok());
    assert!(check_depth(LINEAGE_MAX_DEPTH).is_ok());
    assert!(matches!(check_depth(0), Err(ControlPlaneError::Validation(_))));
    assert!(matches!(check_depth(LINEAGE_MAX_DEPTH + 1), Err(ControlPlaneError::Validation(_))));
}
```

- [ ] **Step 2: Wire the test target in `core/BUCK`**

After the `error-display` target (mirror its shape), add:

```python
rust_test(
    name = "lineage-cursor",
    crate = "lineage_cursor",
    srcs = ["tests/lineage_cursor.rs"],
    crate_root = "tests/lineage_cursor.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [":core"],
)
```

- [ ] **Step 3: Run the test to verify it fails to compile**

Run: `buck2 test //src/control-plane/core:lineage-cursor > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log`
Expected: FAIL — unresolved imports (`check_depth`, `encode_dataset_cursor`, `LINEAGE_MAX_DEPTH`, etc. not found).

- [ ] **Step 4: Add the const, `check_depth`, and cursor codec to `lineage.rs`**

In `src/control-plane/core/src/lineage.rs`, update the imports near the top:

```rust
use crate::error::{ControlPlaneError, Result};
use crate::page::{Cursor, Page, PageReq};
```

Add, after the `DatasetRef` definition (and add `PartialOrd, Ord` to its derive — see Step 6), these items (place them after the type defs, before the `Lineage` trait):

```rust
/// The maximum transitive-closure depth a lineage read may request. A request
/// beyond this is rejected (`Validation`) so a caller can never trigger an
/// unbounded graph walk. A constant for now; a future env/config seam can make it
/// tunable (see `docs/FUTURE.md`).
pub const LINEAGE_MAX_DEPTH: u32 = 32;

/// Validate a requested closure depth. `Ok` iff `1 <= depth <= LINEAGE_MAX_DEPTH`.
/// `depth == 1` is the back-compatible one-hop read; `0` is meaningless (no hops)
/// and over-cap is an unbounded-walk guard — both are `Validation` errors.
pub fn check_depth(depth: u32) -> Result<()> {
    if depth == 0 || depth > LINEAGE_MAX_DEPTH {
        return Err(ControlPlaneError::Validation(format!(
            "lineage depth {depth} out of bounds (allowed 1..={LINEAGE_MAX_DEPTH})"
        )));
    }
    Ok(())
}

/// Encode a `(namespace, name)` keyset as an opaque page cursor for the
/// dataset-closure reads. The encoding (a JSON pair) is adapter-internal and NOT
/// part of the contract — callers round-trip it verbatim.
#[must_use]
pub fn encode_dataset_cursor(d: &DatasetRef) -> Cursor {
    // Serializing a 2-tuple of owned strings is infallible; default to an empty
    // string on the impossible error (decode of "" is a clean Validation error).
    Cursor(serde_json::to_string(&(&d.namespace, &d.name)).unwrap_or_default())
}

/// Decode a dataset cursor produced by [`encode_dataset_cursor`]. A malformed
/// cursor (not our encoding) is a `Validation` error, never a panic.
pub fn decode_dataset_cursor(c: &Cursor) -> Result<DatasetRef> {
    let (namespace, name): (String, String) = serde_json::from_str(&c.0)
        .map_err(|_| ControlPlaneError::Validation("malformed lineage cursor".into()))?;
    Ok(DatasetRef { namespace, name })
}

/// Encode an event-sequence key (postgres `event_id` / memory insert index) as an
/// opaque cursor for `events_for` pagination.
#[must_use]
pub fn encode_event_cursor(seq: i64) -> Cursor {
    Cursor(seq.to_string())
}

/// Decode an event cursor produced by [`encode_event_cursor`].
pub fn decode_event_cursor(c: &Cursor) -> Result<i64> {
    c.0.parse::<i64>()
        .map_err(|_| ControlPlaneError::Validation("malformed lineage cursor".into()))
}
```

- [ ] **Step 5: Re-export the new items from `lib.rs`**

In `src/control-plane/core/src/lib.rs`, change line 33 to:

```rust
pub use lineage::{
    DatasetRef, EventType, LINEAGE_MAX_DEPTH, Lineage, LineageEvent, RunId, check_depth,
    decode_dataset_cursor, decode_event_cursor, encode_dataset_cursor, encode_event_cursor,
};
```

(`Cursor`, `Page`, `PageReq`, `ControlPlaneError` are already re-exported at lines 41 and 29.)

- [ ] **Step 6: Add `Ord`/`PartialOrd` to `DatasetRef` and update trait signatures + docs**

In `src/control-plane/core/src/lineage.rs`, change the `DatasetRef` derive from:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DatasetRef {
```
to:
```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatasetRef {
```

(`Ord` is lexicographic by field declaration order: `namespace` then `name` — matching the SQL `ORDER BY namespace, name` and the C-collation byte order.)

Update the trait methods (`upstream`/`downstream` gain `depth`; rewrite the doc comments). Replace the whole `Lineage` trait body's three read methods with:

```rust
    /// All events for a run, in emit order. Empty if the run is unknown.
    /// Honors `page` (cursor + limit) — a large run is delivered in bounded pages.
    async fn events_for(&self, run: &RunId, page: PageReq) -> Result<Page<LineageEvent>>;
    /// Transitive upstream closure: every dataset reachable within `depth` hops by
    /// walking output→input edges from `dataset` (i.e. its ancestry). `depth == 1`
    /// is the one-hop read; `depth` is capped at [`LINEAGE_MAX_DEPTH`] (over-cap or
    /// `0` → `Validation`). The seed `dataset` is excluded. Result is a stable-ordered,
    /// `page`-bounded set of `DatasetRef` (no per-node depth annotation this slice).
    async fn upstream(
        &self,
        dataset: &DatasetRef,
        depth: u32,
        page: PageReq,
    ) -> Result<Page<DatasetRef>>;
    /// Transitive downstream closure: every dataset reachable within `depth` hops by
    /// walking input→output edges from `dataset` (i.e. its descendancy). Same depth
    /// cap, seed exclusion, ordering, and pagination as [`Lineage::upstream`].
    async fn downstream(
        &self,
        dataset: &DatasetRef,
        depth: u32,
        page: PageReq,
    ) -> Result<Page<DatasetRef>>;
```

Update the module-level doc comment (lines 1–10): change "The graph (`Lineage::upstream`/`Lineage::downstream`) is one hop, computed from each event's own input/output co-membership." to:

```rust
//! A `DatasetRef` is OpenLineage's own `{namespace, name}` identity, deliberately
//! decoupled from [`crate::TableRef`]/[`crate::TypeName`] so the graph can span
//! physical tables, ontology types, and external datasets alike. The graph
//! ([`Lineage::upstream`]/[`Lineage::downstream`]) is a depth-bounded transitive
//! closure over each event's input/output co-membership, capped at
//! [`LINEAGE_MAX_DEPTH`]; all three reads are cursor-paginated.
```

- [ ] **Step 7: Add the `Page::from_keyset` primitive + its test**

In `src/control-plane/core/src/page.rs`, add to the `impl<T> Page<T>` block:

```rust
    /// Build a page from up to `limit + 1` already-ordered items. If more than
    /// `limit` are present, there is a next page: truncate to `limit` and derive
    /// its cursor from the last kept item via `cursor`. Otherwise this is the final
    /// page (`next: None`). Passing `limit == None` (unbounded) always yields a
    /// final page. This is the shared keyset-pagination assembly every adapter uses.
    pub fn from_keyset(mut items: Vec<T>, limit: Option<u32>, cursor: impl Fn(&T) -> Cursor) -> Self {
        let lim = limit.map(|l| usize::try_from(l).unwrap_or(usize::MAX));
        match lim {
            Some(l) if items.len() > l => {
                items.truncate(l);
                let next = items.last().map(cursor);
                Self { items, next }
            }
            _ => Self { items, next: None },
        }
    }
```

In `src/control-plane/core/tests/page.rs`, add these tests (use the existing `Cursor`/`Page` imports; add them if missing):

```rust
#[test]
fn from_keyset_under_limit_is_final_page() {
    let p = Page::from_keyset(vec![1, 2, 3], Some(5), |n| Cursor(n.to_string()));
    assert_eq!(p.items, vec![1, 2, 3]);
    assert_eq!(p.next, None);
}

#[test]
fn from_keyset_over_limit_truncates_and_sets_cursor() {
    // 4 items fetched (limit + 1) signals a next page; truncate to 3, cursor = last kept.
    let p = Page::from_keyset(vec![10, 20, 30, 40], Some(3), |n| Cursor(n.to_string()));
    assert_eq!(p.items, vec![10, 20, 30]);
    assert_eq!(p.next, Some(Cursor("30".into())));
}

#[test]
fn from_keyset_unbounded_is_final_page() {
    let p = Page::from_keyset(vec![1, 2, 3], None, |n| Cursor(n.to_string()));
    assert_eq!(p.items, vec![1, 2, 3]);
    assert_eq!(p.next, None);
}
```

Check the top of `tests/page.rs`: ensure `use control_plane_core::{Cursor, Page, PageReq};` (or equivalent) imports `Cursor` and `Page`. Add what's missing.

- [ ] **Step 8: Run all core tests to verify they pass**

Run: `buck2 test //src/control-plane/core:lineage-cursor //src/control-plane/core:page > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS (both targets). Also lint clean: `buck2 build '//src/control-plane/core:core[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` → empty.

- [ ] **Step 9: Commit**

```bash
git add src/control-plane/core/
git commit -m "feat(lineage): add depth param, depth cap, cursor codec, keyset page primitive"
```

---

### Task 2: Memory adapter + testkit contract — closure & pagination

**Files:**
- Modify: `src/control-plane/memory/src/lineage.rs`
- Modify: `src/control-plane/testkit/src/lib.rs`
- Modify: `src/control-plane/memory/tests/lineage.rs`

**Interfaces:**
- Consumes (from Task 1): `check_depth`, `encode_dataset_cursor`/`decode_dataset_cursor`, `encode_event_cursor`/`decode_event_cursor`, `Page::from_keyset`, `LINEAGE_MAX_DEPTH`, new trait signatures, `DatasetRef: Ord`.
- Produces (consumed by Task 3): `pub async fn lineage_closure_contract<CP: Lineage>(cp: &CP)` and `pub async fn lineage_pagination_contract<CP: Lineage>(cp: &CP)` in testkit.

- [ ] **Step 1: Add the two new contract functions to testkit (failing — adapters not updated yet)**

In `src/control-plane/testkit/src/lib.rs`, first ensure the `use control_plane_core::{...}` import adds **only** `Cursor` and `LINEAGE_MAX_DEPTH` — `ControlPlaneError`, `DatasetRef`, `EventType`, `Lineage`, `LineageEvent`, `Page`, `PageReq`, `RunId` (and `OffsetDateTime`, `HashSet`) are **already imported** (re-adding `ControlPlaneError` is a duplicate-import error). Add the two missing names to the relevant `use control_plane_core::{...}` line.

Then add these two functions (next to the existing `lineage_contract`):

```rust
/// Contract: transitive closure with a depth cap and cycle termination. Run against
/// every `Lineage` adapter.
pub async fn lineage_closure_contract<CP: Lineage>(cp: &CP) {
    let ds = |ns: &str, n: &str| DatasetRef { namespace: ns.to_string(), name: n.to_string() };
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let set = |p: Page<DatasetRef>| p.into_iter().collect::<HashSet<_>>();
    let edge = |inp: DatasetRef, out: DatasetRef| LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: ts,
        inputs: vec![inp],
        outputs: vec![out],
        payload: serde_json::json!({}),
    };

    // chain  A -> B -> C -> D  (each event: input -> output)
    let (a, b, c, d) = (ds("w", "clo.a"), ds("w", "clo.b"), ds("w", "clo.c"), ds("w", "clo.d"));
    cp.emit(edge(a.clone(), b.clone())).await.unwrap();
    cp.emit(edge(b.clone(), c.clone())).await.unwrap();
    cp.emit(edge(c.clone(), d.clone())).await.unwrap();

    // upstream (ancestry) of D
    assert_eq!(
        set(cp.upstream(&d, 1, PageReq::unbounded()).await.unwrap()),
        [c.clone()].into_iter().collect(),
        "depth=1 is one hop"
    );
    assert_eq!(
        set(cp.upstream(&d, 2, PageReq::unbounded()).await.unwrap()),
        [b.clone(), c.clone()].into_iter().collect(),
        "depth=2 = two hops"
    );
    assert_eq!(
        set(cp.upstream(&d, 3, PageReq::unbounded()).await.unwrap()),
        [a.clone(), b.clone(), c.clone()].into_iter().collect(),
        "depth=3 = full ancestry"
    );
    // downstream (descendancy) of A
    assert_eq!(
        set(cp.downstream(&a, 3, PageReq::unbounded()).await.unwrap()),
        [b.clone(), c.clone(), d.clone()].into_iter().collect(),
        "downstream closure of A"
    );

    // depth cap + zero depth are rejected (never an unbounded walk)
    assert!(
        matches!(
            cp.upstream(&d, LINEAGE_MAX_DEPTH + 1, PageReq::unbounded()).await,
            Err(ControlPlaneError::Validation(_))
        ),
        "over-cap depth rejected"
    );
    assert!(
        matches!(
            cp.upstream(&d, 0, PageReq::unbounded()).await,
            Err(ControlPlaneError::Validation(_))
        ),
        "zero depth rejected"
    );

    // cycle  P -> Q -> R -> P  terminates and returns the finite reachable set
    let (p, q, r) = (ds("w", "cyc.p"), ds("w", "cyc.q"), ds("w", "cyc.r"));
    cp.emit(edge(p.clone(), q.clone())).await.unwrap();
    cp.emit(edge(q.clone(), r.clone())).await.unwrap();
    cp.emit(edge(r.clone(), p.clone())).await.unwrap();
    assert_eq!(
        set(cp.upstream(&p, LINEAGE_MAX_DEPTH, PageReq::unbounded()).await.unwrap()),
        [q.clone(), r.clone()].into_iter().collect(),
        "cyclic upstream terminates; seed P excluded"
    );
}

/// Contract: cursor pagination on dataset closure and `events_for`. Run against
/// every `Lineage` adapter.
pub async fn lineage_pagination_contract<CP: Lineage>(cp: &CP) {
    let ds = |ns: &str, n: &str| DatasetRef { namespace: ns.to_string(), name: n.to_string() };
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();

    // fan-out: 7 inputs each feeding one output Z (one event apiece)
    let z = ds("w", "pag.z");
    let inputs: Vec<DatasetRef> = (0..7).map(|i| ds("w", &format!("pag.in{i:02}"))).collect();
    for inp in &inputs {
        cp.emit(LineageEvent {
            run_id: RunId(uuid::Uuid::new_v4()),
            event_type: EventType::Complete,
            event_time: ts,
            inputs: vec![inp.clone()],
            outputs: vec![z.clone()],
            payload: serde_json::json!({}),
        })
        .await
        .unwrap();
    }

    // page upstream(Z) in pages of 3; every dataset exactly once, in sorted order
    let mut seen: Vec<DatasetRef> = Vec::new();
    let mut after: Option<control_plane_core::Cursor> = None;
    loop {
        let req = PageReq { after: after.clone(), limit: Some(3) };
        let page = cp.upstream(&z, 1, req).await.unwrap();
        assert!(page.items.len() <= 3, "page never exceeds limit");
        seen.extend(page.items.iter().cloned());
        match page.next {
            Some(cur) => after = Some(cur),
            None => break,
        }
    }
    let mut expected = inputs.clone();
    expected.sort();
    assert_eq!(seen, expected, "paged upstream returns every dataset once, in stable order");

    // events_for pagination: a run with 5 events, pages of 2
    let run = RunId(uuid::Uuid::new_v4());
    for i in 0..5 {
        cp.emit(LineageEvent {
            run_id: run,
            event_type: EventType::Running,
            event_time: ts,
            inputs: vec![],
            outputs: vec![ds("w", &format!("ev.o{i}"))],
            payload: serde_json::json!({ "i": i }),
        })
        .await
        .unwrap();
    }
    let mut count = 0usize;
    let mut after: Option<control_plane_core::Cursor> = None;
    let mut ended_with_null = false;
    loop {
        let page = cp.events_for(&run, PageReq { after: after.clone(), limit: Some(2) }).await.unwrap();
        assert!(page.items.len() <= 2, "events page never exceeds limit");
        count += page.items.len();
        match page.next {
            Some(cur) => after = Some(cur),
            None => {
                ended_with_null = true;
                break;
            }
        }
    }
    assert_eq!(count, 5, "all events returned across pages, none duplicated/dropped");
    assert!(ended_with_null, "final page signals no next");
}
```

- [ ] **Step 2: Update the existing `lineage_contract` for the new signatures**

In the existing `lineage_contract` body in `src/control-plane/testkit/src/lib.rs`, every `upstream`/`downstream` call must pass `depth = 1` (back-compat one hop). Mechanically change each:
- `.upstream(&ds("warehouse", "main.c"), PageReq::unbounded())` → `.upstream(&ds("warehouse", "main.c"), 1, PageReq::unbounded())`
- `.downstream(&ds("warehouse", "main.a"), PageReq::unbounded())` → `.downstream(&ds("warehouse", "main.a"), 1, PageReq::unbounded())`
- …and the other `upstream`/`downstream` calls at the lines reported by:

Run: `grep -n '\.\(upstream\|downstream\)(' src/control-plane/testkit/src/lib.rs`
Add `1, ` before the `PageReq` argument in each. Leave `events_for` calls unchanged.

- [ ] **Step 3: Wire the two new contracts into the memory test (failing first)**

In `src/control-plane/memory/tests/lineage.rs`, add two `#[tokio::test]` functions alongside the existing one (each gets its own fresh `MemoryControlPlane`):

```rust
#[tokio::test]
async fn memory_passes_lineage_closure_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::lineage_closure_contract(&cp).await;
}

#[tokio::test]
async fn memory_passes_lineage_pagination_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::lineage_pagination_contract(&cp).await;
}
```

(No `BUCK` change: the existing `//src/control-plane/memory:lineage` target globs this file and runs all `#[tokio::test]`s in it.)

- [ ] **Step 4: Run the memory test to verify it fails to compile**

Run: `buck2 test //src/control-plane/memory:lineage > /tmp/t.log 2>&1; grep -E "error\[|expected .* arguments|Tests finished|FAIL" /tmp/t.log`
Expected: FAIL — the memory `impl Lineage` still has the old signatures / one-hop behavior (arity mismatch and/or closure assertions fail).

- [ ] **Step 5: Rewrite the memory adapter for closure + pagination**

Replace the body of `src/control-plane/memory/src/lineage.rs` from the `impl Lineage` block onward. Keep `emit` as-is. The full new file:

```rust
use std::collections::HashSet;

use async_trait::async_trait;
use control_plane_core::{
    Cursor, DatasetRef, Lineage, LineageEvent, Page, PageReq, Result, RunId, check_depth,
    decode_dataset_cursor, decode_event_cursor, encode_dataset_cursor, encode_event_cursor,
};

use crate::MemoryControlPlane;

#[derive(Default)]
pub(crate) struct LineageState {
    pub(crate) events: Vec<LineageEvent>,
}

/// Which way to walk the per-event input/output co-membership graph.
#[derive(Clone, Copy)]
enum Dir {
    /// output→input: ancestry (upstream).
    Upstream,
    /// input→output: descendancy (downstream).
    Downstream,
}

/// One-hop neighbors of `node` in `dir`, scanning `events`. Upstream: for every
/// event that *produces* `node` (node ∈ outputs), its inputs. Downstream: for every
/// event that *consumes* `node` (node ∈ inputs), its outputs.
fn neighbors(events: &[LineageEvent], node: &DatasetRef, dir: Dir) -> Vec<DatasetRef> {
    let mut out = Vec::new();
    for e in events {
        let (probe, yield_) = match dir {
            Dir::Upstream => (&e.outputs, &e.inputs),
            Dir::Downstream => (&e.inputs, &e.outputs),
        };
        if probe.contains(node) {
            out.extend(yield_.iter().cloned());
        }
    }
    out
}

/// Depth-bounded BFS closure with a visited set (the cycle guard). Returns the
/// reachable set excluding the seed, sorted by `(namespace, name)`.
fn closure(events: &[LineageEvent], start: &DatasetRef, depth: u32, dir: Dir) -> Vec<DatasetRef> {
    let mut visited: HashSet<DatasetRef> = HashSet::new();
    visited.insert(start.clone());
    let mut frontier = vec![start.clone()];
    let mut result: Vec<DatasetRef> = Vec::new();
    for _ in 0..depth {
        let mut next_frontier = Vec::new();
        for node in &frontier {
            for nbr in neighbors(events, node, dir) {
                if visited.insert(nbr.clone()) {
                    result.push(nbr.clone());
                    next_frontier.push(nbr);
                }
            }
        }
        if next_frontier.is_empty() {
            break;
        }
        frontier = next_frontier;
    }
    result.sort();
    result
}

/// Apply keyset pagination to an already-sorted dataset set.
fn paginate_datasets(sorted: Vec<DatasetRef>, page: &PageReq) -> Result<Page<DatasetRef>> {
    let after = page.after.as_ref().map(decode_dataset_cursor).transpose()?;
    let filtered: Vec<DatasetRef> = match after {
        Some(a) => sorted.into_iter().filter(|d| *d > a).collect(),
        None => sorted,
    };
    let limited: Vec<DatasetRef> = match page.limit {
        Some(l) => {
            let take = usize::try_from(l).unwrap_or(usize::MAX).saturating_add(1);
            filtered.into_iter().take(take).collect()
        }
        None => filtered,
    };
    Ok(Page::from_keyset(limited, page.limit, |d| encode_dataset_cursor(d)))
}

#[async_trait]
impl Lineage for MemoryControlPlane {
    #[tracing::instrument(skip(self, event), fields(run_id = ?event.run_id, event_type = ?event.event_type), level = "debug")]
    async fn emit(&self, event: LineageEvent) -> Result<()> {
        self.lineage.lock().events.push(event);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn events_for(&self, run: &RunId, page: PageReq) -> Result<Page<LineageEvent>> {
        let after = page.after.as_ref().map(decode_event_cursor).transpose()?;
        let lin = self.lineage.lock();
        // Stable key = the event's insert index (append-only Vec, emit order).
        let mut keyed: Vec<(i64, LineageEvent)> = lin
            .events
            .iter()
            .enumerate()
            .filter(|(_, e)| e.run_id == *run)
            .map(|(i, e)| (i64::try_from(i).unwrap_or(i64::MAX), e.clone()))
            .collect();
        if let Some(a) = after {
            keyed.retain(|(i, _)| *i > a);
        }
        let limited: Vec<(i64, LineageEvent)> = match page.limit {
            Some(l) => {
                let take = usize::try_from(l).unwrap_or(usize::MAX).saturating_add(1);
                keyed.into_iter().take(take).collect()
            }
            None => keyed,
        };
        let paged = Page::from_keyset(limited, page.limit, |(i, _)| encode_event_cursor(*i));
        Ok(Page {
            items: paged.items.into_iter().map(|(_, e)| e).collect(),
            next: paged.next,
        })
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn upstream(
        &self,
        dataset: &DatasetRef,
        depth: u32,
        page: PageReq,
    ) -> Result<Page<DatasetRef>> {
        check_depth(depth)?;
        let set = closure(&self.lineage.lock().events, dataset, depth, Dir::Upstream);
        paginate_datasets(set, &page)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn downstream(
        &self,
        dataset: &DatasetRef,
        depth: u32,
        page: PageReq,
    ) -> Result<Page<DatasetRef>> {
        check_depth(depth)?;
        let set = closure(&self.lineage.lock().events, dataset, depth, Dir::Downstream);
        paginate_datasets(set, &page)
    }
}
```

Notes for the implementer:
- `self.lineage.lock()` returns a guard; `closure(&self.lineage.lock().events, …)` borrows it for the call — fine since `closure` is synchronous and the guard lives to the end of the statement. If the borrow checker complains, bind first: `let events = self.lineage.lock().events.clone();` then pass `&events` (a clone is acceptable for the fake).
- `Cursor` is imported but only used transitively via the codec; if clippy flags it as unused, drop it from the `use`.

- [ ] **Step 6: Run the memory tests to verify they pass**

Run: `buck2 test //src/control-plane/memory:lineage > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS — all three `#[tokio::test]`s (`memory_passes_lineage_contract`, `…_closure_contract`, `…_pagination_contract`).
Lint: `buck2 build '//src/control-plane/memory:memory[clippy.txt]' '//src/control-plane/testkit:testkit[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` → empty.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/memory/ src/control-plane/testkit/
git commit -m "feat(lineage): memory closure + pagination; extend testkit contract"
```

---

### Task 3: Postgres adapter — recursive-CTE closure + paginated `events_for`

**Files:**
- Modify: `src/control-plane/postgres/src/lineage.rs`
- Regenerate: `src/control-plane/postgres/.sqlx/`
- Modify: `src/control-plane/postgres/tests/lineage.rs`

**Interfaces:**
- Consumes (from Tasks 1–2): the trait signatures, `check_depth`, cursor codec, `Page::from_keyset`, and the two new testkit contract functions.
- Produces: postgres `impl Lineage` satisfying the extended contract.

- [ ] **Step 1: Wire the two new contracts into the postgres test (failing first)**

In `src/control-plane/postgres/tests/lineage.rs`, add (each gets a fresh fixture cluster):

```rust
#[tokio::test]
async fn postgres_passes_lineage_closure_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::lineage_closure_contract(&cp).await;
}

#[tokio::test]
async fn postgres_passes_lineage_pagination_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::lineage_pagination_contract(&cp).await;
}
```

(No `BUCK` change — the existing `loom_fixture_test(name = "lineage", …)` globs this file.)

- [ ] **Step 2: Rewrite the postgres adapter's reads**

In `src/control-plane/postgres/src/lineage.rs`: keep `pg_emit` and `event_datasets` unchanged. **Leave the `use crate::{...}` line untouched** — it is currently `use crate::{PgControlPlane, backend, event_type_from_str, event_type_to_str};` and `event_type_to_str` must stay (the unchanged `pg_emit` uses it). Update **only** the `control_plane_core` import:

```rust
use control_plane_core::{
    DatasetRef, Lineage, LineageEvent, Page, PageReq, Result, RunId, check_depth,
    decode_dataset_cursor, decode_event_cursor, encode_dataset_cursor, encode_event_cursor,
};
```

Replace the `impl Lineage for PgControlPlane` block's `events_for`/`upstream`/`downstream` and the `graph_step` helper (delete `graph_step`; add `graph_closure`):

```rust
#[async_trait]
impl Lineage for PgControlPlane {
    #[tracing::instrument(skip(self, event), fields(run_id = ?event.run_id, event_type = ?event.event_type), level = "debug")]
    async fn emit(&self, event: LineageEvent) -> Result<()> {
        pg_emit(&self.pool, &event).await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn events_for(&self, run: &RunId, page: PageReq) -> Result<Page<LineageEvent>> {
        let after = page.after.as_ref().map(decode_event_cursor).transpose()?;
        let fetch = page.limit.map_or(i64::MAX, |l| i64::from(l) + 1);
        let rows = sqlx::query!(
            "select event_id, event_type, event_time, payload from lineage.event \
             where run_id = $1 and ($2::bigint is null or event_id > $2) \
             order by event_id limit $3",
            run.0,
            after,
            fetch,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut keyed: Vec<(i64, LineageEvent)> = Vec::with_capacity(rows.len());
        for r in rows {
            let event_id = r.event_id;
            keyed.push((
                event_id,
                LineageEvent {
                    run_id: *run,
                    event_type: event_type_from_str(&r.event_type),
                    event_time: r.event_time,
                    inputs: self.event_datasets(event_id, "input").await?,
                    outputs: self.event_datasets(event_id, "output").await?,
                    payload: r.payload,
                },
            ));
        }
        let paged = Page::from_keyset(keyed, page.limit, |(id, _)| encode_event_cursor(*id));
        Ok(Page {
            items: paged.items.into_iter().map(|(_, e)| e).collect(),
            next: paged.next,
        })
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn upstream(
        &self,
        dataset: &DatasetRef,
        depth: u32,
        page: PageReq,
    ) -> Result<Page<DatasetRef>> {
        // upstream = walk output→input edges (ancestry).
        self.graph_closure(dataset, "output", "input", depth, page).await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn downstream(
        &self,
        dataset: &DatasetRef,
        depth: u32,
        page: PageReq,
    ) -> Result<Page<DatasetRef>> {
        // downstream = walk input→output edges (descendancy).
        self.graph_closure(dataset, "input", "output", depth, page).await
    }
}
```

Then in the `impl PgControlPlane` block, keep `event_datasets` and add:

```rust
    /// Depth-bounded transitive closure over `lineage.event_dataset`. The seed sits
    /// on `from_dir`; neighbors are taken from `to_dir` of co-member events. A
    /// `WITH RECURSIVE` CTE (mirroring query-api's `/graph` reachability) bounded by
    /// `depth`; `UNION` + the final `DISTINCT` give set semantics and the depth cap
    /// guarantees termination even on cyclic re-run graphs. Keyset-paginated by
    /// `(namespace, name)`.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn graph_closure(
        &self,
        dataset: &DatasetRef,
        from_dir: &str,
        to_dir: &str,
        depth: u32,
        page: PageReq,
    ) -> Result<Page<DatasetRef>> {
        check_depth(depth)?;
        let after = page.after.as_ref().map(decode_dataset_cursor).transpose()?;
        let (after_ns, after_name) = match &after {
            Some(d) => (Some(d.namespace.as_str()), Some(d.name.as_str())),
            None => (None, None),
        };
        let max_depth = i32::try_from(depth).unwrap_or(i32::MAX);
        let fetch = page.limit.map_or(i64::MAX, |l| i64::from(l) + 1);
        let rows = sqlx::query!(
            "with recursive closure(namespace, name, depth) as ( \
                 select $1::text, $2::text, 0 \
               union \
                 select b.namespace, b.name, closure.depth + 1 \
                 from closure \
                 join lineage.event_dataset a \
                   on a.namespace = closure.namespace and a.name = closure.name \
                   and a.direction = $3 \
                 join lineage.event_dataset b \
                   on b.event_id = a.event_id and b.direction = $4 \
                 where closure.depth < $5) \
             select distinct namespace as \"namespace!\", name as \"name!\" \
             from closure \
             where not (namespace = $1 and name = $2) \
               and ($6::text is null or (namespace, name) > ($6, $7)) \
             order by namespace, name \
             limit $8",
            &dataset.namespace,
            &dataset.name,
            from_dir,
            to_dir,
            max_depth,
            after_ns,
            after_name,
            fetch,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let items: Vec<DatasetRef> = rows
            .into_iter()
            .map(|r| DatasetRef { namespace: r.namespace, name: r.name })
            .collect();
        Ok(Page::from_keyset(items, page.limit, |d| encode_dataset_cursor(d)))
    }
```

Implementer notes:
- The `as "namespace!"` / `as "name!"` annotations force sqlx to treat the CTE columns as non-null (CTE-derived columns infer as nullable otherwise). If the build still complains about nullability, the `!` overrides are the fix.
- `after_ns`/`after_name` are `Option<&str>`; sqlx binds them to the `$6::text`/`$7` nullable text params.
- All `as` numeric conversions use `try_from(..).unwrap_or(..)` / `i64::from(..)` to stay clippy-clean (no bare `as`, no `unwrap`).

- [ ] **Step 3: Refresh the `.sqlx` cache**

Run: `bash tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; tail -5 /tmp/sqlx.log`
Expected: it boots hermetic postgres, applies migrations, regenerates `src/control-plane/postgres/.sqlx/`. The old `graph_step` query json is removed; new `graph_closure` + modified `events_for` query json appear.

Run: `git status --short src/control-plane/postgres/.sqlx/`
Expected: shows added/removed/changed `query-*.json` files.

- [ ] **Step 4: Run the postgres lineage tests + the cache-freshness test**

Run: `buck2 test //src/control-plane/postgres:lineage //src/control-plane/postgres:sqlx-cache-check > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS — `postgres_passes_lineage_contract`, `…_closure_contract`, `…_pagination_contract`, and `sqlx-cache-check`.
Lint: `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` → empty.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/
git commit -m "feat(lineage): postgres recursive-CTE closure + paginated reads; refresh .sqlx"
```

---

### Task 4: Fix downstream call sites (transform e2e tests)

**Files:**
- Modify: `src/services/transform/tests/transform_e2e.rs:173`
- Modify: `src/services/transform/tests/typed_transform_e2e.rs:289`

**Interfaces:**
- Consumes: the new `upstream(dataset, depth, page)` signature.

- [ ] **Step 1: Update both `.upstream(..)` calls**

These tests assert the one-hop upstream set, so pass `depth = 1` to preserve behavior.

In `src/services/transform/tests/transform_e2e.rs` (~line 173), change:
```rust
        .upstream(&out_ds, PageReq::unbounded())
```
to:
```rust
        .upstream(&out_ds, 1, PageReq::unbounded())
```

In `src/services/transform/tests/typed_transform_e2e.rs` (~line 289), make the identical change.

- [ ] **Step 2: Build & run the transform tests**

Run: `buck2 test //src/services/transform/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS (these are `loom_fixture_test`s; they assert the same one-hop upstream as before).

- [ ] **Step 3: Commit**

```bash
git add src/services/transform/
git commit -m "test(transform): pass depth=1 to upstream after lineage closure signature change"
```

---

### Task 5: Full-tree verification + register close

**Files:**
- Modify: `docs/ROADMAP.md` (close the item — done via `loom-docs-update` in the finishing step)
- Modify: `docs/FUTURE.md` (the two promoted items are already `[x] promoted`; no change unless reconciliation needed)

- [ ] **Step 1: Full build + test sweep**

Run: `buck2 test //src/... > /tmp/full.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/full.log`
Expected: PASS — no regressions anywhere (all lineage callers were test-only and are updated).

- [ ] **Step 2: Full clippy + prek hooks**

Run: `bash tools/clippy-all.sh > /tmp/clip.log 2>&1; tail -20 /tmp/clip.log` → clean.
Run: `buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; tail -30 /tmp/prek.log` → all hooks pass (commit any in-place fixes the hooks make, especially markdown EOF/whitespace on this plan + the spec).

- [ ] **Step 3: Close the register item**

Use the `loom-docs-update` skill to flip `road-lineage-read-maturation` in `docs/ROADMAP.md` from `- [ ]`→`- [x]`, set `status:done`, and add `pr:#<N>` once the PR number is known. Confirm the two promoted FUTURE items (`fut-lineage-closure`, `fut-lineage-pagination`) are already `[x] status:promoted` (they are) — no change needed.

Run: `bash tools/docs.sh validate > /tmp/v.log 2>&1; cat /tmp/v.log`
Expected: registers valid.

- [ ] **Step 4: Final commit (if docs changed outside the PR step)**

```bash
git add docs/
git commit -m "docs(lineage): close road-lineage-read-maturation register item"
```

---

## Self-Review

**Spec coverage** (against `docs/superpowers/specs/2026-06-30-lineage-read-maturation-design.md`):

- *Transitive closure — `depth` param + cycle guard*: Task 1 (param + `check_depth` + cap const), Task 2 (memory BFS+visited), Task 3 (recursive CTE). ✓
- *`depth = 1` reproduces one-hop (back-compat default)*: closure tests assert `depth=1` == prior one-hop; existing `lineage_contract` calls pass `depth=1`; transform tests pass `depth=1`. ✓
- *`LINEAGE_MAX_DEPTH` cap, over-cap rejected with error*: `check_depth` → `Validation`; tested in core unit test + closure contract (`LINEAGE_MAX_DEPTH + 1` and `0`). ✓
- *`UNION` set-semantics / cycle terminates*: postgres `UNION` + `DISTINCT` + depth bound; memory visited set; cycle contract test (`P→Q→R→P`). ✓
- *Pagination on all three reads, replacing `Page::from_full`, via the cursor convention*: `Page::from_keyset` + cursor codec; `events_for`/`upstream`/`downstream` all paginate; pagination contract test covers datasets + events. ✓
- *Stable order cursor (deterministic)*: `ORDER BY namespace, name` (datasets) / `event_id` (events); `DatasetRef: Ord`; C-collation note. ✓
- *Result stays `Page<DatasetRef>` (no depth annotation)*: closure returns the set, no per-node depth. ✓
- *Both adapters satisfy the same contract; testkit extended (closure incl. cyclic graph, depth-cap, pagination)*: `lineage_closure_contract` + `lineage_pagination_contract` run against both. ✓
- *Compile-time `query!` keeps SQL schema-checked; refresh `.sqlx`*: Task 3 Step 3. ✓
- *Control-plane trait + adapters only — no external HTTP endpoint*: nothing touches query-api routes. ✓
- *Out of scope* (stitching, type↔table join, min-depth annotation, external HTTP): untouched. ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code; no "add error handling"/"similar to Task N" hand-waves. ✓

**Type consistency:** `check_depth`, `encode_dataset_cursor`/`decode_dataset_cursor`, `encode_event_cursor`/`decode_event_cursor`, `Page::from_keyset`, `lineage_closure_contract`/`lineage_pagination_contract`, `graph_closure`, `Dir`, `neighbors`, `closure`, `paginate_datasets` — names and signatures match across all tasks. Trait signature `upstream(&self, dataset, depth, page)` is identical in core (Task 1), memory (Task 2), postgres (Task 3), and the call sites (Tasks 2–4). ✓
