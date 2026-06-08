# Design: control-plane Group 4 items 2–4 (pagination convention, proptest round-trips, error-variant docs)

> **Status:** approved design. The tail of Step 2b's Group 4 — the three items the
> control-plane critical review (`2026-06-06-control-plane-critical-review.md`) flagged
> as "worthwhile but can trail" the bigger work. Group 4 item 1 (compile-time sqlx)
> shipped separately as PR #21. These three are small — type-signature and test
> additions, not beefy implementation — so they share one PR.

## Goal

Three loosely-coupled hardening items for the control plane:

1. **Pagination convention** `[H]`: decide and wire a cursor/page convention into every
   unbounded read method *now*, so adding real limiting later is an additive
   adapter-only change rather than a breaking signature change to every read.
2. **proptest round-trips** `[L]`: replace the single hand-built serde round-trip
   examples with property-based coverage (deep nesting, empty vecs, unicode).
3. **Error variants** `[L/cleanup]`: the unused `ControlPlaneError::{Conflict,
   Unauthorized}` variants — keep them, but document their intended producers so they
   read as a deliberate forward contract, not dead code.

No trait behaviour changes; this is a signatures + tests + docs PR.

---

## 1. Pagination convention (the `[H]` item)

### Decision

Bake the page request param **and** the page return envelope into all unbounded reads
now. Adapters return everything in one page (`next: None`); the request fields are
accepted but not yet enforced. This makes the signatures final — future limiting is a
pure adapter change with zero further trait churn.

### New types — `core/src/page.rs`, re-exported from `lib.rs`

```rust
/// Opaque keyset position. The format is adapter-defined and NOT part of the
/// contract — callers round-trip it verbatim (conventionally base64 of a keyset).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor(pub String);

/// A page request.
///
/// `after`/`limit` are part of the stable signature but **not yet enforced**: every
/// adapter currently returns the full result set in a single page (`Page::next ==
/// None`) regardless of these fields. Real keyset limiting is a future adapter-only
/// change that needs no trait-signature churn. `Default`/`unbounded()` = no limit,
/// from the start.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PageReq {
    pub after: Option<Cursor>,
    pub limit: Option<u32>,
}

impl PageReq {
    pub fn unbounded() -> Self;        // == Default::default()
    pub fn limit(n: u32) -> Self;      // { after: None, limit: Some(n) }
    pub fn after(c: Cursor) -> Self;   // { after: Some(c), limit: None }
}

/// One page of results. `next == None` means "no more".
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next: Option<Cursor>,
}

impl<T> Page<T> {
    /// The whole result set as a single, final page (`next: None`). What every
    /// adapter returns today.
    pub fn from_full(items: Vec<T>) -> Self;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}

impl<T> IntoIterator for Page<T> { /* yields items by value */ }
```

`lib.rs`: `pub use page::{Cursor, Page, PageReq};`. `core/Cargo.toml` already has
`serde`/`serde_json`, so `Cursor`'s derives need no new deps.

### Signature changes — the 8 unbounded reads

Each gains a trailing `page: PageReq` and returns `Page<T>` instead of `Vec<T>`:

| trait | method | was | becomes |
|---|---|---|---|
| `Catalog` | `snapshots(table)` | `Vec<Snapshot>` | `Page<Snapshot>` |
| `Catalog` | `files(table, at)` | `Vec<FileRef>` | `Page<FileRef>` |
| `Ontology` | `list_types()` | `Vec<ObjectType>` | `Page<ObjectType>` |
| `Ontology` | `links(name)` | `Vec<LinkDef>` | `Page<LinkDef>` |
| `Acl` | `policies_for(subject, target)` | `Vec<Policy>` | `Page<Policy>` |
| `Lineage` | `events_for(run)` | `Vec<LineageEvent>` | `Page<LineageEvent>` |
| `Lineage` | `upstream(dataset)` | `Vec<DatasetRef>` | `Page<DatasetRef>` |
| `Lineage` | `downstream(dataset)` | `Vec<DatasetRef>` | `Page<DatasetRef>` |

The `page: PageReq` param is added last on each method, keeping the existing args in
place. Doc comments on each method keep their existing empty/order semantics and gain
one line: page request currently accepted-but-not-enforced; results are a single full
page.

### Adapter changes (memory + postgres)

Every impl of the 8 methods builds `Page::from_full(rows)` from the same query/scan it
does today, ignoring `page.after`/`page.limit`. No SQL changes (so no `.sqlx`
regeneration), no memory-store changes — purely wrapping the existing `Vec` and
threading the unused param.

### Call-site churn

All consumers are in `testkit/src/lib.rs` (the shared conformance suite). Updates are
mechanical: pass `PageReq::unbounded()` at each call, and read `.items` where the test
inspects results (`.len()`/`.is_empty()` also exist directly on `Page`, and `Page`
is `IntoIterator`, so `set(page)`-style collectors and `for` loops keep working). The
`vec![link.clone()]` equality assertions compare against `.items`. `matches!(…,
Err(NotFound(_)))` assertions are unaffected (error path unchanged).

---

## 2. proptest round-trips (both pure-serde and DB envelope)

### Dependency

`proptest` is added to **`core`** and **`postgres`** `[dependencies]` (NOT
`[dev-dependencies]`): reindeer's non-vendored mode skips dev-deps, so a test-only
crate needs a normal-dep declaration to get a `//third-party:proptest` target — the
same precedent as the worker crate's `tracing-test`. It is used only under
`#[cfg(test)]`. After editing each `Cargo.toml`: `cargo generate-lockfile` (or
`reindeer update`) then `./tools/buckify.sh`, and add `//third-party:proptest` to each
crate's test target deps in its `BUCK`.

### core — `RowFilter`/`ScalarValue` serde (in `acl.rs` tests)

A recursive proptest strategy:
- `ScalarValue`: `Text(String)` (incl. unicode), `Int(i64)`, `Bool(bool)`, and bounded
  `List(Vec<ScalarValue>)` (incl. empty).
- `RowFilter`: `Compare { property: String, op: any CompareOp, value: ScalarValue }`
  leaves, and bounded-depth `And(Vec<_>)`/`Or(Vec<_>)` (incl. empty vecs) / `Not(Box<_>)`.

Property: `from_str(to_string(&f)) == f` for every generated `f`. Keep the existing
hand-built `row_filter_json_round_trips` example test (a readable smoke test); the
proptest is additive. Use `proptest!` with a bounded depth (e.g. `prop_recursive` with
depth ≤ 4, ≤ 8 nodes) so cases stay cheap.

### postgres — closed-enum codecs (exhaustive, in the relevant concern's tests)

The pure string/enum codec helpers in `lib.rs` are *closed* sets, so an exhaustive
variant round-trip beats proptest:
- `event_type_from_str(event_type_to_str(e)) == e` for every `EventType` variant
  (both directions exist).
- `cardinality_from_str(cardinality_to_str(c)) == c` for every `Cardinality` variant
  (both directions exist).
- `action_to_str` has **no inverse** (action is written, never decoded back to the
  enum), so assert the forward mapping is total over every `Action` and produces
  distinct, non-empty strings — not a round-trip.

These are plain `#[test]`s iterating a `const`/explicit slice of all variants — no DB,
no proptest runner.

### postgres — lineage envelope DB round-trip (property-based, bounded)

A `#[tokio::test]` that drives `proptest::test_runner::TestRunner` manually (proptest's
`proptest!` macro is sync-only, so we generate inside an async test instead): generate
a bounded number (~16) of arbitrary `LineageEvent`s — arbitrary `payload`
(`serde_json::Value`), `inputs`/`outputs` (incl. empty), unicode dataset
names/namespaces, every `EventType` — and for each: `emit(event)` then `events_for(run)`
and assert the read-back event equals the emitted one. Uses the existing `PgFixture`.
Bounded case count keeps the per-case DB round-trip affordable.

---

## 3. Error variants — keep + document

`ControlPlaneError::{Conflict, Unauthorized}` have no producer today (writes are
idempotent upserts; ACL `check` returns a `Decision` and the control plane never
authenticates a caller). They are kept — both are expected to gain producers in the
near term — but documented so they read as a forward contract:

```rust
/// A write lost an optimistic-concurrency / uniqueness race. No producer yet —
/// today's writes are idempotent upserts; reserved for non-idempotent writes.
#[error("conflict: {0}")]
Conflict(String),
/// The caller is not authorized. Reserved for the Step-3 service auth layer; the
/// control plane itself never authenticates a caller (ACL `check` returns a
/// `Decision`, not an error).
#[error("unauthorized")]
Unauthorized,
```

Add a `Conflict` display test for symmetry with the existing `Unauthorized` one
(`assert_eq!(Conflict("x".into()).to_string(), "conflict: x")`). No behaviour change.

### Minor adjacent cleanup

`postgres/Cargo.toml`'s sqlx comment still claims "no compile-time query macros yet …
arrive in Phase 1" — false since PR #21 (we use `query!` and the cache is committed).
Replace it with an accurate one-liner while here.

---

## Verification

- `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...` — all green. The
  conformance suite still passes against both adapters through the new `Page` returns;
  the new proptest/codec tests pass.
- `tools/clippy-all.sh` clean; `prek run --all-files` green (incl. `reindeer-check`,
  which must stay in sync after adding `proptest`).
- No `.sqlx` regeneration needed (SQL is unchanged — pagination wraps existing scans).
- `rg 'Result<Vec<' src/control-plane/core/src` returns nothing for the 8 migrated
  reads (all now `Page<T>`); the only remaining `Vec` returns are intra-type fields,
  not method returns.

## Scope / non-goals

- **No real pagination enforcement.** `after`/`limit` are accepted and documented as
  not-yet-enforced; adapters return one full page. Implementing keyset limiting is a
  future adapter-only change (explicitly the point of doing the convention now).
- **No new error producers.** `Conflict`/`Unauthorized` stay unproduced; only docs +
  one test are added.
- **No trait-behaviour or store changes** beyond the `Page` wrapping. `queue` is
  untouched (no unbounded reads — `dequeue` returns `Option<Job>`).
- One PR for all three: each is small and independent; together they close out
  Group 4.
