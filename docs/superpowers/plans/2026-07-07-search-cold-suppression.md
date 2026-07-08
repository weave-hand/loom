# Interim `/search` cold-hit suppression — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the existing `/search` survivor post-filter run **unconditionally for identity-bearing vector types** (today it is gated behind a `row_filters.is_empty()` early return), and **identity-dedup** the retained hits — so a COW inline-shadow UPDATE's stale-cold duplicate collapses to one hit and a DELETE's tombstoned cold hit is dropped, even when the type carries no row filter.

**Architecture:** A governed UPDATE/DELETE on an identity-bearing vector type writes an O(change) inline delta (row-version/tombstone) and leaves the pre-mutation vector in the cold Puffin index; `merge_topk` concatenates cold+hot without dedup/suppression. query-api's post-filter (`handler.rs::vector_search`) already re-queries the engine's live **merged view** (`_loom_rn = 1` winner, `_loom_tomb = false`) and retains only surviving hit identities — but only when a row filter exists. This change lifts that gate to an **identity-based carve-out** (identity-less types can't be inline-shadowed, so they keep raw additive hits) and adds per-identity dedup to the retain. Interim query-time mitigation only — durable cold-entry removal stays deferred to slice-2 compaction (`fut-cow-inline-shadow`).

**Tech Stack:** Rust, query-api handler; buck2 `loom_fixture_test` e2e (Postgres + engine writer + in-process serving, reusing `e2e-support` + the COW inline-shadow suites' patterns).

## Global Constraints

- **No read-path change beyond `vector_search`'s early-return + retain.** The `identity_governed()` fail-closed guard stays AHEAD of any post-filter work (a policy denying/masking the identity column still 403s). The `omit-on-missing-link`/derived guards elsewhere are untouched.
- **No change to the Puffin write path, `merge_topk`, the engine cold/hot merge, or MVCC.** Suppression stays a query-api post-filter.
- **Identity-less types return raw hits** (the carve-out): inline-shadow requires a declared identity (`action.rs:1062` errors otherwise), so identity-less vector types accrue no stale cold entries and keep the additive union — no post-filter, no spurious `NoIdentity` internal error now that the block runs unconditionally.
- **The returned identity set is correct** (tombstones removed, stale duplicates collapsed); the interim does NOT fix a surviving identity's *distance* when the stale cold copy sorted nearer (the nearer of the two is kept) — that residual is slice-2's.
- Tests are `rust_test`/`loom_fixture_test`. Clippy strict on production. Commit trailers + Conventional Commits.

---

### Task 1: Unconditional identity-dedup survivor post-filter + `/search` e2e

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (`vector_search`, ~699-746)
- Test: `src/services/query-api/tests/vector_search_cold_suppression_e2e.rs` (new `loom_fixture_test`) + a BUCK target (mirror `cow-inline-shadow-gov-e2e`; **add** deps `//third-party:arrow-array`, `//third-party:arrow-schema`, `//third-party:time`, `//src/control-plane/postgres:postgres` for the direct inline-delta write, plus the usual `:e2e-support`)

**Why the mutations are direct inline writes, not actions:** query-api's action layer **cannot** carry a vector value — `params.rs` rejects `FloatArray` params ("vector parameters are not supported") and query-api's `SqlValue` has no vector variant. So BOTH mutating legs write the inline delta **directly** via `control_plane_postgres::iceberg_inline::{current_inline_version, write_inline_delta}` against the fixture pool — the proven CAS pattern in `postgres/tests/inline_delta_cas.rs`, with the vector `RecordBatch` built as in `postgres/tests/iceberg_inline_vector.rs`. This writes to the same Postgres the `seed_vector_type` serving engine reads (`inline_<table_id>`), so no `spawn_engine_writer`/`ActionDeps`/`run_action` is needed. **Case 4 (identity-less type) is DROPPED** — `build_vector_index` → `identity_column_for` hard-errors without a declared identity, so no vector index (hence no `/search`) can exist over an identity-less type today; the `identity.is_none()` carve-out in Step 3 stays as verified-correct defensive code, just not e2e-reachable (note this in the register close, referencing `fut-cow-inline-shadow`).

**Interfaces:**
- Consumes: `GovernedType { otype, row_filters, denied, masked }` + `identity_governed()` (`governed.rs:20`); `g.otype.identity: Option<String>`; `VectorHit { id: SqlValue, distance: f64 }` (`handler.rs:611`); `sqlvalue_to_id_string` (`handler.rs:652`); `identity_in_predicate`/`compile_select_with`/`fetch_rows` (unchanged usage).
- Produces: no signature change — `vector_search` returns the same `Vec<VectorHit>`, now correctly suppressed/deduped.

- [ ] **Step 1: Write the failing e2e (RED)**

Create `src/services/query-api/tests/vector_search_cold_suppression_e2e.rs`, a `loom_fixture_test`. Read `seed_vector_type` (`e2e_support.rs:708`), `post_search` (`e2e_support.rs:1096`), `postgres/tests/inline_delta_cas.rs` (the `current_inline_version`+`write_inline_delta` CAS pattern), and `postgres/tests/iceberg_inline_vector.rs` (the vector `RecordBatch` builder) first. `seed_vector_type(fx, &db)` returns `(cp, serving, writer)` and seeds `Docs(id Long identity, embedding vector(4))` with 4 orthogonal cold file-tier rows + a built `by_sim` Flat/Cosine index — the cold Puffin index holds each row's ORIGINAL vector. Grant the subject `Read` on `Docs` with **NO row filter** (`grant_read`), so the reproduction depends solely on the unconditional post-filter. Get the `Docs` `TableRef` + `columns` (`id long`, `embedding vector(4)`) and the pool (`fx.pool_for(&db)`).

Local helpers (copy from `iceberg_inline_vector.rs`): `fn vec_batch(id: i64, e: [f32;4]) -> RecordBatch` (Int64 `id` + `List<Float32>` `embedding`), `fn id_batch(id: i64) -> RecordBatch` (just the `id` column), and a `lineage(run, &table)` builder.

```rust
// --- 1. Superseded UPDATE: the stale cold vector must NOT double the identity ---
// Write ONE inline row-version for id=1 with a NEW vector near the probe. The cold
// Puffin index is NOT rebuilt, so it still scores id=1's ORIGINAL vector → pre-fix
// id=1 is returned twice (cold stale + hot fresh).
let v0 = iceberg_inline::current_inline_version(&pool, &docs_table, &columns, "id", &id_batch(1)).await.unwrap();
iceberg_inline::write_inline_delta(
    &pool, &docs_table, &columns, "id",
    false,                                   // tombstone = false → row-version
    &vec_batch(1, [0.9, 0.1, 0.0, 0.0]),      // NEW embedding, near the probe below
    None,                                     // before-image not needed here
    lineage(RunId(uuid::Uuid::new_v4()), &docs_table),
    v0,
).await.unwrap();

let (status, body) = post_search(cp.clone(), serving.clone(), "/search/Docs/by_sim",
    &json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 4 }), "reader").await;
let res = results(status, &body);
let ones = res.iter().filter(|h| h["id"] == json!(1)).count();
assert_eq!(ones, 1, "identity 1 appears exactly once (stale cold duplicate suppressed): {body}");

// --- 2. Tombstoned DELETE: the tombstoned identity must be omitted ---
// A tombstone needs no vector value → write it directly too (tombstone = true).
let v0d = iceberg_inline::current_inline_version(&pool, &docs_table, &columns, "id", &id_batch(2)).await.unwrap();
iceberg_inline::write_inline_delta(
    &pool, &docs_table, &columns, "id",
    true,                                     // tombstone
    &id_batch(2),                             // tombstone batch = identity only
    None, lineage(RunId(uuid::Uuid::new_v4()), &docs_table), v0d,
).await.unwrap();

let (status, body) = post_search(cp.clone(), serving.clone(), "/search/Docs/by_sim",
    &json!({ "query": [0.0, 1.0, 0.0, 0.0], "k": 4 }), "reader").await;   // probe near id=2's original vec
let res = results(status, &body);
assert!(res.iter().all(|h| h["id"] != json!(2)), "tombstoned identity 2 omitted: {body}");
```

(Confirm `write_inline_delta`'s tombstone-batch shape against `inline_delta_cas.rs` — if a tombstone requires the full column set rather than id-only, mirror that file. Confirm the `docs_table`/`columns`/pool accessors against `seed_vector_type`'s return + `fx`.)

Add ONE more case:
- **Row-filter path unchanged** (regression that lifting the early return didn't alter existing behavior): a subject WITH a row filter (mirror `vector_search_e2e.rs::row_filter_drops_nearest_hit`'s `grant_read_filtered`), after the same id=1 inline UPDATE, still gets suppression — assert id=1 appears at most once and any filtered-out id is absent. (Reuse the fixture; a fresh subject with the filtered grant.)

**Do NOT add an identity-less case** — it is unbuildable (`build_vector_index`/`identity_column_for` require a declared identity, so no vector index can exist over an identity-less type). The `identity.is_none()` carve-out (Step 3) is verified-correct defensive code; note in the register close that it is not e2e-reachable today.

- [ ] **Step 2: Run it to verify it fails (RED)**

Run: `buck2 test --console none //src/services/query-api:vector-search-cold-suppression-e2e` (add the BUCK target first — mirror `cow-inline-shadow-gov-e2e`'s `loom_fixture_test` target + the extra deps listed above).
Expected: FAIL — pre-fix, the UPDATE case returns identity 1 **twice** (cold stale + hot fresh, no dedup, and the post-filter is skipped because `row_filters.is_empty()`), and the DELETE case returns the tombstoned identity 2 (post-filter skipped). (The row-filter regression case should already pass — it guards that lifting the early return didn't change the row-filter path.)

- [ ] **Step 3: Lift the early return to an identity carve-out**

In `src/services/query-api/src/handler.rs` `vector_search`, replace the `row_filters.is_empty()` early return (~702-704):

```rust
    if g.row_filters.is_empty() {
        return Ok(hits);
    }
```
with an identity-based carve-out:
```rust
    // Interim cold-hit suppression: the survivor post-filter must run for EVERY
    // identity-bearing type — not only when a row filter happens to exist — because a
    // COW inline-shadow UPDATE/DELETE leaves a stale/tombstoned vector in the cold
    // Puffin index that `merge_topk` does not suppress (#iss-search-cold-superseded-hits).
    // Identity-less types cannot be inline-shadowed (mutation requires a declared
    // identity), so they accrue no stale cold entries and keep the raw additive hits.
    if g.otype.identity.is_none() {
        return Ok(hits);
    }
```

The `identity_governed()` guard (~699-701) stays exactly where it is, ahead of this. The subsequent `let identity = g.otype.identity.clone().ok_or_else(|| … NoIdentity …)?` (~705-710) is now guaranteed `Some` by the carve-out, so it never trips — keep it as a defensive belt (add `// unreachable after the is_none() carve-out above; defensive` to its comment). With `row_filters` possibly empty, `SelectInputs.row_filters = &g.row_filters` is an empty slice, so `compile_select_with` degenerates to `SELECT <identity> WHERE <identity> IN (candidates)` over the merged view — pure suppression, no policy scoping. No other change to the re-query.

- [ ] **Step 4: Identity-dedup the retain**

Replace the plain retain (~745):

```rust
    hits.retain(|h| surviving.contains(&sqlvalue_to_id_string(&h.id)));
```
with a dedup-retain keeping one hit per surviving identity (the nearest — `hits` is in ascending distance order):

```rust
    // Keep only surviving identities, ONE hit per identity (the nearest, since `hits`
    // is distance-ordered): collapses an UPDATE's (stale-cold, fresh-hot) duplicate
    // pair to a single hit. A tombstoned identity survives in neither the merged view
    // nor `surviving`, so it is dropped. May return < k.
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    hits.retain(|h| {
        let id = sqlvalue_to_id_string(&h.id);
        surviving.contains(&id) && seen.insert(id)
    });
```

(`&&` short-circuits: a non-surviving id is dropped without being marked `seen`; a surviving id is kept iff `seen.insert` returns `true`, i.e. first occurrence. The `contains(&id)` borrow ends before `seen.insert(id)` moves `id`, so it borrow-checks.)

- [ ] **Step 5: Run the e2e to verify GREEN**

Run: `buck2 test --console none //src/services/query-api:vector-search-cold-suppression-e2e`
Expected: PASS — UPDATE's identity appears once, DELETE's identity omitted, row-filter path still suppresses, identity-less type returns raw hits with no `NoIdentity` error.

- [ ] **Step 6: Full query-api sweep + clippy**

Run: `buck2 test --console none //src/services/query-api/...`
Expected: `Pass N. Fail 0` — the existing `vector_search_e2e` tests (`search_returns_ranked_ids_for_permitted_subject`, `row_filter_drops_nearest_hit`, etc.) stay green: the ranked-ids test uses an unmutated table (every candidate survives the re-query, deduped set == input, order preserved), and `row_filter_drops_nearest_hit` still drops id=1 (now via the always-run post-filter instead of the old gate). Clippy: `buck2 build --console none '//src/services/query-api:query-api[clippy.txt]'` empty.

(No `.sqlx` change — the survivor query reuses `compile_select_with`.)

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/handler.rs \
        src/services/query-api/tests/vector_search_cold_suppression_e2e.rs \
        src/services/query-api/BUCK
git commit -m "fix(query-api): always run the /search survivor post-filter for identity-bearing types, deduped"
```
