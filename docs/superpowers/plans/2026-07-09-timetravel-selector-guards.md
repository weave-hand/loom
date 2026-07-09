# Time-Travel Selector Guards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the two guards the time-travel read slice deliberately deferred: (A) an `?as_of`/`?as_of_snapshot` selector resolving to a snapshot older than the GC retention horizon currently **under-reads** (partial/empty result once GC reclaims its files) — it must instead return a clear **410 Gone**; (B) `GET /objects/{type}` gates `?as_of_snapshot=` on the mirror's **open-ended-upward liveness range**, so an id above the table's current snapshot silently reads live data — it must 404, matching `GET /datasets/{schema}/{table}`'s exact snapshot-history validation. Both guards land on both GET read surfaces as one work item / one PR.

**Spec of record:** `docs/superpowers/specs/2026-07-06-timetravel-reads-design.md` — the landed time-travel design. This plan implements its two explicitly deferred follow-ups (the *Retention caveat* section and the liveness-vs-history validation asymmetry), merged into register item `#road-timetravel-selector-guards` (`docs/ROADMAP.md:39`).

**Architecture:** Two new methods on the core `Catalog` trait — `snapshot(table, id)` (exact history lookup: the global snapshot `id`, if it exists AND the table is live at it) and `snapshot_horizon(cutoff)` (the GC horizon derivation `max(snapshot_id) WHERE snapshot_time < cutoff`, shared with `iceberg_gc.rs`) — implemented by both adapters and contract-tested. query-api resolution then (1) validates `as_of_snapshot` via `catalog.snapshot` on **both** surfaces (unification, item B) and (2) runs a shared retention guard on every resolved as-of snapshot (item A), rejecting with a new `QueryError::AsOfGone` → 410. The engine and the wire are untouched — validation stays query-api-side, before the `AsOfStatementQuery` ticket, exactly where the spec put resolution.

**Tech Stack:** Rust, axum, sqlx (compile-time `query!`), buck2 `loom_fixture_test`/`rust_test`, the committed `.sqlx` cache.

## Global Constraints

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New fixture tests MUST use `loom_fixture_test` (the query-api `BUCK` already uses it for `as_of_objects_e2e`), or the fixture env (PG binaries, MinIO, boot-slot dir) is missing.
- **After changing any `query!`/`query_scalar!` SQL** run `tools/sqlx-prepare.sh` and commit the `.sqlx/` change; `sqlx-cache-check` enforces freshness. (Local/non-cloud step — `initdb` refuses root; if in a cloud session, surface it to the user.)
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files` (`git add` new files first — prek skips untracked files). Markdown files: no trailing whitespace, exactly one trailing newline.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/`panic`/`indexing_slicing`/`get_unwrap` in production lib/bin code; `#[expect(lint, reason = "...")]` for justified exceptions. Test code is exempted from panic-safety lints by the test macros.
- **No-selector reads stay byte-identical.** The retention guard and the history lookup run **only when an as-of selector is present**; the live-read hot path gains no catalog query. The existing `as_of_objects_e2e` assertions (S1 rows, live rows, 400s, `as_of_snapshot=0` → 404) must stay green.
- **Build/test commands:** build `buck2 build -v0 --console none //src/...`; test `buck2 test --console none <targets>` (cloud: `-M none` on builds, scope tests; full local suite needs `-j 8` for the 8 PG boot-slots).

## Design pins (verified against the code, 2026-07-09)

### The retention horizon is derivable — reclaimed-ness is not recorded

GC (`src/control-plane/postgres/src/iceberg_gc.rs:96-110`) derives its horizon per run and persists nothing: `H = max(snapshot_id) FROM iceberg_mirror.snapshot WHERE snapshot_time < now() - retention` (retention = `Config.gc_retention` from `LOOM_GC_RETENTION_SECS`, default 7 days — `src/services/runtime/src/lib.rs:247,277-281`). Reclaimable rows (`end_snapshot IS NOT NULL AND end_snapshot <= H`) are **deleted outright**; `GcSummary` is returned, not stored. So a read-time guard cannot ask "was this snapshot's data actually reclaimed?" — it must enforce the *retention contract* derived from config + snapshot timestamps. That also makes the guard deterministic: an out-of-window selector is rejected whether or not GC has run yet, instead of behaving differently before/after the GC job fires.

### The exact predicate

Reject a resolved as-of snapshot `at` iff:

```text
at < H            where H = max(snapshot_id) WHERE snapshot_time < now() - gc_retention
AND at < current_snapshot(table).id
```

Derivation (from the GC safety invariant, `iceberg_gc.rs:14-22`): a read at `at` sees a row iff `begin <= at AND end > at`; a reclaimable row has `end <= H`; both hold only when `at < end <= H`, i.e. `at < H`. So `at >= H` is provably complete — strict `<` (this is consistent with, and one snapshot tighter than, the module doc's "in-window readers supply `at > H`"; a read exactly at `H` can never see a row end-capped `<= H`).

The second conjunct is the **quiet-table exemption**: rows of `table` are only ever end-capped at one of the table's *own later write snapshots*, all of which are `<= current_snapshot(table)`. If `at >= current`, no end-cap of this table lies in `(at, H]`, so nothing reclaimable was ever visible at `at` — the read is live-equivalent and complete. Without this, `?as_of_snapshot=<current id>` on a table not written for > retention would 410 (the global `H` advances with *other* tables' commits), which is a false positive. Cost: the `current_snapshot` query runs only when `at < H` — the cold, about-to-be-rejected path.

Both catalog queries run only on the as-of path (selector present).

### Unified snapshot-id validation (item B)

- Today, objects: `resolve_read_snapshot` (`src/services/query-api/src/handler.rs:412-424`) gates via `catalog.schema(table, sid)`, whose `resolve_table` predicate (`src/control-plane/postgres/src/iceberg_catalog.rs:50-65`) is `begin_snapshot <= $3 AND (end_snapshot IS NULL OR end_snapshot > $3)` — open-ended upward: `?as_of_snapshot=999999999` on a live table resolves and reads live data. (The `as_of_objects_e2e.rs` module doc even documents this gap.)
- Today, datasets: `resolve_dataset_snapshot` (`src/services/query-api/src/http.rs:327-342`) fetches `snapshots(table, PageReq::unbounded())` and `.find(|s| s.id == sid)` — bounded by actual history, so a never-allocated id 404s, but O(history) per request.
- Unify on a new `Catalog::snapshot(table, id) -> Result<Option<Snapshot>>`: one indexed query returning the snapshot iff the id **exists in `iceberg_mirror.snapshot`** AND the table is live at it — exactly the `snapshots()` listing predicate restricted to one id. Note the semantics `snapshots()` already has (register prose was slightly loose here): the history is **global** — any global snapshot id at which the table is live is valid, not only the table's own write snapshots. Both surfaces adopt it: objects gain the upper bound (the fix), datasets keep identical semantics and drop the O(history) scan.

### Status codes, per surface

| Condition | Objects | Datasets | Why |
|---|---|---|---|
| Malformed / both selectors | 400 (existing) | 400 (existing) | unchanged |
| Id not in history / not live at id / pre-history timestamp | 404 (existing `AsOfNotFound`) | 404 (existing `NotFound` mapping) | "no such snapshot for this table" — the resource never existed; keeps today's contract, now also fired for above-history ids on objects |
| Resolved snapshot past the retention horizon | **410 Gone** (new `AsOfGone`) | **410 Gone** (same guard fn) | the snapshot *did* exist and still resolves (snapshot rows are never GC'd) but its data is outside the served window — permanently gone, distinct from "never existed"; matches the spec's "clear `410`/`404`" with 410 as the deliberate choice on both surfaces |

## File Structure

**Create:**
- `src/services/query-api/tests/as_of_guards_e2e.rs` — retention-guard + above-history-id e2e (both surfaces).

**Modify (production):**
- `src/control-plane/core/src/catalog.rs` — `Catalog::snapshot` + `Catalog::snapshot_horizon` trait methods.
- `src/control-plane/postgres/src/iceberg_mirror.rs` — shared `horizon_before(pool, cutoff)` helper (the GC horizon query, moved).
- `src/control-plane/postgres/src/iceberg_gc.rs` — `gc_locked` calls `horizon_before` (pure refactor).
- `src/control-plane/postgres/src/iceberg_catalog.rs` — the two new `Catalog` method impls.
- `src/control-plane/memory/src/catalog.rs` — the two new `Catalog` method impls.
- `src/control-plane/postgres/.sqlx/` — regenerated (new `snapshot` query).
- `src/services/query-api/src/handler.rs` — `QueryDeps.gc_retention`; `QueryError::AsOfGone`; `resolve_read_snapshot` history-validates + guards; shared `ensure_within_retention`.
- `src/services/query-api/src/http.rs` — `AppState.gc_retention` + `deps()`; `resolve_dataset_snapshot` uses `catalog.snapshot`; `get_dataset` runs the guard; `query_error_response` 410 arm; utoipa 410 responses on both operations.
- `src/services/query-api/src/serve.rs` — thread `cfg.gc_retention` into `AppState`.

**Modify (tests / support):**
- `src/control-plane/testkit/src/lib.rs` — `catalog_contract` + `catalog_delete_contract` assertions for the two new methods.
- `src/services/query-api/tests/e2e_support.rs` — `gc_retention` on its `AppState` literals; `get_with_retention` helper + a default const.
- `src/services/query-api/tests/as_of_objects_e2e.rs` — above-history-id 404 assertion; module-doc update (it documents the old gap).
- `src/services/query-api/BUCK` — `as_of_guards_e2e` `loom_fixture_test` target.
- Compiler-guided sweeps: ~20 `AppState { … }` literals (query-api tests, `serve.rs`, `http_wire_e2e.rs`, `auth_e2e.rs`) and ~81 `QueryDeps { … }` literals (query-api tests) gain the new field with a 7-day default.

**Docs / registers (final task):**
- `docs/system-capabilities/query-api.md` — rewrite the as-of asymmetry + retention-caveat paragraphs (lines 60-62) and drop the two deferred bullets (lines 103-104).
- `docs/ROADMAP.md` — remove `#road-timetravel-selector-guards` (lines 39-40).

---

## Task 1: `Catalog::snapshot` + `Catalog::snapshot_horizon` — trait, both adapters, testkit contract, `.sqlx`

**Files:**
- Modify: `src/control-plane/core/src/catalog.rs:68-96` (trait)
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs` (shared `horizon_before`), `src/control-plane/postgres/src/iceberg_gc.rs:96-110` (call it), `src/control-plane/postgres/src/iceberg_catalog.rs:203-338` (impls)
- Modify: `src/control-plane/memory/src/catalog.rs:44-153` (impls)
- Modify: `src/control-plane/testkit/src/lib.rs` — `catalog_contract` (line 283) + `catalog_delete_contract` (line 464)
- Regenerate + commit: `src/control-plane/postgres/.sqlx/`

**Interfaces produced (later tasks rely on these EXACT names/types):**

```rust
/// The global snapshot `id`, if it exists in the catalog's history AND `table`
/// is live at it. `None` for an id never allocated (including any id above the
/// newest snapshot), or one at which the table is not live (pre-creation /
/// post-drop). The exact-history gate both time-travel read surfaces validate
/// `?as_of_snapshot=` against. Same per-id semantics as the `snapshots` listing.
async fn snapshot(&self, table: &TableRef, id: SnapshotId) -> Result<Option<Snapshot>>;

/// The GC retention horizon at `cutoff`: the youngest snapshot wholly aged out
/// (max snapshot id with `snapshot_time < cutoff`), or `None` when no snapshot
/// has aged out. The same derivation `iceberg_gc` reclaims under; a read at a
/// snapshot `>=` this horizon is guaranteed complete.
async fn snapshot_horizon(&self, cutoff: OffsetDateTime) -> Result<Option<SnapshotId>>;
```

- [ ] **Step 1: Write the failing contract tests**

In `src/control-plane/testkit/src/lib.rs`, extend `catalog_contract` (after the `snapshot_as_of` block ending ~line 395):

```rust
    // snapshot(id): exact history lookup — a seeded id resolves with its time;
    // an id above the newest global snapshot is None (NOT live-forward like the
    // liveness range); an id below the table's first snapshot is None.
    let got = catalog.snapshot(&t, s0.id).await.unwrap();
    assert_eq!(got.as_ref().map(|s| s.id), Some(s0.id), "seeded id resolves");
    assert_eq!(got.map(|s| s.time), Some(s0.time), "resolved snapshot carries its time");
    assert_eq!(
        catalog.snapshot(&t, SnapshotId(cur.id.0 + 1_000_000)).await.unwrap(),
        None,
        "an id above the newest snapshot does not resolve"
    );
    assert_eq!(
        catalog.snapshot(&t, SnapshotId(s0.id.0 - 1)).await.unwrap(),
        None,
        "an id before the table's first snapshot does not resolve"
    );

    // snapshot_horizon(cutoff): strictly-before max; None when nothing has aged out.
    assert_eq!(
        catalog.snapshot_horizon(s0.time).await.unwrap(),
        None,
        "cutoff at the first snapshot's time: nothing strictly before -> None"
    );
    assert_eq!(
        catalog
            .snapshot_horizon(cur.time + time::Duration::seconds(1))
            .await
            .unwrap(),
        Some(cur.id),
        "cutoff after the newest snapshot -> the newest id"
    );
    if s0.time < s1.time {
        assert_eq!(
            catalog.snapshot_horizon(s1.time).await.unwrap(),
            Some(s0.id),
            "cutoff at s1's time -> s0 (strictly-before semantics)"
        );
    }
```

(The `None` horizon assertion assumes the contract's backend is freshly seeded — both harnesses are: `MemSeeder` on a new `MemoryControlPlane`, the pg harness on a `fresh_db`. If the pg seeder allocates a bootstrap snapshot before `s0`, key the assertion off `hist.first()` instead — adjust to what the seeder actually produces.)

In `catalog_delete_contract` (line 464), after the drop-at-`D` assertions, add: `snapshot(&t, D)` → `None` (not live at the drop snapshot) and `snapshot(&t, <pre-drop seeded id>)` → `Some` (history before the drop stays resolvable).

- [ ] **Step 2: Add the trait methods**

In `src/control-plane/core/src/catalog.rs`, add both methods (doc comments as pinned above) to `trait Catalog` after `snapshots` (line 83). No default impls — every adapter must decide.

- [ ] **Step 3: Run to verify the failure**

Run: `buck2 build -v0 --console none //src/control-plane/...`
Expected: FAIL — `MemoryControlPlane` and `IcebergCatalog` miss the two methods.

- [ ] **Step 4: Memory adapter**

In `src/control-plane/memory/src/catalog.rs` (state: `CatalogState.snapshots: Vec<Snapshot>` + `tables: HashMap<TableRef, Versioned<()>>`):

```rust
async fn snapshot(&self, table: &TableRef, id: SnapshotId) -> Result<Option<Snapshot>> {
    let cat = self.catalog.lock();
    let Some(t) = cat.tables.get(table) else { return Ok(None) };
    Ok(cat.snapshots.iter().find(|sn| sn.id == id && t.live_at(sn.id.0)).cloned())
}

async fn snapshot_horizon(&self, cutoff: OffsetDateTime) -> Result<Option<SnapshotId>> {
    let cat = self.catalog.lock();
    Ok(cat.snapshots.iter().filter(|sn| sn.time < cutoff).map(|sn| sn.id).max())
}
```

- [ ] **Step 5: Postgres adapter + shared horizon helper**

In `src/control-plane/postgres/src/iceberg_mirror.rs`, add (next to `live_table_id`/`dropped_table_ids`):

```rust
/// The GC retention horizon at `cutoff`: max snapshot_id with snapshot_time
/// strictly before it. THE horizon derivation — `iceberg_gc::gc_locked` reclaims
/// under it and `Catalog::snapshot_horizon` guards reads with it; keep them the
/// same query so the two can never drift.
pub(crate) async fn horizon_before(
    pool: &PgPool,
    cutoff: OffsetDateTime,
) -> Result<Option<i64>> {
    sqlx::query_scalar!(
        "select max(snapshot_id) from iceberg_mirror.snapshot where snapshot_time < $1",
        cutoff,
    )
    .fetch_one(pool)
    .await
    .map_err(backend)
}
```

In `iceberg_gc.rs::gc_locked` (lines 100-107), replace the inline horizon `query_scalar!` with `let horizon = crate::iceberg_mirror::horizon_before(pool, cutoff).await?;` (keep the `cutoff` computation line 100 verbatim). Pure refactor; the SQL text is identical so the committed `.sqlx` entry is reused.

In `iceberg_catalog.rs`, add to `impl Catalog for IcebergCatalog`:

```rust
#[tracing::instrument(skip(self), level = "debug")]
async fn snapshot(&self, table: &TableRef, id: SnapshotId) -> Result<Option<Snapshot>> {
    let row = sqlx::query!(
        "select sn.snapshot_id as \"snapshot_id!\", sn.snapshot_time as \"snapshot_time!\", sn.schema_version as \"schema_version!\" \
         from iceberg_mirror.snapshot sn \
         where sn.snapshot_id = $3 and exists ( \
             select 1 from iceberg_mirror.table t \
             where t.table_namespace = $1 and t.table_name = $2 \
               and t.begin_snapshot <= sn.snapshot_id and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id))",
        table.schema,
        table.name,
        id.0,
    )
    .fetch_optional(&self.pool)
    .await
    .map_err(backend)?;
    Ok(row.map(|r| Snapshot {
        id: SnapshotId(r.snapshot_id),
        time: r.snapshot_time,
        schema_version: r.schema_version,
    }))
}

#[tracing::instrument(skip(self), level = "debug")]
async fn snapshot_horizon(&self, cutoff: OffsetDateTime) -> Result<Option<SnapshotId>> {
    Ok(crate::iceberg_mirror::horizon_before(&self.pool, cutoff)
        .await?
        .map(SnapshotId))
}
```

(These are the only two loom-`Catalog` implementors — the `Catalog` impls in `iceberg_writer.rs` and `iceberg_sql_catalog/catalog.rs` are **iceberg-rust's** trait, untouched.)

- [ ] **Step 6: Regenerate the `.sqlx` cache**

Run: `tools/sqlx-prepare.sh` — the new `snapshot` `query!` needs an entry (the horizon query's entry already exists, same text). Commit the `.sqlx/` diff with this task.

- [ ] **Step 7: Run the contract + GC tests**

Run: `buck2 test --console none //src/control-plane/memory:catalog //src/control-plane/postgres:iceberg-catalog //src/control-plane/postgres:iceberg-gc //src/control-plane/postgres:sqlx-cache-check`
(Adjust target names to the actual `BUCK` targets wiring `tests/catalog.rs`, `tests/iceberg_catalog.rs`, `tests/iceberg_gc.rs`.)
Expected: PASS — both adapters satisfy the extended contracts; GC behavior unchanged.

- [ ] **Step 8: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(catalog): snapshot(id) history lookup + snapshot_horizon on the Catalog trait

Exact per-id snapshot resolution (bounded by actual history, unlike the
open-ended liveness range) and the GC retention-horizon derivation, shared
with iceberg_gc via horizon_before so read guard and reclaimer can never
drift. Both adapters + testkit contracts. Groundwork for
road-timetravel-selector-guards; no read-path behavior change yet."
```

---

## Task 2: Item B — validate `?as_of_snapshot=` against snapshot history on both surfaces

**Files:**
- Modify: `src/services/query-api/src/handler.rs:399-438` (`resolve_read_snapshot`)
- Modify: `src/services/query-api/src/http.rs:316-352` (`resolve_dataset_snapshot`)
- Modify: `src/services/query-api/tests/as_of_objects_e2e.rs` (module doc + new assertion)

**Interfaces:** consumes `Catalog::snapshot` (Task 1). No signature changes.

- [ ] **Step 1: Write the failing e2e assertion**

In `src/services/query-api/tests/as_of_objects_e2e.rs`, in `as_of_selectors_resolve_and_apply` after the `as_of_snapshot=0` → 404 block (~line 175), add the assertion the old gate could not pass:

```rust
    // An id ABOVE the newest snapshot -> 404. Previously the object path's
    // open-ended liveness range resolved this as "live" and read current data;
    // history-backed validation (Catalog::snapshot) now rejects it, matching
    // the dataset path.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Thing?as_of_snapshot=999999999",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "as_of_snapshot above history");
```

Update the module doc (lines 10-14): delete the parenthetical explaining why an above-max id is *not* tested, and document that both surfaces now 404 any id absent from the table's snapshot history.

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/services/query-api:as_of_objects_e2e`
Expected: FAIL — the above-history request returns 200 with live rows.

- [ ] **Step 3: Object surface — `resolve_read_snapshot`**

In `handler.rs`, replace the `AsOfSelector::Snapshot` arm (lines 412-425): drop the `catalog.schema(table, sid)` liveness gate and resolve via history —

```rust
        AsOfSelector::Snapshot(id) => {
            let sid = control_plane_core::SnapshotId(*id);
            // Exact-history gate: the id must exist in the catalog's snapshot
            // history AND the table must be live at it. Unlike the previous
            // `schema()` liveness-range check this is bounded above — an id
            // past the newest snapshot 404s instead of reading live data.
            match deps.catalog.snapshot(table, sid).await {
                Ok(Some(s)) => s.id,
                Ok(None) => {
                    return Err(QueryError::AsOfNotFound(format!(
                        "{}.{} has no snapshot {}",
                        table.schema, table.name, id
                    )));
                }
                Err(e) => return Err(QueryError::ControlPlane(e)), // backend fault -> 500
            }
        }
```

Update the function doc (lines 399-403) accordingly. The `AsOfSelector::Time` arm is unchanged (already history-bounded via `snapshot_as_of`).

- [ ] **Step 4: Dataset surface — `resolve_dataset_snapshot`**

In `http.rs` (lines 327-342), replace the `snapshots(table, PageReq::unbounded()) … .find(…)` scan with the O(1) lookup, keeping identical semantics and the 404 mapping:

```rust
        Some(AsOfSelector::Snapshot(id)) => {
            let sid = control_plane_core::SnapshotId(*id);
            // Exact-history gate, shared with the object path (Catalog::snapshot):
            // replaces the previous O(history) snapshots().find(id) scan.
            catalog.snapshot(table, sid).await?.ok_or_else(|| {
                ControlPlaneError::NotFound(format!(
                    "{}.{} has no snapshot {}",
                    table.schema, table.name, id
                ))
            })
        }
```

Drop the now-unused `PageReq` import from `http.rs` if nothing else uses it.

- [ ] **Step 5: Run the e2e**

Run: `buck2 test --console none //src/services/query-api:as_of_objects_e2e`
Expected: PASS — including every pre-existing assertion (S1 rows, live rows, both-params 400, `as_of_snapshot=0` 404, dataset `999999` 404, bad timestamp 400).

- [ ] **Step 6: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "fix(query-api): validate as_of_snapshot against snapshot history on both surfaces

GET /objects/{type} gated the id on the mirror's open-ended liveness range,
so an id above the current snapshot silently read live data. Both surfaces
now resolve via Catalog::snapshot (exists in history AND table live at it):
objects gain the upper bound; datasets keep identical semantics minus the
O(history) scan. Above-history ids 404, matching every other bad-id case."
```

---

## Task 3: Retention plumbing — `gc_retention` through `AppState`/`QueryDeps` (no behavior change)

**Files:**
- Modify: `src/services/query-api/src/http.rs:70-91` (`AppState` + `deps()`), `src/services/query-api/src/handler.rs:111-117` (`QueryDeps`)
- Modify: `src/services/query-api/src/serve.rs:58` (`AppState` literal — `cfg` is in scope)
- Modify: `src/services/query-api/tests/e2e_support.rs` (4 `AppState` literals + a default const)
- Modify: compiler-guided — ~20 `AppState { … }` and ~81 `QueryDeps { … }` literals across query-api tests (incl. `http_wire_e2e.rs`, `auth_e2e.rs`)

**Interfaces produced:**
- `AppState.gc_retention: std::time::Duration` — the deployment's GC retention window (`Config.gc_retention`, `LOOM_GC_RETENTION_SECS`).
- `QueryDeps.gc_retention: std::time::Duration` — threaded by `deps()`.
- `e2e_support::TEST_GC_RETENTION: std::time::Duration` (7 days) — the default every existing test uses.

- [ ] **Step 1: Add the fields**

`AppState` (http.rs, after `default_limit`):

```rust
    /// GC retention window (`LOOM_GC_RETENTION_SECS`). Time-travel reads use it
    /// to reject selectors resolving past the retention horizon (410) — the same
    /// window `gc_table` reclaims under, so guard and reclaimer agree by
    /// construction.
    pub gc_retention: std::time::Duration,
```

`QueryDeps` (handler.rs): `pub gc_retention: std::time::Duration,` with a one-line doc. `AppState::deps()` threads it. `serve.rs:58` passes `gc_retention: cfg.gc_retention,`.

- [ ] **Step 2: Compiler-guided sweep**

Run `buck2 build -v0 --console none //src/services/query-api:query-api` then the test targets; for every `missing field 'gc_retention'`:
- In `e2e_support.rs`: define `pub const TEST_GC_RETENTION: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 3600);` and use it in its 4 `AppState` literals.
- In test files: `gc_retention: e2e_support::TEST_GC_RETENTION,` where the file already deps `:e2e-support`, else a literal `std::time::Duration::from_secs(7 * 24 * 3600)`. The value is inert for these tests (no as-of selector ⇒ the guard never runs); 7 days simply mirrors the production default. Do not grep-and-sed blindly — the compiler enumerates every site.

- [ ] **Step 3: Build the whole tree + run the query-api suite**

Run: `buck2 build -v0 --console none //src/...`
Run: `buck2 test --console none //src/services/query-api:as_of_objects_e2e //src/services/query-api:governed_read //src/services/query-api:datasets_routes //src/services/query-api:http_smoke` (plus any target the sweep touched that looks risky)
Expected: PASS — pure plumbing.

- [ ] **Step 4: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "refactor(query-api): thread gc_retention into AppState/QueryDeps

Plumbing only for the time-travel retention guard: serve.rs forwards
Config.gc_retention (LOOM_GC_RETENTION_SECS); tests default to the 7-day
production value via e2e_support::TEST_GC_RETENTION. No behavior change."
```

---

## Task 4: Item A — the retention-horizon guard (410) on both surfaces

**Files:**
- Modify: `src/services/query-api/src/handler.rs` — `QueryError::AsOfGone` (near line 183); `ensure_within_retention`; call it from `resolve_read_snapshot`
- Modify: `src/services/query-api/src/http.rs` — `query_error_response` 410 arm (line 752-778, the match is total so the compiler forces it); `get_dataset` guard call (after line 293); utoipa `410` responses on both operations (lines 265-270 and the `get_object` responses block ~line 456-462)
- Modify: `src/services/query-api/tests/e2e_support.rs` — `get_with_retention`
- Create: `src/services/query-api/tests/as_of_guards_e2e.rs` + `BUCK` target

**Interfaces produced:**
- `QueryError::AsOfGone(String)` → `StatusCode::GONE`.
- `pub(crate) async fn ensure_within_retention(catalog: &(dyn Catalog + Send + Sync), gc_retention: std::time::Duration, table: &TableRef, at: SnapshotId) -> Result<(), QueryError>` in `handler.rs`.
- `e2e_support::get_with_retention(cp, eng, uri, subject, gc_retention)` — `get` with a caller-chosen retention; `get` delegates with `TEST_GC_RETENTION`.

- [ ] **Step 1: Write the failing e2e**

In `e2e_support.rs`, refactor `get` into `get_with_retention` (same body, `gc_retention` parameter feeding the `AppState` literal) and make `get` delegate with `TEST_GC_RETENTION`. (Extending the shared library per CLAUDE.md, not copy-pasting the router harness.)

Create `src/services/query-api/tests/as_of_guards_e2e.rs`, reusing `as_of_objects_e2e.rs`'s `setup` shape (two appends to `main.thing` → `S1`, `S2`) **plus one later write to a second table** `main.other` (→ global `S3`), so the global horizon can pass the quiet table's current snapshot:

```rust
//! Retention-horizon guard e2e (`road-timetravel-selector-guards`, item A).
//!
//! `main.thing`: S1 lands ids {1,2}, S2 appends {3,4}; a later write to
//! `main.other` allocates global snapshot S3 > S2. With `gc_retention = ZERO`
//! every committed snapshot has aged out, so the horizon H = S3 and:
//!   - objects `?as_of_snapshot={S1}` -> 410 (S1 < H and S1 < thing's current S2).
//!   - objects `?as_of={S1 time}`     -> 410 (the guard covers both selector kinds).
//!   - objects `?as_of_snapshot={S2}` -> 200 ids {1,2,3,4}: the quiet-table
//!     exemption — S2 < H, but S2 == thing's current snapshot, so the read is
//!     live-equivalent and provably complete.
//!   - no selector -> 200 (the guard never runs on the live path).
//!   - datasets `?as_of_snapshot={S1}` -> 410; `{S2}` -> 200 with snapshot_id == S2.
//! With the default (7-day) retention the same S1 selectors stay 200 (in-window).
```

Assertions (subject/grants mirroring `as_of_objects_e2e`; use `get_with_retention(…, Duration::ZERO)` for the guard cases and plain `get` for the in-window control):

1. `get_with_retention(…, "/objects/Thing?as_of_snapshot={s1}", ZERO)` → `StatusCode::GONE`, body mentions the retention horizon.
2. `…"/objects/Thing?as_of={s1_time}"…` → GONE (time selector guarded too).
3. `…"/objects/Thing?as_of_snapshot={s2}"…` → OK, ids `{1,2,3,4}` (quiet-table exemption: `s2 < H` because `main.other` committed `S3` later, yet `s2 ==` thing's current).
4. `…"/objects/Thing"…` (no selector, retention ZERO) → OK — live path untouched.
5. `…"/datasets/main/thing?as_of_snapshot={s1}"…` → GONE; `…{s2}…` → OK with `"snapshot_id" == s2`.
6. `get(…"/objects/Thing?as_of_snapshot={s1}"…)` (default retention) → OK, ids `{1,2}` — in-window time travel unchanged.

(`snapshot_time` is stamped by Postgres at commit and the guard's `now_utc()` runs strictly later on the same host, so retention-ZERO ages every committed snapshot deterministically; if a CI host ever shows clock-skew flakes, switch to `Duration::from_millis(1)` + a 50 ms sleep after seeding.)

Wire `as_of_guards_e2e` in `src/services/query-api/BUCK` as a `loom_fixture_test`, mirroring the `as_of_objects_e2e` target (lines 1931-1947) exactly (deps: `:query-api`, `:e2e-support`, core, postgres, axum, serde_json, time, tokio).

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/services/query-api:as_of_guards_e2e`
Expected: FAIL — the 410 cases return 200 (with under-read-eligible data).

- [ ] **Step 3: `QueryError::AsOfGone` + the 410 mapping**

`handler.rs` (after `AsOfNotFound`, line 183):

```rust
    /// A time-travel selector resolved to a snapshot older than the GC retention
    /// horizon: its data files may already be physically reclaimed, so serving it
    /// could silently under-read. Renders 410 Gone — the snapshot existed (unlike
    /// `AsOfNotFound`) but is permanently outside the served window.
    #[error("as-of snapshot past the retention horizon: {0}")]
    AsOfGone(String),
```

`http.rs::query_error_response` — the total match now fails to compile; add:

```rust
        QueryError::AsOfGone(m) => (StatusCode::GONE, m).into_response(),
```

- [ ] **Step 4: The shared guard**

In `handler.rs`, next to `resolve_read_snapshot`:

```rust
/// Reject a resolved as-of snapshot older than the GC retention horizon.
///
/// Predicate (see the plan/spec): gone iff `at < H` AND `at < current(table)`,
/// where `H = max(snapshot_id) WHERE snapshot_time < now() - gc_retention` —
/// the exact horizon `iceberg_gc` reclaims under (shared derivation:
/// `Catalog::snapshot_horizon`). `at >= H` is provably complete (a reclaimable
/// row has `end <= H`, visible only when `at < end`). `at >= current(table)` is
/// the quiet-table exemption: this table's rows are only end-capped at its own
/// later write snapshots, so nothing reclaimable was ever visible at `at` — a
/// current-snapshot read must not 410 just because OTHER tables kept committing.
/// Deterministic by design: enforced from config + snapshot timestamps whether
/// or not GC has actually run (reclaimed-ness is not recorded anywhere).
pub(crate) async fn ensure_within_retention(
    catalog: &(dyn control_plane_core::Catalog + Send + Sync),
    gc_retention: std::time::Duration,
    table: &control_plane_core::TableRef,
    at: control_plane_core::SnapshotId,
) -> Result<(), QueryError> {
    let cutoff = time::OffsetDateTime::now_utc()
        - time::Duration::seconds(gc_retention.as_secs() as i64);
    let horizon = catalog
        .snapshot_horizon(cutoff)
        .await
        .map_err(QueryError::ControlPlane)?;
    let Some(h) = horizon else { return Ok(()) };
    if at >= h {
        return Ok(());
    }
    let current = catalog
        .current_snapshot(table)
        .await
        .map_err(QueryError::ControlPlane)?;
    if at >= current.id {
        return Ok(());
    }
    Err(QueryError::AsOfGone(format!(
        "{}.{} snapshot {} is older than the GC retention horizon ({}); its data may \
         already be reclaimed — pick a snapshot >= {} or widen LOOM_GC_RETENTION_SECS",
        table.schema, table.name, at.0, h.0, h.0
    )))
}
```

(The cutoff line mirrors `iceberg_gc.rs:100` verbatim, including its accepted `as i64` cast. `SnapshotId` derives `Ord`, so the comparisons are direct.)

At the end of `resolve_read_snapshot`, before `Ok(Some(id))` — covering **both** selector arms:

```rust
    ensure_within_retention(deps.catalog, deps.gc_retention, table, id).await?;
```

- [ ] **Step 5: Dataset surface + OpenAPI**

In `http.rs::get_dataset`, after `resolve_dataset_snapshot` succeeds (line 293), guard only when a selector was given (the live path must not pay the horizon query):

```rust
    if sel.is_some() {
        if let Err(e) = crate::handler::ensure_within_retention(
            catalog,
            st.gc_retention,
            &table_ref,
            snapshot.id,
        )
        .await
        {
            return query_error_response(e, "dataset as-of retention guard");
        }
    }
```

utoipa: add to both operations' `responses(...)` — `get_dataset` (lines 265-270) and `get_object`:

```rust
        (status = 410, description = "Selector resolves to a snapshot older than the GC retention horizon (data may be reclaimed)"),
```

- [ ] **Step 6: Run the e2e + the OpenAPI tests**

Run: `buck2 test --console none //src/services/query-api:as_of_guards_e2e //src/services/query-api:as_of_objects_e2e //src/services/query-api:datasets_routes //src/services/query-api:openapi //src/services/query-api:openapi_gen //src/services/query-api:query_error_http`
Expected: PASS. If an OpenAPI test snapshots the responses set, update its committed golden (the failure names the file) and re-run.

- [ ] **Step 7: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(query-api): 410 retention-horizon guard on time-travel selectors

A selector resolving to a snapshot older than the GC horizon (max snapshot
with snapshot_time < now - LOOM_GC_RETENTION_SECS — the same derivation
gc_table reclaims under, shared via Catalog::snapshot_horizon) now returns
410 Gone on both GET read surfaces instead of silently under-reading.
Quiet-table exemption: a read at/above the table's own current snapshot is
live-equivalent and always allowed. Live (no-selector) reads untouched."
```

---

## Task 5: Docs + register close-out

**Files:**
- Modify: `docs/system-capabilities/query-api.md` (lines 56-62 + 103-104)
- Modify: `docs/ROADMAP.md` (remove lines 39-40)

- [ ] **Step 1: Rewrite the capability doc**

In `docs/system-capabilities/query-api.md` *Time-travel reads (as-of)*:
- Line 58: extend the error-contract sentence — malformed/both ⇒ 400; unresolvable (pre-history timestamp, or an id absent from snapshot history / at which the table is not live) ⇒ 404; a resolved snapshot older than the GC retention horizon ⇒ **410 Gone**.
- Replace the line-60 asymmetry paragraph: both routes now validate `as_of_snapshot` by exact snapshot-history lookup (`Catalog::snapshot`); an id above the current snapshot 404s on both.
- Replace the line-62 retention caveat: describe the enforced guard (predicate: `at < H` with `H = max(snapshot_id) WHERE snapshot_time < now() - LOOM_GC_RETENTION_SECS`, and the at-or-above-current-snapshot exemption), noting it is contract-based — enforced whether or not GC has run. Keep the `#fut-iceberg-time-travel-schema` sentence.
- Remove the two deferred bullets at lines 103-104 (`#fut-timetravel-snapshot-id-validation`, `#fut-timetravel-retention-guard` — both ids already retired from FUTURE when the merged ROADMAP item was cut).

- [ ] **Step 2: Close the register item**

Remove the `#road-timetravel-selector-guards` entry (`docs/ROADMAP.md:39-40`) — registers carry open work only; the capability is now documented in `docs/system-capabilities/query-api.md`. Run the `loom-docs-update` skill if finishing the branch in the same session.

Run: `bash tools/docs.sh validate`
Expected: clean.

- [ ] **Step 3: Full verification + final commit**

Run: `buck2 build -v0 --console none //src/...`
Run: `buck2 test --console none //src/services/query-api:as_of_objects_e2e //src/services/query-api:as_of_guards_e2e //src/control-plane/memory:catalog //src/control-plane/postgres:iceberg-catalog //src/control-plane/postgres:iceberg-gc //src/control-plane/postgres:sqlx-cache-check` — then the full suite locally (`buck2 test --console none -j 8 //src/...`) or the btd-affected set in a cloud session.
Expected: all PASS.

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "docs(query-api): time-travel selector guards shipped; close road-timetravel-selector-guards"
```

Then finish the branch per `superpowers:finishing-a-development-branch` (push + open PR — never a local merge).
