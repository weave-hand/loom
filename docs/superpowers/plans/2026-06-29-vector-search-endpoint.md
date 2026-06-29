# External `/search` vector kNN endpoint — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the engine-side vector kNN search to external clients via a governed `POST /search/{type}/{index_name}` endpoint on query-api, with ACL enforcement and optional per-query `nprobe`/`ef_search` knobs.

**Architecture:** query-api stays a zero-DataFusion wire client. It sends a typed `VectorSearchTicket` over the engine's Flight `do_get` to get top-k `{id, distance}`, then post-filters those candidate ids against the subject's ACL row-filters via a governed identity-projection `SELECT … WHERE id IN (…) AND (<row_filters>)` over the existing SQL `execute` path. Two query-time knobs (`nprobe` for IVF, `ef_search` for HNSW) are threaded request → ticket → the decoded index via a new `VectorIndex::apply_query_knobs` trait method. A query-dim mismatch is validated engine-side and surfaced as 400.

**Tech Stack:** Rust, axum (query-api HTTP), Arrow Flight (engine wire), DataFusion (engine), Iceberg + Postgres mirror, buck2 build.

**Spec:** `docs/superpowers/specs/2026-06-29-vector-search-endpoint-design.md`

## Global Constraints

- **Build/test with buck2 only, never cargo.** Build: `buck2 build //src/...`. Test a target: `buck2 test //src/services/query-api:<target>`. Never pipe `buck2 test`/`bxl` through `tail`/`head` — redirect to a file and grep it: `buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **Tests are `rust_test` integration targets only — NO inline `#[cfg(test)]`/`#[test]` in `src/**`.** Put unit tests in a sibling `tests/<name>.rs` wired as its own `rust_test` in the crate's `BUCK`. The `no-inline-tests` prek hook enforces this.
- **Fixture-backed tests (hermetic Postgres / Iceberg) MUST use the `loom_fixture_test` macro**, not a bare `rust_test`, or they route to remote execution and fail as root. Pure-logic tests use `rust_test` via the `loom_rust_test` wrapper (already loaded in each `BUCK`).
- **Strict clippy (pedantic + restriction).** `unwrap_used`/`expect_used`/`panic`/`indexing_slicing`/`as`-casts (`cast_possible_truncation` etc.) are enforced on production code. Use `#[expect(lint, reason = "…")]` locally (bare `#[allow]` is rejected — `reason` is required). Test code is exempted from panic-safety lints via the wrapper.
- **`K_MAX = 1000`** — the request `k` must be in `1..=K_MAX`; reject out-of-range with 400 before any engine call.
- **Request struct uses `#[serde(deny_unknown_fields)]`.** Unknown fields, missing `query`/`k`, empty `query`, or `k` out of range → **400** before the engine call.
- **Response shape:** `{ "results": [ { "id": <int|string>, "distance": <number> }, … ] }` — ascending distance, ≤ k. The `id` JSON type follows the index's identity kind (integer or string). All candidates filtered out by row-filters ⇒ `200 { "results": [] }`.
- **Error codes:** 400 (bad body / `k` out of range / empty query / query dim ≠ index dim), 403 (no `Read` grant OR unknown type — uniform, no existence leak), 404 (no built index named `{index_name}` for the type), 500 (engine/internal — logged via `internal_error`, no detail leaked).
- **Governance = coarse type `Read` gate + row-filter post-filter.** No column masking (no properties returned). The post-filter is secure and may return `< k`.
- **Knobs apply only to the matching index kind:** `nprobe` → IVF-Flat only, `ef_search` → HNSW only, Flat ignores both.
- **Markdown files end with exactly one trailing newline and no trailing whitespace.**

---

## File Structure

- `src/control-plane/core/src/vector_index.rs` — add `VectorIndex::apply_query_knobs` (trait default + IVF/HNSW overrides; refactor the existing `with_nprobe`/`with_ef_search` builders to delegate to in-place setters). **Task 1.**
- `src/control-plane/core/tests/vector_index.rs` — knob unit tests. **Task 1.**
- `src/services/engine-wire/src/flight.rs` — `VectorSearchTicket` gains `nprobe`/`ef_search`; new `VectorSearchError`; `FlightTableClient::vector_search` classifies the gRPC status code. **Task 2.**
- `src/services/engine/tests/vector_search_flight.rs` — update the two ticket constructors + the no-index error assertion. **Task 2.**
- `src/services/engine-serving/src/serving.rs` — `EngineServingError::DimMismatch`. **Task 3.**
- `src/services/engine-serving/src/vector_search.rs` — `vector_search` gains `nprobe`/`ef_search`, the dim check, and the `apply_query_knobs` call. **Task 3.**
- `src/services/engine/src/flight.rs` — `do_get_vector_search` passes the knobs and maps `DimMismatch`. **Task 3.**
- `src/services/engine-serving/tests/vector_search.rs` — update existing call sites; add knob/dim/Flat-noop tests. **Task 3.**
- `src/services/query-api/src/serving.rs` — `ServingError` gains `NoIndex`/`DimMismatch`; `ServingEngine::vector_search` trait method. **Task 4.**
- `src/services/query-api/src/engine_client.rs` — `EngineServingClient` holds a `FlightTableClient`; implements `vector_search`. **Task 4.**
- `src/services/query-api/tests/e2e_support.rs` — `InProcessServingEngine` gains a `pool` and a `vector_search` impl. **Task 4** (plumbing) and **Task 6** (seeding helper).
- `src/services/query-api/src/handler.rs` — `VectorHit`, `VectorSearchQuery`, `vector_search` governed flow (coarse gate + engine call + row-filter post-filter). **Task 5.**
- `src/services/query-api/src/http.rs` — `VectorSearchRequest`, `K_MAX`, the `POST /search/:type/:index_name` route + `post_search` handler + error mapping. **Task 5.**
- `src/services/query-api/tests/vector_search_filter.rs` — pure unit test of the post-filter SQL compile + request validation. **Task 5.**
- `src/services/query-api/tests/vector_search_e2e.rs` — full governed e2e. **Task 6.**
- `BUCK` files alongside each new test source. **Tasks 1, 5, 6.**

---

### Task 1: `VectorIndex::apply_query_knobs` (core)

Add a per-query knob application path to the index trait, applied after deserialization and before search. The existing consuming builders `with_nprobe`/`with_ef_search` are refactored to delegate to in-place setters so there is no duplicated clamp logic.

**Files:**
- Modify: `src/control-plane/core/src/vector_index.rs`
- Test: `src/control-plane/core/tests/vector_index.rs` (existing target — extend it)

**Interfaces:**
- Produces: `VectorIndex::apply_query_knobs(&mut self, nprobe: Option<u32>, ef_search: Option<u32>)` — trait method, default no-op. IVF applies `nprobe` (clamped `[1, nlist]`); HNSW applies `ef_search` (clamped `≥ 1`); Flat ignores both.
- Produces: `IvfFlatIndex::set_nprobe(&mut self, nprobe: u32)` and `HnswIndex::set_ef_search(&mut self, ef_search: u32)` — the in-place setters the builders and the trait method share.

- [ ] **Step 1: Write the failing test**

Add to `src/control-plane/core/tests/vector_index.rs` (append; reuse the file's existing helpers/imports — it already constructs Flat/IVF/HNSW indexes). The IVF and HNSW constructors used below already exist in this test file or in `vector_index.rs`'s public surface; mirror how the existing tests in this file build each index. If a builder for a small index is not already imported, build via the same public path the sibling tests use.

```rust
#[test]
fn apply_query_knobs_sets_ivf_nprobe_clamped() {
    // Build a small IVF index with nlist >= 2 the same way the existing IVF tests do.
    let mut idx = small_ivf_index(); // helper mirroring existing IVF test construction
    // Above nlist clamps to nlist; the search still returns results.
    idx.apply_query_knobs(Some(9999), None);
    let hits = idx.search(&query_vec(), 2);
    assert!(!hits.is_empty(), "search works after nprobe override");
}

#[test]
fn apply_query_knobs_sets_hnsw_ef_search_min_one() {
    let mut idx = small_hnsw_index(); // helper mirroring existing HNSW test construction
    idx.apply_query_knobs(None, Some(0)); // clamps up to 1
    let hits = idx.search(&query_vec(), 1);
    assert!(!hits.is_empty(), "search works after ef_search override");
}

#[test]
fn apply_query_knobs_is_noop_on_flat() {
    let mut idx = small_flat_index(); // helper mirroring existing Flat test construction
    let before = idx.search(&query_vec(), 2);
    idx.apply_query_knobs(Some(4), Some(64)); // both ignored by Flat
    let after = idx.search(&query_vec(), 2);
    assert_eq!(before, after, "Flat ignores both knobs");
}
```

Note: if the existing test file has no `small_*_index()`/`query_vec()` helpers, add small local helpers at the top of the file that build a 2–4 row index per kind, copying the construction the file's existing tests already use. Keep them in the test file (not `src/`).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test //src/control-plane/core:vector_index > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t1.log`
Expected: FAIL — `apply_query_knobs` does not exist (`no method named apply_query_knobs`).

- [ ] **Step 3: Add the trait method (default no-op)**

In `src/control-plane/core/src/vector_index.rs`, add to the `VectorIndex` trait (after `search`):

```rust
    /// Apply per-query tuning knobs to a decoded index before searching.
    /// `nprobe` tunes IVF-Flat probe count; `ef_search` tunes HNSW candidate width.
    /// The default is a no-op (e.g. `FlatIndex`); each implementation applies only
    /// the knob relevant to its kind and ignores the other.
    fn apply_query_knobs(&mut self, nprobe: Option<u32>, ef_search: Option<u32>) {
        let _ = (nprobe, ef_search);
    }
```

- [ ] **Step 4: Refactor IVF builder to an in-place setter + override the trait method**

In `IvfFlatIndex`'s impl block, replace the body of `with_nprobe` to delegate, and add `set_nprobe`:

```rust
    /// Override the query-time probe count in place (clamped to `[1, nlist]`). No-op when empty.
    pub fn set_nprobe(&mut self, nprobe: u32) {
        if self.nlist > 0 {
            self.nprobe = nprobe.clamp(1, self.nlist);
        }
    }

    /// Override the query-time probe count (clamped to `[1, nlist]`). No-op when empty.
    #[must_use]
    pub fn with_nprobe(mut self, nprobe: u32) -> IvfFlatIndex {
        self.set_nprobe(nprobe);
        self
    }
```

Add the trait override (in the `impl VectorIndex for IvfFlatIndex` block):

```rust
    fn apply_query_knobs(&mut self, nprobe: Option<u32>, _ef_search: Option<u32>) {
        if let Some(n) = nprobe {
            self.set_nprobe(n);
        }
    }
```

- [ ] **Step 5: Refactor HNSW builder to an in-place setter + override the trait method**

In `HnswIndex`'s impl block:

```rust
    /// Override the query-time candidate width in place (clamped to `>= 1`). No-op when empty.
    pub fn set_ef_search(&mut self, ef_search: u32) {
        if !self.keys.is_empty() {
            self.ef_search = ef_search.max(1);
        }
    }

    /// Override the query-time candidate width (clamped to `>= 1`). No-op when empty.
    #[must_use]
    pub fn with_ef_search(mut self, ef_search: u32) -> HnswIndex {
        self.set_ef_search(ef_search);
        self
    }
```

Add the trait override (in the `impl VectorIndex for HnswIndex` block):

```rust
    fn apply_query_knobs(&mut self, _nprobe: Option<u32>, ef_search: Option<u32>) {
        if let Some(e) = ef_search {
            self.set_ef_search(e);
        }
    }
```

`FlatIndex` needs no override — it uses the trait default.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `buck2 test //src/control-plane/core:vector_index > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t1.log`
Expected: PASS. Also confirm clippy is clean: `buck2 build '//src/control-plane/core:core[clippy.txt]' > /tmp/c1.log 2>&1; cat /tmp/c1.log` (empty == clean).

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/core/src/vector_index.rs src/control-plane/core/tests/vector_index.rs
git commit -m "feat(core): VectorIndex::apply_query_knobs for per-query nprobe/ef_search"
```

---

### Task 2: `VectorSearchTicket` knobs + code-preserving `VectorSearchError` (engine-wire)

The ticket gains the two optional knobs (serde-defaulted so older encoders still decode). The engine-wire client currently flattens the gRPC `tonic::Status` code to a string via `be`, losing the 404/400 distinction. Add a small `VectorSearchError` that classifies the status *code* (NotFound → `NoIndex`, InvalidArgument → `DimMismatch`, else `Engine`) so query-api can map it to the right HTTP status.

**Files:**
- Modify: `src/services/engine-wire/src/flight.rs`
- Modify: `src/services/engine-wire/BUCK` (add `//third-party:thiserror` to the library deps)
- Test: `src/services/engine/tests/vector_search_flight.rs` (existing target — update constructors + assertions)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `VectorSearchTicket { schema, name, index_name, query: Vec<f32>, k: u32, nprobe: Option<u32>, ef_search: Option<u32> }` (the two new fields are `#[serde(default)]`).
- Produces: `pub enum VectorSearchError { NoIndex(String), DimMismatch(String), Engine(String) }` (in `engine-wire`).
- Produces: `FlightTableClient::vector_search(&self, ticket: VectorSearchTicket) -> std::result::Result<Vec<RecordBatch>, VectorSearchError>` (return type changes from `Result<Vec<RecordBatch>>`).

- [ ] **Step 1: Update the two ticket constructors + the no-index assertion in the flight test (write the failing expectation)**

In `src/services/engine/tests/vector_search_flight.rs`, every `VectorSearchTicket { … }` literal (two of them, near lines 257 and 342) must add `nprobe: None, ef_search: None,`. The `vector_search_no_index_is_not_found` test currently asserts on a `ControlPlaneError`/string; change it to assert the new typed variant:

```rust
let err = client.vector_search(ticket).await.expect_err("missing index");
assert!(
    matches!(err, engine_wire::flight::VectorSearchError::NoIndex(_)),
    "missing index classifies as NoIndex, got {err:?}"
);
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/services/engine:vector_search_flight > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t2.log`
Expected: FAIL — `VectorSearchError` does not exist and `VectorSearchTicket` has no `nprobe`/`ef_search` fields.

- [ ] **Step 3: Add the ticket fields**

In `src/services/engine-wire/src/flight.rs`, extend the struct (keep `deny_unknown_fields`):

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorSearchTicket {
    pub schema: String,
    pub name: String,
    pub index_name: String,
    pub query: Vec<f32>,
    pub k: u32,
    /// IVF-Flat probe count for this query (ignored by other kinds). Defaults to `None`.
    #[serde(default)]
    pub nprobe: Option<u32>,
    /// HNSW candidate width for this query (ignored by other kinds). Defaults to `None`.
    #[serde(default)]
    pub ef_search: Option<u32>,
}
```

`encode`/`decode` are unchanged.

- [ ] **Step 4: Add `VectorSearchError`**

In the same file (above `FlightTableClient`), add:

```rust
/// Outcome of a kNN `do_get` that preserves the engine's gRPC status *code*,
/// which `client::be` would otherwise flatten to a string. Lets query-api map a
/// missing index to 404 and a query-dim mismatch to 400.
#[derive(Debug, thiserror::Error)]
pub enum VectorSearchError {
    #[error("no vector index: {0}")]
    NoIndex(String),
    #[error("dimension mismatch: {0}")]
    DimMismatch(String),
    #[error("engine: {0}")]
    Engine(String),
}
```

**`thiserror` is NOT currently a dep of `engine-wire`** (its BUCK deps are tonic/prost/arrow-flight/futures). Add `//third-party:thiserror` to the `rust_library` deps in `src/services/engine-wire/BUCK` (it is already vendored — just the BUCK edit). `tonic` (for `Status`/`Code` in Step 5) IS already a dep.

- [ ] **Step 5: Classify the status code in `vector_search`**

Replace `FlightTableClient::vector_search` so the `do_get` error is matched on `tonic::Status::code()` before any flattening, and stream-collection errors fall to `Engine`:

```rust
    /// Send a [`VectorSearchTicket`] via `do_get` and collect the kNN result rows.
    /// The engine's gRPC status code is preserved: `NotFound` → [`VectorSearchError::NoIndex`],
    /// `InvalidArgument` → [`VectorSearchError::DimMismatch`], anything else → `Engine`.
    pub async fn vector_search(
        &self,
        ticket: VectorSearchTicket,
    ) -> std::result::Result<Vec<RecordBatch>, VectorSearchError> {
        let resp = self
            .inner
            .clone()
            .do_get(Ticket {
                ticket: ticket.encode().into(),
            })
            .await
            .map_err(|s: tonic::Status| match s.code() {
                tonic::Code::NotFound => VectorSearchError::NoIndex(s.message().to_string()),
                tonic::Code::InvalidArgument => {
                    VectorSearchError::DimMismatch(s.message().to_string())
                }
                _ => VectorSearchError::Engine(s.message().to_string()),
            })?;
        let stream = FlightRecordBatchStream::new_from_flight_data(
            resp.into_inner()
                .map_err(arrow_flight::error::FlightError::from),
        );
        stream
            .try_collect()
            .await
            .map_err(|e| VectorSearchError::Engine(e.to_string()))
    }
```

(`tonic` is already a dep here — `FlightServiceClient` is tonic-based. Confirm the import path of `Status`/`Code`; use fully-qualified `tonic::Status`/`tonic::Code` to avoid a new `use`.)

- [ ] **Step 6: Run the flight test to verify it passes**

Run: `buck2 test //src/services/engine:vector_search_flight > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t2.log`
Expected: PASS (this is a fixture test — it boots the engine + postgres; it is already wired via `loom_fixture_test`). Also clippy: `buck2 build '//src/services/engine-wire:engine-wire[clippy.txt]' > /tmp/c2.log 2>&1; cat /tmp/c2.log`.

- [ ] **Step 7: Commit**

```bash
git add src/services/engine-wire/src/flight.rs src/services/engine-wire/BUCK src/services/engine/tests/vector_search_flight.rs
git commit -m "feat(engine-wire): ticket nprobe/ef_search + code-preserving VectorSearchError"
```

---

### Task 3: `vector_search` knobs + dim check (engine-serving + engine)

Thread the knobs into the engine's serving function, validate the query dimension against the mirror row, and emit a new `DimMismatch` error mapped to `invalid_argument` at the Flight boundary. The `engine_serving::vector_search` signature gains two trailing params — **every existing call site must be updated** (the `do_get_vector_search` caller and ~11 call sites in `engine-serving/tests/vector_search.rs`).

**Files:**
- Modify: `src/services/engine-serving/src/serving.rs` (add `DimMismatch` variant)
- Modify: `src/services/engine-serving/src/vector_search.rs` (signature + dim check + apply_query_knobs)
- Modify: `src/services/engine/src/flight.rs` (`do_get_vector_search` passes knobs, maps `DimMismatch`)
- Modify/Test: `src/services/engine-serving/tests/vector_search.rs` (update call sites + add tests)

**Interfaces:**
- Consumes: `VectorIndex::apply_query_knobs` (Task 1); `VectorSearchTicket.nprobe`/`.ef_search` (Task 2).
- Produces: `engine_serving::vector_search(catalog: &SqlCatalog, pool: &PgPool, table: &TableRef, index_name: &str, query: &[f32], k: usize, nprobe: Option<u32>, ef_search: Option<u32>) -> Result<RecordBatch, EngineServingError>`.
- Produces: `EngineServingError::DimMismatch(String)`.

- [ ] **Step 1: Add the failing tests (knob passthrough + dim mismatch + Flat-noop)**

Append to `src/services/engine-serving/tests/vector_search.rs`. These reuse the file's existing `seed_and_build`, `seed_and_build_ivf`, `ids`, `distances` helpers (4 cold rows: id 1=[1,0,0,0] … id 4=[0,0,0,1]). Note the file's call sites of `engine_serving::vector_search` will all change to the new 8-arg signature in Step 4 — write these new tests with the 8-arg form:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivf_nprobe_full_reproduces_exact_match() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef { schema: "wh".into(), name: "docs".into() };
    let (catalog, pool, _cp, _wh) = seed_and_build_ivf(&fx, &db, Metric::Cosine).await;

    // nprobe = nlist (2) probes every cluster → the exact nearest is always found.
    let batch = engine_serving::vector_search(
        &catalog, &pool, &table, "by_ivf", &[1.0_f32, 0.0, 0.0, 0.0], 1, Some(2), None,
    )
    .await
    .expect("ivf nprobe=nlist");
    assert_eq!(ids(&batch)[0], 1, "nprobe=nlist reproduces the exact match");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flat_ignores_both_knobs() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef { schema: "wh".into(), name: "docs".into() };
    let (catalog, pool, _cp, _wh) = seed_and_build(&fx, &db, Metric::Cosine).await;

    // Flat: nprobe/ef_search must not change results.
    let plain = engine_serving::vector_search(
        &catalog, &pool, &table, "by_flat", &[1.0_f32, 0.0, 0.0, 0.0], 2, None, None,
    ).await.expect("flat plain");
    let knobbed = engine_serving::vector_search(
        &catalog, &pool, &table, "by_flat", &[1.0_f32, 0.0, 0.0, 0.0], 2, Some(4), Some(64),
    ).await.expect("flat knobbed");
    assert_eq!(ids(&plain), ids(&knobbed), "Flat ignores knobs");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_dim_mismatch_is_error() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef { schema: "wh".into(), name: "docs".into() };
    let (catalog, pool, _cp, _wh) = seed_and_build(&fx, &db, Metric::Cosine).await;

    // Index dim is 4; a length-3 query must be a deterministic DimMismatch, never a panic.
    let err = engine_serving::vector_search(
        &catalog, &pool, &table, "by_flat", &[1.0_f32, 0.0, 0.0], 2, None, None,
    )
    .await
    .expect_err("dim mismatch");
    assert!(
        matches!(err, EngineServingError::DimMismatch(_)),
        "expected DimMismatch, got {err:?}"
    );
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `buck2 test //src/services/engine-serving:vector_search > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t3.log`
Expected: FAIL — compile error (8-arg signature does not exist yet, `DimMismatch` missing).

- [ ] **Step 3: Add the `DimMismatch` variant**

In `src/services/engine-serving/src/serving.rs`, add to `EngineServingError`:

```rust
    /// The query vector's length does not match the index's declared dimension.
    /// Callers should surface this as a 400/bad-request.
    #[error("dimension mismatch: {0}")]
    DimMismatch(String),
```

- [ ] **Step 4: Update `vector_search` (signature + dim check + apply_query_knobs)**

In `src/services/engine-serving/src/vector_search.rs`, change the signature and body. Add the two params; insert the dim check immediately after the mirror row is resolved (`row` carries `dim: i32`); make the decoded index `mut` and apply the knobs before `search`:

```rust
pub async fn vector_search(
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    index_name: &str,
    query: &[f32],
    k: usize,
    nprobe: Option<u32>,
    ef_search: Option<u32>,
) -> Result<RecordBatch, EngineServingError> {
    // … unchanged through the `lookup_vector_index` → `row` resolution …

    // Validate the query dimension against the index's declared dim BEFORE decoding.
    let dim = usize::try_from(row.dim).map_err(to_serving)?;
    if query.len() != dim {
        return Err(EngineServingError::DimMismatch(format!(
            "query has {} dims but index `{}` on {}.{} expects {}",
            query.len(), index_name, table.schema, table.name, dim
        )));
    }

    // … unchanged: load_table / file_io …
    let mut idx = read_vector_index(&file_io, &row.puffin_path)
        .await
        .map_err(to_serving)?;
    idx.apply_query_knobs(nprobe, ef_search);
    let cold: Vec<(VectorKey, f32)> = idx.search(query, k);

    // … unchanged: hot delta merge, merge_topk, build_result_batch …
}
```

(Keep every other line as-is. The only changes are the two new params, the dim-check block, and `let mut idx` + the `apply_query_knobs` call replacing `let idx`.)

- [ ] **Step 5: Update the engine Flight handler**

In `src/services/engine/src/flight.rs`, `do_get_vector_search`: pass the ticket knobs and add the `DimMismatch` → `invalid_argument` mapping:

```rust
    let batch = engine_serving::vector_search(
        &self.catalog,
        &self.pool,
        &table,
        &vs.index_name,
        &vs.query,
        vs.k as usize,
        vs.nprobe,
        vs.ef_search,
    )
    .await
    .map_err(|e| match e {
        engine_serving::EngineServingError::NoIndex(msg) => Status::not_found(msg),
        engine_serving::EngineServingError::DimMismatch(msg) => Status::invalid_argument(msg),
        other => Status::internal(other.to_string()),
    })?;
```

(The `vs.k as usize` cast is pre-existing; do not introduce new casts elsewhere — `usize::try_from` is used in Step 4 for `row.dim`.)

- [ ] **Step 6: Update all existing call sites in the engine-serving test file**

In `src/services/engine-serving/tests/vector_search.rs`, every existing `engine_serving::vector_search(&catalog, &pool, &table, "by_…", &[…], k)` call (the cosine/l2/merge/ivf/hnsw tests, ~11 sites) gains a trailing `, None, None`. Do not change their assertions.

- [ ] **Step 7: Run the engine-serving + engine tests to verify they pass**

Run: `buck2 test //src/services/engine-serving:vector_search //src/services/engine:vector_search_flight > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t3.log`
Expected: PASS. Clippy: `buck2 build '//src/services/engine-serving:engine-serving[clippy.txt]' '//src/services/engine:engine[clippy.txt]' > /tmp/c3.log 2>&1; cat /tmp/c3.log`.

- [ ] **Step 8: Commit**

```bash
git add src/services/engine-serving/src/serving.rs src/services/engine-serving/src/vector_search.rs src/services/engine/src/flight.rs src/services/engine-serving/tests/vector_search.rs
git commit -m "feat(engine-serving): vector_search query knobs + dimension check"
```

---

### Task 4: query-api serving plumbing — `ServingEngine::vector_search`

Give query-api a typed way to call the engine's kNN path. The trait gains a `vector_search` method **with a default body that errors** — there are 11 other `impl ServingEngine` stubs across the query-api and transform test suites that must keep compiling untouched; only the two engines that actually serve search override it. The production `EngineServingClient` builds a `FlightTableClient` alongside its existing `FlightSqlClient` and maps `VectorSearchError` → the extended `ServingError`. The in-process test engine gains a `pool` and serves directly via `engine_serving::vector_search`.

**Files:**
- Modify: `src/services/query-api/src/serving.rs` (`ServingError` variants + trait method)
- Modify: `src/services/query-api/src/engine_client.rs` (`EngineServingClient` second client + impl)
- Modify: `src/services/query-api/tests/e2e_support.rs` (`InProcessServingEngine` + pool + `SqlCatalog` + impl)
- Modify: `src/services/query-api/BUCK` (add `//third-party:iceberg` and `//third-party:tempfile` to the `:e2e-support` **library** deps — currently absent; the `SqlCatalog`/`LocalFsStorageFactory`/`TempDir` recipe needs them)

**Interfaces:**
- Consumes: `VectorSearchTicket` (+knobs), `FlightTableClient::vector_search`, `VectorSearchError` (Task 2); `engine_serving::vector_search` 8-arg (Task 3); `batches_to_rows` (existing).
- Produces: `ServingError::NoIndex(String)` and `ServingError::DimMismatch(String)`.
- Produces: trait method (with a **default body** so the 11 non-serving stubs compile unchanged)
  ```rust
  async fn vector_search(
      &self,
      table: &control_plane_core::TableRef,
      index_name: &str,
      query: &[f32],
      k: usize,
      nprobe: Option<u32>,
      ef_search: Option<u32>,
  ) -> Result<Rows, ServingError> {
      let _ = (table, index_name, query, k, nprobe, ef_search);
      Err(ServingError::Engine("vector search not supported by this engine".to_string()))
  }
  ```
  The two real impls (`EngineServingClient`, `InProcessServingEngine`) override it. Returns a 2-column `Rows` (`columns = ["id", "_distance"]`) with one row per hit, ascending distance.

- [ ] **Step 1: Extend `ServingError` and add the trait method**

In `src/services/query-api/src/serving.rs`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum ServingError {
    #[error("serving engine: {0}")]
    Engine(String),
    /// No built vector index for the requested name/type → 404.
    #[error("no vector index: {0}")]
    NoIndex(String),
    /// Query vector length != index dim → 400.
    #[error("dimension mismatch: {0}")]
    DimMismatch(String),
}
```

Add to the `ServingEngine` trait (after `fetch_rows`, before `dialect`). **It MUST have a default body** — there are 11 `impl ServingEngine` test stubs (in `src/services/query-api/tests/*.rs` and `src/services/transform/tests/transform_serving_support.rs`) that this slice does not touch; a method with no default would force all of them to implement it and break the build:

```rust
    /// Typed kNN search over a named vector index. Returns a 2-column `Rows`
    /// (`id`, `_distance`) in ascending-distance order, ≤ `k` rows. The default
    /// errors — only engines that actually serve search override it.
    async fn vector_search(
        &self,
        table: &control_plane_core::TableRef,
        index_name: &str,
        query: &[f32],
        k: usize,
        nprobe: Option<u32>,
        ef_search: Option<u32>,
    ) -> Result<Rows, ServingError> {
        let _ = (table, index_name, query, k, nprobe, ef_search);
        Err(ServingError::Engine(
            "vector search not supported by this engine".to_string(),
        ))
    }
```

(Confirm `TableRef` import path; `control_plane_core::TableRef` is used throughout query-api. Because the method has a default, the 11 untouched stubs need no edit.)

- [ ] **Step 2: Build a `FlightTableClient` in `EngineServingClient` and implement `vector_search`**

In `src/services/query-api/src/engine_client.rs`:

```rust
pub struct EngineServingClient {
    sql: FlightSqlClient,
    table: engine_wire::flight::FlightTableClient,
}

impl EngineServingClient {
    pub async fn connect(socket: impl Into<String>) -> Result<Self, ServingError> {
        let socket = socket.into();
        let sql = FlightSqlClient::connect(socket.clone())
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        let table = engine_wire::flight::FlightTableClient::connect(socket)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        Ok(Self { sql, table })
    }
}
```

Update the existing `fetch_rows` to call `self.sql.execute(...)` (rename of the field). Add to the `impl ServingEngine`:

```rust
    async fn vector_search(
        &self,
        table: &control_plane_core::TableRef,
        index_name: &str,
        query: &[f32],
        k: usize,
        nprobe: Option<u32>,
        ef_search: Option<u32>,
    ) -> Result<Rows, ServingError> {
        use engine_wire::flight::{VectorSearchError, VectorSearchTicket};
        let k_u32 = u32::try_from(k).map_err(|_| {
            ServingError::Engine("k exceeds u32 range".to_string())
        })?;
        let ticket = VectorSearchTicket {
            schema: table.schema.clone(),
            name: table.name.clone(),
            index_name: index_name.to_string(),
            query: query.to_vec(),
            k: k_u32,
            nprobe,
            ef_search,
        };
        let batches = self.table.vector_search(ticket).await.map_err(|e| match e {
            VectorSearchError::NoIndex(m) => ServingError::NoIndex(m),
            VectorSearchError::DimMismatch(m) => ServingError::DimMismatch(m),
            VectorSearchError::Engine(m) => ServingError::Engine(m),
        })?;
        Ok(batches_to_rows(batches))
    }
```

Confirm `engine-wire` is in query-api's `BUCK` deps (it provides `FlightSqlClient` already, so it is).

- [ ] **Step 3: Give the in-process test engine a pool + a `vector_search` impl**

In `src/services/query-api/tests/e2e_support.rs`:

```rust
pub struct InProcessServingEngine {
    catalog: IcebergCatalog,                       // read-only, for execute_query (fetch_rows)
    pool: sqlx::PgPool,                            // for engine_serving::vector_search
    sql_catalog: control_plane_postgres::iceberg_sql_catalog::SqlCatalog, // for vector_search
}

impl InProcessServingEngine {
    pub fn new(
        catalog: IcebergCatalog,
        pool: sqlx::PgPool,
        sql_catalog: control_plane_postgres::iceberg_sql_catalog::SqlCatalog,
    ) -> Self {
        Self { catalog, pool, sql_catalog }
    }
}
```

(All three fields are real and final — the `vector_search` impl below uses `&self.sql_catalog`. `make_catalog`/`SqlCatalog` come from the now-added `//third-party:iceberg` dep.)

Add to its `impl ServingEngine`:

```rust
    async fn vector_search(
        &self,
        table: &control_plane_core::TableRef,
        index_name: &str,
        query: &[f32],
        k: usize,
        nprobe: Option<u32>,
        ef_search: Option<u32>,
    ) -> Result<query_api::serving::Rows, ServingError> {
        // The in-process engine needs an iceberg SqlCatalog (not the read-only
        // IcebergCatalog used by execute_query). Build one over the same DSN/warehouse
        // the harness already constructed; see Task 6 for the helper that exposes it.
        let batch = engine_serving::vector_search(
            &self.sql_catalog, &self.pool, table, index_name, query, k, nprobe, ef_search,
        )
        .await
        .map_err(|e| match e {
            engine_serving::EngineServingError::NoIndex(m) => ServingError::NoIndex(m),
            engine_serving::EngineServingError::DimMismatch(m) => ServingError::DimMismatch(m),
            other => ServingError::Engine(other.to_string()),
        })?;
        Ok(batches_to_rows(vec![batch]))
    }
```

**Important nuance for the implementer:** `engine_serving::vector_search` requires a `SqlCatalog` (the writable iceberg catalog from `control_plane_postgres::iceberg_sql_catalog::SqlCatalog`), *not* the `IcebergCatalog` that `InProcessServingEngine` currently holds for `execute_query`. So `InProcessServingEngine` stores **three** fields (`catalog`, `pool`, `sql_catalog`), the `sql_catalog` built the same way `engine-serving/tests/vector_search.rs::make_catalog` builds it (over the harness DSN + warehouse, via `iceberg::CatalogBuilder` + `LocalFsStorageFactory`).

Because this task's deliverable must build green, update `InProcessServingEngine::new` to the 3-arg arity AND its single construction site in `setup_iceberg` to pass the pool and a freshly-built `SqlCatalog`. `setup_iceberg` already has the fixture pool and builds an Iceberg warehouse; construct the `SqlCatalog` over that same DSN/warehouse (copy `make_catalog`). The full seeding harness (`seed_vector_type`) is added in Task 6, but the `InProcessServingEngine` plumbing must be complete and green here.

- [ ] **Step 4: Build query-api lib + e2e-support to verify green**

Run: `buck2 build //src/services/query-api:query-api //src/services/query-api:e2e-support > /tmp/t4.log 2>&1; grep -E "BUILD SUCCEEDED|FAIL|error\[" /tmp/t4.log`
Expected: both build. Run the existing query-api e2e sweep to confirm no regression from the `InProcessServingEngine` field change:
`buck2 test //src/services/query-api/... > /tmp/t4b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4b.log`
Clippy: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c4.log 2>&1; cat /tmp/c4.log`.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/serving.rs src/services/query-api/src/engine_client.rs src/services/query-api/tests/e2e_support.rs src/services/query-api/BUCK
git commit -m "feat(query-api): ServingEngine::vector_search plumbing (Flight ticket + in-process)"
```

---

### Task 5: `POST /search/:type/:index_name` route, handler, governance, error mapping

The external surface. `http.rs` parses + validates the request (manual deserialize into a `deny_unknown_fields` struct → 400 on any problem), runs the auth + coarse ACL gate, calls the engine, post-filters against row-filters, and renders `{results:[{id,distance}]}`. The governed flow lives in `handler.rs` next to `read_object`/`load_policy`/`identity_in_predicate`, reusing `compile_select_with` for the post-filter SQL.

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (`VectorHit`, `VectorSearchQuery`, `pub async fn vector_search`)
- Modify: `src/services/query-api/src/http.rs` (`VectorSearchRequest`, `K_MAX`, route, `post_search`)
- Test: `src/services/query-api/tests/vector_search_filter.rs` (new pure unit test) + its `BUCK` target

**Interfaces:**
- Consumes: `ServingEngine::vector_search` (Task 4); `load_policy`, `identity_in_predicate`, `compile_select_with`, `ObjectType`, `TypeName`, `PolicyTarget`, `RowFilter`, `Subject`, `SqlValue` (existing).
- Produces: `pub struct VectorHit { pub id: SqlValue, pub distance: f64 }`.
- Produces: `pub struct VectorSearchQuery { pub type_name: String, pub index_name: String, pub query: Vec<f32>, pub k: usize, pub nprobe: Option<u32>, pub ef_search: Option<u32> }`.
- Produces: `pub async fn vector_search(q: &VectorSearchQuery, subject: &Subject, deps: &QueryDeps<'_>) -> Result<Vec<VectorHit>, QueryError>`.

- [ ] **Step 1: Write the failing pure unit test for the post-filter compile + request validation**

Create `src/services/query-api/tests/vector_search_filter.rs`. This is a **pure-logic** test (no fixtures) — wire it as a plain `rust_test` (via the `loom_rust_test` wrapper), not `loom_fixture_test`. It covers two pure concerns:

1. `VectorSearchRequest` rejects unknown fields / missing fields / bad k (via `serde_json::from_value` + the validation helper).
2. The post-filter id-set SELECT is compiled with the candidate ids ANDed to the row-filters (assert the SQL contains the identity column, `IN (`, and the row-filter column; assert params include the candidate ids).

```rust
use query_api::http::{validate_search_request, VectorSearchRequest, K_MAX};

#[test]
fn request_rejects_unknown_field() {
    let v = serde_json::json!({ "query": [0.1, 0.2], "k": 3, "bogus": 1 });
    assert!(serde_json::from_value::<VectorSearchRequest>(v).is_err());
}

#[test]
fn request_rejects_k_out_of_range() {
    let too_big = VectorSearchRequest { query: vec![0.1], k: K_MAX + 1, nprobe: None, ef_search: None };
    assert!(validate_search_request(&too_big).is_err());
    let zero = VectorSearchRequest { query: vec![0.1], k: 0, nprobe: None, ef_search: None };
    assert!(validate_search_request(&zero).is_err());
}

#[test]
fn request_rejects_empty_query() {
    let empty = VectorSearchRequest { query: vec![], k: 3, nprobe: None, ef_search: None };
    assert!(validate_search_request(&empty).is_err());
}

#[test]
fn request_accepts_valid() {
    let ok = VectorSearchRequest { query: vec![0.1, 0.2], k: 5, nprobe: Some(8), ef_search: None };
    assert!(validate_search_request(&ok).is_ok());
}
```

(If exposing `VectorSearchRequest`/`validate_search_request`/`K_MAX` from `http` requires `pub`, make them `pub` in `http.rs`. The post-filter SQL-compile assertion can be added as a second test module if `compile_select_with` is reachable; if it is `pub` in `sql.rs`, assert on its output for a synthetic `ObjectType` + row-filter + id predicate. Keep this test pure — construct the `ObjectType`/`RowFilter`/`CallerPredicate` in-test.)

- [ ] **Step 2: Add the `BUCK` target + run to verify it fails**

Add a `rust_test` to `src/services/query-api/BUCK` named `vector_search_filter` (mirror an existing pure-logic `rust_test` in that file — e.g. the filter/render tests; deps include `:query-api` and `//third-party:serde_json`). Then:

Run: `buck2 test //src/services/query-api:vector_search_filter > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t5.log`
Expected: FAIL — `validate_search_request`/`VectorSearchRequest`/`K_MAX` not defined.

- [ ] **Step 3: Add the request struct, `K_MAX`, and validation in `http.rs`**

```rust
/// Upper bound on a single `/search` request's `k` — caps per-request work.
pub const K_MAX: usize = 1000;

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorSearchRequest {
    pub query: Vec<f32>,
    pub k: usize,
    #[serde(default)]
    pub nprobe: Option<u32>,
    #[serde(default)]
    pub ef_search: Option<u32>,
}

/// Returns `Err(message)` for an out-of-range `k` or empty `query`. The message is
/// safe to return verbatim in a 400 body (no internal detail).
pub fn validate_search_request(req: &VectorSearchRequest) -> Result<(), String> {
    if req.query.is_empty() {
        return Err("query must be a non-empty f32 array".to_string());
    }
    if req.k == 0 || req.k > K_MAX {
        return Err(format!("k must be in 1..={K_MAX}"));
    }
    Ok(())
}
```

- [ ] **Step 4: Add the governed flow in `handler.rs`**

```rust
/// One ranked kNN hit: the identity value and its distance.
pub struct VectorHit {
    pub id: SqlValue,
    pub distance: f64,
}

pub struct VectorSearchQuery {
    pub type_name: String,
    pub index_name: String,
    pub query: Vec<f32>,
    pub k: usize,
    pub nprobe: Option<u32>,
    pub ef_search: Option<u32>,
}

/// Governed kNN: coarse Read gate → engine kNN → row-filter post-filter.
/// Unknown type and a missing Read grant both return `Forbidden` (no existence leak).
pub async fn vector_search(
    q: &VectorSearchQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<Vec<VectorHit>, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let target = PolicyTarget::Type(type_name.clone());

    // Coarse gate, deny-by-default, BEFORE revealing whether the type exists.
    if deps.acl.check(&subject.0, Action::Read, &target).await? == Decision::Deny {
        return Err(QueryError::Forbidden);
    }
    // Resolve the type; a granted-but-nonexistent type is still no-leak Forbidden.
    let otype = match deps.ontology.get_type(&type_name).await {
        Ok(t) => t,
        Err(control_plane_core::ControlPlaneError::NotFound(_)) => return Err(QueryError::Forbidden),
        Err(e) => return Err(QueryError::ControlPlane(e)),
    };

    // Engine kNN over the named index (ServingError::NoIndex/DimMismatch propagate as Serving).
    let rows = deps
        .serving
        .vector_search(&otype.table, &q.index_name, &q.query, q.k, q.nprobe, q.ef_search)
        .await?;
    let mut hits = rows_to_hits(&rows);
    if hits.is_empty() {
        return Ok(hits);
    }

    // Row-filter post-filter. Empty filters (unrestricted) → return engine hits unchanged.
    let (row_filters, denied, masked) = load_policy(deps.acl, &subject.0, &target).await?;
    if row_filters.is_empty() {
        return Ok(hits);
    }
    let identity = otype.identity.clone().ok_or_else(|| QueryError::NoIdentity(otype.name.0.clone()))?;
    let candidate_strs: Vec<String> = hits.iter().map(|h| sqlvalue_to_id_string(&h.id)).collect();
    let Some(pred) = identity_in_predicate(&otype, &denied, &masked, &candidate_strs)? else {
        return Ok(hits); // no candidates to scope (already handled empty above; defensive)
    };
    let limit = u32::try_from(candidate_strs.len()).unwrap_or(u32::MAX);
    let (sql, params) = compile_select_with(
        deps.serving.dialect(),
        &otype.table,
        std::slice::from_ref(&identity),
        &[],
        &row_filters,
        std::slice::from_ref(&pred),
        &[],
        limit,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    let surviving: std::collections::HashSet<String> = served
        .rows
        .iter()
        .filter_map(|r| r.first())
        .map(sqlvalue_to_id_string)
        .collect();
    hits.retain(|h| surviving.contains(&sqlvalue_to_id_string(&h.id)));
    Ok(hits)
}
```

Add the two private helpers in `handler.rs`:

```rust
/// Decode a 2-column engine result (`id`, `_distance`) into ordered hits.
fn rows_to_hits(rows: &Rows) -> Vec<VectorHit> {
    rows.rows
        .iter()
        .filter_map(|r| {
            let id = r.first()?.clone();
            let distance = match r.get(1) {
                Some(SqlValue::Double(f)) => *f,
                Some(SqlValue::Int(i)) => *i as f64,
                _ => return None,
            };
            Some(VectorHit { id, distance })
        })
        .collect()
}

/// Canonical string key for set membership across engine hits and post-filter rows.
fn sqlvalue_to_id_string(v: &SqlValue) -> String {
    match v {
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Text(s) => s.clone(),
        other => format!("{other:?}"),
    }
}
```

(The `*i as f64` cast on a distance that arrived as `Int` is a defensive fallback; if clippy's `cast_precision_loss` fires, wrap with `#[expect(clippy::cast_precision_loss, reason = "distance fallback, magnitude small")]`. The expected arrow mapping is `Float32 → SqlValue::Double`, so this branch is rarely taken.)

Confirm the `compile_select_with` signature/visibility in `sql.rs` (it is `pub` with `(dialect, table, allowed_cols, mask_cols, row_filters, predicates, derived, limit)`); pass `derived = &[]`. Add `use crate::serving::Rows;` to `handler.rs` if not already in scope (`rows_to_hits` takes `&Rows`); `SqlValue` and the ACL/ontology types are already imported there.

- [ ] **Step 5: Add the route + `post_search` handler in `http.rs`**

Add to the router builder:

```rust
        .route("/search/:type_name/:index_name", post(post_search))
```

Handler (mirror `post_action`'s manual-deserialize-for-400 pattern and per-arm error mapping):

```rust
async fn post_search(
    State(st): State<AppState>,
    Path((type_name, index_name)): Path<(String, String)>,
    subject: Subject,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let req: VectorSearchRequest = match serde_json::from_value(body) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    if let Err(msg) = validate_search_request(&req) {
        return (StatusCode::BAD_REQUEST, msg).into_response();
    }
    let deps = crate::handler::QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
        default_limit: st.default_limit,
    };
    let q = crate::handler::VectorSearchQuery {
        type_name,
        index_name,
        query: req.query,
        k: req.k,
        nprobe: req.nprobe,
        ef_search: req.ef_search,
    };
    match crate::handler::vector_search(&q, &subject, &deps).await {
        Ok(hits) => {
            let results: Vec<serde_json::Value> = hits
                .iter()
                .map(|h| serde_json::json!({ "id": id_json(&h.id), "distance": h.distance }))
                .collect();
            Json(serde_json::json!({ "results": results })).into_response()
        }
        Err(QueryError::Forbidden) => StatusCode::FORBIDDEN.into_response(),
        Err(QueryError::Serving(crate::serving::ServingError::NoIndex(m))) => {
            (StatusCode::NOT_FOUND, m).into_response()
        }
        Err(QueryError::Serving(crate::serving::ServingError::DimMismatch(m))) => {
            (StatusCode::BAD_REQUEST, m).into_response()
        }
        Err(QueryError::NoIdentity(t)) => (StatusCode::BAD_REQUEST, t).into_response(),
        Err(e) => internal_error("vector search serving fault", e),
    }
}
```

Add a small `id_json` helper in `http.rs` (or reuse `render::natural` if it is `pub`): `SqlValue::Int(i) → json!(i)`, `SqlValue::Text(s) → json!(s)`, else `Value::Null`. Confirm the exact `QueryDeps` field names/constructor and `st.cp.ontology()`/`st.cp.acl()` accessors against `handler.rs`/the `ControlPlane` trait (the existing `get_object` path constructs `QueryDeps` the same way — mirror it exactly).

- [ ] **Step 6: Run the unit test + build to verify green**

Run: `buck2 test //src/services/query-api:vector_search_filter > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t5.log`
Expected: PASS. Build the lib: `buck2 build //src/services/query-api:query-api > /tmp/t5b.log 2>&1; grep -E "BUILD SUCCEEDED|FAIL|error\[" /tmp/t5b.log`. Clippy: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c5.log 2>&1; cat /tmp/c5.log`.

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/handler.rs src/services/query-api/src/http.rs src/services/query-api/tests/vector_search_filter.rs src/services/query-api/BUCK
git commit -m "feat(query-api): governed POST /search/:type/:index_name endpoint"
```

---

### Task 6: e2e — seeding harness + full governed end-to-end tests

The heaviest task: extend `e2e-support` with a helper that lands a `vector(N)` type, declares a named index, and builds it against the harness's Iceberg catalog + pool, then assert the full governed surface through the router. This finishes the `InProcessServingEngine` `SqlCatalog` field started in Task 4.

**Files:**
- Modify: `src/services/query-api/tests/e2e_support.rs` (seeding helper)
- Modify: `src/services/query-api/BUCK` (add `//third-party:uuid` and the Arrow deps — `//third-party:arrow-array`, `//third-party:arrow-ipc`, `//third-party:arrow-schema` — to the `:e2e-support` **library** deps; the `seed_vector_type` helper hand-rolls a `list<float32>` Arrow IPC body and uses `RunId(uuid::Uuid::new_v4())`. Confirm the exact arrow target names against how `engine-serving/tests/vector_search.rs` is wired in its `BUCK`, and mirror them.)
- Test: `src/services/query-api/tests/vector_search_e2e.rs` (new) + its `BUCK` target (fixture → `loom_fixture_test`)

**Interfaces:**
- Consumes: everything above; `control_plane_postgres::vector_index::build_vector_index`, `control_plane_postgres::iceberg_landing::land`, `iceberg_sql_catalog::SqlCatalog`, `VectorIndexDef`, `IndexSpec`, the e2e `get`/`subject_with_role`/`grant_read`/`session_token`/`spawn_http` helpers.
- Produces: an `e2e-support` helper, e.g.
  ```rust
  pub async fn seed_vector_type(
      fx: &PgFixture, db: &str,
  ) -> (PgControlPlane, Arc<dyn ServingEngine>, SqlCatalog /* keep alive */, TempDir /* keep alive */);
  ```
  that registers a `Docs(id Long identity, embedding vector(4))` type, lands 4 cold rows, declares a Flat index `by_sim` (Cosine), and builds it — returning a router-ready serving engine whose `vector_search` resolves `by_sim`.

- [ ] **Step 1: Write the failing e2e tests**

Create `src/services/query-api/tests/vector_search_e2e.rs`. Use `e2e_support` helpers. Cover the spec's e2e matrix. Each test boots `PgFixture`. Build the router with `query_api::http::router(AppState { cp, serving, action_engine: StubAction, default_limit })` and drive `POST /search/...` through the same auth gate `get` uses (add a `post` driver to `e2e_support` mirroring `get`, or use `spawn_http` + a real client). Example shape:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_returns_ranked_ids_for_permitted_subject() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _cat, _wh) = e2e_support::seed_vector_type(&fx, &db).await;
    let (subj, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Docs").await;

    let (status, body) = post_search(
        cp_arc(&cp), serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 2 }),
        "alice",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let results = body["results"].as_array().expect("results");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["id"], serde_json::json!(1)); // exact match first
    assert!(results[0]["distance"].as_f64().unwrap() <= results[1]["distance"].as_f64().unwrap());
}
```

Tests to include (one `#[tokio::test]` each, reusing the seeded type):
1. **Ranked results** for a permitted subject (above).
2. **403 — no Read grant:** a subject without `grant_read` → `FORBIDDEN`.
3. **403 — unknown type:** `POST /search/Nope/by_sim` (even for a granted subject of another type) → `FORBIDDEN` (no leak).
4. **404 — no built index:** `POST /search/Docs/missing` → `NOT_FOUND`.
5. **400 — bad body / k=0 / k>K_MAX / dim mismatch:** four sub-asserts (malformed JSON object, `k:0`, `k:1001`, `query` length 3 vs index dim 4 → the last surfaces engine `DimMismatch`).
6. **Row-filter drops the nearest hit:** grant the subject a Read policy whose `row_filter` excludes `id = 1` (the exact match); assert the response omits id 1 and may be shorter than k. (Reuse the policy-granting helper the graph/object-set e2e tests use to attach a `RowFilter`; if none is exported, add one to `e2e_support` mirroring `grant_read`.)
7. **Knob smoke:** `{ "query": […], "k": 1, "nprobe": 2 }` and `{ …, "ef_search": 64 }` both return `200` with valid results (Flat ignores them; the assertion is just a successful ranked response).

- [ ] **Step 2: Add the `BUCK` target + run to verify it fails**

Add a `loom_fixture_test` named `vector_search_e2e` to `src/services/query-api/BUCK` (mirror the existing query-api e2e fixture targets; deps include `:e2e-support`, `:query-api`, the postgres/iceberg crates, `//third-party:serde_json`, `//third-party:tokio`). Then:

Run: `buck2 test //src/services/query-api:vector_search_e2e > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t6.log`
Expected: FAIL — `seed_vector_type`/`post_search` not defined.

- [ ] **Step 3: Implement the seeding helper + finish `InProcessServingEngine` SqlCatalog plumbing**

In `e2e_support.rs`, add `seed_vector_type` modeled on `engine-serving/tests/vector_search.rs::seed_and_build` (build a `SqlCatalog` over the harness DSN/warehouse via the `make_catalog` recipe; `define_type` for `Docs`; `land` 4 rows with `inline_byte_limit = 0`; `define_vector_index` `by_sim` Flat/Cosine; `build_vector_index`). Construct the `InProcessServingEngine` with `(catalog_for_reads, pool, sql_catalog)` so its `vector_search` (Task 4) resolves the built index. Keep the `SqlCatalog` and `TempDir` warehouse alive in the returned tuple (drop = data gone). Add a `post_search` driver (mirror `get`: build the router, insert the `Subject` extension via a session token, `oneshot` a `POST` with a JSON body, return `(StatusCode, serde_json::Value)`).

- [ ] **Step 4: Run the e2e to verify it passes**

Run: `buck2 test //src/services/query-api:vector_search_e2e > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t6.log`
Expected: PASS (fixture test — local execution via `loom_fixture_test`). If a flake appears at fixture boot (`fixture.rs` postgres boot), re-run in isolation to confirm.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/tests/e2e_support.rs src/services/query-api/tests/vector_search_e2e.rs src/services/query-api/BUCK
git commit -m "test(query-api): end-to-end governed /search vector kNN e2e"
```

---

## Final verification (after all tasks)

- [ ] **Full sweep:** `buck2 test //src/... > /tmp/sweep.log 2>&1; grep -E "Tests finished|FAIL" /tmp/sweep.log` — all green (a fixture-boot flake re-run in isolation is acceptable per the project's known cloud-contention behavior).
- [ ] **Clippy across first-party:** `bash tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -5 /tmp/clippy.log` — clean.
- [ ] **No `.sqlx` change needed:** this slice adds no compile-time `query!`/`query_scalar!` (it reuses existing `lookup_vector_index`). Confirm `git status` shows no unexpected `.sqlx` drift.

## Self-Review (completed by plan author)

- **Spec coverage:** route + request (Task 5) ✓; governance coarse gate + row-filter post-filter (Task 5) ✓; ids+distances response (Task 5) ✓; `VectorSearchTicket` knobs (Task 2) ✓; `apply_query_knobs` trait + IVF/HNSW impls (Task 1) ✓; engine dim check + `DimMismatch`→400 (Task 3) ✓; `NoIndex`→404 (Tasks 2–5) ✓; engine→wire→query-api error threading (Tasks 2–5) ✓; engine-serving knob/dim tests + query-api e2e matrix (Tasks 3, 6) ✓; K_MAX, deny_unknown_fields, empty-query/k bounds → 400 (Task 5) ✓; "all filtered out ⇒ 200 []" (Task 5, `hits.retain`) ✓; unknown type → 403 no-leak (Task 5) ✓.
- **Type consistency:** `apply_query_knobs(&mut self, Option<u32>, Option<u32>)`, `VectorSearchTicket{…, nprobe, ef_search}`, `engine_serving::vector_search(…, nprobe, ef_search)`, `ServingEngine::vector_search(table, index_name, query, k, nprobe, ef_search) -> Rows`, `VectorSearchError{NoIndex,DimMismatch,Engine}`, `ServingError{Engine,NoIndex,DimMismatch}`, `VectorHit{id: SqlValue, distance: f64}` — consistent across tasks.
- **Verify-before-coding flags for implementers:** exact `compile_select_with` arity in `sql.rs`; `QueryDeps` field names + `ControlPlane::ontology()/acl()` accessors; that `thiserror`/`tonic` are existing engine-wire deps; the `make_catalog` recipe for the `SqlCatalog` in `e2e_support`. Each is called out inline in the owning task.
