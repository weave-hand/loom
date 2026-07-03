# `/search` engine-fault classification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Classify the two `/search` post-filter faults born from *engine-derived* (non-caller-forgeable) values — a mirror-returned identity cell that fails coercion, and a served vector type with no declared identity — as internal 500s with an operator log, instead of the caller-facing 400 they render today.

**Architecture:** Classify at the source with one new `QueryError::Internal { context, source: Box<QueryError> }` variant plus an `into_internal(self, context)` helper. `vector_search`'s post-filter wraps the two engine-derived error sites; the totality of `query_error_response` (no catch-all) forces one deliberate render arm that routes `Internal` through the existing `internal_error` helper (opaque 500 + one `tracing::error!`). Caller-value paths (caller filters, `?_ids`, cursors) are untouched and keep their 400s. `query_error_response` stays total and path-insensitive.

**Tech Stack:** Rust 2024, buck2 `rust_test` targets (never inline `#[cfg(test)]`), `thiserror`, `control_plane_memory` for the RE-eligible governed-handler unit test, `tracing-subscriber` (0.3, already vendored) capturing writer for the operator-log assertion.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-07-03-search-fault-classification-design.md` (status: approved). Closes `iss-qa-search-badfiltervalue-classification`.
- **The `Internal` variant is only ever constructed in `vector_search`'s post-filter** (the two engine-derived sites). Every caller-supplied path keeps its existing classification: `BadFilterValue` → structured 400, `NoIdentity` (association sites) → 400, `BadPagination` (caller-echoed cursor) → 400.
- **`NoIdentity` has other constructors** (association reads). Do NOT change the `NoIdentity => 400` arm; only wrap the *`vector_search`-site* `NoIdentity` into `Internal`.
- **`query_error_response` must stay total** (no catch-all `_ =>`) — that totality is what forces the deliberate arm. Do not add a wildcard.
- **Opaque 500 body**: the response the client sees for `Internal` is exactly the existing `internal_error` output — status 500, body `"internal error"`, no internal detail (no column/SQL/value echo). The detail goes only to the server `tracing::error!`.
- Tests are `rust_test` integration targets in `tests/*.rs`. The classification/render unit test is RE-eligible (`rust_test`, Memory-backed); the caller-400 regression assertions ride an existing Memory-backed router test. No `loom_fixture_test` needed (no Postgres).
- **Never pipe `buck2 test` through `tail`/`head`** — redirect to a file and grep: `buck2 test <target> > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`. Run buck2 in the FOREGROUND.
- Strict clippy (pedantic + restriction). `into_internal` must be *used* (it is — two sites) so no `dead_code`.

---

### Task 1: `Internal` variant, source-wrapping, and render arm

One cohesive change: the variant + helper + two seam wraps + render arm all compile and are meaningful together (the variant is non-exhaustive without the render arm; `into_internal` is dead without the wraps). Plus the three test groups.

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (`QueryError` enum ~106-155; new `impl QueryError`; `vector_search` post-filter, lines 610-616)
- Modify: `src/services/query-api/src/http.rs` (`query_error_response` combined internal arm, line 447-449)
- Verify-only (no edit): `src/services/query-api/src/flight_export.rs` (`map_query_err` catch-all already correct)
- Test (new): `src/services/query-api/tests/search_fault_classification.rs` (RE-eligible `rust_test`)
- Test (modify): `src/services/query-api/tests/filter_error_http.rs` (caller-400 regression guards)
- Modify: `src/services/query-api/BUCK` (wire the new `rust_test` target)

**Interfaces:**
- Produces: `QueryError::Internal { context: &'static str, source: Box<QueryError> }` and `QueryError::into_internal(self, context: &'static str) -> QueryError`.
- Consumes (unchanged): `identity_in_predicate(&ObjectType, &HashSet<String>, &HashSet<String>, &[String]) -> Result<Option<Predicate>, QueryError>` (returns `Err(QueryError::BadFilterValue(FilterError))` when a value fails to coerce); `vector_search(&VectorSearchQuery, &Subject, &QueryDeps) -> Result<Vec<VectorHit>, QueryError>` (pub); `query_error_response(QueryError, &'static str) -> Response`; `internal_error(&str, impl Display) -> Response` (logs one `tracing::error!(error = %e, "{context}")`, returns 500 `"internal error"`); `ServingEngine` trait; `serving::Rows { columns, rows: Vec<Vec<SqlValue>> }`; `SqlValue::{Text, Double, Int}`.

- [ ] **Step 1: Write the classification + render + log unit test (red)**

Create `src/services/query-api/tests/search_fault_classification.rs`:

```rust
//! `/search` post-filter fault classification: an engine-derived identity cell that
//! fails coercion (server-data-integrity fault, not caller-forgeable) is an internal
//! 500 with one operator log, NOT a caller 400 echoing engine data. RE-eligible:
//! MemoryControlPlane + a stub serving engine returning a type-inconsistent hit; the
//! `tracing::error!` is captured synchronously (current-thread runtime) so no
//! multi-thread subscriber hop can drop it.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, ControlPlane, Effect, ObjectType, Ontology, Policy, PolicyTarget, RoleId,
    RowFilter, ScalarValue, CompareOp, SubjectId, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::handler::{QueryError, QueryDeps, Subject, VectorSearchQuery, vector_search};
use query_api::http::query_error_response;
use query_api::serving::{Rows, ServingEngine, ServingError, SqlValue};
use query_api::sql::{DataFusionDialect, SqlDialect};

/// A serving engine whose kNN returns one hit whose identity cell is `Text` — i.e.
/// does not coerce to a `Long`/`Integer` declared identity. `fetch_rows` is unused here.
struct BadHitServing;

#[async_trait]
impl ServingEngine for BadHitServing {
    async fn fetch_rows(&self, _sql: &str, _params: &[SqlValue]) -> Result<Rows, ServingError> {
        Err(ServingError::Engine("unused".into()))
    }
    async fn vector_search(
        &self,
        _table: &control_plane_core::TableRef,
        _index: &str,
        _query: &[f32],
        _k: usize,
        _nprobe: Option<u32>,
        _ef: Option<u32>,
    ) -> Result<Rows, ServingError> {
        // [id, distance]; id is Text -> will fail to coerce to the Long identity.
        Ok(Rows {
            columns: vec!["id".into(), "distance".into()],
            rows: vec![vec![SqlValue::Text("x".into()), SqlValue::Double(0.0)]],
        })
    }
    fn dialect(&self) -> &'static dyn SqlDialect {
        &DataFusionDialect
    }
}

/// A `Docs` type with a `Long` identity `id`, a subject with Read + a row filter on it
/// (so the post-filter actually runs), in a MemoryControlPlane.
async fn seed(cp: &MemoryControlPlane) -> SubjectId {
    let ty = ObjectType::build("Docs", ("main", "docs"))
        .prop_req("id", "Long")
        .identity("id")
        .done();
    cp.define_type(ty).await.unwrap();
    let subj = SubjectId("alice".into());
    let role = RoleId("alice-role".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(&role, Action::Read, PolicyTarget::Type(TypeName("Docs".into())), Effect::Allow)
        .await
        .unwrap();
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Docs".into())),
            row_filter: Some(RowFilter::Compare {
                property: "id".into(),
                op: CompareOp::Gt,
                value: ScalarValue::Int(0),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    subj
}

/// Capturing `MakeWriter` over a shared buffer, so the `tracing::error!` text is
/// inspectable and countable.
#[derive(Clone)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
    type Writer = CaptureWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn engine_hit_coercion_fault_is_internal_500_with_one_log() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    let subj = seed(&cp).await;
    let serving = BadHitServing;
    let deps = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        serving: &serving,
        default_limit: 1000,
    };
    let q = VectorSearchQuery {
        type_name: "Docs".into(),
        index_name: "by_sim".into(),
        query: vec![1.0, 0.0, 0.0, 0.0],
        k: 2,
        nprobe: None,
        ef_search: None,
    };

    // Classification: the engine-derived coercion fault is wrapped as Internal, NOT
    // surfaced as the caller-facing BadFilterValue.
    let err = vector_search(&q, &Subject(subj), &deps).await.unwrap_err();
    assert!(
        matches!(err, QueryError::Internal { .. }),
        "engine hit coercion fault must classify as Internal, got {err:?}"
    );
    // Its Display chains the wrap context and the underlying coercion detail (column).
    let shown = err.to_string();
    assert!(shown.contains("post-filter"), "context present: {shown}");
    assert!(shown.contains("id"), "coercion detail names the column: {shown}");

    // Render + operator log: opaque 500 body, and exactly one ERROR event carrying the
    // detail. `query_error_response` runs synchronously inside `with_default`, so the
    // thread-local subscriber reliably captures it.
    let buf = Arc::new(Mutex::new(Vec::new()));
    let sub = tracing_subscriber::fmt()
        .with_writer(CaptureWriter(buf.clone()))
        .with_max_level(tracing::Level::ERROR)
        .with_ansi(false)
        .finish();
    let resp = tracing::subscriber::with_default(sub, || query_error_response(err, "search"));
    assert_eq!(resp.status(), axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"internal error", "opaque body, no bad_filter_value echo");

    let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert_eq!(logged.matches("ERROR").count(), 1, "exactly one error log: {logged}");
    // `internal_error` logs `error = %e`, and Internal's Display chains the wrap context
    // and the FilterError detail — so the operator log carries both.
    assert!(logged.contains("post-filter"), "log carries the wrap context: {logged}");
    assert!(logged.contains("id"), "log carries the coercion detail (column): {logged}");
}
```

- [ ] **Step 2: Wire the new test target in BUCK**

In `src/services/query-api/BUCK`, add next to the other `rust_test` targets (mirror `identity-in-predicate` at line 1153 for the pure-logic deps, adding memory + tracing-subscriber + async-trait + axum):

```python
rust_test(
    name = "search-fault-classification",
    crate = "search_fault_classification",
    srcs = ["tests/search_fault_classification.rs"],
    crate_root = "tests/search_fault_classification.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:async-trait",
        "//third-party:axum",
        "//third-party:tokio",
        "//third-party:tracing",
        "//third-party:tracing-subscriber",
    ],
)
```

(Confirm the exact `//third-party:` label spellings against a sibling target that already uses each crate — e.g. `async-trait`, `tracing`, `tracing-subscriber`. If `DataFusionDialect`/`SqlDialect` need a dep already carried transitively by `:query-api`, no extra entry is required.)

- [ ] **Step 3: Run the unit test to verify it fails**

Run: `buck2 test //src/services/query-api:search-fault-classification > /tmp/sfc.log 2>&1; grep -E "Tests finished|FAIL|error\[|no variant|no method" /tmp/sfc.log`
Expected: FAIL to compile — `no variant named \`Internal\` found for enum \`QueryError\`` and `no method named \`into_internal\``. (The variant/helper don't exist yet; an unbuildable test is the red state.)

- [ ] **Step 4: Add the `Internal` variant and `into_internal` helper**

In `src/services/query-api/src/handler.rs`, add the variant to `QueryError` (place it just before the `#[error(transparent)] ControlPlane` group, after `BadPagination`):

```rust
    /// A server-side fault detected while processing engine-derived (non-caller)
    /// values — e.g. a mirror-returned identity cell that fails `coerce_filter`
    /// against its declared logical type, or a served vector type with no declared
    /// identity. Never caller-forgeable: renders as an opaque 500, logged
    /// server-side with the wrapped error's full detail.
    #[error("internal fault: {context}: {source}")]
    Internal {
        context: &'static str,
        source: Box<QueryError>,
    },
```

Then add an `impl` block (immediately after the `enum QueryError { … }` closes, before the `pub use crate::governed::…` line):

```rust
impl QueryError {
    /// Reclassify a fault born from engine-derived (non-caller) input as an internal
    /// fault, boxing the original as the `source` so its detail survives to the log.
    #[must_use]
    pub fn into_internal(self, context: &'static str) -> QueryError {
        QueryError::Internal {
            context,
            source: Box::new(self),
        }
    }
}
```

- [ ] **Step 5: Wrap the two engine-derived sites in `vector_search`**

In `src/services/query-api/src/handler.rs`, the post-filter block (lines 610-616). Replace:

```rust
    let identity = g
        .otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(g.otype.name.0.clone()))?;
    let candidate_strs: Vec<String> = hits.iter().map(|h| sqlvalue_to_id_string(&h.id)).collect();
    let Some(pred) = identity_in_predicate(&g.otype, &g.denied, &g.masked, &candidate_strs)? else {
        return Ok(hits); // no candidates to scope (empty handled above; defensive)
    };
```

with:

```rust
    let identity = g.otype.identity.clone().ok_or_else(|| {
        // A served vector type with no declared identity is server ontology/config
        // state, not caller-forgeable here — classify as internal, not a 400.
        QueryError::NoIdentity(g.otype.name.0.clone())
            .into_internal("vector-search post-filter: served vector type has no declared identity")
    })?;
    let candidate_strs: Vec<String> = hits.iter().map(|h| sqlvalue_to_id_string(&h.id)).collect();
    // The candidate ids are the ENGINE's own hit identities, not caller input; a
    // coercion failure here is server-data-integrity drift → internal, not a caller 400.
    // (The `identity_governed()` guard above already returned Forbidden for the BadFilter arm,
    // so only coercion errors can flow from here.)
    let Some(pred) = identity_in_predicate(&g.otype, &g.denied, &g.masked, &candidate_strs)
        .map_err(|e| e.into_internal("vector-search post-filter: engine hit identity failed coercion"))?
    else {
        return Ok(hits); // no candidates to scope (empty handled above; defensive)
    };
```

- [ ] **Step 6: Add the render arm in `query_error_response`**

In `src/services/query-api/src/http.rs`, fold `Internal` into the existing combined internal arm (line 447-449) so the mapping stays total with no duplicated `internal_error` call. Replace:

```rust
        e @ (QueryError::ControlPlane(_) | QueryError::Serving(_) | QueryError::Malformed(_)) => {
            internal_error(context, e)
        }
```

with:

```rust
        e @ (QueryError::ControlPlane(_)
        | QueryError::Serving(_)
        | QueryError::Malformed(_)
        | QueryError::Internal { .. }) => internal_error(context, e),
```

(The spec illustrates a separate `QueryError::Internal { .. } => internal_error(context, e)` arm; folding into the existing internal group is behavior-identical — all render via `internal_error` — and avoids a duplicate call. `internal_error` logs `error = %e`; `Internal`'s Display carries `context` + the wrapped source detail.)

- [ ] **Step 7: Verify `flight_export::map_query_err` needs no change**

Read `src/services/query-api/src/flight_export.rs:191-201`. Confirm: the `other => internal("flight export governance fault", other)` catch-all already maps the new `Internal` variant to an opaque `Status::internal("internal error")` + one `tracing::error!` (the desired behavior), and `QueryError::BadFilterValue(e) => Status::invalid_argument(...)` is unchanged (export filters are caller-supplied). No edit. Note this reasoning in the commit/PR body so a reviewer doesn't read it as a missed mapping.

- [ ] **Step 8: Run the unit test to verify it passes**

Run: `buck2 test //src/services/query-api:search-fault-classification > /tmp/sfc.log 2>&1; grep -E "Tests finished|FAIL" /tmp/sfc.log`
Expected: PASS — `engine_hit_coercion_fault_is_internal_500_with_one_log` green (Internal classification, opaque 500 body, exactly one ERROR log carrying the column detail).

- [ ] **Step 9: Add caller-path 400 regression guards (red-first only if absent)**

In `src/services/query-api/tests/filter_error_http.rs` (Memory-backed router test that already asserts caller bad-filter 400s), add two assertions confirming the *caller* coercion paths are unaffected by this change — a caller filter value that doesn't coerce still yields the structured **400** `bad_filter_value` body, and a caller `?_ids=notanint` object-set read still yields **400**. Mirror the file's existing `get(...)`/status-assertion pattern and its type/identity setup (an Integer/Long-identity type). If the file already covers both exact cases, add a one-line comment referencing this spec instead of duplicating; otherwise add the two focused cases.

(Run: `buck2 test //src/services/query-api:filter-error-http > /tmp/feh.log 2>&1; grep -E "Tests finished|FAIL" /tmp/feh.log` → PASS; these are unaffected by the seam change, so they stay green — they are guards against regression, not red-first.)

- [ ] **Step 10: Full query-api sweep + clippy + prek**

Run:
```
buck2 test //src/services/query-api/... > /tmp/qa.log 2>&1; grep -E "Tests finished|FAIL" /tmp/qa.log
buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/clip.log 2>&1; echo "exit=$?"; find buck-out -name clippy.txt -path '*query-api*' | head -1 | xargs cat 2>/dev/null | head
buck2 run //tools:prek -- run --files src/services/query-api/src/handler.rs src/services/query-api/src/http.rs src/services/query-api/tests/search_fault_classification.rs src/services/query-api/tests/filter_error_http.rs src/services/query-api/BUCK 2>&1 | grep -E "Passed|Failed"
```
Expected: whole `query-api` suite green (spec acceptance criterion 3); clippy output empty; prek hooks pass. Fix anything they flag.

- [ ] **Step 11: Commit**

```bash
git add src/services/query-api/src/handler.rs src/services/query-api/src/http.rs \
        src/services/query-api/tests/search_fault_classification.rs \
        src/services/query-api/tests/filter_error_http.rs \
        src/services/query-api/BUCK
git commit -m "fix(query-api): classify /search engine-derived coercion faults as internal 500 (iss-qa-search-badfiltervalue-classification)"
```

---

### Task 2: Close the register item (documentation)

No tests. Done at finish via `loom-docs-update` (invoked by `superpowers:finishing-a-development-branch`); recorded here for completeness.

**Files:**
- Modify: `docs/ISSUES.md` (remove the item entry — registers carry open work only)
- Modify: `docs/system-capabilities/query-api.md` (fold in the shipped classification; drop the id from `## Known gaps` if listed)

- [ ] **Step 1: Remove the resolved item and document the capability**

Delete the `iss-qa-search-badfiltervalue-classification` entry (`- [ ]` line + prose) from `docs/ISSUES.md`. In `docs/system-capabilities/query-api.md`, fold into the `/search` governance section that engine-derived post-filter faults (a mirror-returned identity cell that fails coercion, or a served type with no declared identity) now render as opaque logged 500s rather than caller 400s, with the PR ref `(#N)` inline; remove the id from `## Known gaps` if present. Rewrite any surviving `[[iss-qa-search-badfiltervalue-classification]]` link as a plain `` `#iss-qa-search-badfiltervalue-classification` `` code span.

- [ ] **Step 2: Validate and stage**

```bash
bash tools/docs.sh validate
git add docs/ISSUES.md docs/system-capabilities/query-api.md
```
Expected: `docs.sh validate: OK`. Name the closed id + PR in the PR body.

---

## Self-Review

**1. Spec coverage** (against `2026-07-03-search-fault-classification-design.md`):
- Design: `Internal { context, source: Box<QueryError> }` variant → Task 1 Step 4. `into_internal` helper → Step 4. Wrap `identity_in_predicate` (handler.rs:616) + `NoIdentity` (610-614) → Step 5. `query_error_response` deliberate `Internal` arm → Step 6. Flight export mapping (`Internal` → opaque `Status::internal`, `BadFilterValue` stays `invalid_argument`) → Step 7 (verified no-change; catch-all already yields it). All covered.
- Acceptance criteria: (1) engine coercion fault → 500 opaque body + exactly one `tracing::error!` with column/expected detail → Step 1 test. (2) caller `?col=notanint` → 400, `?_ids=notanint` → 400 → Step 9. (3) full suite green + clippy/prek clean → Step 10. All covered.
- Out of scope respected: no change to `sqlvalue_to_id_string` `{other:?}` fallback, none to `query_error_response` totality contract or `BadPagination` 400, no other-service changes.

**2. Placeholder scan:** No TBD/TODO/vague steps. Each code step carries complete code; each run step names the target and expected result. Two explicit *confirm* asks (third-party label spellings in Step 2; existing caller-400 coverage in Step 9) are verification instructions with a concrete fallback, not placeholders.

**3. Type consistency:** `QueryError::Internal { context: &'static str, source: Box<QueryError> }` defined in Step 4, matched in Step 6 (`Internal { .. }`) and constructed via `into_internal` in Step 5 — consistent. `into_internal(self, &'static str) -> QueryError` signature matches all call sites. `vector_search`/`QueryDeps`/`Subject`/`VectorSearchQuery`/`ServingEngine`/`Rows`/`SqlValue` used in the test match their real definitions (`handler.rs:523`, `serving.rs:46`). `identity_in_predicate` returns `Result<Option<_>, QueryError>` whose `Err` is `BadFilterValue`, so `.map_err(|e| e.into_internal(...))` types check.
