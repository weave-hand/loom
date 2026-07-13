# Log-vs-CDC Declaration Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `iss-stream-log-vs-cdc-declare`: a `mode=stream&buckets=N` (log) declaration against a table already declared `kind='cdc'` with the same bucket count is silently accepted (and hash-buckets as CDC); it must reject with `Validation` → HTTP 400, symmetric with the existing `mode=cdc`-against-log guard.

**Architecture:** One new guard in `reconcile_stream_mode`'s count-equal redeclare arm (`src/control-plane/postgres/src/stream.rs:135-176`), gated on `StreamDecl::Log(_)` AND a pre-existing `kind='cdc'` registry row — mirroring the existing CDC-side kind guard at `stream.rs:141-151`. No new I/O (reuses the already-fetched `existing_meta`), no SQL change, no endpoint change; both landing routes (inline + direct Parquet) are covered because they share the seam. Spec: `docs/superpowers/specs/2026-07-09-stream-log-vs-cdc-declare-design.md`.

**Tech Stack:** Rust, sqlx (no cache change), buck2 `loom_fixture_test`, hermetic Postgres fixture, axum/tower oneshot for the HTTP e2e.

## Global Constraints

Carried from the spec; every task implicitly includes these:

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New fixture tests MUST use `loom_fixture_test` (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`, or the fixture env (PG binaries, MinIO, boot-slot dir) is missing. The `no-inline-tests` prek hook fails the build on any inline `#[test]`.
- **No `.sqlx` impact.** The guard adds no `query!`/`query_scalar!` SQL — it reuses `existing_meta` fetched at `stream.rs:124`. Do NOT run `tools/sqlx-prepare.sh`. If the implementation drifts into new compile-time SQL, stop and reconsider (cloud sessions cannot regen the cache — `initdb` refuses root; the `AssertSqlSafe` precedent in this crate is for runtime-checked test/lookup queries only).
- **Non-CDC / log / batch paths must stay byte-identical** (the slice-2 constraint, `2026-07-07-stream-pk-cdc-tables-2a.md:18`). The guard fires only when a `kind='cdc'` row pre-exists. `//src/services/ingest:stream-declare`, `//src/services/ingest:model-cdc-declare`, `//src/control-plane/postgres:stream-merge-declare`, and `//src/services/query-api:stream-cdc-declare` must pass unchanged.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/`indexing_slicing`/`panic`/`unreachable`/`todo` in production code. The guard uses only `matches!` + `is_some_and`. Test code is exempted from the panic-safety lints via `loom_fixture_test`.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files`; `git add` new files FIRST (prek skips untracked files). Markdown ends with exactly one trailing newline, no trailing whitespace.
- **Build/test commands** (from CLAUDE.md): build `buck2 build -v0 --console none //src/...`; test `buck2 test --console none <targets>` (cloud: `-M none` on builds, scope tests to the touched targets, `buck2 clean` between heavy phases; a full local suite needs `-j 8`).

---

## File Structure

**Create:**
- `src/control-plane/postgres/tests/stream_log_vs_cdc_declare.rs` — seam test (repro + precedence + log-redeclare control).
- `src/services/ingest/tests/stream_log_vs_cdc_http.rs` — cross-surface HTTP e2e.

**Modify (production):**
- `src/control-plane/postgres/src/stream.rs` — the symmetric Log-side kind guard in `reconcile_stream_mode`'s `(Some(_), Some(m))` arm; doc-comment + stale-comment updates. **Only file with a production change.**

**Modify (build):**
- `src/control-plane/postgres/BUCK` — `loom_fixture_test` target `stream-log-vs-cdc-declare` (mirror `stream-merge-declare`, which now sits at `BUCK:1672-1687` — the plan's original `BUCK:1414` is stale).
- `src/services/ingest/BUCK` — `loom_fixture_test` target `stream-log-vs-cdc-http` (mirror `model-cdc-declare`, `BUCK:240`).

---

## Task 1: Seam test + the guard in `reconcile_stream_mode`

**Files:**
- Create: `src/control-plane/postgres/tests/stream_log_vs_cdc_declare.rs`
- Modify: `src/control-plane/postgres/BUCK` (new target after `stream-merge-declare`, which closes at line 1687)
- Modify: `src/control-plane/postgres/src/stream.rs:37-46` (doc comment), `:135-176` (the arm)

**Interfaces:**
- Consumes: `land`/`land_cdc`/`CdcDecl`/`InlineLimits` (`control_plane_postgres::iceberg_landing`), `live_table_id` (`iceberg_mirror`), `StreamTables::stream_meta`, `PgFixture`, `local_sql_catalog` (`//src/testing:seed`), `StreamDecl`/`existing_meta` inside `reconcile_stream_mode` (`stream.rs:63-66`/`:124`).
- Produces: the `StreamDecl::Log(_)`-gated kind guard raising `ControlPlaneError::Validation("cannot declare {schema}.{table} as a log stream table: already declared with a different stream kind")` — Task 2's e2e relies on the "different stream kind" substring surviving to the HTTP body.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/stream_log_vs_cdc_declare.rs`, mirroring `stream_merge_declare.rs`'s harness (`PgFixture` / `local_sql_catalog` / `land_cdc`, its `columns`/`batch`/`lineage`/`always_inline` helpers) — same fixture, plus the plain `land` entrypoint for the log-declare side:

```rust
//! Symmetric log-vs-CDC declaration guard (iss-stream-log-vs-cdc-declare),
//! exercised at the reconcile_stream_mode seam both HTTP surfaces share.
//! loom_fixture_test (Postgres).
//!   - mode=stream (log) with a MATCHING count against a CDC table -> Err(Validation),
//!     registry still kind='cdc'  (the defect: silently accepted before the fix)
//!   - mode=stream with a MISMATCHED count against a CDC table -> Err(Conflict)
//!     (count precedence preserved — pins existing behavior)
//!   - control: a second same-count log declare on a LOG table stays Ok
//!     (the byte-identical log redeclare the slice-2 constraint pins)
//!
//! Mirrors `stream_merge_declare.rs`'s PgFixture / local_sql_catalog / land_cdc
//! harness and its columns / batch / lineage / always_inline helpers.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ControlPlaneError, MergeEngine, ObjectType, Ontology, StreamKind, StreamTables, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{CdcDecl, InlineLimits, land, land_cdc};
use control_plane_postgres::iceberg_mirror::live_table_id;
use loom_test_seed::local_sql_catalog;

fn columns() -> Vec<control_plane_core::ColumnSpec> {
    vec![
        control_plane_core::ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        control_plane_core::ColumnSpec {
            name: "name".into(),
            ty: "string".into(),
            nullable: true,
        },
    ]
}

fn batch() -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("a")])),
        ],
    )
    .expect("batch");
    (schema, vec![b])
}

fn lineage() -> control_plane_core::LineageEvent {
    control_plane_core::LineageEvent::completed(vec![], serde_json::json!({ "source": "test" }))
}

fn always_inline() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: usize::MAX,
        flush_byte_threshold: i64::MAX,
    }
}

/// mode=stream (log) with a MATCHING bucket count against an already-declared
/// CDC table must be rejected with Validation, leaving the registry kind='cdc'.
/// Before the fix this is silently accepted (the defect).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_declare_matching_count_against_cdc_table_is_validation_error() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let table = TableRef {
        schema: "s".into(),
        name: "w_cdc".into(),
    };
    cp.define_type(
        ObjectType::build("WCdc", ("s", "w_cdc"))
            .prop_req("id", "Long")
            .prop("name", "String")
            .identity("id")
            .done(),
    )
    .await
    .expect("define type");

    // Declare the table as CDC (buckets=2), as POST /models/{type}?mode=cdc does.
    let (schema, batches) = batch();
    land_cdc(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        None,
        Some(CdcDecl {
            buckets: 2,
            bucket_key: "id".into(),
            merge_engine: MergeEngine::LastRow,
        }),
        &[],
    )
    .await
    .expect("cdc declare lands");

    // The defect repro: a log declare with the MATCHING count (2) against the
    // CDC table, as POST /datasets/s/w_cdc?mode=stream&buckets=2 does.
    let (schema, batches) = batch();
    let res = land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        Some(2),
    )
    .await;
    assert!(
        matches!(res, Err(ControlPlaneError::Validation(_))),
        "log declare with a matching count against a cdc table must be Validation, got {res:?}"
    );

    // The registry row is untouched: still kind='cdc' with its bucket_key.
    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("live_table_id")
        .expect("cdc table has a live mirror row");
    drop(conn);
    let meta = cp
        .stream_meta(tid)
        .await
        .expect("stream_meta")
        .expect("declared stream table");
    assert_eq!(meta.kind, StreamKind::Cdc, "registry stays kind='cdc'");
    assert_eq!(meta.bucket_count, 2, "bucket count unchanged");
    assert_eq!(
        meta.bucket_key.as_deref(),
        Some("id"),
        "bucket_key unchanged"
    );

    // Precedence preserved: a MISMATCHED count against the same cdc table still
    // reports the bucket-count Conflict (the n != m arm fires first, exactly as
    // it does for the cdc-against-log direction today).
    let (schema, batches) = batch();
    let res = land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        Some(3),
    )
    .await;
    assert!(
        matches!(res, Err(ControlPlaneError::Conflict(_))),
        "log declare with a mismatched count against a cdc table stays Conflict, got {res:?}"
    );

    drop(wh);
    drop(catalog);
}

/// Control (non-regression): a second same-count log declare against a LOG
/// table is still accepted — the pure log redeclare path the slice-2
/// byte-identical constraint pins.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_redeclare_matching_count_against_log_table_stays_ok() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let table = TableRef {
        schema: "s".into(),
        name: "w_log".into(),
    };

    let (schema, batches) = batch();
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        Some(2),
    )
    .await
    .expect("first log declare lands");

    let (schema, batches) = batch();
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        always_inline(),
        lineage(),
        Some(2),
    )
    .await
    .expect("same-count log redeclare stays accepted (byte-identical log path)");

    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("live_table_id")
        .expect("log table has a live mirror row");
    drop(conn);
    let meta = cp
        .stream_meta(tid)
        .await
        .expect("stream_meta")
        .expect("declared stream table");
    assert_eq!(meta.kind, StreamKind::Log, "registry reads kind='log'");
    assert_eq!(meta.bucket_count, 2);

    drop(wh);
    drop(catalog);
}
```

(Note the `cp` binding: `stream_merge_declare.rs`'s `fresh_db()` returns the control plane whose `define_type`/`stream_meta` this test calls via the `Ontology`/`StreamTables` traits — keep whatever concrete shape that sibling uses; only the two new `land` calls and the `StreamKind` assertions are new material.)

- [ ] **Step 2: Wire the test target**

In `src/control-plane/postgres/BUCK`, immediately after the `stream-merge-declare` target (it closes at line 1687), add a `loom_fixture_test` mirroring it exactly:

```python
loom_fixture_test(
    name = "stream-log-vs-cdc-declare",
    crate = "stream_log_vs_cdc_declare",
    srcs = ["tests/stream_log_vs_cdc_declare.rs"],
    crate_root = "tests/stream_log_vs_cdc_declare.rs",
    deps = [
        "//src/testing:seed",
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:stream-log-vs-cdc-declare`
Expected: FAIL — `log_declare_matching_count_against_cdc_table_is_validation_error` panics on the first assert (`got Ok(...)`: the matching-count log declare is silently accepted — the defect). `log_redeclare_matching_count_against_log_table_stays_ok` already PASSES (it pins current behavior). If the repro case passes before any production change, STOP — the defect is mischaracterized; re-read `reconcile_stream_mode` before touching anything.

- [ ] **Step 4: Implement the guard**

In `src/control-plane/postgres/src/stream.rs`, inside `reconcile_stream_mode`'s `(Some(_), Some(m))` arm, the existing CDC-side guard reads (lines 135-151):

```rust
        (Some(_), Some(m)) => {
            // A `Cdc` request against an already-declared table must also match its
            // KIND, not just its bucket count — a log table with the same bucket
            // count is not a valid cdc target. (A `Log` request is unaffected: it
            // keeps its original count-only comparison, so log/batch behavior is
            // unchanged.)
            if matches!(decl, StreamDecl::Cdc { .. })
                && existing_meta
                    .as_ref()
                    .is_some_and(|meta| meta.kind != StreamKind::Cdc)
            {
                return Err(ControlPlaneError::Validation(format!(
                    "cannot declare {}.{} as a cdc table: already declared with a \
                     different stream kind",
                    table.schema, table.name
                )));
            }
```

(a) Rewrite the now-stale parenthetical in that comment — replace the sentence `(A `Log` request is unaffected: it keeps its original count-only comparison, so log/batch behavior is unchanged.)` with `(The symmetric Log-side guard follows below.)`.

(b) Insert the symmetric guard immediately AFTER that block and BEFORE the engine-immutability check (currently line 156):

```rust
            // Symmetric kind guard for the Log side (iss-stream-log-vs-cdc-declare):
            // a `mode=stream` (log) request against an already-declared table must
            // also match its KIND — a cdc table with the same bucket count is not a
            // valid log-declare target. Fires only when a `kind='cdc'` registry row
            // already exists, so the pure log/batch paths (no such row) stay
            // byte-identical.
            if matches!(decl, StreamDecl::Log(_))
                && existing_meta
                    .as_ref()
                    .is_some_and(|meta| meta.kind != StreamKind::Log)
            {
                return Err(ControlPlaneError::Validation(format!(
                    "cannot declare {}.{} as a log stream table: already declared \
                     with a different stream kind",
                    table.schema, table.name
                )));
            }
```

(c) Update `reconcile_stream_mode`'s doc comment (lines 37-46): the "Rejections" sentence currently ends `…; a `Cdc` request against an already-declared table of a DIFFERENT kind (e.g. a log table) → `Validation`.` — extend it to `…; a request against an already-declared table of a DIFFERENT kind (cdc-vs-log in either direction) → `Validation`.`

No other production change. `StreamKind` is already imported (`stream.rs:3`); no new SQL, no `.sqlx` regen.

**Micro-batch MV coverage (added at plan review; post-dates the spec).** `#road-stream-continuous` (PR #418) landed `inline_append_mv` (`iceberg_inline.rs:826-848`), which commits an MV's output through `inline_append_decl(…, &StreamDecl::Log(buckets), …)` — i.e. **every micro-batch MV commit now traverses this arm** with a `Log` decl. Two consequences, both already handled by the guard as written:

- The ISSUES entry's second exposure ("an MV whose output names a pre-existing CDC table is refused at neither define nor commit and would stamp log-framing into CDC storage") is **closed by this same guard**, with no extra code: the commit's `StreamDecl::Log` against a `kind='cdc'` output now rejects at `reconcile_stream_mode`.
- A normal MV (log-kind output) must stay unaffected. That is the byte-identical constraint applied to a path the spec's non-regression list predates, so Step 5 sweeps it explicitly.

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test --console none //src/control-plane/postgres:stream-log-vs-cdc-declare`
Expected: PASS (both tests).

Then the non-regression sweep of every suite that pins the arm and the pure log/batch/CDC declare paths:

Run: `buck2 test --console none //src/control-plane/postgres:stream-merge-declare //src/services/ingest:stream-declare //src/services/ingest:model-cdc-declare //src/services/query-api:stream-cdc-declare //src/control-plane/postgres:stream-flush-persist //src/control-plane/postgres:stream-mv-triggers //src/services/engine:mv-commit-wire`
Expected: PASS, no changes to any of those files. (The last three cover the `StreamDecl::Log` callers the spec's list predates: `stream-flush-persist` is the other `inline_append(…, Some(n))` log caller, and the two MV suites now traverse the new guard on every micro-batch commit.)

- [ ] **Step 6: Run prek + commit**

```bash
git add -A
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "fix(stream): reject mode=stream (log) declare against a cdc table

reconcile_stream_mode's count-equal redeclare arm kind-checked only Cdc
requests; a Log request with a matching bucket count against a kind='cdc'
table was silently accepted (rows hash-bucketed as CDC). Add the symmetric
Log-side guard — Validation, mirroring the cdc-against-log message — gated
on a pre-existing cdc registry row so pure log/batch paths stay
byte-identical. Closes iss-stream-log-vs-cdc-declare (seam test; HTTP e2e
follows)."
```

---

## Task 2: Cross-surface HTTP e2e (models declares CDC, datasets attempts log)

**Files:**
- Create: `src/services/ingest/tests/stream_log_vs_cdc_http.rs`
- Modify: `src/services/ingest/BUCK` (new target after `model-cdc-declare`, line 240)

**Interfaces:**
- Consumes: `model_cdc_declare.rs`'s harness verbatim (`app_state`, `protected`, `session_token`, `grant_write_absent_type`, `sample_batch`/`ipc_bytes`, `stream_meta_row`); the Task 1 guard's "different stream kind" message via `IngestError::into_api` (`ingest/src/http.rs:102-104`).
- Produces: `//src/services/ingest:stream-log-vs-cdc-http` — the end-to-end pin of the obscure two-surface reachability.

- [ ] **Step 1: Write the e2e test**

Create `src/services/ingest/tests/stream_log_vs_cdc_http.rs`. Copy `model_cdc_declare.rs`'s helper block verbatim (`sample_batch`, `ipc_bytes`, `app_state`, `protected`, `session_token`, `grant_write_absent_type`, `post_model_q`, `stream_meta_row` — same imports), then add a dataset-POST helper that returns status + body (the body assertion is the one thing `post_model_q` doesn't expose) and the single test:

```rust
//! Cross-surface repro for iss-stream-log-vs-cdc-declare: declare a CDC table
//! via POST /models/{type}?mode=cdc, then attempt a matching-count
//! POST /datasets/{schema}/{table}?mode=stream against the SAME physical table.
//! Must 400 with the kind-mismatch message; the registry row stays kind='cdc'.
//! Hermetic Postgres fixture; tower oneshot, no socket. Mirrors
//! `tests/model_cdc_declare.rs`'s auth + raw-SQL readback harness.

/// POST /datasets/{schema}/{table}?{query} with a bearer token; returns
/// (status, body-text) so the kind-mismatch message can be asserted.
async fn post_dataset_q(
    app: Router,
    schema: &str,
    table: &str,
    query: &str,
    token: &str,
    body: Vec<u8>,
) -> (StatusCode, String) {
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/datasets/{schema}/{table}?{query}"))
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test(flavor = "multi_thread")]
async fn mode_stream_matching_count_against_cdc_table_is_400() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "widget").await;
    let token = session_token(&pg, "alice").await;

    // Surface 1: declare main.widget as CDC (buckets=2) via the models path.
    let app = protected(state.clone(), pg.clone());
    let status = post_model_q(
        app,
        "widget",
        "identity=id&mode=cdc&buckets=2",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "mode=cdc declare lands");
    assert_eq!(
        stream_meta_row(&pool, "main", "widget").await,
        Some((2, "cdc".to_string(), Some("id".to_string())))
    );

    // Surface 2: the SAME physical table via the datasets path, mode=stream
    // with the MATCHING bucket count — the silently-accepted case before the
    // fix. Must 400 with the kind-mismatch message.
    let app = protected(state, pg.clone());
    let (status, body) = post_dataset_q(
        app,
        "main",
        "widget",
        "mode=stream&buckets=2",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "mode=stream against a cdc table is rejected (body: {body})"
    );
    assert!(
        body.contains("different stream kind"),
        "the kind-mismatch message is echoed to the client, got: {body}"
    );

    // The rejected write changed nothing: still kind='cdc', count 2, key id.
    assert_eq!(
        stream_meta_row(&pool, "main", "widget").await,
        Some((2, "cdc".to_string(), Some("id".to_string()))),
        "the rejected log declare leaves the cdc registry row untouched"
    );
}
```

(Everything not shown — imports, `sample_batch`, `ipc_bytes`, `app_state`, `protected`, `session_token`, `grant_write_absent_type`, `post_model_q`, `stream_meta_row` — is copied unchanged from `tests/model_cdc_declare.rs`; do not restructure it. The `/datasets` route sits behind the same `protect` wrapper, so the dataset POST carries the same bearer token; it performs no per-type ACL check, so the existing `widget` write grant is sufficient.)

- [ ] **Step 2: Wire the test target**

In `src/services/ingest/BUCK`, immediately after the `model-cdc-declare` target (line 240), add a `loom_fixture_test` mirroring its deps exactly:

```python
loom_fixture_test(
    name = "stream-log-vs-cdc-http",
    crate = "stream_log_vs_cdc_http",
    srcs = ["tests/stream_log_vs_cdc_http.rs"],
    crate_root = "tests/stream_log_vs_cdc_http.rs",
    deps = [
        ":ingest",
        "//src/services/runtime:runtime",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow",
        "//third-party:axum",
        "//third-party:http-body-util",
        "//third-party:iceberg",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:tower",
    ],
)
```

- [ ] **Step 3: Run the test to verify it passes (and distinguishes)**

Run: `buck2 test --console none //src/services/ingest:stream-log-vs-cdc-http`
Expected: PASS (Task 1's guard is already in). This test distinguishes the fix — on the pre-fix tree the dataset POST returns 200 and the `BAD_REQUEST` assert fails; if paranoid, verify once with `git stash` of the Task 1 `stream.rs` hunk (expect FAIL), then unstash.

- [ ] **Step 4: Run prek + commit**

```bash
git add -A
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "test(stream): cross-surface e2e for log-vs-cdc declare reject

POST /models/{type}?mode=cdc declares main.widget as CDC; a matching-count
POST /datasets/main/widget?mode=stream must 400 with the kind-mismatch
message and leave the registry row (kind='cdc', bucket_key) untouched —
the two-surface reachability the register entry called obscure, pinned
end to end."
```

---

## Task 3: Final verification + close the register item

**Files:**
- Modify: `docs/ISSUES.md` — remove the `iss-stream-log-vs-cdc-declare` entry (closed).
- Modify: `docs/system-capabilities/stream.md` — two exact edits (located at plan review): **`:42-44`**, whose bullet currently reads "A `mode=cdc` request against a table already declared a **different kind** (e.g. an existing `log` table) is rejected — but the converse is not: see `#iss-stream-log-vs-cdc-declare` in Known gaps" → rewrite as a symmetric kind check in **either** direction (log-vs-cdc), noting it also refuses a micro-batch MV commit whose declared log output names a pre-existing CDC table; and **`:657-659`**, the Known-gaps bullet for the issue → delete it.

- [ ] **Step 1: Full verification sweep**

```bash
buck2 build -v0 --console none //src/...
buck2 test --console none //src/control-plane/postgres:stream-log-vs-cdc-declare //src/services/ingest:stream-log-vs-cdc-http //src/control-plane/postgres:stream-merge-declare //src/services/ingest:stream-declare //src/services/ingest:model-cdc-declare //src/services/query-api:stream-cdc-declare //src/services/query-api:stream-cdc-e2e //src/services/query-api:stream-cdc-consolidate //src/control-plane/postgres:stream-flush-persist //src/control-plane/postgres:stream-mv-triggers //src/services/engine:mv-commit-wire
```

Expected: build silent (exit 0); `Tests finished: Pass N. Fail 0`. (Locally, a wider `buck2 test //src/... -j 8` is the belt-and-braces option; in a cloud session scope to the list above.)

- [ ] **Step 2: Close the register item via `loom-docs-update`**

Run the `loom-docs-update` skill (or edit directly, matching the register grammar): remove the `iss-stream-log-vs-cdc-declare` entry from `docs/ISSUES.md`; update `docs/system-capabilities/stream.md` per above. Validate: `bash tools/docs.sh validate`.

- [ ] **Step 3: Commit + finish the branch**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "docs(stream): close iss-stream-log-vs-cdc-declare

The log-vs-cdc declaration kind check is now symmetric; remove the ISSUES
entry and update the stream capability doc."
```

Then use the finishing-a-development-branch skill (push + open PR; per project convention, always the PR option).

---

## Self-Review (run after writing the plan)

**1. Spec coverage.** Guard placement/message (Architecture) ⇒ Task 1 Step 4. Seam test cases 1-3 (Testing) ⇒ Task 1 Step 1 (repro + Conflict precedence + log-redeclare control). Cross-surface HTTP e2e ⇒ Task 2. Non-regression suites named by the spec ⇒ Task 1 Step 5 + Task 3 Step 1. No-`.sqlx` constraint ⇒ Global Constraints + Task 1 Step 4 note. Register close ⇒ Task 3. ✓

**2. Placeholder scan.** No `TBD`/`TODO`; the one deliberate "copy from sibling" instruction (Task 2 Step 1) names the exact file and helper list rather than sketching them, matching how `stream_merge_declare.rs` itself was specified against `stream_cdc_declare.rs`. ✓

**3. Type consistency.** `land(pool, catalog, table, columns, schema, batches, limits, lineage, stream_buckets)` — 9 params (`iceberg_landing.rs:108`); `land_cdc` adds `cdc, jobs` (`:148`); `CdcDecl { buckets, bucket_key, merge_engine }` (`:45`); `StreamMeta { bucket_count, kind, bucket_key, changelog_table_id, merge_engine }`; `stream_meta_row` returns `Option<(i32, String, Option<String>)>`. All verified against the current tree. ✓

**4. Failure-mode honesty.** Task 1 Step 3 names the exact expected failure (repro accepts with `Ok`) and instructs STOP if it passes pre-fix; Task 2 Step 3 explains how the e2e distinguishes the fix. ✓
