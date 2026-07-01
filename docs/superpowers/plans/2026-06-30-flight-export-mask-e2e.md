# Masked-column governed Flight export — e2e coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one end-to-end test that drives a **column-masked** scalar through the live engine's Arrow Flight export, proving the advertised `get_flight_info` schema and the streamed `do_get` data schema agree (both `Utf8`) and the masked column's values are the literal `'***'`.

**Architecture:** Pure test-coverage addition to the existing `governed_flight_export_e2e.rs` integration target. Add a `setup_with_mask` harness variant mirroring `setup_with_cap` (same landed `vector(4)` `Chunk` dataset, same engine/export wiring) that grants Read through `e2e_support::grant_read_columns` with the scalar `id` column masked, then add one `#[tokio::test]` case asserting advertised-vs-data schema lockstep + masked values + an unmasked-column spot check. No production code changes; no BUCK changes (the target already depends on `arrow-schema`, `arrow-flight`, `e2e-support`).

**Tech Stack:** Rust, buck2 (`loom_fixture_test`), Arrow Flight 58.3.0, tonic, hermetic Postgres fixture (`PgFixture`).

## Global Constraints

- **Tests are `rust_test` integration targets, never inline `#[cfg(test)]`.** This change adds a `#[tokio::test]` to an existing `tests/*.rs` file already wired as `loom_fixture_test` — correct by construction. Do **not** add any `#[test]`/`#[tokio::test]` to a `src/**.rs` file (the `no-inline-tests` hook fails the build).
- **No production code change.** Masking is shared with the governed HTTP read path and is unchanged; this slice is coverage only (`src/services/query-api/src/flight_export.rs`, `handler.rs`, `sql.rs` stay untouched).
- **Run the fixture suite via buck2, not cargo:** `buck2 test //src/services/query-api:governed-flight-export-e2e` (a `loom_fixture_test`, local-routed). Redirect long buck2 output to a file and grep it — never pipe `buck2 test` through `tail`/`head`.
- **The masked column is a scalar** (`id`) — its `'***'` constant renders as `Utf8`. The `vector(4)` `embedding` is **not** masked (vector-column masking is out of scope) and stays `List<Float32>`.
- **Conventional Commits** on the commit message (the `conventional-commit` hook). Use a `test(query-api): …` subject.

---

### Task 1: `setup_with_mask` harness variant + masked-export e2e case

**Files:**
- Modify: `src/services/query-api/tests/governed_flight_export_e2e.rs` (add one helper fn + one `#[tokio::test]` fn; extend the file's doc comment)
- Test: same file (the deliverable **is** the test)

**Interfaces:**
- Consumes (already present in this file / `e2e_support`):
  - `fn setup_with_cap(fx: &PgFixture, max_rows: u32) -> (SocketAddr, String, Arc<PgControlPlane>, TempDir, TempDir)` — the existing full harness; copy its body for the mask variant.
  - `e2e_support::grant_read_columns(cp: &PgControlPlane, role: &RoleId, type_name: &str, deny_columns: Vec<String>, mask_columns: Vec<String>)` — coarse Read Allow + a column policy (no row filter). Already a dep; signature confirmed in `tests/e2e_support.rs:546`.
  - `e2e_support::subject_with_role`, `e2e_support::session_token` — used by `setup_with_cap`.
  - `chunk_cmd() -> ExportCommand`, `authed<T>(msg, token) -> tonic::Request<T>`, `spawn_flight`, `make_catalog`, `columns`, `ipc_body`, `lineage` — all already in this file.
  - `arrow_flight::flight_service_client::FlightServiceClient`, `FlightDescriptor`, `FlightRecordBatchStream`, `FlightError` — already imported.
  - `arrow_schema::DataType` — already imported (line 20).
  - `arrow_array::{StringArray, Float32Array, ListArray, RecordBatch}` — `RecordBatch`, `Float32Array`, `ListArray` already imported (line 13); **add `StringArray`** to that import for the masked-value assertion.
  - `arrow_flight::FlightInfo::try_decode_schema(self) -> Result<arrow_schema::Schema, arrow::error::ArrowError>` — decodes the IPC-encoded advertised schema from the `FlightInfo`. Consumes `self`; `FlightInfo` is `Clone`, so decode from a clone and read the ticket from the original (or vice-versa).
- Produces: nothing consumed downstream — a leaf test case.

**Masked column choice:** `Chunk` has exactly two properties — `id` (`long`, scalar, also the declared identity) and `embedding` (`vector(4)`). The masked column is therefore `id`. Masking the identity column is the documented purpose of `grant_read_columns` (`tests/e2e_support.rs:545` — "the shape that governs the identity column"), and the compiled export SQL has **no `ORDER BY`**, so replacing `id` with the `'***'` constant does not perturb row order. The **unmasked spot check** is on the non-masked `embedding` column: it must still arrive as `List<Float32>` carrying the value-exact row-0 vector `[0.1, 0.2, 0.3, 0.4]`, proving only `id` was replaced (no over-masking).

- [ ] **Step 1: Add the `setup_with_mask` helper**

Insert directly **after** `setup_with_cap` (after its closing brace near line 270). It is `setup_with_cap` with the cap fixed at `100_000` (same as `setup`) and the ACL grant swapped from `grant_read` to `grant_read_columns(..., mask_columns: vec!["id"])`. Copy the body verbatim from `setup_with_cap`, changing only the two highlighted lines.

```rust
/// Like [`setup`] but masks the scalar `id` column via a column policy, for the masked-export
/// case. Identical landing / engine / export wiring; only the ACL grant differs (coarse Read
/// Allow + a `mask_columns: ["id"]` policy). The `vector(4)` `embedding` stays unmasked.
async fn setup_with_mask(
    fx: &PgFixture,
) -> (
    std::net::SocketAddr,
    String,
    Arc<PgControlPlane>,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let wh = tempfile::tempdir().expect("warehouse");
    let warehouse = wh.path().display().to_string();

    let table = TableRef {
        schema: "wh".into(),
        name: "chunks".into(),
    };
    let catalog = make_catalog(dsn.clone(), &warehouse).await;
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(1500),
        0,
        i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "chunks"),
    )
    .await
    .expect("land vector");

    cp.define_type(ObjectType {
        name: TypeName("Chunk".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "long".into(),
                required: true,
            },
            PropertyDef {
                name: "embedding".into(),
                ty: "vector(4)".into(),
                required: true,
            },
        ],
        derived: vec![],
        table: table.clone(),
        identity: Some("id".into()),
    })
    .await
    .expect("define Chunk type");

    // ACL: Read Allow on Chunk refined by a column policy that MASKS the scalar `id`.
    let (_subj, role) = e2e_support::subject_with_role(&cp, "reader").await;
    e2e_support::grant_read_columns(&cp, &role, "Chunk", vec![], vec!["id".into()]).await;
    let token = e2e_support::session_token(&cp, "reader").await;

    let (sock_dir, sock_str) = spawn_flight(fx, &db, &warehouse).await;
    let cp = Arc::new(cp);
    let auth: Arc<dyn Auth + Send + Sync> = cp.clone();
    let cp_dyn: Arc<dyn ControlPlane> = cp.clone();
    let flight_engine = FlightSqlClient::connect(sock_str)
        .await
        .expect("engine connect");
    let export = FlightExportService::new(auth, cp_dyn, flight_engine, 100_000);

    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind tcp");
    let addr = tcp.local_addr().expect("addr");
    let incoming = TcpListenerStream::new(tcp);
    tokio::spawn(async move {
        let _serve = Server::builder()
            .add_service(FlightServiceServer::new(export))
            .serve_with_incoming(incoming)
            .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (addr, token, cp, wh, sock_dir)
}
```

- [ ] **Step 2: Add `StringArray` to the `arrow_array` import**

Change line 13 from:

```rust
use arrow_array::{Float32Array, Int64Array, ListArray, RecordBatch};
```

to:

```rust
use arrow_array::{Float32Array, Int64Array, ListArray, RecordBatch, StringArray};
```

- [ ] **Step 3: Write the masked-export e2e test case**

Append at the end of the file (after `export_cap_exceeded_errors_stream`). The test drives authed `get_flight_info` + `do_get` exactly like `export_streams_vectors_value_exact`, then asserts the three spec requirements plus the unmasked spot check.

```rust
/// A column-masked scalar (`id`) exports end-to-end through the live engine with the advertised
/// `get_flight_info` schema and the streamed `do_get` data schema in lockstep: both report the
/// masked column as `Utf8`, every masked value is the literal `"***"`, and the UNmasked
/// `embedding` survives value-exact as `List<Float32>` (no over-masking).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_masked_scalar_schema_and_values() {
    let fx = PgFixture::start();
    let (addr, token, _cp, _wh, _sock) = setup_with_mask(&fx).await;

    let url = format!("http://{addr}");
    let channel = tonic::transport::Endpoint::try_from(url)
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    let mut client = FlightServiceClient::new(channel);
    let desc = FlightDescriptor::new_cmd(chunk_cmd().encode());
    let info = client
        .get_flight_info(authed(desc, &token))
        .await
        .expect("get_flight_info")
        .into_inner();

    // (1) Advertised schema: the masked `id` field is Utf8 (the '***' constant's type), not Int64.
    let advertised = info
        .clone()
        .try_decode_schema()
        .expect("decode advertised schema");
    let adv_id = advertised.field_with_name("id").expect("advertised id field");
    assert_eq!(
        adv_id.data_type(),
        &DataType::Utf8,
        "the masked `id` must be advertised as Utf8 (the '***' constant), not its declared Int64"
    );

    let ticket = info
        .endpoint
        .into_iter()
        .next()
        .and_then(|e| e.ticket)
        .expect("ticket");
    let stream = client
        .do_get(authed(ticket, &token))
        .await
        .expect("do_get")
        .into_inner();
    let data = stream.map_err(FlightError::from);
    let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(data)
        .try_collect()
        .await
        .expect("collect batches");
    assert!(!batches.is_empty(), "export produced at least one batch");

    // (2) Data-schema lockstep: the streamed `id` column is ALSO Utf8 (advertised == data).
    let data_id = batches[0]
        .schema()
        .field_with_name("id")
        .expect("data id field")
        .data_type()
        .clone();
    assert_eq!(
        data_id,
        DataType::Utf8,
        "the streamed `id` must be Utf8 — the divergence guard (advertised schema == data schema)"
    );

    // (3) Masked values: every `id` across all batches is the literal "***".
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id is a Utf8/StringArray");
        for i in 0..ids.len() {
            assert_eq!(ids.value(i), "***", "every masked id value must be the '***' literal");
        }
    }

    // Spot check (no over-masking): the UNmasked `embedding` survives as List<Float32>, and row 0's
    // value-exact vector [0.1, 0.2, 0.3, 0.4] is still present — only `id` was replaced.
    let emb = batches[0]
        .column_by_name("embedding")
        .expect("embedding column");
    assert!(
        matches!(emb.data_type(), DataType::List(_)),
        "the unmasked embedding must stay List<Float32>, not be masked to Utf8"
    );
    let mut embeddings: Vec<Vec<f32>> = Vec::new();
    for batch in &batches {
        let list = batch
            .column_by_name("embedding")
            .expect("embedding column")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list array");
        for row in list.iter().flatten() {
            let f = row
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("float32 element");
            embeddings.push(f.values().to_vec());
        }
    }
    assert!(
        embeddings.contains(&vec![0.1f32, 0.2, 0.3, 0.4]),
        "the unmasked embedding must carry row 0's exact vector [0.1, 0.2, 0.3, 0.4]"
    );
}
```

- [ ] **Step 4: Extend the file's module doc comment**

Update the header doc comment (lines 1–8) to mention the new masked case alongside the four it already lists. Add a bullet to the asserted list, e.g. after the existing items:

```rust
//! (4) a column-masked scalar (`id`) is advertised AND streamed as `Utf8` with every value the
//! literal `'***'`, while the unmasked `embedding` survives value-exact as `List<Float32>`.
```

- [ ] **Step 5: Run the new case and verify it passes**

Run (redirect to a file — never pipe `buck2 test` through `tail`/`head`):

```bash
buck2 test //src/services/query-api:governed-flight-export-e2e > /tmp/mask.log 2>&1; grep -E "Tests finished|FAIL|PASS|error\[" /tmp/mask.log
```

Expected: the target's test cases all pass — the four pre-existing cases plus `export_masked_scalar_schema_and_values`. Confirm `Tests finished: Pass N. Fail 0.` with N ≥ 5.

- [ ] **Step 6: Run clippy on the test crate**

The test file is exempt from panic-safety lints (`loom_fixture_test` injects the allows) but must still be clippy-clean otherwise.

```bash
buck2 build '//src/services/query-api:governed-flight-export-e2e[clippy.txt]' > /tmp/mask-clippy.log 2>&1; cat $(buck2 build --show-output '//src/services/query-api:governed-flight-export-e2e[clippy.txt]' 2>/dev/null | awk '{print $2}') 2>/dev/null; grep -E "error|warning" /tmp/mask-clippy.log || echo "clippy clean"
```

Expected: empty clippy output (clean).

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/tests/governed_flight_export_e2e.rs
git commit -m "test(query-api): e2e masked-scalar Flight export schema + values"
```

---

## Self-Review

**1. Spec coverage** (`docs/superpowers/specs/2026-06-30-flight-export-mask-e2e-design.md`):
- "Add a `setup_with_mask` helper mirroring `setup_with_cap`" → Step 1. ✓
- "grant through `grant_read_columns(..., deny_columns: vec![], mask_columns: vec!["<scalar col>"])`" → Step 1 uses `vec![]`, `vec!["id".into()]`. ✓
- "masked column is a scalar property (not the `vector(4)` embedding)" → `id` is the only scalar; embedding left unmasked. ✓
- Assertion 1 (advertised masked field is `Utf8`) → Step 3 part (1). ✓
- Assertion 2 (streamed data-schema masked field is `Utf8`, lockstep) → Step 3 part (2). ✓
- Assertion 3 (every masked value is `"***"`) → Step 3 part (3). ✓
- "spot check on an unmasked column guards against over-masking" → Step 3 embedding spot check. ✓
- "one `#[tokio::test]` case … reusing the existing harness" → exactly one new test fn, harness reused. ✓
- Out of scope: no production change, no vector-column mask, Iceberg-only — honored (test-only, masks `id`, reuses the Iceberg landing path). ✓
- Acceptance: "new case passes in `buck2 test //src/...`; existing four cases continue to pass" → Step 5. ✓

**2. Placeholder scan:** No TBD/TODO/"handle edge cases"/"similar to" — all code is literal. ✓

**3. Type consistency:** `setup_with_mask` returns the same 5-tuple as `setup`/`setup_with_cap` and is destructured identically in the test. `try_decode_schema` consumes `self`, so the test calls it on `info.clone()` before moving `info.endpoint`. `StringArray` import added in Step 2 before use in Step 3. `grant_read_columns` arg order (deny, then mask) matches `e2e_support.rs:546`. ✓
