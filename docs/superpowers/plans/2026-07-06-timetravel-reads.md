# Time-travel reads (`AS OF`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let clients read typed objects and dataset detail *as of* a past snapshot via `?as_of_snapshot=<id>` or `?as_of=<rfc3339>` on `GET /objects/{type}` and `GET /datasets/{schema}/{table}`.

**Architecture:** query-api resolves the selector to a concrete `SnapshotId` (using the control-plane catalog), then — for object data — ships SQL + that id on a **new loom-native `AsOfStatementQuery` Flight ticket** to the engine, which registers the referenced table's provider at that snapshot instead of `current_snapshot`. Dataset detail is a pure query-api catalog resolution (no engine). The no-selector path is byte-identical to today.

**Tech Stack:** Rust, buck2, DataFusion, Arrow Flight (tonic), sqlx compile-time queries, hermetic Postgres fixture tests.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`. Put each test in a sibling `tests/<name>.rs` wired as its own target in the crate `BUCK`, mirroring an existing target. The `no-inline-tests` prek hook fails on any `#[test]`/`#[tokio::test]` under `src/`.
- **Fixture tests use the `loom_fixture_test` macro**, not bare `rust_test`, or they boot without the PG/MinIO env.
- **Run tests:** `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`. Never pipe `buck2 test`/`bxl` through `tail`/`head` (stalls). On a root cloud host, fixture test *runs* go to RE (the buck2 shim injects `--unstable-allow-all-tests-on-re`).
- **Postgres SQL changes** require regenerating the committed `.sqlx` cache: `tools/sqlx-prepare.sh`, then commit the `.sqlx` diff. The `sqlx-cache-check` rust_test enforces freshness.
- **Before every commit:** `buck2 run //tools:prek -- run --all-files` (rustfmt is a *separate* hook from clippy — clippy-clean ≠ lint-clean).
- **Clippy is strict** (pedantic + restriction). No `unwrap`/`expect`/`panic`/indexing in production code; test code is exempt via `loom_rust_test`/`loom_fixture_test`. Use `#[expect(lint, reason = "...")]` for local allows.
- **Commit trailer** (every commit):
  ```
  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01KrF8cSY7q1odAy9AW6aAnd
  ```
- **Param-ordering convention for this plan:** append the new `at: Option<SnapshotId>` as the **last** parameter of each existing function it is added to, to minimize call-site churn.
- Branch is already `feat/timetravel-reads` (off `origin/main`). Do not open the PR until the whole plan is green (a later `finishing-a-development-branch` step handles that).

---

### Task 1: `Catalog::snapshot_as_of` across all adapters + contract

**Files:**
- Modify: `src/control-plane/core/src/catalog.rs` (add trait method)
- Modify: `src/control-plane/memory/src/catalog.rs` (impl)
- Modify: `src/control-plane/postgres/src/iceberg_catalog.rs` (impl)
- Modify: `src/control-plane/testkit/src/lib.rs` (extend `catalog_contract` — the red-first test)
- Modify: `src/control-plane/postgres/.sqlx/` (regenerated cache — committed)

**Interfaces:**
- Produces: `Catalog::snapshot_as_of(&self, table: &TableRef, ts: OffsetDateTime) -> Result<Option<Snapshot>>` — the latest snapshot at or before `ts` at which `table` is live, or `None` if the table has no such snapshot. Every `Catalog` impl must define it. Covered for **both** adapters by the shared `catalog_contract` (already run by `memory/tests/catalog.rs` and `postgres/tests/iceberg_catalog.rs`).

Note: `CatalogSeed` is implemented on test-only wrappers (`MemSeeder` in `memory/tests/catalog.rs`, `IcebergSeeder` in `postgres/tests/iceberg_catalog.rs`), **not** on the control planes themselves — which is exactly why the coverage goes through `catalog_contract`, not a standalone per-adapter test.

- [ ] **Step 1: Write the failing contract assertion (red)**

In `src/control-plane/testkit/src/lib.rs`, inside `catalog_contract`, after the `snapshots` history block (after ~line 359, where `hist` is ascending and includes `seeded[0]`/`seeded[1]`), add:

```rust
    // snapshot_as_of: exact latest time -> latest; a time strictly between the two
    // seeded snapshots -> the earlier; strictly before the first -> None.
    let s0 = hist.iter().find(|s| s.id == seeded[0].snapshot).unwrap();
    let s1 = hist.iter().find(|s| s.id == seeded[1].snapshot).unwrap();
    assert_eq!(
        catalog.snapshot_as_of(&t, s1.time).await.unwrap().map(|s| s.id),
        Some(s1.id),
        "as_of at the latest snapshot's time resolves to it"
    );
    // Guard the midpoint assertion: only meaningful when the two snapshots have
    // distinct times (a very fast backend may stamp both within clock resolution).
    if s0.time < s1.time {
        let between = s0.time + (s1.time - s0.time) / 2;
        assert_eq!(
            catalog.snapshot_as_of(&t, between).await.unwrap().map(|s| s.id),
            Some(s0.id),
            "as_of between the two resolves to the earlier"
        );
    }
    assert_eq!(
        catalog
            .snapshot_as_of(&t, s0.time - time::Duration::seconds(1))
            .await
            .unwrap(),
        None,
        "as_of before the first snapshot resolves to None"
    );
```

Run: `buck2 build //src/control-plane/testkit/... 2>&1 | grep -iE "no method|error\[" ` — expect a compile error (`snapshot_as_of` not on `Catalog`). That is the red state.

- [ ] **Step 2: Add the trait method**

In `src/control-plane/core/src/catalog.rs`, inside `trait Catalog`, after `current_snapshot` (~line 71):

```rust
    /// The latest snapshot at or before `ts` at which `table` is live, or `None`
    /// if the table has no live snapshot at/before that instant (created later, or
    /// never existed). Time-travel resolution for a wall-clock `as_of` read.
    async fn snapshot_as_of(
        &self,
        table: &TableRef,
        ts: OffsetDateTime,
    ) -> Result<Option<Snapshot>>;
```

- [ ] **Step 3: Implement it for the memory fake**

In `src/control-plane/memory/src/catalog.rs`, add to `impl Catalog for MemoryControlPlane` (after `current_snapshot`, ~line 56):

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn snapshot_as_of(
        &self,
        table: &TableRef,
        ts: OffsetDateTime,
    ) -> Result<Option<Snapshot>> {
        let cat = self.catalog.lock();
        let Some(t) = cat.tables.get(table) else {
            return Ok(None);
        };
        // Latest (highest id) snapshot with time <= ts at which the table is live.
        Ok(cat
            .snapshots
            .iter()
            .rev()
            .find(|sn| sn.time <= ts && t.live_at(sn.id.0))
            .cloned())
    }
```

- [ ] **Step 4: Implement it for the pg adapter**

In `src/control-plane/postgres/src/iceberg_catalog.rs`, add to `impl Catalog for IcebergCatalog` (after `current_snapshot`, ~line 181) — `current_snapshot`'s query plus a `snapshot_time <= $3` bound, returning `Option` instead of erroring:

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn snapshot_as_of(
        &self,
        table: &TableRef,
        ts: OffsetDateTime,
    ) -> Result<Option<Snapshot>> {
        let row = sqlx::query!(
            "select sn.snapshot_id as \"snapshot_id!\", sn.snapshot_time as \"snapshot_time!\", sn.schema_version as \"schema_version!\" \
             from iceberg_mirror.snapshot sn \
             where sn.snapshot_time <= $3 and exists ( \
                 select 1 from iceberg_mirror.table t \
                 where t.table_namespace = $1 and t.table_name = $2 \
                   and t.begin_snapshot <= sn.snapshot_id and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id)) \
             order by sn.snapshot_id desc limit 1",
            table.schema,
            table.name,
            ts,
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
```

Ensure `OffsetDateTime` is imported in this file (add `use time::OffsetDateTime;` if the compiler flags it).

- [ ] **Step 5: Regenerate the sqlx cache**

Run: `./tools/sqlx-prepare.sh`
Expected: a new `src/control-plane/postgres/.sqlx/query-*.json` for the `snapshot_as_of` query; `git status` shows the added `.sqlx` file.

- [ ] **Step 6: Run both contract targets + the cache check (green)**

Run: `buck2 test //src/control-plane/postgres/... //src/control-plane/memory/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all pass — `memory_passes_catalog_contract`, `iceberg_passes_catalog_contract`, and `sqlx-cache-check`.

- [ ] **Step 7: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/core/src/catalog.rs src/control-plane/memory/src/catalog.rs src/control-plane/postgres/src/iceberg_catalog.rs src/control-plane/testkit/src/lib.rs src/control-plane/postgres/.sqlx
git commit -m "feat(control-plane): Catalog::snapshot_as_of across adapters + contract"   # + trailer
```

---

### Task 2: engine-serving — read the provider at an arbitrary snapshot

**Files:**
- Modify: `src/services/engine-serving/src/serving.rs` (`build_serving_provider`, `register_iceberg_table`, `execute_query_stream`)
- Test: `src/services/engine-serving/tests/serving_as_of.rs` (new fixture test) OR extend an existing serving fixture test — prefer a new file.
- Modify: `src/services/engine-serving/BUCK`

**Interfaces:**
- Consumes: `Catalog::schema` / `files_with_stats` (existing).
- Produces:
  - `build_serving_provider(ctx, catalog, table, serving_store, at: Option<SnapshotId>) -> Result<Option<Arc<dyn TableProvider>>, EngineServingError>` — `None` ⇒ current snapshot (unchanged); `Some(id)` ⇒ read at `id`, and return `Ok(None)` if the table is not live at `id`.
  - `register_iceberg_table(ctx, catalog, table, serving_store, at: Option<SnapshotId>)`.
  - `execute_query_stream(catalog, sql, serving_store, at: Option<SnapshotId>)`.

- [ ] **Step 1: Thread `at` into `build_serving_provider`**

In `src/services/engine-serving/src/serving.rs`, change the signature (append `at`) and replace the snapshot-resolution block (currently line 94, `let snap = catalog.current_snapshot(table)...`). Replace lines 94–101 (the `snap` + `table_schema` resolution) with a resolved `snap_id`:

```rust
    let snap_id = match at {
        None => catalog.current_snapshot(table).await.map_err(to_serving)?.id,
        Some(id) => id,
    };
    // The MIRROR is authoritative for the served schema at `snap_id`. For an
    // as-of read (`at = Some`), a table not live at `snap_id` (created later) is
    // simply skipped: `schema` NotFound -> Ok(None). For the current path (`at =
    // None`), `snap_id` came from `current_snapshot`, so this never NotFounds.
    let table_schema = match catalog.schema(table, snap_id).await {
        Ok(s) => s,
        Err(control_plane_core::ControlPlaneError::NotFound(_)) if at.is_some() => {
            return Ok(None);
        }
        Err(e) => return Err(to_serving(e)),
    };
```

Then, in the rest of the function, replace every remaining `snap.id` with `snap_id` (the `files_with_stats(table, snap.id)` call at ~line 104 and the `build_inline_provider(..., snap.id, ...)` call at ~line 132). There is no other use of `snap`.

Update the signature line:

```rust
pub async fn build_serving_provider(
    ctx: &SessionContext,
    catalog: &IcebergCatalog,
    table: &TableRef,
    serving_store: Option<&ServingStore>,
    at: Option<SnapshotId>,
) -> Result<Option<Arc<dyn TableProvider>>, EngineServingError> {
```

Add `SnapshotId` to the `control_plane_core` import at the top of the file if not already imported.

- [ ] **Step 2: Thread `at` through `register_iceberg_table` and `execute_query_stream`**

`register_iceberg_table` (~line 273): append `at: Option<SnapshotId>` and forward it:

```rust
    let Some(provider) = build_serving_provider(ctx, catalog, table, serving_store, at).await?
    else {
        return Ok(());
    };
```

`execute_query_stream` (~line 617): append `at: Option<SnapshotId>` and forward it in the loop:

```rust
    for table in catalog.live_tables().await.map_err(to_serving)? {
        register_iceberg_table(&ctx, catalog, &table, serving_store, at).await?;
    }
```

- [ ] **Step 3: Fix the two existing callers to pass `None`**

- `src/services/engine-serving/src/governed.rs:297`: `build_serving_provider(&ctx, catalog, &table, serving_store)` → append `, None`.
- `src/services/engine/src/flight.rs` `do_get_sql` (~line 81): `execute_query_stream(&self.serving_catalog, &sql, self.serving_store.as_ref())` → append `, None`.

Also fix any other callers surfaced by: `grep -rn "build_serving_provider\|register_iceberg_table\|execute_query_stream" src/ --include=*.rs` (tests included) — pass `None` at each existing call site.

- [ ] **Step 4: Write the failing fixture test**

Create `src/services/engine-serving/tests/serving_as_of.rs`. Model its fixture boot + landing on an existing engine-serving fixture test (open the sibling test dir, e.g. `grep -rl "build_serving_provider\|IcebergCatalog::" src/services/engine-serving/tests`, and copy its setup harness verbatim — the fixture spin-up, `IcebergCatalog` construction, and the land-a-batch helper). The new behavior to assert:

```rust
// After landing the SAME table twice (snapshot S1 then S2, each adding rows):
//   * build_serving_provider(.., Some(S1)) sees only the S1 row count
//   * build_serving_provider(.., None)     sees the S2 (cumulative) row count
// Read the provider by scanning it through the SessionContext and counting rows.
```

Concretely (adapt names to the copied harness — `land(&catalog, &table, rows)` returns the new `SnapshotId`; `count_rows(&ctx, provider)` collects `provider.scan` into a row count):

```rust
    let s1 = land(&catalog, &table, 3).await;      // 3 rows at snapshot S1
    let _s2 = land(&catalog, &table, 5).await;     // +5 rows at snapshot S2 (8 total)

    let ctx = SessionContext::new();
    let at_s1 = build_serving_provider(&ctx, &catalog, &table, store.as_ref(), Some(s1))
        .await
        .unwrap()
        .expect("provider at S1");
    assert_eq!(count_rows(&ctx, at_s1).await, 3, "as-of S1 sees only the first write");

    let live = build_serving_provider(&ctx, &catalog, &table, store.as_ref(), None)
        .await
        .unwrap()
        .expect("live provider");
    assert_eq!(count_rows(&ctx, live).await, 8, "live sees both writes");
```

Wire `serving_as_of` as a `loom_fixture_test` in `src/services/engine-serving/BUCK` (copy a sibling fixture test target block; it must use `loom_fixture_test`, not `rust_test`).

- [ ] **Step 5: Run**

Run: `buck2 test //src/services/engine-serving:serving_as_of > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.`

- [ ] **Step 6: Build the dependents to catch call-site breaks, then prek + commit**

Run: `buck2 build //src/services/engine-serving/... //src/services/engine/... > /tmp/b.log 2>&1; grep -iE "error|BUILD FAILED|BUILD SUCCEEDED" /tmp/b.log`

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/engine-serving/ src/services/engine/src/flight.rs
git commit -m "feat(engine-serving): read serving provider at an arbitrary snapshot"   # + trailer
```

---

### Task 3: engine as-of read plane — wire ticket + client + handler

**Files:**
- Modify: `src/services/engine-wire/src/flight.rs` (`AsOfStatementQuery`, `EngineTicket::AsOfSql`, decode, client send)
- Modify: `src/services/engine/src/flight.rs` (dispatch arm + `do_get_as_of_sql`)
- Test: `src/services/engine/tests/as_of_wire.rs` (new fixture test)
- Modify: `src/services/engine/BUCK`, `src/services/engine-wire/BUCK` (if a new unit test is added there)

**Interfaces:**
- Produces:
  - `engine_wire::flight::AsOfStatementQuery { sql: String, as_of_snapshot: i64 }` with `encode`/`decode`.
  - `EngineTicket::AsOfSql(AsOfStatementQuery)` variant.
  - `FlightSqlClient::execute_as_of(&self, sql: String, as_of_snapshot: i64) -> Result<Vec<RecordBatch>>` — single-hop `do_get` of the new ticket.

- [ ] **Step 1: Add the ticket type**

In `src/services/engine-wire/src/flight.rs`, after `GovernedStatementQuery` (~line 96), add:

```rust
/// A loom-native as-of read ticket: run `sql` with every referenced table read at
/// snapshot `as_of_snapshot` instead of its current snapshot. Bypasses the standard
/// `CommandStatementQuery` (loom's external Flight SQL interop surface) so that
/// surface stays a bare query string. `deny_unknown_fields` keeps it disjoint from
/// the other JSON ticket shapes for the `EngineTicket` decode.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AsOfStatementQuery {
    pub sql: String,
    pub as_of_snapshot: i64,
}

impl AsOfStatementQuery {
    #[must_use]
    #[expect(
        clippy::expect_used,
        reason = "serde_json of an owned serializable type is infallible; matches GovernedStatementQuery::encode"
    )]
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("AsOfStatementQuery is always serializable")
    }

    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}
```

- [ ] **Step 2: Add the `EngineTicket` variant + decode arm**

In the `EngineTicket` enum (~line 130), add a variant:

```rust
    /// Loom-native as-of SQL plane: client SQL + a resolved snapshot id.
    AsOfSql(AsOfStatementQuery),
```

In `EngineTicket::decode` (the ordered fall-through — find it just below the enum), add an arm for `AsOfStatementQuery` **before** the file-ticket terminal and disjoint from `GovernedSql`. Both are `deny_unknown_fields` JSON: `GovernedStatementQuery` requires field `catalog`, `AsOfStatementQuery` requires `as_of_snapshot`, so neither parses the other. Mirror the existing `GovernedSql`/`VectorSearch` decode arms exactly (same `try` order pattern):

```rust
        if let Ok(q) = AsOfStatementQuery::decode(bytes) {
            return Ok(EngineTicket::AsOfSql(q));
        }
```

Place it adjacent to the `GovernedSql` decode attempt. Preserve the existing decode ORDER comment; extend it to mention the new shape.

- [ ] **Step 3: Add the client send method**

In `impl FlightSqlClient` (`src/services/engine-wire/src/flight.rs` ~line 306), add — modeled on `FlightTableClient::vector_search`'s single-hop `do_get`:

```rust
    /// Execute `sql` with every referenced table read at `as_of_snapshot`, buffering
    /// the streamed result. Single-hop `do_get` of an `AsOfStatementQuery` ticket
    /// (the standard `CommandStatementQuery` cannot carry the snapshot id).
    pub async fn execute_as_of(
        &self,
        sql: String,
        as_of_snapshot: i64,
    ) -> Result<Vec<RecordBatch>> {
        let ticket = AsOfStatementQuery { sql, as_of_snapshot };
        let resp = self
            .inner
            .clone()
            .do_get(Ticket { ticket: ticket.encode().into() })
            .await
            .map_err(crate::client::sql_status)?;
        decode_batches(resp).map_err(crate::client::be).try_collect().await
    }
```

Confirm `Ticket` and `try_collect` are already imported in this file (they are used by `FlightTableClient`/`execute`). Add imports if the compiler flags them.

- [ ] **Step 4: Add the engine dispatch arm + handler**

In `src/services/engine/src/flight.rs`:

Add the handler next to `do_get_sql` (~line 91):

```rust
    /// Run a loom-native as-of SQL statement (client SQL read at a resolved snapshot)
    /// through the plain serving path pinned to that snapshot. Mirrors `do_get_sql`'s
    /// error mapping (planning -> `invalid_argument`, execution -> `internal`).
    async fn do_get_as_of_sql(
        &self,
        q: engine_wire::flight::AsOfStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let stream = engine_serving::execute_query_stream(
            &self.serving_catalog,
            &q.sql,
            self.serving_store.as_ref(),
            Some(control_plane_core::SnapshotId(q.as_of_snapshot)),
        )
        .await
        .map_err(serving_status)?;
        Ok(Self::encode_response(stream.map_err(|e| {
            FlightError::from_external_error(Box::new(e))
        })))
    }
```

In the `do_get` dispatch `match` over `EngineTicket` (find it in the `FlightService::do_get` impl below, where `EngineTicket::GovernedSql(q) => self.do_get_governed_sql(q).await` etc. are matched), add:

```rust
            EngineTicket::AsOfSql(q) => self.do_get_as_of_sql(q).await,
```

Confirm `control_plane_core::SnapshotId` is importable here (add a `use` if needed).

- [ ] **Step 5: Write the failing wire test**

Create `src/services/engine/tests/as_of_wire.rs`. Copy the fixture + engine-boot + client-connect harness from an existing engine wire test (`src/services/engine/tests/wire.rs` is the template — reuse its server spin-up and `FlightSqlClient::connect`). Assert:

```rust
// Land table twice (S1 then S2). Over the wire:
//   * client.execute_as_of("SELECT count(*) ... ", s1) -> first-write count
//   * client.execute("SELECT count(*) ...")            -> cumulative count
```

Use the same land helper the `wire.rs` test uses; extract the scalar count from the returned `RecordBatch` (mirror how `wire.rs` reads a result batch). Wire `as_of_wire` as a `loom_fixture_test` in `src/services/engine/BUCK` (copy the `wire` target block, rename).

- [ ] **Step 6: Run**

Run: `buck2 test //src/services/engine:as_of_wire > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.`

- [ ] **Step 7: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/engine-wire/src/flight.rs src/services/engine/src/flight.rs src/services/engine/tests/as_of_wire.rs src/services/engine/BUCK
git commit -m "feat(engine): as-of SQL read plane (AsOfStatementQuery ticket + handler)"   # + trailer
```

---

### Task 4: query-api serving seam — `fetch_rows(at)` + catalog in deps

**Files:**
- Modify: `src/services/query-api/src/serving.rs` (`ServingEngine::fetch_rows` signature)
- Modify: `src/services/query-api/src/engine_client.rs` (`EngineServingClient::fetch_rows` impl)
- Modify: `src/services/query-api/src/handler.rs` (`QueryDeps.catalog`; all `fetch_rows` call sites)
- Modify: `src/services/query-api/src/http.rs` (`st.deps()` builds `catalog`)
- Modify: any test double implementing `ServingEngine` (search `impl ServingEngine`/`impl.*ServingEngine for`)

**Interfaces:**
- Consumes: `FlightSqlClient::execute_as_of` (Task 3).
- Produces:
  - `ServingEngine::fetch_rows(&self, sql: &str, params: &[SqlValue], at: Option<SnapshotId>) -> Result<Rows, ServingError>`.
  - `QueryDeps.catalog: &'a (dyn Catalog + Send + Sync)`.

- [ ] **Step 1: Change the trait method signature**

In `src/services/query-api/src/serving.rs` (~line 287):

```rust
    async fn fetch_rows(
        &self,
        sql: &str,
        params: &[SqlValue],
        at: Option<control_plane_core::SnapshotId>,
    ) -> Result<Rows, ServingError>;
```

- [ ] **Step 2: Implement the new arm in `EngineServingClient`**

In `src/services/query-api/src/engine_client.rs`, replace `fetch_rows` (line 34) so `at` selects the wire method:

```rust
    async fn fetch_rows(
        &self,
        sql: &str,
        params: &[SqlValue],
        at: Option<control_plane_core::SnapshotId>,
    ) -> Result<Rows, ServingError> {
        let inlined = inline_params(sql, params);
        let map_err = |e| match e {
            control_plane_core::ControlPlaneError::Validation(m) => ServingError::Plan(m),
            other => ServingError::Engine(other.to_string()),
        };
        let batches = match at {
            None => self.sql.execute(inlined).await.map_err(map_err)?,
            Some(id) => self.sql.execute_as_of(inlined, id.0).await.map_err(map_err)?,
        };
        Ok(batches_to_rows(batches))
    }
```

- [ ] **Step 3: Add `catalog` to `QueryDeps` and update its constructor(s)**

In `src/services/query-api/src/handler.rs` (~line 99):

```rust
pub struct QueryDeps<'a> {
    pub ontology: &'a (dyn Ontology + Send + Sync),
    pub acl: &'a (dyn Acl + Send + Sync),
    pub serving: &'a dyn ServingEngine,
    pub catalog: &'a (dyn control_plane_core::Catalog + Send + Sync),
    pub default_limit: u32,
}
```

In `src/services/query-api/src/http.rs`, find `st.deps()` (the `AppState` method building `QueryDeps`) and add `catalog: st.cp.catalog()`. (`st.cp.catalog()` returns `&(dyn Catalog + Send + Sync)` — the same handle used by `get_dataset`.)

- [ ] **Step 4: Pass `None` at every existing `fetch_rows` call site**

Update each call in `src/services/query-api/src/handler.rs` (lines ~396, 510, 665, 912, 980, 1060, 1100) — append `, None` for now (Task 5 changes the two object-read sites to pass the resolved `at`). Search to be exhaustive: `grep -rn "fetch_rows(" src/services/query-api/src`.

- [ ] **Step 5: Update `ServingEngine` test doubles**

Find every test/stub impl: `grep -rn "impl ServingEngine\|ServingEngine for\|fn fetch_rows" src/services/query-api/tests src/services/query-api/src`. For each stub `fetch_rows`, add the `at: Option<control_plane_core::SnapshotId>` parameter (the stubs can ignore it, or — for tests added in Task 5/6 — record it). This includes `tests/e2e_support.rs` if it defines a serving stub, and any `StubAction`/serving double.

- [ ] **Step 6: Build query-api (no behavior change yet) and run its existing tests**

Run: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: existing suite still green (this task is a pure signature extension threading `None`).

- [ ] **Step 7: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/query-api/
git commit -m "feat(query-api): thread as-of snapshot through the serving seam"   # + trailer
```

---

### Task 5: query-api object read — parse, resolve, apply `?as_of`

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (`AsOfSelector`, `ObjectQuery.as_of`, resolution helper, `read_object`/`read_object_page`)
- Modify: `src/services/query-api/src/http.rs` (`get_object` parses `as_of`/`as_of_snapshot`)
- Test: `src/services/query-api/tests/as_of_objects_e2e.rs` (new)
- Modify: `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `QueryDeps.catalog`, `fetch_rows(.., at)` (Task 4), `Catalog::snapshot_as_of`/`schema` (Task 1).
- Produces: `AsOfSelector { Snapshot(i64), Time(OffsetDateTime) }`; `ObjectQuery.as_of: Option<AsOfSelector>`; `resolve_read_snapshot(...) -> Result<Option<SnapshotId>, QueryError>`.

- [ ] **Step 1: Define `AsOfSelector` and extend `ObjectQuery`**

In `src/services/query-api/src/handler.rs`, near `ObjectQuery` (~line 83):

```rust
/// A parsed-but-unresolved time-travel selector from `?as_of_snapshot=` / `?as_of=`.
/// Resolved to a concrete `SnapshotId` (against the target table) inside the read path.
#[derive(Debug, Clone, PartialEq)]
pub enum AsOfSelector {
    /// An exact mirror snapshot id (`?as_of_snapshot=`).
    Snapshot(i64),
    /// A wall-clock instant (`?as_of=`, RFC3339) -> the latest snapshot at/before it.
    Time(time::OffsetDateTime),
}
```

Add to `ObjectQuery`:

```rust
    /// Optional time-travel selector; `None` = read the live snapshot.
    pub as_of: Option<AsOfSelector>,
```

- [ ] **Step 2: Add the resolution helper**

In `src/services/query-api/src/handler.rs`, add (near `read_object`):

```rust
/// Resolve a time-travel selector to a concrete snapshot id for `table`, or `None`
/// when no selector was given (live read). `NotFound`-class faults render as 404:
/// an as-of snapshot id the table is not live at, or a timestamp before the table's
/// first snapshot.
async fn resolve_read_snapshot(
    deps: &QueryDeps<'_>,
    table: &control_plane_core::TableRef,
    sel: Option<&AsOfSelector>,
) -> Result<Option<control_plane_core::SnapshotId>, QueryError> {
    let Some(sel) = sel else { return Ok(None) };
    let id = match sel {
        AsOfSelector::Snapshot(id) => {
            let sid = control_plane_core::SnapshotId(*id);
            // Liveness gate: `schema` NotFounds if the table is not live at `sid`.
            deps.catalog.schema(table, sid).await?;
            sid
        }
        AsOfSelector::Time(ts) => deps
            .catalog
            .snapshot_as_of(table, *ts)
            .await?
            .ok_or_else(|| {
                QueryError::ControlPlane(control_plane_core::ControlPlaneError::NotFound(format!(
                    "{}.{} has no snapshot at or before {ts}",
                    table.schema, table.name
                )))
            })?
            .id,
    };
    Ok(Some(id))
}
```

Verify `query_error_response` maps `QueryError::ControlPlane(ControlPlaneError::NotFound(_))` → 404 (it must, since `resolve_governed`'s unknown-type path already relies on that). If it does not, add that arm to the mapping in `http.rs`.

- [ ] **Step 3: Wire resolution into both read functions**

`read_object` (~line 380): resolve governance first so the table is known, then resolve `at`, then pass it. Rewrite the body:

```rust
pub async fn read_object(
    q: &ObjectQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let g = resolve_governed(deps.ontology, deps.acl, &subject.0, &type_name, OnMissing::NotFound)
        .await?;
    let at = resolve_read_snapshot(deps, &g.otype.table, q.as_of.as_ref()).await?;
    let gr = compile_object_read_with(
        &g, q, subject, deps.ontology, deps.acl, deps.serving.dialect(),
        deps.default_limit, None, None,
    )
    .await?;
    let served = deps.serving.fetch_rows(&gr.sql, &gr.params, at).await?;
    Ok(gr.into_object_rows(served))
}
```

`read_object_page` (~line 420): `g` is already resolved (line 434). After it, add `let at = resolve_read_snapshot(deps, &g.otype.table, q.as_of.as_ref()).await?;` and change the `fetch_rows` call (line 510) to `deps.serving.fetch_rows(&gr.sql, &gr.params, at).await?`.

- [ ] **Step 4: Parse the selector in `get_object`**

In `src/services/query-api/src/http.rs` `get_object` (~line 415): add `as_of` and `as_of_snapshot` to the reserved keys and parse them. Replace the `split_reserved` call (line 425) reserved list with `&["_ids", "_or", "limit", "cursor", "as_of", "as_of_snapshot"]`, then after the `_or`/`limit`/`cursor` extraction add:

```rust
    let as_of = match parse_as_of(reserved.last("as_of"), reserved.last("as_of_snapshot")) {
        Ok(sel) => sel,
        Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
    };
```

Set `as_of` on **both** `ObjectQuery { .. }` literals in this handler (the paginated and un-paginated construction sites).

Add the parser (free function in `http.rs`):

```rust
/// Parse the mutually-exclusive `?as_of=` (RFC3339) / `?as_of_snapshot=` (i64) selectors.
/// Both present, a non-integer id, or an unparseable timestamp -> `Err(message)` (400).
fn parse_as_of(
    as_of: Option<&str>,
    as_of_snapshot: Option<&str>,
) -> Result<Option<crate::handler::AsOfSelector>, String> {
    match (as_of, as_of_snapshot) {
        (Some(_), Some(_)) => Err("as_of and as_of_snapshot are mutually exclusive".into()),
        (None, None) => Ok(None),
        (None, Some(id)) => id
            .parse::<i64>()
            .map(|n| Some(crate::handler::AsOfSelector::Snapshot(n)))
            .map_err(|_| "as_of_snapshot must be an integer snapshot id".into()),
        (Some(ts), None) => time::OffsetDateTime::parse(
            ts,
            &time::format_description::well_known::Rfc3339,
        )
        .map(|t| Some(crate::handler::AsOfSelector::Time(t)))
        .map_err(|_| "as_of must be an RFC3339 timestamp".into()),
    }
}
```

Ensure any other `ObjectQuery { .. }` construction in the codebase gets `as_of: None` (search `ObjectQuery {` — e.g. link/graph handlers that build one internally). Add `as_of: None` to each so it compiles.

- [ ] **Step 5: Write the failing e2e test**

Create `src/services/query-api/tests/as_of_objects_e2e.rs`, using the `e2e-support` library (`use e2e_support::{...}`) — reuse `tref`/`land`/`prop`, the router driver `get`, `subject_with_role`, `grant_read`, and `ids`. The test must land a type's table **twice** with different row sets so the two snapshots differ, capturing the first snapshot id.

Assert (via the HTTP router `get`):
- `GET /objects/{type}?as_of_snapshot={S1}` returns only the first-write ids.
- `GET /objects/{type}` (no selector) returns the full/live id set.
- `GET /objects/{type}?as_of={rfc3339-of-S1}` returns the first-write ids. (Read S1's time from the dataset detail or catalog seeded in the fixture.)
- `GET /objects/{type}?as_of=x&as_of_snapshot=1` → 400.
- `GET /objects/{type}?as_of_snapshot=999999` (id the table isn't live at) → 404.
- `GET /objects/{type}?as_of=not-a-date` → 400.

Because the `e2e-support` seed helpers land through the control plane, capture the snapshot id returned by the landing helper (or read `catalog.snapshots(table)` in the test) to build `{S1}` and its RFC3339 time. If the shared seed fixture only lands once, add a second `land(..)` for the same table in this test to create S2. Wire `as_of_objects_e2e` in `src/services/query-api/BUCK` (copy an existing e2e target block; add `":e2e-support"` to `deps`; use `loom_fixture_test` if the e2e targets are fixture-backed — match the siblings).

- [ ] **Step 6: Run**

Run: `buck2 test //src/services/query-api:as_of_objects_e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.`

- [ ] **Step 7: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/query-api/
git commit -m "feat(query-api): as-of selector on GET /objects/{type}"   # + trailer
```

---

### Task 6: query-api dataset detail — `?as_of` on `GET /datasets/{schema}/{table}`

**Files:**
- Modify: `src/services/query-api/src/http.rs` (`get_dataset`)
- Test: extend `src/services/query-api/tests/as_of_objects_e2e.rs` or a small new `tests/as_of_dataset_e2e.rs`
- Modify: `src/services/query-api/BUCK` (if a new test file)

**Interfaces:**
- Consumes: `parse_as_of` (Task 5), `Catalog::snapshot_as_of`/`schema`.

- [ ] **Step 1: Accept + resolve the selector in `get_dataset`**

In `src/services/query-api/src/http.rs`, change `get_dataset` (~line 269) to take the query params and resolve the snapshot. Add `Query(params): Query<Vec<(String, String)>>` to its args, then:

```rust
    let catalog = st.cp.catalog();
    let table_ref = TableRef { schema, name: table };
    let reserved = crate::query_params::split_reserved(params, &["as_of", "as_of_snapshot"]).0;
    let sel = match parse_as_of(reserved.last("as_of"), reserved.last("as_of_snapshot")) {
        Ok(s) => s,
        Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
    };
    let snapshot = match resolve_dataset_snapshot(catalog, &table_ref, sel.as_ref()).await {
        Ok(s) => s,
        Err(e) => return cp_read_error("catalog snapshot resolution fault", e),
    };
```

Then keep the existing `catalog.schema(&table_ref, snapshot.id)` + rendering (it already uses `snapshot.id`/`snapshot.time`, so as-of "just works" once `snapshot` is the resolved one).

Add the resolver (returns the concrete `Snapshot`, defaulting to `current_snapshot` when no selector):

```rust
/// Resolve dataset-detail's target snapshot: the selector's snapshot when given,
/// else the current one. `NotFound` (bad id / pre-history timestamp / unknown table)
/// propagates to the 404 mapping.
async fn resolve_dataset_snapshot(
    catalog: &(dyn control_plane_core::Catalog + Send + Sync),
    table: &TableRef,
    sel: Option<&crate::handler::AsOfSelector>,
) -> Result<control_plane_core::Snapshot, ControlPlaneError> {
    use crate::handler::AsOfSelector;
    match sel {
        None => catalog.current_snapshot(table).await,
        Some(AsOfSelector::Snapshot(id)) => {
            let sid = control_plane_core::SnapshotId(*id);
            // Liveness + fetch: `snapshots` lists the table's live snapshots; pick `sid`.
            catalog
                .snapshots(table, PageReq::unbounded())
                .await?
                .items
                .into_iter()
                .find(|s| s.id == sid)
                .ok_or_else(|| {
                    ControlPlaneError::NotFound(format!(
                        "{}.{} not live at snapshot {}",
                        table.schema, table.name, id
                    ))
                })
        }
        Some(AsOfSelector::Time(ts)) => catalog.snapshot_as_of(table, *ts).await?.ok_or_else(|| {
            ControlPlaneError::NotFound(format!(
                "{}.{} has no snapshot at or before {ts}",
                table.schema, table.name
            ))
        }),
    }
}
```

(Using `snapshots(..).find(id)` for the explicit-id case returns the full `Snapshot` — id + time + schema_version — which the response needs, unlike `schema()` which returns only columns.)

Update the `get_dataset` `#[utoipa::path]` `params(...)` to document `as_of` and `as_of_snapshot`, and add a `400` response row.

- [ ] **Step 2: Write the failing dataset e2e assertions**

Add to the e2e test: after landing the table twice,
- `GET /datasets/{schema}/{table}?as_of_snapshot={S1}` → `snapshot_id == S1` and the columns as of S1.
- `GET /datasets/{schema}/{table}` (no selector) → the current snapshot id (S2).
- `GET /datasets/{schema}/{table}?as_of_snapshot=999999` → 404.
- `GET /datasets/{schema}/{table}?as_of=bad` → 400.

- [ ] **Step 3: Run**

Run: `buck2 test //src/services/query-api:as_of_objects_e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: pass (including the new dataset assertions).

- [ ] **Step 4: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/query-api/
git commit -m "feat(query-api): as-of selector on GET /datasets/{schema}/{table}"   # + trailer
```

---

### Task 7: docs — OpenAPI, system-capabilities, registers

**Files:**
- Modify: `src/services/query-api/src/openapi.rs` (param docs on the two operations — if not already covered by the `#[utoipa::path]` edits)
- Modify: `docs/system-capabilities/` (query-api read path)
- Modify: `docs/ROADMAP.md`, `docs/FUTURE.md` (register the item + the deferred follow-up)

**Interfaces:** none (docs only).

- [ ] **Step 1: OpenAPI params**

Confirm both `as_of` and `as_of_snapshot` appear on `GET /objects/{type_name}` and `GET /datasets/{schema}/{table}` in the generated OpenAPI. They are declared via the `#[utoipa::path(params(...))]` blocks (Tasks 5–6). Add the `as_of`/`as_of_snapshot` `("as_of" = Option<String>, Query, description = "...")` entries to `get_object`'s `params(...)` (Task 5 added the reserved parsing but not necessarily the doc entries) and regenerate/verify via the openapi test if one exists (`grep -rn "openapi" src/services/query-api/tests`).

- [ ] **Step 2: system-capabilities doc**

In the query-api capability doc under `docs/system-capabilities/` (find it: `ls docs/system-capabilities/`), add a short "Time-travel reads" subsection: the two endpoints, the two mutually-exclusive selectors, that resolution is query-api-side to a concrete snapshot id, and the **retention caveat** (a selector resolving to a GC'd snapshot under-reads; not enforced).

- [ ] **Step 3: Registers via loom-docs-update**

Invoke the `loom-docs-update` skill to:
- add a **closed/landed** capability note for this slice, or add `road-timetravel-reads` and mark it done per the register grammar,
- add `fut-timetravel-retention-guard` to `docs/FUTURE.md` (`{#fut-timetravel-retention-guard area:<engine|ingest> status:deferred from:this-slice pr:- spec:2026-07-06-timetravel-reads-design}`) — "reject as-of reads that resolve outside the GC retention window with a clear 410/404 instead of under-reading",
- note that `fut-iceberg-time-travel-schema` remains the schema-as-of follow-up.

Run `bash tools/docs.sh validate` and fix any grammar/link errors.

- [ ] **Step 4: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/query-api/src/openapi.rs docs/
git commit -m "docs(query-api): document time-travel reads + register follow-ups"   # + trailer
```

---

### Task 8: full-suite verification

- [ ] **Step 1: Build the affected first-party tree**

Run: `buck2 build //src/... > /tmp/b.log 2>&1; grep -iE "BUILD (FAILED|SUCCEEDED)|error\[" /tmp/b.log`
Expected: `BUILD SUCCEEDED`.

- [ ] **Step 2: Run the full test suite**

Run (locally use `-j 8` to avoid PG boot-slot starvation; on a root cloud host RE handles fixtures): `buck2 test //src/... -j 8 > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all pass, no FAIL. Investigate any fixture flake per CLAUDE.md (boot-slot timeouts are non-deterministic; re-run the specific target).

- [ ] **Step 3: Final prek**

Run: `buck2 run //tools:prek -- run --all-files`
Expected: all hooks pass.

- [ ] **Step 4: Finish the branch**

Use the `finishing-a-development-branch` skill. Per the user's standing preference, this means push + open a PR (never a local merge, and don't clean up the worktree).

---

## Notes for the implementer

- **The no-selector path must stay byte-identical.** Every function that gains `at`/`as_of` defaults to the current behavior when it is `None`. If any existing test changes output, something is wrong.
- **404 vs 400 discipline:** malformed selector (both present, non-integer id, bad RFC3339) = 400; well-formed selector that resolves to nothing (unknown/pre-history/not-live snapshot) = 404. The tests pin both.
- **Retention caveat is documented, not enforced** — do not add a horizon check in this slice (that is `fut-timetravel-retention-guard`).
- **Schema-as-of** is not a goal — the serving path already reads the mirror schema at the resolved snapshot, which equals the current schema until schema evolution ships. Do not wire `catalog.schema(at)` differently.
