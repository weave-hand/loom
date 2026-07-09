# External SQL Wire (Slice 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the external Arrow Flight SQL wire — a TCP `FlightServiceServer` in query-api that authenticates bearer tokens (sessions AND service tokens), resolves the subject's ACL into a per-request `GovernedCatalog`, and forwards arbitrary client SQL to the engine's governed-SQL plane (slice 1's `execute_governed_sql_stream`), flipped to closed-world registration.

**Architecture:** query-api is the edge (auth + policy resolution, zero DataFusion); the engine executes blindly. Policy travels in the existing `GovernedStatementQuery { sql, catalog }` ticket over the internal UDS Flight wire. The external listener mirrors the governed Flight export's posture (per-verb auth, per-call governance re-derivation, stream relay + row cap, scrubbed errors) but carries standard Flight SQL messages, never loom-native JSON tickets.

**Tech Stack:** Rust, tonic/arrow-flight, DataFusion (engine-side only), `loom_config` typed config, buck2 `rust_test`/`loom_fixture_test`.

**Spec:** `docs/superpowers/specs/2026-07-09-external-sql-wire-design.md`.

## Global Constraints

Carried from the spec + CLAUDE.md; every task implicitly includes these:

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New fixture tests MUST use `loom_fixture_test` (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`, or the fixture env (PG binaries, MinIO, boot-slot dir) is missing. The `no-inline-tests` prek hook fails the build on any inline `#[test]`.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/`indexing_slicing`/`panic`/`todo`/`map_err_ignore` in production lib/bin code. Use `#[expect(lint, reason = "...")]` for a justified local exception. Test code is exempted from the panic-safety lints via `loom_rust_test`/`loom_fixture_test`.
- **No new third-party deps are expected** (tonic/arrow-flight/prost are already in-tree for both crates). If one becomes necessary: Cargo.toml → `cargo generate-lockfile` → `./tools/buckify.sh` → diff the lock against merge-base for native/`links` crate movement (reindeer-update footgun).
- **No compile-time SQL changes** — nothing here touches `query!` macros, so no `tools/sqlx-prepare.sh` run is needed.
- **Security invariants (re-check at every task):** the external `do_get` decodes ONLY Any-packed `TicketStatementQuery` — a loom-native JSON ticket (especially `GovernedStatementQuery`) must be rejected, never executed; catalog resolution fails closed (an ACL/ontology error aborts the request — never an empty-policy fallback entry); error messages to external clients carry no internal detail beyond DataFusion plan errors over the client's own SQL.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files` (stage new files with `git add` first — prek skips untracked files). Markdown files end with exactly one trailing newline and no trailing whitespace.
- **Build/test commands:** build `buck2 build -v0 --console none //src/...`; test `buck2 test --console none <targets>` (cloud: `-M none` on builds, scope tests, `buck2 clean` between heavy phases; a full local suite needs `-j 8` for the 8 PG boot-slots).

---

## File Structure

**Create:**
- `src/services/query-api/src/flight_auth.rs` — shared gRPC-metadata bearer authentication (session + service token).
- `src/services/query-api/src/flight_sql.rs` — `FlightSqlWireService`, the external TCP Flight SQL listener.
- `src/services/query-api/tests/governed_catalog_resolve.rs` — `resolve_governed_catalog` unit test (memory control plane; pure `rust_test`).
- `src/services/query-api/tests/sql_wire_config.rs` — `SqlWireTuning` config-seam unit test (pure `rust_test`).
- `src/services/query-api/tests/external_sql_wire_e2e.rs` — the acceptance e2e (`loom_fixture_test`).

**Modify (production):**
- `src/services/engine-serving/src/governed.rs` — closed-world registration in `execute_governed_sql_stream` (line 289).
- `src/control-plane/core/src/governed.rs` — rewrite the `GovernedCatalog` absent-entry doc contract (lines 24-27).
- `src/services/engine-wire/src/flight.rs` — `FlightSqlClient::execute_governed_stream`.
- `src/services/runtime/src/auth.rs` — make `resolve_bearer` (line 82) `pub`; re-export from the runtime crate root.
- `src/services/query-api/src/flight_export.rs` — `authenticate` (line 161) moves to `flight_auth.rs` and gains the service-token fallback.
- `src/services/query-api/src/governed.rs` — `resolve_governed_catalog` next to `load_policy` (line 103).
- `src/services/query-api/src/config.rs` — `SqlWireTuning` domain on `QueryApiConfig`.
- `src/services/query-api/src/serve.rs` — spawn the SQL wire from the typed config.
- `src/services/query-api/src/lib.rs` — `pub mod flight_sql;` + `pub(crate) mod flight_auth;`.

**Modify (tests):**
- `src/services/engine-serving/tests/governed_sql.rs` — rewrite `empty_policy_is_full_visibility` (line 275) for the closed-world contract.
- `src/services/engine/tests/governed_flight.rs` — add a client-method round-trip over a real UDS engine.
- `src/services/query-api/BUCK`, `src/services/engine/BUCK` — new test targets.

---

## Task 1: Closed-world governed registration (engine-serving)

**Files:**
- Modify: `src/services/engine-serving/src/governed.rs:289-308` (`execute_governed_sql_stream`)
- Modify: `src/control-plane/core/src/governed.rs:24-27` (doc contract)
- Test: `src/services/engine-serving/tests/governed_sql.rs:274-293` (rewrite `empty_policy_is_full_visibility`)

**Interfaces:**
- Consumes: `GovernedCatalog::table_for` (`core/src/governed.rs:36`).
- Produces: `execute_governed_sql_stream` that registers ONLY tables with a `GovernedTable` entry. Signature unchanged.

- [ ] **Step 1: Rewrite the pinning test first**

In `src/services/engine-serving/tests/governed_sql.rs`, replace `empty_policy_is_full_visibility` (line 275) with two tests sharing its seed harness:

- `empty_policy_entry_is_full_visibility` — same seed, but the catalog carries an entry: `GovernedCatalog { tables: vec![GovernedTable { table: gt("s","t"), row_filters: vec![], denied: vec![], masked: vec![] }] }`. Assert the same `vec![0, 1, 2]` result (an *entry* with empty policy stays fully visible).
- `unlisted_table_is_not_registered` — seed the table, pass `GovernedCatalog::default()` (no entries), run `SELECT "id" FROM "s"."t"`. The current `run` helper unwraps success — this case needs the error: call `execute_governed_sql_stream` directly and assert `Err`, and assert the error `Display` mentions the table resolution failure (DataFusion's "table not found" class) — i.e. indistinguishable from a nonexistent table.

- [ ] **Step 2: Run to verify the new test fails**

Run: `buck2 test --console none //src/services/engine-serving:governed-sql`
Expected: FAIL — `unlisted_table_is_not_registered` gets rows back under the current absent⇒visible semantics.

- [ ] **Step 3: Flip the loop to closed-world**

In `execute_governed_sql_stream` (`governed.rs:296-305`), skip tables with no catalog entry:

```rust
    for table in catalog.live_tables().await.map_err(to_serving)? {
        // Closed-world: a live table with NO GovernedTable entry is not
        // registered at all — it does not exist for this session. Deny-by-
        // default holds even if the edge under-lists (unbound datasets,
        // ungranted types). See 2026-07-09-external-sql-wire-design.md.
        if governed.table_for(&table).is_none() {
            continue;
        }
        let Some(inner) =
            build_serving_provider(&ctx, catalog, &table, serving_store, None).await?
        else {
            continue;
        };
        let policy = policy_for(governed, &table);
        ...
```

Update the function's doc comment ("for each live table" → "for each live table **listed in the governed catalog**"). `policy_for` (`:152`) keeps its absent-⇒-empty arm (now unreachable from this loop; still used by tests/other callers) — leave it, its doc already says "absent ⇒ visible **when asked**".

- [ ] **Step 4: Rewrite the core doc contract**

In `src/control-plane/core/src/governed.rs:24-27`, replace the `GovernedCatalog` doc:

```rust
/// A fully-resolved governed catalog: one `GovernedTable` per type the caller may see.
/// The engine's governed-SQL path is CLOSED-WORLD over this catalog: a live table with
/// no entry is not registered at all (unresolvable, indistinguishable from nonexistent).
/// The edge (query-api) lists exactly the tables the subject may read; the engine
/// enforces the omission. An entry with an empty policy is fully visible.
```

- [ ] **Step 5: Run the governed suites**

Run: `buck2 test --console none //src/services/engine-serving:governed-sql //src/services/engine:governed-flight //src/services/engine-wire:governed-ticket //src/services/engine-serving:row-filter-to-expr`
Expected: PASS (`governed_flight.rs` lists its table explicitly, so it is unaffected).

- [ ] **Step 6: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "fix(engine): closed-world governed-SQL registration

A live table absent from the GovernedCatalog is no longer registered
fully-visible — it is not registered at all. Amends slice 1's absent=>visible
decision (which would have exposed unbound datasets to external SQL); the
governed ticket has no production caller yet, so the blast radius is the
slice-1 tests, rewritten to pin the closed-world contract."
```

---

## Task 2: `FlightSqlClient::execute_governed_stream` (engine-wire) + engine round-trip test

**Files:**
- Modify: `src/services/engine-wire/src/flight.rs` (after `execute_stream`, line 386)
- Test: `src/services/engine/tests/governed_flight.rs` (new test fn) + `src/services/engine/BUCK` (add `//src/testing:flight` to the `governed-flight` target's deps if absent)

**Interfaces:**
- Consumes: `GovernedStatementQuery` (`flight.rs:93`), `decode_batches` (`flight.rs:247`), `crate::client::sql_status`/`be` (the error mappings `execute_as_of` uses, `flight.rs:391-412`).
- Produces: `pub async fn execute_governed_stream(&self, sql: String, catalog: GovernedCatalog) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>>` on `FlightSqlClient`.

- [ ] **Step 1: Write the failing test**

In `src/services/engine/tests/governed_flight.rs`, add a test that goes over a REAL UDS (the existing test calls `svc.do_get` in-process):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_execute_governed_stream_applies_policy() {
    // seed s.orders with ids 0..4 + a name column (reuse the existing harness shape)
    // policy: id >= 2, mask "name"
    let eng = loom_test_flight::spawn_flight_uds(fx, &db, &warehouse).await;
    let client = engine_wire::flight::FlightSqlClient::connect(eng.sock.clone())
        .await
        .expect("connect");
    let batches: Vec<_> = client
        .execute_governed_stream(
            r#"SELECT "id", "name" FROM "s"."orders" ORDER BY "id""#.into(),
            cat, // the GovernedCatalog with the filter + mask
        )
        .await
        .expect("stream open")
        .try_collect()
        .await
        .expect("collect");
    // assert ids [2,3,4]; every "name" value is "***"
}
```

Check `src/services/engine/BUCK`'s `governed-flight` target deps: it needs `//src/testing:flight` (mirror `query-api`'s `governed-flight-export-e2e` deps at `src/services/query-api/BUCK:556`) and `//third-party:futures` if not present.

- [ ] **Step 2: Run to verify it fails to compile**

Run: `buck2 build -v0 --console none //src/services/engine:governed-flight`
Expected: FAIL — no method `execute_governed_stream`.

- [ ] **Step 3: Add the client method**

In `src/services/engine-wire/src/flight.rs`, after `execute_stream` (line 386), mirroring `execute_as_of`'s single-hop `do_get` but returning the stream like `execute_stream`:

```rust
    /// Execute arbitrary client `sql` under a caller-resolved governed catalog,
    /// returning the decoded result stream. Single-hop `do_get` of a
    /// `GovernedStatementQuery` ticket (the standard `CommandStatementQuery`
    /// cannot carry the catalog). The stream never materialises in the caller —
    /// query-api's external SQL wire relays it straight out.
    pub async fn execute_governed_stream(
        &self,
        sql: String,
        catalog: GovernedCatalog,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> {
        let ticket = GovernedStatementQuery { sql, catalog };
        let resp = self
            .inner
            .clone()
            .do_get(Ticket {
                ticket: ticket.encode().into(),
            })
            .await
            .map_err(crate::client::sql_status)?;
        Ok(Box::pin(decode_batches(resp).map_err(crate::client::be)))
    }
```

(`GovernedCatalog` is already imported at `flight.rs:13`; `sql_status` keeps plan errors in the `Validation` class so the wire service can map them to `invalid_argument`.)

- [ ] **Step 4: Run the test**

Run: `buck2 test --console none //src/services/engine:governed-flight`
Expected: PASS.

- [ ] **Step 5: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(engine-wire): FlightSqlClient::execute_governed_stream

The governed sibling of execute_stream/execute_as_of: a single-hop do_get of
a GovernedStatementQuery ticket, returning the decoded batch stream. Proven
over a real UDS engine with a row filter + column mask applied."
```

---

## Task 3: Shared Flight bearer auth (session + service tokens)

**Files:**
- Modify: `src/services/runtime/src/auth.rs:82` (`resolve_bearer` → `pub`) + the runtime crate-root re-export (add to the `pub use` in `src/services/runtime/src/lib.rs` next to `token_sha256`)
- Create: `src/services/query-api/src/flight_auth.rs`
- Modify: `src/services/query-api/src/flight_export.rs:159-176` (delete the local `authenticate`, use the shared one)
- Modify: `src/services/query-api/src/lib.rs` (`pub(crate) mod flight_auth;`)
- Test: non-regression — `//src/services/query-api:governed-flight-export-e2e` stays green; the service-token acceptance is proven end-to-end in Task 6's e2e.

**Interfaces:**
- Consumes: `resolve_bearer` (`runtime/src/auth.rs:82` — session first, then service token), `token_sha256` (`runtime/src/crypto.rs:50`).
- Produces: `pub(crate) async fn flight_auth::authenticate(auth: &(dyn Auth + Send + Sync), md: &tonic::metadata::MetadataMap) -> Result<SubjectId, Status>`.

- [ ] **Step 1: Make `resolve_bearer` public**

In `src/services/runtime/src/auth.rs:82`, change `async fn resolve_bearer` to `pub async fn resolve_bearer` and extend its doc: it is now the single bearer-resolution seam for HTTP middleware AND the Flight surfaces. Re-export it from the runtime crate root alongside the other auth exports.

- [ ] **Step 2: Extract the shared `authenticate`**

Create `src/services/query-api/src/flight_auth.rs` — move `flight_export.rs`'s `authenticate` (lines 159-176) verbatim, with ONE change: `auth.resolve_session(...)` becomes `service_runtime::resolve_bearer(auth, &hash, OffsetDateTime::now_utc())` (session → service-token fallback). Keep the error mapping exactly: missing header → `Status::unauthenticated("missing bearer token")`, unresolved → `unauthenticated("invalid or expired token")`, store fault → opaque internal (move or re-import the `internal` helper — keep `internal` in `flight_export.rs` and have `flight_auth` carry its own copy of the two-line log-and-opaque mapping, or hoist `internal` into `flight_auth` and re-export; pick whichever keeps both modules clippy-clean without a cycle).

Update `flight_export.rs`'s two call sites (`:222`, `:248`) to `crate::flight_auth::authenticate(...)` and delete the local fn. Add `pub(crate) mod flight_auth;` to `lib.rs`.

- [ ] **Step 3: Build + run the export e2e**

Run: `buck2 build -v0 --console none //src/services/query-api:query-api //src/services/runtime:runtime`
Run: `buck2 test --console none //src/services/query-api:governed-flight-export-e2e //src/services/runtime:auth` (check the runtime BUCK for the auth test target's exact name; run whatever targets cover `auth.rs`).
Expected: PASS — session-token behavior unchanged; the export now also accepts service tokens (a strict widening, documented in the commit).

- [ ] **Step 4: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(query): shared Flight bearer auth resolves service tokens

Extract the Flight-surface authenticate (gRPC authorization metadata ->
SubjectId) into flight_auth, backed by the now-pub
service_runtime::resolve_bearer (session -> service-token fallback). The
governed Flight export thereby accepts service tokens too — previously it
checked login sessions only."
```

---

## Task 4: `resolve_governed_catalog` (query-api) + memory-control-plane unit test

**Files:**
- Modify: `src/services/query-api/src/governed.rs` (new pub fn after `load_policy`, line 122)
- Test: `src/services/query-api/tests/governed_catalog_resolve.rs` (new) + a `rust_test` target in `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `Ontology::list_types` (`core/src/ontology.rs:919`), `Acl::check`, `load_policy` (`governed.rs:103`), `GovernedCatalog`/`GovernedTable` (`core/src/governed.rs`).
- Produces: `pub async fn resolve_governed_catalog(ontology: &(dyn Ontology + Send + Sync), acl: &(dyn Acl + Send + Sync), subject: &SubjectId) -> Result<GovernedCatalog, QueryError>`.

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/governed_catalog_resolve.rs` using `control_plane_memory::MemoryControlPlane` (pure — no fixture needed). Seed: three types (`Orders` with a row-filter + mask policy for role R; `Customers` with a plain Read grant; `Secrets` with no grant), subject S in role R. Assert:

- the catalog lists exactly `Orders`' and `Customers`' `TableRef`s (no `Secrets` — deny-by-default);
- `Orders`' entry carries the row filter, denied and masked column sets from the seeded policies (as `Vec`s);
- `Customers`' entry has empty policy vectors;
- **duplicate table:** define a fourth granted type bound to `Orders`' same `TableRef` — the catalog contains ONE entry for that `TableRef` (first wins).

Wire a plain `rust_test` target `governed-catalog-resolve` in `src/services/query-api/BUCK`, mirroring an existing non-fixture target (e.g. `export-command`): `srcs = ["tests/governed_catalog_resolve.rs"]`, deps `[":query-api", "//src/control-plane/core:core", "//src/control-plane/memory:memory", "//third-party:tokio"]`.

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/services/query-api:governed-catalog-resolve`
Expected: FAIL — `resolve_governed_catalog` not found.

- [ ] **Step 3: Implement**

In `src/services/query-api/src/governed.rs`, after `load_policy`:

```rust
/// Resolve the subject's per-request governed catalog: one `GovernedTable` per
/// ontology type the subject holds a coarse Read grant on (deny-by-default —
/// an ungranted type is OMITTED, and the engine's closed-world registration
/// makes an omitted table unresolvable). Fail-closed: any ACL/ontology error
/// aborts the whole resolution — never an empty-policy fallback entry. If two
/// allowed types bind the same table the first (by list order) wins — each
/// type's policy is independently reachable over the HTTP read path already,
/// so this leaks nothing beyond existing capability; a warn fires for audit.
pub async fn resolve_governed_catalog(
    ontology: &(dyn Ontology + Send + Sync),
    acl: &(dyn Acl + Send + Sync),
    subject: &SubjectId,
) -> Result<control_plane_core::GovernedCatalog, QueryError> {
    let types = ontology.list_types(PageReq::unbounded()).await?;
    let mut tables: Vec<control_plane_core::GovernedTable> = Vec::new();
    for ty in types.items {
        let target = PolicyTarget::Type(ty.name.clone());
        if acl.check(subject, Action::Read, &target).await? == Decision::Deny {
            continue;
        }
        if tables.iter().any(|gt| gt.table == ty.table) {
            tracing::warn!(table = ?ty.table, ty = %ty.name.0,
                "duplicate table binding in governed catalog; first entry wins");
            continue;
        }
        let (row_filters, denied, masked) = load_policy(acl, subject, &target).await?;
        tables.push(control_plane_core::GovernedTable {
            table: ty.table,
            row_filters,
            denied: denied.into_iter().collect(),
            masked: masked.into_iter().collect(),
        });
    }
    Ok(control_plane_core::GovernedCatalog { tables })
}
```

(Adjust imports; `GovernedCatalog`/`GovernedTable` may need adding to the `control_plane_core` use list. `denied`/`masked` are `HashSet` → collect to `Vec`; sort them if the test wants determinism.)

- [ ] **Step 4: Run the test**

Run: `buck2 test --console none //src/services/query-api:governed-catalog-resolve`
Expected: PASS.

- [ ] **Step 5: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(query): resolve_governed_catalog — subject ACL -> GovernedCatalog

The edge half of the external SQL wire: enumerate ontology types, coarse
Read gate (deny-by-default: ungranted types are omitted), fold load_policy
into one GovernedTable per visible type. Fail-closed on any ACL/ontology
error; duplicate table bindings resolve first-wins with a warn."
```

---

## Task 5: `SqlWireTuning` on the typed config seam

**Files:**
- Modify: `src/services/query-api/src/config.rs` (new domain + `QueryApiConfig` field)
- Test: `src/services/query-api/tests/sql_wire_config.rs` (new) + a `rust_test` target in `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `loom_config::{overlay_opt, invalid, LayeredConfig, load}` (`src/loom-config/src/lib.rs:35/:25/:91/:104`).
- Produces: `SqlWireTuning { bind_addr: Option<String>, max_rows: u32 }`, default `{ None, 1_000_000 }`; `QueryApiConfig.sql_wire`; env `LOOM_SQL_WIRE_BIND_ADDR` / `LOOM_SQL_WIRE_MAX_ROWS`.

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/sql_wire_config.rs` driving `loom_config::load::<query_api::config::QueryApiConfig>` with hand-built env maps (check `config.rs`'s visibility — make the `config` module pub if it is not):

- empty env → `sql_wire.bind_addr == None`, `max_rows == 1_000_000`;
- `LOOM_SQL_WIRE_BIND_ADDR=127.0.0.1:31337`, `LOOM_SQL_WIRE_MAX_ROWS=500` → both land;
- `LOOM_SQL_WIRE_BIND_ADDR=not-an-addr` → `Err` (validate rejects an unparseable `SocketAddr`);
- `LOOM_SQL_WIRE_MAX_ROWS=0` → `Err`.

Wire a plain `rust_test` target `sql-wire-config` in `src/services/query-api/BUCK` (deps: `":query-api"`, `"//src/loom-config:loom-config"`).

- [ ] **Step 2: Run to verify it fails, then implement**

Run: `buck2 test --console none //src/services/query-api:sql-wire-config` — expected FAIL. Then in `config.rs` add:

```rust
/// External Flight SQL wire tuning. `bind_addr` unset => the listener does not
/// start (opt-in, like the Flight export). Typed from day one — the new wire's
/// knobs never join the export's raw env reads (fut-flight-export-config-seam).
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct SqlWireTuning {
    pub bind_addr: Option<String>,
    pub max_rows: u32,
}
```

with a hand-rolled `Default` (`max_rows: 1_000_000`) — note `derive(Default)` would give 0, so implement `Default` manually like `ServingTuning` (`config.rs:15-21`). `overlay_env`: `bind_addr` is `Option<String>` (no `FromStr`), so overlay manually — `if let Some(v) = vars.get("LOOM_SQL_WIRE_BIND_ADDR") { self.bind_addr = Some(v.clone()); }`; `overlay_opt(vars, "LOOM_SQL_WIRE_MAX_ROWS", &mut self.max_rows)?`. `validate()`: if `Some(addr)`, `addr.parse::<std::net::SocketAddr>()` must succeed (else `invalid("LOOM_SQL_WIRE_BIND_ADDR", e)`); `max_rows >= 1`. Thread both calls through `QueryApiConfig`'s `LayeredConfig` impl (`config.rs:46-56`).

- [ ] **Step 3: Run the test, prek, commit**

Run: `buck2 test --console none //src/services/query-api:sql-wire-config` — expected PASS.

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(query): SqlWireTuning — typed config for the external SQL wire

LOOM_SQL_WIRE_BIND_ADDR (opt-in listener; validated as a SocketAddr at
startup) + LOOM_SQL_WIRE_MAX_ROWS (stream-side row cap, default 1e6) on the
QueryApiConfig layered seam. Fail-loud on malformed values."
```

---

## Task 6: `FlightSqlWireService` + serve.rs wiring

**Files:**
- Create: `src/services/query-api/src/flight_sql.rs`
- Modify: `src/services/query-api/src/lib.rs` (`pub mod flight_sql;`)
- Modify: `src/services/query-api/src/serve.rs` (spawn from `app_cfg.sql_wire`, mirroring `spawn_flight_export` at `:107`)
- Test: compiles + spawns; behavior is Task 7's e2e.

**Interfaces:**
- Consumes: `flight_auth::authenticate` (T3), `resolve_governed_catalog` (T4), `FlightSqlClient::execute_governed_stream` (T2), `SqlWireTuning` (T5), the export's relay/cap/scrub shapes (`flight_export.rs:244-293`), the engine's `CommandStatementQuery`→`TicketStatementQuery` dance (`engine/src/flight.rs:268-294`).
- Produces: `pub struct FlightSqlWireService { auth, cp, engine: FlightSqlClient, max_rows }` with `pub fn new(...)`; `spawn_sql_wire` in `serve.rs`.

- [ ] **Step 1: The service**

`flight_sql.rs`, a raw `FlightService` impl (mirror `flight_export.rs`'s trait skeleton and type aliases):

- **`get_flight_info`:** `authenticate` → decode `descriptor.cmd` as `Any` → `unpack::<CommandStatementQuery>()` (decode failure → `invalid_argument("bad flight-sql command: ...")`; a non-`CommandStatementQuery` Any → `unimplemented("only CommandStatementQuery is supported on this wire")` — mirror `engine/src/flight.rs:273-280` verbatim) → reply with a `FlightInfo` whose endpoint ticket is `TicketStatementQuery { statement_handle: cmd.query.into_bytes().into() }`, **no schema attached** (comment why: arbitrary SQL — the client reads the schema from the `do_get` stream's first message).
- **`do_get`:** `authenticate` → decode the ticket as `Any` + `unpack::<TicketStatementQuery>()` **only** — anything else (including every loom-native JSON ticket) is `invalid_argument("bad flight-sql ticket")`. Add the load-bearing comment: *the external wire must never decode `GovernedStatementQuery` — a client-supplied catalog would be a total governance bypass; the catalog is resolved server-side per call from the authenticated subject.* Then: `String::from_utf8(handle)` (non-UTF-8 → `invalid_argument`) → `resolve_governed_catalog(self.cp.ontology(), self.cp.acl(), &subject)` (map `QueryError` via a local `map_query_err` clone of `flight_export.rs:192-202` — backend faults opaque) → `self.engine.execute_governed_stream(sql, catalog)` — map `ControlPlaneError::Validation` to `invalid_argument` (plan errors over the client's own SQL are safe to echo), everything else to the opaque `internal` helper → wrap the stream in the row-cap `map` (copy the export's cap block, `flight_export.rs:267-289`, with the cap message naming `LOOM_SQL_WIRE_MAX_ROWS`; no `+1` sentinel — pure stream-side counting, comment the divergence) → `FlightDataEncoderBuilder` out.
- Every other verb: `unimplemented` (slice 3+).

- [ ] **Step 2: Spawn from the typed config**

In `serve.rs`, after the export block (`:94-101`):

```rust
    // Optional external Flight SQL wire (opt-in via LOOM_SQL_WIRE_BIND_ADDR; typed seam).
    if let Some(bind) = app_cfg.sql_wire.bind_addr.clone() {
        spawn_sql_wire(
            &bind,
            &engine_socket,
            auth_sql,   // Arc<dyn Auth> clone captured like auth_flight (serve.rs:45)
            cp_sql,     // Arc<dyn ControlPlane> clone captured like cp_flight (serve.rs:44)
            app_cfg.sql_wire.max_rows,
        )
        .await?;
    }
```

`spawn_sql_wire` mirrors `spawn_flight_export` (`serve.rs:107-144`): parse the addr (already validated by config, but keep the parse-with-context error), `FlightSqlClient::connect(engine_socket)`, eager `TcpListener::bind` (hard startup failure), `tokio::spawn` the tonic server with a `tracing::info!(%addr, "starting external Flight SQL wire")`. Capture the `auth`/`cp` clones BEFORE `cp` moves into `AppState` (same pattern as `serve.rs:43-45`).

- [ ] **Step 3: Build + clippy**

Run: `buck2 build -v0 --console none //src/services/query-api:query-api`
Run: `buck2 build --console none --show-simple-output '//src/services/query-api:query-api[clippy.txt]'` and `cat` the printed path — must be empty (strict pedantic+restriction; no `unwrap`/`expect`/`indexing_slicing` crept in).

- [ ] **Step 4: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(query): external Flight SQL wire — TCP listener + governed forward

FlightSqlWireService: bearer-authenticated CommandStatementQuery over TCP;
per-call resolve_governed_catalog for the subject; forward over the internal
UDS wire as a GovernedStatementQuery; relay the batch stream out under
LOOM_SQL_WIRE_MAX_ROWS. Only protobuf Flight SQL tickets decode — loom-native
JSON tickets (a client-supplied catalog) are rejected by construction.
Spawned from the typed SqlWireTuning config."
```

---

## Task 7: Acceptance e2e — arbitrary SQL, governed, over real TCP

**Files:**
- Create: `src/services/query-api/tests/external_sql_wire_e2e.rs`
- Modify: `src/services/query-api/BUCK` — `loom_fixture_test` target `external-sql-wire-e2e`, deps mirroring `governed-flight-export-e2e` (`BUCK:556-573`: `//src/testing:seed`, `//src/testing:flight`, `":query-api"`, `":e2e-support"`, `"//src/services/engine:engine"`, `"//src/services/engine-wire:engine-wire"`, core, postgres, arrow-*, tonic, tokio, futures, prost + `//third-party:arrow-flight`).

**Interfaces:**
- Consumes: everything Tasks 1-6 produced; `spawn_flight_uds` (`src/testing/flight.rs:144`); e2e-support `subject_with_role`/`grant_read`/`grant_read_filtered`/`grant_read_columns`/`session_token` (`e2e_support.rs:205/:214/:647/:671/:228`); `Auth::create_service_account`/`create_service_token` (`core/src/auth.rs:195/:200`) + `service_runtime::{generate_session_token, token_sha256}` for the service-token mint.

- [ ] **Step 1: The harness**

Mirror `governed_flight_export_e2e.rs`'s `setup` (`:123-231`) — land through the REAL landing path, boot the engine over UDS, stand the service on an ephemeral TCP port — with this seed:

- `wh.orders` (`id` long identity, `customer_id` long, `email` string) bound to type `Order`; `wh.customers` (`id` long identity, `name` string, `ssn` string) bound to type `Customer`; **plus one landed dataset `wh.raw_dump` bound to NO type**.
- Subject `reader`: `grant_read_filtered` on `Order` (row filter `id >= 2`), `grant_read_columns` on `Order` masking `email`; `grant_read_columns` on `Customer` denying `ssn` (check the helper's deny/mask arg order at `e2e_support.rs:671`). No grant on anything else.
- A session token via `session_token(&cp, "reader")`, AND a service token: `create_service_account(&NewServiceAccount { subject_id: svc_subject, name: "ci-bot" })`, put the account's subject in the reader role too, `create_service_token(&svc_subject, &token_sha256(&tok), "e2e", now + 1h)`.
- Stand `FlightSqlWireService::new(auth, cp_dyn, flight_engine, max_rows)` on `127.0.0.1:0` exactly as the export e2e does (`:217-228`); a `setup_with_cap` variant for the cap case.
- Drive with a real `FlightServiceClient` + the export e2e's `authed(msg, token)` helper and its `get_flight_info` → ticket → `do_get` → `FlightRecordBatchStream` decode dance (`:342-363`).

- [ ] **Step 2: The cases** (each its own `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]`)

1. `arbitrary_join_is_governed` — `SELECT o."id", c."name", o."email" FROM "wh"."orders" o JOIN "wh"."customers" c ON o."customer_id" = c."id" ORDER BY o."id"` with the session token → only `id >= 2` rows; every `email` value is `"***"` (Utf8); `name` survives value-exact. Also a `GROUP BY` flavor (`SELECT "email", count(*) FROM "wh"."orders" GROUP BY "email"`) → a single `'***'` group.
2. `denied_column_absent_and_unnameable` — `SELECT * FROM "wh"."customers"` → no `ssn` field in the schema; `SELECT "ssn" FROM "wh"."customers"` → `invalid_argument`.
3. `ungranted_and_unbound_tables_unresolvable` — a granted-subject query against a type the subject holds no grant on (seed a third typed table with no grant) AND against `"wh"."raw_dump"` → both `invalid_argument` with a table-resolution error; assert the two errors are the same class as querying `"wh"."does_not_exist"` (no existence leak).
4. `auth_matrix` — no bearer → `Unauthenticated`; garbage bearer → `Unauthenticated`; the **service token** runs case-1's query successfully.
5. `forged_governed_ticket_rejected` — build a `GovernedStatementQuery { sql, catalog }` with a wide-open catalog (`engine_wire::flight::GovernedStatementQuery::encode`), send its bytes as the `do_get` ticket with a VALID token → `invalid_argument`, and assert no rows ever arrive.
6. `row_cap_errors_stream` — `setup_with_cap(…, small_n)`; a query returning more than `small_n` rows fails the stream (mirror `export_cap_exceeded_errors_stream`, `governed_flight_export_e2e.rs:479-517`).

- [ ] **Step 3: Run**

Run: `buck2 test --console none //src/services/query-api:external-sql-wire-e2e`
Expected: PASS, all cases.

- [ ] **Step 4: Non-regression sweep**

Run: `buck2 test --console none //src/services/query-api:governed-flight-export-e2e //src/services/engine-serving:governed-sql //src/services/engine:governed-flight //src/services/engine-wire:governed-ticket`
Then the touched crates' full target lists (rebase footgun — build ALL their test targets):
`buck2 test --console none //src/services/query-api: //src/services/engine: //src/services/engine-serving: //src/services/engine-wire: //src/services/runtime:` (locally add `-j 8`).
Expected: PASS.

- [ ] **Step 5: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "test(query): external SQL wire acceptance e2e

Real TCP Flight SQL client against the full stack (landing -> engine UDS ->
wire service): arbitrary JOIN/GROUP BY SQL row-filtered and column-masked per
bearer subject; denied columns unnameable; ungranted/unbound tables
unresolvable (no existence leak); session AND service tokens accepted; missing
and bogus tokens Unauthenticated; forged loom-native governed tickets
rejected; the row cap errors the stream."
```

---

## Task 8: Registers + capability docs (close the item)

**Files:**
- Modify: `docs/ROADMAP.md` — remove the `#road-external-sql-wire` entry (the promoted item this branch builds; open work only — closed items leave the register).
- Modify: `docs/FUTURE.md`:
  - Delete the `#fut-external-sql-wire` entry (`FUTURE.md:138-139`) — slice 2 shipped; slices 3+ are already their own items.
  - Rewrite every `[[fut-external-sql-wire]]` cross-link so `tools/docs.sh validate` stays green (dangling `[[id]]` links fail it): `fut-governed-scan-pushdown` (`:141` — "Deferred until the external wire … makes read latency matter" → "the external SQL wire (shipped 2026-07-09) makes read latency measurable; take this when it matters on real workloads"), `fut-flight-sql-surface` (`:143` — "the external wire [[fut-external-sql-wire]]" → "the external SQL wire (shipped; see `docs/system-capabilities/engine.md`)"), the insta item (`:264`), `fut-ui-sql-editor-diagnostics` (`:328`), and `fut-ui-sql-query-console` (`:330` — note the governed execution substrate now exists; the HTTP console endpoint remains the deferred half).
  - `fut-flight-export-tls` and `fut-flight-export-config-seam` stay open unchanged (TLS is slice 3+; the export's raw-env knobs are still raw — only the NEW knobs are typed).
- Modify: `docs/system-capabilities/engine.md` — update the governed-egress section ("Internal-only today; the external TCP listener is the deferred next slice" → describe the shipped wire: TCP Flight SQL listener in query-api, bearer auth incl. service tokens, per-request `resolve_governed_catalog`, closed-world engine registration, row cap, no-JSON-ticket rule) and drop `#fut-external-sql-wire` from its Known gaps list. If query-api has its own capability page under `docs/system-capabilities/`, note the listener + config knobs there too.

- [ ] **Step 1: Edit the three surfaces above** (follow the `loom-docs-update` skill if running interactively).

- [ ] **Step 2: Validate + full-suite gate**

```bash
bash tools/docs.sh validate
buck2 build -v0 --console none //src/...
buck2 test --console none //src/... # locally: -j 8; cloud: scope to btd-affected targets instead
```

Expected: validate clean; build silent; tests `Fail 0`.

- [ ] **Step 3: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "docs(registers): close road-external-sql-wire; slice 3+ stays deferred

External SQL wire slice 2 shipped (TCP Flight SQL + bearer auth + per-
connection governed catalog). fut-external-sql-wire retires; catalog-metadata
commands / prepared statements / do_put remain fut-flight-sql-surface and TLS
remains fut-flight-export-tls. Capability documented in
system-capabilities/engine.md."
```

Then finish the branch per `superpowers:finishing-a-development-branch` — push and open a PR (never local-merge), title `feat(query): external SQL wire slice 2 — TCP Flight SQL + bearer auth + governed catalog`.
