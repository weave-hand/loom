# Design: governed object-read slice (Step 3, query — part 1)

> **Status:** approved design (2026-06-10). First sub-project of **Step 3 (query /
> read path)**, re-scoped against the revised `ARCHITECTURE.md` (DuckDB serving layer
> + Rust HTTP governance chokepoint; DataFusion scoped to ingestion). Supersedes the
> roadmap's old read-path bullets ("Quack-over-DataFusion server shim" + "Query API
> with DataFusion plan rewrite"), which described the pre-revision architecture.

## Goal

Prove the new read architecture end-to-end on its **thinnest governed path**: an HTTP
request naming an ontology type returns rows from loom's DuckLake catalog, with
**ontology resolution + a minimal ACL predicate compiled into the SQL**, executed on an
**embedded DuckDB serving engine behind a swappable seam**. This is the read-path analog
of the ingest snapshot-commit primitive: the smallest load-bearing slice that de-risks
the central architectural claim before the full service is built.

## Scope: this is part 1 of the query effort

The query/serving path is large; this spec is only its load-bearing read slice.
Decomposition:

- **Part 1 (this spec):** a `query-api` crate exposing one governed read —
  `GET /objects/{type}` → resolve the ontology type to a physical DuckLake table →
  inject a minimal ACL row predicate + column projection → execute on an embedded
  DuckDB (`duckdb-rs`) that `ATTACH`es loom's catalog read-only, behind a `ServingEngine`
  seam → return JSON rows.
- **Later specs:** the serving *tier* over Quack (separate `quack_serve`'d DuckDB
  process; the embedded engine becomes a Quack client behind the same seam); the
  client-facing Quack endpoint (loom's HTTP API runs `quack_serve` and governs inbound
  client SQL); the **actions** write path through the serving layer; full ACL semantics
  (deny-override, column masking, role hierarchy); rich ontology (links, derived
  properties); multi-type queries / joins; authentication.

## Architecture decisions (settled during brainstorming)

These forks were resolved before this spec; recorded here so the plan inherits them.

1. **DuckDB is the serving engine; the Rust API is the governance chokepoint.** Per the
   revised `ARCHITECTURE.md`. Reads enter the Rust HTTP query API, which resolves the
   ontology and applies ACL by *generating SQL*, then runs that SQL on real DuckDB. loom
   reimplements neither a query engine nor a wire protocol for reads.

2. **Embedded `duckdb-rs` behind a seam (not a separate Quack server) — for this slice.**
   A Quack "client" *is* a DuckDB instance (`CALL quack_serve('quack:host')` on the
   server side; `ATTACH 'quack:host'` on the client side — there is no standalone Rust
   Quack client library). So the separate-process option is *this* option plus a remote
   `ATTACH` target and a `quack_serve` on the far side — a config/deployment change behind
   the same `ServingEngine` seam, not a rewrite. We build the embedded path now and defer
   the Quack wire entirely to a later "serving tier" spec.

3. **Client-facing surface is plain HTTP (JSON), not Quack.** The "any DuckDB client can
   `ATTACH` loom" promise lives at the Rust API later (it runs `quack_serve` and rewrites
   inbound client SQL — a large surface, correctly deferred). Governance only holds while
   the serving DuckDB is private to the API; a client-facing Quack endpoint must therefore
   be the *governed* API surface, not the raw serving DuckDB.

4. **Request shape: ontology-typed read + minimal ACL.** The slice exercises the whole
   chokepoint chain (ontology → ACL → SQL), not just the serving path, but with minimal
   ACL semantics. Raw-SQL-with-type-rewrite was rejected (governing arbitrary client SQL
   is a large surface, wrong for a thin slice).

## Components

New crate **`src/services/query-api`** — the first crate under a new `src/services/`
namespace (Ingest and Transform join it in later specs). A library plus a thin binary,
consuming `control-plane-core` (`Ontology`, `Acl`, `Catalog`) and a
`control-plane-postgres` handle.

### `ServingEngine` seam

The abstraction that makes decision 2 a stepping-stone rather than a dead end:

```rust
/// Executes read-only SQL against loom's DuckLake catalog and returns rows.
/// `params` are bound positionally; SQL never interpolates caller values.
#[async_trait]
pub trait ServingEngine: Send + Sync {
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError>;
}
```

- **`EmbeddedDuckDb` (the only impl in this slice):** holds a `duckdb-rs` connection that
  has loaded the vendored `ducklake` extension and `ATTACH`ed loom's DuckLake catalog
  read-only (same Postgres backend, same single-catalog layout the interop test uses).
  `fetch_rows` prepares the SQL, binds params, and collects rows into a backend-neutral
  `Rows` (column names + typed cells).
- **Later:** a `QuackClient` impl (a `duckdb-rs` connection that `ATTACH`es
  `'quack:host'`) drops in here with zero handler changes.

### Handler core (socket-free — the primary testable unit)

```rust
async fn read_object(
    type_name: &str,
    filter: &[(String, SqlValue)],   // simple equality predicates
    subject: &Subject,
    deps: &QueryDeps,                // ontology + acl + serving
) -> Result<Rows, QueryError>;
```

Steps:
1. `ontology.resolve(type_name)` → physical `TableRef` + the type's column set.
2. `acl.policies_for(subject, type)` → a row predicate (optional) + the allowed column
   subset (minimal semantics: a single AND-ed predicate and a projection allow-list; no
   deny-override or masking).
3. Compile `SELECT <allowed cols> FROM <schema.table> WHERE <acl predicate> [AND <filter…>]`
   — **every caller value is a bound parameter**, never string-interpolated. Identifiers
   (table, columns) come from the resolved ontology/ACL metadata, not from the request, so
   they are trusted; request-supplied *values* are always bound.
4. `serving.fetch_rows(sql, params)` → `Rows`.
5. Serialize `Rows` → JSON.

### HTTP layer (thin, `axum`)

`GET /objects/{type}` — equality filters via query params (`?status=open`), subject via an
`X-Loom-Subject` header. The route deserializes, calls `read_object`, and returns the
JSON rows (or a mapped error status). `axum` is the first web dependency in the tree;
chosen as the de-facto-standard Rust HTTP framework. The route is deliberately trivial so
all logic — and all tests — live in the socket-free core.

## Data flow

```
client ──GET /objects/{type}──▶ axum route
                                   │  (deserialize type, filter, subject)
                                   ▼
                              read_object (handler core)
                       ┌───────────┴───────────┐
                       ▼                        ▼
            ontology.resolve            acl.policies_for      ← control-plane over Postgres
            (type → table+cols)         (subject → predicate + allowed cols)
                       └───────────┬───────────┘
                                   ▼  compile SELECT … (values bound)
                          ServingEngine.fetch_rows
                                   ▼
                    EmbeddedDuckDb (duckdb-rs, ATTACH ducklake, read-only)
                                   ▼
                              Rows → JSON ──▶ client
```

## Governance scope

- **Subject** is taken from the `X-Loom-Subject` request header; there is no
  authentication in this slice (authn is a later spec). The header value is enough to
  drive `policies_for`. An absent header → an "anonymous"/no-policy subject (sees only
  what an empty policy set allows).
- **ACL** is minimal: at most one AND-ed row predicate and a column allow-list projection.
  Deny-override, masking, and role hierarchy are explicitly out of scope.
- **Injection safety:** request-supplied values are always bound parameters. Only
  identifiers sourced from trusted ontology/ACL metadata reach the SQL string.

## Testing (hermetic; mirrors existing control-plane patterns)

Reuse `PgFixture` (boots pinned Postgres, applies migrations, `ATTACH`es a real DuckLake
catalog). Seed, in one fixture:
- an **ontology type** mapped to a physical `main.<table>`;
- **real data** by reusing the **snapshot-commit primitive** (`create_table` +
  `append_files`) plus a DuckDB-written Parquet at the resolved path (as the interop test
  does), so the embedded engine has actual rows to scan;
- an **ACL policy** for a test subject (a row predicate + a restricted column).

The `EmbeddedDuckDb` `ATTACH`es the same catalog read-only. Tests drive the **handler
core** directly (plus one HTTP smoke test that binds the `axum` server and issues a real
`GET`) and assert:
- the ontology type name resolved to the physical table (rows come back);
- the ACL **row predicate filtered out** a row the subject may not see;
- the ACL **projection dropped** a restricted column from the result;
- a request **equality filter** further narrows the rows;
- bound-parameter handling (a value containing `'` does not break or inject).

`duckdb-rs` is pinned so its embedded DuckDB is **1.5.3** (see risk below), and loads the
vendored `ducklake` extension via `extension_directory`, exactly as the interop test does.

## Key risk — DuckDB version / extension-ABI lock

The `ducklake` extension is ABI-locked to its DuckDB version: a `ducklake` build for 1.5.3
loads **only** into DuckDB 1.5.3. The embedded serving engine therefore must be exactly
**1.5.3** — our pinned catalog format (`duckdb/ducklake@e6a3bd0a`, spec v1.0) — to load the
vendored extension and read the catalog the rest of loom writes.

**Resolution (plan's first task is a spike to confirm it):** pin `duckdb-rs` so its DuckDB
is 1.5.3, and load the *same vendored `ducklake` build* the interop test uses, giving one
DuckDB version across the CLI, the extension, and the embedded engine. If no `duckdb-rs`
release bundles exactly 1.5.3, the fallbacks, in order: (a) link `duckdb-rs` against loom's
already-vendored DuckDB 1.5.3 libraries (non-bundled) so the version matches exactly and
stays hermetic with the existing pin; (b) adopt whatever `duckdb-rs` bundles and re-vendor
a matching `ducklake` extension build, updating the pin everywhere. Because this slice is
**read-only**, the stakes are lower than for a writer, but the extension must still load.

## What this slice is NOT

No Quack wire and no separate serving process; no client-facing Quack endpoint; no
actions/writes; no full ACL semantics (deny-override, masking, roles); no rich ontology
(links, derived/computed properties); no multi-type queries or joins; no pagination beyond
a `LIMIT`; no authentication. Each is a named later spec above.

## How this revises the roadmap

The roadmap (`2026-06-06-loom-roadmap.md`) Step 3 lists, against the pre-revision
architecture, a "Quack-over-DataFusion server shim" and a "Query API service" doing
"DataFusion plan rewrite." Both are superseded by the revised `ARCHITECTURE.md`: there is
no Quack-over-DataFusion shim (real DuckDB speaks Quack), and ACL is compiled into
generated SQL rather than rewritten into a DataFusion plan. This spec is **query part 1**
under that revised shape; the roadmap's read-path bullets should be updated to point here.
