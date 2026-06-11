# Serving tier over Quack — `QuackServingEngine` — Design

**Date:** 2026-06-11
**Status:** Approved (brainstorm)
**Roadmap:** Step 3, the "serving tier over Quack (the seam's Quack-client impl)" item.

## Goal

Add a second `ServingEngine` implementation, `QuackServingEngine`, that runs loom's
governed read SQL against a **separate DuckDB serving process** over the **Quack**
wire protocol — instead of the in-process `EmbeddedDuckDb`. Prove it returns
byte-identical `Rows` to `EmbeddedDuckDb` for the same compiled SQL (parity),
including the injection-safety contract. This mirrors the production shape: a
long-lived DuckDB `ATTACH`es the DuckLake catalog and serves Quack; loom's
query-api is a Quack **client**.

Scope is the engine + a hermetic serving fixture + parity tests. **Out of scope:**
wiring engine selection into the HTTP service, production serving-process
lifecycle, and the client-facing Quack endpoint (see "Findings / constraints").

## Mechanism (spike-proven against pinned DuckDB v1.5.3)

The `quack` extension is a registered v1.5.3 extension ("The DuckDB 'Quack'
Client/Server Protocol"), fetchable from `extensions.duckdb.org` like
ducklake/postgres_scanner. Server and client functions are table functions:

- **Server:** `SELECT … FROM quack_serve('quack://host:port', token := '<tok>', disable_ssl := true)` — starts an HTTP Quack server in a background thread; the process must stay alive to keep serving. `token` pins a fixed auth token (otherwise a random one is generated). `disable_ssl := true` for plain HTTP (local/test).
- **Client (stateless):** `SELECT * FROM quack_query('quack:host:port', '<sql>', token := '<tok>', disable_ssl := true)` — runs `<sql>` **on the server** and returns the result set over the wire. Confirmed returning real DuckLake data end-to-end.

### Two findings that shape the design

1. **A quack-forwarded query runs in a fresh server session that does *not*
   inherit the serving process's `USE lake`.** So a forwarded query must reach the
   catalog explicitly. `quack_query` accepts **multi-statement** SQL, and
   `quack_query('…', 'USE lake; SELECT … FROM "main"."orders"', …)` works
   (confirmed). So `QuackServingEngine` prepends `USE lake; ` to the forwarded SQL
   — and `compile_select` / `EmbeddedDuckDb` stay **untouched** (the Quack concern
   is fully localized to the new engine).

2. **`ATTACH` (the alternative client style) is *not viable* in v1.5.3 for loom.**
   A client `ATTACH 'quack:…'` exposes only the server's *primary* catalog; loom's
   DuckLake is a *secondary* ATTACH (`lake`) on the server, and addressing it
   through the client alias (`r.lake.main.orders`) returns
   `Parser Error: NameListToString NOT IMPLEMENTED`. So the `quack_query` style is
   the chosen (and only working) client mechanism here. The faithful "external
   client ATTACHes loom and sees tables like local" experience is the separate,
   later roadmap item *the client-facing Quack endpoint*; it will need this
   secondary-catalog-over-Quack support (a newer DuckDB, or exposing lake as the
   serving engine's primary catalog) and is explicitly deferred.

## Components

### 1. Bundle the `quack` extension — `src/control-plane/postgres/BUCK`

The offline extension dir (`duckdb-extensions` genrule) currently holds
ducklake + postgres_scanner (+ parquet). Add `quack`:

- New `http_file` `quack-ext.gz` →
  `http://extensions.duckdb.org/{DUCKDB_VERSION}/linux_amd64/quack.duckdb_extension.gz`
  (pin its sha256, refreshed like the others).
- Extend the `duckdb-extensions` genrule to gunzip it into
  `$OUT/{DUCKDB_VERSION}/linux_amd64/quack.duckdb_extension`.

Both the serving process (LOADs ducklake + postgres_scanner + quack) and the
client engine (LOADs quack) read this same `DUCKDB_EXTENSION_DIR`.

### 2. `QuackServingEngine` — `src/services/query-api/src/serving.rs`

A new `ServingEngine` impl alongside `EmbeddedDuckDb`. It is a thin Quack client:
an in-process `duckdb-rs` connection that only `LOAD quack`s and forwards SQL via
`quack_query`. It holds the server coordinates:

```rust
pub struct QuackServingEngine {
    uri: String,    // "quack:127.0.0.1:<port>"
    token: String,  // fixed auth token agreed with the server
}

impl QuackServingEngine {
    pub fn new(uri: impl Into<String>, token: impl Into<String>) -> Self { … }
}
```

`fetch_rows(sql, params)` builds the forwarded SQL and runs it:

1. **Inline the params into `sql`** (see §3) → `inlined`.
2. **Prepend the catalog context:** `forwarded = format!("USE lake; {inlined}")`.
3. **Build the outer `quack_query` call.** `quack_query` is a table function, whose
   arguments DuckDB requires to be **constant** (a `?` bind placeholder is not
   allowed in a table-function argument), so the forwarded SQL is embedded as a
   string literal — escaped once more for that outer literal:
   ```sql
   SELECT * FROM quack_query('<uri>', '<escape(forwarded)>', token := '<token>', disable_ssl := true)
   ```
   where `escape(s) = s.replace('\'', "''")`. `uri` and `token` are loom-internal
   constants. The two escaping layers compose correctly: each layer is a DuckDB
   string literal, so doubling `'` at each level round-trips exactly (a `Text`
   value unwraps to itself at the server). The injection case (§5) proves this.
4. **Run it via duckdb-rs and map the result set to `Rows`** using the same column/row extraction
   `EmbeddedDuckDb` already uses (refactor that row-mapping out of `run_sync` into a
   shared private helper so both engines share one code path). Runs on a blocking
   thread (`spawn_blocking`), like `EmbeddedDuckDb`.

`ServingError::Engine` for failures (same as today). The `ServingEngine` trait is
unchanged — callers see the same `fetch_rows(sql, params)` contract.

### 3. Parameter inlining — the one place values enter SQL as text

`quack_query` takes the inner SQL as a string, so the Quack path cannot bind the
inner `?` placeholders the way `EmbeddedDuckDb` does. `QuackServingEngine` renders
each typed `SqlValue` into a DuckDB SQL literal and substitutes the placeholders:

```rust
fn render(v: &SqlValue) -> String {
    match v {
        SqlValue::Int(n)  => n.to_string(),
        SqlValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        SqlValue::Null    => "NULL".into(),
        // DuckDB standard string literal: single-quoted, escape ' by doubling.
        SqlValue::Text(s) => format!("'{}'", s.replace('\'', "''")),
    }
}
```

Substitution is a **single left-to-right pass** over `sql`, emitting the next
rendered param at each `?` and copying every other character verbatim. This relies
on the contract that **`compile_select` emits `?` only as bind placeholders**
(never a literal `?` inside a string) — true today (`sql.rs`: placeholders are the
injection boundary). The pass must not re-scan substituted text (a rendered `Text`
value may itself contain `?`), so a naive global replace is wrong; iterate the
original `sql` once.

**Injection safety:** the values are *typed scalars produced by the governance
layer* (`Int`/`Bool`/`Null`/`Text`), not raw wire strings, and `Text` is escaped by
doubling `'` — the complete and correct escape for DuckDB standard string literals
(no backslash escapes by default). The governance predicates/projections are still
generated by `compile_select` exactly as for `EmbeddedDuckDb`; inlining only changes
how the typed *values* land. This is verified by reusing the existing evil-value
case (below).

### 4. Hermetic serving fixture — `QuackServer` (in the parity test)

A test-support helper (lives in the parity test file `tests/quack_serving.rs`,
since only that test needs it; promote later if reused). It spawns the pinned
`duckdb-cli` (from `DUCKDB_BIN`) as a child process running a script that:

```
SET extension_directory='<DUCKDB_EXTENSION_DIR>';
LOAD ducklake; LOAD postgres_scanner; LOAD quack;
ATTACH 'ducklake:postgres:dbname=<db> host=<sock> user=postgres' AS lake (DATA_PATH '<data>/', DATA_INLINING_ROW_LIMIT 0);
USE lake;
SELECT … FROM quack_serve('quack://127.0.0.1:<port>', token := '<fixed-token>', disable_ssl := true);
```

then stays alive (the child's stdin is held open so the server thread keeps
running). `QuackServer::start(socket, db, data_path, ext_dir)`:

- picks a free localhost TCP port (bind a `TcpListener` to `127.0.0.1:0`, read the
  port, drop it — then hand the port to `quack_serve`);
- spawns the child, then polls the port until it accepts a TCP connection (bounded,
  ~a few seconds, mirroring `PgFixture::wait_ready`);
- exposes `uri()` (`"quack:127.0.0.1:<port>"`) and `token()`;
- kills the child on `Drop`.

It composes with the existing `PgFixture` + `DuckLakeWriter` test-support (from
`control-plane-postgres`): `PgFixture` gives the socket; `DuckLakeWriter` seeds a
`lake.main.<table>`; `QuackServer` serves that same catalog/data-path; the engines
read it.

### 5. Parity tests — `src/services/query-api/tests/quack_serving.rs`

A new `loom_fixture_test(duckdb = True)` target. Each test boots `PgFixture`, seeds
a catalog with `DuckLakeWriter`, starts a `QuackServer`, and constructs both an
`EmbeddedDuckDb` and a `QuackServingEngine` against the same catalog. Cases:

1. **Parity on a seeded read:** pick a SQL+params pair (hand-written or via
   `compile_select`) that reads the seeded table with a bound filter value; assert
   `embedded.fetch_rows(sql, params) == quack.fetch_rows(sql, params)` — identical
   `columns` and `rows`.
2. **Zero-rows still reports columns** through the Quack path (mirror the existing
   `EmbeddedDuckDb` test).
3. **Injection safety:** reuse the evil value `x'; DROP TABLE lake.t; --` as a
   `Text` param; assert it round-trips as data (one row, the literal string) and the
   table still exists — proving the inline-escape is airtight, the Quack-path analog
   of `params_are_bound_not_interpolated`.

`EmbeddedDuckDb`'s existing `tests/serving_engine.rs` stays as-is (it remains the
embedded-engine's own test); the new file is the cross-engine parity layer.

## Data flow

```
query-api (compile_select → sql + params[])
        │  QuackServingEngine.fetch_rows(sql, params)
        │    inline params → "USE lake; <sql>", escape for the outer literal
        ▼  duckdb-rs:  SELECT * FROM quack_query('quack:host:port', '<forwarded>', token:=…, disable_ssl:=true)
   Quack wire (HTTP)
        ▼
DuckDB serving process  ── USE lake (forwarded stmt) ─→ runs <sql> against
   ATTACH ducklake AS lake                                lake.main.<table>
        ▼
   DuckLake catalog (Postgres) + Parquet (data_path)
```

## Error handling

- Server-side SQL/auth errors surface through `quack_query` as a duckdb-rs error →
  mapped to `ServingError::Engine`.
- `QuackServer::start` panics on spawn/bind/timeout (test-only support, like
  `PgFixture`).
- Blocking duckdb-rs work runs on `spawn_blocking`; a join failure → `ServingError::Engine`.

## Testing strategy

- The parity target is a hermetic `loom_fixture_test` (postgres + duckdb-cli +
  quack), so it pins its own run local and is exercised by plain
  `buck2 test //src/...` — it inherits the local-routing that PR #40 already
  proved for all `loom_fixture_test` targets; no `--local-only` / `BUCK_PREFER_REMOTE`
  needed.
- The parity assertion (`embedded == quack`) is the core guarantee; the injection
  case guards the one place the Quack path diverges (inline vs bind).

## Findings / constraints (recorded for later)

- **v1.5.3 quack `ATTACH` cannot reach a secondary catalog** (`NameListToString
  NOT IMPLEMENTED`). The client-facing Quack endpoint roadmap item is blocked on
  this; revisit with a newer DuckDB or a primary-catalog ducklake exposure.
- **Quack is beta** — protocol/functions may change; we pin DuckDB v1.5.3 and treat
  upgrades as breaking (consistent with ARCHITECTURE.md).
- Forwarded sessions don't inherit `USE lake`; the `USE lake; ` prefix is required
  on every forwarded statement.

## Out of scope

- HTTP-service engine selection (Embedded vs Quack) and config — a later slice.
- Production serving-process supervision/deployment.
- The client-facing Quack endpoint (blocked per above).
- Any change to `compile_select`, `EmbeddedDuckDb`, ACL, or the ontology.
