# Serving Tier over Quack (`QuackServingEngine`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `QuackServingEngine` that runs loom's governed read SQL against a separate `quack_serve`'d DuckDB over the Quack wire protocol, proven byte-identical to `EmbeddedDuckDb` (parity), including injection safety.

**Architecture:** A second `ServingEngine` impl forwards SQL to a standalone serving DuckDB via the `quack_query` table function. Because a quack-forwarded session does not inherit the server's `USE lake`, the engine prefixes `USE lake;`; because `quack_query` takes SQL as a string (no bind slot), the engine inlines typed params as escaped SQL literals. `compile_select` and `EmbeddedDuckDb` are untouched — the Quack concern is fully localized to the new engine. A hermetic `QuackServer` test helper spawns the pinned `duckdb-cli` serving the DuckLake catalog.

**Tech Stack:** Rust, `duckdb-rs` (bundled DuckDB v1.5.3) as a thin Quack client, the `quack` DuckDB extension, buck2, the existing `PgFixture`/`DuckLakeWriter` hermetic test-support.

**Reference:** spec at `docs/superpowers/specs/2026-06-11-serving-tier-quack-design.md`. The full mechanism is spike-proven against v1.5.3: `quack_query('quack:host:port', 'USE lake; SELECT … FROM "main"."orders"', token := …, disable_ssl := true)` returns real DuckLake data over the wire.

**Conventions (loom-specific — read before starting):**
- Tests are `rust_test` integration targets only — NEVER inline `#[cfg(test)]`. A prek hook (`no-inline-tests`) blocks inline tests in `src/**`.
- Fixture-backed tests use the **`loom_fixture_test`** macro (`src/control-plane/postgres/defs.bzl`), never a bare `rust_test`, so the test run pins itself local.
- Run the suite with plain **`buck2 test //src/...`** (the macro routes fixture tests local; no `--local-only`).
- **Never pipe `buck2 test` through `tail`** — redirect to a file and grep: `buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`. `buck2 build … | tail` is fine.
- rustfmt is a **check-only** commit hook — run `buck2 run //tools:rustfmt -- <files>` before committing `.rs` files, or the commit aborts. Never `--no-verify`.
- Commit messages: Conventional Commits, ending with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Work on branch `feat/serving-tier-quack` (already created off `main`). Do NOT switch branches.

---

### Task 1: Bundle the `quack` extension into the offline extension dir

The serving process and the client engine both `LOAD quack` from `DUCKDB_EXTENSION_DIR`. The `duckdb-extensions` genrule currently ships ducklake + postgres_scanner; add `quack`.

**Files:**
- Modify: `src/control-plane/postgres/BUCK` (add an `http_file` after `postgres-scanner-ext.gz`, ~line 82; extend the `duckdb-extensions` genrule, ~line 86-95)

- [ ] **Step 1: Add the `http_file` for the quack extension**

Immediately after the `postgres-scanner-ext.gz` `http_file` block, add:

```python
http_file(
    name = "quack-ext.gz",
    urls = ["http://extensions.duckdb.org/{}/linux_amd64/quack.duckdb_extension.gz".format(DUCKDB_VERSION)],
    sha256 = "609be01482fde0bc1d99a64c7ae87134a134638c9c45e39285c64cd57d953f0c",
)
```

- [ ] **Step 2: Extend the `duckdb-extensions` genrule to unpack quack**

In the `duckdb-extensions` genrule's `cmd` list, add a third gunzip line (after the postgres_scanner one):

```python
        "gzip -dc $(location :quack-ext.gz) > $OUT/{}/linux_amd64/quack.duckdb_extension".format(DUCKDB_VERSION),
```

So the `cmd` becomes the `" && ".join([...])` of: the `mkdir`, the ducklake gunzip, the postgres_scanner gunzip, and the new quack gunzip.

- [ ] **Step 3: Build the extdir and confirm quack is present**

Run:
```bash
EXT="$PWD/$(buck2 build //src/control-plane/postgres:duckdb-extensions --show-output 2>/dev/null | awk '{print $2}')"
ls -1 "$EXT"/v1.5.3/linux_amd64/
```
Expected: lists `ducklake.duckdb_extension`, `postgres_scanner.duckdb_extension`, and `quack.duckdb_extension`.

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/postgres/BUCK
git commit -m "$(cat <<'EOF'
build(postgres): bundle the quack extension in the offline extdir

The serving tier (quack_serve) and the Quack-client engine both LOAD quack from
DUCKDB_EXTENSION_DIR. Pinned for v1.5.3/linux_amd64 like ducklake/postgres_scanner.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Parameter inlining (the typed-scalar → SQL-literal renderer)

`quack_query` takes the inner SQL as a string, so the Quack path inlines params instead of binding them. This is the one place values enter SQL as text; isolate and unit-test it (server-free).

**Files:**
- Modify: `src/services/query-api/src/serving.rs` (add three functions)
- Create: `src/services/query-api/tests/quack_inline.rs`
- Modify: `src/services/query-api/BUCK` (add the `quack-inline` test target)

- [ ] **Step 1: Write the failing unit test**

Create `src/services/query-api/tests/quack_inline.rs`:

```rust
//! Unit tests for the Quack path's parameter inlining: typed scalars render to
//! SQL literals and `?` placeholders are substituted left-to-right. The escape
//! (doubling `'`) is the injection boundary for the inline path — exercised here
//! server-free; the behavioural proof is in tests/quack_serving.rs.

use query_api::serving::{inline_params, SqlValue};

#[test]
fn renders_each_scalar_type() {
    assert_eq!(inline_params("?", &[SqlValue::Int(42)]), "42");
    assert_eq!(inline_params("?", &[SqlValue::Bool(true)]), "TRUE");
    assert_eq!(inline_params("?", &[SqlValue::Bool(false)]), "FALSE");
    assert_eq!(inline_params("?", &[SqlValue::Null]), "NULL");
    assert_eq!(
        inline_params("?", &[SqlValue::Text("hi".into())]),
        "'hi'"
    );
}

#[test]
fn escapes_single_quotes_in_text() {
    // The injection case: doubling ' makes this a safe string literal.
    assert_eq!(
        inline_params("WHERE s = ?", &[SqlValue::Text("x'; DROP TABLE lake.t; --".into())]),
        "WHERE s = 'x''; DROP TABLE lake.t; --'"
    );
}

#[test]
fn substitutes_placeholders_in_order_and_passes_other_chars_through() {
    let sql = "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?";
    let out = inline_params(
        sql,
        &[SqlValue::Int(1), SqlValue::Text("two".into()), SqlValue::Null],
    );
    assert_eq!(out, "SELECT * FROM t WHERE a = 1 AND b = 'two' AND c = NULL");
}

#[test]
fn does_not_rescan_substituted_text() {
    // A rendered Text value containing '?' must NOT consume a later param.
    let out = inline_params("? ?", &[SqlValue::Text("a?b".into()), SqlValue::Int(9)]);
    assert_eq!(out, "'a?b' 9");
}
```

- [ ] **Step 2: Add the test target and run it to verify it fails to compile**

In `src/services/query-api/BUCK`, add (plain `rust_test` — this one needs no fixture):

```python
rust_test(
    name = "quack-inline",
    crate = "quack_inline",
    srcs = ["tests/quack_inline.rs"],
    crate_root = "tests/quack_inline.rs",
    edition = "2024",
    deps = [":query-api"],
)
```

Run: `buck2 build //src/services/query-api:quack-inline 2>&1 | tail -20`
Expected: FAILS — `inline_params` is not found / not public in `query_api::serving`.

- [ ] **Step 3: Implement the inlining helpers**

In `src/services/query-api/src/serving.rs`, add these functions (place them near `to_duck`/`from_duck`). They are `pub` so the integration test can exercise them directly:

```rust
/// Escape a string for embedding in a DuckDB single-quoted literal: double every
/// `'`. This is the complete escape for DuckDB standard string literals (no
/// backslash escapes by default).
fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

/// Render a typed scalar as a DuckDB SQL literal.
fn render_literal(v: &SqlValue) -> String {
    match v {
        SqlValue::Int(n) => n.to_string(),
        SqlValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Text(s) => format!("'{}'", sql_escape(s)),
    }
}

/// Substitute each `?` placeholder in `sql` with the next rendered param, copying
/// every other character verbatim. The Quack path uses this because `quack_query`
/// takes SQL as a string with no bind slot. Relies on the `compile_select`
/// contract that `?` appears ONLY as a bind placeholder (never a literal `?`
/// inside a string), so a single left-to-right pass over the ORIGINAL `sql` is
/// correct — it never re-scans substituted text (a rendered value may contain `?`).
pub fn inline_params(sql: &str, params: &[SqlValue]) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut it = params.iter();
    for ch in sql.chars() {
        if ch == '?' {
            match it.next() {
                Some(p) => out.push_str(&render_literal(p)),
                None => out.push('?'),
            }
        } else {
            out.push(ch);
        }
    }
    out
}
```

- [ ] **Step 4: Run the unit test to verify it passes**

Run: `buck2 test //src/services/query-api:quack-inline > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass 1.` (one target; 4 test fns pass).

- [ ] **Step 5: rustfmt + commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/serving.rs src/services/query-api/tests/quack_inline.rs
git add src/services/query-api/src/serving.rs src/services/query-api/tests/quack_inline.rs src/services/query-api/BUCK
git commit -m "$(cat <<'EOF'
feat(query-api): inline_params — typed scalars to escaped SQL literals

The Quack path forwards SQL as a string (quack_query has no bind slot), so it
inlines params; the ' -> '' escape is the inline path's injection boundary.
Unit-tested server-free, incl. the evil-quote case.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: `QuackServingEngine` + `QuackServer` fixture + end-to-end smoke

Implement the engine and the hermetic serving process together (they are co-dependent), proving the wire works with a trivial query.

**Files:**
- Modify: `src/services/query-api/src/serving.rs` (add `QuackServingEngine`)
- Create: `src/services/query-api/tests/quack_serving.rs` (the `QuackServer` helper + smoke test)
- Modify: `src/services/query-api/BUCK` (add the `quack-serving` fixture target)

- [ ] **Step 1: Implement `QuackServingEngine`**

In `src/services/query-api/src/serving.rs`, add below `EmbeddedDuckDb`:

```rust
/// A Quack-protocol client `ServingEngine`: forwards SQL to a separate
/// `quack_serve`'d DuckDB (which owns the DuckLake `ATTACH`) via the `quack_query`
/// table function. The local duckdb-rs connection only `LOAD quack`s — no catalog
/// is attached client-side; execution happens on the serving tier.
pub struct QuackServingEngine {
    /// e.g. "quack:127.0.0.1:9494" — the server's listen URI (client form).
    uri: String,
    /// Auth token agreed with the server.
    token: String,
    /// Offline extension dir to `LOAD quack` from (DUCKDB_EXTENSION_DIR).
    ext_dir: String,
}

impl QuackServingEngine {
    pub fn new(uri: impl Into<String>, token: impl Into<String>) -> Result<Self, ServingError> {
        let ext_dir = std::env::var("DUCKDB_EXTENSION_DIR")
            .map_err(|_| ServingError::Engine("DUCKDB_EXTENSION_DIR unset".into()))?;
        Ok(Self { uri: uri.into(), token: token.into(), ext_dir })
    }
}

#[async_trait]
impl ServingEngine for QuackServingEngine {
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError> {
        // 1. Inline params into the compiled SQL (quack_query has no bind slot).
        // 2. Prefix `USE lake;` — a quack-forwarded session does NOT inherit the
        //    server's USE lake, so unqualified `"main"."orders"` must be set here.
        let forwarded = format!("USE lake; {}", inline_params(sql, params));
        // 3. Wrap in quack_query, embedding `forwarded` as a string literal (escaped
        //    once more for THIS literal — table-function args must be constant, so no
        //    bind). uri/token are loom-internal constants. Escaping composes: each
        //    layer is a DuckDB string literal, so doubling `'` round-trips exactly.
        let wrapper = format!(
            "SELECT * FROM quack_query('{}', '{}', token := '{}', disable_ssl := true)",
            self.uri,
            sql_escape(&forwarded),
            self.token,
        );
        // 4. Run via a local duckdb-rs Quack client; reuse run_sync's row-mapping.
        let preamble = format!("SET extension_directory='{}';\nLOAD quack;", self.ext_dir);
        tokio::task::spawn_blocking(move || run_sync(&preamble, &wrapper, &[]))
            .await
            .map_err(|e| ServingError::Engine(format!("join: {e}")))?
    }
}
```

- [ ] **Step 2: Write the `QuackServer` helper + the failing smoke test**

Create `src/services/query-api/tests/quack_serving.rs`:

```rust
//! Quack serving tier: a hermetic `quack_serve`'d DuckDB (QuackServer) over a
//! seeded DuckLake catalog, and the QuackServingEngine reading through it.
//! Proves parity with EmbeddedDuckDb (tests/serving_engine.rs covers the embedded
//! engine's own mechanics).

use std::io::Write;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use query_api::serving::{EmbeddedDuckDb, QuackServingEngine, ServingEngine, SqlValue};

const TOKEN: &str = "loom-test-quack-token";

/// A DuckDB process running `quack_serve` over a DuckLake catalog. Spawns the
/// pinned duckdb-cli (DUCKDB_BIN), holds its stdin open so the server thread keeps
/// running, and kills it on Drop.
struct QuackServer {
    child: Child,
    // Keep the stdin pipe open: closing it would EOF the CLI and stop the server.
    _stdin: std::process::ChildStdin,
    port: u16,
}

impl QuackServer {
    /// `socket`/`db`/`data_path` must match a bootstrapped DuckLakeWriter so the
    /// ATTACH'd DATA_PATH agrees with the catalog. Reads DUCKDB_BIN and
    /// DUCKDB_EXTENSION_DIR from the env (set by the loom_fixture_test rule).
    fn start(socket: &Path, db: &str, data_path: &Path) -> Self {
        let bin = PathBuf::from(std::env::var("DUCKDB_BIN").expect("DUCKDB_BIN"));
        let ext = std::env::var("DUCKDB_EXTENSION_DIR").expect("DUCKDB_EXTENSION_DIR");
        // Pick a free localhost port: bind :0, read it, drop the listener.
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
            l.local_addr().expect("addr").port()
        };
        let script = format!(
            "SET extension_directory='{ext}';\n\
             LOAD ducklake; LOAD postgres_scanner; LOAD quack;\n\
             ATTACH 'ducklake:postgres:dbname={db} host={sock} user=postgres' AS lake \
             (DATA_PATH '{data}/', DATA_INLINING_ROW_LIMIT 0);\n\
             USE lake;\n\
             SELECT listen_uri FROM quack_serve('quack://127.0.0.1:{port}', token := '{TOKEN}', disable_ssl := true);\n",
            sock = socket.display(),
            data = data_path.display(),
        );
        let mut child = Command::new(&bin)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn duckdb quack server");
        let mut stdin = child.stdin.take().expect("child stdin");
        stdin.write_all(script.as_bytes()).expect("write serve script");
        stdin.flush().expect("flush serve script");
        let server = Self { child, _stdin: stdin, port };
        server.wait_ready();
        server
    }

    fn wait_ready(&self) {
        for _ in 0..200 {
            if TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("quack server did not accept connections within ~10s");
    }

    /// Client-form URI for quack_query (single-colon scheme).
    fn uri(&self) -> String {
        format!("quack:127.0.0.1:{}", self.port)
    }
}

impl Drop for QuackServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Boot pg + bootstrap a DuckLake catalog + start a QuackServer over it. Returns
/// the pieces a test needs; keep `_pg`/`writer` alive (they own the cluster/dirs).
async fn harness() -> (PgFixture, DuckLakeWriter, QuackServer, String) {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let server = QuackServer::start(fx.socket_path(), &db, writer.data_path());
    let uri = server.uri();
    (fx, writer, server, uri)
}

#[tokio::test(flavor = "multi_thread")]
async fn quack_engine_executes_a_trivial_query_over_the_wire() {
    // `_server` keeps the serving process alive for the test's scope (Drop kills it);
    // `uri` is the client-form URI returned by harness(), `TOKEN` the shared token.
    let (_fx, _writer, _server, uri) = harness().await;
    let eng = QuackServingEngine::new(uri, TOKEN).expect("engine");
    let rows = eng.fetch_rows("SELECT 42 AS n, 'hi' AS s", &[]).await.unwrap();
    assert_eq!(rows.columns, vec!["n".to_string(), "s".to_string()]);
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], SqlValue::Int(42));
    assert_eq!(rows.rows[0][1], SqlValue::Text("hi".into()));
}
```

- [ ] **Step 3: Add the fixture test target and run to verify it fails (engine not yet compiling / wiring)**

In `src/services/query-api/BUCK`, add (uses the macro — it boots postgres + duckdb):

```python
loom_fixture_test(
    name = "quack-serving",
    crate = "quack_serving",
    srcs = ["tests/quack_serving.rs"],
    crate_root = "tests/quack_serving.rs",
    duckdb = True,
    deps = [
        ":query-api",
        "//src/control-plane/postgres:postgres",
        "//third-party:tokio",
    ],
)
```

(`loom_fixture_test` is already loaded at the top of this BUCK file from Task 4 of the fixture-routing work; if the `load(...)` line is absent, add `load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")` at the top.)

Run: `buck2 build //src/services/query-api:quack-serving 2>&1 | tail -20`
Expected: FAILS until `QuackServingEngine` (Step 1) is in place and the test uses `TOKEN` correctly. Fix the test line to `QuackServingEngine::new(uri, TOKEN)` per Step 2's note.

- [ ] **Step 4: Run the smoke test to verify it passes**

Run: `buck2 test //src/services/query-api:quack-serving > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|owned by user" /tmp/t.log`
Expected: `Tests finished: Pass 1.` and an `owned by user "<non-root>"` line (the fixtures ran local). If the server never binds, `wait_ready` panics — check the duckdb serve script.

- [ ] **Step 5: rustfmt + commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/serving.rs src/services/query-api/tests/quack_serving.rs
git add src/services/query-api/src/serving.rs src/services/query-api/tests/quack_serving.rs src/services/query-api/BUCK
git commit -m "$(cat <<'EOF'
feat(query-api): QuackServingEngine + hermetic QuackServer fixture

QuackServingEngine forwards SQL to a quack_serve'd DuckDB via quack_query
(USE lake prefix + inlined params); QuackServer spawns the pinned duckdb-cli
serving a DuckLake catalog. Smoke test proves a query round-trips over the wire.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Parity, zero-rows, and injection tests against a seeded catalog

Prove the core guarantee: `QuackServingEngine` returns the same `Rows` as `EmbeddedDuckDb`, and the inline path is injection-safe.

**Files:**
- Modify: `src/services/query-api/tests/quack_serving.rs` (add three tests + a small seed helper)

- [ ] **Step 1: Add a seed helper and the parity test**

Append to `src/services/query-api/tests/quack_serving.rs`:

```rust
/// Build an EmbeddedDuckDb against the same catalog the QuackServer serves.
async fn embedded(fx: &PgFixture, db: &str, data_path: &Path) -> EmbeddedDuckDb {
    EmbeddedDuckDb::attach(fx.socket_path(), db, data_path)
        .await
        .expect("attach embedded")
}

#[tokio::test(flavor = "multi_thread")]
async fn parity_with_embedded_on_a_seeded_read() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    // Seed lake.main.orders with two batches (3 rows total).
    writer
        .seed(
            "main",
            "orders",
            &[("id".into(), "INTEGER".into(), false), ("region".into(), "VARCHAR".into(), false)],
            &[2, 1],
        )
        .await;
    let server = QuackServer::start(fx.socket_path(), &db, writer.data_path());
    let emb = embedded(&fx, &db, writer.data_path()).await;
    let quack = QuackServingEngine::new(server.uri(), TOKEN).expect("engine");

    // A governed-style read with a bound filter value and a stable order.
    let sql = "SELECT \"id\" FROM \"main\".\"orders\" WHERE \"region\" = ? ORDER BY \"id\"";
    let params = [SqlValue::Text("x".into())]; // DuckLakeWriter seeds VARCHAR cols as 'x'

    let e = emb.fetch_rows(sql, &params).await.unwrap();
    let q = quack.fetch_rows(sql, &params).await.unwrap();
    assert_eq!(e, q, "QuackServingEngine must match EmbeddedDuckDb");
    assert_eq!(q.rows.len(), 3, "all three seeded rows match region 'x'");
}
```

Note on the seed: `DuckLakeWriter::seed(schema, table, columns, batches)` creates the table and inserts `batches` (each entry = a row count); integer columns count up from `i`, non-integer columns are the constant `'x'` cast to the type (see `fixture.rs`). So every `region` is `'x'`.

- [ ] **Step 2: Add the zero-rows and injection tests**

Append:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn zero_rows_still_reports_columns_over_quack() {
    let (_fx, _writer, _server, uri) = harness().await;
    let eng = QuackServingEngine::new(uri, TOKEN).expect("engine");
    let rows = eng.fetch_rows("SELECT 42 AS n, 'hi' AS s WHERE 1 = 0", &[]).await.unwrap();
    assert!(rows.rows.is_empty());
    assert_eq!(rows.columns, vec!["n".to_string(), "s".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn evil_text_param_is_inlined_safely() {
    // The inline path's analog of EmbeddedDuckDb's params_are_bound_not_interpolated:
    // a value carrying a quote + statement terminator must round-trip as DATA and
    // not execute. (Two escaping layers — inner literal + outer quack_query literal.)
    let (_fx, _writer, _server, uri) = harness().await;
    let eng = QuackServingEngine::new(uri, TOKEN).expect("engine");
    let evil = SqlValue::Text("x'; DROP TABLE lake.t; --".into());
    let rows = eng.fetch_rows("SELECT ? AS v", std::slice::from_ref(&evil)).await.unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], SqlValue::Text("x'; DROP TABLE lake.t; --".into()));
}
```

- [ ] **Step 3: Run the full quack-serving target**

Run: `buck2 test //src/services/query-api:quack-serving > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass 1.` (the target's 4 test fns — smoke, parity, zero-rows, injection — all pass).

- [ ] **Step 4: rustfmt + commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/tests/quack_serving.rs
git add src/services/query-api/tests/quack_serving.rs
git commit -m "$(cat <<'EOF'
test(query-api): QuackServingEngine parity + zero-rows + injection

Asserts byte-identical Rows vs EmbeddedDuckDb on a seeded read, columns reported
on a zero-row result, and an evil quote-bearing param round-trips as data (the
inline path's injection-safety proof).

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Full-suite verification

**Files:** none (verification only).

- [ ] **Step 1: Run the whole suite**

Run: `buck2 test //src/... > /tmp/full.log 2>&1; grep -E "Tests finished|FAIL|BUILD FAILED" /tmp/full.log`
Expected: `Tests finished: Pass <N>. Fail 0. … Build failure 0` — all targets pass (existing `serving-engine`/`governed-read` unaffected; new `quack-inline` + `quack-serving` green).

- [ ] **Step 2: Lint clean**

Run: `tools/clippy-all.sh 2>&1 | tail -5` and `buck2 run //tools:prek -- run --all-files 2>&1 | tail -20`
Expected: clippy clean; all prek hooks pass.

- [ ] **Step 3: No further commit** unless a hook auto-fixed whitespace (`git add -A && git commit -m "chore: lint"`).

---

## Notes for the implementer

- **Do not** modify `compile_select`, `EmbeddedDuckDb`, `sql.rs`, ACL, or the ontology — the Quack concern is intentionally localized to `QuackServingEngine`.
- `run_sync` (already in `serving.rs`) is reused verbatim by passing the quack preamble as `attach`, the `quack_query` wrapper as `sql`, and empty params — no refactor needed.
- The serving process spawns the pinned **duckdb-cli** (`DUCKDB_BIN`), not duckdb-rs; the **client** engine uses **duckdb-rs**. Both `LOAD quack` from the same `DUCKDB_EXTENSION_DIR` (now containing quack, from Task 1).
- `quack_serve` URI uses the `quack://host:port` form; the client (`quack_query`) uses the `quack:host:port` form (the server's reported `listen_uri`). Both are spike-confirmed.
- If `wait_ready` flakes, increase the poll bound — do NOT weaken the readiness check or the assertions.
