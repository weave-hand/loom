# DuckLake Seams — WS2: SQL-Dialect Seam — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop hardwiring `query-api`'s SQL generation to DuckDB: introduce a `SqlDialect` seam that the SQL compiler consults for the dialect-variant tokens (identifier quoting, parameter placeholder, LIMIT clause), ship `DuckDbDialect` as the only dialect (byte-identical output today), and route the handler through the serving engine's dialect — so a future non-DuckDB serving engine adds a dialect impl, not a `sql.rs` rewrite.

**Architecture:** The DuckDB-specific tokens in `query-api/src/sql.rs` are a small, finite set. We add a `SqlDialect` trait + `DuckDbDialect` impl, thread `&dyn SqlDialect` through the compile functions (`compile_select`/`compile_chain` keep their public signatures and delegate to new `*_with(dialect, …)` variants defaulting to `DuckDbDialect`), and give `ServingEngine` a `dialect()` method (defaulting to `DuckDbDialect`) that the handler passes to the `*_with` compilers. Nothing observable changes today; the golden SQL tests stay byte-identical. A test-only `BacktickDialect` proves the seam is genuinely pluggable.

**Tech Stack:** Rust (edition 2024), buck2, the embedded DuckDB serving engine.

**Spec:** `docs/superpowers/specs/2026-06-16-ducklake-format-seams-design.md` (§Workstream 2). This is the SERVING-ENGINE axis — independent of WS1 (table-format axis); it touches only `src/services/query-api/`.

**Scope note (read before starting):** This is the workstream the originating spike flagged as the more speculative one (there is no second serving engine today — both `EmbeddedDuckDb` and `QuackServingEngine` speak DuckDB). The discipline here is **build only the seam, change nothing observable**. Do NOT add a second production dialect, do NOT abstract `op_sql` comparison operators or the aggregate spellings (`COUNT(*)`, `COALESCE(SUM(…),0)`, `AVG/MIN/MAX`) — those are ANSI-standard and shared across the engines we care about; widening the trait to cover them is speculative until a real dialect needs it. The trait covers exactly the three tokens that a non-DuckDB engine is most likely to differ on: identifier quoting, parameter placeholder, and the LIMIT clause.

---

## Conventions for this plan

- **Build the crate:** `buck2 build //src/services/query-api:query-api`.
- **Test (redirect + grep; NEVER pipe `buck2 test`/`bxl` to tail/head — it stalls):** `buck2 test //src/services/query-api:sql-compile > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|Pass" /tmp/t.log`. `buck2 build //src/... 2>&1 | tail -5` is fine.
- **No inline `#[test]`** in `src/**` (prek hook enforces) — unit tests go in `tests/<name>.rs` wired as a `rust_test` in `BUCK`.
- **rustfmt is check-only in the prek hook** — it fails the commit on a diff but does NOT rewrite. Run `buck2 run //tools:rustfmt -- --edition 2024 <files>` yourself before committing, or the commit/amend silently aborts (staged changes remain, SHA unchanged). After any `git commit`/`--amend`, VERIFY the SHA actually changed.
- **Stage by explicit path** — do NOT `git add -A` (there is an unrelated uncommitted `docs/TO_BE_PLANNED.md` and an untracked `.claude/` that must never be committed). After staging, run `git status` and confirm `docs/TO_BE_PLANNED.md` is NOT staged.
- Commit with `git commit -F - <<'EOF' … EOF` heredoc.

---

## File Structure

**`src/services/query-api/src/sql.rs`** — add `SqlDialect` trait + `DuckDbDialect`; thread `&dyn SqlDialect` through the private helpers (`col_ref`, `filter_sql`, `derived_aggregate_sql`) and the compile functions. The free `quote_ident` becomes `dialect.quote_ident`; the literal `"?"` becomes `dialect.placeholder(params.len())`; the ` LIMIT {n}` becomes `dialect.limit_clause(n)`. Public `compile_select`/`compile_chain` keep their signatures and delegate to new `compile_select_with`/`compile_chain_with`.

**`src/services/query-api/src/serving.rs`** — add `ServingEngine::dialect(&self) -> &'static dyn SqlDialect` with a default returning `&DuckDbDialect`. All existing impls inherit it.

**`src/services/query-api/src/handler.rs`** — route the two compile call sites through the engine dialect: `compile_select_with(deps.serving.dialect(), …)` and `compile_chain_with(deps.serving.dialect(), …)`.

**`src/services/query-api/src/lib.rs`** — export `SqlDialect`, `DuckDbDialect`, `compile_select_with`, `compile_chain_with`.

**`src/services/query-api/tests/sql_dialect.rs`** *(new)* — a test-only `BacktickDialect` proving `compile_select_with`/`compile_chain_with` consult the dialect (quote char + placeholder swap), plus a check that `compile_select` (the default) equals `compile_select_with(&DuckDbDialect, …)`.

**`src/services/query-api/tests/sql_compile.rs`** — UNCHANGED (the DuckDB golden oracle; it calls the default `compile_select`/`compile_chain`, which must stay byte-identical).

**`src/services/query-api/BUCK`** — add the `sql-dialect` `rust_test` target.

---

## Task 1 — `SqlDialect` trait + `DuckDbDialect`, threaded through the compiler

**Files:**
- Modify: `src/services/query-api/src/sql.rs`
- Modify: `src/services/query-api/src/lib.rs`
- Create: `src/services/query-api/tests/sql_dialect.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Add the trait and the DuckDB impl at the top of `sql.rs`** (after the imports, before `MASK_MARKER`).

```rust
/// The dialect-variant tokens of the read-SQL the compiler emits. The compiler is
/// otherwise dialect-neutral (ANSI joins/predicates); only these three knobs differ
/// across the serving engines loom might target. Extend the trait only when a real
/// dialect needs a token that is currently hardcoded (e.g. an aggregate spelling).
pub trait SqlDialect: Send + Sync {
    /// Quote a (trusted, ontology/ACL-derived) identifier.
    fn quote_ident(&self, id: &str) -> String;
    /// The placeholder for the `one_based`-th bound parameter. DuckDB ignores the
    /// index (`?`); a positional dialect would render `$1`, `$2`, ….
    fn placeholder(&self, one_based: usize) -> String;
    /// The trailing row-limit clause (no leading space added by the dialect).
    fn limit_clause(&self, limit: u32) -> String;
}

/// The DuckDB dialect — loom's only serving dialect today. Identifiers are
/// double-quoted, parameters are positional-`?`, LIMIT is `LIMIT n`.
pub struct DuckDbDialect;

impl SqlDialect for DuckDbDialect {
    fn quote_ident(&self, id: &str) -> String {
        assert!(
            !id.contains('"'),
            "identifier must not contain a double quote: {id}"
        );
        format!("\"{id}\"")
    }
    fn placeholder(&self, _one_based: usize) -> String {
        "?".to_string()
    }
    fn limit_clause(&self, limit: u32) -> String {
        format!("LIMIT {limit}")
    }
}
```

- [ ] **Step 2: Replace the free `quote_ident`/`col_ref` helpers to take the dialect.**

Delete the free `fn quote_ident(id: &str) -> String` (its assertion now lives in `DuckDbDialect::quote_ident`). Change `col_ref` to:
```rust
fn col_ref(dialect: &dyn SqlDialect, alias: &str, id: &str) -> String {
    if alias.is_empty() {
        dialect.quote_ident(id)
    } else {
        format!("{alias}.{}", dialect.quote_ident(id))
    }
}
```
Every existing `quote_ident(x)` call elsewhere in the file becomes `dialect.quote_ident(x)`; every `col_ref(alias, id)` becomes `col_ref(dialect, alias, id)`. (You will thread `dialect` into the functions that call these in the next steps.)

- [ ] **Step 3: Thread `dialect` through `filter_sql` and replace the placeholder.**

Change the signature to `fn filter_sql(dialect: &dyn SqlDialect, f: &RowFilter, alias: &str, params: &mut Vec<SqlValue>) -> String` and, at every point a parameter is bound, emit the dialect placeholder using the post-push 1-based index. Concretely:
- The `In`/`NotIn` arm: `for it in items { scalar(it, params); placeholders.push(dialect.placeholder(params.len())); }` (was `placeholders.push("?")`).
- The default compare arm: `scalar(value, params); format!("({} {} {})", col_ref(dialect, alias, property), op_sql(*op), dialect.placeholder(params.len()))` (was the literal `?`).
- `IsNull`/`IsNotNull` arms: `col_ref(dialect, alias, property)` (no placeholder; unchanged otherwise).
- The `And`/`Or`/`Not` recursive arms: pass `dialect` into the recursive `filter_sql` calls.
- `op_sql` is UNCHANGED (comparison operators are dialect-invariant).

- [ ] **Step 4: Thread `dialect` through `derived_aggregate_sql`.**

Change the signature to `fn derived_aggregate_sql(dialect: &dyn SqlDialect, d: &DerivedAggregate, params: &mut Vec<SqlValue>) -> String`. Replace every `quote_ident(x)` with `dialect.quote_ident(x)`, and pass `dialect` into the `filter_sql(dialect, f, "sub", params)` call. The aggregate spellings (`COUNT(*)`, `COALESCE(SUM(sub.…), 0)`, `AVG/MIN/MAX`) stay hardcoded (ANSI; out of scope per the scope note).

- [ ] **Step 5: Convert `compile_select` into `compile_select_with` + a default wrapper.**

Rename the existing `pub fn compile_select(...)` body to:
```rust
pub fn compile_select_with(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    allowed_cols: &[String],
    mask_cols: &[String],
    row_filters: &[RowFilter],
    eq_filters: &[(String, SqlValue)],
    derived: &[DerivedSelect],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    // ... existing body, with these substitutions:
    //  - quote_ident(c)                  -> dialect.quote_ident(c)
    //  - filter_sql(f, "", &mut params)  -> filter_sql(dialect, f, "", &mut params)
    //  - derived_aggregate_sql(a, ...)   -> derived_aggregate_sql(dialect, a, ...)
    //  - the eq_filters conjunct: format!("({} = {})", dialect.quote_ident(col), dialect.placeholder(params.len()))
    //      (push the value FIRST so params.len() is the 1-based index), e.g.:
    //        params.push(val.clone());
    //        conjuncts.push(format!("({} = {})", dialect.quote_ident(col), dialect.placeholder(params.len())));
    //  - the masked-column marker line: format!("'{MASK_MARKER}' AS {}", dialect.quote_ident(c))
    //  - the FROM identifiers: dialect.quote_ident(&table.schema)/(&table.name)
    //  - the trailing limit: replace `sql.push_str(&format!(" LIMIT {limit}"));`
    //      with `sql.push_str(&format!(" {}", dialect.limit_clause(limit)));`
}
```
Then add the backwards-compatible default wrapper:
```rust
/// Compile a governed SELECT for loom's default (DuckDB) dialect.
pub fn compile_select(
    table: &TableRef,
    allowed_cols: &[String],
    mask_cols: &[String],
    row_filters: &[RowFilter],
    eq_filters: &[(String, SqlValue)],
    derived: &[DerivedSelect],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    compile_select_with(
        &DuckDbDialect,
        table,
        allowed_cols,
        mask_cols,
        row_filters,
        eq_filters,
        derived,
        limit,
    )
}
```
IMPORTANT: keep the eq_filters param order identical to today (value pushed, placeholder emitted) so positional alignment and the golden strings are unchanged. The existing code does `conjuncts.push(format!("({} = ?)", quote_ident(col))); params.push(val.clone());` — reorder to push the value first, THEN format with `dialect.placeholder(params.len())`, so the rendered token for DuckDB stays `?` and the param vector is identical.

- [ ] **Step 6: Convert `compile_chain` the same way.**

Rename the body to `pub fn compile_chain_with(dialect: &dyn SqlDialect, types: &[ChainType], hops: &[LinkBacking], allowed_cols: &[String], mask_cols: &[String], limit: u32) -> Result<(String, Vec<SqlValue>), CompileError>` with the same substitutions: the `tbl` closure uses `dialect.quote_ident`; the join `ON`/projection identifiers use `dialect.quote_ident`; per-position `eq_filters` emit `dialect.placeholder(params.len())` after pushing; `filter_sql(dialect, f, &a, &mut params)`; the trailing limit uses `dialect.limit_clause(limit)`. Add the default wrapper:
```rust
pub fn compile_chain(
    types: &[ChainType],
    hops: &[LinkBacking],
    allowed_cols: &[String],
    mask_cols: &[String],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    compile_chain_with(&DuckDbDialect, types, hops, allowed_cols, mask_cols, limit)
}
```

- [ ] **Step 7: Export the new public items.**

In `src/services/query-api/src/lib.rs`, add to the `sql` re-export (match the existing style — find how `compile_select`/`compile_chain` are surfaced; they are used as `query_api::sql::compile_select` in tests, so the `sql` module is `pub`). Ensure `SqlDialect`, `DuckDbDialect`, `compile_select_with`, `compile_chain_with` are reachable as `query_api::sql::*` (they are `pub` in the `pub mod sql`, so no change may be needed — verify by reading lib.rs).

- [ ] **Step 8: Run the golden SQL tests — they MUST stay byte-identical.**

Run: `buck2 test //src/services/query-api:sql-compile > /tmp/golden.log 2>&1; grep -E "Tests finished|FAIL|Pass" /tmp/golden.log`
Expected: all PASS, unchanged. If any golden string differs, you changed observable output — fix the threading (most likely an eq_filter ordering or a stray space around the LIMIT clause) until the strings match exactly.

- [ ] **Step 9: Add the seam-proving test.**

Create `src/services/query-api/tests/sql_dialect.rs`:
```rust
use control_plane_core::{CompareOp, RowFilter, ScalarValue, TableRef};
use query_api::serving::SqlValue;
use query_api::sql::{DuckDbDialect, SqlDialect, compile_select, compile_select_with};

/// A second, test-only dialect: backtick identifiers and `$N` positional params,
/// `LIMIT n` unchanged. Proves the compiler genuinely consults the dialect.
struct BacktickDialect;
impl SqlDialect for BacktickDialect {
    fn quote_ident(&self, id: &str) -> String {
        format!("`{id}`")
    }
    fn placeholder(&self, one_based: usize) -> String {
        format!("${one_based}")
    }
    fn limit_clause(&self, limit: u32) -> String {
        format!("LIMIT {limit}")
    }
}

fn t() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "orders".into(),
    }
}

#[test]
fn default_compile_select_equals_explicit_duckdb() {
    let f = RowFilter::Compare {
        property: "status".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("open".into()),
    };
    let a = compile_select(&t(), &["id".into()], &[], std::slice::from_ref(&f), &[], &[], 100).unwrap();
    let b = compile_select_with(
        &DuckDbDialect,
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&f),
        &[],
        &[],
        100,
    )
    .unwrap();
    assert_eq!(a, b, "the default wrapper must equal explicit DuckDbDialect");
}

#[test]
fn dialect_controls_quoting_and_placeholders() {
    let f = RowFilter::Compare {
        property: "status".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("open".into()),
    };
    let (sql, params) = compile_select_with(
        &BacktickDialect,
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&f),
        &[],
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT `id` FROM `main`.`orders` WHERE (`status` = $1) LIMIT 100"
    );
    assert_eq!(params, vec![SqlValue::Text("open".into())]);
}
```

In `src/services/query-api/BUCK`, add a `rust_test` named `sql-dialect` mirroring the `sql-compile` target's wiring (`srcs`/`crate_root = ["tests/sql_dialect.rs"]`, `crate = "sql_dialect"`, same `deps` as `sql-compile` — look at the `sql-compile` target and copy its deps).

- [ ] **Step 10: Run the new test + build the crate.**

Run: `buck2 test //src/services/query-api:sql-dialect > /tmp/seam.log 2>&1; grep -E "Tests finished|FAIL|Pass" /tmp/seam.log`
Expected: PASS (proves the seam is live and the default equals explicit DuckDB).
Run: `buck2 build //src/services/query-api:query-api 2>&1 | tail -3` — expect success.

- [ ] **Step 11: Format, then commit.**

```bash
buck2 run //tools:rustfmt -- --edition 2024 src/services/query-api/src/sql.rs src/services/query-api/src/lib.rs src/services/query-api/tests/sql_dialect.rs
git add src/services/query-api/src/sql.rs src/services/query-api/src/lib.rs src/services/query-api/tests/sql_dialect.rs src/services/query-api/BUCK
git status   # confirm docs/TO_BE_PLANNED.md is NOT staged
git commit -F - <<'EOF'
feat(query-api): SqlDialect seam — SQL generation no longer hardwired to DuckDB

Introduce a SqlDialect trait (quote_ident / placeholder / limit_clause) consulted by
the read-SQL compiler; ship DuckDbDialect as the only dialect (byte-identical output —
the sql_compile golden tests are unchanged). compile_select/compile_chain keep their
signatures and delegate to compile_select_with/compile_chain_with(dialect, ...). A
test-only BacktickDialect proves the seam is genuinely pluggable.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```
VERIFY the commit SHA changed (`git rev-parse --short HEAD`); if rustfmt re-flagged a file the commit aborts silently.

---

## Task 2 — Wire the dialect through the serving engine + handler

**Files:**
- Modify: `src/services/query-api/src/serving.rs`
- Modify: `src/services/query-api/src/handler.rs`

- [ ] **Step 1: Give `ServingEngine` a default `dialect()`.**

In `src/services/query-api/src/serving.rs`, add to the `ServingEngine` trait (alongside `fetch_rows`) a defaulted method. Add the import `use crate::sql::{DuckDbDialect, SqlDialect};` at the top.
```rust
#[async_trait]
pub trait ServingEngine: Send + Sync {
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError>;

    /// The SQL dialect this engine speaks. Defaults to DuckDB — every serving engine
    /// loom ships today (`EmbeddedDuckDb`, `QuackServingEngine`) is DuckDB-compatible.
    /// A future non-DuckDB engine overrides this.
    fn dialect(&self) -> &'static dyn SqlDialect {
        &DuckDbDialect
    }
}
```
(`&DuckDbDialect` is a constant expression — Rust static-promotes it to `&'static`, so no `static` item is needed. If the borrow checker objects, add `static DUCKDB: DuckDbDialect = DuckDbDialect;` near the trait and return `&DUCKDB`.)

- [ ] **Step 2: Route the handler's two compile sites through the engine dialect.**

In `src/services/query-api/src/handler.rs`:
- Change the import (line ~11) to `use crate::sql::{compile_chain_with, compile_select_with};`.
- At the `compile_select` call (~line 230): `let (sql, params) = compile_select_with(deps.serving.dialect(), table, …)?;` — pass `deps.serving.dialect()` as the new first argument, keeping all other arguments exactly as they are.
- At the `compile_chain` call (~line 453): `let (sql, params) = compile_chain_with(deps.serving.dialect(), &ctypes, &hops, &to_allowed, &to_mask_cols, DEFAULT_LIMIT)?;`.

- [ ] **Step 3: Build + run the query-api fixture/e2e tests.**

Run: `buck2 build //src/... 2>&1 | tail -3` — expect success.
Run: `buck2 test //src/services/query-api:sql-compile //src/services/query-api:sql-dialect //src/services/query-api:governed-read //src/services/query-api:bind-read-e2e //src/services/query-api:link-traversal //src/services/query-api:derived-properties-e2e //src/services/query-api:multi-hop-traversal-e2e //src/services/query-api:typed-filter-e2e > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|Pass" /tmp/t2.log`
Expected: all PASS. The governed-read + traversal e2e tests passing proves the handler still produces working SQL through the dialect seam (observably unchanged — the engine's dialect is DuckDB).

- [ ] **Step 4: Format + commit.**

```bash
buck2 run //tools:rustfmt -- --edition 2024 src/services/query-api/src/serving.rs src/services/query-api/src/handler.rs
git add src/services/query-api/src/serving.rs src/services/query-api/src/handler.rs
git status   # confirm docs/TO_BE_PLANNED.md NOT staged
git commit -F - <<'EOF'
feat(query-api): route the handler's SQL compilation through the engine's dialect

ServingEngine gains a defaulted dialect() (DuckDB); the governed read + chain
handlers compile via compile_select_with/compile_chain_with(serving.dialect(), ...),
so the dialect is engine-selected rather than hardwired. Output is unchanged today
(the only dialect is DuckDB); a future non-DuckDB engine overrides dialect().

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```
VERIFY the SHA changed.

---

## Task 3 — Whole-tree verification

**Files:** none (verification only).

- [ ] **Step 1: Full build.** `buck2 build //src/... 2>&1 | tail -5` — expect success.
- [ ] **Step 2: Full test sweep.** `buck2 test //src/... > /tmp/ws2-all.log 2>&1; grep -E "Tests finished|FAILED" /tmp/ws2-all.log` — expect `Fail 0`. (If a failure appears, read `/tmp/ws2-all.log`.)
- [ ] **Step 3: Clippy.** `./tools/clippy-all.sh > /tmp/clippy.log 2>&1; grep -ciE "warning:|error" /tmp/clippy.log` — expect `0`.
- [ ] **Step 4: prek.** `buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -E "Passed|Failed" /tmp/prek.log` — expect all Passed. If prek rewrote files, `git add <them by path>` and amend the last task commit (then verify SHA changed).

---

## Self-Review (run before handing off)

**Spec coverage (§Workstream 2):**
- `SqlDialect` trait covering the dialect-variant tokens → Task 1 Steps 1-6. ✓
- `DuckDbDialect` only, byte-identical → Task 1 Step 1 + the unchanged `sql_compile` golden tests (Step 8). ✓
- `ServingEngine::dialect()` so engine + dialect can't desync → Task 2 Step 1. ✓
- Handler routes through the engine dialect → Task 2 Step 2. ✓
- Seam genuinely pluggable (not a no-op) → Task 1 Step 9's `BacktickDialect` test. ✓
- Scope discipline (no second production dialect, op_sql/aggregates untouched) → enforced by the scope note + Steps 3/4. ✓

**Type consistency:** `SqlDialect`, `DuckDbDialect`, `compile_select_with`, `compile_chain_with`, `ServingEngine::dialect` — names used identically across tasks.

**Observable-no-change invariant:** the `sql_compile.rs` golden tests are never edited; their continued passing is the proof WS2 changed nothing for DuckDB.
