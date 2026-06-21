# Recursive-CTE-over-DataFusion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make loom's `WITH RECURSIVE` reachability SQL execute correctly through `DataFusionServingEngine` over Iceberg-mirror-backed tables, and prove it — closing `iss-recursive-cte-iceberg`.

**Architecture:** Implementation surfaced that the recursive-CTE `/graph` SQL does **not** run on DataFusion as written: the compilers rely on the SQL-standard CTE column-name list `reach(id, depth)` to alias the anchor term's columns, which DuckDB honors but **DataFusion ignores** (it names the anchor's `0` literal column `Int64(0)`, so the recursive term's `r.depth` reference fails to resolve — `Schema error: No field named r.depth`). The spec's premise that "DataFusion 54 executes the linear recursion unchanged — no compiler change is expected" is therefore wrong. The fix is small and portable: alias the anchor (and recursive) columns explicitly in all three graph compilers (`SELECT s.{id} AS id, 0 AS depth …`), which both engines accept. Then a new `loom_fixture_test` lands self-link graphs into the `iceberg_mirror` (real Parquet) and asserts the reachable sets through `DataFusionServingEngine` match the DuckDB `graph-reach-e2e` / `graph-union-e2e` expectations. A small fixture helper (`IcebergWriter::seed_arrays` + `SeedCol`) lands *explicit* graph edges (the auto-gen `seed` only emits sequential `0..N`).

**Tech Stack:** Rust, buck2 (`loom_fixture_test`), DataFusion 54, Iceberg (vendored `SqlCatalog`), hermetic Postgres fixture (`PgFixture`).

**Spec:** `docs/superpowers/specs/2026-06-17-iceberg-datafusion-serving-engine-design.md` (the "Recursive-CTE graph reads (`/graph`)" follow-up, ~lines 49–60 — premise corrected by this plan). Issue: `iss-recursive-cte-iceberg`.

**Working-tree note:** a prior implementer already created the FK integration test, the BUCK target, and the `seed_arrays`/`SeedCol`/`ensure_table` fixture changes (uncommitted). They are the TDD driver for Task 1: the FK test currently fails at runtime with the DataFusion `r.depth` schema error. Do **not** discard them.

---

## Orientation (read before starting)

- **Three compilers emit the same anchor shape** in `src/services/query-api/src/sql.rs`, all relying on the `reach(id, depth)` column list for the `depth` alias:
  - `compile_graph_reach` (anchor at ~line 820: `SELECT s.{id}, 0 FROM {tbl} s{seed_where}`; recursive at ~822: `SELECT nxt.{id}, r.depth + 1 …`).
  - `compile_graph_reach_union` (recursive at ~914: `SELECT e.to_id, r.depth + 1 …`; outer anchor at ~938: `SELECT s.{id}, 0 …`).
  - `recursive_reach_cte` (anchor + recursive at ~1000–1002: `SELECT s.{id}, 0 …` / `SELECT nxt.{id}, r.depth + 1 …`) — used by `compile_graph_reach_tail`.
- **No existing unit test asserts the un-aliased anchor form**, and the aliases add **no params**, so the `params.len()` asserts in `compile_graph_reach.rs` / `compile_graph_reach_union.rs` are unaffected. The portability change is low-blast-radius.
- **DuckDB regression coverage:** `graph-reach-e2e`, `graph-union-e2e`, `graph-tail-e2e`, `graph-reach`, `graph-reach-union`, `graph-reach-tail` (`src/services/query-api/tests/`) run the same SQL against DuckDB — they must stay green after the alias change (DuckDB accepts explicit aliases alongside the column list).
- **`DataFusionServingEngine`** — `serving_datafusion.rs`: `DataFusionServingEngine::new(catalog: IcebergCatalog)`, `async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError>`. Registers each live mirror table under `"schema"."table"`.
- **Types:** `query_api::sql::{DuckDbDialect, GraphStep, compile_graph_reach, compile_graph_reach_union}`; `query_api::filter::CallerPredicate { column, op, values }`; `query_api::serving::{Rows, SqlValue, ServingEngine}` (`Rows { columns: Vec<String>, rows: Vec<Vec<SqlValue>> }`); `control_plane_core::{CompareOp, LinkBacking, TableRef}`; `GraphStep { backing, next_table, next_filters }`; `LinkBacking::ForeignKey { from_column, to_column }` / `JoinTable { table, from_key, from_column, to_column, to_key }`. Call-shape template: `tests/compile_graph_reach.rs`.
- **Arrow version boundary:** `control-plane-postgres` uses `arrow-array` **57**; query-api uses `arrow` **58**. They are distinct, non-unifiable types — so the fixture helper accepts plain Rust (`SeedCol`) and builds arrays internally; the test crate needs no arrow dep.
- **CLAUDE.md rules:** never pipe `buck2 test` through `tail`/`head` (redirect to a file, then grep); tests are `loom_fixture_test`/`rust_test` integration targets (no inline `#[cfg(test)]`).

---

## Task 1: Make the recursive-CTE anchor portable (compiler fix)

Alias the anchor/recursive columns in all three graph compilers so the `depth` column resolves on DataFusion as well as DuckDB. TDD via a SQL-substring assertion per compiler, plus the (already-present) FK integration test as the end-to-end driver.

**Files:**
- Modify: `src/services/query-api/src/sql.rs`
- Modify: `src/services/query-api/tests/compile_graph_reach.rs`, `tests/compile_graph_reach_union.rs`, `tests/compile_graph_reach_tail.rs`

- [ ] **Step 1: Add the failing portability assertions**

In each of the three unit tests, add one assertion that the anchor aliases `depth` explicitly. Add to the FIRST test function in each file (after the existing `WITH RECURSIVE` assertion):

- `tests/compile_graph_reach.rs`, in `fk_self_link_recursive_reach` (after the `r.depth < 3` assert):
  ```rust
  assert!(
      sql.contains("0 AS depth"),
      "anchor aliases depth explicitly (portable to DataFusion, which ignores the CTE column list): {sql}"
  );
  ```
- `tests/compile_graph_reach_union.rs`, in its first test (after its `WITH RECURSIVE` assert):
  ```rust
  assert!(
      sql.contains("0 AS depth"),
      "anchor aliases depth explicitly (portable to DataFusion): {sql}"
  );
  ```
- `tests/compile_graph_reach_tail.rs`, in its first test (after its `WITH RECURSIVE` assert):
  ```rust
  assert!(
      sql.contains("0 AS depth"),
      "anchor aliases depth explicitly (portable to DataFusion): {sql}"
  );
  ```

- [ ] **Step 2: Run the unit tests to verify they FAIL**

Run: `buck2 test //src/services/query-api:compile-graph-reach //src/services/query-api:compile-graph-reach-union //src/services/query-api:compile-graph-reach-tail > /tmp/cg.log 2>&1; grep -E "Tests finished|FAIL|panicked" /tmp/cg.log`
Expected: failures on the new `0 AS depth` assertions (the anchor currently emits bare `, 0`).

- [ ] **Step 3: Apply the alias fix in `sql.rs`**

Make these exact edits (all aliases are unquoted `id` / `depth`, matching the `reach(id, depth)` column list):

1. In `compile_graph_reach`'s `format!` (the anchor and recursive lines):
   - `SELECT s.{id}, 0 FROM {tbl} s{seed_where}` → `SELECT s.{id} AS id, 0 AS depth FROM {tbl} s{seed_where}`
   - `SELECT nxt.{id}, r.depth + 1 FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}` → `SELECT nxt.{id} AS id, r.depth + 1 AS depth FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}`
2. In `compile_graph_reach_union`:
   - the `recursive` binding: `SELECT e.to_id, r.depth + 1 FROM reach r JOIN ({edges_sql}) e ON r.id = e.from_id JOIN {tbl} nxt ON e.to_id = nxt.{id} WHERE {rec_where}` → `SELECT e.to_id AS id, r.depth + 1 AS depth FROM reach r JOIN ({edges_sql}) e ON r.id = e.from_id JOIN {tbl} nxt ON e.to_id = nxt.{id} WHERE {rec_where}`
   - the outer `format!` anchor: `SELECT s.{id}, 0 FROM {tbl} s{seed_where}` → `SELECT s.{id} AS id, 0 AS depth FROM {tbl} s{seed_where}`
3. In `recursive_reach_cte`'s `format!`:
   - `SELECT s.{id}, 0 FROM {tbl} s{seed_where}` → `SELECT s.{id} AS id, 0 AS depth FROM {tbl} s{seed_where}`
   - `SELECT nxt.{id}, r.depth + 1 FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}` → `SELECT nxt.{id} AS id, r.depth + 1 AS depth FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}`

Leave everything else (the `reach(id, depth)` column list, projection `SELECT id FROM reach WHERE depth >= 1`, `r.depth < {depth}`) unchanged — the bare `id`/`depth` references still resolve against the CTE's declared columns.

- [ ] **Step 4: Run the unit tests to verify they PASS**

Run: `buck2 test //src/services/query-api:compile-graph-reach //src/services/query-api:compile-graph-reach-union //src/services/query-api:compile-graph-reach-tail > /tmp/cg.log 2>&1; grep -E "Tests finished|FAIL" /tmp/cg.log`
Expected: all pass.

- [ ] **Step 5: Verify the DuckDB graph e2es have NOT regressed**

Run: `buck2 test //src/services/query-api:graph-reach-e2e //src/services/query-api:graph-union-e2e //src/services/query-api:graph-tail-e2e //src/services/query-api:graph-reach //src/services/query-api:graph-reach-union //src/services/query-api:graph-reach-tail > /tmp/ge.log 2>&1; grep -E "Tests finished|FAIL" /tmp/ge.log`
Expected: all pass — DuckDB accepts the explicit aliases; behavior is unchanged.

- [ ] **Step 6: Commit (compiler fix only)**

Stage only the compiler + its unit tests (the integration test / fixture land in Task 2):
```bash
git add src/services/query-api/src/sql.rs src/services/query-api/tests/compile_graph_reach.rs src/services/query-api/tests/compile_graph_reach_union.rs src/services/query-api/tests/compile_graph_reach_tail.rs
git commit -m "fix(query-api): alias recursive-CTE anchor columns so /graph runs on DataFusion"
```

---

## Task 2: FK self-link recursive reach through DataFusion (+ `seed_arrays` helper)

The integration test + fixture helper already exist in the working tree (verify their contents match below). After Task 1 the FK test passes.

**Files:**
- Create (already in tree): `src/services/query-api/tests/recursive_cte_over_datafusion.rs`
- Modify (already in tree): `src/services/query-api/BUCK`
- Modify (already in tree): `src/control-plane/postgres/src/fixture.rs`

- [ ] **Step 1: Verify the test file matches**

`src/services/query-api/tests/recursive_cte_over_datafusion.rs` should contain (the FK case only at this point):

```rust
//! Recursive-CTE reachability (WITH RECURSIVE, emitted by compile_graph_reach /
//! compile_graph_reach_union) executed through DataFusionServingEngine over
//! Iceberg-mirror-backed tables — closing iss-recursive-cte-iceberg. The DuckDB
//! graph e2es (graph-reach-e2e / graph-union-e2e) prove the same reachable sets
//! against the DuckLake/DuckDB engine; this proves them against loom's own engine.

use control_plane_core::{CompareOp, LinkBacking, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use query_api::filter::CallerPredicate;
use query_api::serving::{Rows, ServingEngine, SqlValue};
use query_api::serving_datafusion::DataFusionServingEngine;
use query_api::sql::{DuckDbDialect, GraphStep, compile_graph_reach, compile_graph_reach_union};

/// Sorted `id` column values from a `Rows` whose projection is `("id", "name")`.
fn ids_of(rows: &Rows) -> Vec<i64> {
    let idx = rows
        .columns
        .iter()
        .position(|c| c == "id")
        .expect("id column present");
    let mut out: Vec<i64> = rows
        .rows
        .iter()
        .map(|r| match &r[idx] {
            SqlValue::Int(n) => *n,
            other => panic!("id column not Int: {other:?}"),
        })
        .collect();
    out.sort_unstable();
    out
}

fn person() -> TableRef {
    TableRef {
        schema: "graph".into(),
        name: "person".into(),
    }
}

// person(id, name, knows_id): FK self-link cycle 1->2->3->1. Reachability from
// {1} at depth 3 must visit 2, 3 and (via the cycle) 1 again, deduped — the same
// "cycle terminates + dedups" property graph-reach-e2e asserts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fk_self_link_recursive_reach_over_datafusion() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
        ("knows_id".to_string(), "long".to_string(), false),
    ];
    writer
        .seed_arrays(
            "graph",
            "person",
            &cols,
            &[
                SeedCol::Long(vec![1, 2, 3]),
                SeedCol::Str(vec!["ann", "bob", "cal"]),
                SeedCol::Long(vec![2, 3, 1]), // 1->2, 2->3, 3->1 (cycle)
            ],
        )
        .await;

    let engine = DataFusionServingEngine::new(IcebergCatalog::new(pool));

    let step = GraphStep {
        backing: LinkBacking::ForeignKey {
            from_column: "knows_id".into(),
            to_column: "id".into(),
        },
        next_table: person(),
        next_filters: vec![],
    };
    let seed = vec![CallerPredicate {
        column: "id".into(),
        op: CompareOp::In,
        values: vec![SqlValue::Int(1)],
    }];
    let (sql, params) = compile_graph_reach(
        &DuckDbDialect,
        &person(),
        "id",
        &[step],
        &seed,
        &[],
        &["id".to_string(), "name".to_string()],
        &[],
        3,
        100,
    )
    .expect("compile_graph_reach");
    assert!(
        sql.contains("WITH RECURSIVE reach(id, depth) AS"),
        "sanity: recursive CTE compiled: {sql}"
    );

    let rows = engine
        .fetch_rows(&sql, &params)
        .await
        .expect("fetch_rows over iceberg");

    assert_eq!(rows.columns, vec!["id", "name"]);
    assert_eq!(
        ids_of(&rows),
        vec![1, 2, 3],
        "FK cycle reachable from 1 (deduped, terminates): {rows:?}"
    );

    drop(writer);
}
```

(The `compile_graph_reach_union` import is unused until Task 3 — leave it; Task 3 adds the union test in the same commit window. If the build warns/errors on the unused import before Task 3, add `#[allow(unused_imports)]` is NOT acceptable — instead just proceed straight into Task 3 so the import is used, then commit Task 2+3 together. See Task 3 note.)

- [ ] **Step 2: Verify the BUCK target matches**

`src/services/query-api/BUCK`, immediately after the `datafusion-serving` target:
```starlark
loom_fixture_test(
    name = "recursive-cte-over-datafusion",
    crate = "recursive_cte_over_datafusion",
    srcs = ["tests/recursive_cte_over_datafusion.rs"],
    crate_root = "tests/recursive_cte_over_datafusion.rs",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Verify the fixture helper matches**

`src/control-plane/postgres/src/fixture.rs` should contain: (a) the extracted private `ensure_table(&self, catalog: &SqlCatalog, ns, name, columns)` (verbatim lift of `seed`'s create-if-absent block), (b) `seed` rewritten to call it, (c) the new `pub async fn seed_arrays(&self, ns, name, columns, data: &[SeedCol<'_>]) -> i64` (which builds the batch from `data.iter().map(SeedCol::to_array)` against the table's own arrow schema), and (d) the `pub enum SeedCol<'a> { Long(Vec<i64>), NullableLong(Vec<Option<i64>>), Str(Vec<&'a str>) }` with `fn to_array(&self) -> ArrayRef` building arrow-array-57 arrays. No new imports (all already in scope). If any of these is missing or wrong, fix it to match the descriptions in the Architecture/Orientation sections.

- [ ] **Step 4: Run the FK test → PASS**

Run: `buck2 test //src/services/query-api:recursive-cte-over-datafusion > /tmp/rcte.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/rcte.log`
Expected: `Tests finished: Pass 1. Fail 0.` (the Task 1 alias fix makes the recursive CTE resolve on DataFusion).

- [ ] **Step 5: Confirm `seed` didn't regress**

Run: `buck2 test //src/services/query-api:datafusion-serving //src/control-plane/postgres/... > /tmp/seed.log 2>&1; grep -E "Tests finished|FAIL" /tmp/seed.log`
Expected: all pass.

- [ ] **Step 6: Commit**

Proceed to Task 3 BEFORE committing if the unused-`compile_graph_reach_union`-import would error; otherwise commit now:
```bash
git add src/services/query-api/tests/recursive_cte_over_datafusion.rs src/services/query-api/BUCK src/control-plane/postgres/src/fixture.rs
git commit -m "test(iceberg): recursive-CTE reach over DataFusion (FK self-link) + seed_arrays"
```

---

## Task 3: Union-of-self-links recursive reach through DataFusion

Mirrors `graph-union-e2e`: a FK self-link (`knows_id`) unioned with a join-table self-link (`colleagues`) reaches more nodes than either alone. Exercises `compile_graph_reach_union`'s single-recursive-self-reference shape on DataFusion. (Adding this test consumes the `compile_graph_reach_union` import from Task 2.)

**Files:**
- Modify: `src/services/query-api/tests/recursive_cte_over_datafusion.rs`

- [ ] **Step 1: Append the union test function**

```rust
// person(id, name, knows_id) FK self-link + colleagues(a, b) join-table self-link.
// Edges — knows: 1->2, 2->3, 5->6 ; colleagues: 1->4, 6->5. From {1} at depth 3 the
// UNION of both links reaches {2, 3, 4} (knows alone gives {2,3}; colleagues adds the
// colleagues-only node 4) — the same "union adds colleagues-only node" property
// graph-union-e2e asserts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn union_self_links_recursive_reach_over_datafusion() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);

    let person_cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
        ("knows_id".to_string(), "long".to_string(), true),
    ];
    writer
        .seed_arrays(
            "graph",
            "person",
            &person_cols,
            &[
                SeedCol::Long(vec![1, 2, 3, 4, 5, 6]),
                SeedCol::Str(vec!["ann", "bob", "cal", "dee", "eve", "fin"]),
                // knows: 1->2, 2->3, 5->6 (3, 4, 6 have no FK out-edge)
                SeedCol::NullableLong(vec![Some(2), Some(3), None, None, Some(6), None]),
            ],
        )
        .await;

    let colleagues_cols = vec![
        ("a".to_string(), "long".to_string(), false),
        ("b".to_string(), "long".to_string(), false),
    ];
    writer
        .seed_arrays(
            "graph",
            "colleagues",
            &colleagues_cols,
            // colleagues: 1->4, 6->5
            &[SeedCol::Long(vec![1, 6]), SeedCol::Long(vec![4, 5])],
        )
        .await;

    let engine = DataFusionServingEngine::new(IcebergCatalog::new(pool));

    let backings = vec![
        LinkBacking::ForeignKey {
            from_column: "knows_id".into(),
            to_column: "id".into(),
        },
        LinkBacking::JoinTable {
            table: TableRef {
                schema: "graph".into(),
                name: "colleagues".into(),
            },
            from_key: "id".into(),
            from_column: "a".into(),
            to_column: "b".into(),
            to_key: "id".into(),
        },
    ];
    let seed = vec![CallerPredicate {
        column: "id".into(),
        op: CompareOp::In,
        values: vec![SqlValue::Int(1)],
    }];
    let (sql, params) = compile_graph_reach_union(
        &DuckDbDialect,
        &person(),
        "id",
        &backings,
        &seed,
        &[],
        &["id".to_string(), "name".to_string()],
        &[],
        3,
        100,
    )
    .expect("compile_graph_reach_union");
    assert!(
        sql.contains("WITH RECURSIVE reach(id, depth) AS"),
        "sanity: recursive union CTE compiled: {sql}"
    );

    let rows = engine
        .fetch_rows(&sql, &params)
        .await
        .expect("fetch_rows over iceberg");

    assert_eq!(rows.columns, vec!["id", "name"]);
    assert_eq!(
        ids_of(&rows),
        vec![2, 3, 4],
        "union reaches more than either link alone (colleagues adds node 4): {rows:?}"
    );

    drop(writer);
}
```

- [ ] **Step 2: Run the test → BOTH functions PASS**

Run: `buck2 test //src/services/query-api:recursive-cte-over-datafusion > /tmp/rcte.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/rcte.log`
Expected: `Tests finished: Pass 2. Fail 0.`

- [ ] **Step 3: Commit**

If Task 2's commit was deferred (unused-import), this single commit covers both; otherwise commit just the test file:
```bash
git add src/services/query-api/tests/recursive_cte_over_datafusion.rs
git commit -m "test(iceberg): recursive-CTE union reach over DataFusion (FK + join-table)"
```

---

## Task 4: Close the register item + correct the spec premise

**Files:**
- Modify: `docs/ISSUES.md`
- Modify: `docs/superpowers/specs/2026-06-17-iceberg-datafusion-serving-engine-design.md`

- [ ] **Step 1: Flip `iss-recursive-cte-iceberg` to fixed**

In `docs/ISSUES.md`, under `## iceberg`, change the `iss-recursive-cte-iceberg` entry to `- [x]`, `status:open`→`status:fixed`, add `pr:#N` (set the real number when the PR exists), and replace the prose:

```markdown
- [x] **Recursive-CTE /graph reads unverified on DataFusion engine** `{#iss-recursive-cte-iceberg area:iceberg status:fixed from:graph-traversal pr:#N spec:2026-06-17-iceberg-datafusion-serving-engine-design}`
  Fixed (PR #N): verifying the recursive-CTE `/graph` SQL on DataFusion revealed it did NOT run as emitted — DataFusion ignores the `reach(id, depth)` CTE column-name list, so the anchor's `0` literal stayed named `Int64(0)` and the recursive `r.depth` reference failed (`Schema error: No field named r.depth`). The three graph compilers (`compile_graph_reach`, `compile_graph_reach_union`, `recursive_reach_cte`) now alias the anchor/recursive columns explicitly (`SELECT s.id AS id, 0 AS depth …`), which both DuckDB and DataFusion accept. The new `recursive-cte-over-datafusion` `loom_fixture_test` lands self-link graphs into the `iceberg_mirror` (real Parquet, via the new `IcebergWriter::seed_arrays`) and asserts the same reachable sets as the DuckDB `graph-reach-e2e` / `graph-union-e2e` (FK cycle → `[1,2,3]`; FK ∪ join-table → `[2,3,4]`). The DuckDB graph e2es remain green.
```

Keep exactly one trailing newline, no trailing whitespace.

- [ ] **Step 2: Correct the spec's "no compiler change" note**

In `docs/superpowers/specs/2026-06-17-iceberg-datafusion-serving-engine-design.md`, in the "Recursive-CTE graph reads (`/graph`)" out-of-scope bullet, append one sentence recording the correction (do not rewrite the section):

```markdown
  **Update (resolved, PR #N):** the expectation that "no compiler change is expected" was wrong — DataFusion ignores the `reach(id, depth)` CTE column-name list, so the anchor columns had to be aliased explicitly (`0 AS depth`) in all three graph compilers for `r.depth` to resolve. The recursive-CTE-over-DataFusion test now passes; see `iss-recursive-cte-iceberg`.
```

- [ ] **Step 3: Validate the registers**

Run: `bash tools/docs.sh validate`
Expected: `OK (3 files)`.

- [ ] **Step 4: Commit**

```bash
git add docs/ISSUES.md docs/superpowers/specs/2026-06-17-iceberg-datafusion-serving-engine-design.md
git commit -m "docs(iceberg): close iss-recursive-cte-iceberg (recursive CTE fixed + verified on DataFusion)"
```

---

## Final verification

- [ ] `buck2 test //src/services/query-api:recursive-cte-over-datafusion > /tmp/rcte.log 2>&1; grep -E "Tests finished|FAIL" /tmp/rcte.log` → Pass 2, Fail 0.
- [ ] `buck2 test //src/services/query-api:graph-reach-e2e //src/services/query-api:graph-union-e2e //src/services/query-api:graph-tail-e2e //src/services/query-api:compile-graph-reach //src/services/query-api:compile-graph-reach-union //src/services/query-api:compile-graph-reach-tail > /tmp/ge.log 2>&1; grep -E "Tests finished|FAIL" /tmp/ge.log` → all pass (no DuckDB regression).
- [ ] `bash tools/clippy-all.sh` → clean.
- [ ] `bash tools/docs.sh validate` → `OK (3 files)`.
- [ ] Full `buck2 test //src/...` before opening the PR (CLAUDE.md backstop; first-party-only change, no `Cargo.toml`/reindeer change).
