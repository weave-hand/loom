# road-qa-read-path-consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Behavior-preserving consolidation of query-api's read path: sql.rs's
reach-family compilers reuse the helpers they re-inline and take a `ReachSpec<'a>`
params struct (deleting five `#[allow(too_many_arguments)]`); the three panic-y
`#[expect(indexing_slicing)]` in `caller_predicate_sql` become fallible slice
patterns; the three parallel graph handler entry points collapse onto one
`GraphReadSpec` spine over the merged governed-read layer (`resolve_governed` /
`GovernedType` / `Projection`); http.rs gets a reserved-param splitter
(`query_params`), a `parse_graph_mode` route selector, one **total**
`QueryError → Response` mapping, `AppState::deps()`, and a shared
`respond_shaped` chain tail; flight_export's duplicated governed-read block
becomes one helper.

**Register drift (corrections to the ROADMAP prose, verified against the tree
2026-07-02 — several sibling PRs landed since the audit census):**

- **sql.rs has SEVEN `#[allow(too_many_arguments)]`, not six** (`sql.rs:451,
  505, 921, 979, 1062, 1170, 1236`). Five are the reach family
  (`compile_graph_reach` :921, `compile_graph_tree` :979,
  `compile_graph_reach_union` :1062, `recursive_reach_cte` :1170,
  `compile_graph_reach_tail` :1236) — `ReachSpec` deletes those five. The other
  two (`compile_select_with` :451, `compile_select` :505) are the flat-SELECT
  builder family with a different parameter shape (predicates / or_groups /
  derived / order_by); they are **out of scope** (a `SelectSpec` is a separate
  seam with a much wider test-file ripple and no register mandate). The Task 10
  close prose records this.
- **`AppState`'s `QueryDeps` literal is hand-built 8×, not 7** (`http.rs:203,
  329, 402, 782, 818, 853, 893, 1051`).
- **The census pair `http.rs:404-430 ≈ 487-513` has moved**: it is now the
  `get_linked` tail (`http.rs:329-351`) ≈ `get_linked_chain` tail
  (`http.rs:402-421`). Same duplication, new lines.
- **The tail path's "`final_*` fold" is now a `final_g: GovernedType` fold**
  (`handler.rs:1353,1371`) — post-#299 it folds the whole governed type, not
  three loose `final_type/final_denied/final_masked` locals. Pinned by
  `tests/graph_tail_e2e.rs` (final-type projection + identity-keyed dedup).
- **A fifth partial `QueryError` mapping exists**: besides the register's four
  (`get_object` paginated `http.rs:239-245`, `get_object` plain :261-266,
  `chain_error` :467-479, `graph_error` :756-768), `post_search` (:1073-1081)
  partially maps `QueryError` with `Serving(NoIndex)`/`Serving(DimMismatch)`
  special cases. The total mapping absorbs all five.
- **`masked_col_exprs` is re-inlined 3×, not 2×**: `compile_chain_with`
  (sql.rs:657-666) carries the identical pattern alongside the union
  (:1128-1138) and tail (:1289-1298) copies. All three switch to the helper.
- **`reach_recursive_where` and `validate_reach_filters` are also re-inlined**
  (their empty-path degenerate) in the union (:1118-1122, :1078-1080) and the
  CTE (:1206-1210, :1184-1186) — the same dedup pass covers them.
- None of the recently merged parallel PRs (#291 lineage-naming, #240
  vector-index auto-rebuild, #236 transform-write-tuning spec, #297-#309)
  collapsed any of this item's targets — every duplication above was re-verified
  in the current tree.

**Architecture (key decisions, verified against the tree):**

- **`ReachSpec<'a>` (sql.rs) carries the reach family's common core** —
  `table`, `identity`, `seed_predicates`, `row_filters`, `allowed_cols`,
  `mask_cols`, `depth` — and deliberately **excludes `limit`**:
  `compile_graph_tree` emits no LIMIT by design (a LIMIT could orphan a child),
  and a spec field silently ignored by one consumer is a trap. `limit` stays an
  explicit argument of the three LIMIT-emitting compilers. Resulting arities:
  reach 4, tree 3, union 4, tail 7, cte 4 — all at/under clippy's
  threshold, so all five `#[allow]`s are deleted.
- **Graph handler collapse = borrow enum + shared spine, wrappers kept.**
  `GraphReadSpec<'q> { PathCycle(&'q GraphQuery), UnionSelfLinks(&'q
  GraphUnionQuery), CoreTail(&'q GraphTailQuery) }` dispatches through one
  `read_graph_reach_spec` (shared fetch + `Projection::into_object_rows`
  epilogue); the duplicated resolve+identity prologue becomes
  `governed_identity`. The three existing `pub` entry points
  (`read_graph_reach`, `read_graph_reach_union`, `read_graph_reach_with_tail`)
  become one-line delegates so the fake-backed handler tests
  (`tests/graph_reach.rs`, `graph_reach_union.rs`, `graph_reach_tail.rs`,
  `graph_tree.rs`) stay **byte-unmodified**. `read_graph_tree` stays separate
  (it returns `ObjectTree`, not `ObjectRows`).
- **`parse_graph_mode` (path_parse.rs) owns `/graph`'s mode grammar** —
  path-vs-links exclusivity, the `*` recursive-core rules, the tree
  restrictions — returning `GraphMode::{PathCycle, Union, CoreTail}` or the
  byte-identical 400 message. This plus `GraphReadSpec` is what takes
  `get_graph_path` (cc 25, http.rs:607-753) down to a scrape + one match.
- **The total `QueryError → Response` mapping lives in http.rs**
  (`pub fn query_error_response(e, context)`), NOT on `QueryError` itself —
  handler.rs stays transport-agnostic (its module contract). The match has
  **no catch-all over `QueryError` variants** (a new variant fails
  compilation there); `Serving` maps `NoIndex → 404`, `DimMismatch → 400`,
  `Engine → 500`. Reachability audit: `NoIndex`/`DimMismatch` are constructed
  only in `engine_client.rs:74-75` (the vector-search RPC), and `read_object`
  cannot emit `UnknownLink`/`BadChain`/`NotCyclicPath`/`BadGraphPath`
  (verified over `compile_object_read_with`), so unioning the five partial
  copies changes exactly ONE constructible response — `/search`'s
  `BadFilterValue` 500→400, whitelisted in §4 — plus defensive dead arms.
- **`query_params` is a new pure module** (mirroring `path_parse`'s
  socket-free pattern): `split_reserved(params, keys)` (per-route reserved-key
  sets — a universal set would silently consume `_direction` on `/objects`,
  changing its 400), `parse_ids`, `parse_depth`, `comma_list`. Single-valued
  keys read the LAST occurrence — exactly the loop-overwrite the routes
  hand-roll today.
- **Slice patterns make `caller_predicate_sql` total** and therefore fallible
  (`-> Result<String, CompileError>`); the ripple is contained to sql.rs
  (`select_where_conjuncts`, `chain_from_where`, `reach_seed_where` become
  fallible; every public compiler already returns `Result`). Reuses
  `CompileError::MalformedFilter` (no new variant → zero exhaustive-match
  risk); the arity violation surfaces as the existing opaque 500 instead of a
  request-thread panic.
- **flight_export**: `FlightExportService::governed(&self, cmd, subject)` is
  the one compile-the-governed-read both Flight verbs share (`get_flight_info`
  passes `cmd.clone()` — it still needs `cmd.encode()` for the ticket).

**Tech Stack:** Rust (edition 2024), buck2, `loom_rust_test` (pure targets; two
new: `query-params`, `query-error-http`) + existing `loom_fixture_test` e2e
targets (hermetic Postgres). No third-party dep changes, no `Cargo.toml` /
lockfile / `.sqlx` changes, no BUCK changes beyond the two new test targets.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md`
  (§ "road-qa-read-path-consolidation"), with the register-drift corrections
  above. Register: `docs/ROADMAP.md#road-qa-read-path-consolidation`.
- **Behavior-preserving:** wire responses, SQL text, error bodies, and status
  codes stay byte-identical except the four whitelisted changes below. The
  chain/select compile tests assert **exact SQL strings**, but the graph-reach
  family's existing tests are **fragment (`contains`) pins only** — and both
  union tests pass empty `mask_cols`, so the masked branch Task 1 replaces has
  no compile-level coverage. Task 1 therefore FIRST adds two exact full-SQL
  pins (union with a masked projection; tail pinning the whole
  `recursive_reach_cte` text), verified green against the current compiler,
  and only then substitutes the helpers under them.
- **Existing e2e tests pass unmodified.** The ONLY test files this plan may
  touch are: the six compile-test files in Task 3 (call **shape** only —
  every assertion stays byte-identical), plus appends to
  `tests/compile_graph_reach_union.rs` + `tests/compile_graph_reach_tail.rs`
  (Task 1's exact-SQL pins), `tests/sql_compile.rs` (Task 2) and
  `tests/path_parse.rs` (Task 6), and the two new test files.
  `tests/e2e_support.rs` is not touched.
- **TDD:** every new fn/type lands with its test written first and observed
  red; pure-refactor tasks run their pinning suite green before AND after.
- Tests are separate `rust_test` targets wired in
  `src/services/query-api/BUCK` — never inline `#[cfg(test)]` (the
  `no-inline-tests` hook enforces this).
- Clippy pedantic+restriction is on for prod code: no
  unwrap/expect/panic/indexing in `src/**.rs`. This item **deletes** three
  `#[expect(indexing_slicing)]` and six `#[allow(too_many_arguments)]`
  (5 sql.rs + 1 http.rs) — do NOT add new expects without a documented reason.
- **Never pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to
  a file and grep it. Fixture (e2e) runs use `-j 8` (postgres boot-slot
  starvation). Cloud sessions: `buck2 build -M none` for any whole-tree build;
  scope tests to the targets each task lists.
- `buck2 run //tools:prek -- run --all-files` must report zero `Failed`
  before **every** commit (rustfmt is a separate hook from clippy).
  Conventional Commits messages; one commit per task.
- `MAX_GRAPH_DEPTH`/`DEFAULT_GRAPH_DEPTH` stay `const` in http.rs (safety
  guardrails, deliberately not config).

## Verified claim inventory (register claim → current evidence)

| Register claim | Verdict | Current evidence |
| --- | --- | --- |
| `reach_seed_where` logic ×3 in one file | CONFIRMED | helper `sql.rs:813`; re-inlined `sql.rs:1087-1098` (union), `sql.rs:1191-1202` (cte) |
| `masked_col_exprs` re-inlined | CONFIRMED (×3) | helper `sql.rs:351`; inlined `sql.rs:657-666` (chain), `:1128-1138` (union), `:1289-1298` (tail) |
| `reach_projection_where` re-inlined | CONFIRMED | helper `sql.rs:898`; inlined `sql.rs:1139-1144` (union) |
| six `#[allow(too_many_arguments)]` | DRIFTED: seven | `sql.rs:451,505,921,979,1062,1170,1236`; five reach-family in scope |
| three `#[expect(indexing_slicing)]` in `caller_predicate_sql` | CONFIRMED | `sql.rs:302,321,333` (each behind a `debug_assert_eq!`) |
| union/tail are parallel copies of `resolve_graph` | CONFIRMED | `handler.rs:1202-1269`, `:1292-1399` vs `resolve_graph` `:1107-1181`; prologue+epilogue duplicated 3× |
| tail `final_*` fold e2e-pinned | CONFIRMED (renamed) | `final_g` fold `handler.rs:1353,1371`; pinned by `tests/graph_tail_e2e.rs` |
| `get_graph_path` cc 25 | CONFIRMED (shape) | 147-line fn `http.rs:607-753` |
| scraper loop hand-rolled 5× | CONFIRMED | `http.rs:184-202, 298-312, 378-392, 526-551, 620-653` (+2 lineage variants `:1118-1131, :1226-1232`) |
| census pair sharing `respond_shaped` | CONFIRMED (moved) | `http.rs:329-351` ≈ `http.rs:402-421` |
| four partial `QueryError → Response` copies | CONFIRMED (+5th) | `http.rs:239-245, 261-266, 467-479, 756-768`; partial 5th `:1073-1081` (post_search) |
| `AppState::deps()` replacing 7 literals | DRIFTED: eight | `http.rs:203,329,402,782,818,853,893,1051` |
| flight_export duplicated governed-read block | CONFIRMED | `flight_export.rs:196-212` ≈ `:240-256` |
| governance layer landed (reuse, don't reinvent) | CONFIRMED | `governed.rs`: `GovernedType` :20, `resolve_governed` :59, `Projection` :131, `seed_predicates` :321, `resolve_hop` :274 (PR #299) |

## Call-site inventory (verified by grep; the plan updates every one)

| Symbol | Call sites | Updated in |
| --- | --- | --- |
| `compile_graph_reach` | `handler.rs:976`; tests: `compile_graph_reach.rs` ×3, `recursive_cte_over_datafusion.rs:85` | Task 3 |
| `compile_graph_tree` | `handler.rs:1015`; tests: `compile_graph_tree.rs` ×3, `compile_graph_tree_exec.rs:69` | Task 3 |
| `compile_graph_reach_union` | `handler.rs:1255`; tests: `compile_graph_reach_union.rs` ×2 (+1 exact pin added in Task 1), `recursive_cte_over_datafusion.rs:190` | Task 3 |
| `compile_graph_reach_tail` | `handler.rs:1383`; tests: `compile_graph_reach_tail.rs` ×4 (+1 exact pin added in Task 1) | Task 3 |
| `recursive_reach_cte` (private) | `sql.rs:1272` (inside `compile_graph_reach_tail`) | Tasks 1-3 |
| `caller_predicate_sql` (private) | `sql.rs:429,440,637,821,1089,1193` | Task 2 (fallible) |
| `reach_seed_where` | `sql.rs:944,1001` (+ the 2 inline copies) | Task 1 (reuse), Task 2 (fallible) |
| `read_graph_reach` / `read_graph_reach_union` / `read_graph_reach_with_tail` | `http.rs:788,859,899`; tests `graph_reach.rs`, `graph_reach_union.rs`, `graph_reach_tail.rs` (kept green via unchanged `pub` wrappers) | Task 4 (spine), Task 6 (http dispatch) |
| `QueryDeps { … }` literal | `http.rs:203,329,402,782,818,853,893,1051` | Tasks 6/8 (`AppState::deps()`) |
| `chain_error` / `graph_error` | `http.rs:428,435` / `:802,838,873,914` | Task 7 (deleted → total mapping) |
| `compile_object_read` (flight) | `flight_export.rs:196,240` | Task 9 |
| Route scraper loops | `http.rs:184,298,378,526,620,1118,1226` | Task 5 |

## Deliberate behavior changes (everything else is byte-identical)

1. **Panic → error on the caller-predicate arity invariant** (the encouraged
   exception class). A `CallerPredicate` whose `values` arity violates its
   operator (Between ≠ 2 operands; text-pattern/scalar ≠ 1) previously hit
   `debug_assert!` + index-panic ("fail-closed" by aborting the request task);
   it now returns `CompileError::MalformedFilter` → `QueryError::Malformed` →
   the existing opaque `500 internal error`. New (server-side-only) error
   texts: ``between predicate on `{col}` requires exactly two operands`` /
   ``text-pattern predicate on `{col}` requires exactly one operand`` /
   ``scalar predicate on `{col}` requires exactly one operand``. Unreachable
   through the HTTP layer today (`filter::coerce_predicate` enforces arity);
   still fail-closed — no SQL is emitted.
2. **`GET /objects/{type}` repeated-`_ids` edge**: `?_ids=&_ids=1` was 400
   (mid-loop check on the first, empty occurrence); it becomes 200 with
   `ids=["1"]` — last-occurrence-wins, matching the other four routes'
   post-loop check. (`?_ids=` alone and `?_ids=1&_ids=` stay 400.) No test
   pins the old edge.
3. **Multi-invalid-param 400 precedence**: when several reserved params are
   simultaneously invalid (e.g. `?depth=abc&_ids=`), the 400 body now follows
   a fixed check order (`_ids`, `depth`, `tree`, depth-range) instead of
   query-string arrival order. Any single-error request returns the identical
   status + body. No test pins multi-error precedence.
4. **Total error mapping unifies the route×variant grid**: almost all newly
   mapped combos are unreachable — e.g. a hypothetical `UnknownLink` on
   `/objects/{type}` would now be 404 (was opaque 500), `Serving(NoIndex)`
   outside `/search` would be 404; verified (`NoIndex`/`DimMismatch` built
   only in `engine_client.rs:74-75`; `read_object`'s error surface has no
   link/graph/chain variants). **One combo IS constructible:** on `/search`,
   `QueryError::BadFilterValue` can arise via `vector_search` →
   `identity_in_predicate` → `coerce_filter` (`handler.rs:614`,
   `governed.rs:238-242`) when an engine-served identity value fails
   round-trip coercion — today post_search's catch-all returns an opaque 500
   (`http.rs:1081`); under the total mapping it becomes the structured
   `bad_filter_value` 400. Deliberate: it only fires on a pathological
   engine/ontology inconsistency (the identity values came from the engine
   itself), and the 400 body echoes only the identity column + offending
   value. Everything else is defensive dead arms, making the mapping
   future-proof-total.

---

### Task 1: sql.rs — reuse the reach helpers in the union / CTE / chain compilers

Pure substitution — but the existing graph-reach tests are **fragment pins
only** (`contains(…)`; both union tests pass `mask_cols = &[]`, so the masked
branch being replaced has no compile-level coverage, and `recursive_reach_cte`
is pinned only via fragments + the DataFusion exec test). Step 1 therefore
adds two **exact full-SQL** pins, green against the CURRENT compiler; the
substitution then happens under them. The chain family is already
exact-pinned (`tests/sql_compile.rs`, e.g.
`identity_masked_dedups_on_raw_identity_via_window`).

**Files:**
- Test (append FIRST): `src/services/query-api/tests/compile_graph_reach_union.rs`,
  `src/services/query-api/tests/compile_graph_reach_tail.rs` (existing
  targets — no BUCK change)
- Modify: `src/services/query-api/src/sql.rs` (`compile_graph_reach_union`
  :1066-1157, `recursive_reach_cte` :1174-1219, `compile_graph_reach_tail`
  cols block :1287-1298, `compile_chain_with` cols block :655-666)

**Interfaces:**
- Consumes: existing helpers `validate_reach_filters` (:786),
  `reach_seed_where` (:813), `reach_recursive_where` (:875, with `path = &[]`
  the degenerate emits the identical `r.depth < N` + filters-at-`nxt` text),
  `reach_projection_where` (:898), `masked_col_exprs` (:351).
- Produces: no signature changes; SQL output byte-identical (param push order
  identical: seed predicates, seed filters, recursive filters, projection
  filters), proven by the new exact pins.

- [x] **Step 1: Write the exact full-SQL pins (green against the current compiler)**

The expected strings below were captured from the CURRENT compilers (probe
`assert_eq!` runs against the pre-refactor tree, 2026-07-02) — byte-exact,
including every space. Both tests exercise every helper-replaced region: seed
predicates + row filters (`reach_seed_where`), the depth bound +
filters-at-`nxt` (`reach_recursive_where`), a NON-EMPTY `mask_cols`
(`masked_col_exprs`), and the reach-membership projection
(`reach_projection_where`).

Append to `src/services/query-api/tests/compile_graph_reach_union.rs` (its
existing imports cover everything):

```rust
#[test]
fn union_masked_projection_full_sql_is_byte_exact() {
    // One FK self-link, one seed predicate, one row filter, one MASKED column —
    // every helper-replaced region of the compiler renders. Byte-exact pin for the
    // helper-reuse refactor (the other union tests assert fragments only and pass
    // empty mask_cols).
    let fk = LinkBacking::ForeignKey {
        from_column: "knows_id".into(),
        to_column: "id".into(),
    };
    let seed = vec![CallerPredicate {
        column: "name".into(),
        op: CompareOp::Eq,
        values: vec![SqlValue::Text("Ada".into())],
    }];
    let row_filters = vec![RowFilter::Compare {
        property: "active".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Bool(true),
    }];
    let (sql, params) = compile_graph_reach_union(
        &DataFusionDialect,
        &person(),
        "id",
        &[fk],
        &seed,
        &row_filters,
        &["id".to_string(), "name".to_string(), "email".to_string()],
        &["email".to_string()],
        3,
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"WITH RECURSIVE reach(id, depth) AS (SELECT s."id" AS id, 0 AS depth FROM "main"."person" s WHERE (s."name" = ?) AND (s."active" = ?) UNION SELECT e.to_id AS id, r.depth + 1 AS depth FROM reach r JOIN (SELECT cur."id" AS from_id, nxt."id" AS to_id FROM "main"."person" cur JOIN "main"."person" nxt ON cur."knows_id" = nxt."id") e ON r.id = e.from_id JOIN "main"."person" nxt ON e.to_id = nxt."id" WHERE r.depth < 3 AND (nxt."active" = ?)) SELECT p."id", p."name", '***' AS "email" FROM "main"."person" p WHERE p."id" IN (SELECT id FROM reach WHERE depth >= 1) AND (p."active" = ?) LIMIT 100"#
    );
    assert_eq!(
        params,
        vec![
            SqlValue::Text("Ada".into()),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]
    );
}
```

Append to `src/services/query-api/tests/compile_graph_reach_tail.rs` (its
existing imports cover everything):

```rust
#[test]
fn tail_full_sql_including_cte_is_byte_exact() {
    // FK core with seed predicate + core row filter (pins the WHOLE
    // recursive_reach_cte text), FK tail with a tail-type row filter, MASKED final
    // column, no declared final identity (DISTINCT branch). Byte-exact pin for the
    // helper-reuse refactor (the other tail tests assert fragments only).
    let core = LinkBacking::ForeignKey {
        from_column: "knows_id".into(),
        to_column: "id".into(),
    };
    let seed = vec![CallerPredicate {
        column: "name".into(),
        op: CompareOp::Eq,
        values: vec![SqlValue::Text("Ada".into())],
    }];
    let core_rf = vec![RowFilter::Compare {
        property: "active".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Bool(true),
    }];
    let tail_types = vec![
        ChainType {
            table: tref("person"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tref("company"),
            row_filters: vec![RowFilter::Compare {
                property: "public".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Bool(true),
            }],
            predicates: vec![],
        },
    ];
    let tail_hops = vec![LinkBacking::ForeignKey {
        from_column: "worksat_id".into(),
        to_column: "id".into(),
    }];
    let (sql, params) = compile_graph_reach_tail(
        &DataFusionDialect,
        &tref("person"),
        "id",
        &core,
        &seed,
        &core_rf,
        &tail_types,
        &tail_hops,
        &["id".to_string(), "cname".to_string()],
        &["cname".to_string()],
        None,
        2,
        50,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"WITH RECURSIVE reach(id, depth) AS (SELECT s."id" AS id, 0 AS depth FROM "main"."person" s WHERE (s."name" = ?) AND (s."active" = ?) UNION SELECT nxt."id" AS id, r.depth + 1 AS depth FROM reach r JOIN "main"."person" cur ON cur."id" = r.id JOIN "main"."person" nxt ON cur."knows_id" = nxt."id" WHERE r.depth < 2 AND (nxt."active" = ?)) SELECT DISTINCT t_1."id", '***' AS "cname" FROM "main"."company" t_1 JOIN "main"."person" t_0 ON t_0."worksat_id" = t_1."id" WHERE t_0."id" IN (SELECT id FROM reach WHERE depth >= 1) AND (t_1."public" = ?) LIMIT 50"#
    );
    assert_eq!(
        params,
        vec![
            SqlValue::Text("Ada".into()),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]
    );
}
```

(NOTE for Task 3: these two tests are compiler call sites and get the same
mechanical `ReachSpec` call-shape migration there — assertions byte-identical,
like every other compile test.)

- [x] **Step 2: Run the pinning suite green (baseline, incl. the new pins)**

Run: `buck2 test //src/services/query-api:sql-compile //src/services/query-api:sql-dialect //src/services/query-api:compile-graph-reach //src/services/query-api:compile-graph-tree //src/services/query-api:compile-graph-tree-exec //src/services/query-api:compile-graph-reach-union //src/services/query-api:compile-graph-reach-tail //src/services/query-api:compile-chain-pairs //src/services/query-api:recursive-cte-over-datafusion > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS — the two new pins are green against the UNMODIFIED compiler.
If either fails here, the expected string was mis-transcribed: fix the TEST,
never the compiler.

- [x] **Step 3: Substitute the helpers**

In `compile_graph_reach_union`, replace the validate loop (:1078-1080), the
inline seed block (:1086-1098), the inline rec-where block (:1114-1122 —
keep the two explanatory comments), and the inline cols/proj blocks
(:1127-1144) so the body reads:

```rust
    validate_reach_filters(row_filters, &[])?;
    let q = |id: &str| dialect.quote_ident(id);
    let tbl = format!("{}.{}", q(&table.schema), q(&table.name));
    let id = q(identity);
    let mut params: Vec<SqlValue> = Vec::new();

    let seed_where = reach_seed_where(dialect, seed_predicates, row_filters, &mut params);

    // Edge subquery: each backing contributes one non-recursive arm that emits (from_id, to_id)
    // pairs. Arms are joined with UNION ALL (duplicates acceptable here; the outer CTE dedupes).
    // The join-table alias `j{i}` is per-arm so multiple join-table links never collide.
    let edge_arms: Vec<String> = backings
        .iter()
        .enumerate()
        .map(|(i, backing)| {
            let jt_alias = format!("j{i}");
            let joins = link_join(dialect, backing, "cur", "nxt", &tbl, &jt_alias);
            format!("SELECT cur.{id} AS from_id, nxt.{id} AS to_id FROM {tbl} cur{joins}")
        })
        .collect();
    let edges_sql = edge_arms.join(" UNION ALL ");

    // Recursive step: single join of `reach r` to the edge subquery, then to `nxt` for filter.
    // Row-filters are applied at `nxt` (the landing node). This single `reach` reference avoids
    // a "Circular reference to CTE" planner error that arises from multiple arms each referencing
    // the CTE name. The empty-path `reach_recursive_where` is exactly this degenerate: the depth
    // bound plus the start row-filters at `nxt`.
    let rec_where = reach_recursive_where(dialect, &[], row_filters, depth, &mut params);
    let recursive = format!(
        "SELECT e.to_id AS id, r.depth + 1 AS depth FROM reach r JOIN ({edges_sql}) e ON r.id = e.from_id JOIN {tbl} nxt ON e.to_id = nxt.{id} WHERE {rec_where}"
    );

    // Projection of `p`: visible columns (masked -> marker), reachable in >= 1 hop, governed.
    let cols = masked_col_exprs(dialect, allowed_cols, mask_cols, "p.").join(", ");
    let proj_where = reach_projection_where(dialect, &id, row_filters, &mut params);

    let limit_clause = dialect.limit_clause(limit);

    let sql = format!(
        "WITH RECURSIVE reach(id, depth) AS (\
           SELECT s.{id} AS id, 0 AS depth FROM {tbl} s{seed_where} \
           UNION \
           {recursive}\
         ) \
         SELECT {cols} FROM {tbl} p WHERE {proj_where} {limit_clause}"
    );
    Ok((sql, params))
```

In `recursive_reach_cte`, replace the validate loop (:1184-1186), the inline
seed block (:1191-1202), and the inline rec-where block (:1206-1210):

```rust
    validate_reach_filters(row_filters, &[])?;
    let q = |id: &str| dialect.quote_ident(id);
    let tbl = format!("{}.{}", q(&table.schema), q(&table.name));
    let id = q(identity);

    let seed_where = reach_seed_where(dialect, seed_predicates, row_filters, params);

    let joins = link_join(dialect, backing, "cur", "nxt", &tbl, "j");

    let rec_where = reach_recursive_where(dialect, &[], row_filters, depth, params);

    Ok(format!(
        "WITH RECURSIVE reach(id, depth) AS (\
           SELECT s.{id} AS id, 0 AS depth FROM {tbl} s{seed_where} \
           UNION \
           SELECT nxt.{id} AS id, r.depth + 1 AS depth FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}\
         )"
    ))
```

In `compile_graph_reach_tail`, replace the inline cols block (:1289-1298):

```rust
    let col_exprs: Vec<String> =
        masked_col_exprs(dialect, allowed_cols, mask_cols, &format!("{final_alias}."));
```

In `compile_chain_with`, replace the inline cols block (:657-666):

```rust
    let col_exprs: Vec<String> =
        masked_col_exprs(dialect, allowed_cols, mask_cols, &format!("{final_alias}."));
```

(`masked_col_exprs`'s `alias` is a raw prefix — `"p."` / `"t_2."` — so the
rendered exprs are byte-identical to the inline `format!` forms.)

- [x] **Step 4: Run the pinning suite green (post-substitution)**

Run: `buck2 test //src/services/query-api:sql-compile //src/services/query-api:sql-dialect //src/services/query-api:compile-graph-reach //src/services/query-api:compile-graph-tree //src/services/query-api:compile-graph-tree-exec //src/services/query-api:compile-graph-reach-union //src/services/query-api:compile-graph-reach-tail //src/services/query-api:compile-chain-pairs //src/services/query-api:recursive-cte-over-datafusion > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS — identical test list; the two Step-1 pins prove byte-identity
of the rewritten compilers; zero assertion changes anywhere.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c1.log 2>&1; cat /tmp/c1.log` — artifact empty.

- [x] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p1.log 2>&1; grep -c Failed /tmp/p1.log` — expected `0`.

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/tests/compile_graph_reach_union.rs src/services/query-api/tests/compile_graph_reach_tail.rs
git commit -m "refactor(query-api): reuse reach SQL helpers in union/CTE/tail compilers

compile_graph_reach_union and recursive_reach_cte re-inlined reach_seed_where
(three copies of the seed WHERE in one file), reach_recursive_where (empty-path
degenerate), validate_reach_filters, and reach_projection_where;
compile_graph_reach_tail, compile_graph_reach_union, and compile_chain_with
re-inlined masked_col_exprs. Pure substitution, proven byte-identical by two
new exact full-SQL pins (union with a masked projection; tail pinning the
whole recursive_reach_cte text) captured green against the pre-refactor
compiler — the pre-existing graph tests were fragment assertions only.

Part of road-qa-read-path-consolidation."
```

---

### Task 2: sql.rs — fallible slice patterns replace the three `indexing_slicing` expects

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (`caller_predicate_sql`
  :276-345 and its transitive callers `select_where_conjuncts`,
  `compile_select_with`, `chain_from_where`, `reach_seed_where`,
  `compile_graph_reach`, `compile_graph_tree`, `compile_graph_reach_union`,
  `recursive_reach_cte`)
- Test: `src/services/query-api/tests/sql_compile.rs` (append; existing
  target `//src/services/query-api:sql-compile` — no BUCK change)

**Interfaces:**
- Produces: `fn caller_predicate_sql(…) -> Result<String, CompileError>`
  (private), `fn select_where_conjuncts(…) -> Result<Vec<String>, CompileError>`
  (private), `fn reach_seed_where(…) -> Result<String, CompileError>`
  (private). All `pub` compiler signatures unchanged.
- Deletes: the three `#[expect(clippy::indexing_slicing)]` (:302, :321, :333)
  and their three `debug_assert_eq!` guards.

- [x] **Step 1: Write the failing tests**

Append to `src/services/query-api/tests/sql_compile.rs` (extend its sql import
line to `use query_api::sql::{ChainType, CompileError, DerivedAggregate, DerivedSelect, compile_chain, compile_select};`;
the file's existing `t()` helper builds the `TableRef`):

```rust
#[test]
fn between_with_one_operand_is_an_error_not_a_panic() {
    let p = CallerPredicate {
        column: "age".into(),
        op: CompareOp::Between,
        values: vec![SqlValue::Int(1)],
    };
    let res = compile_select(&t(), &["age".into()], &[], &[], &[p], &[], &[], 10);
    assert!(
        matches!(res, Err(CompileError::MalformedFilter(ref m)) if m.contains("exactly two operands")),
        "got: {res:?}"
    );
}

#[test]
fn text_pattern_with_no_operand_is_an_error_not_a_panic() {
    let p = CallerPredicate {
        column: "name".into(),
        op: CompareOp::Contains,
        values: vec![],
    };
    let res = compile_select(&t(), &["name".into()], &[], &[], &[p], &[], &[], 10);
    assert!(
        matches!(res, Err(CompileError::MalformedFilter(ref m)) if m.contains("exactly one operand")),
        "got: {res:?}"
    );
}

#[test]
fn scalar_with_no_operand_is_an_error_not_a_panic() {
    let p = CallerPredicate {
        column: "age".into(),
        op: CompareOp::Eq,
        values: vec![],
    };
    let res = compile_select(&t(), &["age".into()], &[], &[], &[p], &[], &[], 10);
    assert!(
        matches!(res, Err(CompileError::MalformedFilter(ref m)) if m.contains("exactly one operand")),
        "got: {res:?}"
    );
}
```

- [x] **Step 2: Run to see them fail**

Run: `buck2 test //src/services/query-api:sql-compile > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|panicked" /tmp/t2.log`
Expected: FAIL — the three new tests panic (`debug_assert` / index out of
bounds) instead of returning `Err`.

- [x] **Step 3: Make `caller_predicate_sql` total and fallible**

Replace `caller_predicate_sql` (:276-345) with:

```rust
/// Render one caller predicate at `alias` (empty = unqualified), pushing its operand
/// params in conjunct order. Scalar ops use `op_sql`; set ops expand to N placeholders;
/// null ops emit no param. The column is a trusted ontology identifier (quoted), every
/// operand a bound placeholder. Total over the operand arity: a predicate whose
/// `values` violates its operator's arity (normally impossible — enforced upstream by
/// `filter::coerce_predicate`) is a `CompileError`, never a panic and never SQL with a
/// placeholder bound to a stale param — fail-closed on the injection boundary.
fn caller_predicate_sql(
    dialect: &dyn SqlDialect,
    p: &CallerPredicate,
    alias: &str,
    params: &mut Vec<SqlValue>,
) -> Result<String, CompileError> {
    use control_plane_core::CompareOp::*;
    let col = col_ref(dialect, alias, &p.column);
    Ok(match p.op {
        In | NotIn => {
            let kw = if matches!(p.op, In) { "IN" } else { "NOT IN" };
            let mut placeholders = Vec::with_capacity(p.values.len());
            for v in &p.values {
                params.push(v.clone());
                placeholders.push(dialect.placeholder(params.len()));
            }
            format!("({col} {kw} ({}))", placeholders.join(", "))
        }
        IsNull => format!("({col} IS NULL)"),
        IsNotNull => format!("({col} IS NOT NULL)"),
        Between => {
            let [lo_v, hi_v] = p.values.as_slice() else {
                return Err(CompileError::MalformedFilter(format!(
                    "between predicate on `{}` requires exactly two operands",
                    p.column
                )));
            };
            params.push(lo_v.clone());
            let lo = dialect.placeholder(params.len());
            params.push(hi_v.clone());
            let hi = dialect.placeholder(params.len());
            format!("({col} BETWEEN {lo} AND {hi})")
        }
        Contains | StartsWith | EndsWith => {
            let [v] = p.values.as_slice() else {
                return Err(CompileError::MalformedFilter(format!(
                    "text-pattern predicate on `{}` requires exactly one operand",
                    p.column
                )));
            };
            params.push(v.clone());
            format!(
                "({col} ILIKE {} ESCAPE '\\')",
                dialect.placeholder(params.len())
            )
        }
        _ => {
            let [v] = p.values.as_slice() else {
                return Err(CompileError::MalformedFilter(format!(
                    "scalar predicate on `{}` requires exactly one operand",
                    p.column
                )));
            };
            params.push(v.clone());
            format!(
                "({col} {} {})",
                op_sql(p.op),
                dialect.placeholder(params.len())
            )
        }
    })
}
```

Propagate the fallibility (each is a mechanical `?`):

1. `select_where_conjuncts` returns
   `Result<Vec<String>, CompileError>`: the two push sites become
   `conjuncts.push(caller_predicate_sql(dialect, p, "", params)?);` and the
   OR-group members become
   `let members: Vec<String> = group.iter().map(|m| caller_predicate_sql(dialect, m, "", params)).collect::<Result<_, _>>()?;`;
   wrap the final `conjuncts` in `Ok(…)`.
2. `compile_select_with`: `let conjuncts = select_where_conjuncts(dialect, row_filters, predicates, or_groups, &mut params)?;`
3. `chain_from_where` (:637): `conjuncts.push(caller_predicate_sql(dialect, p, &a, &mut params)?);`
4. `reach_seed_where` returns `Result<String, CompileError>`: the push site
   becomes `seed_conj.push(caller_predicate_sql(dialect, p, "s", params)?);`,
   the tail becomes `Ok(if seed_conj.is_empty() { String::new() } else { format!(" WHERE {}", seed_conj.join(" AND ")) })`.
5. Its callers append `?`: `compile_graph_reach` (:944),
   `compile_graph_tree` (:1001), and the Task-1 call sites in
   `compile_graph_reach_union` and `recursive_reach_cte`.

- [x] **Step 4: Run to green**

Run: `buck2 test //src/services/query-api:sql-compile //src/services/query-api:compile-graph-reach //src/services/query-api:compile-graph-tree //src/services/query-api:compile-graph-reach-union //src/services/query-api:compile-graph-reach-tail //src/services/query-api:compile-chain-pairs //src/services/query-api:sql-dialect > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS (new tests + every pre-existing exact-SQL assertion).
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c2.log 2>&1; cat /tmp/c2.log` — artifact empty (the three
`indexing_slicing` expects are gone; nothing new fires).

- [x] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p2.log 2>&1; grep -c Failed /tmp/p2.log` — expected `0`.

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/tests/sql_compile.rs
git commit -m "refactor(query-api): fallible slice patterns in caller_predicate_sql

let [lo, hi] = values.as_slice() else { return Err(..) } makes the operand
arity total on the injection boundary: a violated invariant is now
CompileError::MalformedFilter (-> the existing opaque 500), not a request-task
panic. Deletes the three #[expect(indexing_slicing)] + debug_asserts; the
fallibility ripples only through sql.rs internals (public signatures already
returned Result). Whitelisted behavior change #1.

Part of road-qa-read-path-consolidation."
```

---

### Task 3: sql.rs — `ReachSpec<'a>` replaces the five reach-family `too_many_arguments`

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (new struct; re-sign
  `compile_graph_reach`, `compile_graph_tree`, `compile_graph_reach_union`,
  `compile_graph_reach_tail`, `recursive_reach_cte`; delete their five
  `#[allow(clippy::too_many_arguments)]`)
- Modify: `src/services/query-api/src/handler.rs` (4 call sites: :976, :1015,
  :1255, :1383)
- Modify (call shape ONLY — every assertion byte-identical):
  `src/services/query-api/tests/compile_graph_reach.rs` (3 calls),
  `tests/compile_graph_tree.rs` (3), `tests/compile_graph_tree_exec.rs` (1),
  `tests/compile_graph_reach_union.rs` (3, incl. Task 1's exact pin),
  `tests/compile_graph_reach_tail.rs` (5, incl. Task 1's exact pin),
  `tests/recursive_cte_over_datafusion.rs` (2)

**Interfaces:**
- Produces (all `pub` in `query_api::sql`):

```rust
/// The parameters every reach-family compiler shares: the queried (seed/recursion)
/// type's physical table, its declared identity (the recursion's dedup key), the
/// caller seed predicates (anchor alias `s`), the queried type's ACL row-filters
/// (rendered at `s`/`nxt`/`p` per compiler; the recursive CORE's governance for the
/// tail compiler), the visible/masked projection (the FINAL type's for the tail
/// compiler), and the inlined depth bound. `limit` is deliberately NOT here:
/// `compile_graph_tree` emits no LIMIT by design (a LIMIT could orphan a child), so
/// the LIMIT-emitting compilers take it explicitly instead of carrying a field one
/// consumer silently ignores. Copy (all fields are borrows + u32) so the compilers
/// can destructure `*spec` without ceremony.
#[derive(Clone, Copy)]
pub struct ReachSpec<'a> {
    pub table: &'a TableRef,
    pub identity: &'a str,
    pub seed_predicates: &'a [CallerPredicate],
    pub row_filters: &'a [RowFilter],
    pub allowed_cols: &'a [String],
    pub mask_cols: &'a [String],
    pub depth: u32,
}
```

  and the re-signed compilers (bodies unchanged except reading `spec.*`; doc
  comments keep their existing text, with the parameter references updated):

```rust
pub fn compile_graph_reach(
    dialect: &dyn SqlDialect,
    spec: &ReachSpec<'_>,
    path: &[GraphStep],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError>

pub fn compile_graph_tree(
    dialect: &dyn SqlDialect,
    spec: &ReachSpec<'_>,
    path: &[GraphStep],
) -> Result<(String, Vec<SqlValue>), CompileError>

pub fn compile_graph_reach_union(
    dialect: &dyn SqlDialect,
    spec: &ReachSpec<'_>,
    backings: &[LinkBacking],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError>

pub fn compile_graph_reach_tail(
    dialect: &dyn SqlDialect,
    spec: &ReachSpec<'_>,
    core_backing: &LinkBacking,
    tail_types: &[ChainType],
    tail_hops: &[LinkBacking],
    final_identity: Option<&str>,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError>

// private; ignores spec.allowed_cols/mask_cols (the CTE projects only id/depth) —
// documented on the fn.
fn recursive_reach_cte(
    dialect: &dyn SqlDialect,
    spec: &ReachSpec<'_>,
    backing: &LinkBacking,
    params: &mut Vec<SqlValue>,
) -> Result<String, CompileError>
```

- [x] **Step 1: Migrate the signatures + bodies (compiler-driven)**

In `sql.rs`: add `ReachSpec` above `compile_graph_reach`; change each
signature as above; delete the five `#[allow(clippy::too_many_arguments)]`
blocks; inside each body, bind the old names once at the top so the rest of
the body is unchanged, e.g. for `compile_graph_reach`:

```rust
    let ReachSpec {
        table,
        identity,
        seed_predicates,
        row_filters,
        allowed_cols,
        mask_cols,
        depth,
    } = *spec;
```

(same destructuring line in all five fns; `compile_graph_reach_tail` renames
its binding `row_filters` — the spec field carries what was
`core_row_filters`, so destructure as
`row_filters: core_row_filters` there to keep its body text unchanged).

In `compile_graph_reach_tail`, the internal `recursive_reach_cte` call
(:1272-1281) becomes:

```rust
    let cte = recursive_reach_cte(dialect, spec, core_backing, &mut params)?;
```

- [x] **Step 2: Update the four handler.rs call sites**

`read_graph_reach` (:976):

```rust
    let (sql, params) = crate::sql::compile_graph_reach(
        deps.serving.dialect(),
        &crate::sql::ReachSpec {
            table: &r.g.otype.table,
            identity: &r.identity,
            seed_predicates: &r.seed_predicates,
            row_filters: &r.g.row_filters,
            allowed_cols: &r.proj.columns,
            mask_cols: &r.proj.masked,
            depth: q.depth,
        },
        &r.steps,
        deps.default_limit,
    )?;
```

`read_graph_tree` (:1015) — no `limit` (the tree compiler emits none):

```rust
    let (sql, params) = crate::sql::compile_graph_tree(
        deps.serving.dialect(),
        &crate::sql::ReachSpec {
            table: &r.g.otype.table,
            identity: &r.identity,
            seed_predicates: &r.seed_predicates,
            row_filters: &r.g.row_filters,
            allowed_cols: &r.proj.columns,
            mask_cols: &r.proj.masked,
            depth: q.depth,
        },
        &r.steps,
    )?;
```

`read_graph_reach_union` (:1255):

```rust
    let (sql, params) = crate::sql::compile_graph_reach_union(
        deps.serving.dialect(),
        &crate::sql::ReachSpec {
            table: &g.otype.table,
            identity: &identity,
            seed_predicates: &seeds,
            row_filters: &g.row_filters,
            allowed_cols: &proj.columns,
            mask_cols: &proj.masked,
            depth: q.depth,
        },
        &backings,
        deps.default_limit,
    )?;
```

`read_graph_reach_with_tail` (:1383):

```rust
    let (sql, params) = crate::sql::compile_graph_reach_tail(
        deps.serving.dialect(),
        &crate::sql::ReachSpec {
            table: &g.otype.table,
            identity: &identity,
            seed_predicates: &seeds,
            row_filters: &g.row_filters,
            allowed_cols: &proj.columns,
            mask_cols: &proj.masked,
            depth: q.depth,
        },
        &core_backing,
        &tail_types,
        &tail_hops,
        final_g.otype.identity.as_deref(),
        deps.default_limit,
    )?;
```

- [x] **Step 3: Update the 17 test call sites (call shape only)**

Mechanical rule — the old positional args map into the spec:
old `(dialect, table, identity, path/backings/core…, seed_predicates,
row_filters, allowed_cols, mask_cols, depth, limit)` becomes
`(dialect, &ReachSpec { table, identity, seed_predicates, row_filters,
allowed_cols, mask_cols, depth }, path/backings/core…, limit)`. Worked
example — `tests/compile_graph_reach_union.rs:40-52` becomes:

```rust
    let (sql, params) = compile_graph_reach_union(
        &DataFusionDialect,
        &ReachSpec {
            table: &person(),
            identity: "id",
            seed_predicates: &[], // no seed predicates
            row_filters: &row_filters,
            allowed_cols: &["id".to_string(), "name".to_string()],
            mask_cols: &[],
            depth: 3,
        },
        &[fk, jt],
        1000,
    )
    .unwrap();
```

Apply the same transformation at every call site listed in the inventory
(`compile_graph_reach.rs` ×3, `compile_graph_tree.rs` ×3 — no `limit` arg,
`compile_graph_tree_exec.rs:69`, `compile_graph_reach_union.rs` ×3 (incl.
Task 1's `union_masked_projection_full_sql_is_byte_exact`),
`compile_graph_reach_tail.rs:45,134` + its 2 others + Task 1's
`tail_full_sql_including_cte_is_byte_exact`,
`recursive_cte_over_datafusion.rs:85,190`), adding `ReachSpec` to each file's
`use query_api::sql::{…}` line. Temporaries: where an old arg was an inline
expression (e.g. `&person()` / `&tref("person")`), bind it to a `let` above
the spec literal if the borrow checker requires an lvalue. **Every assertion
line stays byte-identical** (these tests construct `ReachSpec` directly —
they are the pure-logic `rust_test` coverage for the new seam).

- [x] **Step 4: Run to green**

Run: `buck2 test //src/services/query-api:sql-compile //src/services/query-api:compile-graph-reach //src/services/query-api:compile-graph-tree //src/services/query-api:compile-graph-tree-exec //src/services/query-api:compile-graph-reach-union //src/services/query-api:compile-graph-reach-tail //src/services/query-api:recursive-cte-over-datafusion //src/services/query-api:graph-reach //src/services/query-api:graph-tree //src/services/query-api:graph-reach-union //src/services/query-api:graph-reach-tail > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c3.log 2>&1; cat /tmp/c3.log` — artifact empty (five allows deleted, no
`too_many_arguments` fires: arities are 4/3/4/7/4).

- [x] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p3.log 2>&1; grep -c Failed /tmp/p3.log` — expected `0`.

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/src/handler.rs src/services/query-api/tests
git commit -m "refactor(query-api): ReachSpec params struct for the reach compiler family

One borrowed spec (table/identity/seeds/row-filters/projection/depth) replaces
the shared positional prefix of compile_graph_reach/tree/reach_union/reach_tail
and recursive_reach_cte, deleting all five reach-family
#[allow(too_many_arguments)]. limit stays explicit (tree emits no LIMIT by
design). SQL output unchanged; test assertions byte-identical (call shapes
migrated). The select-family allows (compile_select_with/compile_select) are a
different parameter shape and stay — recorded at register close.

Part of road-qa-read-path-consolidation."
```

---

### Task 4: handler.rs — graph read spine (`GraphReadSpec` + one prologue/epilogue)

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (:1089-1399: `resolve_graph`,
  `read_graph_reach_union`, `read_graph_reach_with_tail`; `read_graph_reach`
  :969; new `governed_identity`, `GraphReadSpec`, `read_graph_reach_spec`,
  `compile_reach_cycle`/`compile_reach_union`/`compile_reach_tail`)

**Interfaces:**
- Produces (`pub` in `query_api::handler`):

```rust
/// One /graph reachability read, whichever recursion structure the route selected.
/// Borrowed: the HTTP layer builds the query struct and hands a reference in.
pub enum GraphReadSpec<'q> {
    /// `?path=l1,..,lk` (or the single `/graph/:link`): a path-cycle repeated to depth.
    PathCycle(&'q GraphQuery),
    /// `?links=l1,..`: a union of self-links.
    UnionSelfLinks(&'q GraphUnionQuery),
    /// `?path=l0*,l1,..`: a recursive core + relational tail.
    CoreTail(&'q GraphTailQuery),
}

pub async fn read_graph_reach_spec(
    spec: GraphReadSpec<'_>,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError>
```

- The three existing `pub` entry points survive as one-line delegates —
  their signatures, doc comments, and error behavior unchanged, so
  `tests/graph_reach.rs` / `graph_reach_union.rs` / `graph_reach_tail.rs` /
  `graph_tree.rs` and every e2e stay byte-unmodified.
- `read_graph_tree` and `resolve_graph` keep their signatures.

- [x] **Step 1: Extract the shared prologue and per-variant compile stages**

Add above `GraphResolved` (:1094):

```rust
/// The shared prologue of every /graph read variant: Read-gate + resolve the queried
/// type (`resolve_governed`, deny-before-existence-leak) and require its declared
/// identity — the recursion's dedup key. Previously copied verbatim into
/// `resolve_graph`, the union read, and the tail read.
async fn governed_identity(
    type_name: &str,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<(GovernedType, String), QueryError> {
    let name = TypeName(type_name.to_string());
    let g = resolve_governed(deps.ontology, deps.acl, &subject.0, &name, OnMissing::NotFound)
        .await?;
    let identity = g
        .otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(type_name.to_string()))?;
    Ok((g, identity))
}
```

Rewrite `resolve_graph`'s opening (:1112-1127) to consume it (the rest of the
fn — path-cycle walk, projection, seeds — is unchanged):

```rust
    let (g, identity) = governed_identity(&q.type_name, subject, deps).await?;
    let type_name = TypeName(q.type_name.clone());
```

Split each of the three reach reads into a compile stage returning
`(Projection, String, Vec<SqlValue>)` and move the fetch+zip epilogue into
the spine. `compile_reach_cycle` (new; replaces `read_graph_reach`'s body):

```rust
/// Path-cycle compile stage: resolve + govern the cycle, compile the reach SQL.
async fn compile_reach_cycle(
    q: &GraphQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<(Projection, String, Vec<SqlValue>), QueryError> {
    let r = resolve_graph(q, subject, deps).await?;
    let (sql, params) = crate::sql::compile_graph_reach(
        deps.serving.dialect(),
        &crate::sql::ReachSpec {
            table: &r.g.otype.table,
            identity: &r.identity,
            seed_predicates: &r.seed_predicates,
            row_filters: &r.g.row_filters,
            allowed_cols: &r.proj.columns,
            mask_cols: &r.proj.masked,
            depth: q.depth,
        },
        &r.steps,
        deps.default_limit,
    )?;
    Ok((r.proj, sql, params))
}
```

`compile_reach_union` (new): the current `read_graph_reach_union` body with
(a) the opening resolve+identity block (:1207-1222) replaced by
`let (g, identity) = governed_identity(&q.type_name, subject, deps).await?;
let type_name = TypeName(q.type_name.clone());`, (b) the trailing
`fetch_rows` + `into_object_rows` (:1267-1268) replaced by
`Ok((proj, sql, params))`. The self-link resolution (:1228-1250), projection,
seeds, and Task-3 compile call are unchanged. Return type as above.

`compile_reach_tail` (new): the current `read_graph_reach_with_tail` body
with the same two replacements (opening :1297-1312 → `governed_identity` +
local `type_name`; trailing :1398-1399 → `Ok((proj, sql, params))`). The
**`final_g` fold** (:1345-1373), tail validation, and compile call stay
verbatim — `tests/graph_tail_e2e.rs` pins its dedup/projection behavior.

- [x] **Step 2: Add the spine and turn the pub entry points into delegates**

```rust
/// The one /graph reachability spine: run the variant's compile stage, execute on
/// the serving engine, zip the projection onto the served rows. Governance lives in
/// the compile stages (each starts at `governed_identity` -> `resolve_governed`).
pub async fn read_graph_reach_spec(
    spec: GraphReadSpec<'_>,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let (proj, sql, params) = match spec {
        GraphReadSpec::PathCycle(q) => compile_reach_cycle(q, subject, deps).await?,
        GraphReadSpec::UnionSelfLinks(q) => compile_reach_union(q, subject, deps).await?,
        GraphReadSpec::CoreTail(q) => compile_reach_tail(q, subject, deps).await?,
    };
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    Ok(proj.into_object_rows(served))
}
```

The delegates (keep each fn's existing doc comment verbatim):

```rust
pub async fn read_graph_reach(
    q: &GraphQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    read_graph_reach_spec(GraphReadSpec::PathCycle(q), subject, deps).await
}

pub async fn read_graph_reach_union(
    q: &GraphUnionQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    read_graph_reach_spec(GraphReadSpec::UnionSelfLinks(q), subject, deps).await
}

pub async fn read_graph_reach_with_tail(
    q: &GraphTailQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    read_graph_reach_spec(GraphReadSpec::CoreTail(q), subject, deps).await
}
```

(`read_graph_tree` keeps calling `resolve_graph` + its identity-governed
guard — unchanged.)

- [x] **Step 3: Run the pinned handler + e2e suites to green**

Run: `buck2 test //src/services/query-api:graph-reach //src/services/query-api:graph-tree //src/services/query-api:graph-reach-union //src/services/query-api:graph-reach-tail //src/services/query-api:associations > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log`
Expected: PASS — all four fake-backed handler test files unmodified.
Run: `buck2 test -j 8 //src/services/query-api:graph-reach-e2e //src/services/query-api:graph-path-e2e //src/services/query-api:graph-union-e2e //src/services/query-api:graph-tail-e2e //src/services/query-api:graph-tree-e2e //src/services/query-api:graph-inverse-e2e //src/services/query-api:inverse-hops-e2e > /tmp/t4e.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4e.log`
Expected: PASS — the e2e pins (incl. the `final_g` fold behavior) unmodified.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c4.log 2>&1; cat /tmp/c4.log` — artifact empty.

- [x] **Step 4: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p4.log 2>&1; grep -c Failed /tmp/p4.log` — expected `0`.

```bash
git add src/services/query-api/src/handler.rs
git commit -m "refactor(query-api): graph read spine — GraphReadSpec over one prologue/epilogue

read_graph_reach_union/read_graph_reach_with_tail were parallel copies of the
resolve_graph read differing only in the recursion-structure middle. Now:
governed_identity is the one resolve+identity prologue, per-variant compile
stages return (Projection, sql, params), and read_graph_reach_spec owns the
fetch + into_object_rows epilogue. The three pub entry points stay as one-line
delegates so every handler test and e2e passes unmodified (graph_tail_e2e pins
the final_g fold, kept verbatim).

Part of road-qa-read-path-consolidation."
```

---

### Task 5: http.rs — `query_params` reserved-param splitter + typed extractors

**Files:**
- Create: `src/services/query-api/src/query_params.rs`
- Modify: `src/services/query-api/src/lib.rs` (add `pub mod query_params;`
  after `pub mod path_parse;`)
- Modify: `src/services/query-api/src/http.rs` (the 5 route loops + 2 lineage
  loops + new `graph_knobs`)
- Create: `src/services/query-api/tests/query_params.rs`
- Modify: `src/services/query-api/BUCK` (new `query-params` target)

**Interfaces:**
- Produces: `query_api::query_params::{ReservedParams, split_reserved,
  comma_list, parse_ids, parse_depth}` (signatures in Step 3) and a private
  `http.rs` helper
  `fn graph_knobs(&ReservedParams) -> Result<(Vec<String>, u32, bool), axum::response::Response>`.
- Consumed by: Task 6's `get_graph_path` rewrite (same `reserved` scrape).

- [ ] **Step 1: Write the failing tests**

Create `src/services/query-api/tests/query_params.rs`:

```rust
//! Pure tests for the reserved-query-param splitter + typed extractors the HTTP
//! routes share (query_params.rs): per-route key sets, last-occurrence-wins single
//! values, accumulated `_or`, filter order/repeat preservation, and the exact 400
//! messages the routes serve.
use query_api::query_params::{comma_list, parse_depth, parse_ids, split_reserved};

fn pairs(xs: &[(&str, &str)]) -> Vec<(String, String)> {
    xs.iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn split_keeps_filter_order_and_repeats() {
    let (r, filters) = split_reserved(
        pairs(&[("a", "1"), ("_ids", "7"), ("a", "2"), ("b", "3")]),
        &["_ids"],
    );
    assert_eq!(filters, pairs(&[("a", "1"), ("a", "2"), ("b", "3")]));
    assert_eq!(r.last("_ids"), Some("7"));
}

#[test]
fn single_valued_keys_take_the_last_occurrence() {
    let (r, _) = split_reserved(pairs(&[("depth", "1"), ("depth", "2")]), &["depth"]);
    assert_eq!(r.last("depth"), Some("2"));
}

#[test]
fn or_accumulates_every_occurrence_in_order() {
    let (r, _) = split_reserved(pairs(&[("_or", "a:1,b:2"), ("_or", "c:3")]), &["_or"]);
    assert_eq!(r.all("_or"), ["a:1,b:2".to_string(), "c:3".to_string()]);
}

#[test]
fn unreserved_keys_stay_filters_per_route() {
    // `_direction` is reserved on /links but NOT on /objects — the splitter must
    // never consume a key the route did not reserve.
    let (r, filters) = split_reserved(pairs(&[("_direction", "inverse")]), &["_ids", "_or"]);
    assert!(!r.present("_direction"));
    assert_eq!(filters, pairs(&[("_direction", "inverse")]));
}

#[test]
fn present_tracks_even_empty_values() {
    let (r, _) = split_reserved(pairs(&[("_ids", "")]), &["_ids"]);
    assert!(r.present("_ids"));
    assert_eq!(r.last("_ids"), Some(""));
}

#[test]
fn parse_ids_absent_is_no_scoping() {
    assert_eq!(parse_ids(None).unwrap(), Vec::<String>::new());
}

#[test]
fn parse_ids_splits_and_drops_empty_elements() {
    assert_eq!(
        parse_ids(Some("1,,2")).unwrap(),
        vec!["1".to_string(), "2".to_string()]
    );
}

#[test]
fn parse_ids_empty_is_the_routes_400_message() {
    assert_eq!(parse_ids(Some("")).unwrap_err(), "_ids requires at least one value");
    assert_eq!(parse_ids(Some(",")).unwrap_err(), "_ids requires at least one value");
}

#[test]
fn parse_depth_defaults_parses_and_rejects() {
    assert_eq!(parse_depth(None, 5).unwrap(), 5);
    assert_eq!(parse_depth(Some("3"), 5).unwrap(), 3);
    assert_eq!(
        parse_depth(Some("abc"), 5).unwrap_err(),
        "depth must be a positive integer"
    );
    assert_eq!(
        parse_depth(Some("-1"), 5).unwrap_err(),
        "depth must be a positive integer"
    );
}

#[test]
fn comma_list_drops_empties() {
    assert_eq!(comma_list("a,,b,"), vec!["a".to_string(), "b".to_string()]);
}
```

Add to `src/services/query-api/BUCK` (next to the `path-parse` target):

```python
rust_test(
    name = "query-params",
    crate = "query_params",
    srcs = ["tests/query_params.rs"],
    crate_root = "tests/query_params.rs",
    edition = "2024",
    deps = [":query-api"],
)
```

- [ ] **Step 2: Run to see them fail**

Run: `buck2 test //src/services/query-api:query-params > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t5.log`
Expected: FAIL — compile error (`query_api::query_params` unresolved).

- [ ] **Step 3: Implement the module**

Create `src/services/query-api/src/query_params.rs`:

```rust
//! Pure reserved-query-param scraping at the HTTP edge — socket-free (like
//! `path_parse`) so per-route splits are unit-testable without axum. Each route names
//! ITS OWN reserved keys (a universal set would silently consume another route's
//! filter columns); everything unreserved stays a caller filter with order and
//! repeats preserved (a column may carry several predicates, e.g. a range).
//! Single-valued keys take the LAST occurrence — the loop-overwrite semantics the
//! routes previously hand-rolled; multi-valued keys (`_or`) read every occurrence.

use std::collections::HashMap;

/// The reserved values pulled out of one request's query params, keyed by reserved
/// key; a key's values accumulate in arrival order.
pub struct ReservedParams(HashMap<String, Vec<String>>);

impl ReservedParams {
    /// The last occurrence of a single-valued key (`?depth=1&depth=2` -> `"2"`).
    pub fn last(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.last()).map(String::as_str)
    }

    /// Every occurrence of a multi-valued key (`_or`), in arrival order.
    pub fn all(&self, key: &str) -> &[String] {
        self.0.get(key).map_or(&[], Vec::as_slice)
    }

    /// True when the key appeared at all (even with an empty value).
    pub fn present(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }
}

/// Split `params` into (reserved, filters): a pair whose key is in `keys` is
/// reserved; anything else is a caller filter (order/repeats preserved).
pub fn split_reserved(
    params: Vec<(String, String)>,
    keys: &[&str],
) -> (ReservedParams, Vec<(String, String)>) {
    let mut reserved: HashMap<String, Vec<String>> = HashMap::new();
    let mut filters = Vec::with_capacity(params.len());
    for (k, v) in params {
        if keys.contains(&k.as_str()) {
            reserved.entry(k).or_default().push(v);
        } else {
            filters.push((k, v));
        }
    }
    (ReservedParams(reserved), filters)
}

/// Split a comma-separated list value, dropping empty elements (`"a,,b,"` -> `[a, b]`).
pub fn comma_list(v: &str) -> Vec<String> {
    v.split(',')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

/// Parse the `_ids` object-set input: absent -> empty (no scoping); present with no
/// non-empty element -> the routes' shared 400 message.
pub fn parse_ids(raw: Option<&str>) -> Result<Vec<String>, &'static str> {
    match raw {
        None => Ok(Vec::new()),
        Some(v) => {
            let ids = comma_list(v);
            if ids.is_empty() {
                Err("_ids requires at least one value")
            } else {
                Ok(ids)
            }
        }
    }
}

/// Parse a `depth` value: absent -> `default`; present-but-unparsable -> the routes'
/// shared 400 message. Range policy stays with the caller (the graph routes cap at
/// `MAX_GRAPH_DEPTH`; lineage delegates to the capability's cap).
pub fn parse_depth(raw: Option<&str>, default: u32) -> Result<u32, &'static str> {
    match raw {
        None => Ok(default),
        // Named binding, not `|_|` — the enforced clippy::map_err_ignore lint
        // rejects a wildcard closure param (see engine_client.rs:59 precedent).
        Some(v) => v
            .parse::<u32>()
            .map_err(|_parse_err| "depth must be a positive integer"),
    }
}
```

Add `pub mod query_params;` to `src/services/query-api/src/lib.rs` (after
`pub mod path_parse;`).

- [ ] **Step 4: Run the unit tests to green**

Run: `buck2 test //src/services/query-api:query-params > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log`
Expected: PASS.

- [ ] **Step 5: Rewire the seven scraper loops**

In `http.rs` add (below `parse_bool_flag`):

```rust
/// The shared `/graph` route knobs — `_ids`, `depth` (default + `MAX_GRAPH_DEPTH`
/// range guardrail), `tree` — pulled from the route's reserved params. Err carries
/// the ready 400 response (fixed check order: ids, depth parse, tree, depth range).
fn graph_knobs(
    reserved: &crate::query_params::ReservedParams,
) -> Result<(Vec<String>, u32, bool), axum::response::Response> {
    let ids = crate::query_params::parse_ids(reserved.last("_ids"))
        .map_err(|m| (StatusCode::BAD_REQUEST, m).into_response())?;
    let depth = crate::query_params::parse_depth(reserved.last("depth"), DEFAULT_GRAPH_DEPTH)
        .map_err(|m| (StatusCode::BAD_REQUEST, m).into_response())?;
    let tree = match reserved.last("tree") {
        None => false,
        Some(v) => parse_bool_flag(v).map_err(|()| {
            (StatusCode::BAD_REQUEST, "tree must be true or false").into_response()
        })?,
    };
    if !(1..=MAX_GRAPH_DEPTH).contains(&depth) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("depth must be 1..={MAX_GRAPH_DEPTH}"),
        )
            .into_response());
    }
    Ok((ids, depth, tree))
}
```

`get_object` — replace the loop + locals (:179-202) with:

```rust
    let (reserved, filters) =
        crate::query_params::split_reserved(params, &["_ids", "_or", "limit", "cursor"]);
    let ids = match crate::query_params::parse_ids(reserved.last("_ids")) {
        Ok(ids) => ids,
        Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
    };
    let or_raw: Vec<String> = reserved.all("_or").to_vec();
    let raw_limit = reserved.last("limit").map(String::from);
    let raw_cursor = reserved.last("cursor").map(String::from);
```

(keep the route's explanatory comment, rephrased for the splitter; the rest
of the fn is unchanged — whitelisted change #2 applies here.)

`get_linked` — replace :293-319 (the scraper locals + loop + `_ids` check +
`parse_direction` match — the replacement below includes the direction parse,
so replacing only through :315 would leave a dangling duplicate) with:

```rust
    let (reserved, filter_params) =
        crate::query_params::split_reserved(params, &["_direction", "_shape", "_ids"]);
    let ids = match crate::query_params::parse_ids(reserved.last("_ids")) {
        Ok(ids) => ids,
        Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
    };
    let direction = match parse_direction(reserved.last("_direction")) {
        Ok(d) => d,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
```

and where the tail read `shape.as_deref()`, use `reserved.last("_shape")`.

`get_linked_chain` — replace :373-395 with:

```rust
    let (reserved, filter_params) =
        crate::query_params::split_reserved(params, &["_path", "_shape", "_ids"]);
    let ids = match crate::query_params::parse_ids(reserved.last("_ids")) {
        Ok(ids) => ids,
        Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
    };
    let hops: Vec<Hop> = reserved
        .last("_path")
        .map(parse_path_hops)
        .unwrap_or_default();
```

and its tail's `shape.as_deref()` likewise becomes `reserved.last("_shape")`.

`get_graph` — replace :521-561 with:

```rust
    let (reserved, filters) =
        crate::query_params::split_reserved(params, &["depth", "_ids", "tree"]);
    let (ids, depth, tree) = match graph_knobs(&reserved) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
```

`get_graph_path` — replace :613-663 with:

```rust
    let (reserved, filters) =
        crate::query_params::split_reserved(params, &["path", "links", "depth", "_ids", "tree"]);
    let (ids, depth, tree) = match graph_knobs(&reserved) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let path: Vec<Hop> = reserved.last("path").map(parse_path_hops).unwrap_or_default();
    let links: Vec<String> = reserved
        .last("links")
        .map(crate::query_params::comma_list)
        .unwrap_or_default();
```

(the mode-selection block :664-753 stays as-is until Task 6.)

`lineage_closure` — replace :1115-1131 with:

```rust
    let (reserved, _) =
        crate::query_params::split_reserved(params, &["depth", "after", "limit"]);
    let depth = match crate::query_params::parse_depth(reserved.last("depth"), 1) {
        Ok(d) => d,
        Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
    };
    let after = reserved.last("after").map(String::from);
    let limit = reserved.last("limit").map(String::from);
```

(unknown params were ignored before; the discarded "filters" side keeps that.)

`get_lineage_run_events` — replace :1224-1232 with:

```rust
    let (reserved, _) = crate::query_params::split_reserved(params, &["after", "limit"]);
    let after = reserved.last("after").map(String::from);
    let limit = reserved.last("limit").map(String::from);
```

- [ ] **Step 6: Run the wire pins to green**

Run: `buck2 test //src/services/query-api:query-params //src/services/query-api:http-smoke //src/services/query-api:path-parse > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log`
Expected: PASS.
Run: `buck2 test -j 8 //src/services/query-api:object-set-e2e //src/services/query-api:object-pagination-e2e //src/services/query-api:filter-error-http //src/services/query-api:link-traversal //src/services/query-api:multi-hop-traversal-e2e //src/services/query-api:association-e2e //src/services/query-api:graph-path-e2e //src/services/query-api:graph-tail-e2e //src/services/query-api:lineage-http-e2e //src/services/query-api:http-wire-e2e > /tmp/t5e.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5e.log`
Expected: PASS — all unmodified.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c5.log 2>&1; cat /tmp/c5.log` — artifact empty.

- [ ] **Step 7: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p5.log 2>&1; grep -c Failed /tmp/p5.log` — expected `0`.

```bash
git add src/services/query-api/src/query_params.rs src/services/query-api/src/lib.rs src/services/query-api/src/http.rs src/services/query-api/tests/query_params.rs src/services/query-api/BUCK
git commit -m "refactor(query-api): query_params splitter replaces 7 hand-rolled scraper loops

split_reserved(params, keys) with per-route reserved-key sets (a universal set
would consume other routes' filter columns), last-occurrence-wins single
values, accumulated _or; parse_ids/parse_depth own the shared 400 messages;
graph_knobs bundles the /graph trio + MAX_GRAPH_DEPTH guardrail. Wire behavior
identical except whitelisted #2 (get_object repeated-_ids edge) and #3
(multi-invalid-param 400 precedence). New pure rust_test :query-params.

Part of road-qa-read-path-consolidation."
```

---

### Task 6: http.rs — `parse_graph_mode` + one graph respond tail

**Files:**
- Modify: `src/services/query-api/src/path_parse.rs` (add `GraphMode`,
  `parse_graph_mode`)
- Modify: `src/services/query-api/src/http.rs` (`get_graph` :562-585 tail,
  `get_graph_path` :664-753 tail; `graph_respond`/`graph_tree_respond`
  re-signed; `graph_union_respond`/`graph_tail_respond` deleted — including
  http.rs's one `#[allow(too_many_arguments)]`)
- Test: `src/services/query-api/tests/path_parse.rs` (append; existing target
  `//src/services/query-api:path-parse` — no BUCK change)

**Interfaces:**
- Produces (`pub` in `query_api::path_parse`):

```rust
pub enum GraphMode {
    PathCycle(Vec<Hop>),
    Union(Vec<String>),
    CoreTail { core_link: String, tail_links: Vec<String> },
}
pub fn parse_graph_mode(path: Vec<Hop>, links: Vec<String>, tree: bool) -> Result<GraphMode, String>
```

  Every `Err` string is byte-identical to the message `get_graph_path`
  currently serves for that case, in the same precedence order.
- http.rs keeps `graph_tree_respond(st, q: GraphQuery, subject)` and gains
  `graph_respond(st, spec: GraphReadSpec<'_>, subject)` (consumes Task 4's
  spine).

- [ ] **Step 1: Write the failing tests**

Append to `src/services/query-api/tests/path_parse.rs` (extend its import to
include `GraphMode, parse_graph_mode`, and `Direction, Hop` from
`query_api::handler` if not already imported):

```rust
fn hop(link: &str) -> Hop {
    Hop {
        link: link.to_string(),
        direction: Direction::Forward,
    }
}

fn ihop(link: &str) -> Hop {
    Hop {
        link: link.to_string(),
        direction: Direction::Inverse,
    }
}

#[test]
fn graph_mode_rejects_path_and_links_together() {
    let err = parse_graph_mode(vec![hop("a")], vec!["b".into()], false).unwrap_err();
    assert_eq!(err, "specify either path or links, not both");
}

#[test]
fn graph_mode_union_and_its_tree_rejection() {
    assert!(matches!(
        parse_graph_mode(vec![], vec!["a".into(), "b".into()], false),
        Ok(GraphMode::Union(links)) if links == ["a".to_string(), "b".to_string()]
    ));
    assert_eq!(
        parse_graph_mode(vec![], vec!["a".into()], true).unwrap_err(),
        "tree view is not supported with links (union)"
    );
}

#[test]
fn graph_mode_empty_is_400() {
    assert_eq!(
        parse_graph_mode(vec![], vec![], false).unwrap_err(),
        "path or links requires at least one link"
    );
}

#[test]
fn graph_mode_plain_path_cycle_allows_tree() {
    assert!(matches!(
        parse_graph_mode(vec![hop("knows")], vec![], true),
        Ok(GraphMode::PathCycle(p)) if p.len() == 1
    ));
}

#[test]
fn graph_mode_core_tail_strips_star_and_reemits_inverse_sigil() {
    let got = parse_graph_mode(vec![hop("knows*"), hop("worksAt"), ihop("owns")], vec![], false);
    assert!(matches!(
        got,
        Ok(GraphMode::CoreTail { ref core_link, ref tail_links })
            if core_link == "knows" && *tail_links == ["worksAt".to_string(), "~owns".to_string()]
    ));
}

#[test]
fn graph_mode_star_rules() {
    assert_eq!(
        parse_graph_mode(vec![hop("a*"), hop("b*")], vec![], false).unwrap_err(),
        "at most one path segment may be marked recursive with `*`"
    );
    assert_eq!(
        parse_graph_mode(vec![hop("a"), hop("b*")], vec![], false).unwrap_err(),
        "the recursive `*` segment must be the first path segment"
    );
    assert_eq!(
        parse_graph_mode(vec![hop("*"), hop("b")], vec![], false).unwrap_err(),
        "recursive core link name must not be empty"
    );
    assert_eq!(
        parse_graph_mode(vec![hop("a*"), hop("b")], vec![], true).unwrap_err(),
        "tree view is not supported for a recursive-core (*) path"
    );
    // `~foo*` is NOT a core marker — it stays a path-cycle inverse hop.
    assert!(matches!(
        parse_graph_mode(vec![ihop("foo*")], vec![], false),
        Ok(GraphMode::PathCycle(_))
    ));
}
```

- [ ] **Step 2: Run to see them fail**

Run: `buck2 test //src/services/query-api:path-parse > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t6.log`
Expected: FAIL — `GraphMode`/`parse_graph_mode` unresolved.

- [ ] **Step 3: Implement `parse_graph_mode`**

Append to `src/services/query-api/src/path_parse.rs` (its `use` already
brings `Direction, Hop`):

```rust
/// The recursion structure a `/objects/:type/graph` request selected.
#[derive(Debug)]
pub enum GraphMode {
    /// `?path=l1,..,lk`: an ordered cycle repeated to depth (tree view allowed).
    PathCycle(Vec<Hop>),
    /// `?links=l1,..`: a union of self-links.
    Union(Vec<String>),
    /// `?path=l0*,l1,..`: a recursive core + relational tail. `core_link` has the
    /// `*` stripped; each tail hop is re-emitted by name with the `~` sigil
    /// re-attached for inverse hops (a forward-only tail resolves `~x` as an
    /// absent forward link rather than silently dropping the sigil).
    CoreTail {
        core_link: String,
        tail_links: Vec<String>,
    },
}

/// Select the graph mode from the parsed `path`/`links` params and the `tree` flag,
/// enforcing the route's grammar in its historical precedence order. Every `Err`
/// string is served verbatim as the 400 body. A `*`-suffixed FORWARD segment marks
/// the recursive core; `~foo*` is NOT a core (it stays a path-cycle inverse hop
/// whose name ends in `*`, resolving to UnknownLink downstream).
pub fn parse_graph_mode(
    path: Vec<Hop>,
    links: Vec<String>,
    tree: bool,
) -> Result<GraphMode, String> {
    if !path.is_empty() && !links.is_empty() {
        return Err("specify either path or links, not both".to_string());
    }
    if !links.is_empty() {
        if tree {
            return Err("tree view is not supported with links (union)".to_string());
        }
        return Ok(GraphMode::Union(links));
    }
    if path.is_empty() {
        return Err("path or links requires at least one link".to_string());
    }
    let starred: Vec<usize> = path
        .iter()
        .enumerate()
        .filter(|(_, h)| h.direction == Direction::Forward && h.link.ends_with('*'))
        .map(|(i, _)| i)
        .collect();
    if starred.is_empty() {
        return Ok(GraphMode::PathCycle(path));
    }
    if tree {
        return Err("tree view is not supported for a recursive-core (*) path".to_string());
    }
    if starred.len() > 1 {
        return Err("at most one path segment may be marked recursive with `*`".to_string());
    }
    if starred.first().copied().unwrap_or(0) != 0 {
        return Err("the recursive `*` segment must be the first path segment".to_string());
    }
    let Some(first_hop) = path.first() else {
        return Err("empty path".to_string());
    };
    let core_link = first_hop.link.trim_end_matches('*').to_string();
    if core_link.is_empty() {
        return Err("recursive core link name must not be empty".to_string());
    }
    let tail_links: Vec<String> = path
        .get(1..)
        .unwrap_or_default()
        .iter()
        .map(|h| match h.direction {
            Direction::Forward => h.link.clone(),
            Direction::Inverse => format!("~{}", h.link),
        })
        .collect();
    Ok(GraphMode::CoreTail {
        core_link,
        tail_links,
    })
}
```

(Precedence check against the old `get_graph_path`: current code checks
tree-with-star at :699 BEFORE starred-count at :706 — preserved. NOTE the old
code checked tree-with-star only when `starred` was non-empty; identical
here.)

- [ ] **Step 4: Run the mode tests to green**

Run: `buck2 test //src/services/query-api:path-parse > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6.log`
Expected: PASS.

- [ ] **Step 5: Collapse the respond tails in http.rs**

Add `GraphReadSpec` and `read_graph_reach_spec` to the `crate::handler` import
list. Replace `graph_respond` (:773-804), `graph_tree_respond` (:809-840),
`graph_union_respond` (:844-875), and `graph_tail_respond` (:879-916, incl.
its `#[allow(too_many_arguments)]`) with:

```rust
/// The one /graph reachability tail: run the spec'd read via the handler spine,
/// render, map errors.
async fn graph_respond(
    st: &AppState,
    spec: GraphReadSpec<'_>,
    subject: &Subject,
) -> axum::response::Response {
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
        default_limit: st.default_limit,
    };
    match read_graph_reach_spec(spec, subject, &deps).await {
        Ok(rows) => Json(crate::render::objects_to_json(&rows, None)).into_response(),
        Err(e) => graph_error(e),
    }
}

/// Tree tail for `?tree=true` on the path-cycle routes: run `read_graph_tree`,
/// render `{roots, nodes}` via `tree_to_json`, map errors via `graph_error`.
async fn graph_tree_respond(st: &AppState, q: GraphQuery, subject: &Subject) -> axum::response::Response {
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
        default_limit: st.default_limit,
    };
    match read_graph_tree(&q, subject, &deps).await {
        Ok(tree) => Json(crate::render::tree_to_json(&tree)).into_response(),
        Err(e) => graph_error(e),
    }
}
```

(The `QueryDeps` literals collapse onto `st.deps()` in Task 8.)

`get_graph`'s tail (after Task 5's knobs) becomes:

```rust
    let q = GraphQuery {
        type_name,
        path: vec![link_name.into()],
        depth,
        filters,
        ids,
    };
    if tree {
        graph_tree_respond(&st, q, &subject).await
    } else {
        graph_respond(&st, GraphReadSpec::PathCycle(&q), &subject).await
    }
```

`get_graph_path`'s tail (everything from the old both-given check through the
end of the fn) becomes:

```rust
    match crate::path_parse::parse_graph_mode(path, links, tree) {
        Err(m) => (StatusCode::BAD_REQUEST, m).into_response(),
        Ok(crate::path_parse::GraphMode::Union(links)) => {
            let q = GraphUnionQuery {
                type_name,
                links,
                depth,
                filters,
                ids,
            };
            graph_respond(&st, GraphReadSpec::UnionSelfLinks(&q), &subject).await
        }
        Ok(crate::path_parse::GraphMode::CoreTail {
            core_link,
            tail_links,
        }) => {
            let q = GraphTailQuery {
                type_name,
                core_link,
                tail_links,
                depth,
                filters,
                ids,
            };
            graph_respond(&st, GraphReadSpec::CoreTail(&q), &subject).await
        }
        Ok(crate::path_parse::GraphMode::PathCycle(path)) => {
            let q = GraphQuery {
                type_name,
                path,
                depth,
                filters,
                ids,
            };
            if tree {
                graph_tree_respond(&st, q, &subject).await
            } else {
                graph_respond(&st, GraphReadSpec::PathCycle(&q), &subject).await
            }
        }
    }
```

- [ ] **Step 6: Run the graph wire pins to green**

Run: `buck2 test //src/services/query-api:path-parse //src/services/query-api:http-smoke > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6.log`
Expected: PASS.
Run: `buck2 test -j 8 //src/services/query-api:graph-reach-e2e //src/services/query-api:graph-path-e2e //src/services/query-api:graph-union-e2e //src/services/query-api:graph-tail-e2e //src/services/query-api:graph-tree-e2e //src/services/query-api:graph-inverse-e2e //src/services/query-api:inverse-hops-e2e > /tmp/t6e.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6e.log`
Expected: PASS — every mode/error-message assertion unmodified.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c6.log 2>&1; cat /tmp/c6.log` — artifact empty (http.rs's
`too_many_arguments` allow is gone).

- [ ] **Step 7: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p6.log 2>&1; grep -c Failed /tmp/p6.log` — expected `0`.

```bash
git add src/services/query-api/src/path_parse.rs src/services/query-api/src/http.rs src/services/query-api/tests/path_parse.rs
git commit -m "refactor(query-api): parse_graph_mode + single graph respond tail

path_parse::parse_graph_mode owns /graph's mode grammar (path-vs-links
exclusivity, the * recursive-core rules, tree restrictions) with byte-identical
400 messages in the historical precedence order; get_graph_path becomes a
scrape + one match. graph_union_respond/graph_tail_respond fold into one
graph_respond over handler::GraphReadSpec, deleting http.rs's
too_many_arguments allow.

Part of road-qa-read-path-consolidation."
```

---

### Task 7: http.rs — one total `QueryError → Response` mapping

**Files:**
- Modify: `src/services/query-api/src/http.rs` (new `query_error_response`;
  delete `chain_error` + `graph_error`; rewire `get_object` ×2,
  `respond_objects`, `respond_associations`, `graph_respond`,
  `graph_tree_respond`, `post_search`)
- Create: `src/services/query-api/tests/query_error_http.rs`
- Modify: `src/services/query-api/BUCK` (new `query-error-http` target)

**Interfaces:**
- Produces: `pub fn query_error_response(e: QueryError, context: &'static str)
  -> axum::response::Response` in `query_api::http` — total over `QueryError`
  (no catch-all arm over its variants; a new variant fails compilation here).
  `bad_filter_value_response` and `internal_error` stay as its delegates.

- [ ] **Step 1: Write the failing tests**

Create `src/services/query-api/tests/query_error_http.rs`:

```rust
//! Totality pin for the single QueryError -> HTTP response mapping. The match in
//! http.rs has no catch-all over QueryError's variants (a NEW variant fails
//! compilation there); this test pins the status each existing variant maps to —
//! the union of the four partial per-route copies it replaced.
use axum::http::StatusCode;
use control_plane_core::ControlPlaneError;
use query_api::filter::FilterError;
use query_api::handler::QueryError;
use query_api::http::query_error_response;
use query_api::serving::ServingError;
use query_api::sql::CompileError;

fn status(e: QueryError) -> StatusCode {
    query_error_response(e, "test context").status()
}

#[test]
fn not_found_variants() {
    assert_eq!(status(QueryError::UnknownType("T".into())), StatusCode::NOT_FOUND);
    assert_eq!(status(QueryError::UnknownLink("l".into())), StatusCode::NOT_FOUND);
    assert_eq!(
        status(QueryError::Serving(ServingError::NoIndex("i".into()))),
        StatusCode::NOT_FOUND
    );
}

#[test]
fn bad_request_variants() {
    assert_eq!(status(QueryError::AmbiguousLink("l".into())), StatusCode::BAD_REQUEST);
    assert_eq!(status(QueryError::BadFilter("c".into())), StatusCode::BAD_REQUEST);
    assert_eq!(
        status(QueryError::BadFilterValue(FilterError::BadValue(
            "c".into(),
            "m".into()
        ))),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(status(QueryError::BadChain("m".into())), StatusCode::BAD_REQUEST);
    assert_eq!(status(QueryError::NoIdentity("t".into())), StatusCode::BAD_REQUEST);
    assert_eq!(status(QueryError::NotCyclicPath("p".into())), StatusCode::BAD_REQUEST);
    assert_eq!(status(QueryError::BadGraphPath("m".into())), StatusCode::BAD_REQUEST);
    assert_eq!(status(QueryError::BadPagination("m".into())), StatusCode::BAD_REQUEST);
    assert_eq!(
        status(QueryError::Serving(ServingError::DimMismatch("d".into()))),
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn forbidden_is_403() {
    assert_eq!(status(QueryError::Forbidden), StatusCode::FORBIDDEN);
}

#[test]
fn internal_variants_are_opaque_500() {
    assert_eq!(
        status(QueryError::ControlPlane(ControlPlaneError::NotFound("x".into()))),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        status(QueryError::Serving(ServingError::Engine("e".into()))),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        status(QueryError::Malformed(CompileError::MalformedFilter("f".into()))),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}
```

Add to `src/services/query-api/BUCK` (next to `query-params`):

```python
rust_test(
    name = "query-error-http",
    crate = "query_error_http",
    srcs = ["tests/query_error_http.rs"],
    crate_root = "tests/query_error_http.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//third-party:axum",
    ],
)
```

- [ ] **Step 2: Run to see them fail**

Run: `buck2 test //src/services/query-api:query-error-http > /tmp/t7.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t7.log`
Expected: FAIL — `query_error_response` unresolved.

- [ ] **Step 3: Implement the total mapping and rewire every site**

Add to `http.rs`, replacing `chain_error` (:467-479) and `graph_error`
(:756-769) — both deleted:

```rust
/// The single, TOTAL `QueryError` -> HTTP response mapping — the union of the four
/// partial per-route copies it replaced (object/chain/graph/search). No catch-all
/// over `QueryError`'s variants: adding a variant fails compilation here, forcing a
/// deliberate status. Client-fault variants echo only caller-supplied names (type/
/// column/link — no internal detail); backend faults go through `internal_error`
/// (logged server-side, opaque body). `Serving` splits: `NoIndex` is the /search
/// 404, `DimMismatch` its 400 — both constructed only on the vector-search path —
/// and everything else is an opaque 500.
pub fn query_error_response(e: QueryError, context: &'static str) -> axum::response::Response {
    match e {
        QueryError::UnknownType(t) => (StatusCode::NOT_FOUND, t).into_response(),
        QueryError::UnknownLink(l) => (StatusCode::NOT_FOUND, l).into_response(),
        QueryError::AmbiguousLink(l) => (StatusCode::BAD_REQUEST, l).into_response(),
        QueryError::Forbidden => StatusCode::FORBIDDEN.into_response(),
        QueryError::BadFilter(c) => (StatusCode::BAD_REQUEST, c).into_response(),
        QueryError::BadFilterValue(e) => bad_filter_value_response(&e),
        QueryError::BadChain(m) => (StatusCode::BAD_REQUEST, m).into_response(),
        QueryError::NoIdentity(t) => (StatusCode::BAD_REQUEST, t).into_response(),
        QueryError::NotCyclicPath(p) => (StatusCode::BAD_REQUEST, p).into_response(),
        QueryError::BadGraphPath(m) => (StatusCode::BAD_REQUEST, m).into_response(),
        QueryError::BadPagination(m) => (StatusCode::BAD_REQUEST, m).into_response(),
        QueryError::Serving(crate::serving::ServingError::NoIndex(m)) => {
            (StatusCode::NOT_FOUND, m).into_response()
        }
        QueryError::Serving(crate::serving::ServingError::DimMismatch(m)) => {
            (StatusCode::BAD_REQUEST, m).into_response()
        }
        e @ (QueryError::ControlPlane(_) | QueryError::Serving(_) | QueryError::Malformed(_)) => {
            internal_error(context, e)
        }
    }
}
```

Rewire (contexts preserve today's log lines exactly):

- `get_object` paginated match (:236-246) →
  `Ok((rows, next)) => Json(crate::render::objects_to_json(&rows, next.as_ref())).into_response(), Err(e) => query_error_response(e, "object read serving fault"),`
- `get_object` plain match (:260-267) →
  `Ok(rows) => Json(crate::render::objects_to_json(&rows, None)).into_response(), Err(e) => query_error_response(e, "object read serving fault"),`
- `respond_objects` / `respond_associations` `Err` arms →
  `query_error_response(e, "chain/association read serving fault")`
- `graph_respond` / `graph_tree_respond` `Err` arms →
  `query_error_response(e, "graph read serving fault")`
- `post_search` (:1073-1082) — the whole error tail collapses:

```rust
    match crate::handler::vector_search(&q, &subject, &deps).await {
        Ok(hits) => {
            let results: Vec<serde_json::Value> = hits
                .iter()
                .map(|h| serde_json::json!({ "id": id_json(&h.id), "distance": h.distance }))
                .collect();
            Json(serde_json::json!({ "results": results })).into_response()
        }
        Err(e) => query_error_response(e, "vector search serving fault"),
    }
```

- [ ] **Step 4: Run to green**

Run: `buck2 test //src/services/query-api:query-error-http //src/services/query-api:http-smoke > /tmp/t7.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t7.log`
Expected: PASS.
Run: `buck2 test -j 8 //src/services/query-api:object-set-e2e //src/services/query-api:object-pagination-e2e //src/services/query-api:filter-error-http //src/services/query-api:link-traversal //src/services/query-api:association-e2e //src/services/query-api:graph-path-e2e //src/services/query-api:vector_search_e2e //src/services/query-api:http-wire-e2e > /tmp/t7e.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t7e.log`
Expected: PASS — every status/body assertion unmodified (whitelist #4's only
constructible change — `/search` `BadFilterValue` 500→400 — fires solely on a
pathological engine/ontology inconsistency no e2e seeds).
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c7.log 2>&1; cat /tmp/c7.log` — artifact empty.

- [ ] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p7.log 2>&1; grep -c Failed /tmp/p7.log` — expected `0`.

```bash
git add src/services/query-api/src/http.rs src/services/query-api/tests/query_error_http.rs src/services/query-api/BUCK
git commit -m "refactor(query-api): one total QueryError -> HTTP response mapping

query_error_response unions the four partial per-route copies (get_object x2,
chain_error, graph_error) plus post_search's Serving special cases. No
catch-all over QueryError variants — a new variant fails compilation, forcing
a deliberate status. One constructible change, whitelisted (#4): /search's
BadFilterValue (engine-served identity failing round-trip coercion) goes
opaque-500 -> structured 400; everything else newly mapped is a defensive
dead arm (NoIndex/DimMismatch are vector-search-only; the read paths cannot
emit the link/graph variants). New pure rust_test :query-error-http pins
every variant's status.

Part of road-qa-read-path-consolidation."
```

---

### Task 8: http.rs — `AppState::deps()` + shared `respond_shaped` chain tail

**Files:**
- Modify: `src/services/query-api/src/http.rs` (`impl AppState`; the 8
  `QueryDeps` literals — post-Task-6 they live in `get_object`, `get_linked`,
  `get_linked_chain`, `graph_respond`, `graph_tree_respond`, `post_search`;
  new `respond_shaped`; `get_linked`/`get_linked_chain` tails)

**Interfaces:**
- Produces: `impl AppState { pub fn deps(&self) -> QueryDeps<'_> }` and the
  private
  `async fn respond_shaped(st: &AppState, query: ChainQuery, shape: Option<&str>, subject: &Subject) -> axum::response::Response`.

- [ ] **Step 1: Implement**

```rust
impl AppState {
    /// The per-request borrowed dependency bundle every read handler passes down —
    /// one construction point instead of a hand-built literal per route.
    pub fn deps(&self) -> QueryDeps<'_> {
        QueryDeps {
            ontology: self.cp.ontology(),
            acl: self.cp.acl(),
            serving: self.serving.as_ref(),
            default_limit: self.default_limit,
        }
    }
}
```

Replace every remaining `let deps = QueryDeps { … };` literal with
`let deps = st.deps();` (in `graph_respond`/`graph_tree_respond`: `st.deps()`
on their `&AppState` param).

Add the shared chain tail (next to `respond_objects`):

```rust
/// The shared single-hop/chain response tail: dispatch `_shape` (objects default,
/// association pairs, unknown -> 400) over the governed chain read. The get_linked
/// and get_linked_chain tails were verbatim copies of this.
async fn respond_shaped(
    st: &AppState,
    query: ChainQuery,
    shape: Option<&str>,
    subject: &Subject,
) -> axum::response::Response {
    let deps = st.deps();
    match shape {
        None | Some("objects") => {
            respond_objects(read_linked_chain(&query, subject, &deps).await)
        }
        Some("association") => {
            respond_associations(read_associations(&query, subject, &deps).await)
        }
        Some(other) => (StatusCode::BAD_REQUEST, format!("unknown shape: {other}")).into_response(),
    }
}
```

`get_linked`'s tail (deps literal + `ChainQuery` + shape match) becomes:

```rust
    let query = ChainQuery {
        from_type,
        path: vec![Hop {
            link: link_name,
            direction,
        }],
        filters,
        ids,
    };
    respond_shaped(&st, query, reserved.last("_shape"), &subject).await
```

`get_linked_chain`'s tail becomes:

```rust
    let query = ChainQuery {
        from_type,
        path: hops,
        filters,
        ids,
    };
    respond_shaped(&st, query, reserved.last("_shape"), &subject).await
```

- [ ] **Step 2: Run the wire pins to green**

Run: `buck2 test //src/services/query-api:http-smoke //src/services/query-api:query-params //src/services/query-api:query-error-http > /tmp/t8.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t8.log`
Expected: PASS.
Run: `buck2 test -j 8 //src/services/query-api:link-traversal //src/services/query-api:multi-hop-traversal-e2e //src/services/query-api:association-e2e //src/services/query-api:object-set-e2e //src/services/query-api:vector_search_e2e //src/services/query-api:graph-path-e2e > /tmp/t8e.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t8e.log`
Expected: PASS — all unmodified.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c8.log 2>&1; cat /tmp/c8.log` — artifact empty.

- [ ] **Step 3: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p8.log 2>&1; grep -c Failed /tmp/p8.log` — expected `0`.

```bash
git add src/services/query-api/src/http.rs
git commit -m "refactor(query-api): AppState::deps() + shared respond_shaped chain tail

deps() replaces the eight hand-built QueryDeps literals; respond_shaped is the
one _shape dispatch the get_linked/get_linked_chain tails carried verbatim
(the register's census pair). Wire behavior identical.

Part of road-qa-read-path-consolidation."
```

---

### Task 9: flight_export.rs — one governed-read helper for both Flight verbs

**Files:**
- Modify: `src/services/query-api/src/flight_export.rs` (`get_flight_info`
  :196-212, `do_get` :240-256; new `impl FlightExportService` method)

**Interfaces:**
- Produces: private
  `async fn governed(&self, cmd: ExportCommand, subject: SubjectId) -> Result<crate::handler::GovernedRead, Status>`.
- Consumes: `compile_object_read` (unchanged), `map_query_err` (unchanged).

- [ ] **Step 1: Implement the helper and collapse both call sites**

Add to the existing `impl FlightExportService` block (after `new`):

```rust
    /// The one governed-read compile both Flight verbs share: govern `cmd` for THIS
    /// authenticated subject and compile the ACL'd SELECT with the row cap + 1 (so an
    /// over-cap slice is detectable, not silently truncated). `get_flight_info` uses
    /// the result's schema; `do_get` executes its SQL — a forged/replayed ticket is
    /// still a governed request because governance re-derives here per call.
    async fn governed(
        &self,
        cmd: ExportCommand,
        subject: SubjectId,
    ) -> Result<crate::handler::GovernedRead, Status> {
        compile_object_read(
            &ObjectQuery {
                type_name: cmd.type_name,
                filters: cmd.filters,
                ids: cmd.ids,
                or_raw: Vec::new(),
            },
            &Subject(subject),
            self.cp.ontology(),
            self.cp.acl(),
            &DataFusionDialect,
            self.max_rows.saturating_add(1),
            None,
            None,
        )
        .await
        .map_err(map_query_err)
    }
```

In `get_flight_info`, replace :196-212 with (it still needs `cmd.encode()`
for the ticket below, hence the clone):

```rust
        let governed = self.governed(cmd.clone(), subject).await?;
```

In `do_get`, replace :240-256 with:

```rust
        let governed = self.governed(cmd, subject).await?;
```

(Delete the now-redundant per-site comment in `do_get`; its content lives on
the helper. Imports: `crate::handler::GovernedRead` is already reachable via
the existing `crate::handler::{…}` use — extend that line with
`GovernedRead`.)

- [ ] **Step 2: Run the flight pins to green**

Run: `buck2 test //src/services/query-api:export-command > /tmp/t9.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t9.log`
Expected: PASS.
Run: `buck2 test -j 8 //src/services/query-api:governed-flight-export-e2e > /tmp/t9e.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t9e.log`
Expected: PASS — schema/data/governance assertions unmodified.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c9.log 2>&1; cat /tmp/c9.log` — artifact empty.

- [ ] **Step 3: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p9.log 2>&1; grep -c Failed /tmp/p9.log` — expected `0`.

```bash
git add src/services/query-api/src/flight_export.rs
git commit -m "refactor(query-api): one governed-read helper for the Flight export verbs

get_flight_info and do_get carried verbatim copies of the compile_object_read
block (subject-scoped governance + cap+1 compile). governed(cmd, subject) is
the single copy; per-subject re-derivation semantics unchanged.

Part of road-qa-read-path-consolidation."
```

---

### Task 10: Full-suite sweep + register close

**Files:**
- Modify: `docs/ROADMAP.md` (`road-qa-read-path-consolidation` :292-293)

- [ ] **Step 1: Full query-api suite**

Run: `buck2 build -M none //src/services/query-api/... > /tmp/b10.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)" /tmp/b10.log`
Expected: `BUILD SUCCEEDED`.
Run: `buck2 test -j 8 //src/services/query-api/... > /tmp/t10.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t10.log`
Expected: PASS (fixture-heavy — the `-j 8` cap avoids postgres boot-slot
starvation). If any unrelated fixture test flakes on timeout, re-run that
target alone before investigating.

- [ ] **Step 2: Close the register item**

In `docs/ROADMAP.md:292-293`, flip the checkbox/status and replace the prose:

```markdown
- [x] **query-api read-path consolidation (sql.rs / graph / http.rs)** `{#road-qa-read-path-consolidation area:quality status:done from:2026-07-02-pillar-idioms-audit-design pr:- spec:2026-07-02-pillar-idioms-audit-design}`
  Done (PR #-). **sql.rs:** the union/CTE/tail/chain compilers reuse `reach_seed_where`/`reach_recursive_where`/`validate_reach_filters`/`reach_projection_where`/`masked_col_exprs` (byte-identical SQL, pinned by exact full-SQL tests added ahead of the substitution — the pre-existing graph tests asserted fragments only); `ReachSpec<'a>` replaces the positional prefix of the five reach-family compilers, deleting their five `#[allow(too_many_arguments)]` (the register's "six" was drift — seven existed; the two `compile_select_with`/`compile_select` allows are a different parameter shape and deliberately remain); the three `#[expect(indexing_slicing)]` in `caller_predicate_sql` became fallible slice patterns (`CompileError::MalformedFilter`, fail-closed without panicking the request task). **Graph:** `governed_identity` + per-variant compile stages + `read_graph_reach_spec` over a `GraphReadSpec` borrow enum collapse the three parallel entry points onto one spine (pub fns kept as delegates — handler tests and e2es, incl. the `final_g`-fold pin in graph_tail_e2e, passed unmodified); `path_parse::parse_graph_mode` owns `/graph`'s mode grammar. **http.rs:** `query_params::{split_reserved,parse_ids,parse_depth}` replaces the 7 hand-rolled scraper loops (per-route reserved-key sets); one **total** `query_error_response` replaces the four partial `QueryError` mappings + post_search's fifth; `AppState::deps()` replaces 8 literals (census said 7); `respond_shaped` collapses the get_linked/get_linked_chain census pair; flight_export's duplicated governed-read block is one `governed()` helper. Deliberate behavior changes were limited to the plan's four-item whitelist (panic→500 on the predicate-arity invariant; two degenerate repeated/multi-invalid-param edges; the total-mapping unification — dead defensive arms plus one constructible `/search` `BadFilterValue` 500→400 on a pathological engine/ontology inconsistency).
```

Run: `bash tools/docs.sh validate`
Expected: exit 0, no grammar/id/vocab errors.

- [ ] **Step 3: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p10.log 2>&1; grep -c Failed /tmp/p10.log` — expected `0`.

```bash
git add docs/ROADMAP.md
git commit -m "docs(registers): close road-qa-read-path-consolidation

Register prose records the verified drift (7 too_many_arguments not 6 — the
two select-family allows deliberately remain; 8 QueryDeps literals not 7) and
the four-item behavior-change whitelist. pr:- updated when the PR opens.

Part of road-qa-read-path-consolidation."
```

(When the branch's PR is opened, update `pr:-` to `pr:#N` in the same PR.)
