# road-cp-adapter-hygiene Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the spec's control-plane adapter hygiene batch — a set of small,
independently verifiable idiom cleanups across `src/control-plane/{postgres,memory,core}`
— behavior-preserving except THREE whitelisted changes (fail-loud enum codecs,
parse/dim error reclassification, memory tx replay order).

**Architecture:** Each surviving spec sub-item becomes one task/commit. Core
gains the shared vocabulary (enum wire codecs, `PolicyTarget::key_parts`,
policy write-time checks, `PageReq` fetch-limit helpers, `Validation`-classed
parse/dim errors); the postgres adapter deduplicates its own boilerplate
(exists checks, `LinkRow`, one `backend()` boxing helper, inline-table
preamble, batched stat inserts, N+1 collapses); the memory adapter converges
on `TableRef` keys and a postgres-ordered staged-write log. The testkit
contracts pin every surface; where a surface is weakly pinned the task adds a
contract/fixture pin FIRST, green against current code.

**Tech Stack:** Rust (edition 2024), buck2, sqlx 0.9 compile-time queries,
testkit contract tests, `loom_fixture_test` + `PgFixture::shared()` for
anything booting postgres.

**Register drift (verified against the tree on branch
`work/road-cp-adapter-hygiene`, 2026-07-03 — ALL spec/register line numbers
are stale; every claim below re-anchored by symbol):**

- **The spec's final sub-item — "the `dc!` fallible-macro fix
  (`iss-inline-downcast-panic`) if not already shipped" — is ALREADY SHIPPED.**
  PR #296 made `cell_from_arrow`'s `dc!` macro fallible
  (`iceberg_inline.rs:158-166` now `ok_or_else(.. Validation ..)`), and the
  ISSUES entry is closed. Dropped from this batch; nothing to close in the
  registers for it.
- **The spec's "`.sqlx` cache refresh required" applies to only TWO tasks.**
  Only Task 7 (unnest stat inserts) and Task 9 (N+1 collapses) change
  compile-time SQL text. The exists-helper and `LinkRow` dedups reuse the
  byte-identical SQL strings (the `.sqlx` cache is keyed by query-text hash,
  so deduplicating call sites of an identical string produces no cache delta),
  and the inline-path work (Tasks 6) touches only runtime `AssertSqlSafe`
  queries. If any OTHER task finds itself editing a `query!`/`query_as!`/
  `query_scalar!` string, stop and re-plan.
- **The spec's `existing_inline_table(conn, table)` lands as
  `inline_table_exists(conn, table_id)`** — every call site has already
  resolved the mirror `table_id` (it is needed for `inline_table_name`), so
  taking a `TableRef` would force a redundant `live_table_id` lookup.
- **The spec's `PageReq::fetch_limit()` lands as two typed methods**
  (`fetch_limit_i64` for the SQL `LIMIT` bind, `fetch_take` for the iterator
  take-count) — the two adapters consume the "+1 sentinel" in different
  integer domains, which is exactly why two overflow styles evolved.
- **`set_policy`'s "cc 17" census figure predates #308/#320 churn** — not
  re-measured; the exists-helper + core-check extraction shrinks it regardless.

## Verified claim inventory (spec sub-item → verdict, all evidence current)

| # | Spec sub-item | Verdict | Current evidence |
| --- | --- | --- | --- |
| A | Exists-check helpers; `select exists(...)` "9×"; most of `set_policy`'s cc | CONFIRMED — 10×, one more than the spec counted | The two shared texts appear 10×: role-exists ×4 (`acl.rs:57` assign_role, `:137` add_inheritance in-tx, `:204` grant, `:273` set_policy); object-type-exists ×6 (`ontology.rs:115` define_link, `:230` links, `:272` links_to, `:334` define_action IN-TX (`&mut *tx`), `acl.rs:219` grant, `:291` set_policy). Other `select exists` sites (subject `acl.rs:44`, role_member `:83`, cycle-CTE `:155`, `auth.rs`, `iceberg_mirror.rs:423`) are distinct one-off queries — out of scope |
| B | `links`/`links_to` dedup via `query_as!` + named `LinkRow` | CONFIRMED | `ontology.rs:228-268` ≈ `:270-310`: byte-identical except `from_type = $1` vs `to_type = $1`; identical exists-preamble + row→`LinkDef` mapping |
| C | Shared policy-target validation (pure decision fn in core, adapter supplies lookups) | CONFIRMED | postgres `acl.rs:217-232` (grant) + `:289-323` (set_policy) duplicated in memory `acl.rs:192-199` + `:229-247`, identical error strings |
| D | Enum↔string codecs to core, fail-loud (`Cardinality`, `EventType`, `Action`, `Effect`, `ActionKind`) | CONFIRMED | postgres `lib.rs:127-174`: `cardinality_from_str` silently coerces to `One` (`:137`), `event_type_from_str` to `Start` (`:157`); `ActionKind` inline in `ontology.rs:347-351` (to) + `:433-437` (from, silent `Insert`). `Action`/`Effect` have to-str only (never parsed back — `check` computes `Decision` in SQL). Memory stores enums directly — codecs are postgres-only consumers today |
| E | `PolicyTarget::key_parts()` replaces twice-written target encoding | CONFIRMED | postgres `lib.rs:176` `target_cols` + memory `acl.rs:12-24` `target_key` — same encoding written twice |
| F | One `backend()` boxing helper (spec says "four variants, two Display-flatten") | CONFIRMED — actually SIX named variants (three Display-flatten) plus inline sites | Named: `lib.rs:123` `backend(sqlx::Error)` (source-carrying), `iceberg_read.rs:19` `be` (generic, source-carrying), `iceberg_landing.rs:38` `be` (generic, source-carrying, 24 `map_err(be)` sites), `iceberg_mirror.rs:16` `iceberg_err(iceberg::Error)` (source-carrying, 4 sites `:383,392,401,404`), `puffin.rs:16` `be<Display>` (FLATTENS, 7 sites), **`vector_index.rs:16` `backend<Display>` (FLATTENS and SHADOWS `lib.rs::backend` — its 10 `map_err(backend)` sites `:77,101,148,160,177,279,291,355,562,591` all sever sources today)**. Inline flattens: `iceberg_flush.rs:113`, `iceberg_inline.rs:558`, `vector_index.rs:201,205,373,472,476`. Inline `\|e\| Backend(Box::new(e))` closures: `lib.rs:93`, `auth.rs:350`, `iceberg_stats.rs:69`, `iceberg_inline.rs:58,69,254`, `commit_mirror.rs:93`. `auth.rs` `conflict_or_backend`/`notfound_or_backend` are domain mappers, NOT variants — kept |
| G | Reclassify parse/dim failures `Backend`→`Validation` | CONFIRMED (edit); impact framing corrected | `Metric::from_str` (`core/src/vector_index/mod.rs:117`), `IndexKind::from_str` (`:151`), `IndexSpec::from_label` (`:60`), `pack_rows` dim mismatch (`codec.rs:202`) all return `Backend`. No existing test pins the class (all assert `.is_err()` only: `core/tests/vector_index.rs:65,91,228,385`, `index_spec_build.rs:70`). **No status code changes today**: the search path class-erases via `to_serving` → `EngineServingError::Engine` (internal) before and after; the governance plane's status mapping sends `Backend` AND `Validation` to internal alike; the worker's `JobFailure` policy is class-insensitive; #317's `Validation`→`InvalidArgument` lives on the Flight SQL plane these errors never traverse. The change is honest classification + the `"validation error: "` Display prefix — groundwork for future planes |
| H | Inline-table access preamble + `mvcc_live_pred` + `quote_ident` | CONFIRMED | formatted `to_regclass('{}')` preamble ×3: `iceberg_inline.rs:114-121` (`has_live_inline_rows`), `:491-498` (`inline_live_batch`), `vector_index.rs:284-292` (`inline_delta_batch`, still there post-#320); live-pred text duplicated `iceberg_inline.rs:124-127`, `:511-514`, `vector_index.rs:347-349` (as the alive-at conjuncts); quote-escape pattern ×5 (`iceberg_inline.rs:253,330,506`, `vector_index.rs:342-343`) |
| I | `inline_append` invariant SQL out of per-row loop | CONFIRMED | `iceberg_inline.rs:333-345` — `placeholders` + `sql` rebuilt inside `for row in 0..batch.num_rows()` |
| J | `unnest` the `project_files` stat inserts | CONFIRMED | `iceberg_mirror.rs:150-168` — one INSERT per column-stat inside the per-file loop. SQL text changes → `.sqlx` refresh |
| K | `PageReq::fetch_limit()` (+1 sentinel, two overflow styles) | CONFIRMED | postgres `lineage.rs:69,172` `map_or(i64::MAX, \|l\| i64::from(l) + 1)`; memory `lineage.rs:75-81,112-118` `usize::try_from(l).unwrap_or(usize::MAX).saturating_add(1)` |
| L | Memory adapter keyed by `TableRef` | CONFIRMED | memory `catalog.rs:16-18` `HashMap<(String, String), _>` ×3 + tuple keys rebuilt throughout `catalog.rs`/`transaction.rs`; `TableRef` derives `Eq + Hash` (`core/src/catalog.rs:19`) |
| M | `vector_indexes_for` N+1 → one query | CONFIRMED | postgres `ontology.rs:517-532` — name-list query then `vector_index_def_row` per name. Contract sorts names before asserting (`testkit:1211-1219`) — order-insensitive. New SQL text → `.sqlx` refresh |
| N | `events_for` N+1 → `event_id = any($1)` | CONFIRMED | postgres `lineage.rs:81-96` — 2 `event_datasets` queries per event row; `event_datasets` has no other callers. New SQL text → `.sqlx` refresh. Cross-reference `fut-lineage-events-page-hydration` at close |
| O | Memory `transaction.rs::commit` → ordered `StagedOp` log mirroring postgres | CONFIRMED + real divergence | memory applies grouped `staged_files` THEN `staged_replacements` (`transaction.rs:120-161`); postgres `IcebergTx` keeps one `staged_files: Vec<(_, _, WriteMode)>` applied in staged order (`iceberg_control_plane.rs:91,125`). Replace-then-append in ONE tx: postgres leaves the append live; memory end-caps it — divergent, unpinned by any contract |
| P | `dc!` fallible-macro fix | ABSORBED by #296 | `iceberg_inline.rs:158-166` already returns `Validation`; ISSUES entry closed. DROPPED |

**Testkit pin map (which contracts pin each task):** A/B/C/E →
`ontology_contract`, `acl_contract`, `existence_validation_contract` (both
adapters); D → same + a NEW corrupt-row fixture pin (Task 2) +
`lineage_contract`; F → whole postgres fixture suite (Display text unchanged —
`Backend` is `#[error(transparent)]`, and `Box::new(e)` and
`e.to_string().into()` display identically); G → NEW class-asserting unit
tests (Task 5) + engine-serving/query-api vector suites; H/I → postgres inline
suites (`iceberg-inline`, `iceberg-inline-types`, `iceberg-inline-vector`,
`vector-index-inline-delta`, `inline-flush-trigger`,
`overwrite-end-caps-inline`); J → `iceberg-column-stats`,
`iceberg-files-with-stats`, `iceberg-landing`, `iceberg-writer`; K/N →
`lineage_contract`, `lineage_pagination_contract` + NEW
`events_for_hydration_contract` (Task 9, green-first); M →
`ontology_contract`; L → memory `catalog`/`snapshot`/`tx`/`facade` targets;
O → NEW `snapshot_write_order_contract` (Task 11, green on postgres first).

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md`,
  § "road-cp-adapter-hygiene". Register: `docs/ROADMAP.md`
  `#road-cp-adapter-hygiene` (locate by id).
- **Behavior-preserving except the three whitelisted changes below.** Wire/API
  behavior, SQL semantics, error variants AND error Display text stay
  identical everywhere else. Every error message string that moves (exists
  checks, target validation) is carried byte-identical.
- **Compile-time SQL:** Tasks 7 and 9 change `query!` SQL text — each runs
  `tools/sqlx-prepare.sh`, commits the `.sqlx` delta in the same commit, and
  verifies `//src/control-plane/postgres:sqlx-cache-check`. No other task may
  touch a compile-time SQL string.
- **Existing tests pass unmodified** — no existing test function or assertion
  is edited. New tests are appended to existing test files (no BUCK churn) or
  added as new files with BUCK targets shown.
- **TDD:** new fns land red-first (for pure extractions red = compile failure
  on the missing symbol); behavior-change tasks show a real red (assertion
  failure) before the production edit; pure-refactor tasks run their pinning
  suites green before AND after.
- Tests are `rust_test`/`loom_fixture_test` BUCK targets, never inline
  `#[cfg(test)]`. Anything booting postgres uses `loom_fixture_test` +
  `PgFixture::shared()`.
- Clippy pedantic+restriction on prod code: no unwrap/expect/panic/indexing;
  `map_err` closures use named bindings (never `\|_\|`); **no new
  `#[expect]`**; new pub fns get `#[must_use]` where applicable and `///` docs
  (`missing_docs` on core).
- **Never pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to
  a file and grep it. Multi-target fixture runs use `-j 8`. Whole-tree builds
  only as `buck2 build -M none //src/...`.
- `buck2 run //tools:prek -- run --all-files` must report zero `Failed`
  before **every** commit. Conventional Commits; one commit per task.

## Deliberate behavior changes (the whitelist — everything else is identical)

1. **Fail-loud enum wire codecs (Task 2).** Unknown persisted tokens for
   `Cardinality`/`EventType`/`ActionKind` now return
   `ControlPlaneError::Validation("unknown <what> '<token>'")` instead of
   silently coercing to `One`/`Start`/`Insert`. Reachable ONLY via corrupt DB
   rows (writers emit `as_str()` tokens exclusively); pinned red-first by a
   hand-corrupted-row fixture test.
2. **Parse/dim reclassification (Task 5).** `Metric::from_str`,
   `IndexKind::from_str`, `IndexSpec::from_label` (unknown kind) and
   `pack_rows` (vector dim mismatch) return `Validation` instead of `Backend`
   — message text unchanged apart from the variant's own `"validation error: "`
   Display prefix. **This changes no status code on any currently reachable
   plane**: the search path class-erases via `to_serving` →
   `EngineServingError::Engine` (internal error) both before and after; the
   governance-plane status mapping sends `Backend` and `Validation` to
   internal alike; the worker's `JobFailure` handling is class-insensitive;
   #317's `Validation`→`InvalidArgument` mapping lives on the Flight SQL
   plane, which these errors never traverse. The change is honest
   classification (caller/data-shaped, not backend fault) + the Display
   prefix — groundwork so future planes can map the class correctly. No
   existing test pins the old class (all assert `.is_err()` only).
3. **Memory tx replay order (Task 11).** `replace_files` staged BEFORE
   `append_files` in one `Tx` now leaves the append's files live — matching
   postgres `IcebergTx` (the authority), which applies staged writes in
   order. Memory-only; pinned by the new `snapshot_write_order_contract`,
   proven green against postgres BEFORE the memory fix.

---

### Task 1: Exists-check helpers + `LinkRow` links dedup (spec items A + B)

Postgres-only, behavior-preserving. The two duplicated `select exists` texts
collapse onto `role_exists`/`object_type_exists` over `impl PgExecutor<'_>`
(the add_inheritance and define_action sites run inside transactions, so the
executor must be generic and those sites stay in-tx),
and `links`/`links_to` share one `LinkRow` + `link_defs` mapping.

**Files:**
- Modify: `src/control-plane/postgres/src/ontology.rs` (helper + `links`/
  `links_to`/`define_link`/`define_action` sites, `LinkRow`, `link_defs`)
- Modify: `src/control-plane/postgres/src/acl.rs` (helper + `grant`/
  `set_policy`/`add_inheritance` sites)

**Interfaces:**
- Produces: `pub(crate) async fn object_type_exists(ex: impl sqlx::PgExecutor<'_>, name: &str) -> Result<bool>`
  (in `ontology.rs`, imported by `acl.rs`); private `async fn role_exists(ex: impl sqlx::PgExecutor<'_>, id: &str) -> Result<bool>`
  (in `acl.rs`); private `struct LinkRow` + `fn link_defs(rows: Vec<LinkRow>) -> Page<LinkDef>`
  (in `ontology.rs`; Task 2 makes `link_defs` fallible).

- [x] **Step 1: Run the pinning suites green against the unmodified tree**

```bash
buck2 test //src/control-plane/postgres:ontology //src/control-plane/postgres:acl \
  //src/control-plane/postgres:existence-validation \
  //src/control-plane/postgres:action_kind_persist -j 8 \
  > /tmp/t1pre.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1pre.log
```

Expected: `Fail 0`.

- [x] **Step 2: Add the helpers** — in `ontology.rs` (module level, near
  `backing_from_row`):

```rust
/// True if an ontology object type named `name` exists. THE single existence
/// probe shared by the ontology reads/writes (`define_link`, `links`,
/// `links_to`, `define_action`) and the ACL write-time target checks
/// (`grant`, `set_policy`) — previously six verbatim copies of the same
/// `select exists` query.
pub(crate) async fn object_type_exists(
    ex: impl sqlx::PgExecutor<'_>,
    name: &str,
) -> Result<bool> {
    Ok(sqlx::query_scalar!(
        "select exists (select 1 from ontology.object_type where name = $1)",
        name,
    )
    .fetch_one(ex)
    .await
    .map_err(backend)?
    .unwrap_or(false))
}
```

In `acl.rs` (module level):

```rust
/// True if an ACL role with `id` exists. Shared by `assign_role`/`grant`/
/// `set_policy` (pool executor) and `add_inheritance` (in-tx executor).
async fn role_exists(ex: impl sqlx::PgExecutor<'_>, id: &str) -> Result<bool> {
    Ok(sqlx::query_scalar!(
        "select exists (select 1 from acl.role where id = $1)",
        id,
    )
    .fetch_one(ex)
    .await
    .map_err(backend)?
    .unwrap_or(false))
}
```

**The SQL strings are byte-identical to every site they replace** (verify with
`grep -n "select exists (select 1 from ontology.object_type"` /
`"select exists (select 1 from acl.role where id"` before deleting) — the
`.sqlx` cache is untouched.

- [x] **Step 3: Convert the ten sites.** Each inline
  `sqlx::query_scalar!(...).fetch_one(...).await.map_err(backend)?.unwrap_or(false)`
  block becomes a helper call; the surrounding `if !exists { return Err(...) }`
  logic, its message strings, AND each site's executor (pool vs in-tx) stay
  byte-identical:

  - `acl.rs` `assign_role` (`:57-66`): the role check becomes
    `let r_exists = role_exists(&self.pool, &role.0).await?;` (the subject
    check just above it is a different SQL text — left alone).
  - `acl.rs` `grant`: `let r_exists = role_exists(&self.pool, &role.0).await?;`
    and the Type-target block: `let type_exists = object_type_exists(&self.pool, &name.0).await?;`
    (add `use crate::ontology::object_type_exists;` to `acl.rs`'s imports).
  - `acl.rs` `set_policy`: same two conversions.
  - `acl.rs` `add_inheritance`: inside the loop,
    `let exists = role_exists(&mut *tx, id).await?;` (in-tx executor).
  - `ontology.rs` `define_link` (per-endpoint loop), `links`, `links_to`:
    `object_type_exists(&self.pool, ...)`.
  - `ontology.rs` `define_action`: **runs inside its transaction today**
    (`:333-340` uses `&mut *tx`) — convert to
    `object_type_exists(&mut *tx, &action.target.0).await?` so the check
    stays transactional (the executor-generic helper exists precisely for
    this; do NOT move it to `&self.pool`).

- [x] **Step 4: Dedup `links`/`links_to`.** In `ontology.rs`, add beside
  `backing_from_row`:

```rust
/// One `ontology.link` row. `links` and `links_to` run the same projection,
/// differing only in which endpoint column they filter on — `query_as!` into
/// this named row lets them share one mapping (`link_defs`).
struct LinkRow {
    name: String,
    from_type: String,
    to_type: String,
    cardinality: String,
    backing_kind: String,
    from_column: String,
    to_column: String,
    from_key: Option<String>,
    to_key: Option<String>,
    join_table_schema: Option<String>,
    join_table_name: Option<String>,
}

/// Map fetched link rows into a full (unpaginated) `Page<LinkDef>` — the
/// shared tail of `links`/`links_to`.
fn link_defs(rows: Vec<LinkRow>) -> Page<LinkDef> {
    Page::from_full(
        rows.into_iter()
            .map(|r| LinkDef {
                name: r.name,
                from: TypeName(r.from_type),
                to: TypeName(r.to_type),
                cardinality: cardinality_from_str(r.cardinality.as_str()),
                backing: backing_from_row(
                    &r.backing_kind,
                    r.from_column,
                    r.to_column,
                    r.from_key,
                    r.to_key,
                    r.join_table_schema,
                    r.join_table_name,
                ),
            })
            .collect(),
    )
}
```

Then both methods shrink to (SQL strings unchanged — only the macro switches
to `query_as!`, which reuses the same cache entry):

```rust
async fn links(&self, name: &TypeName, _page: PageReq) -> Result<Page<LinkDef>> {
    if !object_type_exists(&self.pool, &name.0).await? {
        return Err(ControlPlaneError::NotFound(name.0.clone()));
    }
    let rows = sqlx::query_as!(
        LinkRow,
        "select name, from_type, to_type, cardinality, backing_kind, from_column, \
                to_column, from_key, to_key, join_table_schema, join_table_name \
         from ontology.link where from_type = $1",
        name.0,
    )
    .fetch_all(&self.pool)
    .await
    .map_err(backend)?;
    Ok(link_defs(rows))
}
```

`links_to` is identical with `where to_type = $1`.

- [x] **Step 5: Build + pinning suites green; no `.sqlx` delta**

```bash
buck2 build //src/control-plane/postgres:postgres > /tmp/b1.log 2>&1; tail -2 /tmp/b1.log
buck2 test //src/control-plane/postgres:ontology //src/control-plane/postgres:acl \
  //src/control-plane/postgres:existence-validation \
  //src/control-plane/postgres:action_kind_persist -j 8 \
  > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log
git status --porcelain src/control-plane/postgres/.sqlx   # expect empty
```

Expected: build OK, `Fail 0`, no `.sqlx` change. If `query_as!` surprises with
a cache miss (offline-build failure naming a missing query hash), STOP — the
SQL text drifted; restore it byte-identical.

- [x] **Step 6: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek1.log 2>&1; grep -E "Failed" /tmp/prek1.log || echo CLEAN
git add src/control-plane/postgres/src/ontology.rs src/control-plane/postgres/src/acl.rs
git commit -m "refactor(control-plane): exists-check helpers + LinkRow links dedup"
```

---

### Task 2: Enum wire codecs to core, fail-loud (spec item D — whitelisted change 1)

`Cardinality`/`EventType`/`ActionKind` gain `as_str()` + `FromStr` in core
(mirroring `Metric`/`IndexKind`); `Action`/`Effect` gain `as_str()` (they are
never parsed back). The postgres free fns (`lib.rs:127-174` + the two
`ActionKind` inline matches) are deleted. Unknown tokens now error.

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (`Cardinality`,
  `ActionKind` impls)
- Modify: `src/control-plane/core/src/lineage.rs` (`EventType` impl)
- Modify: `src/control-plane/core/src/acl.rs` (`Action`, `Effect` impls)
- Create: `src/control-plane/core/tests/enum_wire_codecs.rs`
- Modify: `src/control-plane/core/BUCK` (new `enum-wire-codecs` target)
- Modify: `src/control-plane/postgres/src/lib.rs` (delete
  `cardinality_to_str`/`cardinality_from_str`/`event_type_to_str`/
  `event_type_from_str`/`action_to_str`/`effect_to_str`)
- Modify: `src/control-plane/postgres/src/ontology.rs`,
  `src/control-plane/postgres/src/lineage.rs`,
  `src/control-plane/postgres/src/acl.rs` (call sites)
- Test (append): `src/control-plane/postgres/tests/ontology.rs` (corrupt-row
  red pin)

**Interfaces:**
- Produces: `Cardinality::as_str(self) -> &'static str`,
  `impl FromStr for Cardinality` (Err = `ControlPlaneError`,
  `Validation("unknown cardinality '<t>'")`); same shape for `EventType`
  (`"start"/"running"/"complete"/"abort"/"fail"`, `Validation("unknown event
  type '<t>'")`) and `ActionKind` (`"insert"/"update"/"delete"`,
  `Validation("unknown action kind '<t>'")`); `Action::as_str` /
  `Effect::as_str` (`"read"/"write"`, `"allow"/"deny"`).
- Consumes (Task 1): the single `link_defs` mapping site.

- [x] **Step 1: Write the failing unit tests** — create
  `src/control-plane/core/tests/enum_wire_codecs.rs`:

```rust
//! The enum↔string wire codecs (persisted tokens). Round-trip + fail-loud:
//! an unknown token is a Validation error, never a silent default — the
//! postgres adapter's old free fns coerced corrupt rows to One/Start/Insert.

use std::str::FromStr;

use control_plane_core::{
    Action, ActionKind, Cardinality, ControlPlaneError, Effect, EventType,
};

#[test]
fn cardinality_round_trips_and_fails_loud() {
    assert_eq!(Cardinality::One.as_str(), "one");
    assert_eq!(Cardinality::Many.as_str(), "many");
    assert_eq!(Cardinality::from_str("one").unwrap(), Cardinality::One);
    assert_eq!(Cardinality::from_str("many").unwrap(), Cardinality::Many);
    assert!(matches!(
        Cardinality::from_str("both"),
        Err(ControlPlaneError::Validation(_))
    ));
}

#[test]
fn event_type_round_trips_and_fails_loud() {
    let all = [
        (EventType::Start, "start"),
        (EventType::Running, "running"),
        (EventType::Complete, "complete"),
        (EventType::Abort, "abort"),
        (EventType::Fail, "fail"),
    ];
    for (v, s) in all {
        assert_eq!(v.as_str(), s);
        assert_eq!(EventType::from_str(s).unwrap(), v);
    }
    assert!(matches!(
        EventType::from_str("finished"),
        Err(ControlPlaneError::Validation(_))
    ));
}

#[test]
fn action_kind_round_trips_and_fails_loud() {
    let all = [
        (ActionKind::Insert, "insert"),
        (ActionKind::Update, "update"),
        (ActionKind::Delete, "delete"),
    ];
    for (v, s) in all {
        assert_eq!(v.as_str(), s);
        assert_eq!(ActionKind::from_str(s).unwrap(), v);
    }
    assert!(matches!(
        ActionKind::from_str("upsert"),
        Err(ControlPlaneError::Validation(_))
    ));
}

#[test]
fn action_and_effect_tokens() {
    assert_eq!(Action::Read.as_str(), "read");
    assert_eq!(Action::Write.as_str(), "write");
    assert_eq!(Effect::Allow.as_str(), "allow");
    assert_eq!(Effect::Deny.as_str(), "deny");
}
```

BUCK target (append to `src/control-plane/core/BUCK`, mirroring `page`):

```python
rust_test(
    name = "enum-wire-codecs",
    crate = "enum_wire_codecs",
    srcs = ["tests/enum_wire_codecs.rs"],
    crate_root = "tests/enum_wire_codecs.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [":core"],
)
```

- [x] **Step 2: Run — expect RED** (compile failure: no `as_str`/`FromStr` on
  these types)

```bash
buck2 test //src/control-plane/core:enum-wire-codecs > /tmp/t2red.log 2>&1; \
  grep -E "error\[|no method|Tests finished|FAIL" /tmp/t2red.log | head -5
```

- [x] **Step 3: Implement the core impls.** In
  `src/control-plane/core/src/ontology.rs`, beside `Cardinality`:

```rust
impl Cardinality {
    /// The persisted wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Cardinality::One => "one",
            Cardinality::Many => "many",
        }
    }
}

impl std::str::FromStr for Cardinality {
    type Err = ControlPlaneError;

    /// Parse the persisted token. Unknown tokens are a loud error (a corrupt
    /// row), never a silent default.
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "one" => Ok(Cardinality::One),
            "many" => Ok(Cardinality::Many),
            other => Err(ControlPlaneError::Validation(format!(
                "unknown cardinality '{other}'"
            ))),
        }
    }
}
```

`ActionKind` gets the identical shape (`"insert"/"update"/"delete"`, error
`"unknown action kind '{other}'"`). In `core/src/lineage.rs`, `EventType` gets
the five-token version (error `"unknown event type '{other}'"`). In
`core/src/acl.rs`:

```rust
impl Action {
    /// The persisted wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Action::Read => "read",
            Action::Write => "write",
        }
    }
}

impl Effect {
    /// The persisted wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Effect::Allow => "allow",
            Effect::Deny => "deny",
        }
    }
}
```

(If any of these enums lacks `Copy`, take `&self` instead — check the derive
line first; all five are `Copy` today.) Import `ControlPlaneError` where
missing: `core/src/ontology.rs` and `core/src/acl.rs` reference it ZERO times
today (add `use crate::error::ControlPlaneError;` to each); `lineage.rs`
already imports it.

- [x] **Step 4: Run — unit tests PASS**

```bash
buck2 test //src/control-plane/core:enum-wire-codecs > /tmp/t2a.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t2a.log
```

- [x] **Step 5: Append the corrupt-row red pin** to
  `src/control-plane/postgres/tests/ontology.rs` (seeding via the #304
  `ObjectType::build` DSL — `define_min_type` is testkit-internal, not pub):

```rust
/// RED pre-migration: a corrupt persisted cardinality token must surface as a
/// loud Validation error from `links`, not silently coerce to One (the old
/// `cardinality_from_str` fallback). Whitelisted change 1 of
/// road-cp-adapter-hygiene.
#[tokio::test]
async fn corrupt_cardinality_token_fails_loud() {
    use control_plane_core::{
        Cardinality, ControlPlane, ControlPlaneError, LinkBacking, LinkDef, ObjectType, PageReq,
        TypeName,
    };
    let fixture = control_plane_postgres::fixture::PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    for (ty, table) in [("A", "a"), ("B", "b")] {
        cp.ontology()
            .define_type(
                ObjectType::build(ty, ("wh", table))
                    .prop_req("id", "Long")
                    .identity("id")
                    .done(),
            )
            .await
            .expect("define_type");
    }
    cp.ontology()
        .define_link(LinkDef {
            name: "a_to_b".into(),
            from: TypeName("A".into()),
            to: TypeName("B".into()),
            cardinality: Cardinality::One,
            backing: LinkBacking::ForeignKey {
                from_column: "id".into(),
                to_column: "id".into(),
            },
        })
        .await
        .expect("define_link");
    // Corrupt the persisted token behind the adapter's back.
    sqlx::query("update ontology.link set cardinality = 'weird' where name = 'a_to_b'")
        .execute(cp.pool())
        .await
        .expect("corrupt row");
    let err = cp
        .ontology()
        .links(&TypeName("A".into()), PageReq::unbounded())
        .await
        .expect_err("corrupt cardinality must not silently coerce");
    assert!(matches!(err, ControlPlaneError::Validation(_)), "got {err:?}");
}
```

(`cp.pool()` is `PgControlPlane::pool`, pub.) The `ontology` BUCK target's
deps today are `[":postgres", "//src/control-plane/testkit:testkit",
"//third-party:tokio"]` — extend them:

```python
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//src/control-plane/testkit:testkit",
        "//third-party:sqlx",
        "//third-party:tokio",
    ],
```

Run it — expect RED (the call currently succeeds with `Cardinality::One`):

```bash
buck2 test //src/control-plane/postgres:ontology -j 8 > /tmp/t2red2.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t2red2.log
```

- [x] **Step 6: Migrate the postgres adapter.** Delete the six free fns from
  `lib.rs:127-174` (`target_cols` stays until Task 3). Call-site conversions:

  - `ontology.rs` `define_link` insert: `cardinality_to_str(link.cardinality)`
    → `link.cardinality.as_str()`.
  - `ontology.rs` `link_defs` becomes fallible:

```rust
/// Map fetched link rows into a full (unpaginated) `Page<LinkDef>` — the
/// shared tail of `links`/`links_to`. Errors on a corrupt cardinality token.
fn link_defs(rows: Vec<LinkRow>) -> Result<Page<LinkDef>> {
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(LinkDef {
            name: r.name,
            from: TypeName(r.from_type),
            to: TypeName(r.to_type),
            cardinality: r.cardinality.parse()?,
            backing: backing_from_row(
                &r.backing_kind,
                r.from_column,
                r.to_column,
                r.from_key,
                r.to_key,
                r.join_table_schema,
                r.join_table_name,
            ),
        });
    }
    Ok(Page::from_full(out))
}
```

    and both callers' tails become `link_defs(rows)`.
  - `ontology.rs` `define_action`: the inline `let kind = match action.kind
    {...}` → `action.kind.as_str()`; `get_action`'s inline
    `match row.kind.as_str() {...}` (silent `Insert` fallback) →
    `kind: row.kind.parse()?` in the `ActionDef` literal.
  - `lineage.rs`: `event_type_to_str(event.event_type)` in `pg_emit` →
    `event.event_type.as_str()`; `event_type_from_str(&r.event_type)` in
    `events_for` → `r.event_type.parse()?` (it is inside a plain `for` loop —
    `?` propagates).
  - `acl.rs`: every `action_to_str(action)` → `action.as_str()`; every
    `effect_to_str(effect)` → `effect.as_str()`. Remove the deleted names from
    the `use crate::{...}` lists in all three files.

- [x] **Step 7: Run — red pin now GREEN, contracts green on both adapters**

```bash
buck2 test //src/control-plane/core:enum-wire-codecs \
  //src/control-plane/postgres:ontology //src/control-plane/postgres:acl \
  //src/control-plane/postgres:lineage //src/control-plane/postgres:action_kind_persist \
  //src/control-plane/memory:ontology //src/control-plane/memory:acl \
  //src/control-plane/memory:lineage -j 8 \
  > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log
git status --porcelain src/control-plane/postgres/.sqlx   # expect empty
```

- [x] **Step 8: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek2.log 2>&1; grep -E "Failed" /tmp/prek2.log || echo CLEAN
git add src/control-plane/core src/control-plane/postgres
git commit -m "refactor(control-plane): enum wire codecs to core with fail-loud parsing"
```

---

### Task 3: `PolicyTarget::key_parts` + shared policy write checks (spec items C + E)

The `(kind, a, b)` target encoding moves to core as a method; the duplicated
grant/set_policy write-time validation becomes two pure core fns (adapters
supply the lookups — the `validate_constraints` pattern).

**Files:**
- Modify: `src/control-plane/core/src/acl.rs` (`key_parts`,
  `check_grant_target`, `check_policy_write`)
- Modify: `src/control-plane/core/src/lib.rs` (export the two fns)
- Create: `src/control-plane/core/tests/policy_write_checks.rs`
- Modify: `src/control-plane/core/BUCK` (new `policy-write-checks` target)
- Modify: `src/control-plane/postgres/src/lib.rs` (delete `target_cols`)
- Modify: `src/control-plane/postgres/src/acl.rs` (all `target_cols` sites +
  grant/set_policy validation)
- Modify: `src/control-plane/memory/src/acl.rs` (`target_key` delegates;
  grant/set_policy validation)

**Interfaces:**
- Produces: `PolicyTarget::key_parts(&self) -> (&'static str, String, String)`;
  `pub fn check_grant_target(target: &PolicyTarget, type_exists: bool) -> Result<()>`;
  `pub fn check_policy_write(policy: &Policy, type_props: Option<&HashSet<String>>) -> Result<()>`
  (for a `Type` target, `type_props = None` means "type unknown"; for a
  `Table` target the argument is ignored).

- [x] **Step 1: Write the failing unit tests** — create
  `src/control-plane/core/tests/policy_write_checks.rs`:

```rust
//! Pure write-time checks for ACL targets, shared by the memory and postgres
//! adapters (which supply the existence/property lookups).

use std::collections::HashSet;

use control_plane_core::{
    CompareOp, ControlPlaneError, Policy, PolicyTarget, RowFilter, ScalarValue, TableRef,
    TypeName, check_grant_target, check_policy_write,
};

fn ttype(n: &str) -> PolicyTarget {
    PolicyTarget::Type(TypeName(n.into()))
}

fn ttable() -> PolicyTarget {
    PolicyTarget::Table(TableRef {
        schema: "main".into(),
        name: "raw".into(),
    })
}

fn filter(prop: &str) -> RowFilter {
    RowFilter::Compare {
        property: prop.into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("x".into()),
    }
}

fn policy(target: PolicyTarget, row_filter: Option<RowFilter>) -> Policy {
    Policy {
        target,
        row_filter,
        deny_columns: vec![],
        mask_columns: vec![],
    }
}

#[test]
fn key_parts_encodes_both_target_kinds() {
    assert_eq!(
        ttype("T").key_parts(),
        ("type", "T".to_string(), String::new())
    );
    assert_eq!(
        ttable().key_parts(),
        ("table", "main".to_string(), "raw".to_string())
    );
}

#[test]
fn grant_target_checks() {
    assert!(check_grant_target(&ttype("T"), true).is_ok());
    let err = check_grant_target(&ttype("Nope"), false).unwrap_err();
    let ControlPlaneError::Validation(msg) = err else {
        panic!("expected Validation");
    };
    assert_eq!(msg, "grant references unknown type `Nope`");
    // Table targets stay unvalidated (deferred) — exists flag ignored.
    assert!(check_grant_target(&ttable(), false).is_ok());
}

#[test]
fn policy_write_checks() {
    let props: HashSet<String> = ["col".to_string()].into_iter().collect();
    // known type, no filter / valid filter
    assert!(check_policy_write(&policy(ttype("T"), None), Some(&props)).is_ok());
    assert!(check_policy_write(&policy(ttype("T"), Some(filter("col"))), Some(&props)).is_ok());
    // unknown type (always checked, filter or not)
    let err = check_policy_write(&policy(ttype("Nope"), None), None).unwrap_err();
    let ControlPlaneError::Validation(msg) = err else {
        panic!("expected Validation");
    };
    assert_eq!(msg, "policy references unknown type Nope");
    // known type, filter on unknown property
    assert!(matches!(
        check_policy_write(&policy(ttype("T"), Some(filter("ghost"))), Some(&props)),
        Err(ControlPlaneError::Validation(_))
    ));
    // Table target: structural filter validation only, props ignored
    assert!(check_policy_write(&policy(ttable(), Some(filter("anything"))), None).is_ok());
}
```

BUCK target (append to `src/control-plane/core/BUCK`):

```python
rust_test(
    name = "policy-write-checks",
    crate = "policy_write_checks",
    srcs = ["tests/policy_write_checks.rs"],
    crate_root = "tests/policy_write_checks.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [":core"],
)
```

- [x] **Step 2: Run — expect RED** (missing symbols)

```bash
buck2 test //src/control-plane/core:policy-write-checks > /tmp/t3red.log 2>&1; \
  grep -E "error\[|cannot find|Tests finished" /tmp/t3red.log | head -5
```

- [x] **Step 3: Implement in `core/src/acl.rs`** (beside `PolicyTarget` /
  `validate_row_filter`; error strings byte-identical to the adapters'
  current ones):

```rust
impl PolicyTarget {
    /// The canonical `(kind, a, b)` encoding of a target — postgres persists
    /// it as the `(target_kind, target_a, target_b)` columns, the memory
    /// adapter as its grant/policy map key. `Type(n)` → `("type", n, "")`;
    /// `Table(s.t)` → `("table", s, t)`.
    #[must_use]
    pub fn key_parts(&self) -> (&'static str, String, String) {
        match self {
            PolicyTarget::Type(n) => ("type", n.0.clone(), String::new()),
            PolicyTarget::Table(r) => ("table", r.schema.clone(), r.name.clone()),
        }
    }
}

/// Write-time existence check for `grant`: a `Type` target must reference an
/// existing ontology type (`type_exists` is the adapter's lookup result);
/// `Table` targets stay unvalidated (deferred) and ignore the flag.
pub fn check_grant_target(target: &PolicyTarget, type_exists: bool) -> Result<()> {
    if let PolicyTarget::Type(name) = target
        && !type_exists
    {
        return Err(ControlPlaneError::Validation(format!(
            "grant references unknown type `{}`",
            name.0
        )));
    }
    Ok(())
}

/// Write-time validation for `set_policy`. For a `Type` target, `type_props`
/// is the type's property set when it exists (`None` = unknown type — always
/// rejected, row_filter or not); a present row_filter validates against that
/// set. For a `Table` target `type_props` is ignored and a present row_filter
/// validates structurally only.
pub fn check_policy_write(
    policy: &Policy,
    type_props: Option<&HashSet<String>>,
) -> Result<()> {
    match &policy.target {
        PolicyTarget::Type(name) => {
            let Some(props) = type_props else {
                return Err(ControlPlaneError::Validation(format!(
                    "policy references unknown type {}",
                    name.0
                )));
            };
            if let Some(f) = &policy.row_filter {
                validate_row_filter(f, Some(props)).map_err(ControlPlaneError::Validation)?;
            }
        }
        PolicyTarget::Table(_) => {
            if let Some(f) = &policy.row_filter {
                validate_row_filter(f, None).map_err(ControlPlaneError::Validation)?;
            }
        }
    }
    Ok(())
}
```

Add `check_grant_target`, `check_policy_write` to the `pub use acl::{...}`
list in `core/src/lib.rs`. Run Step 1's target — expect PASS.

- [x] **Step 4: Migrate postgres `acl.rs`.** Delete `target_cols` from
  `lib.rs` (and its import in `acl.rs`); every `let (kind, a, b) =
  target_cols(x);` becomes `let (kind, a, b) = x.key_parts();` (6 sites:
  `acl.rs:233,254,322,360,382,420`).
  `grant`'s validation block becomes:

```rust
        if !role_exists(&self.pool, &role.0).await? {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        // Best-effort, non-transactional existence lookup (unchanged); the
        // decision itself is the shared core check.
        let type_exists = match &target {
            PolicyTarget::Type(name) => object_type_exists(&self.pool, &name.0).await?,
            PolicyTarget::Table(_) => true,
        };
        check_grant_target(&target, type_exists)?;
```

`set_policy`'s validation block becomes (property fetch still only when a
row_filter is present — round-trip count unchanged):

```rust
        if !role_exists(&self.pool, &role.0).await? {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        // Best-effort, non-transactional lookups (unchanged semantics); the
        // decision is `check_policy_write`. The property set is fetched only
        // when a row_filter needs it — an existing type with no filter passes
        // an empty set, which the check never reads.
        let type_props: Option<std::collections::HashSet<String>> = match &policy.target {
            PolicyTarget::Type(name) => {
                if !object_type_exists(&self.pool, &name.0).await? {
                    None
                } else if policy.row_filter.is_some() {
                    let names = sqlx::query_scalar!(
                        "select name from ontology.property where type_name = $1",
                        &name.0,
                    )
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                    Some(names.into_iter().collect())
                } else {
                    Some(std::collections::HashSet::new())
                }
            }
            PolicyTarget::Table(_) => None,
        };
        check_policy_write(&policy, type_props.as_ref())?;
```

Add `check_grant_target`, `check_policy_write` to the
`use control_plane_core::{...}` list; drop the now-unused
`validate_row_filter` import if nothing else uses it.

- [x] **Step 5: Migrate memory `acl.rs`.** `TargetKey` becomes
  `type TargetKey = (&'static str, String, String);` and
  `fn target_key(t: &PolicyTarget) -> TargetKey { t.key_parts() }`. `grant`'s
  Type-check block becomes:

```rust
        let type_exists = match &target {
            PolicyTarget::Type(name) => self.type_exists(&name.0),
            PolicyTarget::Table(_) => true,
        };
        check_grant_target(&target, type_exists)?;
```

`set_policy`'s validation `match` becomes (lock-ordering comment preserved —
the ontology lookup still happens with no acl lock held):

```rust
        let type_props: Option<HashSet<String>> = match &policy.target {
            PolicyTarget::Type(name) => self
                .type_properties(&name.0)
                .map(|v| v.into_iter().collect()),
            PolicyTarget::Table(_) => None,
        };
        check_policy_write(&policy, type_props.as_ref())?;
```

Update imports (`check_grant_target`, `check_policy_write`; drop
`validate_row_filter` if now unused).

- [x] **Step 6: Run the pinning contracts on BOTH adapters**

```bash
buck2 test //src/control-plane/core:policy-write-checks \
  //src/control-plane/postgres:acl //src/control-plane/postgres:existence-validation \
  //src/control-plane/memory:acl //src/control-plane/memory:existence-validation -j 8 \
  > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log
```

Expected: `Fail 0` (the contracts assert the exact `Validation` rejections and
the Table-target deferred boundary).

- [x] **Step 7: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek3.log 2>&1; grep -E "Failed" /tmp/prek3.log || echo CLEAN
git add src/control-plane/core src/control-plane/postgres src/control-plane/memory
git commit -m "refactor(control-plane): PolicyTarget::key_parts + shared policy write checks"
```

---

### Task 4: One source-carrying `backend()` boxing helper (spec item F)

Generalize `lib.rs::backend` from `fn(sqlx::Error)` to any
`E: std::error::Error + Send + Sync + 'static`; delete the FIVE module-local
variants and convert the flattening/inline sites. **Display text is
unchanged** (`Backend` is `#[error(transparent)]`, and boxing the source
displays the same string the old `e.to_string()` did) — only the `source()`
chain improves.

**The complete variant census (verified by grep; nothing else defines a
Backend-boxing fn in the crate):**

| Variant | Kind | Call sites | Fix |
| --- | --- | --- | --- |
| `lib.rs:123` `backend(sqlx::Error)` | source-carrying, sqlx-only | crate-wide `map_err(backend)` | generalize (Step 1) |
| `iceberg_read.rs:19` `be<E: Error + Send + Sync + 'static>` | source-carrying | 8 `map_err(be)` | delete; rename sites |
| `iceberg_landing.rs:38` `be<E: Error + Send + Sync + 'static>` | source-carrying (identical bound) | 24 `map_err(be)` | delete; rename sites |
| `iceberg_mirror.rs:16` `iceberg_err(iceberg::Error)` | source-carrying, iceberg-only | 4 (`:383,392,401,404`) | delete; rename sites |
| `puffin.rs:16` `be<E: Display>` | **FLATTENS** | 7 `map_err(be)` (all `iceberg::Error`) | delete; rename sites |
| `vector_index.rs:16` `backend<E: Display>` | **FLATTENS + SHADOWS `lib.rs::backend`** | 10 `map_err(backend)` (`:77,101,148,160,177,279,291,355,562,591` — all sqlx) | delete the shadow; add `use crate::backend;` — the 10 sites then resolve to the shared helper unchanged |

**Files:**
- Modify: `src/control-plane/postgres/src/lib.rs` (generic `backend`)
- Modify: `src/control-plane/postgres/src/iceberg_read.rs` (delete local `be`)
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (delete local
  `be`)
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs` (delete
  `iceberg_err`; fix the `:326` doc comment that names it)
- Modify: `src/control-plane/postgres/src/puffin.rs` (delete local `be`)
- Modify: `src/control-plane/postgres/src/vector_index.rs` (delete the
  shadowing `backend<Display>`; convert its 5 inline flattens)
- Modify: `src/control-plane/postgres/src/iceberg_flush.rs`,
  `src/control-plane/postgres/src/iceberg_inline.rs`,
  `src/control-plane/postgres/src/iceberg_stats.rs`,
  `src/control-plane/postgres/src/auth.rs`,
  `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs`
  (inline boxing/flattening closures → `map_err(backend)`)

- [x] **Step 1: Generalize the helper** in `lib.rs`:

```rust
/// Box a concrete error as `ControlPlaneError::Backend`, carrying the source.
/// THE single boxing helper — call sites use `map_err(backend)` (or a
/// domain-mapping helper like `auth::conflict_or_backend`); never flatten via
/// `.to_string()`, which severs the source chain. `Backend` is
/// `#[error(transparent)]`, so the Display text is the source's own.
fn backend<E: std::error::Error + Send + Sync + 'static>(e: E) -> ControlPlaneError {
    ControlPlaneError::Backend(Box::new(e))
}
```

Every existing `map_err(backend)` site keeps compiling (inference picks
`E = sqlx::Error`).

- [x] **Step 2: Delete the five module-local variants** (census above):

  - `iceberg_read.rs`: delete `be`, add `use crate::backend;`, rename its 8
    `map_err(be)` → `map_err(backend)` (types: `iceberg::Error`, arrow/parquet
    stream errors — all `Error + Send + Sync + 'static`).
  - `iceberg_landing.rs`: delete `be` (its bound is IDENTICAL to the new
    shared helper, so the rename is safe by construction), add
    `use crate::backend;`, rename its 24 `map_err(be)` sites (types:
    `ArrowError` from `StreamReader`/`concat_batches`/`RecordBatch::try_new`,
    `sqlx::Error` from acquire/begin/commit, `iceberg::Error` from
    `load_table`/`schema_to_arrow_schema`/namespace + table ops).
  - `iceberg_mirror.rs`: delete `iceberg_err`, rename its 4 sites
    (`:383,392,401,404`, all `iceberg::Error`) to `map_err(backend)` —
    `use crate::backend;` is already imported (`:14`). Update the `:326` doc
    comment ("as `Backend` (matching `iceberg_err`)") to name `backend`
    instead.
  - `puffin.rs`: delete `be<Display>`, add `use crate::backend;`, rename its
    7 sites (all `iceberg::Error` — now source-carrying instead of
    flattened).
  - `vector_index.rs`: delete the `backend<E: Display>` shadow (`:16-18`) and
    add `use crate::backend;` — its 10 existing `map_err(backend)` call
    sites (`:77,101,148,160,177,279,291,355,562,591`, all `sqlx::Error`)
    resolve to the shared source-carrying helper with no text change. Convert
    its 5 inline flattens to `map_err(backend)`: `:201`/`:205`
    (`ArrowError` from `schema().index_of`), `:373` (`ArrowError` from
    `RecordBatch::try_new`), `:472` (`iceberg::Error` from
    `TableIdent::from_strs`), `:476` (`iceberg::Error` from `load_table`).

- [x] **Step 3: Convert the remaining inline sites.** `iceberg_flush.rs:113`
  (`serde_json::Error`) and any other
  `.map_err(|e| ControlPlaneError::Backend(e.to_string().into()))` /
  `.map_err(|e| ControlPlaneError::Backend(Box::new(e)))` /
  `.map_err(|e| ControlPlaneError::Backend(e.into()))` on an
  **error-typed** `e` → `.map_err(backend)`. Find them all with:

```bash
grep -rn "Backend(Box::new(e))\|Backend(e.to_string().into())\|Backend(e.into())" \
  src/control-plane/postgres/src/
grep -rn "fn be\b\|fn be<\|fn iceberg_err\|fn backend" src/control-plane/postgres/src/
```

  The second grep must end with exactly ONE definition (`lib.rs`). Leave every
  `Backend(format!(...).into())` / `Backend("msg".into())` **message-literal**
  construction untouched — those are named-condition errors, not source
  boxing. If a grep hit's `e` is not an `Error + Send + Sync + 'static` type,
  leave that site as-is with a one-line
  `// not an Error type; flattening is deliberate here` comment.
  (`commit_mirror.rs:93` needs `use crate::backend;` — it lives in a
  submodule directory.)

- [x] **Step 4: Full postgres-crate build + suite** (error paths are spread
  across the crate; the fixture suites pin observable messages)

```bash
buck2 build //src/control-plane/postgres:postgres > /tmp/b4.log 2>&1; tail -2 /tmp/b4.log
buck2 test //src/control-plane/postgres: -j 8 > /tmp/t4.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t4.log
```

Expected: `Fail 0`.

- [x] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek4.log 2>&1; grep -E "Failed" /tmp/prek4.log || echo CLEAN
git add src/control-plane/postgres
git commit -m "refactor(control-plane): one source-carrying backend() boxing helper"
```

---

### Task 5: Classify parse/dim failures as Validation (spec item G — whitelisted change 2)

`Metric::from_str`, `IndexKind::from_str`, `IndexSpec::from_label` and
`pack_rows`' dim mismatch flip `Backend` → `Validation`, message text
unchanged. **Honest classification only — no status code changes on any
currently reachable plane** (see whitelist entry 2 for the per-plane
mechanism); the observable delta is the class itself plus the variant's
`"validation error: "` Display prefix. The codec's **bytes are untouched**
(one error-variant line in `codec.rs`; the byte-golden tests must stay green
unmodified).

**Files:**
- Test (append): `src/control-plane/core/tests/vector_index.rs`
- Modify: `src/control-plane/core/src/vector_index/mod.rs`
- Modify: `src/control-plane/core/src/vector_index/codec.rs` (`pack_rows`
  error variant + the doc line above it)

- [x] **Step 1: Append the class-asserting tests (RED)** to
  `core/tests/vector_index.rs`:

```rust
#[test]
fn parse_and_dim_failures_are_validation() {
    use control_plane_core::{ControlPlaneError, IndexKind, IndexSpec, Metric};
    use std::str::FromStr;
    // Parse failures: caller/wire/row-shaped tokens are Validation (honest
    // classification; no plane maps the class to a status code today).
    // Whitelisted change 2 of road-cp-adapter-hygiene.
    assert!(matches!(
        Metric::from_str("hamming"),
        Err(ControlPlaneError::Validation(_))
    ));
    assert!(matches!(
        IndexKind::from_str("nope"),
        Err(ControlPlaneError::Validation(_))
    ));
    assert!(matches!(
        IndexSpec::from_label(Some("nope"), None, None, None),
        Err(ControlPlaneError::Validation(_))
    ));
    // Dim mismatch: caller-shaped data (a landed vector of the wrong length).
    let rows = vec![(control_plane_core::VectorKey::Int(1), vec![1.0, 0.0, 0.0])];
    assert!(matches!(
        control_plane_core::FlatIndex::build(4, Metric::Cosine, rows),
        Err(ControlPlaneError::Validation(_))
    ));
}
```

```bash
buck2 test //src/control-plane/core:vector-index > /tmp/t5red.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t5red.log
```

Expected: exactly ONE failing test (the new one — the errors are `Backend`
today); every pre-existing test green. If the new test passes, STOP — the
claim inventory is wrong.

- [x] **Step 2: Flip the four sites.** In `vector_index/mod.rs`, the three
  `ControlPlaneError::Backend(format!(...).into())` returns in
  `Metric::from_str`, `IndexKind::from_str`, `IndexSpec::from_label` become
  `ControlPlaneError::Validation(format!(...))` (message strings unchanged;
  drop the `.into()` — `Validation` holds a `String`). In `codec.rs`'s
  `pack_rows`, the dim-mismatch return becomes:

```rust
            return Err(ControlPlaneError::Validation(format!(
                "vector dim mismatch: expected {d}, got {}",
                v.len()
            )));
```

  and the doc comment above (`codec.rs:191`) that names the error keeps its
  text (only the class changed — extend it with "(a `Validation` error:
  caller-shaped data)" if it states the variant). Touch NOTHING else in
  `codec.rs`.

- [x] **Step 3: Run — new test green, codec goldens + downstream vector
  suites green unmodified**

```bash
buck2 test //src/control-plane/core:vector-index \
  //src/control-plane/core:vector-index-codec //src/control-plane/core:index-spec-build \
  > /tmp/t5a.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5a.log
buck2 test //src/control-plane/postgres:vector-index-build \
  //src/control-plane/postgres:vector-index-ivf //src/control-plane/postgres:vector-index-hnsw \
  //src/control-plane/postgres:vector-index-mirror //src/control-plane/postgres:vector-index-inline-delta \
  //src/services/engine-serving: //src/services/query-api:vector_search_e2e -j 8 \
  > /tmp/t5b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5b.log
```

(Target names verified against both BUCK files.) Expected: `Fail 0` — nothing
pins the old class.

- [x] **Step 4: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek5.log 2>&1; grep -E "Failed" /tmp/prek5.log || echo CLEAN
git add src/control-plane/core
git commit -m "fix(control-plane): classify vector parse/dim failures as Validation"
```

---

### Task 6: Inline-table access preamble + loop-invariant insert SQL (spec items H + I)

Runtime-SQL only (`AssertSqlSafe`) — **no compile-time SQL changes**. Three
helpers concentrate the safety argument that is currently re-justified per
site; `inline_append` stops rebuilding its insert statement per row.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (helpers + the
  `has_live_inline_rows`/`inline_live_batch`/`inline_ddl`/`inline_append`
  sites + the existing parameterized `to_regclass` site near `:52`)
- Modify: `src/control-plane/postgres/src/vector_index.rs`
  (`inline_delta_batch` preamble + quoting)

**Interfaces:**
- Produces (all `pub(crate)`, in `iceberg_inline.rs`):
  `fn quote_ident(name: &str) -> String`;
  `fn mvcc_live_pred(at: i64) -> String`;
  `async fn inline_table_exists(conn: &mut PgConnection, table_id: i64) -> Result<bool>`.

- [x] **Step 1: Pinning suites green pre-change**

```bash
buck2 test //src/control-plane/postgres:iceberg-inline \
  //src/control-plane/postgres:iceberg-inline-types //src/control-plane/postgres:iceberg-inline-vector \
  //src/control-plane/postgres:vector-index-inline-delta \
  //src/control-plane/postgres:inline-flush-trigger \
  //src/control-plane/postgres:overwrite-end-caps-inline -j 8 \
  > /tmp/t6pre.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6pre.log
```

(Target spellings verified against `src/control-plane/postgres/BUCK`.)
Expected: `Fail 0`.

- [x] **Step 2: Add the helpers** to `iceberg_inline.rs`:

```rust
/// Quote `name` as a PG identifier: wrap in double quotes, escaping embedded
/// quotes. THE identifier-splice guard for the runtime inline-table SQL in
/// this module — quoting makes the spliced name inert, which is the safety
/// argument every `AssertSqlSafe` here leans on.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The MVCC "live at snapshot `at`" predicate over an inline table's
/// `begin_snapshot`/`end_snapshot` columns. `at` is a trusted i64 snapshot id
/// (never caller text), so splicing it is safe.
pub(crate) fn mvcc_live_pred(at: i64) -> String {
    format!("begin_snapshot <= {at} and (end_snapshot is null or end_snapshot > {at})")
}

/// True if the physical `inline_<table_id>` relation exists (`to_regclass`
/// returns NULL for a missing relation). The one inline-existence preamble —
/// parameterized, so no splice at all.
pub(crate) async fn inline_table_exists(
    conn: &mut PgConnection,
    table_id: i64,
) -> Result<bool> {
    let exists: Option<String> = sqlx::query_scalar("select to_regclass($1)::text")
        .bind(inline_table_name(table_id))
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
    Ok(exists.is_some())
}
```

- [x] **Step 3: Convert the sites** (each replacement keeps the surrounding
  early-return shape byte-identical):

  - `has_live_inline_rows`: the two-statement preamble becomes
    `if !inline_table_exists(conn, tid).await? { return Ok(false); }`, and the
    `any` query becomes
    `format!("select exists(select 1 from {} where {})", inline_table_name(tid), mvcc_live_pred(at.0))`.
  - `inline_live_batch`: preamble →
    `if !inline_table_exists(&mut conn, tid).await? { return Ok(None); }`;
    `col_list` mapping → `.map(|c| quote_ident(&c.name))`; the WHERE clause →
    `format!("select loom_row_id, {col_list} from {} where {} order by loom_row_id", inline_table_name(tid), mvcc_live_pred(at.0))`.
  - The existing parameterized `to_regclass($1)` site near
    `iceberg_inline.rs:52` (the drop/end-cap path — locate with
    `grep -n "to_regclass" src/control-plane/postgres/src/iceberg_inline.rs`):
    replace its inline two-statement check with `inline_table_exists`,
    preserving its skip behavior. (`iceberg_gc.rs:234` probes relations by a
    *stored name string*, not a live `table_id` — leave it.)
  - `inline_ddl`: the `write!(cols, ", \"{}\" {}", c.name.replace(...), pg)`
    escape → `write!(cols, ", {} {}", quote_ident(&c.name), pg)`.
  - `inline_append`: `col_list` → `.map(|c| quote_ident(&c.name))`, and hoist
    the loop-invariant statement (spec item I):

```rust
    // 3. Insert each row with the new begin_snapshot. The statement text is
    //    loop-invariant — only the binds change per row.
    let placeholders = (0..columns.len())
        .map(|i| format!("${}", i + 2)) // $1 = begin_snapshot
        .collect::<Vec<_>>()
        .join(", ");
    let insert_sql = format!(
        "insert into {} (begin_snapshot, {col_list}) values ($1, {placeholders})",
        inline_table_name(tid),
    );
    for row in 0..batch.num_rows() {
        let mut q = sqlx::query(AssertSqlSafe(insert_sql.clone())).bind(at.0);
        // ... existing per-cell bind loop unchanged ...
    }
```

  - `vector_index.rs` `inline_delta_batch`: preamble (the formatted
    `to_regclass('{}')` block) →
    `if !inline_table_exists(&mut conn, tid).await? { return Ok(None); }`
    (import `inline_table_exists` alongside the existing `iceberg_inline`
    imports); `id_quoted`/`vec_quoted` → `quote_ident(&identity_col)` /
    `quote_ident(&vector_col)`; the WHERE clause keeps its window conjunct and
    reuses the helper for the alive-at half:

```rust
    let rows = sqlx::query(AssertSqlSafe(format!(
        "select {id_quoted}, {vec_quoted} \
         from {} \
         where begin_snapshot > {born_after} \
           and {} \
         order by loom_row_id",
        inline_table_name(tid),
        mvcc_live_pred(at),
    )))
```

- [x] **Step 4: Re-run Step 1's suites — green**

```bash
buck2 test //src/control-plane/postgres:iceberg-inline \
  //src/control-plane/postgres:iceberg-inline-types //src/control-plane/postgres:iceberg-inline-vector \
  //src/control-plane/postgres:vector-index-inline-delta \
  //src/control-plane/postgres:inline-flush-trigger \
  //src/control-plane/postgres:overwrite-end-caps-inline -j 8 \
  > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6.log
git status --porcelain src/control-plane/postgres/.sqlx   # expect empty
```

- [x] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek6.log 2>&1; grep -E "Failed" /tmp/prek6.log || echo CLEAN
git add src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/src/vector_index.rs
git commit -m "refactor(iceberg): inline-table access preamble helpers + loop-invariant insert SQL"
```

---

### Task 7: Batch `project_files` column stats via unnest (spec item J — touches compile-time SQL)

The per-stat INSERT inside the per-file loop becomes one `unnest` INSERT per
`project_files` call. The per-file `data_file` insert keeps its
`returning data_file_id` (the id feeds the stat rows). Same rows written, same
transaction.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs` (`project_files`)
- Modify: `src/control-plane/postgres/.sqlx/` (regenerated)

- [x] **Step 1: Pinning suites green pre-change**

```bash
buck2 test //src/control-plane/postgres:iceberg-column-stats \
  //src/control-plane/postgres:iceberg-files-with-stats \
  //src/control-plane/postgres:iceberg-landing //src/control-plane/postgres:iceberg-writer -j 8 \
  > /tmp/t7pre.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t7pre.log
```

- [x] **Step 2: Rewrite `project_files`**:

```rust
/// Write the data-file rows for loom snapshot `at`, plus every file's
/// per-column footer stats in ONE batched insert (previously one INSERT per
/// stat row — an N×M loop), all in the caller's transaction so a written file
/// always carries its stats.
pub async fn project_files(
    conn: &mut PgConnection,
    table_id: i64,
    at: SnapshotId,
    files: &[ProjectedFile],
) -> Result<()> {
    let mut stat_file_ids: Vec<i64> = Vec::new();
    let mut stat_columns: Vec<String> = Vec::new();
    let mut stat_null_counts: Vec<i64> = Vec::new();
    let mut stat_sizes: Vec<i64> = Vec::new();
    let mut stat_mins: Vec<Option<String>> = Vec::new();
    let mut stat_maxs: Vec<Option<String>> = Vec::new();

    for f in files {
        let data_file_id = sqlx::query_scalar!(
            "insert into iceberg_mirror.data_file \
             (table_id, path, file_format, record_count, file_size_bytes, begin_snapshot) \
             values ($1, $2, $3, $4, $5, $6) returning data_file_id as \"id!\"",
            table_id,
            f.path,
            f.file_format,
            f.record_count,
            f.file_size_bytes,
            at.0,
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;

        for s in &f.column_stats {
            stat_file_ids.push(data_file_id);
            stat_columns.push(s.column_name.clone());
            stat_null_counts.push(s.null_count);
            stat_sizes.push(s.column_size_bytes);
            stat_mins.push(s.min.as_ref().map(crate::iceberg_stats::stat_to_text));
            stat_maxs.push(s.max.as_ref().map(crate::iceberg_stats::stat_to_text));
        }
    }

    if !stat_file_ids.is_empty() {
        sqlx::query!(
            "insert into iceberg_mirror.data_file_column_stat \
             (data_file_id, column_name, null_count, column_size_bytes, min_value, max_value) \
             select * from unnest($1::bigint[], $2::text[], $3::bigint[], $4::bigint[], \
                                  $5::text[], $6::text[])",
            &stat_file_ids,
            &stat_columns,
            &stat_null_counts,
            &stat_sizes,
            &stat_mins as &[Option<String>],
            &stat_maxs as &[Option<String>],
        )
        .execute(&mut *conn)
        .await
        .map_err(backend)?;
    }
    Ok(())
}
```

(The `&x as &[Option<String>]` casts are sqlx's documented idiom for nullable
array binds. If `stat_to_text`'s signature takes `&StatValue`, the closure
form matches the current call — copy it verbatim from the deleted loop.)

- [x] **Step 3: Refresh the `.sqlx` cache and verify freshness**

```bash
bash tools/sqlx-prepare.sh
git status --porcelain src/control-plane/postgres/.sqlx   # expect a delta (new stat query)
buck2 test //src/control-plane/postgres:sqlx-cache-check -j 8 > /tmp/t7sqlx.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t7sqlx.log
```

- [x] **Step 4: Re-run the pinning suites — green**

```bash
buck2 test //src/control-plane/postgres:iceberg-column-stats \
  //src/control-plane/postgres:iceberg-files-with-stats \
  //src/control-plane/postgres:iceberg-landing //src/control-plane/postgres:iceberg-writer \
  //src/control-plane/postgres:iceberg_catalog -j 8 \
  > /tmp/t7.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t7.log
```

- [x] **Step 5: prek + commit (production change + `.sqlx` delta in the SAME commit)**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek7.log 2>&1; grep -E "Failed" /tmp/prek7.log || echo CLEAN
git add src/control-plane/postgres/src/iceberg_mirror.rs src/control-plane/postgres/.sqlx
git commit -m "perf(iceberg): batch project_files column stats via unnest"
```

---

### Task 8: `PageReq` keyset fetch-limit helpers (spec item K)

The "+1 sentinel" that `Page::from_keyset` consumes is re-derived at four call
sites in two overflow styles. Two documented methods on `PageReq` own it.

**Files:**
- Modify: `src/control-plane/core/src/page.rs`
- Test (append): `src/control-plane/core/tests/page.rs`
- Modify: `src/control-plane/postgres/src/lineage.rs` (2 sites)
- Modify: `src/control-plane/memory/src/lineage.rs` (2 sites)

- [x] **Step 1: Append the failing tests** to `core/tests/page.rs`:

```rust
#[test]
fn fetch_limit_helpers_carry_the_plus_one_sentinel() {
    use control_plane_core::PageReq;
    assert_eq!(PageReq::limit(3).fetch_limit_i64(), 4);
    assert_eq!(PageReq::unbounded().fetch_limit_i64(), i64::MAX);
    assert_eq!(PageReq::limit(u32::MAX).fetch_limit_i64(), i64::from(u32::MAX) + 1);
    assert_eq!(PageReq::limit(3).fetch_take(), 4);
    assert_eq!(PageReq::unbounded().fetch_take(), usize::MAX);
}
```

Run: `buck2 test //src/control-plane/core:page > /tmp/t8red.log 2>&1; grep -E "Tests finished|error" /tmp/t8red.log | head -3`
— expect RED (missing methods).

- [x] **Step 2: Implement** in `core/src/page.rs` inside `impl PageReq`:

```rust
    /// Keyset fetch size as a SQL `LIMIT` bind: `limit + 1` — the "+1
    /// sentinel" [`Page::from_keyset`] consumes to detect a next page.
    /// Unbounded → `i64::MAX`.
    #[must_use]
    pub fn fetch_limit_i64(&self) -> i64 {
        self.limit.map_or(i64::MAX, |l| i64::from(l) + 1)
    }

    /// The same "+1 sentinel" as an iterator take-count. Unbounded →
    /// `usize::MAX` (take-everything).
    #[must_use]
    pub fn fetch_take(&self) -> usize {
        self.limit.map_or(usize::MAX, |l| {
            usize::try_from(l).map_or(usize::MAX, |n| n.saturating_add(1))
        })
    }
```

- [x] **Step 3: Migrate the four sites.** postgres `lineage.rs` (`events_for`
  + `graph_closure`): `let fetch = page.limit.map_or(i64::MAX, |l|
  i64::from(l) + 1);` → `let fetch = page.fetch_limit_i64();`. Memory
  `lineage.rs` — `paginate_datasets`:

```rust
    let limited: Vec<DatasetRef> = filtered.into_iter().take(page.fetch_take()).collect();
```

  and `events_for`:

```rust
    let limited: Vec<(i64, LineageEvent)> = keyed.into_iter().take(page.fetch_take()).collect();
```

  (each replaces the whole `match page.limit { ... }` block).

- [x] **Step 4: Run — unit + both adapters' lineage contracts green**

```bash
buck2 test //src/control-plane/core:page //src/control-plane/postgres:lineage \
  //src/control-plane/memory:lineage -j 8 > /tmp/t8.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t8.log
```

- [x] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek8.log 2>&1; grep -E "Failed" /tmp/prek8.log || echo CLEAN
git add src/control-plane/core src/control-plane/postgres/src/lineage.rs src/control-plane/memory/src/lineage.rs
git commit -m "refactor(control-plane): PageReq keyset fetch-limit helpers"
```

---

### Task 9: Collapse the N+1 reads (spec items M + N — touches compile-time SQL)

`vector_indexes_for` becomes one query; `events_for` hydrates all page
datasets with one `event_id = any($1)` query (deleting `event_datasets`,
whose only callers these were). A new hydration contract pins the surface
FIRST, green against current code.

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs` (new
  `events_for_hydration_contract`)
- Test (append): `src/control-plane/postgres/tests/lineage.rs`,
  `src/control-plane/memory/tests/lineage.rs` (wire the contract)
- Modify: `src/control-plane/postgres/src/ontology.rs` (`vector_indexes_for`)
- Modify: `src/control-plane/postgres/src/lineage.rs` (`events_for`; delete
  `event_datasets`)
- Modify: `src/control-plane/postgres/.sqlx/` (regenerated)

- [x] **Step 1: Add the pin (GREEN against current code).** Append to
  `testkit/src/lib.rs` (beside `lineage_pagination_contract`):

```rust
/// Contract: `events_for` hydrates every event's inputs/outputs completely
/// and in ordinal order, however the adapter batches the reads (pins the
/// per-event-N+1 → `event_id = any($1)` collapse). `cp` must be freshly empty.
pub async fn events_for_hydration_contract<CP: Lineage>(cp: &CP) {
    let ds = |n: &str| DatasetRef {
        namespace: "w".to_string(),
        name: n.to_string(),
    };
    let run = RunId(uuid::Uuid::new_v4());
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    for i in 0..3 {
        cp.emit(LineageEvent {
            run_id: run,
            event_type: EventType::Complete,
            event_time: ts,
            inputs: vec![ds(&format!("in{i}.a")), ds(&format!("in{i}.b"))],
            outputs: vec![ds(&format!("out{i}.a")), ds(&format!("out{i}.b"))],
            payload: serde_json::json!({ "i": i }),
        })
        .await
        .unwrap();
    }
    let page = cp.events_for(&run, PageReq::unbounded()).await.unwrap();
    assert_eq!(page.items.len(), 3, "all three events returned in order");
    for (i, e) in page.items.iter().enumerate() {
        assert_eq!(
            e.inputs,
            vec![ds(&format!("in{i}.a")), ds(&format!("in{i}.b"))],
            "event {i}: inputs hydrated in ordinal order"
        );
        assert_eq!(
            e.outputs,
            vec![ds(&format!("out{i}.a")), ds(&format!("out{i}.b"))],
            "event {i}: outputs hydrated in ordinal order"
        );
    }
}
```

Wire it — append to `postgres/tests/lineage.rs`:

```rust
#[tokio::test]
async fn postgres_passes_events_for_hydration_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::events_for_hydration_contract(&cp).await;
}
```

and to `memory/tests/lineage.rs`:

```rust
#[tokio::test]
async fn memory_passes_events_for_hydration_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::events_for_hydration_contract(&cp).await;
}
```

Run both — expect GREEN (this pins CURRENT behavior; a red here means the pin
is mis-written — fix the test):

```bash
buck2 test //src/control-plane/postgres:lineage //src/control-plane/memory:lineage -j 8 \
  > /tmp/t9pin.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t9pin.log
```

- [x] **Step 2: `vector_indexes_for` — one query.** Replace the postgres impl
  (`ontology.rs`), mirroring `vector_index_def_row`'s field mapping verbatim:

```rust
    async fn vector_indexes_for(&self, type_name: &TypeName) -> Result<Vec<VectorIndexDef>> {
        let rows = sqlx::query!(
            "select name, property_name, metric, index_kind, nlist, m, ef_construction \
             from ontology.vector_index_definition where type_name = $1",
            type_name.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(VectorIndexDef {
                name: r.name,
                type_name: type_name.clone(),
                property: r.property_name,
                metric: r.metric.parse()?,
                spec: IndexSpec::from_label(
                    Some(r.index_kind.as_str()),
                    r.nlist.map(|v| v as u32),
                    r.m.map(|v| v as u32),
                    r.ef_construction.map(|v| v as u32),
                )?,
            });
        }
        Ok(out)
    }
```

(Copy the exact `spec:`/cast expressions from `vector_index_def_row` — if that
fn uses `u32::try_from` instead of `as`, mirror it. `vector_index_def_row`
itself is unchanged; `get_vector_index` still uses it.)

- [x] **Step 3: `events_for` — one hydration query.** Replace the per-event
  loop in postgres `lineage.rs` and delete `event_datasets`:

```rust
        let ids: Vec<i64> = rows.iter().map(|r| r.event_id).collect();
        // ONE query hydrates every page event's datasets (was 2 queries per
        // event). `order by event_id, ordinal` preserves each direction's
        // ordinal order after the per-event split below.
        let ds_rows = sqlx::query!(
            "select event_id, direction, namespace, name from lineage.event_dataset \
             where event_id = any($1) order by event_id, ordinal",
            &ids,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut inputs: std::collections::HashMap<i64, Vec<DatasetRef>> =
            std::collections::HashMap::new();
        let mut outputs: std::collections::HashMap<i64, Vec<DatasetRef>> =
            std::collections::HashMap::new();
        for d in ds_rows {
            let bucket = if d.direction == "input" {
                &mut inputs
            } else {
                &mut outputs
            };
            bucket.entry(d.event_id).or_default().push(DatasetRef {
                namespace: d.namespace,
                name: d.name,
            });
        }
        let mut keyed: Vec<(i64, LineageEvent)> = Vec::with_capacity(rows.len());
        for r in rows {
            let event_id = r.event_id;
            keyed.push((
                event_id,
                LineageEvent {
                    run_id: *run,
                    event_type: r.event_type.parse()?,
                    event_time: r.event_time,
                    inputs: inputs.remove(&event_id).unwrap_or_default(),
                    outputs: outputs.remove(&event_id).unwrap_or_default(),
                    payload: r.payload,
                },
            ));
        }
```

(The page-assembly tail — `Page::from_keyset` + the items/next remap — is
unchanged. The first query and its cursor/fetch logic are unchanged.)

- [x] **Step 4: Refresh `.sqlx` (new queries in, `event_datasets`' entry out)
  and run the pins**

```bash
bash tools/sqlx-prepare.sh
buck2 test //src/control-plane/postgres:sqlx-cache-check \
  //src/control-plane/postgres:lineage //src/control-plane/postgres:lineage-roundtrip \
  //src/control-plane/postgres:ontology //src/control-plane/memory:lineage -j 8 \
  > /tmp/t9.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t9.log
```

Expected: `Fail 0` — including the Step-1 hydration pin and the
`vector_indexes_for` assertions inside `ontology_contract`.

- [x] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek9.log 2>&1; grep -E "Failed" /tmp/prek9.log || echo CLEAN
git add src/control-plane/testkit src/control-plane/postgres src/control-plane/memory/tests/lineage.rs
git commit -m "perf(control-plane): collapse N+1 reads in vector_indexes_for + events_for"
```

---

### Task 10: Memory catalog keyed by `TableRef` (spec item L)

`CatalogState`'s three maps drop the cloned `(String, String)` tuples for the
already-`Eq + Hash` `TableRef`. Mechanical; compiler-driven.

**Files:**
- Modify: `src/control-plane/memory/src/catalog.rs`
- Modify: `src/control-plane/memory/src/transaction.rs` (key construction
  sites)
- Modify: `src/control-plane/memory/src/lib.rs` (key construction at `:117`
  in `seed_catalog` and `:169` in `drop_table_catalog`)

- [x] **Step 1: Pinning suites green pre-change**

```bash
buck2 test //src/control-plane/memory:catalog //src/control-plane/memory:snapshot \
  //src/control-plane/memory:tx //src/control-plane/memory:facade -j 8 \
  > /tmp/t10pre.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t10pre.log
```

- [x] **Step 2: Re-key the state**:

```rust
#[derive(Default)]
pub(crate) struct CatalogState {
    pub(crate) next_snapshot: i64,
    pub(crate) snapshots: Vec<Snapshot>,
    pub(crate) tables: HashMap<TableRef, Versioned<()>>,
    pub(crate) columns: HashMap<TableRef, Vec<Versioned<ColumnDef>>>,
    pub(crate) files: HashMap<TableRef, Vec<Versioned<FileRef>>>,
}
```

`latest_live` takes `key: &TableRef`. Then sweep every construction site the
compiler flags: `let key = (table.schema.clone(), table.name.clone());` →
either `cat.files.get(table)` directly (lookups take `&TableRef`) or
`let key = table.clone();` where an owned key is inserted. All three files
(`catalog.rs`, `transaction.rs`, and `lib.rs`'s `seed_catalog:117` +
`drop_table_catalog:169`); no logic changes.

- [x] **Step 3: Re-run Step 1's suites — green; commit**

```bash
buck2 test //src/control-plane/memory:catalog //src/control-plane/memory:snapshot \
  //src/control-plane/memory:tx //src/control-plane/memory:facade -j 8 \
  > /tmp/t10.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t10.log
buck2 run //tools:prek -- run --all-files > /tmp/prek10.log 2>&1; grep -E "Failed" /tmp/prek10.log || echo CLEAN
git add src/control-plane/memory
git commit -m "refactor(memory): key catalog state by TableRef"
```

---

### Task 11: Memory tx ordered staged-write log (spec item O — whitelisted change 3)

`MemoryTx` merges `staged_files` + `staged_replacements` into ONE ordered
`Vec<StagedWrite>` replayed in staging order — mirroring postgres
`IcebergTx`'s `WriteMode`-tagged log and removing the replay-order divergence
(a replace staged before an append currently end-caps the append on memory but
not on postgres). Contract first, proven green on postgres.

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs` (new
  `snapshot_write_order_contract`)
- Test (append): `src/control-plane/postgres/tests/iceberg_control_plane.rs`,
  `src/control-plane/memory/tests/snapshot.rs`
- Modify: `src/control-plane/postgres/BUCK` (`iceberg-control-plane` target
  gains the testkit dep)
- Modify: `src/control-plane/memory/src/transaction.rs`

- [x] **Step 1: Add the contract** (beside `snapshot_replace_contract`; note
  it reads through `cp.catalog()` so it runs against BOTH
  `IcebergControlPlane` and `MemoryControlPlane`):

```rust
/// Contract: staged writes replay in STAGING ORDER within one `Tx`. A
/// `replace_files` end-caps only what is live before it in the log, so a
/// replace staged BEFORE an append leaves the append's files live — postgres
/// `IcebergTx` semantics, which the memory fake must match. `cp` must be
/// freshly empty.
pub async fn snapshot_write_order_contract<C: control_plane_core::ControlPlane>(cp: &C) {
    use control_plane_core::{ColumnSpec, DataFile, FileFormat, PageReq, TableRef};
    let t = TableRef {
        schema: "main".into(),
        name: "write_order".into(),
    };
    let file = |path: &str, rows: i64| DataFile {
        path: path.into(),
        path_is_relative: true,
        file_format: FileFormat::Parquet,
        record_count: rows,
        file_size_bytes: rows * 16,
        column_stats: vec![],
        parquet_footer_size: Some(10),
    };
    let cols = vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }];

    // Seed: create + append f0.
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(&t, &cols).await.unwrap();
    tx.append_files(&t, &[file("f0.parquet", 1)]).await.unwrap();
    let s1 = tx.commit().await.unwrap().expect("seed snapshot");

    // One tx: REPLACE with r.parquet, THEN APPEND a.parquet.
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(&t, &cols).await.unwrap(); // idempotent; IcebergTx resolves columns in-tx
    tx.replace_files(&t, &[file("r.parquet", 2)]).await.unwrap();
    tx.append_files(&t, &[file("a.parquet", 3)]).await.unwrap();
    let s2 = tx.commit().await.unwrap().expect("write snapshot");

    let mut live: Vec<String> = cp
        .catalog()
        .files(&t, s2, PageReq::unbounded())
        .await
        .unwrap()
        .into_iter()
        .map(|f| f.path)
        .collect();
    live.sort();
    assert_eq!(
        live,
        vec!["a.parquet".to_string(), "r.parquet".to_string()],
        "replace-then-append: the append (staged AFTER the replace) stays live"
    );

    // f0 was live before the replace -> end-capped; time travel still sees it.
    let back: Vec<String> = cp
        .catalog()
        .files(&t, s1, PageReq::unbounded())
        .await
        .unwrap()
        .into_iter()
        .map(|f| f.path)
        .collect();
    assert_eq!(back, vec!["f0.parquet".to_string()], "prior snapshot time-travels");
}
```

- [x] **Step 2: Wire postgres FIRST — must be GREEN (authority check).**
  Append to `postgres/tests/iceberg_control_plane.rs` (reusing its
  `iceberg_cp` fixture helper):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_order_contract() {
    let fx = PgFixture::shared();
    let (cp, _wh) = iceberg_cp(fx).await;
    control_plane_testkit::snapshot_write_order_contract(&cp).await;
}
```

Add `"//src/control-plane/testkit:testkit",` to the `iceberg-control-plane`
target's `deps` in `src/control-plane/postgres/BUCK`.

```bash
buck2 test //src/control-plane/postgres:iceberg-control-plane -j 8 \
  > /tmp/t11pg.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t11pg.log
```

Expected: `Fail 0`. **If this is red, STOP** — the divergence claim is
inverted; re-verify `IcebergTx::commit`'s replay before touching memory.

- [x] **Step 3: Wire memory — expect RED.** Append to
  `memory/tests/snapshot.rs`:

```rust
#[tokio::test]
async fn snapshot_write_order_contract() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::snapshot_write_order_contract(&cp).await;
}
```

```bash
buck2 test //src/control-plane/memory:snapshot > /tmp/t11red.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t11red.log
```

Expected: exactly ONE fail (the new test — memory applies appends before
replacements, so `a.parquet` gets end-capped).

- [x] **Step 4: Restructure `MemoryTx`.** In `memory/src/transaction.rs`:

```rust
/// One staged catalog file write, in STAGING ORDER. Mirrors the postgres
/// `IcebergTx`'s `WriteMode`-tagged log: append and replace stay one ordered
/// sequence, so a replace end-caps only what is live before it in the log.
enum StagedWrite {
    Append(TableRef, Vec<DataFile>),
    Replace(TableRef, Vec<DataFile>),
}
```

`MemoryTx` fields: `staged_files` + `staged_replacements` (`transaction.rs:25-26`)
are replaced by `staged_writes: Vec<StagedWrite>`; update the constructor in
`memory/src/lib.rs:214-215` (`staged_writes: Vec::new()`). The `Tx` methods:

```rust
    async fn append_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()> {
        self.staged_writes
            .push(StagedWrite::Append(table.clone(), files.to_vec()));
        Ok(())
    }

    async fn replace_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()> {
        self.staged_writes
            .push(StagedWrite::Replace(table.clone(), files.to_vec()));
        Ok(())
    }
```

(Keep the existing `#[tracing::instrument]` attributes.) In `commit`, the two
grouped loops ("Apply staged file appends" + "Apply staged file replacements")
become one ordered replay — each arm's body is the old loop body moved
verbatim (with Task 10's `TableRef` keys):

```rust
            // --- catalog file writes, replayed in STAGING order (matches the
            // postgres IcebergTx: a replace end-caps only files live before
            // it in the log; a later append stays live) ---
            for write in self.staged_writes {
                match write {
                    StagedWrite::Append(table, files) => {
                        let s = cat.new_snapshot();
                        last_snapshot = Some(s);
                        for file in files {
                            cat.files.entry(table.clone()).or_default().push(Versioned {
                                begin: s,
                                end: None,
                                val: FileRef {
                                    path: file.path,
                                    record_count: file.record_count,
                                    file_size_bytes: file.file_size_bytes,
                                },
                            });
                        }
                    }
                    StagedWrite::Replace(table, files) => {
                        let s = cat.new_snapshot();
                        last_snapshot = Some(s);
                        if let Some(existing) = cat.files.get_mut(&table) {
                            for f in existing.iter_mut() {
                                if f.end.is_none() {
                                    f.end = Some(s);
                                }
                            }
                        }
                        for file in files {
                            cat.files.entry(table.clone()).or_default().push(Versioned {
                                begin: s,
                                end: None,
                                val: FileRef {
                                    path: file.path,
                                    record_count: file.record_count,
                                    file_size_bytes: file.file_size_bytes,
                                },
                            });
                        }
                    }
                }
            }
```

Everything else in `commit` is untouched: the three-lock atomicity block and
its comment, the compaction precondition validation (still BEFORE any
mutation), staged creates first, compactions last (postgres also replays
compactions after all file writes), the `staged_any` notify. Update the
`staged_any` computation if it referenced the deleted fields (it references
`self.staged` — the queue — only; leave it).

- [x] **Step 5: Run — memory contract now GREEN; full memory + postgres tx
  suites green**

```bash
buck2 test //src/control-plane/memory: //src/control-plane/postgres:iceberg-control-plane -j 8 \
  > /tmp/t11.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t11.log
```

- [x] **Step 6: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek11.log 2>&1; grep -E "Failed" /tmp/prek11.log || echo CLEAN
git add src/control-plane/testkit src/control-plane/memory src/control-plane/postgres
git commit -m "fix(memory): ordered staged-write log matches postgres tx replay order"
```

---

### Task 12: Affected-package sweep + register close

**Files:**
- Modify: `docs/ROADMAP.md` (`#road-cp-adapter-hygiene` — locate by id)

- [x] **Step 1: Whole-tree build + affected-package sweep.** Core changed, so
  every downstream service rebuilds; the adapters feed engine/worker/ingest/
  query-api through unchanged signatures but must be swept:

```bash
buck2 build -M none //src/... > /tmp/build12.log 2>&1; tail -3 /tmp/build12.log
buck2 test //src/control-plane/core: //src/control-plane/memory: \
  //src/control-plane/testkit: //src/control-plane/postgres: \
  //src/services/engine-serving: //src/services/engine: \
  //src/services/worker: //src/services/query-api: //src/services/ingest: -j 8 \
  > /tmp/sweep12.log 2>&1; grep -E "Tests finished|FAIL" /tmp/sweep12.log
```

Expected: build success; `Fail 0`. (Local runs keep `-j 8`; in a cloud
session do NOT widen to a bare whole-tree `buck2 test` and `buck2 clean`
between heavy phases if disk pressure appears.)

- [x] **Step 2: Close the ROADMAP item** — replace the
  `road-cp-adapter-hygiene` entry (checkbox, status, prose) with:

```markdown
- [x] **Control-plane adapter hygiene batch** `{#road-cp-adapter-hygiene area:quality status:done from:2026-07-02-pillar-idioms-audit-design pr:- spec:2026-07-02-pillar-idioms-audit-design}`
  Done, one commit per sub-item. Postgres: `role_exists`/`object_type_exists` over `PgExecutor` collapse the 10 duplicated exists checks (in-tx sites kept in-tx); `links`/`links_to` share `LinkRow` + `link_defs`; ONE generic source-carrying `backend()` boxing helper — deleted all five module-local variants (`iceberg_read::be`, `iceberg_landing::be`, `iceberg_mirror::iceberg_err`, `puffin::be`, and `vector_index`'s Display-flattening `backend` shadow) plus the inline `.to_string()` flattens; inline-table preamble → `inline_table_exists` + `mvcc_live_pred` + `quote_ident` (the `AssertSqlSafe` argument now lives in one place); `inline_append`'s statement text hoisted out of its row loop; `project_files` stat inserts batched via `unnest`; `vector_indexes_for` and `events_for` N+1s collapsed (one query / `event_id = any($1)`, pinned by the new `events_for_hydration_contract`; hydration follow-ups tracked by [[fut-lineage-events-page-hydration]]). Core: enum wire codecs (`Cardinality`/`EventType`/`ActionKind` `as_str`+`FromStr`, `Action`/`Effect` `as_str`) with FAIL-LOUD unknown tokens (was silent One/Start/Insert coercion — whitelisted); `PolicyTarget::key_parts()`; shared `check_grant_target`/`check_policy_write` write-time decisions (adapters supply lookups); `PageReq::{fetch_limit_i64,fetch_take}` own the keyset +1 sentinel; parse/dim failures (`Metric`/`IndexKind`/`from_label`/`pack_rows`) reclassified `Backend`→`Validation` (whitelisted honest-classification change; no status code changes on currently reachable planes; nothing pinned the old class). Memory: catalog keyed by `TableRef`; `MemoryTx` replays ONE ordered `StagedWrite` log matching postgres `IcebergTx` (whitelisted replay-order fix, pinned by the new `snapshot_write_order_contract`, proven green on postgres first). The spec's `dc!` sub-item was already shipped by #296 and dropped.
```

- [x] **Step 3: Validate + commit**

```bash
bash tools/docs.sh validate
buck2 run //tools:prek -- run --all-files > /tmp/prek12.log 2>&1; grep -E "Failed" /tmp/prek12.log || echo CLEAN
git add docs/ROADMAP.md
git commit -m "docs(registers): close road-cp-adapter-hygiene"
```

If the PR number is known when this runs (`gh pr view --json number` on the
pushed branch), replace `pr:-` with `pr:#<N>`; otherwise leave it for the
finishing flow.

---

## Self-review notes (performed at plan time)

- **Spec coverage:** all 15 atomized sub-items of the spec's
  road-cp-adapter-hygiene section are accounted for — 14 land in Tasks 1-11,
  the `dc!` item is verified ABSORBED (#296) and only recorded in the close
  prose. The spec's `.sqlx` note is honored where true (Tasks 7, 9) and
  refuted with a mechanism (hash-keyed cache) where not.
- **Type consistency:** `object_type_exists`/`role_exists` take
  `impl PgExecutor<'_>` (Task 1) matching the in-tx calls in
  `add_inheritance`/`define_action` and the Task-3 grant/set_policy uses;
  `link_defs` is defined infallible in
  Task 1 and explicitly redefined fallible in Task 2 (single site, sequenced);
  `check_policy_write(policy, Option<&HashSet<String>>)` matches both
  adapters' Task-3 call shapes and the Task-3 unit tests;
  `fetch_limit_i64`/`fetch_take` match the four call-site domains;
  `StagedWrite` arms carry `(TableRef, Vec<DataFile>)` matching Task 10's
  re-keyed maps.
- **Red-test honesty:** three real reds (corrupt-cardinality fixture in
  Task 2, class assertion in Task 5, write-order contract on memory in
  Task 11), each with a STOP instruction if it unexpectedly passes; two
  green-first pins (Task 9 hydration contract, Task 11 postgres leg) with
  fix-the-test / STOP guidance respectively.
- **Known judgment calls, recorded:** helper naming (`inline_table_exists`
  vs the spec's `existing_inline_table`), two typed fetch-limit methods vs
  the spec's single `fetch_limit()`, new-codec errors classed `Validation`
  from birth (so Task 5 only reclassifies pre-existing sites), and leaving
  `iceberg_gc.rs`'s name-string `to_regclass` probe alone.
- **Adversarial-review corrections applied (2026-07-03):** Task 4's variant
  census completed by re-grep — SIX named Backend-boxing fns, not four
  (added `iceberg_landing::be` 24 sites, `iceberg_mirror::iceberg_err` 4
  sites, and `vector_index`'s Display-flattening `backend` SHADOW with 10
  call sites + 5 inline flattens); Task 5/whitelist-2 impact framing
  corrected (honest classification + Display prefix only — no currently
  reachable plane maps the class to a status code); exists-check census is
  10× incl. `assign_role`, with `define_action`'s check kept in-tx; Task 10
  covers `memory/src/lib.rs` (`seed_catalog`/`drop_table_catalog`) as the
  third file; `target_cols` has 6 sites; core `ontology.rs`/`acl.rs` need
  the `ControlPlaneError` import added.

## Post-final-review addendum (2026-07-03)

The final adversarial review found the Task 9 collapsed `events_for` query's
naive bucketing (`direction != "input"` → outputs) silently MISFILED corrupt
direction tokens into outputs, where the pre-branch per-event queries silently
DROPPED them. Fixed on this branch as part of whitelist entry 1's fail-loud
family: the bucketing now matches the token and errors
(`unknown lineage direction '{other}'`, `ControlPlaneError::Validation`) on
anything but `input`/`output`, mirroring the core enum codecs. Red-first
corrupt-row test (`corrupt_direction_token_is_a_loud_error`, direct-SQL
`'sideways'` token) proved the misfiling before the fix. Rust-side bucketing —
SQL text untouched, zero `.sqlx` delta.
