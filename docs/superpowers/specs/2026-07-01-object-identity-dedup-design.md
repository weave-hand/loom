# Object-identity dedup for many-to-many traversal — design (2026-07-01)

- **Date:** 2026-07-01
- **Area:** query
- **Register item:** [[road-object-identity-dedup]] (promotes [[fut-object-identity-dedup]])
- **From:** [[2026-06-17-object-identity-association-design]]
- **Status:** spec (ready for a work agent to plan + build)

> Query-pillar correctness fix. Many-to-many traversal (`GET /objects/:from/links/:link`,
> `read_linked_chain`) dedups its final-target set with `SELECT DISTINCT` over the
> *visible* (ACL-projected) columns. When column-level ACL denies or masks the target's
> identity column, that dedup key is wrong: distinct objects **collapse** into one row.
> `ObjectType.identity` now exists (Part A of the association slice); rework the dedup to
> key on the target's **raw identity value** — below the masking layer — so results are
> per-object correct even when the identity is suppressed in the output.

## Problem

`read_linked_chain` (`src/services/query-api/src/handler.rs`) resolves a governed chain
and then projects the final target's **visible** columns through `compile_chain_with`
(`src/services/query-api/src/sql.rs:579`). The projection masks columns the subject may
not see — a masked column is emitted as the literal `'***'` (`MASK_MARKER`, `sql.rs:51`)
aliased to the column name — and the whole row set is deduplicated with `SELECT DISTINCT`:

```sql
SELECT DISTINCT t_2.name, '***' AS ssn, t_2.city
FROM customer t_0
  JOIN orders t_1 ON ...
  JOIN person  t_2 ON ...
WHERE <row-filters + caller predicates>
LIMIT <n>
```

A many-to-many join produces **one row per path**, so dedup is genuinely required (a
target reached by two intermediate rows appears twice). Today the dedup key is *the
visible projection*. That is only a correct proxy for object identity when the identity
column survives projection. When ACL **masks** (or **denies**) the identity column, the
key is wrong.

### Concrete failure — masked identity collapses two distinct objects

Ontology: `Person` has `identity = "ssn"` (a required PK), plus `name`, `city`. A
policy grants the subject `Read` on `Person` but **masks `ssn`**. Two distinct people
are reachable through the traversal:

| ssn (raw) | name  | city   |
|-----------|-------|--------|
| `111`     | `Kim` | `Ames` |
| `222`     | `Kim` | `Ames` |

These are two different objects (two PKs). The compiled projection masks `ssn`, so the
serving engine sees:

| ssn      | name  | city   |
|----------|-------|--------|
| `'***'`  | `Kim` | `Ames` |
| `'***'`  | `Kim` | `Ames` |

`SELECT DISTINCT` collapses them to **one** row. The traversal reports a single reachable
`Person` where there are two. The masking of a governance-sensitive column has silently
changed the *cardinality of the object set* — a correctness bug, not just a redaction.
(The symmetric "split" framing in the register item cannot occur here: every projected
column is read from the single final-target row `t_k`, so a single object's projected
tuple is constant across the paths that reach it; masking can only *reduce* distinctness,
i.e. collapse. The realizable defect is collapse/undercount.)

The same `SELECT DISTINCT {visible cols}`-off-a-JOIN shape appears in the graph tail
compiler (`compile_graph_reach_tail`, `sql.rs:1138`) and carries the identical defect.
The recursive-reachability compilers (`compile_graph_reach` `:864`,
`compile_graph_reach_union` `:887`) project `SELECT DISTINCT {cols} FROM tbl p WHERE
p.id IN (SELECT id FROM reach …)` — they select from a *single* table keyed by its PK
via the `IN`, so rows are already object-unique and the trailing `DISTINCT` is either a
no-op or a collapse hazard on masked identity; they take a simpler variant of the same
fix.

## Approach

**Dedup on the raw identity value, below the masking layer.** The identity column is a
declared, required primary key (validated at bind, [[2026-06-17-object-identity-association-design]]
Part A). The engine can read the *unmasked* `t_k.<identity>` column while the output
projection still emits `'***'` (or omits it, if denied). So the compiler partitions/keys
the dedup on the raw identity and projects the masked/visible columns unchanged.

Decision matrix, by the final-target type's declared identity and its ACL state:

- **Identity declared, and visible (not denied, not masked):** the visible projection
  already contains the identity, so `DISTINCT` is already correct — but keying on the raw
  identity is *equivalently* correct and we take the uniform path (no special case).
- **Identity declared, but masked or denied:** dedup keys on the raw `t_k.<identity>`;
  the output still masks/omits it. **This is the bug-fixing case.**
- **Identity NOT declared (`identity: None`):** there is no per-object key, so behaviour
  is unchanged — `SELECT DISTINCT` over the visible projection (documented limitation;
  without a declared PK there is no notion of "per-object correct" to uphold). This keeps
  every pre-identity type back-compatible.

Crucially, the dedup key is **never** required to be caller-visible. Unlike
`read_associations` (which *projects* the identity as the result and therefore demands it
be visible, returning `Forbidden` when it is governed — `handler.rs` `identity_is_governed`),
this fix uses the identity only as an internal grouping key. The subject still cannot
*read* a masked identity; they simply get the correct *number* of masked rows.

## SQL / query-shape change

Replace the `SELECT DISTINCT {visible cols}` terminal projection in `compile_chain_with`
(and, mirrored, in `compile_graph_reach_tail`) with an identity-keyed dedup when the
final-target type has a declared identity. The chosen shape is a **windowed
row-number**, evaluated in a subquery whose outer select keeps exactly today's visible
projection:

```sql
SELECT <visible cols: t_k.col, or '***' AS col for masked>
FROM (
  SELECT
    <same visible cols>,
    ROW_NUMBER() OVER (PARTITION BY t_k.<raw_identity>) AS _loom_rn
  FROM <from/joins>
  WHERE <conjuncts>
) _dedup
WHERE _loom_rn = 1
LIMIT <n>
```

Why row-number over `GROUP BY <raw_identity>`:

- **No per-column aggregate wrapping.** DataFusion (the sole serving engine — query-api
  is a zero-DataFusion wire client that ships this SQL over Flight SQL `CommandStatementQuery`)
  requires every non-grouped `SELECT` column to be an aggregate under `GROUP BY`. The
  window form projects the visible columns verbatim, so the existing masked/visible
  column-expression builder (`masked_col_exprs` / the inline mask branch) is reused
  unchanged in the inner select — the only new artifact is the partition clause and the
  `_loom_rn = 1` outer filter.
- **Arbitrary representative is correct.** No `ORDER BY` is needed inside the window
  (see Correctness): every projected column is functionally dependent on the partition
  key, so all rows in a partition are identical in their projected values and any one is
  a valid representative. Omitting the window `ORDER BY` also avoids sorting cost.

The `raw_identity` is the physical identity column name of the final-target `ObjectType`
(`type.identity.as_deref()`), quoted via the dialect. It is referenced at the final alias
`t_k`. When it is *masked*, the inner select still projects `'***' AS <identity>` for
output **and** references the raw `t_k.<identity>` in the `PARTITION BY` — the two are
independent expressions over the same row, so no leak occurs (the outer select never sees
the raw value).

`compile_graph_reach` / `compile_graph_reach_union` (single-table `p WHERE p.id IN (…)`)
need only drop the redundant `DISTINCT`: the `IN (SELECT id FROM reach)` already yields
one `p` row per reachable PK, so plain `SELECT {cols}` is object-correct and the masked
`DISTINCT` collapse cannot arise. (Keeping `DISTINCT` there is what *introduces* the
masked-collapse bug in the reachability read; removing it is both the fix and a
simplification.)

### Threading the identity into the compiler

`compile_chain_with` and `compile_graph_reach_tail` gain the final-target identity as an
`Option<&str>` parameter (`None` ⇒ keep today's `SELECT DISTINCT` path). The caller
`read_linked_chain` already holds the final-target `ObjectType` (`metas.last()`), so it
passes `target.otype.identity.as_deref()`. No new ACL loads, no new round-trips — the
denied/masked sets are already computed per position (`HopMeta`), and the raw identity is
a physical column that requires no visibility to *reference* in SQL.

## Correctness argument — why keying on raw identity below masking is right

1. **Single-source projection ⇒ functional dependence.** Every projected column of the
   final target is read from the single row `t_k` (the target's own table row). The
   declared identity is that row's primary key. Therefore, for a fixed identity value,
   all projected columns (masked or not) take a single, well-defined value — they are
   functionally dependent on the identity. A partition by the raw identity groups exactly
   the rows belonging to one object, and any representative row reproduces that object's
   projected tuple. Hence the `_loom_rn = 1` pick is deterministic in its *output*, even
   though *which physical path row* is picked is arbitrary.

2. **Correct cardinality under masking.** The number of output rows equals the number of
   distinct raw identity values in the (governed) join — i.e. the number of distinct
   reachable target objects the row-filters permit — regardless of whether the identity
   (or any other column) is masked. Masking changes only the *displayed* value of a
   column, never the partition key, so it can no longer change the object count. This is
   exactly the property `SELECT DISTINCT`-over-visible-columns fails to hold.

3. **No visibility leak.** The raw identity appears only in `PARTITION BY` inside the
   subquery; the outer select projects only the visible columns (masked/denied handled by
   the unchanged column-expression builder). A subject who cannot read the identity still
   never receives it — they receive the correct count of masked (or identity-omitted)
   rows.

4. **Governance is unchanged.** Row-filters, per-hop Read gates, and caller predicates are
   built identically (`chain_from_where` is untouched); the change is purely in the
   terminal dedup/projection. A row-filter that removes rows still removes the
   corresponding partitions.

## Interaction with pagination / ordering

Traversal reads are **unordered** today — `compile_chain_with` appends only
`dialect.limit_clause(limit)` with no `ORDER BY` (`sql.rs:606`). Two consequences:

- The window needs no `ORDER BY` within `PARTITION BY` (any representative is correct, per
  Correctness §1). We deliberately omit it to avoid a sort.
- `LIMIT` semantics improve. Today `DISTINCT … LIMIT n` can return fewer than `n` distinct
  objects when masked collapse merges rows *before* the limit is understood by a client as
  "objects". After the fix, `LIMIT n` bounds *deduped object rows*, so it truncates the
  correct object set. Because the traversal is unordered, *which* `n` objects are returned
  remains unspecified (unchanged from today); only the guarantee that each returned row is
  a distinct object is added. Ordered/keyset pagination over traversal is out of scope
  (there is no traversal ordering to page over yet).

## Error handling

- **No new error variants.** Unlike `read_associations`, this path never fails on a
  governed identity — the identity is an internal key, not a projected result, so
  `NoIdentity`/`Forbidden` are *not* raised here. A masked or denied identity is a normal,
  supported case that now dedups correctly.
- **`identity: None`** falls back to `SELECT DISTINCT` (no error) — back-compatible with
  every type that predates declared identity.
- The empty-projection guard (`to_allowed.is_empty()` ⇒ `Forbidden`, `handler.rs:848`)
  and all existing chain-resolution errors (`BadChain`, `BadFilter`, depth bound) are
  unchanged.

## Testing

loom tests are `rust_test` **integration** targets, never inline `#[cfg(test)]` (the
`no-inline-tests` hook enforces this). Two layers:

### Compiler unit tests (`src/services/query-api`, pure-logic `rust_test`)

Mirror the existing `sql.rs` compiler tests (e.g. the chain-compile tests). Assert the
generated SQL text for `compile_chain_with`:

- **identity present + masked** ⇒ emits the windowed `ROW_NUMBER() OVER (PARTITION BY
  t_k.<identity>)` subquery with `_loom_rn = 1`, the identity appears in `PARTITION BY`,
  and the output projection emits `'***' AS <identity>` (raw referenced only in the
  partition).
- **identity present + visible** ⇒ same windowed shape, partition on the visible identity.
- **identity `None`** ⇒ byte-identical to today's `SELECT DISTINCT …` (regression guard on
  the fallback).
- **`compile_graph_reach` / `_union`** ⇒ the `DISTINCT` is dropped from the `p`-projection.

### e2e regression (reuse `//src/services/query-api:e2e-support`)

Add a fixture test that reuses the shared e2e support library
(`tests/e2e_support.rs` — `use e2e_support::{…}`; add `":e2e-support"` to `deps`),
extending it only if a new shared helper is genuinely reusable. Seed a many-to-many
traversal whose final-target type has a declared `identity`, and seed **two distinct
target objects that share every non-identity column** (the collapse trap: e.g. two
`Person` rows with the same `name`/`city`, different `ssn`). Then:

- **Baseline (identity visible):** a subject with full `Read` sees **two** distinct
  target rows. Anchors that the fixture really has two objects.
- **The bug case (identity masked):** a subject whose policy masks the identity column
  still gets **two** rows (identity rendered `'***'` in both). Assert row count `== 2`.
  Under today's code this returns **one** — this test fails before the fix, passes after.
- **Identity denied (dropped from projection):** same subject with the identity *denied*
  rather than masked still gets two rows (identity column absent, count preserved).
- **Genuine duplicate collapse still holds:** a single target reached via two intermediate
  paths yields **one** row (dedup still merges same-object paths) — guards against
  over-splitting.

Use the generic seed helpers (`tref`/`land`/`prop`) and the ACL helpers
(`subject_with_role`/`grant_read`, plus a mask/deny grant), and the `ids`/`ids_i64`
extractors, exactly as the existing graph/object-set e2e tests do.

## Non-goals

- **`read_associations` is untouched.** It *projects* the identity as its result and
  already requires the identity be visible (`Forbidden` otherwise). Its `DISTINCT` over the
  two identity columns is correct because the identities *are* the projection. This spec
  only fixes the object-set traversal (`read_linked_chain`) and the graph reads that
  project non-identity columns.
- **Ordered/keyset pagination over traversal.** Traversal remains unordered; adding a
  stable order + cursor is separate future work.
- **Composite / multi-column identity.** `ObjectType.identity` is a single declared
  column; multi-column PKs are out of scope (as in the association slice).
- **Changing masking semantics.** Masked columns still render `'***'`; denied columns are
  still omitted. Only the dedup key changes.

## Open questions

1. **Scope of the graph compilers.** Should `compile_graph_reach` /
   `compile_graph_reach_union` / `compile_graph_reach_tail` be fixed in the *same* PR (they
   share the identical `SELECT DISTINCT`-over-visible defect and have `identity` in hand),
   or split into a fast follow-up to keep this slice minimal? Recommendation: include the
   `compile_graph_reach_tail` window fix (it is the same JOIN shape) and the trivial
   `DISTINCT`-drop in the two reachability compilers, since leaving them buggy is an
   inconsistent governance boundary.
2. **`GROUP BY` vs `ROW_NUMBER`.** The window form is chosen to avoid per-column aggregate
   wrapping and to reuse the masked-column builder verbatim. If a future serving-engine
   change makes `GROUP BY <identity>` with `first_value(col)` cheaper, revisit — but the
   window form is strictly simpler to generate today. Confirm the DataFusion planner emits
   an efficient partition-only (`ORDER BY`-free) window over the joined input.
3. **DISTINCT fallback longevity.** Once identity is *required* on every ontology type
   (not just opt-in), the `identity: None` `DISTINCT` fallback becomes dead code and can be
   removed. Track alongside any future "mandatory identity" migration.
