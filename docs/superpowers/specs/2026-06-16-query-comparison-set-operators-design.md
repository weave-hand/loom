# Design: comparison / set operators on filters (query — read path)

> **Status:** approved design (2026-06-16). The capstone of the filter arc. Today every caller
> equality filter — on `read_object`, single-hop traversal, and multi-hop chains, at every position
> (after `2026-06-16-query-target-intermediate-filters-design.md`) — is **equality-only** (`col = v`).
> This slice adds a richer operator surface (`ne`, `lt`, `le`, `gt`, `ge`, `in`, `nin`, `isnull`,
> `isnotnull`) so a caller can express ranges, set membership, inequality, and null checks. It reuses
> the existing `control_plane_core::CompareOp` enum (the ACL row-filter vocabulary) but keeps caller
> operands on `SqlValue` so the typed coercion of `Double`/`Date`/`Timestamp` (from the
> typed-input-filters slice) is preserved.

## Goal

`GET /objects/Order?amount=gt:100` filters `amount > 100`; `?status=in:open,paid` filters set
membership; `?closed_at=isnotnull` filters non-null; `?amount=ge:100&amount=le:200` expresses a
range (two ANDed predicates on one column). The operator rides on the query-param **value**
(`op:operand`), so the key keeps the column / `<linkname>.col` positioned-filter convention intact
and operators compose with positioned chain filters (`?placed.created=ge:2026-01-01`). Each operand
still coerces to the column's declared ontology logical type via the existing `filter::coerce_filter`.

## Scope

**In scope:**
- The operator set `eq, ne, lt, le, gt, ge, in, nin, isnull, isnotnull` (full `CompareOp` parity),
  expressed value-side as `op:operand` (bare value = `eq`; null ops are the bare token, no colon).
- Per-operand typed coercion (reusing `coerce_filter`) and per-op arity validation.
- Multiple predicates per column (ANDed) via repeated query keys — the HTTP extractor switches to a
  duplicate-preserving multimap.
- Uniform application to `read_object`, single-hop traversal, and multi-hop chains at every position.

**NOT in scope (later slices):**
- **`or`-combined caller predicates.** All caller predicates are ANDed (matching today's equality
  filters and the typical filter semantics). Disjunction across caller predicates is deferred.
- **`between` sugar.** A range is two predicates (`ge` + `le`); a dedicated `between:lo,hi` operator
  is deferred (repeated keys already express any AND-combination).
- **A literal comma inside an `in` operand.** `in:` splits on commas; an operand containing a comma
  cannot be expressed. Deferred (would need an escaping or alternate-delimiter convention).
- **Text-pattern / case-insensitive matching** (`like`, `ilike`, `contains`). A separate slice;
  `CompareOp` has no such variant today.
- **Predicates on derived properties.** Derived (aggregate) properties remain non-filterable
  (eq-filters validate against physical columns only) — unchanged by this slice.
- **Caller predicates as `RowFilter`.** Deliberately NOT converged onto `RowFilter`/`ScalarValue`:
  `ScalarValue` is `{Text, Int, Bool, List}` ("Float and temporal omitted so the type derives `Eq`"),
  so converging would silently regress `Double`/`Date`/`Timestamp` filtering. Caller predicates stay
  on `SqlValue`.

## Design

### 1. Wire grammar (`http.rs` + the value-side parser)

The query-param **key** is unchanged: a bare column (`read_object`, source filters) or `<linkname>.col`
(positioned chain filters, resolved by `chain_filter::resolve_chain_filters`). The **value** carries
the operator:

| Value form | `CompareOp` | Operand arity | Example |
|---|---|---|---|
| bare (no recognized op) | `Eq` | 1 (the whole value) | `region=CA` |
| `eq:<v>` | `Eq` | 1 | `name=eq:gt:foo` (escape: literal `gt:foo`) |
| `ne:<v>` | `Ne` | 1 | `status=ne:cancelled` |
| `lt:<v>` `le:<v>` `gt:<v>` `ge:<v>` | `Lt`/`Le`/`Gt`/`Ge` | 1 | `amount=gt:100` |
| `in:<v1>,<v2>,…` | `In` | ≥1 | `status=in:open,paid` |
| `nin:<v1>,<v2>,…` | `NotIn` | ≥1 | `region=nin:NY,TX` |
| `isnull` / `isnotnull` (bare token) | `IsNull`/`IsNotNull` | 0 | `closed_at=isnull` |

**Recognition rule (single, unambiguous).** Split the value at the **first** `:` into `head` and
`rest` (`rest` absent if there is no `:`):
1. If `head` is a known op token (`eq, ne, lt, le, gt, ge, in, nin, isnull, isnotnull`) → that
   operator, with operands taken from `rest`:
   - null ops (`isnull`/`isnotnull`): `rest` must be absent or empty — else **arity error**;
   - scalar ops (`eq, ne, lt, le, gt, ge`): the single operand is `rest`; `rest` absent → **arity
     error**;
   - set ops (`in`/`nin`): operands are `rest` split on `,` (each non-empty, ≥1); `rest` absent or
     empty → **arity error**.
2. Else (`head` is not a known op token) → `Eq` with the **entire value** as the single operand.

Consequences: `amount=gt100` (no colon, `head="gt100"` not a token) → `Eq("gt100")` (→ coercion
against `Double` fails → 400, honest about the missing colon). `amount=gt` (`head="gt"`, no `rest`) →
arity error → 400. `name=eq:gt:foo` → `Eq("gt:foo")` (`rest` is everything after the first `:`, so the
`eq:` escape passes a literal `gt:foo` through). `closed_at=isnull` → `IsNull`; `closed_at=isnull:x`
(null op given an operand) → arity error → 400. `status=isnull` is the `IsNull` op (the one surprise:
to filter the literal string `"isnull"`, use `status=eq:isnull`).

Repeated keys are preserved (see §5) and each becomes its own predicate; all predicates on a column
are ANDed.

### 2. Caller-predicate type & parsing (`filter.rs`)

A new owned type carries one parsed, coerced predicate:

```rust
pub struct CallerPredicate {
    pub column: String,
    pub op: control_plane_core::CompareOp,
    /// Coerced operands: arity 0 (null ops), 1 (scalar ops), or N (set ops).
    pub values: Vec<crate::serving::SqlValue>,
}
```

`filter.rs` gains the value-side parser, built on the existing per-scalar `coerce_filter`:

```rust
pub fn coerce_predicate(
    column: &str,
    logical_ty: &str,
    raw: &str,
) -> Result<CallerPredicate, FilterError>;
```

It (a) applies the §1 recognition rule to get the `CompareOp` and operand string(s); (b) coerces
**each** operand through `coerce_filter(column, logical_ty, operand)` (unchanged — the typed-coercion
building block, still independently unit-tested); (c) validates arity for the op (scalar=1, set≥1,
null=0); and returns `CallerPredicate`. Any parse/coercion/arity failure → `FilterError` (existing
type) → mapped to `BadFilter` by the handler. `coerce_filter` itself is retained and reused — the
former eq-only call sites now go through `coerce_predicate` (bare value = `Eq`, identical result).

### 3. Compiler (`sql.rs`)

The caller-filter slot generalizes from equality pairs to predicates:
- `ChainType.eq_filters: Vec<(String, SqlValue)>` → `predicates: Vec<CallerPredicate>`.
- `compile_select`'s `eq_filters: &[(String, SqlValue)]` parameter → `predicates: &[CallerPredicate]`.

A single renderer serves both the bare-table (`read_object`, no alias) and chain (`t_i` alias) paths:

```rust
fn caller_predicate_sql(p: &CallerPredicate, alias: &str, params: &mut Vec<SqlValue>) -> String;
```

- scalar ops → `({col} {OP} ?)` (reusing the existing `op_sql(CompareOp) -> &str`), pushing the one
  operand;
- `In`/`NotIn` → `({col} IN (?, ?, …))` / `({col} NOT IN (…))`, pushing each operand (mirrors the
  existing `filter_sql` IN-expansion, but over `SqlValue`);
- `IsNull`/`IsNotNull` → `({col} IS NULL)` / `({col} IS NOT NULL)`, no params;

where `{col}` is `quote_ident(column)` (bare) or `alias."col"` (chain). Params are pushed in
conjunct-emission order, so positional `?` alignment holds exactly as today. `compile_select` and
`compile_chain` replace their inline `(col = ?)` emission with a call to this renderer per predicate.
The projection, JOINs, masking, derived subqueries, `DISTINCT`, and `LIMIT` are unchanged.

(The compiler trusts predicate arity — the handler's `coerce_predicate` validated it. A defensive
`debug_assert` on arity in the renderer guards future misuse without a release-path cost.)

### 4. Handler (`handler.rs`)

`read_object` and `read_linked_chain` keep the **visibility-then-coerce** discipline, only swapping
the coercion call:
1. **Visibility first** (unchanged): the filter column must be in the (position's) allowed projection
   and not masked — else `BadFilter` → 400. A denied/masked column is rejected before parsing, so no
   operator (including `isnull`) can probe a column the subject can't see.
2. **Then `coerce_predicate(col, ty, raw)`** against that column's declared logical type; on `Err` →
   `BadFilter` → 400.
3. Push the `CallerPredicate` into the slot — `read_object`'s `predicates`, or
   `ctypes[position].predicates` for a chain. Repeated keys on the same `(position, column)` produce
   multiple predicates, all ANDed.

The per-hop `Read` gate and N-ends governance are unchanged; caller predicates only narrow within
already-permitted visibility.

### 5. HTTP extractor (`http.rs`)

The three GET handlers switch their extractor from `Query<HashMap<String, String>>` to
`Query<Vec<(String, String)>>` (serde_urlencoded deserializes the param sequence into a `Vec`,
preserving duplicate keys). This is localized:
- `get_object`: the `Vec` becomes `ObjectQuery.eq_filters` directly (already `Vec<(String,String)>`).
- `get_linked`: the `Vec` is resolved by `chain_filter::resolve_chain_filters` (already takes a
  `Vec<(String,String)>`).
- `get_linked_chain`: remove the `path` pair(s) from the `Vec` (a `path` key may appear once;
  filter it out), parse `path`, resolve the rest. (Previously `params.remove("path")` on a HashMap;
  now a `Vec` retain/partition.)

No new status codes — every filter parse/coercion/arity failure stays `BadFilter` → 400; the resolver
errors stay 400.

### Error handling (reuse existing variants)

- Bad operand (uncoercible), bad arity, denied/masked/unknown filter column → `QueryError::BadFilter`
  → 400.
- An unrecognized op prefix is NOT an error — it is an `Eq` operand (per §1), which then succeeds or
  fails coercion like any value.
- Resolver errors (unknown link prefix, ambiguous repeated-link filter) → 400, unchanged.

### File structure

- **Modify:** `src/services/query-api/src/filter.rs` (add `CallerPredicate` + `coerce_predicate`;
  retain `coerce_filter` as the per-operand building block).
- **Modify:** `src/services/query-api/src/sql.rs` (`ChainType.predicates`; `compile_select` /
  `compile_chain` take `&[CallerPredicate]`; add `caller_predicate_sql`).
- **Modify:** `src/services/query-api/src/handler.rs` (`read_object` / `read_linked_chain` build
  `CallerPredicate`s via `coerce_predicate`).
- **Modify:** `src/services/query-api/src/http.rs` (multimap extractor on the three GET handlers;
  `path` removal from a `Vec`).
- **Tests:** extend `tests/filter_coerce.rs` (or a new `tests/predicate_coerce.rs`) for
  `coerce_predicate`; extend `tests/sql_compile.rs` for `caller_predicate_sql` shapes; an e2e
  (extend `tests/typed_filter_e2e.rs` and/or `tests/multi_hop_traversal_e2e.rs`) for ranges/set/null
  end-to-end through DuckDB; update existing eq-filter test construction sites for the renamed
  `predicates` field / new `compile_*` parameter type.
- **Docs:** roadmap delivered marker; `docs/FUTURE.md` follow-ups (`or`, `between`, `like`).

## Testing

- **Unit — `coerce_predicate` (pure `rust_test`):** bare→`Eq`; each scalar op (`ne/lt/le/gt/ge`)
  with a typed operand (e.g. `gt:100` on `Double` → `Gt [Double(100.0)]`); `in:`/`nin:` with multiple
  typed operands (e.g. `in:1,2,3` on `Long` → `In [Int(1),Int(2),Int(3)]`); `isnull`/`isnotnull` →
  zero operands; the `eq:` escape (`eq:gt:foo` → `Eq [Text("gt:foo")]`); arity errors (`gt:1,2`,
  empty `in:`, `isnull:x`); a bad operand (`gt:abc` on `Double`) → `Err`.
- **Unit — `caller_predicate_sql` / `compile_*` (pure `rust_test`):** each op renders the right SQL
  fragment and params at the bare table and at a `t_i` alias; a two-predicate range on one column
  ANDs (`(amount >= ?) AND (amount <= ?)`) with params in order; `in` expands to N placeholders; null
  ops emit no param.
- **Governed e2e (`loom_fixture_test`):** through `read_object` / `read_linked_chain` against real
  DuckDB — a `ge`+`le` range on a `Double` column returns exactly the in-range rows; `in:` on a
  `String` column; `isnotnull` on a nullable column; a comparison predicate on an **intermediate**
  chain hop (positioned + operator compose); a predicate on a **denied** column → 400 (governance).
- **Regression:** existing equality-filter e2es stay green (bare value = `Eq`, identical SQL/params).

## Decisions

- Value-prefixed operator grammar (`op:operand`); bare value = `Eq`; `eq:` escape; null ops are the
  bare token; unambiguous recognition (known op token before the first `:`, or a bare null token).
- Full `CompareOp` parity incl. `isnull`/`isnotnull`; operand model `values: Vec<SqlValue>` with
  arity 0/1/N validated per op.
- Reuse `CompareOp` (the enum) and `op_sql`; keep operands on `SqlValue` (not `ScalarValue`/`RowFilter`)
  to preserve `Double`/`Date`/`Timestamp` typing; `coerce_predicate` wraps the retained
  `coerce_filter`.
- Ranges / multiple predicates per column via repeated query keys (ANDed); HTTP extractor →
  duplicate-preserving `Vec<(String,String)>`.
- Errors reuse `BadFilter` → 400; no new `QueryError` variant.

## Follow-ups (later slices)

- **`or`-combined caller predicates** (today all ANDed) — a disjunction grammar.
- **`between:lo,hi`** range sugar (repeated keys already cover it; sugar only).
- **Literal comma inside an `in` operand** — an escaping / alternate-delimiter convention.
- **Text-pattern matching** (`like`/`ilike`/`contains`) — needs a new `CompareOp` variant.
- **Predicates on derived (aggregate) properties** — currently non-filterable; shared with the
  derived-properties "filter/sort targets" follow-up.

## Roadmap

Lands under Step 3 → Query as the operator capstone of the filter arc, directly after typed input
filters and target/intermediate filters. It makes every caller filter — at every read path and every
chain position — express the full `CompareOp` surface, not just equality, completing real analytical
filtering on the governed read path. Builds on `filter::coerce_filter` (typed coercion), the
positioned `predicates` slot (target/intermediate filters), and the existing `CompareOp`/`op_sql`
renderer (ACL row-filters).
