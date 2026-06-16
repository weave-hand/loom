# Design: caller-supplied target / intermediate filters (slice C part-2, query — read path)

> **Status:** approved design (2026-06-16). Extends governed multi-hop traversal
> (`2026-06-15-query-multi-hop-traversal-design.md`) so a caller can filter **any type** in
> the chain by value, not just the source. Today a chain's caller equality-filters bind only to
> the source (`t_0`); intermediate and final-target narrowing is policy-only (ACL row-filters),
> so a caller cannot say "final `LineItem.sku = ABC`" — only the policy can. This slice adds
> typed, governed per-hop equality filters addressed by the link that reaches each type. It also
> draws an explicit boundary between **relational** traversal (`/links`, this endpoint) and a
> deferred **graph** traversal concept (`/graph`).

## Goal

Let a caller narrow the final-target set of a traversal by equality on intermediate and
final-target types, addressed by the link that reaches the type:
`GET /objects/Customer/links?path=placed,contains&active=true&placed.status=open&contains.sku=ABC`
filters the source `Customer` (bare `active`), the intermediate `Order` (`placed.status`), and the
final `LineItem` (`contains.sku`). Each per-hop filter is coerced to **its own type's** declared
ontology logical type (reusing `filter::coerce_filter` from the typed-input-filters slice) and is
visibility-checked against **its own type's** governed projection. The N-ends `Read` governance of
the chain is unchanged — caller filters only **narrow within already-permitted visibility**, never
widen access.

## Scope

**In scope:**
- Per-hop caller equality filters on **any** type in the chain (source, every intermediate, final
  target), addressed by a `<linkname>.<column>` key convention; bare keys remain source filters.
- Per-type typed coercion (against that type's `PropertyDef.ty`) and per-type visibility checking
  (allowed projection, non-masked) — the same visibility-then-coerce discipline already applied to
  source filters.
- The single-hop route (`GET /objects/{from}/links/{link}`) inherits target filtering for free as
  the `N=1` chain (`?<link>.col=v` filters the single hop's target).
- The **relational/graph boundary**: a per-hop *filter* whose prefix names a link that repeats in
  the path is rejected (`400`), pointing at the deferred `/graph` capability. Plain traversal over a
  repeated link (source-only filters) still works — no regression.

**NOT in scope (later slices):**
- **Graph traversal as a first-class concept** — a `/graph` (and possibly `/tree`) surface for
  cyclic / self-link / variable-length / recursive paths, with graph-aware filter addressing
  (positional or per-occurrence). The relational `/links` chain deliberately scopes these out; see
  Follow-ups. This slice does **not** design that surface.
- **Comparison / set operators** (`>`, `<`, `in`, ranges) — equality-only, exactly as the source
  filters are today. Inherits the typed-input-filters comparison-operators follow-up.
- **Inverse-direction hops, object-set inputs, source→target association** — the remaining slice-C
  parts, unchanged by this slice.
- **Define-time chain/link validation** — the chain is still resolved at read time; a broken chain
  (unknown link, a link whose `from` is not the current type) still surfaces then as a `400`.

## Design

### 1. Surface & semantics (`http.rs`)

Endpoint unchanged: `GET /objects/{from}/links?path=l1,l2,…`. The query-param filter keys gain a
prefix convention, resolved in the HTTP layer (which holds the raw `params: HashMap<String,String>`)
into `(position, column, raw_value)` triples the handler can govern:

- **Bare `col`** (no `.`) → a source filter (chain position `0`). Backward-compatible with the
  existing single-hop and chain source-filter behavior.
- **`<linkname>.col`** → a filter on the type reached *via* `<linkname>`, i.e. the chain position
  that link occupies in `path`. The prefix is the substring before the **first** `.`; the remainder
  is the column.

A key is interpreted as **prefixed iff it contains a `.`**. If it contains a `.` but the prefix
names no link in `path`, it is an **unknown filter target** → `400`. Bare keys are always source.
(Ontology column names with literal dots are not supported; a dotted key is always read as
`prefix.column`. Known limitation, consistent with how identifiers are already treated by
`quote_ident`.)

**Graph boundary.** Before resolving prefixes to positions, the handler maps each link name in
`path` to its occurrence count. A *prefixed filter* whose link name occurs **more than once** is
rejected → `400` (`BadChain`) with a message pointing at the deferred `/graph` capability. Plain
traversal over a repeated link (`?path=knows,knows` with only bare/source filters) is unaffected —
the chain still traverses; only an *ambiguous per-hop filter* is refused.

**Single-hop route.** `GET /objects/{from}/links/{link}` continues to forward into the chain handler
as `path=[link]`. With the prefix convention, `?<link>.col=v` addresses position `1` (the hop's
target), so single-hop target filtering falls out with no new code path; bare keys remain source.

### 2. Filter representation (handler input)

`ChainQuery`'s filter field changes from source-only to position-addressed. Today:

```rust
pub struct ChainQuery {
    pub from_type: String,
    pub path: Vec<String>,
    pub source_filters: Vec<(String, String)>, // raw, bound to t_0
}
```

becomes a flat list of `(position, column, raw)` triples (position `0` = source):

```rust
pub struct ChainQuery {
    pub from_type: String,
    pub path: Vec<String>,
    pub filters: Vec<ChainFilter>, // each: { position: usize, column: String, raw: String }
}
```

The HTTP layer resolves each key's prefix to a position using `path` (bare → `0`,
`<linkname>` → its index+1, unknown prefix → `400`, repeated-link prefix → `400`) and passes the
typed-by-position triples in. `LinkQuery` (single-hop) likewise forwards its parsed filters; bare
keys → position `0`, `<link>.col` → position `1`.

### 3. Governance & coercion (per type) (`handler.rs`)

The chain-resolution loop in `read_linked_chain` already resolves every type and loads every type's
policy per hop (`load_policy` → row-filters + denied + masked) but currently retains only the final
target's `denied`/`masked`. Extend it to retain, per position, the data needed to govern a filter
there: the type's `properties` (for the logical type), its `denied` set, and its `masked` set (the
allowed projection is `project_allowed(properties, denied)`).

Then, for each `(position, column, raw)` caller filter:

1. **Visibility first** (no type-info leak): the column must be in that position's allowed
   projection and not masked — else `400` (`BadFilter`). A denied/masked column is rejected before
   coercion, so the filter never reveals a column the subject may not see.
2. **Then coerce**: `crate::filter::coerce_filter(column, ty, raw)` where `ty` is *that position's*
   `PropertyDef.ty`. On `Err` → `400` (`BadFilter`).
3. Attach the resulting `(column, SqlValue)` to that position's `ChainType.eq_filters` (below).

The per-hop `Read` gate (N-ends governance) is unchanged: a filter on an intermediate type is only
reachable if the subject already holds `Read` on that type (else the whole request is `Forbidden`
before filters are considered) and the column is visible there. Caller filters strictly narrow.

### 4. Compiler change (`sql.rs::compile_chain`)

`ChainType` gains caller equality filters alongside its policy row-filters:

```rust
pub struct ChainType {
    pub table: TableRef,
    pub row_filters: Vec<RowFilter>,            // ACL policy (existing)
    pub eq_filters: Vec<(String, SqlValue)>,    // caller filters at this position (new)
}
```

The standalone `source_eq_filters` parameter of `compile_chain` is **removed** and folded into
`types[0].eq_filters` — one mechanism for every position, no source special-case. The WHERE-building
loop already iterates `types` and ANDs each type's `row_filters` at alias `t_i`; it now also ANDs
that type's `eq_filters` (`t_i."col" = ?`) at the same alias. Params are pushed in conjunct-emission
order, so positional `?` alignment holds automatically (as it does today). The projection
(`SELECT DISTINCT` over the final target's visible columns), the JOIN construction, and the `LIMIT`
are unchanged.

### 5. Error handling (reuse existing variants, no widening)

All failures map to the existing `400` path; **no new error variant**:
- Unknown link prefix, denied/masked/absent filter column, coercion failure → `QueryError::BadFilter`.
- Per-hop filter on a repeated link → `QueryError::BadChain` (graph-pointing message).
- The chain-shape and per-hop `Read` errors (`BadChain` for depth/empty, `Forbidden`,
  `UnknownLink`, `UnknownType`) are unchanged.

### File structure

- **Modify:** `src/services/query-api/src/sql.rs` (`ChainType.eq_filters`; `compile_chain` ANDs
  per-position eq-filters at `t_i`; drop `source_eq_filters` param).
- **Modify:** `src/services/query-api/src/handler.rs` (`ChainQuery`/`LinkQuery` filter shape →
  position-addressed; retain per-position properties/denied/masked in the chain loop; per-position
  visibility-then-coerce; build each `ChainType.eq_filters`).
- **Modify:** `src/services/query-api/src/http.rs` (resolve key prefixes → positions against
  `path`; bare → source; unknown prefix → `400`; repeated-link prefix → `400` graph-pointing
  message; forward position-addressed filters; single-hop maps `<link>.col` → position 1).
- **Tests:** extend `tests/sql_compile.rs` (chain eq-filters on intermediate + final target, param
  order); a new or extended `tests/typed_filter_e2e.rs` / chain e2e covering target + intermediate +
  source filters, typed coercion on a non-text target column, and the `400` cases (repeated-link
  filter, unknown prefix, denied target column). Update existing chain/single-hop test sites that
  construct `source_filters` to the new position-addressed shape.
- **BUCK:** no new target expected (extends existing `rust_test`s); add one only if a new e2e file
  is introduced.
- **Docs:** roadmap delivered marker; `docs/FUTURE.md` (the new graph-traversal defer item; note
  comparison operators still pending for these filters too).

## Testing

- **Unit (`tests/sql_compile.rs`, pure `rust_test`):** `compile_chain` with `eq_filters` on an
  intermediate `ChainType` and on the final-target `ChainType` — assert each predicate binds at the
  correct `t_i` alias and the param vector is in conjunct order; a two-hop chain carrying source +
  intermediate + target eq-filters together; the no-filter chain output is unchanged.
- **Governed e2e (`loom_fixture_test`):** seed `Customer → Order → LineItem` (FK or join-table
  backings as the existing multi-hop e2e does), then through `read_linked_chain`:
  - filter the **final target** (`contains.sku=…`) and assert only matching final targets return;
  - filter an **intermediate** (`placed.status=…`) and assert it narrows the final set;
  - combine a **source** filter with hop filters;
  - prove **typed** coercion on a non-text target column (e.g. `Order.amount` `Double`,
    `Order.active` `Boolean`) — a value that a `Text` bind would never match;
  - assert `400` for a **repeated-link** per-hop filter (`?path=knows,knows&knows.x=…`), an
    **unknown prefix**, and a **denied target column** (governance: the column is denied by policy
    on the target type).
- **Regression:** the existing source-only chain and single-hop e2es stay green — bare keys still
  mean source, and a chain with no hop filters compiles identically.

## Decisions

- Filterable scope: **any** type in the chain (source, intermediates, final target).
- Addressing: **link-name-prefixed** keys (`<linkname>.<column>`); bare keys are source. The
  relational `/links` chain has distinct link names per path by construction, so link-name
  addressing is unambiguous within this (relational) domain.
- Repeated-link handling: traversal still works; only a **per-hop filter** on a repeated link is
  rejected (`400`, `BadChain`), framed as the relational/graph boundary — not a parser limitation.
- Per-type visibility-then-coerce, reusing `filter::coerce_filter`; uncoercible / denied / unknown
  → `400` (`BadFilter`), reusing existing variants (no widening).
- `compile_chain`: per-position `eq_filters` on `ChainType`, source folded in (no special case).

## Follow-ups (later slices)

- **Graph traversal as a first-class concept.** A `/graph` surface (a tree is a special case; a
  `/tree` surface can split out later if it earns its keep — not committed now) for cyclic /
  self-link / variable-length / recursive paths, with graph-aware filter addressing (positional or
  per-occurrence) that resolves the repeated-link case this slice rejects. The relational `/links`
  chain deliberately scopes these out: its execution is a fixed set of INNER JOINs over relational
  DuckLake tables, not a graph engine.
- **Comparison / set operators** on per-hop filters — equality-only here, inheriting the
  typed-input-filters comparison-operators follow-up (a shared richer filter grammar would cover
  source and per-hop filters at once).
- **Inverse-direction hops, object-set inputs, source→target association** — the remaining slice-C
  parts, unchanged.
- **Define-time chain/link validation** — shared with slice A's deferred `define_link` column
  validation.

## Roadmap

Lands under Step 3 → Query as **slice C part-2**, directly after multi-hop traversal (part-1) and
typed input filters. It completes "typed filters everywhere" — every type a traversal touches is now
caller-filterable, not just the source — and draws the relational-vs-graph boundary that scopes the
future `/graph` work. Builds on the resolvable-link + governed-join primitive (slice A), the chain
compiler (part-1), and `filter::coerce_filter` (typed input filters).
