# Filter operators — `between`, text-pattern ops, and the `eq_filters`→`filters` rename

- **Date:** 2026-07-01
- **Area:** query
- **Register items:** promotes [[fut-between-sugar]] + [[fut-text-pattern-ops]] + [[fut-rename-eq-filters]] → mints [[road-filter-operators]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

The caller-predicate grammar gains two additive operator families — a `between:lo,hi` range
and case-insensitive text-pattern matching (`contains` / `startswith` / `endswith`) — and the
internal request field is renamed from `eq_filters` to `filters` to match the grammar it now
carries (and the Flight-export struct that already calls it `filters`). All three are small,
additive, and share the same file surface; none changes the always-bound-param injection
boundary.

## Current state

Caller predicates parse in `coerce_predicate` (`query-api/src/filter.rs:115`), which splits a
URI query param on the first `:` into `(op, rest)` and maps the op token to a `CompareOp`
(`control-plane-core/src/acl.rs:60`: `Eq Ne Lt Le Gt Ge In NotIn IsNull IsNotNull`). Tokens
are parsed at `filter.rs:132`. Each predicate renders in `caller_predicate_sql`
(`sql.rs:269`) via `op_sql` (`sql.rs:87`), and **every operand is a bound placeholder**
(`sql.rs:295`/`299`) — never string-interpolated (the injection boundary,
`sql.rs:1`). Predicates are collected in `handler.rs:262` and AND-joined in
`select_where_conjuncts` (`sql.rs:427`, `join(" AND ")`).

The request carries them as `ObjectQuery.eq_filters: Vec<(String, String)>`
(`handler.rs:48`), built generically from the URI query params (`http.rs:140`/`153`). Because
it is a `Vec` of pairs, repeated keys already express a range today
(`?amount=ge:11&amount=le:25`). The Flight-export command struct already names the equivalent
field `filters` (`flight_export.rs:37`).

Gaps: no range **sugar** (a range is two predicates), no **text-pattern** operator at all
(`filter.rs` has no `like`/`ilike`/`contains`), and the field name `eq_filters` is stale — it
carries the full operator grammar, not just equality.

## Design

### `between:lo,hi`

Add `CompareOp::Between`. Parse `between` in `coerce_predicate` (`filter.rs:132`) taking
**exactly two** comma-split operands (reusing `split_set_operands`); a count ≠ 2 is a
`FilterError`. Coerce each operand through the existing per-type coercion (so `between` obeys
the same type rules as `ge`/`le`). Render in `caller_predicate_sql` as `(col BETWEEN ? AND ?)`
with the two operands pushed as bound params in order. Pure sugar — identical results to
`ge:lo` + `le:hi`, one predicate instead of two.

### Text-pattern operators (case-insensitive, escaped)

Add three ops — `contains`, `startswith`, `endswith` — as `CompareOp::Contains` /
`::StartsWith` / `::EndsWith`. They apply to **string properties only** (a define-time/coerce
check against the property's logical type; a non-string target is a `FilterError`). Rendering:
each maps to `col ILIKE ?` (case-insensitive by decision), and the **bound param value** is the
operand with LIKE metacharacters escaped and wrapped:

- escape `\`, `%`, `_` in the operand (prefix with the SQL `ESCAPE` char), then wrap:
  `contains` → `%<esc>%`, `startswith` → `<esc>%`, `endswith` → `%<esc>`;
- render with an explicit `ESCAPE '\'` clause so the escapes are honored.

The operand stays a **bound parameter** (no interpolation); the escaping is a *semantic*
guard so a user searching for a literal `%` matches the character, not "anything". A raw
`like`/`ilike` with caller-supplied wildcards is **out of scope** (it re-introduces
wildcard-injection surprises and a DoS-via-`%%%` surface); these three cover the ergonomic
need safely.

### Rename `eq_filters` → `filters`

Rename the struct field (`handler.rs:48`) and its two read sites (`http.rs:140`/`153`), plus
the e2e tests that name it. This is **internal only**: for `GET /objects/{type}` the wire
carries the actual column/operator query params (e.g. `?amount=gt:5`), not a field literally
named `eq_filters`, so there is no external contract change; the rename simply aligns the
in-process name with the Flight-export struct's existing `filters`.

### Decided (not open)

- **Text-pattern is case-insensitive (`ILIKE`) with escaped, wrapped, bound operands** — no raw
  wildcard operator in this slice.
- **`between` is exactly-two-operand sugar**, same coercion/type rules as `ge`/`le`.
- **Rename is internal** — no GET wire change; aligns with `flight_export.rs`.
- Values remain **bound params** throughout — the injection boundary is untouched.

## Scope

In scope: `CompareOp::{Between, Contains, StartsWith, EndsWith}` + parse tokens + SQL
rendering; two-operand parsing for `between`; string-only + metacharacter-escaping for the
text-pattern ops; the `eq_filters`→`filters` rename across handler/http/e2es. Applies to the
object read path and, where predicates flow through, link-traversal/chain filters and the
Flight-export filter list (consistency).

Out of scope: raw `like`/`ilike` with caller wildcards; regex operators; OR/disjunction
([[fut-or-predicates]] → [[road-filter-or-predicates]]); collation/locale control for
case-insensitivity; numeric/date "between" on non-orderable types (rejected by coercion).

## Testing

`filter_coerce.rs` (pure parse/coerce) + `typed_filter_e2e.rs` / `sql_compile.rs`
(`loom_fixture_test` e2e):

1. **`between` parse+coerce:** `between:11,25` yields a two-operand `Between` predicate;
   wrong-arity (`between:11` / `between:1,2,3`) is a `FilterError`; type-mismatched operand is
   rejected like `ge`.
2. **`between` e2e + SQL shape:** a read with `amount=between:11,25` returns exactly the rows
   `ge:11`+`le:25` returns; `sql_compile` asserts `(amount BETWEEN ? AND ?)` with two bound
   params.
3. **Text-pattern e2e:** `name=contains:AC` matches case-insensitively; `startswith`/`endswith`
   anchor correctly; a literal `%` in the operand matches the character (escaping works).
4. **Text-pattern type guard:** a `contains` on a non-string property is a `FilterError`.
5. **Injection/escape:** `sql_compile` confirms the operand is a bound param with an `ESCAPE`
   clause and metacharacters escaped — no interpolation.
6. **Rename:** all filter e2es pass against the renamed `filters` field; the export path's
   `filters` is unaffected.

## Risk

- **Additive and bounded** — new `CompareOp` variants + parse tokens + render arms; the AND
  collection, ACL/visibility gating, and coercion are unchanged and run on the new ops exactly
  as on existing ones.
- **The one real safety point is LIKE-metacharacter escaping** for text-pattern ops; pinned by
  the literal-`%` test (3) and the escape assertion (5). Bound params keep injection off the
  table regardless.
- **The rename is mechanical**; the only footgun is a missed reference — the e2e sweep (6) and
  a grep for the literal are the guard. Zero external wire impact.
