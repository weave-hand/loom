# OR-combined caller predicates — bounded, non-nested disjunction

- **Date:** 2026-07-01
- **Area:** query
- **Register items:** promotes [[fut-or-predicates]] → mints [[road-filter-or-predicates]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

A caller can express **disjunction across columns** — `(amount > 100 OR status = 'vip')` —
not only the all-ANDed predicates available today. The grammar stays flat and non-nested: the
top level remains an AND of conjuncts, where a conjunct is either a single predicate (today) or
a parenthesized **OR-group**. One level of OR, no arbitrary boolean nesting — enough for the
real ergonomic need without a general expression parser.

## Current state

Every caller predicate is collected in `handler.rs:262` and AND-joined in
`select_where_conjuncts` (`sql.rs:427`, `conjuncts.join(" AND ")`, `sql.rs:432`). The request
model `ObjectQuery.eq_filters: Vec<(String, String)>` (`handler.rs:48`) is a **flat pair
list** with no notion of grouping, so there is no way to say "either of these" across
columns. Per-column disjunction over a *set* of values already exists (`in:a,b,c`,
`CompareOp::In`), but cross-column OR does not, and cannot be expressed in the flat list.

## Design

### Grammar — an `_or` group param

Disjunction is carried by a reserved query param `_or` (mirroring the existing reserved `_ids`
handled at `http.rs:142`). Its value is a comma-separated list of **member predicates**, each
in the existing `col:op:rest` predicate form:

```
?_or=amount:gt:100,status:eq:vip     ->  (amount > ? OR status = ?)
```

Each member is parsed by the **existing** `coerce_predicate` (`filter.rs:115`) — so members
support every operator (`eq`/`gt`/`in`/`between`/text-pattern/…) and the same coercion, and
each member column runs the **same visibility/ACL check** a plain predicate does
(`handler.rs:264`). Multiple `_or=` params are allowed; each becomes its **own** OR-group.

### Model + rendering

`ObjectQuery` gains an `or_groups: Vec<Vec<Predicate>>` alongside the flat predicate list (or
the predicate collection generalizes to `enum Conjunct { One(Predicate), Or(Vec<Predicate>) }`
— a plan-time call; the register item's build agent picks the cleaner shape). `http.rs`
extracts each `_or` param into a group; `handler.rs` coerces + visibility-checks every member.
`select_where_conjuncts` (`sql.rs:427`) renders:

- a single predicate → its existing SQL (unchanged);
- an OR-group → `( m1 OR m2 OR … )`, each member rendered by the existing
  `caller_predicate_sql` (`sql.rs:269`) with its operands pushed as **bound params** in order;

and the top level still `join(" AND ")`s all conjuncts. So the final shape is
`plainA AND plainB AND ( or1 OR or2 ) AND ( … )` — the existing AND spine with parenthesized
OR-groups slotted in as first-class conjuncts. Params stay positional and bound; the injection
boundary is untouched.

### Semantics + guards

- An **empty** `_or` (no members) or a single-member group is a `FilterError` (a group of one
  is just a plain predicate — reject to keep intent explicit), a plan-time-confirmable choice;
  the safe default is to require ≥2 members.
- Members are **independent predicates**: each carries its own column, so per-member column
  visibility/denial applies (a denied column inside an `_or` fails the whole request exactly as
  a denied plain predicate does — no leak).
- **Row-filter ACL and identity/`_ids` predicates remain top-level ANDed** — governance
  conjuncts are never OR-weakened. The OR-group only combines **caller** predicates; it cannot
  disjoin away a security predicate.

### Decided (not open)

- **One level of OR, top-level AND** — non-nested; no `(a AND b) OR (c AND d)`, no parenthesis
  grammar. A future slice can generalize if a real need appears.
- **Members reuse `coerce_predicate` + the existing per-member visibility/ACL** — OR adds a
  combinator, not a new predicate type or a governance bypass.
- **Governance/row-filter/`_ids` conjuncts stay ANDed** above any OR-group.
- Params stay **bound**; rendering reuses `caller_predicate_sql`.

## Scope

In scope: the `_or` group param + extraction in `http.rs`; the conjunct model change
(`or_groups` / `Conjunct` enum) in `handler.rs`; per-member coercion + visibility/ACL;
OR-group rendering in `select_where_conjuncts`; ≥2-member validation; e2e + SQL-shape tests.
Applies to the object read path (and link-traversal filters if predicates flow there —
consistency, plan-time scoped).

Out of scope: nested boolean expressions / a general predicate-tree grammar; OR across
row-filter or identity/governance conjuncts (kept ANDed by design); NOT/negation of groups;
OR inside the Flight-export command (unless trivially inherited).

## Testing

`sql_compile.rs` (shape) + `typed_filter_e2e.rs` (`loom_fixture_test`):

1. **Parse:** `_or=amount:gt:100,status:eq:vip` yields a two-member group; each member coerces
   via the existing path; a one-member or empty `_or` is a `FilterError`.
2. **SQL shape:** `sql_compile` asserts `… AND (amount > ? OR status = ?)` with bound params in
   member order, and that plain predicates + the OR-group are all AND-joined.
3. **e2e semantics:** a read with an OR-group returns the **union** of rows matching either
   member, intersected with any plain predicates (AND).
4. **Multiple groups:** two `_or=` params render as two ANDed OR-groups.
5. **Governance not weakened:** a member naming a **denied** column fails the request (no leak);
   an OR-group does **not** disjoin away the row-filter/`_ids` predicates — a subject restricted
   by a row filter still sees only permitted rows even when the OR-group would match more.
6. **Composition:** an OR-group whose members use `between`/`contains`
   ([[road-filter-operators]]) renders correctly (operator reuse).

## Risk

- **The heaviest of the filter-ergonomics batch** — it changes the predicate *collection*
  shape (flat list → conjuncts-with-groups), not just adds an operator. Bounded by keeping OR
  **one-level and top-level-AND**: the render spine and the AND-join are preserved, an OR-group
  is just a new conjunct kind.
- **Governance is the load-bearing invariant** — OR must never disjoin a security predicate.
  The design keeps row-filter/`_ids`/ACL conjuncts strictly ANDed above OR-groups; test (5) is
  the guard and must be treated as a correctness gate, not a nicety.
- **Members reuse the proven `coerce_predicate` + `caller_predicate_sql`**, so per-member
  behavior (coercion, binding, visibility) is inherited; blast radius is the grouping model +
  the OR render arm.
