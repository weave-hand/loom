# `/search` must fail closed when the identity column is governed

- **Date:** 2026-06-29
- **Area:** query
- **Register item:** [[iss-vector-search-identity-mask]]
- **Status:** spec (ready for a work agent to plan + build)

## Problem

The governed `POST /search/{type}/{index_name}` endpoint ([[road-vector-search-endpoint]],
PR #227) returns `{id, distance}` hits from an engine-side kNN, then applies the
subject's **row** policy as a post-filter. The handler (`vector_search`,
`src/services/query-api/src/handler.rs`) does:

```rust
let (row_filters, denied, masked) = load_policy(deps.acl, &subject.0, &target).await?;
if row_filters.is_empty() {
    return Ok(hits);                 // <-- ignores `denied` / `masked`
}
// non-empty path: identity_in_predicate(..) fails on a governed identity column
```

A subject with coarse `Read` but a policy that **denies or masks the identity column
with no row filter** trips the `row_filters.is_empty()` early return and receives the
very identity values `/objects` would have suppressed. The values returned *are* the
identity column (the search response is a list of ids), so a deny/mask on that column
is being silently disregarded.

The two policy shapes are **asymmetric**:

- **No row filter, governed identity** → leaks the ids (the bug).
- **Has a row filter, governed identity** → `identity_in_predicate`
  (`handler.rs:167`) rejects the governed identity column and returns
  `QueryError::BadFilter(identity)`, which fails closed — but as an incidental
  `BadFilter`/500, not a deliberate authorization refusal.

This is narrow and spec-sanctioned (the original `/search` slice scoped column masking
out, since clients re-hydrate properties through governed `/objects`), and it is **not
a row leak** — but it is an inconsistent governance boundary on a client-facing
endpoint. The fix makes both shapes fail closed the same, deliberate way.

## Design

**Fail closed, symmetrically, with a deliberate 403.**

The "is the identity column governed?" predicate already exists, inline, inside
`identity_in_predicate`:

```rust
let allowed = project_allowed(&otype.properties, denied);   // properties minus denied
if !allowed.contains(&identity) || masked.contains(&identity) {
    return Err(QueryError::BadFilter(identity));
}
```

### Part 1 — extract the predicate

Factor that condition into a small pure helper alongside the others in `handler.rs`:

```rust
/// True when the type's declared identity column is denied or masked by policy,
/// so its values must not be revealed. Identity-less types are never governed here.
fn identity_is_governed(
    otype: &ObjectType,
    denied: &HashSet<String>,
    masked: &HashSet<String>,
) -> bool {
    match &otype.identity {
        Some(id) => denied.contains(id) || masked.contains(id),
        None => false,
    }
}
```

`identity_in_predicate` keeps its existing `BadFilter` contract for the *filter*-lowering
call site (an object-set `ids` filter naming a governed identity column is a bad
**filter**, distinct from a `/search` authorization decision), but its column check is
re-expressed in terms of the shared helper so the two sites cannot drift. (Note the
helper uses `denied.contains(id)` directly; `project_allowed` computes the same
membership via `!denied.contains`, so the two agree.)

### Part 2 — guard `/search` before the row-filter branch

In `vector_search`, immediately after `load_policy` and **before** the
`row_filters.is_empty()` check:

```rust
let (row_filters, denied, masked) = load_policy(deps.acl, &subject.0, &target).await?;
if identity_is_governed(&otype, &denied, &masked) {
    return Err(QueryError::Forbidden);   // identity values must not leak via /search
}
if row_filters.is_empty() {
    return Ok(hits);
}
```

This closes the empty-filter leak and, because the guard runs first, makes the
non-empty path return the same deliberate `Forbidden` (403) instead of the incidental
`BadFilter`/500 — the two policy shapes now fail closed identically.

`Forbidden` (not an empty result) is the right response: it mirrors the endpoint's
existing deny-by-default coarse gate (`handler.rs:454`, also `Forbidden`) and tells the
caller the column is governed rather than silently dropping hits.

### Unchanged cases

- **No declared identity** → `identity_is_governed` is `false`; behavior unchanged
  (these never reach the governed post-filter identity logic anyway).
- **Identity ungoverned** (subject may see it) → guard is `false`; the existing
  row-filter post-filter runs exactly as today, including the value-exact path.
- The engine kNN call, the row-filter scoping SQL, and the hit ordering are all
  untouched.

## Scope

In scope: the `identity_is_governed` helper and the one guard in `vector_search`;
re-expressing `identity_in_predicate`'s column check on the shared helper; the e2e test
below.

Out of scope:

- Adding **column masking** to `/search` (returning masked ids) — the slice's original
  decision stands; `/search` returns ids or refuses, and clients hydrate properties
  through governed `/objects`. This spec only makes a governed identity column fail
  closed, it does not introduce per-value masking.
- Any change to `/objects`, the Flight export path, or the engine wire.

## Testing

Extend the vector-search governance e2e (the `/search` ACL suite under
`src/services/query-api/tests/`, e.g. `vector_search_e2e.rs` / its governance case;
reuse the `e2e-support` seed + ACL helpers). Add fixture cases over a type whose index
is seeded and whose identity column is the search id:

1. **Identity denied, no row filter** — subject has coarse `Read` + a policy denying
   the identity column, no row filter → `/search` returns **403** (was: leaked ids).
2. **Identity masked, no row filter** — same with `mask_columns` instead of
   `deny_columns` → **403**.
3. **Identity governed, with a row filter** — the previously-500 path now also returns
   **403** (symmetry assertion).
4. **Ungoverned identity** (regression guard) — subject may read the identity column →
   `/search` returns the kNN hits, value-exact, exactly as today.

All cases are `loom_fixture_test` `rust_test` integration targets (hermetic Postgres),
never inline `#[cfg(test)]`, per loom's testing rules.

## Risk

- Behavior change is confined to subjects whose policy governs the identity column —
  previously a leak (empty-filter) or a 500 (row-filter), now a uniform 403. No
  ungoverned or identity-less path changes.
- The extracted helper is a pure refactor of an existing inline check; the
  `identity_in_predicate` filter contract is preserved, so the object-set read path is
  unaffected.
- Lowest-risk shape: one guard returning the endpoint's own existing `Forbidden`
  variant, no new error kinds, no wire/proto/serving surface.
