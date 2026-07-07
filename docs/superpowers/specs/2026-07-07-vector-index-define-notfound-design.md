# Vector-index define returns 404 for an unknown type — design

**Item:** `#iss-vector-index-define-status`

## Problem

`POST /admin/models/{type}/vector-indexes` documents **404 for an unknown
type** (`src/services/runtime/src/admin.rs:651`), and the route maps the
adapter error straight through `status_for` (`admin.rs:697`), where
`NotFound → 404` and `Validation → 400` (`src/services/runtime/src/auth.rs:44,46`).

The two control-plane adapters disagree on which error an unknown type yields:

- **Memory** (`src/control-plane/memory/src/ontology.rs:151-156`) probes the
  type map first and returns `ControlPlaneError::NotFound(type)` — the
  documented 404.
- **Postgres** (`src/control-plane/postgres/src/ontology.rs:496-519`) never
  checks type existence. It runs a single `select ty from ontology.property
  where type_name = $1 and name = $2`; for an unknown type that query returns
  `None`, falling into the missing-property arm (`ontology.rs:513-518`) and
  returning `Validation("type `X` has no property `Y`")` → **400 with a
  misleading message** (the type, not the property, is what is missing).

This is pre-existing cross-adapter divergence: the same request answers 404 on
one backend and a wrong-cause 400 on the other, and only the memory answer
matches the OpenAPI contract.

## Scope

Align the postgres adapter with the memory adapter and the documented route:
`define_vector_index` checks type existence first and returns `NotFound` for an
unknown type. Pin the behavior for **both** adapters with a testkit
ontology-contract case.

**Non-goals (explicit):**

- **No change to the memory adapter** — it already returns `NotFound`.
- **No change to the HTTP layer** — `status_for` already maps `NotFound → 404`
  and the route already documents 404; the fix is entirely in the adapter.
- **No broader status-code audit** beyond this one route (other adapter
  divergences, if any, are out of scope unless trivially adjacent).
- **No new drop route or other vector-index surface** — unrelated.

## Design

### Postgres: early type-existence check

`define_vector_index` (`src/control-plane/postgres/src/ontology.rs:496`) gains a
leading probe using the existing shared helper `object_type_exists`
(`ontology.rs:635`) — the single `select exists` type probe already used by
`define_link`, `define_action`, and the ACL target checks:

```rust
if !object_type_exists(&self.pool, &def.type_name.0).await? {
    return Err(ControlPlaneError::NotFound(def.type_name.0.clone()));
}
```

placed before the `select ty from ontology.property …` lookup. With the type
known to exist, the subsequent `None` arm now unambiguously means "type exists
but has no such property" — its `Validation` message is correct again. The
non-vector-property arm is unchanged. No SQL text changes (the helper is a
pre-existing compile-time `query_scalar!`), so **no `.sqlx` regen is required**;
the fixture suite still re-validates the cache.

### Error → HTTP mapping

No HTTP change. `status_for(&ControlPlaneError::NotFound(_)) → 404`
(`auth.rs:44`), so the route now answers the documented 404 for an unknown type
without touching `define_vector_index_route`.

### Testkit contract case (both adapters)

The ontology contract already exercises the vector-index path against both
adapters and covers the non-vector-property and missing-property rejections
(`src/control-plane/testkit/src/lib.rs:1406-1428`) but asserts only `is_err()`.
Extend it to pin the **status kind** for the unknown-type case:

```rust
let unknown_type = VectorIndexDef {
    name: "bad3".into(),
    type_name: tn("Ghost"),        // never defined
    property: "embedding".into(),
    metric: Metric::Cosine,
    spec: IndexSpec::Flat,
};
assert!(
    matches!(
        o.define_vector_index(unknown_type).await,
        Err(ControlPlaneError::NotFound(_))
    ),
    "unknown type is NotFound, not Validation"
);
```

Because the contract body runs against memory and postgres, this case fails on
postgres before the fix and passes on both after, locking the adapters
together.

## Testing

- **Contract** (`testkit` ontology contract): the unknown-type `NotFound` case
  above, run against both memory and postgres — the postgres leg is the
  regression pin.
- **Admin HTTP** (`src/services/runtime/tests/…`, memory adapter idiom): a
  `POST /admin/models/Ghost/vector-indexes` for an undefined type asserts
  **404**, matching the documented response. Memory already returns 404, so
  this guards the documented contract at the HTTP boundary; the adapter fix is
  what makes postgres agree.
