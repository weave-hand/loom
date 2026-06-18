# Object-set inputs + reserved control-param namespace — design (2026-06-18)

> Query-pillar slice. Closes the object-set/graph-query arc from the identity +
> association slice (`2026-06-17-object-identity-association-design.md`): a `?_ids=`
> query parameter scopes a read to a provided set of source objects, addressed by the
> type's first-class identity. Bundled with a reserved-namespace cleanup that makes the
> control-param/column-name collision class impossible by construction.

## Motivation

With object identity now first-class, the natural Foundry idiom is "give me these
objects by primary key" — an object set. `?_ids=1,2,3` expresses it without the caller
knowing the physical identity column name. It lowers to an `In` predicate on the source
identity, reusing the shipped comparison/set-operator machinery.

Adding a fourth control query-param (`ids`) surfaced a latent design smell: control
params (`path`, `direction`, `shape`) share the query-key namespace with bare column
filter keys (`?status=open`), so a type with a property literally named `path`/`shape`/
`direction`/`ids` is silently un-filterable — the reserved key shadows it. `ids` is a
common column name, making this acute. The fix: reserve a `_` prefix for all control
params and forbid `_`-prefixed property names, so collisions become impossible by
construction.

## Scope

Three concerns in one slice:

1. **Reserved control-param namespace** — migrate every control param to a `_` prefix
   (`_path`, `_direction`, `_shape`, plus the new `_ids`).
2. **Ontology guard** — at bind, reject a type whose property or derived-property name
   begins with `_` (reserved for control/system fields).
3. **Object-set input** — `?_ids=` → an `In` predicate on the source identity, across
   plain reads and traversals (including association).

No external consumers exist yet (the external SQL wire is deferred), so renaming the
shipped chain params is safe.

## Concern 1 — reserved control-param namespace

The HTTP layer (`http.rs`) matches control-param keys exactly and pulls them out of the
query string before the remaining pairs become column filters. Migrate the keys:

| Before | After |
|--------|-------|
| `direction` | `_direction` |
| `shape` | `_shape` |
| `path` | `_path` |
| (new) | `_ids` |

Only the matched key strings change (`get_linked`'s `"direction"`/`"shape"`,
`get_linked_chain`'s `"path"`/`"shape"`). The value parsers (`parse_direction`,
`parse_path_hops`) are unchanged — they parse a param's *value*, not its key. Filter
keys (bare column, or `<link>.col`) are unaffected.

Tests that send these query strings over the HTTP router — `association_e2e.rs`
(`?shape=`, `?path=`) — are updated to the prefixed keys. Tests that drive the handler
functions directly (`multi_hop_traversal_e2e.rs`, `inverse_hops_e2e.rs`) construct
`ChainQuery` in Rust and use no query keys, so they are unaffected; `path_parse.rs`
tests the value parsers directly and is likewise unaffected.

## Concern 2 — ontology guard (collision-proof by construction)

At **bind** (`ingest/bind.rs`, the existing validation chokepoint), reject any declared
**property or derived-property** whose name begins with `_`. A new
`BindViolationReason::ReservedName` carries the offending name. Because every control
param is `_`-prefixed and no property may be, a property can never shadow a control
param — current or future. The check runs alongside the existing per-property and
identity validations; nothing persists on rejection.

Out of scope for the guard: **link names**. A link contributes filter keys of the form
`<link>.col` (dot-qualified), which can never equal a bare `_x` control key, so links do
not collide with control params. (`define_type` itself continues to trust its input, as
it already does for property types — bind is the semantic gate; directly-defined test
types simply avoid `_` names, which they already do.)

## Concern 3 — object-set input (`?_ids=`)

### Shared helper

```rust
/// Build the `In` predicate that scopes a read to the given object identities. `ids` are
/// raw query strings, coerced to the identity property's logical type. Returns `None` for
/// an empty `ids` (no scoping). Errors: the type has no declared identity (`NoIdentity`);
/// the identity column is not a permitted filter column — denied or masked (`BadFilter`);
/// or a value does not coerce (`BadFilter`).
fn identity_in_predicate(
    otype: &ObjectType,
    denied: &HashSet<String>,
    masked: &HashSet<String>,
    ids: &[String],
) -> Result<Option<CallerPredicate>, QueryError>;
```

Resolves `otype.identity`; visibility-checks it against `project_allowed(otype, denied)`
and `masked` (the same rule any caller filter obeys — you may filter on a column you are
permitted to filter on); coerces each id via the existing `coerce_filter`; returns
`Some(CallerPredicate { column: identity, op: CompareOp::In, values })`.

### Wiring

- **`ObjectQuery`** and **`ChainQuery`** each gain `ids: Vec<String>` (empty = absent).
- **`read_object`**: after building `predicates`, append the helper's predicate (if any)
  — using the type's own `denied`/`masked`.
- **`resolve_chain`** (shared by `read_linked_chain` *and* `read_associations`): append
  the helper's predicate to the **source** position (`ctypes[0].predicates`), using the
  source's `denied`/`masked` from `metas[0]`. One code path covers plain reads,
  single-hop, multi-hop, and association.

### HTTP

Parse `?_ids=` (comma-split into `Vec<String>`) out of the params on every read route
(`get_object`, `get_linked`, `get_linked_chain`) alongside the other `_`-prefixed
control keys, and set the `ids` field. A present-but-empty `?_ids=` (zero non-empty
values) → 400, mirroring `in:` requiring at least one operand. The error mapping already
covers `NoIdentity`→400 and `BadFilter`→400 (`get_object` gains the `NoIdentity` arm,
which it lacks today).

### Governance

No new governance. `?_ids=` lowers to an ordinary caller `In` predicate, inheriting the
existing filter governance: the source visibility check, per-type row-filters AND'd in,
and (in a traversal) the unchanged N-ends Read guarantee. It only narrows within
already-permitted rows.

## Testing

- **Guard matrix** (`ingest`): a property named `_x` → `ReservedName`; a derived property
  named `_y` → `ReservedName`; all-plain names → accepted.
- **`identity_in_predicate` unit** (`query-api`): ids → `In` predicate on the identity;
  empty → `None`; no declared identity → `NoIdentity`; denied identity → `BadFilter`;
  masked identity → `BadFilter`; uncoercible value → `BadFilter`.
- **e2e** (DuckDB, via the HTTP router):
  - `/objects/Customer?_ids=1,2` returns exactly those two objects.
  - a traversal `…/links/orders?_ids=1` scopes the source to object 1 before traversing.
  - `?_ids=` combined with `?_shape=association` scopes the association's source.
  - `?_ids=` on a type with no declared identity → 400.
  - a present-but-empty `?_ids=` → 400.
- The migrated control params keep working: the updated `association_e2e.rs` (`?_shape=`,
  `?_path=`) stays green.

## Task breakdown

1. **Ontology guard** — `BindViolationReason::ReservedName` + the `_`-prefix check on
   properties and derived properties; bind matrix tests.
2. **Control-param migration** — rename the `_path`/`_direction`/`_shape` key matches in
   `http.rs`; update `association_e2e.rs` query strings; confirm the traversal suite stays
   green.
3. **Object-set input** — `ids` field on `ObjectQuery`/`ChainQuery`; `identity_in_predicate`;
   wire into `read_object` and `resolve_chain`; `_ids` parsing + the `NoIdentity` arm on
   `get_object`; `identity_in_predicate` unit tests + the object-set e2e.
4. **Docs** — mark object-set inputs delivered; record the reserved `_` control-param
   convention.
