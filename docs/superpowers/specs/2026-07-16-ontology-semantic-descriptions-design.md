# Ontology semantic descriptions Design

> **Status:** design (direction). This spec makes `fut-ontology-semantic-descriptions` build-ready
> (promoted to ROADMAP as `road-ontology-semantic-descriptions`). **Two PRs, strictly ordered** —
> PR 1 is a pure constructor refactor with no `description` in it; PR 2 is the feature.

## Problem

loom's ontology is a *semantic* model — object types, properties, links, actions — but it carries
no human-readable prose about any of it. A `Customer.email` property declares `ty: "EmailAddress"`
and `required: true`; nothing records *what it means*, who owns it, or why it exists. Foundry's
ontology treats that description as first-class metadata; loom has nowhere to put it.

Nothing in the engine would consume it — it is pure annotation. But it is the substrate for
`fut-autogen-api-specs` (richer generated API docs), a future ontology-browsing UI, and LLM /
semantic search over the schema. Two consumers already exist in-tree and are the reason this is
worth building now rather than later:

- `GET /ontology/types/{name}` (`src/services/query-api/src/http.rs:121`) already serves per-type
  detail — properties, identity, backing table, link adjacency — with no prose anywhere.
- `src/services/query-api/src/openapi_gen.rs` already generates an OpenAPI document *from the
  ontology*, and already sets `description` on schemas it synthesizes (e.g. `openapi_gen.rs:67`,
  `"int64 encoded as a decimal string"`). Every description it emits today is hardcoded, because
  the ontology has none to offer.

## Scope

An optional `description: Option<String>` on **every declarable ontology entity**, carried from the
`define_*` write path through both adapters to the JSON and OpenAPI read surfaces.

Seven structs in `src/control-plane/core/src/ontology.rs`:

| Struct | Postgres table | Read surface after this work |
|---|---|---|
| `ObjectType` | `ontology.object_type` | `/ontology/types/{name}` + OpenAPI |
| `PropertyDef` | `ontology.property` | `/ontology/types/{name}` + OpenAPI |
| `LinkDef` | `ontology.link` | `/ontology/types/{name}` + OpenAPI |
| `ActionDef` | `ontology.action` | OpenAPI (`POST /actions/{name}`) |
| `ParamDef` | `ontology.action_param` | OpenAPI (request-body schema) |
| `DerivedPropertyDef` | `ontology.derived_property` | **none — persist-only** |
| `VectorIndexDef` | `ontology.vector_index_definition` | **none — persist-only** |

`ontology.action_step` gets no column: steps are positional, not named — there is no user-facing
thing to describe.

**Serde on all seven:** `#[serde(default, skip_serializing_if = "Option::is_none")]`.

This is what makes the change free on the wire. engine-wire ships `ObjectType`/`LinkDef`/`ActionDef`
as **serde-JSON strings inside proto `string` fields** (`engine_control.proto` — `payload`,
`columns_json`, …; `engine-wire/src/client.rs:95-101` is the `de`/`se` pair). So:

- **no `.proto` change**, no engine-wire version skew;
- an old payload with no `description` key decodes to `None` (`serde(default)`);
- a `None` re-encodes to **byte-identical** JSON to today's (`skip_serializing_if`).

## Why two PRs

`ObjectType`/`PropertyDef`/`LinkDef` and friends are built with **struct literals in 746 places**.
Adding a field breaks every one. The breakdown is what drives the sequencing:

| Where | Sites |
|---|---|
| `src/**/tests/` (~60 integration-test files) | 611 |
| `testkit/src/lib.rs` (shared fixtures) | 94 |
| Production code (5 files) | 41 |

The production 41 are concentrated where the work belongs anyway: `core/src/ontology.rs` (22 —
builders, serde bridges, doctests, i.e. the file defining the structs), `postgres/src/ontology.rs`
(11 — the row↔struct mapping the new columns land in), `runtime/src/admin.rs` (5),
`ingest/src/model.rs` (2), `testing/seed.rs` (1).

So ~95% of the blast radius is `description: None` noise in tests. Rather than pay that noise now
*and again* for the next ontology field, PR 1 gives every struct the fluent surface `ObjectType`
and `ActionDef` already have (`core/src/ontology.rs:70`, `:694`) and moves the literals onto it.
After PR 1 the field costs approximately nothing at call sites.

## PR 1 — constructors (pure refactor, no `description`)

**No `description` appears in this PR.** It must be behaviour-preserving and independently green.

Extend the constructor surface to mirror the existing `ObjectTypeBuilder` idiom:

```rust
PropertyDef::new("email", "EmailAddress").required().constrained(c)
LinkDef::build("customer", "Order", "Customer", Cardinality::One).fk("customer_id", "id")
LinkDef::build("tags", "Doc", "Tag", Cardinality::Many).join_table(("wh","doc_tag"), "id", "doc_id", "tag_id", "id")
DerivedPropertyDef::new("orderCount", "Long", "orders", Aggregation::Count)
ParamDef::new("email", "EmailAddress").required().binds("email_address")
VectorIndexDef::new("byEmbedding", "Doc", "embedding", Metric::Cosine, spec)
```

plus `ObjectTypeBuilder::add_prop(impl Into<PropertyDef>)` so the terse `.prop("a","B")` /
`.prop_req(..)` shorthands survive alongside the full form. Each builder's `.done()`/`Into` target
is the plain struct — no validation, no I/O, matching `ObjectType::build`'s documented contract
(validation stays with `Ontology::define_*`).

Then migrate the 611 test + 94 testkit literals onto these constructors.

### The risk, stated plainly

This refactor is **not compiler-checked in the direction that matters**. `PropertyDef { required:
true, .. }` migrated to `.prop("x","Y")` — which is `required: false` — compiles clean and silently
inverts a test's meaning. Only tests that assert on requiredness would catch it.

Mitigations, both required:

1. Migrate **file-by-file**, not with one global regex.
2. Treat the PR-1 diff as **read line-by-line**, not skim. ~60 files is a real review burden and
   pretending otherwise is how a weakened test lands.

If the trade stops looking worth it in practice, the fallback is the mechanical `description: None`
pass — dumber, larger, but semantically inert. That decision belongs to whoever reviews PR 1's
first files, not to this spec.

## PR 2 — the feature

### Migration

One migration, `0029_ontology_description.sql`: `add column description text` (nullable, no
default) on the seven tables above. Nullable-add only — no rewrite, no backfill.

Then `tools/sqlx-prepare.sh` and commit the `.sqlx` churn. `//src/control-plane/postgres:sqlx-cache-check`
enforces freshness in the normal `buck2 test //src/...` sweep.

> **Renumber if `0029` is taken by the time this lands.** Concurrent PRs both taking the next
> migration number is a known failure mode here; the tell is every fixture going red at once.

### Constructors

Every builder from PR 1 gains `.described(impl Into<String>)`, and `ObjectTypeBuilder` /
`ActionDefBuilder` gain one for the type/action itself. This is the *only* ergonomic surface added
in PR 2 — the structs' fields stay public, so a literal can still set `description` directly.

### Persistence

`postgres/src/ontology.rs`: `description` into each `define_*` insert, out of each row→struct
mapping. The memory fake stores the structs whole — it round-trips for free.

**Clear-on-redefine.** `define_*` is already replace-not-merge, so redefining a type without a
description **clears** it. That falls out of the existing upsert; do not special-case description
as sticky. The contract test pins this so it is a decision, not an accident.

**No validation, no length cap** — YAGNI. It is `text`, and nothing else in the ontology caps a
string. The one nicety: `.described()` trims and maps empty→`None`, so a blank string cannot
round-trip as `Some("")`.

### Read surface

- **`GET /ontology/types/{name}`** — `description` on the type, on each `PropertyView`, and on each
  `LinkView` (both `links` and `links_to`). **Omitted when absent**, matching the serde skip.
  `TypeDetailResponse` / `PropertyView` / `LinkView` in `src/services/query-api/src/openapi.rs:121-145`
  gain the field.
- **`GET /ontology/types`** stays `{"types": [names]}`. Enriching it to objects is a breaking
  response-shape change this feature does not need.
- **`openapi_gen.rs`** — `type_component_schema` (`:126`) sets the object schema's description;
  the per-property schema (`property_schema`, `:91`) carries the property's; `action_op` (`:319`)
  and `link_op` (`:231`) set the operation description.

### The persist-only gap

`DerivedPropertyDef` and `VectorIndexDef` will persist descriptions **with nowhere to read them**:
neither derived properties nor vector indexes appear on the HTTP or OpenAPI surface today (grep for
`derived` in `http.rs` returns nothing; in `openapi_gen.rs` only a doc comment at `:382`). Exposing
them is its own feature, not this one. PR 2 stops at persistence for those two and logs a FUTURE
item for surfacing them.

## Testing

TDD. `rust_test` integration targets only — never inline `#[cfg(test)]` (buck2 builds but never
runs those). Fixture tests use `loom_fixture_test`, not bare `rust_test`.

- **`testkit::ontology_contract`** (`testkit/src/lib.rs:1215`) — the main event. Descriptions
  survive `define_*` → `get`/`list` for all seven entities; absent stays `None`; redefine-without
  clears. **One contract, both adapters** — memory and postgres both run it.
- **core serde round-trip** — extends `core/tests/governance_serde_roundtrip.rs`; must include that
  a payload with **no `description` key** decodes to `None` (the engine-wire compat guarantee) and
  that a `None` serializes to a payload with no `description` key.
- **core constructors** — PR 1's surface (each builder produces the struct the literal it replaced
  did). PR 2 adds one case: `.described("  ")` normalizes to `None`.
- **query-api e2e** — `/ontology/types/{name}` emits type/property/link descriptions and omits
  absent ones; the `openapi_gen` test asserts they reach the generated document.

## Out of scope

- Surfacing derived properties / vector indexes on reads (the persist-only gap above) — new FUTURE
  item.
- Enriching `GET /ontology/types` to per-type objects (breaking response shape).
- Any engine consumption of descriptions. They are annotation; nothing plans or executes on them.
- Descriptions on non-ontology entities (datasets, lineage, ACL policies).
- i18n / multiple locales per description.

## Register bookkeeping

Promote `fut-ontology-semantic-descriptions` (FUTURE) → `road-ontology-semantic-descriptions`
(ROADMAP, `status:planned`, `spec:2026-07-16-ontology-semantic-descriptions-design`). The entry's
original scope said *types, properties, and links*; this spec widens it to all seven declarable
entities and records the persist-only limit for two of them.

On completion (PR 2), remove the ROADMAP entry per the registers' open-work-only rule and document
the landed capability under `docs/system-capabilities/`.
