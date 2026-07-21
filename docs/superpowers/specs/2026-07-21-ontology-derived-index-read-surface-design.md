# Ontology derived-property & vector-index read surface Design

> **Status:** design (direction). This spec makes `road-ontology-derived-index-read-surface`
> build-ready (promoted from `fut-ontology-derived-index-read-surface`). A separate work agent
> writes the implementation plan from it and builds it.

## Problem

`road-ontology-semantic-descriptions` (shipped, PR #451) gave every declarable ontology entity a
persisted `description`, and the type-detail endpoint surfaces it for **types, properties, and
links** — but `DerivedPropertyDef.description` and `VectorIndexDef.description` have **no reader
anywhere**:

- `GET /ontology/types/{name}` (`query-api/src/http.rs:215-253`) renders `name`, `table`,
  `identity`, `properties[]`, `links[]`, `links_to[]`, `description` — it never reads
  `ty.derived` and never calls a vector-index op.
- Derived properties **are served** as columns on object reads (`handler.rs:325-378` appends
  `DerivedSelect`s to the projection) yet are undeclared in both the type detail and the
  generated per-type OpenAPI read schemas (`openapi_gen.rs:143-159` iterates only
  `ty.properties`) — the generated docs are inaccurate today, independent of descriptions.
- Vector-index definitions are readable **nowhere** over HTTP: the only index-facing route is
  `POST /search/{type}/{index}`, which requires already knowing the index's name. The read ops
  exist end-to-end (`Ontology::vector_indexes_for` — `core/src/ontology.rs:1247` — and query-api's
  `WireControlPlane` at `wire_control_plane.rs:163-172`) but nothing calls them.

## Decision (operator, 2026-07-21): full type-detail + OpenAPI surface

One slice, three legs:

### 1. Type detail: `derived[]`

The handler already holds the data — `get_type` returns `ObjectType` with
`derived: Vec<DerivedPropertyDef>` (`core/src/ontology.rs:100-102`). Render each as
`{ name, ty, link, agg, description? }` (struct fields at `ontology.rs:443-452`; `description`
via the existing `set_description` convention, `http.rs:174-183` — key present only when set).
Extend the static response schema: a `DerivedPropertyView` in `openapi.rs` added to
`TypeDetailResponse` (`openapi.rs:139-154`).

### 2. Type detail: `vector_indexes[]`

Add one `onto.vector_indexes_for(type_name)` call to the handler (available on both the direct
and wire control planes, so the credential-free wire-governed deployment shape is unaffected).
Render each as `{ name, property, metric, spec, description? }` — omit `type_name` (redundant on
the type's own detail); serialize `metric`/`spec` with their existing serde representations
(`VectorIndexDef` fields at `ontology.rs:680-692`). Add a `VectorIndexView` to
`TypeDetailResponse`.

Governance posture: the type detail is already served to any authenticated subject (the schema
catalog is not secret; ACL governs data). Index declarations are the same class of metadata as
property declarations — no new gating. (Per-subject catalog filtering remains
`[[fut-openapi-per-subject-catalog]]`.)

### 3. Per-type OpenAPI read schemas: declare derived properties

`type_component_schema` (`openapi_gen.rs:143-159`) additionally iterates `ty.derived`, emitting
each as a property of the per-type read component: mapped from its declared `ty` string like
physical properties, marked **`readOnly: true`** (they are computed, never writable — and the
write/action schemas must NOT gain them), carrying its `description` when set. This makes the
generated document match what object-read rows actually contain.

Vector indexes do **not** enter the per-type read schemas — they are not row columns. Their
OpenAPI presence is solely via `TypeDetailResponse`.

## Testing

- `query-api/tests/ontology_type_detail.rs`: seed a type with a derived property and a defined
  vector index (with and without descriptions); extend the exact-array oracle (`:167-180`) and
  the description assertions (`:206-215`) to cover `derived[]` and `vector_indexes[]`, including
  key-absence when no description is set, and empty-array shape for types with neither.
- `query-api/tests/openapi_gen.rs`: assert a derived property appears in the per-type read
  component with `readOnly: true` + description, and does **not** appear in write/action
  schemas; assert `TypeDetailResponse` documents the two new arrays.
- The e2e seed helpers (`e2e-support`) already cover derived-property seeding
  (`derived_properties_e2e.rs`); reuse rather than re-copying setup.

## Non-regression

- Response keys are **additive**; existing consumers (UI object explorer, tests) see the same
  keys plus two new arrays. The exact-array oracle tests that pin full response bodies will need
  the new keys added — that is the only intended test churn.
- No control-plane, schema, or wire changes: every read op used already exists on the `Ontology`
  trait and the wire client. No `.sqlx` changes.
- `POST /search` behaviour untouched.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `buck2 run //tools:prek -- run --all-files` before
  every commit.
- e2e tests reuse `//src/services/query-api:e2e-support` helpers per CLAUDE.md.

## Acceptance

1. `GET /ontology/types/{name}` returns `derived[]` and `vector_indexes[]` with descriptions,
   documented in `TypeDetailResponse`.
2. Per-type OpenAPI read schemas declare derived properties (`readOnly: true`, with
   descriptions); write/action schemas do not.
3. `DerivedPropertyDef.description` and `VectorIndexDef.description` are no longer write-only.
4. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `get_ontology_type` (`query-api/src/http.rs:215-253`), `set_description` /
  `link_view_json` (`http.rs:174-197`), `ObjectType.derived` (`core/src/ontology.rs:100-102`),
  `DerivedPropertyDef` (`:443-452`), `VectorIndexDef` (`:680-692`),
  `Ontology::vector_indexes_for` (`:1247`), `WireControlPlane` vector-index ops
  (`query-api/src/wire_control_plane.rs:163-172`), `TypeDetailResponse` / views
  (`query-api/src/openapi.rs:110-154`), `type_component_schema` (`openapi_gen.rs:143-159`),
  tests `ontology_type_detail.rs` / `openapi_gen.rs`.
- Produces: the two response arrays + views, the readOnly derived properties in per-type read
  schemas, the extended tests.
