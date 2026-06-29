# Ingest directly into a model — `POST /models/{type}` (slice 1: pre-existing type)

- **Date:** 2026-06-29
- **Area:** ingest
- **Register items:** mints [[road-ingest-into-model]] (this slice) + [[fut-ingest-model-inference]] (slice 2)
- **Status:** spec (ready for a work agent to plan + build)

## North star

A client POSTs Arrow data at a **model** and the rows land as governed, typed objects in
one call — no separate land-then-bind round trip. The endpoint is a **unified**
`POST /models/{type}`: when the named type already exists it conforms-and-lands (this
slice); when it does not, it infers an ontology type from the data and creates it
(slice 2, [[fut-ingest-model-inference]]). This spec builds the **pre-existing** branch
and commits inference as the named follow-on.

## Current state

Ingest is two-step today:

1. `POST /datasets/{schema}/{table}` (`ingest/src/http.rs`) lands raw Arrow → inferred
   Iceberg schema → Parquet/inline (+ an *optional* `ModelShape` conformance **gate**,
   `gate.rs` — "this data is this model", structural column presence + type match).
2. **Binding** ([[road-ingest-model-binding]], `bind.rs`) separately validates a *landed*
   table against a *declared* `ObjectType` and `define_type`s it into a serveable typed
   object.

Two facts make this slice small:

- `gate.rs` already stubs the missing seam verbatim: *"a later slice derives a
  `ModelShape` from an `ObjectType`."* That derivation is the core of this slice.
- `ObjectType` (`control-plane/core/src/ontology.rs:33`) already carries everything
  needed: `table: TableRef` (the landing target), ordered `properties: Vec<PropertyDef>`,
  and `identity: Option<String>`. A pre-existing type is already `define_type`'d, so
  "binding" is done — this slice only needs to **conform + land into `otype.table`**.

The ingest service currently has **no ACL/subject plumbing** (it is a trusted bulk
plane). Per the planning decision this slice adds governance, reusing query-api's
machinery (`service_runtime::Subject`, `Acl::check`).

## Design — the conform-and-land flow

`POST /models/{type}` with an Arrow IPC body:

1. **Auth + ACL (deny-by-default, no existence leak).** Extract the
   `service_runtime::Subject` exactly as query-api does. Resolve the coarse gate:
   `acl.check(&subject.0, Action::Write, &PolicyTarget::Type(TypeName(type)))`. `Deny`
   → **403**. A granted-but-nonexistent type is also **403** (mirrors query-api's
   no-leak read gate — existence is not revealed before authorization). The ingest
   `AppState` already holds a `ControlPlane` (it enqueues compaction via `st.cp.queue()`),
   so `st.cp.acl()` / `st.cp.ontology()` are in reach.
2. **Resolve the model.** `ontology.get_type(&type)` → `ObjectType`. The URL names a
   **model**, not a `schema/table`; the landing target is `otype.table`. (`NotFound`
   after the Write grant → still 403, no leak.)
3. **Derive `ModelShape` from the `ObjectType`.** A small pure helper
   `model_shape_from_type(&ObjectType) -> ModelShape`: one `ColumnShape { name, ty,
   required }` per `PropertyDef` (`ty` = the property's logical type; `required` = the
   property is non-nullable or is the declared `identity`). This is the `gate.rs` seam.
4. **Gate the batch** against that shape with the existing conformance gate. Any
   structural violation → **422** with the existing `violations_json` body (reuse, do not
   reinvent the shape).
5. **Land into `otype.table`** through the existing materializer
   (`LandingMaterializer::land`, inline/Parquet per current rules), emitting
   **type-named lineage** (the typed-transform provenance pattern) so the landed rows
   trace to the model. Returns the mirror `snapshot_id` (same success shape as
   `/datasets`).

The pre-existing type is unchanged — no `define_type` runs here; this is a typed write
into an already-bound model.

### Governance boundary

A model ingest is a **typed write**, governed exactly like query-api's governed
typed-insert: authorize **before** landing, deny-by-default, no existence leak. The
engine/materializer stays governance-free — a denied write never lands. This makes the
ingest plane consistent with the serving plane's stance (authorize at the edge, execute
blindly).

## Defaults (decided, not open)

- **Append-only.** Rows append; identity-dedup / upsert is its own deferred area
  ([[fut-cow-identity-change]]). Conformance checks the identity *property* exists and is
  required (via `ModelShape`), but no row-level dedup runs.
- **Type must pre-exist.** Unknown type → 403 (no leak). The infer-and-create branch is
  [[fut-ingest-model-inference]].
- **No new write knobs** — reuses the configured materializer (and inherits
  [[road-transform-write-tuning]]'s `LOOM_WRITE_*` posture via the shared write path).

## Scope

In scope (slice 1):

- `POST /models/:type` route on the ingest router; Arrow IPC decode reused from
  `/datasets`.
- Subject extraction + `Action::Write` ACL gate on the ingest plane (new to ingest;
  reuses `service_runtime::Subject` + `cp.acl()`).
- `model_shape_from_type(&ObjectType)` (the `gate.rs` seam) + wiring the existing gate.
- Land into `otype.table` with type-named lineage; 422-on-violation reusing
  `violations_json`; success returns `snapshot_id`.

Out of scope:

- **Slice 2 ([[fut-ingest-model-inference]]):** inferring an `ObjectType` from the
  batch schema (column→property mapping, identity selection, `define_type`-on-ingest)
  and the unified endpoint's auto-detect (type-absent → infer) branch.
- Upsert / identity dedup ([[fut-cow-identity-change]]); per-value (not structural)
  constraint enforcement; multi-table / typed-graph ingest; changing the existing
  `/datasets` raw-landing endpoint (unchanged).

## Testing

A `loom_fixture_test` over-the-wire/in-process router test (the ingest e2e harness;
`rust_test` integration target, never inline `#[cfg(test)]`):

1. **Conforming land into an existing model:** `define_type` a model over a table; POST a
   conforming Arrow batch to `/models/{type}` as a Write-granted subject → **200** +
   `snapshot_id`; assert the rows are then **readable as typed objects** through the
   serving/read path (the round-trip that proves "landed as the model").
2. **Non-conforming → 422:** POST a batch missing a required property / with a
   type-mismatched column → **422** with a `violations_json` body naming the offending
   column(s); nothing lands.
3. **ACL deny → 403:** a subject without Write on the type → **403**, nothing lands.
4. **Unknown type → 403:** a granted subject targeting a non-existent type → **403** (no
   existence leak), distinct from a 404.

## Risk

- New surface is one route + the ACL gate; both copy established patterns (the
  `/datasets` land path and query-api's Write-ACL typed-write). The conformance gate and
  `violations_json` already exist and are tested.
- Adding ACL to the ingest plane is the one genuinely new posture; mitigated by reusing
  `service_runtime::Subject` + `cp.acl()` wholesale and by the deny/unknown-type tests
  pinning the no-leak behavior.
- Behavior-preserving for the existing `/datasets` endpoint (untouched) and for tables
  without a declared model (unreachable via `/models/{type}`).
