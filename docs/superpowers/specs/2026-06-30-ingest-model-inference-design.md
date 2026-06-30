# Ingest model inference — `POST /models/{type}` slice 2 (infer + create on the absent branch)

- **Date:** 2026-06-30
- **Area:** ingest
- **Register items:** promotes [[fut-ingest-model-inference]] → mints [[road-ingest-model-inference]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

`POST /models/{type}` is a **unified** endpoint: when the named type exists it
conforms-and-lands (slice 1, [[road-ingest-into-model]]); when it does not, it
**infers** an `ObjectType` from the incoming Arrow batch schema, `define_type`s it,
and lands. This spec builds the **type-absent** branch, completing the auto-detect
endpoint. A client can stand up a governed, typed object from raw Arrow in one call.

## Current state

Slice 1 ([`2026-06-29-ingest-into-model-design.md`](2026-06-29-ingest-into-model-design.md),
PR #241) built the pre-existing-type branch of `POST /models/{type}`:

- `service_runtime::Subject` extraction + a deny-by-default `acl.check(subject,
  Write, Type(name))` gate on the ingest plane (no existence leak — unknown type → 403,
  not 404).
- `model_shape_from_type(&ObjectType) -> ModelShape` (the `gate.rs` seam) + the existing
  conformance gate (422 + `violations_json` on mismatch).
- Land into `otype.table` with type-named lineage; success returns `snapshot_id`.

Slice 1 explicitly resolved unknown-type → **403** (no leak). This slice changes that
**only for an authorized subject**: an unknown type is no longer a dead end but the
*trigger* to infer-and-create. The no-leak posture is preserved because the same
`Write`-on-`Type(name)` grant authorizes both branches — a subject without the grant
still gets 403 whether or not the type exists.

Two existing seams make this slice small:

- The landing path already infers Arrow → logical types (the `/datasets` raw-land path
  builds an inferred Iceberg schema from the batch). Slice 2 reuses that mapping to
  build `PropertyDef`s rather than an Iceberg schema.
- `ObjectType` (`control-plane/core/src/ontology.rs:33`) already carries `table:
  TableRef`, ordered `properties: Vec<PropertyDef>`, and `identity: Option<String>` —
  exactly the inference output. Slice 1's seam runs `ObjectType → ModelShape`; this
  slice runs the reverse, `schema → ObjectType`.

## Design — the infer-and-create flow

`POST /models/{type}` with an Arrow IPC body:

1. **Auth + ACL (unchanged from slice 1).** `acl.check(subject, Write,
   Type(type))`. `Deny` → **403**. The grant authorizes both branches; existence is
   never revealed before authorization.
2. **Branch on existence.** `ontology.get_type(type)`:
   - **Exists** → slice-1 conform-and-land, unchanged.
   - **Absent** → infer (this slice).
3. **Infer the `ObjectType`.** A pure helper `infer_object_type(name, &Schema,
   identity: Option<&str>) -> Result<ObjectType, InferError>`:
   - one ordered `PropertyDef` per Arrow field, name = field name, logical type via the
     **existing landing Arrow→logical-type mapping**, `nullable` = the Arrow field's
     nullability;
   - `table` = the conventional `otype.table` for the type name (same target slice 1
     lands into);
   - **identity** = the optional `?identity=<col>` query param: validated that the column
     exists in the batch (else **400/422**) and forced `required` on its `PropertyDef`;
     absent → `identity = None`.
   - An Arrow type with no logical-type mapping → **422** (`violations_json`-shaped:
     names the offending column), nothing lands.
4. **Create-or-conform.** `define_type(inferred)`. If a **concurrent** request already
   created the type (uniqueness race), do not fail: re-resolve via `get_type` and fall
   through to the slice-1 conform path against the now-existing type (so a two-batch race
   resolves to one winner; the loser conforms — and 422s if its batch differs). This is
   the idempotency guard.
5. **Land** into `otype.table` through the existing materializer with type-named
   lineage; success returns the mirror `snapshot_id` (same shape as slice 1).

### Identity selection (decided)

Identity is **caller-declared, not heuristic**: `?identity=<col>`. Rationale — a wrong
identity is hard to undo on a governance platform and silently changes dedup/upsert
semantics later; an explicit opt-in beats a guess. Absent the param the inferred type
has `identity = None`, which is still fully serveable for `GET /objects/{type}` (only
`?_ids` / `?_shape=association` / future upsert need an identity). A later
`define_type` can set an identity if the first ingest omitted it.

### Re-inference (decided)

Inference runs **only on the type-absent branch**. Once the type exists, every later
`POST /models/{type}` hits slice 1's conform-and-land gate (422 on structural
mismatch). "Idempotent re-infer" therefore means *it never re-infers*: a differing
later batch is a conformance failure, not a schema change. Additive schema evolution /
widening stays deferred ([[fut-iceberg-additive-inline]], [[fut-ingest-followups]]).
The only concurrency hazard — two first-batches inferring at once — is closed by the
create-or-conform guard in step 4.

## Defaults (decided, not open)

- **Append-only.** Identity-dedup / upsert is its own deferred area
  ([[fut-cow-identity-change]]). The declared identity sets the *property*; no row-level
  dedup runs.
- **Authoring authz reuses the per-type Write grant** — no new `PolicyTarget`/`Action`
  surface (a coarse ontology-authoring capability is a possible future, not this slice).
- **No new write knobs** — reuses the configured materializer (inherits the
  `LOOM_WRITE_*` posture via the shared write path).

## Scope

In scope (slice 2):

- The type-absent branch of `POST /models/{type}`: `infer_object_type` (the reverse of
  slice 1's `model_shape_from_type` seam), `?identity=` handling, `define_type` with the
  create-or-conform race guard, and land-with-type-named-lineage.
- Reuse of the existing Arrow→logical-type inference (no new type mapping).

Out of scope:

- **Schema evolution / additive widening** on a differing later batch
  ([[fut-iceberg-additive-inline]]).
- Identity **heuristics** (auto-pick); per-value (not structural) constraints; upsert /
  identity dedup ([[fut-cow-identity-change]]).
- A coarse ontology-authoring ACL capability (a new `PolicyTarget`); changes to
  `/datasets` (unchanged) or to the slice-1 conform path (unchanged).

## Testing

A `loom_fixture_test` over-the-wire/in-process ingest e2e (the slice-1 harness;
`rust_test` integration target, never inline `#[cfg(test)]`):

1. **Infer-and-create:** POST a batch to an **absent** type as a Write-granted subject →
   **200** + `snapshot_id`; then read the rows back as **typed objects** through the
   serving/read path, asserting the inferred type's properties (names + logical types) and
   nullability match the batch schema.
2. **`?identity=` honored:** POST with `?identity=<col>` → the inferred type's `identity`
   is that column and its `PropertyDef` is `required`; the value round-trips through
   `?_ids`. A `?identity=` naming a column absent from the batch → **422/400**, nothing
   created.
3. **Unknown type, no grant → 403:** an authorized-for-nothing subject targeting an
   absent type → **403** (no existence leak), distinct from a 404.
4. **Re-infer is conform:** a second batch **conforming** to the just-created type →
   **200**; a second batch **differing** (missing required prop / type mismatch) →
   **422**, nothing lands; the type is unchanged.
5. **Unmappable column → 422:** a batch with an Arrow column that has no logical-type
   mapping → **422** naming the column, nothing created.

## Risk

- The inference helper is the one genuinely new logic; mitigated by reusing the existing
  Arrow→logical-type mapping wholesale (it already drives `/datasets`), so only the
  `PropertyDef` assembly + identity validation is new.
- The create-or-conform race guard is the subtle part; pinned by test 4's concurrent-shape
  reasoning and by leaning on `define_type`'s existing uniqueness behavior (re-resolve +
  fall through rather than surface a 500).
- Behavior-preserving for the type-**present** branch (slice 1 path untouched) and for
  `/datasets` (untouched). The no-leak ACL posture is identical to slice 1; only the
  authorized-absent outcome changes (403 dead-end → create).
