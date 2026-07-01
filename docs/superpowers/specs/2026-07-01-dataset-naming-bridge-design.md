# Design: TableRef/TypeName → DatasetRef naming bridge (deployment-aware)

> **Status:** approved design (2026-07-01) for `road-dataset-naming-bridge` (area:
> lineage). Promotes `fut-dataset-naming-bridge`. This is the **shared prerequisite**
> that unblocks least-disclosure lineage governance (`road-lineage-acl-filtering`) and
> lineage `DatasetRef` validation (`fut-lineage-datasetref-validation`): both need to
> resolve a `DatasetRef` back to the ACL'd object it names. Builds on the qualified
> dataset-identity slice (`2026-06-12-qualified-dataset-identity-design.md`), which
> introduced `core`'s `DatasetId`/`TypeId` and the logical `"loom"`/`"loom:type"`
> namespaces — this slice adds the **deployment-aware, reversible** layer on top,
> without breaking any of it.

## Problem

`core` already bridges a `TableRef`/`TypeName` to an OpenLineage `DatasetRef` for
loom's *own* governed datasets, via `DatasetId::dataset_ref()` /
`TypeId::dataset_ref()`, using two deployment-independent **logical** namespaces
(`LOOM_DATASET_NAMESPACE = "loom"`, `LOOM_TYPE_NAMESPACE = "loom:type"`) and a
dotted `schema.name` local name. That slice deliberately reversed only the part of
the naming that was never deployment-dependent, and explicitly left the rest in the
services layer (`core/src/lineage.rs` doc: *"that mapping … depends on deployment
context … belongs to the consuming services, not to `core`"*).

Two now-planned capabilities need more than the logical mapping:

1. **`road-lineage-acl-filtering`** — least-disclosure filtering of lineage reads
   must, for each `DatasetRef` node in an upstream/downstream/event result, resolve it
   back to the loom object it names (a `TableRef` or `TypeName`), look up that object's
   ACL policy, and omit nodes the subject cannot read. This is the **reverse**
   direction, and it is the load-bearing reason this item is a prerequisite.

2. **`fut-lineage-datasetref-validation`** — validating the `DatasetRef`s a
   `LineageEvent` references on `emit` needs an **internal-vs-external** convention
   first: a `DatasetRef` may legitimately name an *external* dataset (a source S3 path,
   a Kafka topic, another system's table), so a naive resolver that rejects anything it
   can't map back to a loom table would reject legitimate external lineage.

Neither is satisfiable by `core`'s current mapping because:

- The logical `"loom"` namespace is **not OpenLineage-conformant** for external
  interop. The OpenLineage naming spec wants a *datasource-derived* namespace (`s3://bucket`,
  `file://…`). A cross-tool consumer, and a second loom deployment reading the same
  bucket, both need the physical storage location encoded — which is **deployment
  context** (`ObjectStoreConfig.warehouse_uri`) that `core` deliberately does not hold.
- The internal-vs-external boundary is itself deployment-dependent: "is this
  `DatasetRef` one of *my* governed datasets or something external?" can only be
  answered against *this* deployment's warehouse location.

So the reverse resolver, and the physical-storage-aware forward mapping, cannot be
`core` constants. They belong to the Step-3 services layer, parameterized by the
already-parsed deployment config.

## Approach

Add a small, **postgres-free, deployment-parameterized** naming bridge in the services
layer that:

1. is constructed from the existing `store_config::ObjectStoreConfig` (the parsed
   `warehouse_uri` + backend — loom's single source of deployment storage truth);
2. **forward**: maps a loom `TableRef`/`TypeName` to an OpenLineage-conformant,
   storage-derived `DatasetRef` (namespace localized to the warehouse datasource; name
   the deployment-independent `schema.table`);
3. **reverse** (the prerequisite): resolves *any* `DatasetRef` — logical-loom,
   storage-derived, or external — back to a `ResolvedDataset { Table | Type | External }`,
   as a **total, never-erroring** function, so external lineage is a first-class,
   representable outcome rather than a rejection.

The design is **purely additive**. `core`'s `DatasetId`/`TypeId`, the logical
namespaces, and the `From<&TableRef> for DatasetRef` convenience impls are **kept
unchanged** — they remain loom's stable *internal* identity and, crucially, the
name-parsing primitive the bridge *reuses* (`DatasetId::from_dataset_ref`,
`TypeId::from_dataset_ref`). Control-plane emitters (e.g.
`postgres/src/iceberg_flush.rs`, which calls `DatasetId::from(table).dataset_ref()`
with no deployment context) are untouched: they keep emitting the logical form, which
the bridge's reverse resolver still recognizes.

### Why the logical form survives (and the reverse resolver accepts both)

The graph will legitimately contain **two** loom-namespace conventions:

- **logical** (`"loom"` / `"loom:type"`), emitted by deployment-context-free
  control-plane producers — stable across deployments, the internal identity;
- **storage-derived** (`s3://bucket` / `file://…`), the OpenLineage-conformant form
  emitted at loom's external boundary and expected from external tools.

Both name the same loom object. The reverse resolver recognizes **both** as governed
(delegating name-parsing to `core` for the logical case, stripping the warehouse
prefix for the storage case) and maps them to the same `ResolvedDataset::Table`/`Type`.
This is what preserves back-compat (existing logical emits still resolve to their
object) while adding storage-derived conformance.

## Components / interfaces

### New crate `//src/services/lineage-naming` (`lineage_naming`)

Postgres-free; deps `//src/control-plane/core` (for `DatasetRef`/`TableRef`/`TypeName`/
`DatasetId`/`TypeId`) and `//src/services/store-config` (for `ObjectStoreConfig`). A
**plain value struct, not a trait** — there is exactly one implementation, no
polymorphism to abstract, and it is pure logic over config (deployment config in, pure
functions out). A trait would be premature abstraction; add one only if a second
naming scheme ever appears.

```rust
/// Deployment-aware bridge between loom's typed identities (TableRef/TypeName) and
/// OpenLineage DatasetRefs, and back. Built from the deployment's ObjectStoreConfig
/// so the physical storage location is encoded in the OpenLineage namespace.
pub struct LineageNaming {
    /// This deployment's storage-derived OpenLineage namespace, from `warehouse_uri`:
    /// the datasource authority only — `s3://<bucket>` or `file://<root>` — never the
    /// key prefix, so the namespace is stable per warehouse and the `schema.table`
    /// name stays deployment-independent.
    site_namespace: String,
}

/// The reverse-resolution outcome. Total: External is a first-class result, never an
/// error — a DatasetRef may legitimately name a dataset loom does not govern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedDataset {
    /// Names a physical table this deployment governs.
    Table(TableRef),
    /// Names an ontology type this deployment governs.
    Type(TypeName),
    /// Names a dataset outside this deployment's governance — an external datasource,
    /// or a different loom site's warehouse. Carries the raw ref verbatim.
    External(DatasetRef),
}

impl LineageNaming {
    /// Derive the bridge from parsed deployment config.
    pub fn from_object_store(cfg: &ObjectStoreConfig) -> Self;

    // ---- forward: loom identity -> OpenLineage-conformant DatasetRef ----

    /// Storage-derived ref for a governed table:
    /// `{ namespace: site_namespace, name: "<schema>.<table>" }`.
    pub fn dataset_ref(&self, table: &TableRef) -> DatasetRef;

    /// Storage-derived ref for a governed ontology type. Types have no physical
    /// storage of their own, so this stays on the logical type namespace
    /// (`"loom:type"`) — see Open questions.
    pub fn type_ref(&self, ty: &TypeName) -> DatasetRef;

    // ---- reverse: DatasetRef -> the loom object it names (THE prerequisite) ----

    /// Resolve any DatasetRef back to the governed object it names, or External.
    /// Total; never errors. Recognizes, in order:
    ///   1. the logical loom namespaces (via core `DatasetId`/`TypeId::from_dataset_ref`);
    ///   2. this deployment's `site_namespace` with a well-formed `schema.table` name;
    ///   3. everything else -> External(raw ref).
    pub fn resolve(&self, dr: &DatasetRef) -> ResolvedDataset;
}
```

### Namespace convention (the internal-vs-external rule)

A `DatasetRef` is **governed by this deployment** iff its namespace is one of:

- `LOOM_DATASET_NAMESPACE` (`"loom"`) — logical table identity;
- `LOOM_TYPE_NAMESPACE` (`"loom:type"`) — logical type identity;
- `self.site_namespace` — this deployment's storage datasource (`s3://<bucket>` or
  `file://<root>`), derived from `warehouse_uri`,

**and** its name parses as a well-formed `schema.table` (for the table cases) or a
non-empty identifier (for the type case). Anything else — a different bucket, a
`postgres://host`, a Kafka topic, a malformed name under a loom namespace — is
`External`. The resolver therefore **never rejects** legitimate external lineage; it
classifies it.

`site_namespace` is the datasource **authority**, not the full warehouse URI:
`s3://my-bucket/warehouse/prefix` → `s3://my-bucket`; `file:///var/lib/loom/warehouse`
→ `file:///var/lib/loom/warehouse` (local has no bucket authority, so the root path is
the datasource). This keeps the namespace collision-free across buckets/deployments
while the `schema.table` name remains stable and prefix-independent.

### Reverse-resolution consumers (downstream, not this slice)

`road-lineage-acl-filtering` (in query-api) constructs a `LineageNaming` from the
service's `ObjectStoreConfig`, then per lineage node:

- `ResolvedDataset::Table(t)` → ACL `PolicyTarget::Table(t)` → existing decision path;
- `ResolvedDataset::Type(ty)` → ACL `PolicyTarget::Type(ty)` → existing decision path;
- `ResolvedDataset::External(_)` → not governed by loom's ACL; the filtering policy
  decides whether to show it (default: show — loom does not govern external data, and
  provenance that something external fed in is itself low-sensitivity; see Open
  questions). This spec defines the resolution seam; the filtering policy is that
  slice's decision.

## Data flow

Forward (emit / external boundary):

```
TableRef{schema,name}  --LineageNaming.dataset_ref-->  DatasetRef{ s3://bucket, "schema.table" }
TypeName("Customer")   --LineageNaming.type_ref----->  DatasetRef{ loom:type, "Customer" }
```

Reverse (ACL filtering / validation):

```
DatasetRef{ "loom",       "main.orders" }  --resolve-->  Table(TableRef{main,orders})   # logical, back-compat
DatasetRef{ "s3://bucket", "main.orders" } --resolve-->  Table(TableRef{main,orders})   # storage, this warehouse
DatasetRef{ "loom:type",  "Customer" }     --resolve-->  Type(TypeName("Customer"))
DatasetRef{ "s3://other",  "raw/events" }  --resolve-->  External(..)                    # different datasource
DatasetRef{ "kafka://…",   "topic" }       --resolve-->  External(..)                    # external source
```

## Error handling

- `resolve` is **total** — no `Result`, no panic. Non-governed / unparseable-under-a-
  loom-namespace inputs become `External`, never errors. This is the whole point: a
  resolver that could error would reintroduce the "reject legitimate external lineage"
  hazard the item calls out.
- `from_object_store` is infallible given an already-parsed `ObjectStoreConfig` (parse
  errors were already surfaced by `ObjectStoreConfig::parse`). Deriving `site_namespace`
  from a well-formed `warehouse_uri` cannot fail; a defensive fallback (use the raw
  `warehouse_uri` if authority extraction is somehow empty) keeps it total.
- The forward methods are infallible constructors (mirroring `DatasetId::dataset_ref`).
- Name-parsing reuses `core`'s existing `from_dataset_ref` guards (empty side, embedded
  `.`), so malformed loom-namespaced names degrade to `External` rather than mis-parse.

## Testing

The bridge is a **services** crate, so it gets its own `rust_test` integration targets
(`lineage-naming/tests/naming.rs`), per the repo's no-inline-tests rule — pure logic,
runs on RE, no fixture. No control-plane contract/testkit change is required because
this slice adds **no `core` trait surface** and no adapter behavior: it reuses the
already-contract-tested `DatasetId`/`TypeId::from_dataset_ref` round-trip. (If a later
slice lifts `ResolvedDataset` or the storage-parse into `core`, *that* addition would
get a `core` `rust_test` round-trip matrix — noted, not done here.)

Test matrix (`rust_test`, parameterized over a `file://` and an `s3://bucket` warehouse
config built with `ObjectStoreConfig::for_s3_test` / a local config):

- **Forward, s3 warehouse:** `dataset_ref(main.orders)` ==
  `{ namespace: "s3://bucket", name: "main.orders" }`; `type_ref(Customer)` ==
  `{ namespace: "loom:type", name: "Customer" }`.
- **Forward, file warehouse:** namespace is the `file://` root; name unchanged.
- **Namespace derivation:** `s3://bucket/warehouse/prefix` → `s3://bucket` (authority
  only, prefix dropped); a non-default schema (`analytics.report`) round-trips.
- **Reverse, back-compat:** the logical `{ "loom", "main.orders" }` and
  `{ "loom:type", "Customer" }` resolve to `Table`/`Type` **regardless** of this
  deployment's warehouse (control-plane emitters have no storage context).
- **Reverse, storage-derived:** `{ "s3://bucket", "main.orders" }` resolves to
  `Table(main.orders)` when `site_namespace == "s3://bucket"`.
- **Reverse, external not rejected:** `{ "s3://other-bucket", … }`,
  `{ "postgres://h", … }`, `{ "kafka://…", … }` → `External(raw)` (raw ref preserved
  verbatim).
- **Reverse, malformed-under-loom:** `{ "loom", "nodot" }`, `{ "loom", ".x" }`,
  `{ "s3://bucket", "a.b.c" }` → `External` (degrade, never panic).
- **Round-trip:** `resolve(dataset_ref(&t))` == `Table(t)` for the configured
  deployment; `resolve(type_ref(&ty))` == `Type(ty)`.

## Non-goals

- **The ACL-filtering logic itself** (`road-lineage-acl-filtering`) — this slice only
  provides the reverse-resolution seam it consumes; which nodes to omit, and the
  external-node policy, are that slice's decisions.
- **`emit`-time `DatasetRef` validation** (`fut-lineage-datasetref-validation`) — this
  slice provides the internal-vs-external classifier that validation needs, but adds no
  `emit` check (lineage stays store-don't-validate this slice).
- **Migrating the persisted `DatasetRef` shape or the control-plane emit path.**
  Control-plane emitters keep emitting the logical form; no deployment context is
  threaded into `core`/`postgres`. Any move to storage-derived *emit* is a later,
  independent decision (see Open questions).
- **Deprecating `core`'s logical namespaces or `From` impls** — they are retained as
  the internal identity and the reused name-parse primitive.
- **Multi-datasource / non-warehouse external forward mapping** (mapping an arbitrary
  external source system to its OpenLineage namespace on ingest) — the bridge maps
  *loom-governed* datasets forward; external datasets enter the graph as raw
  `DatasetRef`s their emitter constructs.

## Open questions

1. **Forward emit form — logical vs storage-derived.** This slice keeps loom emitting
   the **logical** `"loom"` form internally (zero control-plane change) and offers the
   storage-derived form only at the external boundary. Should loom eventually switch its
   *canonical emitted* output ref to the storage-derived form for full OpenLineage
   conformance in the stored graph? That would require threading `ObjectStoreConfig`
   into the control-plane emitters (`iceberg_flush.rs` et al.) — a larger change with a
   data-migration tail. Recommendation: defer; the dual-recognition resolver makes it a
   non-breaking follow-up.
2. **Type namespace under storage derivation.** Ontology types have no physical storage,
   so `type_ref` stays on the logical `"loom:type"` namespace. Is that the right
   OpenLineage modelling, or should a type's ref borrow its backing table's storage
   namespace (via `ObjectType.table`)? Kept logical here for simplicity and because the
   resolver already handles it; flag for review.
3. **External-node ACL-filtering default.** When `road-lineage-acl-filtering` meets a
   `ResolvedDataset::External`, the default here is **show** (loom governs no policy for
   it). Should external nodes instead be hidden by default (treating "who fed loom" as
   sensitive), or made configurable? A governance-policy call for that slice; noted so
   it is a conscious choice.
4. **Crate placement.** New `//src/services/lineage-naming` crate vs folding the bridge
   into `//src/services/store-config` (which already owns `ObjectStoreConfig` and is
   postgres-free). A separate crate keeps `store-config` free of a `control-plane-core`
   dependency; folding in avoids a crate. Leaning separate-crate for dependency
   hygiene; low-stakes, decide at implementation.
