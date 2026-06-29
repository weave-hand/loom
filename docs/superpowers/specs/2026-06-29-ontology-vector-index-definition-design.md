# Ontology vector-index definition — design

**Status:** approved (brainstorming) — ready for `loom-work-plan` to land, then a work agent to plan/build.

**Slice of:** `fut-puffin-vector-index-ann` (the Puffin vector-index arc). Prerequisite
for the external `/search` endpoint (the *next* slice — see Out of scope).

## Problem

loom can *build* vector indexes (Flat / IVF-Flat / HNSW) and serve engine-side kNN
over them, but it has **no ontology-level record that an index should exist**. Today:

- An index is created only by an out-of-band `build_vector_index` **queue job** that
  carries kind/params ad-hoc (`BuildVectorIndexJob { schema, name, column,
  index_kind, nlist, m, ef_construction }`), with `metric` passed loose to the build
  primitive.
- `iceberg_mirror.vector_index` (PK `(table_id, column_name, covered_snapshot)`) is
  written *only after* a successful build — it is post-build **evidence**, not a
  **declaration**.
- The mirror's PK allows **one index per column**.

So there is no durable source of truth for "this type's `embedding` property has an
HNSW/Cosine index", nowhere to hang automatic rebuild later, and no way to declare
**multiple** indexes on the same property — which is a real need: an HNSW index for
nearest-neighbour similarity (1→many) *and* an IVF-Flat index for clustering/grouping
("how do these objects group") over the same column.

## Goal

Make a **named vector index** a first-class ontology object. The ontology
*declares* indexes; the mirror *materializes* them at build; the read path keys off
the mirror. The declaration is the **single source of truth** for an index's
kind/metric/params. Support **multiple named indexes per property**.

Clean split of responsibility:

- **Declaration** (`ontology.vector_index_definition`) → feeds **build**.
- **Mirror** (`iceberg_mirror.vector_index`) → feeds **read**.
- Build copies declaration → mirror row.

## Non-goals / Out of scope (deferred to `fut-puffin-vector-index-ann`)

- **The external `/search` HTTP endpoint** — the *next* slice. This slice wires the
  index *name* through the existing engine call + tests so named indexes resolve
  coherently, but adds no query-api HTTP route. (The parked `/search` design:
  `POST /search/:type/:index_name`, ids+distances result, coarse Read gate +
  row-filter post-filter, optional `nprobe`/`ef_search` knobs.)
- **Automatic build / staleness-on-flush.** Declaring an index does *not* trigger a
  build; building stays an explicit enqueue. All triggering (on-define and on-flush
  rebuild) remains deferred.
- **`drop_vector_index` + Puffin-sidecar GC.** Redeclaring a name replaces its spec
  (upsert); there is no drop operation this slice (drop forces the deferred
  Puffin-GC question). A stale mirror row after a spec change is acceptable
  (staleness handling is deferred).
- **ACL on ontology `define_*`.** Definition ops are internal/programmatic and
  ungoverned today; `define_vector_index` inherits that. No new HTTP surface.

## Decisions (settled in brainstorming)

1. **Named, first-class indexes** via a standalone `define_vector_index` op (not
   inlined on the type definition); **multiple per property**, distinguished by name.
2. **Declaration is authoritative.** Build params (kind/metric/nlist/m/ef_construction)
   come off `BuildVectorIndexJob` and the build RPC; the build primitive reads them
   from the named declaration.
3. **Declaration only.** Building remains a separate explicit step.
4. **No HTTP / no new ACL** — consistent with the existing `define_*` surface.

## Design

### 1. Domain model (`src/control-plane/core`)

Reuse the existing `IndexSpec` (`Flat | IvfFlat { nlist } | Hnsw { m, ef_construction }`)
and `Metric` (`Cosine | L2`). Add one struct:

```rust
/// A named vector index declared on an object type's vector property.
/// Dimension is NOT restated — it is derived from the property's `vector(N)` type.
pub struct VectorIndexDef {
    pub name: String,         // unique per type, e.g. "by_sim", "by_cluster"
    pub type_name: TypeName,
    pub property: String,     // a `vector(N)` property of `type_name`
    pub metric: Metric,
    pub spec: IndexSpec,      // kind + params
}
```

Extend the `Ontology` trait (`core/src/ontology.rs`):

```rust
/// Declare (upsert) a named vector index. Replaces an existing index of the
/// same (type, name) — matching `define_type`'s replace semantics. Validates
/// that `def.property` exists on `def.type_name` and is a `vector(N)` type;
/// errors otherwise.
async fn define_vector_index(&self, def: VectorIndexDef) -> Result<()>;

/// Look up one named index declaration.
async fn get_vector_index(&self, type_name: &TypeName, name: &str)
    -> Result<Option<VectorIndexDef>>;

/// All index declarations on a type (for build resolution / future /search
/// discovery).
async fn vector_indexes_for(&self, type_name: &TypeName)
    -> Result<Vec<VectorIndexDef>>;
```

Validation (property exists and is `vector(N)`) lives with the operation. The
implementation reads the type's properties to confirm the referenced property is a
vector type; the declared name is unique per type (PK-enforced). Errors:
type/property not found, or property not a `vector(N)` type.

### 2. Storage (`src/control-plane/postgres`)

**Migration `0020_vector_index_definition.sql`:**

```sql
create table ontology.vector_index_definition (
    type_name       text    not null references ontology.object_type (name) on delete cascade,
    name            text    not null,
    property_name   text    not null,
    metric          text    not null,
    index_kind      text    not null,
    nlist           integer,            -- IVF-Flat only, nullable
    m               integer,            -- HNSW only, nullable
    ef_construction integer,            -- HNSW only, nullable
    primary key (type_name, name)
);
```

**Migration `0021_vector_index_named.sql`** — extend the mirror so N named indexes
coexist per column:

```sql
alter table iceberg_mirror.vector_index
    add column index_name text not null default 'default';
alter table iceberg_mirror.vector_index
    drop constraint vector_index_pkey;
alter table iceberg_mirror.vector_index
    add primary key (table_id, column_name, index_name, covered_snapshot);
```

(The `'default'` backfill keeps any pre-existing dev rows valid; new builds write the
real `index_name`.)

**Adapter** (`postgres/src/ontology.rs` + `postgres/src/vector_index.rs`):

- Implement `define_vector_index` (upsert with `on conflict (type_name, name) do
  update`), `get_vector_index`, `vector_indexes_for` — mirror the existing
  `define_type` / property-insert patterns. `metric`/`index_kind` persist as their
  `as_str()` strings; `nlist`/`m`/`ef_construction` map from `IndexSpec` to the
  nullable columns (and back via `IndexSpec`/`Metric`/`IndexKind` `from_str`).
- `lookup_vector_index` and `insert_vector_index` gain `index_name` (lookup keys on
  `(table_id, index_name, at)` returning the newest `covered_snapshot <= at`; insert
  includes `index_name` in the column list + `on conflict` key).
- **Compile-time sqlx:** every new/changed `query!` requires regenerating the
  committed `.sqlx` cache via `tools/sqlx-prepare.sh`. The
  `sqlx-cache-check` test gates freshness.

### 3. Build path (declaration → mirror)

- `BuildVectorIndexJob` (`core/src/vector_index_job.rs`) shrinks to
  `{ schema, name, index_name }` — **drops** `index_kind`/`nlist`/`m`/`ef_construction`.
- `build_vector_index` primitive (`postgres/src/vector_index.rs`) signature becomes
  `(catalog, pool, table, index_name, run_id)`. It resolves the table → type
  (via `ontology.object_type` by `(table_schema, table_name)` — the same lookup
  `identity_column_for` already performs), loads the named declaration
  (`(type_name, index_name)`), and builds with the declaration's `property` (→ the
  vector column), `metric`, and `spec`. It writes the mirror row **with
  `index_name`**. No declaration for the name ⇒ a clear build error
  ("no such vector index definition: <name>").
- The engine build RPC (`engine-wire` `engine_control.proto`
  `BuildVectorIndexRequest`) and the worker handler (`services/worker/src/handler.rs`)
  carry `index_name` in place of the dropped kind/params/metric fields; the engine
  service handler forwards to the primitive.

### 4. Read path (name-keyed resolution)

So named indexes resolve unambiguously (a column may now back several indexes):

- `engine_serving::vector_search` (`services/engine-serving/src/vector_search.rs`)
  gains an `index_name` parameter and looks the mirror up by
  `(table_id, index_name, at)`. The mirror row already carries
  `column_name`/`metric`/`index_kind`/`dim`/`puffin_path`, so the read path needs no
  ontology lookup — it reads the index by name, then performs the existing cold
  Puffin + hot-delta merge against the row's `column_name`.
- `VectorSearchTicket` (`engine-wire/src/flight.rs`) gains `index_name`; the engine
  Flight handler (`engine/src/flight.rs do_get_vector_search`) passes it through.
- No declaration is read at query time; the mirror is the materialized read record.

### Data flow

```
define_vector_index("by_sim", document, "embedding", Hnsw, Cosine, m=16, efc=200)
   → ontology.vector_index_definition row                       [DECLARE]

enqueue build_vector_index{ schema, name(table), index_name:"by_sim" }
   → primitive: table→type→declaration "by_sim"
   → build HNSW/Cosine/m16/efc200 over `embedding`
   → Puffin blob + iceberg_mirror.vector_index(…, index_name="by_sim")  [BUILD]

vector_search(table, index_name:"by_sim", query, k)
   → mirror lookup (table_id,"by_sim",Q) → column,metric,kind,puffin
   → cold ∪ hot merge → top-k                                   [READ]
```

## Error handling

- `define_vector_index`: type or property not found → error; property not `vector(N)`
  → error; duplicate `(type, name)` → upsert (replace). Changing a spec does not
  rebuild an already-built index (staleness deferred); the next build uses the new
  spec.
- `build_vector_index{index_name}`: no declaration for `index_name` on the resolved
  type → build error.
- `vector_search(index_name)`: no mirror row for `index_name` → the existing
  `NoIndex` error (surfaces as 404 once `/search` lands).

## Testing

All tests are `rust_test` integration targets (no inline `#[cfg(test)]`); fixture
tests use the `loom_fixture_test` macro.

- **Ontology declaration (postgres fixture):** `define_vector_index` persists and
  reads back via `get_vector_index` / `vector_indexes_for`; validation rejects a
  missing property and a non-`vector` property.
- **Headline acceptance — multiple indexes per property (postgres/engine fixture):**
  declare two indexes on one `vector(N)` property — HNSW `by_sim` (Cosine) and
  IVF-Flat `by_cluster` (L2) — build **both**, assert **two** mirror rows
  `(index_name = by_sim | by_cluster)`, and `vector_search` each **by name**
  independently returns results.
- **Build-by-name (worker/postgres fixture):** a `build_vector_index{index_name}` job
  resolves the declaration, builds, and writes a mirror row with the right
  `index_name`, `index_kind`, and `metric`; an unknown `index_name` fails the build.
- **Read-by-name (engine-serving fixture):** `vector_search` keyed by `index_name`
  returns the expected ranked ids+distances; the cold∪hot freshness invariant holds.

## Affected components (for the implementation plan)

- `core`: `VectorIndexDef`, three `Ontology` trait methods, `BuildVectorIndexJob`
  field change.
- `postgres`: migrations `0020`/`0021`, ontology + vector_index adapter changes,
  `lookup_vector_index`/`insert_vector_index` `index_name`, `.sqlx` regen.
- `engine-wire`: `engine_control.proto` `BuildVectorIndexRequest`,
  `VectorSearchTicket` `index_name`.
- `engine` / `engine-serving`: build RPC handler, `vector_search` `index_name`.
- `worker`: `build_vector_index` job handler.
- Tests across the postgres / worker / engine-serving fixtures (above).
