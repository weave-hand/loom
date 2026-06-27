# Puffin-backed vector index — flat/exact walking skeleton, engine-side

_Design spec. 2026-06-27._

## Context

loom already stores embeddings as governed, lineage-tracked data: the
**`vector(N)` column type** (`road-vector-column-type`, PR #168,
`2026-06-23-vector-column-type-design`) lands an `f32` embedding column as an
Iceberg `list<float>`, versioned per snapshot, read back value-exact through the
columnar serving path. That slice drew an explicit boundary — **"loom stores, not
searches"**: no ANN, no distance, no index; vector *search* lived in an external
hot tier.

This slice **deliberately extends that boundary**: loom gains the ability to
**precompute a vector index over a table's embeddings, store it as an Apache
Puffin sidecar bound to a snapshot, and answer an exact k-nearest-neighbour
query** — merging the cold precomputed index with the table's un-flushed inline
("hot") rows at query time. It is grounded in **arxiv 2606.04196**
("Puffin-Backed Vector Indexes: Attaching Approximate Nearest Neighbor Indexes
to Apache Iceberg Snapshots for Compute-Disaggregated Query Engines"), adapted to
loom's Postgres-mirror + DataFusion-engine architecture.

The paper attaches *approximate* (Vamana/DiskANN) indexes via a distributed
coordinator/executor build and a disaggregated beam-search query. This spec
scopes only the **first slice**: a **flat (exact) index** behind a swappable
`VectorIndex` abstraction, single-node build, engine-side query. The deliverable
is **the abstraction and the Puffin + hot/cold-merge integration seam** — the
exact algorithm is the first implementation, and HNSW/Vamana slot behind the same
trait later without touching the binding or merge plumbing.

## What Puffin is, in this context

**Puffin** is Apache Iceberg's auxiliary binary file format — a standardized
container for index/statistics *blobs* that do not belong inside the row data. It
is a **sidecar**: its own object-store file next to the table, not embedded in the
Parquet data files. Layout: magic `PFA1`, a sequence of opaque blobs, and a JSON
footer (`FileMetadata`) describing each blob — a `type` string, the table `fields`
it covers, `snapshot-id`, `offset`/`length`, `compression-codec`, and a free-form
`properties` map. Introduced for table stats (`apache-datasketches-theta-v1`) and
v3 deletion vectors (`deletion-vector-v1`); the blob `type` is an arbitrary
string, so **custom blob types are first-class**.

Here the Puffin sidecar is the **container for the precomputed vector index**:
the blob (`loom-vector-index-v1`) holds the serialized index over the table's
vectors. At query time the **row data** comes from Parquet (cold) + inline
Postgres (hot), while the **index** is read from the Puffin sidecar. Reusing
Puffin gives the index Iceberg's lifecycle for free — snapshot-scoped, versioned
with the table, GC'd with old snapshots — and keeps the file a recognized,
interoperable Iceberg artifact.

**The one loom twist:** the paper binds the sidecar through an Iceberg **REST
catalog**'s `statistics-file` snapshot-summary property. loom has **no REST
catalog** — the **Postgres mirror is the catalog** — so loom records the
`(table, column, covered-snapshot) → Puffin path` binding as a mirror row. The
*file format* stays the standard Iceberg sidecar; only the *binding pointer* is
loom-native.

## Current state (the seams to extend)

- **Vector column** (`road-vector-column-type`): `BaseType::Vector(u32)`
  (`core/src/logical_type.rs`), stored as Iceberg `list<float>`, read back
  value-exact via the columnar path. Element type fixed `f32`.
- **Hot tier — inline writes** (`postgres/src/iceberg_inline.rs`): small writes
  land as typed rows in a per-table `iceberg_mirror.inline_<table_id>` Postgres
  table — a mirror-only commit (snapshot + rows + lineage in one tx), MVCC-filtered
  by `begin_snapshot`/`end_snapshot`. External Iceberg clients do not see inline
  rows until a flush.
- **Cold tier — flush** (`core/src/flush.rs`, `FLUSH_JOB_KIND = "flush_table"`,
  worker): a queue job materializes inline rows into Parquet and clears them
  (`end_snapshot` set).
- **The union already exists** (`engine-serving/src/provider.rs`): `PgTableProvider`
  is a DataFusion `TableProvider` that scans inline Pg rows with the per-query
  MVCC snapshot filter; the engine unions them with the table's Parquet at read
  time (`datafusion_inline_union` tests). The **engine service is the sole serving
  path** (DataFusion over the Iceberg mirror); **query-api** is a zero-DataFusion
  wire client over internal Flight SQL.
- **Object identity** (`ObjectType.identity`): the per-object key, already used by
  traversal dedup.
- **No Puffin support anywhere** today (grep: zero references). iceberg-rust (the
  pinned `main` dep) ships an `iceberg::puffin` module — read/write of blobs +
  footer metadata.

## Decision

### 1. The abstraction — `VectorIndex` (in `core`)

A trait that later approximate implementations slot behind unchanged:

```rust
pub enum Metric { Cosine, L2 }          // declared at build; default Cosine

pub trait VectorIndex {
    fn metric(&self) -> Metric;
    fn dim(&self) -> u32;
    /// Exact-or-approximate top-k by `metric`. Returns (object identity, distance),
    /// ascending distance. Slice-1 impl is exact.
    fn search(&self, query: &[f32], k: usize) -> Vec<(VectorKey, f32)>;
}
```

`VectorKey` is the object identity value (the `ObjectType.identity` column value)
carried alongside each indexed vector so results map back to objects. Slice-1
implementation: **`FlatIndex`** — packed `[f32; dim]` rows + a parallel identity
column, exact brute-force `search`. The trait — not the algorithm — is the
deliverable; HNSW/Vamana become alternative impls in a later slice.

Both metrics ship: **Cosine** (default; embeddings norm) and **L2**. The metric
is a property of the built index (recorded in blob metadata and the mirror), not a
per-query choice — a query against an index uses the index's metric.

### 2. Storage — the `loom-vector-index-v1` Puffin blob

Serialize a `FlatIndex` to a single Puffin blob written via iceberg-rust's
`puffin` module to the object store (same store as the table's Parquet):

- **Blob payload:** a small self-describing binary — header (`dim`, `metric`,
  `index-kind`, `row-count`) + the packed `f32` vectors + the parallel identity
  column (identity logical type recorded so it decodes).
- **Blob `type`:** `loom-vector-index-v1`.
- **Blob metadata/`properties`:** `dim`, `metric`, `index-kind=flat`, `column`
  (the `vector(N)` property name), `identity-column`, `row-count`,
  `covered-snapshot` (the `S` the index was built as of). `fields` carries the
  Iceberg field id of the vector column.

> **De-risk first task.** Before any wiring: write a `loom-vector-index-v1` blob
> through the pinned iceberg-rust `puffin` writer and read it back byte-exact via
> the reader, asserting the footer metadata round-trips. This is the one
> load-bearing external dependency (the iceberg `puffin` module's read/write
> surface); confirm it first, exactly as the vector-column slice confirmed the
> `List<Float32>` Parquet round-trip first.

### 3. Binding — a mirror artifact row

loom has no REST catalog; the Postgres mirror is the catalog. Add a mirror table
(new migration) binding an index to a snapshot:

- `vector_index(table_id, column, covered_snapshot, metric, index_kind, dim,
  row_count, puffin_path, created_at)`, unique on
  `(table_id, column, covered_snapshot)`.
- The build writes this row in the **same transaction** as the lineage event, so
  the index is a governed, lineage-tracked artifact riding the snapshot exactly
  like a Parquet data file. A `LineageEvent` records the index build (inputs: the
  covered snapshot's data; output: the Puffin artifact).
- The query path looks up the **latest** `vector_index` row for `(table, column)`
  with `covered_snapshot <= Q` to find the sidecar and its `S`.

GC of orphaned Puffin sidecars (snapshot expiry) reuses the existing Iceberg GC
seam and is **out of scope** here (tracked follow-on; the artifact is recorded so
GC can find it).

### 4. Build — a primitive plus a queue-job kind

Both ship (per the scope decision):

- **Build primitive** (`postgres` or a new `services/` module, callable +
  fixture-tested, like the landing materializer): for `(table, column)` as of
  snapshot `S`, read **all** vectors via the serving read path (`Parquet ∪
  inline@S`) together with their identity column → build `FlatIndex` → serialize →
  write the Puffin sidecar → insert the `vector_index` mirror row + lineage event
  in one tx.
- **Queue job kind** `build_vector_index` (mirrors `FLUSH_JOB_KIND`): payload
  `{schema, name, column}`; the worker handler invokes the primitive. This is how
  a build is triggered in production. **Automatic** triggering (rebuild-on-flush /
  staleness) is **deferred** — slice 1 builds on explicit enqueue.

Building over the **serving read** (not raw Parquet) means the index covers
*everything live at `S`*, flushed or not — which makes the query-time invariant
(below) clean.

### 5. Query — the hot/cold merge, engine-side

A DataFusion capability in the **engine** (the sole serving path), reached over
the existing internal Flight SQL surface. **No external query-api endpoint in this
slice** — the engine-side capability is the deliverable; exposing it externally is
a later slice. Given `(table, column, query_vec, k)` at the query's MVCC snapshot
`Q`:

1. **Cold.** Look up the bound index (`covered_snapshot S <= Q`), read its Puffin
   sidecar, deserialize the `FlatIndex`, run exact top-k.
2. **Hot delta.** Scan inline rows via `PgTableProvider` with `begin_snapshot > S
   AND live@Q`, brute-force exact top-k over them by the index's metric.
3. **Merge.** Combine the two top-k lists, keep the global top-k `(identity,
   distance)` ascending.

**Correctness invariant (append-only).** The index covers everything live at `S`;
the hot delta is exactly the inline rows *born after* `S`. A row flushed between
`S` and `Q` is counted once — present in the index (it was live at `S`), excluded
from the delta (`begin > S`) — so **no double-count and no miss**. This holds
because loom is append-only today (updates/deletes are deferred,
`fut-update-delete-actions`); when those land, the index gains a deletion-vector
or rebuild story (later slice).

If **no index is bound** for `(table, column)`, the query is a deterministic
"no index" error in this slice (a pure brute-force-everything fallback is a
possible later convenience, not built here).

### 6. Governance

The index build and query ride loom's existing governance: the build emits a
lineage event and the artifact is mirror-recorded; the query path runs in the
engine under the same ACL/snapshot context as any other serving read (the kNN
reads the same governed columns). No new ACL concern is introduced in this slice
beyond reusing the read path's authorization.

## Surface

- `core`: `VectorIndex` trait, `Metric`, `VectorKey`; `FlatIndex`
  (serialize/deserialize + exact `search`); `build_vector_index` job
  payload/kind constant (alongside `flush.rs`).
- `postgres`: `vector_index` mirror table + migration; the build primitive
  (serving-read → `FlatIndex` → Puffin write → mirror row + lineage, one tx);
  Puffin write/read helpers over iceberg-rust `puffin`; mirror lookup for the
  query path.
- `worker`: `build_vector_index` handler invoking the primitive.
- `engine` / `engine-serving`: the kNN merge capability (cold Puffin index ∪ hot
  inline brute-force) exposed over internal Flight SQL.
- `third-party`: confirm the iceberg `puffin` module is reachable (no new crate
  expected; it is part of the pinned iceberg dep).

## Testing

`rust_test` (pure-logic where possible) + `loom_fixture_test` (hermetic Postgres +
object store):

- **De-risk Puffin round-trip (fixture/io):** write a `loom-vector-index-v1` blob
  via iceberg-rust `puffin` and read it back byte-exact incl. footer metadata. Do
  it first.
- **`FlatIndex` (pure):** build over a handful of known vectors; exact top-k by
  Cosine and by L2 returns the mathematically correct neighbours and distances;
  `k` larger than row-count returns all; serialize→deserialize is value-exact.
- **Build primitive (fixture):** land a typed object with an `embedding: vector(4)`
  column (some flushed, some inline) over Iceberg; run the build as of `S`; assert
  a Puffin sidecar exists, a `vector_index` mirror row + lineage event were written
  in one tx, and the blob covers all rows live at `S`.
- **Query hot/cold merge (fixture, the headline):** with an index built at `S`,
  insert further inline rows (born after `S`), run an engine kNN query at `Q > S`;
  assert the returned top-k is the exact global nearest set across cold-index +
  hot-inline rows, with no row double-counted and none missed (incl. a row flushed
  between `S` and `Q`).
- **No-index error (fixture):** a kNN query against a `(table, column)` with no
  bound index is a deterministic error, not a panic.
- **Job wiring (fixture):** enqueue a `build_vector_index` job; the worker handler
  produces the same artifact as the direct primitive call.
- **Defaults unchanged:** `buck2 test //src/...` green; non-vector serving and the
  existing inline-union path behave identically.

## Scope boundary

- **In:** `VectorIndex` trait + `FlatIndex` (exact, Cosine + L2); Puffin
  `loom-vector-index-v1` read/write; `vector_index` mirror binding + migration +
  lineage; build primitive + `build_vector_index` queue job + worker handler;
  engine-side hot/cold exact-kNN merge over internal Flight SQL; the tests above.
- **Out (deferred, tracked under `fut-puffin-vector-index-ann`):** approximate
  ANN (HNSW/Vamana) behind the trait; automatic rebuild / staleness-on-flush;
  clustering surface; sharded/distributed build + tiered probe + beam search (the
  paper's coordinator/executor); external query-api search endpoint; pruning the
  cold index by ACL/predicate before kNN; deletion-vector / update-delete index
  maintenance; non-`f32` element types and metrics beyond Cosine/L2; Puffin
  sidecar GC on snapshot expiry; DuckLake path (`fut-vector-ducklake`).

## Acceptance criteria

1. A `loom-vector-index-v1` Puffin blob round-trips byte-exact (payload + footer)
   through the pinned iceberg-rust `puffin` module.
2. The build primitive, run for a `vector(N)` column as of snapshot `S`, writes a
   Puffin sidecar and a `vector_index` mirror row + lineage event in one
   transaction, covering every row live at `S` (flushed or inline).
3. A `build_vector_index` queue job, processed by the worker, produces the same
   artifact as the direct primitive call.
4. An engine-side kNN query at snapshot `Q` returns the **exact** global top-k
   across the cold Puffin index and the hot inline delta (born after `S`), with no
   double-count and no miss — including a row flushed between `S` and `Q`. Cosine
   and L2 both verified.
5. A kNN query with no bound index is a deterministic error, not a panic.
6. `buck2 test //src/...` is green; all existing serving / inline-union behaviour
   and defaults are unchanged.

## Research

Grounding for the representation and binding choices.

- **Puffin is the standardized Iceberg sidecar for index/stat blobs**, with
  arbitrary string blob `type`s (so a custom `loom-vector-index-v1` is spec-legal),
  a JSON footer carrying per-blob `fields`/`snapshot-id`/`properties`, and a
  lifecycle bound to table snapshots. (Iceberg Puffin spec.)
- **arxiv 2606.04196** stores Vamana/DiskANN graphs in Puffin blobs bound to a
  snapshot via the REST catalog's `statistics-file` summary property, with a
  coordinator/executor distributed build (executor shard blobs + a coordinator
  routing blob carrying a centroid codebook and covered file paths) and a
  disaggregated tiered-probe + beam-search query. This slice adapts the
  *binding-via-snapshot* and *index-in-Puffin* ideas to a **single-node, exact**
  first cut, replacing the REST-catalog binding with a loom mirror row and the
  distributed build/query with loom's queue+worker (build) and engine+DataFusion
  (query) split. The approximate/distributed parts map to later slices.
- **loom's hot/cold split is the natural fit for query-time freshness:** the
  precomputed index covers the cold (flushed) data, and loom's existing inline Pg
  rows + `PgTableProvider` MVCC scan supply the hot delta, merged exactly — so a
  query is fresh to the current snapshot without rebuilding the index per write.
  (`engine-serving/src/provider.rs`, `iceberg_inline.rs`.)
