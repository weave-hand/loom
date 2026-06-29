# Ontology Vector-Index Definition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a **named vector index** a first-class ontology object so a `vector(N)` property can carry a durable, authoritative index declaration (kind/metric/params) — multiple per property — that drives builds and resolves at read time by name.

**Architecture:** A new `Ontology::define_vector_index` op persists named declarations to a new `ontology.vector_index_definition` table (the **single source of truth** for build params); the `iceberg_mirror.vector_index` PK gains `index_name` so N named indexes coexist per column; the build primitive resolves the named declaration (dropping ad-hoc job/RPC params); the read path (`vector_search`/`VectorSearchTicket`) resolves by `index_name`. Declaration only — building stays an explicit enqueue.

**Tech Stack:** Rust, async-trait, sqlx **compile-time** `query!` (committed `.sqlx` cache), Postgres (`ontology` + `iceberg_mirror` schemas), tonic/prost (engine gRPC), buck2 (`loom_fixture_test` for hermetic-postgres tests).

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-06-29-ontology-vector-index-definition-design.md` is authoritative.
- **Tests are `rust_test` integration targets only** — no inline `#[cfg(test)]`/`#[test]` in `src/**`. Fixture (hermetic-postgres) tests use the **`loom_fixture_test`** macro, never bare `rust_test`.
- **Build/test via buck2 only, never cargo.** Never pipe `buck2 test`/`bxl` through `tail`/`head` — redirect to a file and grep.
- **Strict clippy** (pedantic + restriction): production code needs `#[expect(lint, reason = "...")]` for any `unwrap`/`expect`/`panic`/`indexing_slicing`/`as`-truncation/`as`-sign-loss where unavoidable. Test code is exempted from panic-safety lints via the macros.
- **Compile-time sqlx:** any new/changed `query!`/`query_scalar!` in `src/control-plane/postgres` requires regenerating the committed cache with `tools/sqlx-prepare.sh` and committing `src/control-plane/postgres/.sqlx/`. The `//src/control-plane/postgres:sqlx-cache-check` test gates freshness.
- **Markdown** files end with exactly one trailing newline, no trailing whitespace.
- **Declaration is authoritative:** build params (kind/metric/nlist/m/ef_construction) come OFF `BuildVectorIndexJob`, the build RPC, and the build primitive signature.
- **`index_name` is required end-to-end** (no `Option`); names are unique per type.
- **Every task must leave `buck2 build //src/...` green** — a task that changes a public signature updates ALL its callers (src AND tests) in the same commit. The temporary `"default"` literal introduced in Task B is threaded through every caller consistently and is fully removed in Task C.
- **No HTTP, no new ACL** — `define_vector_index` is an internal `Ontology` op like the other `define_*`.

---

## File Structure

**New files:**
- `src/control-plane/postgres/migrations/0020_vector_index_definition.sql` — the declaration table.
- `src/control-plane/postgres/migrations/0021_vector_index_named.sql` — mirror `index_name` + PK change.
- `src/control-plane/postgres/tests/vector_index_named.rs` — fixture: mirror insert/lookup by `index_name`.
- `src/control-plane/postgres/tests/vector_index_multi.rs` — headline acceptance: two named indexes on one property.

**Modified files:**
- `src/control-plane/core/src/ontology.rs` — `VectorIndexDef` struct + 3 trait methods.
- `src/control-plane/core/src/vector_index.rs` — `IndexSpec::as_cols` mapping helper.
- `src/control-plane/core/src/lib.rs` — export `VectorIndexDef`.
- `src/control-plane/core/src/vector_index_job.rs` — `BuildVectorIndexJob` → `{ schema, name, index_name }`.
- `src/control-plane/core/tests/vector_index_job.rs` — rewrite for the new job shape.
- `src/control-plane/memory/src/ontology.rs` — memory-fake impls + `OntologyState` field.
- `src/control-plane/postgres/src/ontology.rs` — postgres adapter impls + `vector_index_def_row` free helper.
- `src/control-plane/postgres/src/vector_index.rs` — `VectorIndexRow.index_name`; `lookup`/`insert` gain `index_name`; `build_vector_index` resolves the declaration; `type_name_for` helper.
- `src/control-plane/postgres/tests/{vector_index_hnsw,vector_index_mirror,vector_index_build,vector_index_ivf}.rs` — declare-then-build-by-name.
- `src/control-plane/testkit/src/lib.rs` — extend `ontology_contract` with a vector-index block.
- `src/services/engine-wire/proto/engine_control.proto` — `BuildVectorIndexRequest` → `{ schema, name, index_name }`.
- `src/services/engine-wire/src/flight.rs` — `VectorSearchTicket` gains `index_name`, drops `column`.
- `src/services/engine-wire/src/client.rs` — `build_vector_index` client method shrinks.
- `src/services/engine-wire/tests/vector_search_ticket.rs` (if present) — ticket round-trip with `index_name`.
- `src/services/engine/src/service.rs` — build RPC handler resolves declaration.
- `src/services/engine/src/flight.rs` — `do_get_vector_search` passes `index_name`.
- `src/services/engine/tests/vector_search_flight.rs` — primitive call + ticket construction updated.
- `src/services/engine-serving/src/vector_search.rs` — `vector_search` gains `index_name`.
- `src/services/engine-serving/tests/vector_search.rs` — declare-then-build-then-search by name.
- `src/services/worker/src/handler.rs` — `handle_build_vector_index` shrinks.
- `src/services/worker/tests/build_vector_index.rs` — declare-then-build-by-name.
- Various `BUCK` files for the new test targets.

---

## Task A: Ontology vector-index declaration (core + memory + postgres + contract)

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (add `VectorIndexDef` + 3 trait methods after line 194)
- Modify: `src/control-plane/core/src/vector_index.rs` (`IndexSpec::as_cols`)
- Modify: `src/control-plane/core/src/lib.rs` (export `VectorIndexDef`)
- Modify: `src/control-plane/memory/src/ontology.rs` (impls + `OntologyState.vector_indexes`)
- Create: `src/control-plane/postgres/migrations/0020_vector_index_definition.sql`
- Modify: `src/control-plane/postgres/src/ontology.rs` (impls + free helper)
- Modify: `src/control-plane/testkit/src/lib.rs` (extend `ontology_contract`)

**Interfaces:**
- Produces:
  - `pub struct VectorIndexDef { pub name: String, pub type_name: TypeName, pub property: String, pub metric: Metric, pub spec: IndexSpec }` (derives `Clone, Debug, PartialEq, Eq`)
  - `Ontology::define_vector_index(&self, def: VectorIndexDef) -> Result<()>`
  - `Ontology::get_vector_index(&self, type_name: &TypeName, name: &str) -> Result<Option<VectorIndexDef>>`
  - `Ontology::vector_indexes_for(&self, type_name: &TypeName) -> Result<Vec<VectorIndexDef>>`
  - `IndexSpec::as_cols(&self) -> (&'static str, Option<u32>, Option<u32>, Option<u32>)`
  - postgres free helper `pub async fn vector_index_def_row(pool: &PgPool, type_name: &str, name: &str) -> Result<Option<VectorIndexDef>>` (consumed by Task C's build primitive)
- Consumes: existing `IndexSpec`, `Metric`, `TypeName`, `Result`.

- [ ] **Step 1: Confirm the set of `Ontology` implementors.** Run `grep -rn "impl Ontology for" src` — expect exactly two: `memory` and `postgres`. If a third exists, it MUST gain the new methods in this task too (the trait change otherwise fails its build).

- [ ] **Step 2: Add the `VectorIndexDef` struct + trait methods (core).** In `src/control-plane/core/src/ontology.rs`, add `use crate::vector_index::{IndexSpec, Metric};` near the top (after line 15), then add this struct after `ActionDef` (after line 168):

```rust
/// A named vector index declared on an object type's `vector(N)` property. The
/// declaration is the authoritative source of an index's kind/metric/params;
/// the build primitive resolves it and copies it into the mirror row. Dimension
/// is NOT restated — it is derived from the property's `vector(N)` type. Multiple
/// indexes may exist per property, distinguished by `name` (unique per type).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VectorIndexDef {
    pub name: String,
    pub type_name: TypeName,
    pub property: String,
    pub metric: Metric,
    pub spec: IndexSpec,
}
```

Then add three methods to the `Ontology` trait just before its closing `}` (after `get_action`, line 194):

```rust
    /// Declare (upsert) a named vector index, keyed by `(type, name)`. Replaces an
    /// existing index of the same key — matching `define_type`'s replace semantics.
    /// Validates that `def.property` exists on `def.type_name` and is a `vector(N)`
    /// type; returns `Validation` otherwise.
    async fn define_vector_index(&self, def: VectorIndexDef) -> Result<()>;
    /// Fetch one named index declaration. `None` if absent.
    async fn get_vector_index(&self, type_name: &TypeName, name: &str)
        -> Result<Option<VectorIndexDef>>;
    /// All index declarations on `type_name` (order unspecified).
    async fn vector_indexes_for(&self, type_name: &TypeName) -> Result<Vec<VectorIndexDef>>;
```

- [ ] **Step 3: Add `IndexSpec::as_cols` (core).** In `src/control-plane/core/src/vector_index.rs`, add to `impl IndexSpec` (after `from_label`, line 41):

```rust
    /// The persisted `(index_kind, nlist, m, ef_construction)` column tuple for a
    /// declaration row. Inverse of [`IndexSpec::from_label`].
    #[must_use]
    pub fn as_cols(&self) -> (&'static str, Option<u32>, Option<u32>, Option<u32>) {
        match self {
            IndexSpec::Flat => ("flat", None, None, None),
            IndexSpec::IvfFlat { nlist } => ("ivf_flat", *nlist, None, None),
            IndexSpec::Hnsw { m, ef_construction } => ("hnsw", None, *m, *ef_construction),
        }
    }
```

- [ ] **Step 4: Export `VectorIndexDef` (core lib).** In `src/control-plane/core/src/lib.rs`, add `VectorIndexDef` to the `pub use` line that re-exports ontology items (the one exporting `ObjectType, Ontology, PropertyDef, TypeName, ...`).

- [ ] **Step 5: Verify the trait change ripples (expected breakage).** Run `buck2 build //src/control-plane/memory:memory //src/control-plane/postgres:postgres 2>&1 | tail -8`. Expect a **build failure**: both crates' `impl Ontology for` is now missing three methods. This confirms the trait change is in place; Steps 6–8 satisfy the impls.

- [ ] **Step 6: Implement in the memory fake.** In `src/control-plane/memory/src/ontology.rs`: add `VectorIndexDef` to the `control_plane_core` import list; add a field to `OntologyState`:

```rust
    pub(crate) vector_indexes: HashMap<(String, String), VectorIndexDef>,
```

(keyed by `(type_name, index_name)`). Then add the three impls inside `impl Ontology for MemoryControlPlane` (after `get_action`):

```rust
    async fn define_vector_index(&self, def: VectorIndexDef) -> Result<()> {
        let mut ont = self.ontology.lock();
        let ty = ont
            .types
            .get(&def.type_name.0)
            .ok_or_else(|| ControlPlaneError::NotFound(def.type_name.0.clone()))?;
        match ty.properties.iter().find(|p| p.name == def.property) {
            Some(p) if p.ty.starts_with("vector(") => {}
            Some(_) => {
                return Err(ControlPlaneError::Validation(format!(
                    "property `{}` on type `{}` is not a vector type",
                    def.property, def.type_name.0
                )));
            }
            None => {
                return Err(ControlPlaneError::Validation(format!(
                    "type `{}` has no property `{}`",
                    def.type_name.0, def.property
                )));
            }
        }
        ont.vector_indexes
            .insert((def.type_name.0.clone(), def.name.clone()), def);
        Ok(())
    }

    async fn get_vector_index(
        &self,
        type_name: &TypeName,
        name: &str,
    ) -> Result<Option<VectorIndexDef>> {
        Ok(self
            .ontology
            .lock()
            .vector_indexes
            .get(&(type_name.0.clone(), name.to_string()))
            .cloned())
    }

    async fn vector_indexes_for(&self, type_name: &TypeName) -> Result<Vec<VectorIndexDef>> {
        Ok(self
            .ontology
            .lock()
            .vector_indexes
            .values()
            .filter(|d| d.type_name == *type_name)
            .cloned()
            .collect())
    }
```

- [ ] **Step 7: Write the migration.** Create `src/control-plane/postgres/migrations/0020_vector_index_definition.sql`:

```sql
-- A named vector index declared on an object type's vector property. The
-- declaration is the authoritative source of an index's kind/metric/params;
-- the build primitive resolves it and copies it into the iceberg_mirror.vector_index
-- row. Multiple indexes may exist per property, distinguished by name.
create table ontology.vector_index_definition (
    type_name       text    not null references ontology.object_type (name) on delete cascade,
    name            text    not null,
    property_name   text    not null,
    metric          text    not null,
    index_kind      text    not null,
    nlist           integer,
    m               integer,
    ef_construction integer,
    primary key (type_name, name)
);
```

- [ ] **Step 8: Implement in the postgres adapter.** In `src/control-plane/postgres/src/ontology.rs`: add `VectorIndexDef`, `Metric`, `IndexSpec` to the `control_plane_core` import list. Add the three impls inside `impl Ontology for PgControlPlane` (after `get_action`); the casts get a method-level `#[expect]`:

```rust
    #[expect(
        clippy::cast_possible_truncation,
        reason = "index params (nlist/m/ef_construction) are small build-time knobs, well within i32"
    )]
    async fn define_vector_index(&self, def: VectorIndexDef) -> Result<()> {
        let prop_ty: Option<String> = sqlx::query_scalar!(
            "select ty from ontology.property where type_name = $1 and name = $2",
            def.type_name.0,
            def.property,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        match prop_ty {
            Some(t) if t.starts_with("vector(") => {}
            Some(_) => {
                return Err(ControlPlaneError::Validation(format!(
                    "property `{}` on type `{}` is not a vector type",
                    def.property, def.type_name.0
                )));
            }
            None => {
                return Err(ControlPlaneError::Validation(format!(
                    "type `{}` has no property `{}`",
                    def.type_name.0, def.property
                )));
            }
        }
        let (kind, nlist, m, ef) = def.spec.as_cols();
        sqlx::query!(
            "insert into ontology.vector_index_definition \
               (type_name, name, property_name, metric, index_kind, nlist, m, ef_construction) \
             values ($1, $2, $3, $4, $5, $6, $7, $8) \
             on conflict (type_name, name) do update set \
               property_name = excluded.property_name, metric = excluded.metric, \
               index_kind = excluded.index_kind, nlist = excluded.nlist, \
               m = excluded.m, ef_construction = excluded.ef_construction",
            def.type_name.0,
            def.name,
            def.property,
            def.metric.as_str(),
            kind,
            nlist.map(|v| v as i32),
            m.map(|v| v as i32),
            ef.map(|v| v as i32),
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn get_vector_index(
        &self,
        type_name: &TypeName,
        name: &str,
    ) -> Result<Option<VectorIndexDef>> {
        vector_index_def_row(&self.pool, &type_name.0, name).await
    }

    async fn vector_indexes_for(&self, type_name: &TypeName) -> Result<Vec<VectorIndexDef>> {
        let rows = sqlx::query!(
            "select name from ontology.vector_index_definition where type_name = $1",
            type_name.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            if let Some(def) = vector_index_def_row(&self.pool, &type_name.0, &r.name).await? {
                out.push(def);
            }
        }
        Ok(out)
    }
```

Then add the shared free helper at module scope (the `as`-casts get an `#[expect]` covering both truncation and sign-loss):

```rust
/// Read one named vector-index declaration as a `VectorIndexDef`. Shared by the
/// `Ontology::get_vector_index` impl and the build primitive (which has a raw pool).
#[expect(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    reason = "index params are small non-negative build-time knobs persisted as nullable i32"
)]
pub async fn vector_index_def_row(
    pool: &sqlx::PgPool,
    type_name: &str,
    name: &str,
) -> Result<Option<VectorIndexDef>> {
    let row = sqlx::query!(
        "select property_name, metric, index_kind, nlist, m, ef_construction \
         from ontology.vector_index_definition where type_name = $1 and name = $2",
        type_name,
        name,
    )
    .fetch_optional(pool)
    .await
    .map_err(backend)?;
    let Some(r) = row else { return Ok(None) };
    Ok(Some(VectorIndexDef {
        name: name.to_string(),
        type_name: TypeName(type_name.to_string()),
        property: r.property_name,
        metric: r.metric.parse()?,
        spec: IndexSpec::from_label(
            Some(r.index_kind.as_str()),
            r.nlist.map(|v| v as u32),
            r.m.map(|v| v as u32),
            r.ef_construction.map(|v| v as u32),
        )?,
    }))
}
```

- [ ] **Step 9: Regenerate the `.sqlx` cache.** Run `tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; tail -5 /tmp/sqlx.log`. Expect success and new files under `src/control-plane/postgres/.sqlx/`.

- [ ] **Step 10: Extend the testkit contract.** In `src/control-plane/testkit/src/lib.rs`, add `VectorIndexDef`, `Metric`, `IndexSpec` to imports, and append to `ontology_contract` (before its closing `}`):

```rust
    // --- vector index declarations -------------------------------------------
    let doc = ObjectType {
        name: tn("Document"),
        table: tref("main", "document"),
        properties: vec![
            PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
            PropertyDef { name: "embedding".into(), ty: "vector(8)".into(), required: true },
            PropertyDef { name: "title".into(), ty: "Text".into(), required: false },
        ],
        derived: vec![],
        identity: Some("id".into()),
    };
    o.define_type(doc.clone()).await.expect("define Document");

    let by_sim = VectorIndexDef {
        name: "by_sim".into(),
        type_name: tn("Document"),
        property: "embedding".into(),
        metric: Metric::Cosine,
        spec: IndexSpec::Hnsw { m: Some(16), ef_construction: Some(200) },
    };
    let by_cluster = VectorIndexDef {
        name: "by_cluster".into(),
        type_name: tn("Document"),
        property: "embedding".into(),
        metric: Metric::L2,
        spec: IndexSpec::IvfFlat { nlist: Some(4) },
    };
    o.define_vector_index(by_sim.clone()).await.expect("define by_sim");
    o.define_vector_index(by_cluster.clone()).await.expect("define by_cluster");

    assert_eq!(o.get_vector_index(&tn("Document"), "by_sim").await.unwrap(), Some(by_sim.clone()));
    assert_eq!(o.get_vector_index(&tn("Document"), "by_cluster").await.unwrap(), Some(by_cluster.clone()));
    assert_eq!(o.get_vector_index(&tn("Document"), "nope").await.unwrap(), None);

    let mut names: Vec<String> =
        o.vector_indexes_for(&tn("Document")).await.unwrap().into_iter().map(|d| d.name).collect();
    names.sort();
    assert_eq!(names, vec!["by_cluster".to_string(), "by_sim".to_string()]);

    // upsert replaces (exercises as_cols → from_label round-trip on the postgres adapter)
    let by_sim_v2 = VectorIndexDef { metric: Metric::L2, ..by_sim.clone() };
    o.define_vector_index(by_sim_v2.clone()).await.expect("redeclare by_sim");
    assert_eq!(o.get_vector_index(&tn("Document"), "by_sim").await.unwrap(), Some(by_sim_v2));

    // validation: non-vector property and missing property both error
    let bad_prop = VectorIndexDef {
        name: "bad".into(), type_name: tn("Document"), property: "title".into(),
        metric: Metric::Cosine, spec: IndexSpec::Flat,
    };
    assert!(o.define_vector_index(bad_prop).await.is_err(), "non-vector property rejected");
    let missing = VectorIndexDef {
        name: "bad2".into(), type_name: tn("Document"), property: "ghost".into(),
        metric: Metric::Cosine, spec: IndexSpec::Flat,
    };
    assert!(o.define_vector_index(missing).await.is_err(), "missing property rejected");
```

- [ ] **Step 11: Build + run the contract against both adapters.**
```
buck2 test //src/control-plane/memory:ontology //src/control-plane/postgres:ontology //src/control-plane/postgres:sqlx-cache-check > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expect green (the contract runs against memory + postgres). Then `tools/clippy-all.sh > /tmp/c.log 2>&1; tail -3 /tmp/c.log` clean.

- [ ] **Step 12: Commit.**
```bash
git add src/control-plane/core src/control-plane/memory src/control-plane/postgres/migrations/0020_vector_index_definition.sql src/control-plane/postgres/src/ontology.rs src/control-plane/postgres/.sqlx src/control-plane/testkit
git commit -m "feat(ontology): declare named vector indexes (define_vector_index)"
```

---

## Task B: Mirror gains `index_name` (plumbing under a consistent `"default"`)

This task changes the mirror schema + the `lookup`/`insert` signatures and threads a temporary `"default"` index_name through EVERY caller so the tree stays green and self-consistent (builds insert `"default"`, reads look up `"default"`). Task C replaces all of it with real names and removes `"default"` entirely.

**Files:**
- Create: `src/control-plane/postgres/migrations/0021_vector_index_named.sql`
- Modify: `src/control-plane/postgres/src/vector_index.rs` (`VectorIndexRow`, `insert_vector_index`, `lookup_vector_index`, the build-primitive insert)
- Modify: `src/services/engine-serving/src/vector_search.rs` (the lookup caller)
- Modify: every test constructing `VectorIndexRow` or calling `lookup_vector_index` (enumerate via grep, Step 5)
- Modify: `src/control-plane/postgres/.sqlx` (regen)
- Create: `src/control-plane/postgres/tests/vector_index_named.rs` + BUCK target

**Interfaces:**
- Produces:
  - `VectorIndexRow` gains `pub index_name: String` (after `column`).
  - `insert_vector_index(tx, row)` writes `index_name` (now in the PK).
  - `lookup_vector_index(pool, table_id, index_name: &str, at)` — the `column: &str` param becomes `index_name: &str`.

- [ ] **Step 1: Write the migration.** Create `src/control-plane/postgres/migrations/0021_vector_index_named.sql`:

```sql
-- Multiple named vector indexes per column: add index_name and fold it into the
-- mirror PK so N named indexes coexist for one (table_id, column_name, snapshot).
-- Existing rows backfill to 'default'.
alter table iceberg_mirror.vector_index
    add column index_name text not null default 'default';
alter table iceberg_mirror.vector_index
    drop constraint vector_index_pkey;
alter table iceberg_mirror.vector_index
    add primary key (table_id, column_name, index_name, covered_snapshot);
```

- [ ] **Step 2: Extend `VectorIndexRow`.** In `src/control-plane/postgres/src/vector_index.rs`, add `pub index_name: String,` to the struct (after `pub column: String,`).

- [ ] **Step 3: Update `insert_vector_index`.** Add `index_name` to the column list, values, and `on conflict` key:

```rust
    sqlx::query!(
        "insert into iceberg_mirror.vector_index \
         (table_id, column_name, index_name, covered_snapshot, metric, index_kind, dim, row_count, puffin_path) \
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
         on conflict (table_id, column_name, index_name, covered_snapshot) do update set \
             metric = excluded.metric, \
             index_kind = excluded.index_kind, \
             dim = excluded.dim, \
             row_count = excluded.row_count, \
             puffin_path = excluded.puffin_path, \
             created_at = now()",
        row.table_id,
        row.column,
        row.index_name,
        row.covered_snapshot,
        row.metric,
        row.index_kind,
        row.dim,
        row.row_count,
        row.puffin_path,
    )
```

- [ ] **Step 4: Update `lookup_vector_index`.** Change `column: &str` → `index_name: &str`, key the query on `index_name`, and select+map `index_name`:

```rust
pub async fn lookup_vector_index(
    pool: &PgPool,
    table_id: i64,
    index_name: &str,
    at: i64,
) -> Result<Option<VectorIndexRow>> {
    let row = sqlx::query!(
        "select table_id, column_name, index_name, covered_snapshot, metric, index_kind, dim, \
                row_count, puffin_path \
         from iceberg_mirror.vector_index \
         where table_id = $1 and index_name = $2 and covered_snapshot <= $3 \
         order by covered_snapshot desc limit 1",
        table_id,
        index_name,
        at,
    )
    .fetch_optional(pool)
    .await
    .map_err(backend)?;
    Ok(row.map(|r| VectorIndexRow {
        table_id: r.table_id,
        column: r.column_name,
        index_name: r.index_name,
        covered_snapshot: r.covered_snapshot,
        metric: r.metric,
        index_kind: r.index_kind,
        dim: r.dim,
        row_count: r.row_count,
        puffin_path: r.puffin_path,
    }))
}
```

- [ ] **Step 5: Thread `"default"` through EVERY caller.** Run `grep -rn 'lookup_vector_index\|VectorIndexRow {' src` to enumerate. Update each so the tree compiles and stays consistent:
  - `src/control-plane/postgres/src/vector_index.rs` — in `build_vector_index`, the `VectorIndexRow { ... }` passed to `insert_vector_index` gains `index_name: "default".to_string(),` (after `column:`). Add `// TODO(task C): replace "default" with the resolved index_name`.
  - `src/services/engine-serving/src/vector_search.rs` — the lookup at step 3 becomes `lookup_vector_index(pool, table_id, "default", q)`. Add the same TODO.
  - `src/control-plane/postgres/tests/vector_index_mirror.rs` — add `index_name: "default".into(),` to the `VectorIndexRow` literal (~line 35) AND change its `lookup_vector_index(&pool, table_id, <column>, ...)` call to pass `"default"`.
  - `src/control-plane/postgres/tests/vector_index_hnsw.rs` — change its `lookup_vector_index(&pool, table_id, <column>, q)` call to pass `"default"` (the build inserts `"default"`, so the lookup must match).
  - `src/control-plane/postgres/tests/vector_index_build.rs` and `vector_index_ivf.rs` — same shape: each calls the old 8-arg `build_vector_index(…, "embedding", Metric::…, IndexSpec::…, run)` (still compiles in B — its signature is unchanged until Task C) plus a `lookup_vector_index(&pool, table_id, "embedding", …)`; change those lookups to pass `"default"` so they match the build's inserted `"default"`.
  - Any other hit from the grep (e.g. a worker fixture that looks up the mirror): pass `"default"`.

- [ ] **Step 6: Regenerate `.sqlx`.** `tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; tail -5 /tmp/sqlx.log`.

- [ ] **Step 7: Write the named-lookup fixture test.** Create `src/control-plane/postgres/tests/vector_index_named.rs` — boot `PgFixture`, seed a live mirror table (copy the live-table seeding helper verbatim from `tests/vector_index_mirror.rs`), insert two `VectorIndexRow`s differing only by `index_name`, assert `lookup_vector_index` resolves each by name and returns `None` for an unknown name:

```rust
//! Two named index rows coexist per column; lookup_vector_index resolves by name.
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::vector_index::{insert_vector_index, lookup_vector_index, VectorIndexRow};
// + the live-mirror-table seeding helper/imports copied from vector_index_mirror.rs

#[tokio::test]
async fn lookup_resolves_by_index_name() {
    let fx = PgFixture::start().await;
    let pool = fx.pool();
    let table_id = seed_live_table(&pool, "main", "document").await; // verbatim helper
    let mut conn = pool.acquire().await.unwrap();
    for (name, kind) in [("by_sim", "hnsw"), ("by_cluster", "ivf_flat")] {
        insert_vector_index(&mut conn, &VectorIndexRow {
            table_id, column: "embedding".into(), index_name: name.into(),
            covered_snapshot: 1, metric: "cosine".into(), index_kind: kind.into(),
            dim: 8, row_count: 10, puffin_path: format!("/p/{name}.puffin"),
        }).await.unwrap();
    }
    drop(conn);
    assert_eq!(lookup_vector_index(&pool, table_id, "by_sim", 1).await.unwrap().unwrap().index_kind, "hnsw");
    assert_eq!(lookup_vector_index(&pool, table_id, "by_cluster", 1).await.unwrap().unwrap().index_kind, "ivf_flat");
    assert!(lookup_vector_index(&pool, table_id, "nope", 1).await.unwrap().is_none());
}
```

- [ ] **Step 8: Add the BUCK target.** In `src/control-plane/postgres/BUCK`, add a `loom_fixture_test` named `vector-index-named` mirroring the `vector-index-mirror` block.

- [ ] **Step 9: Full build + run affected tests.**
```
buck2 build //src/... > /tmp/b.log 2>&1; tail -5 /tmp/b.log
buck2 test //src/control-plane/postgres:vector-index-named //src/control-plane/postgres:vector-index-mirror //src/control-plane/postgres:vector-index-hnsw //src/control-plane/postgres:sqlx-cache-check > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expect a green `//src/...` build (every caller threads `"default"`) and green tests.

- [ ] **Step 10: Commit.**
```bash
git add src/control-plane/postgres src/services/engine-serving/src/vector_search.rs
git commit -m "feat(mirror): key vector_index by index_name (default placeholder)"
```

---

## Task C: Authoritative build + name-keyed read (atomic)

Build and read flip to `index_name` **together** so a build and the matching search use the same key. This task removes every `"default"` placeholder from Task B and updates ALL signature-change callers in one commit.

**Files:**
- Modify: `src/control-plane/core/src/vector_index_job.rs` (`BuildVectorIndexJob` → `{ schema, name, index_name }`)
- Modify: `src/control-plane/core/tests/vector_index_job.rs` (rewrite for new shape)
- Modify: `src/control-plane/postgres/src/vector_index.rs` (`build_vector_index` resolves declaration; `type_name_for`)
- Modify: `src/services/engine-wire/proto/engine_control.proto`, `src/services/engine-wire/src/client.rs`, `src/services/engine-wire/src/flight.rs`
- Modify: `src/services/engine/src/service.rs`, `src/services/engine/src/flight.rs`
- Modify: `src/services/engine-serving/src/vector_search.rs`
- Modify: `src/services/worker/src/handler.rs`
- Modify (test callers, declare-then-build/search by real name): `src/control-plane/postgres/tests/{vector_index_hnsw,vector_index_mirror,vector_index_build,vector_index_ivf}.rs`, `src/services/worker/tests/build_vector_index.rs`, `src/services/engine-serving/tests/vector_search.rs`, `src/services/engine/tests/vector_search_flight.rs`, `src/services/engine-wire/tests/vector_search_ticket.rs` (if present)
- Modify: `src/control-plane/postgres/.sqlx` (regen, for `type_name_for`)

**Interfaces:**
- Produces:
  - `BuildVectorIndexJob { pub schema: String, pub name: String, pub index_name: String }` (drops `column`/`index_kind`/`nlist`/`m`/`ef_construction` and the `index_spec()` method).
  - `build_vector_index(catalog, pool, table, index_name: &str, run_id) -> Result<BuiltIndex>` (the `column`/`metric`/`index_spec` params are removed).
  - `pub async fn type_name_for(pool: &PgPool, table: &TableRef) -> Result<String>`.
  - `BuildVectorIndexRequest { schema, name, index_name }`; client `build_vector_index(schema, name, index_name) -> Result<(i64, String, i64)>`.
  - `vector_search(catalog, pool, table, index_name: &str, query, k) -> Result<RecordBatch, EngineServingError>` (the `column` param is removed; the hot path uses `row.column`).
  - `VectorSearchTicket { schema, name, index_name, query, k }` (`column` removed).
- Consumes: `vector_index_def_row` (Task A), `lookup`/`insert` (Task B), `identity_column_for` (existing).

- [ ] **Step 1: Enumerate callers.** Run `grep -rn 'build_vector_index(\|BuildVectorIndexJob {\|VectorSearchTicket {\|vector_search(\|\.index_spec()' src`. Every hit must be reconciled in this task. Keep the list; Step 11 re-greps to prove `"default"` is gone.

- [ ] **Step 2: Shrink `BuildVectorIndexJob`.** Replace `src/control-plane/core/src/vector_index_job.rs` body with:

```rust
//! The build-vector-index job contract, shared by the producer (enqueue) and the
//! consumer (worker → engine RPC). Lives in core so a zero-pool worker can read it
//! without the postgres adapter.

/// The queue `kind` for a vector-index build. Protocol invariant, not a tunable.
pub const BUILD_VECTOR_INDEX_JOB_KIND: &str = "build_vector_index";

/// Payload of a `build_vector_index` job. References a named index declaration
/// (`ontology.vector_index_definition`); the build resolves kind/metric/params
/// from the declaration — the job carries no build knobs.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct BuildVectorIndexJob {
    pub schema: String,
    pub name: String,
    pub index_name: String,
}
```

- [ ] **Step 3: Rewrite `core/tests/vector_index_job.rs`.** The old tests asserted `BuildVectorIndexJob::index_spec()` (now removed). Replace them with a serde round-trip of the new shape:

```rust
use control_plane_core::BuildVectorIndexJob;

#[test]
fn job_round_trips() {
    let job = BuildVectorIndexJob { schema: "main".into(), name: "document".into(), index_name: "by_sim".into() };
    let json = serde_json::to_value(&job).unwrap();
    let back: BuildVectorIndexJob = serde_json::from_value(json).unwrap();
    assert_eq!(back.schema, "main");
    assert_eq!(back.name, "document");
    assert_eq!(back.index_name, "by_sim");
}
```

(`IndexSpec::from_label`/`as_cols` are exercised by the Task A testkit contract; they need no test here.)

- [ ] **Step 4: Add `type_name_for` + rewrite `build_vector_index` head.** In `src/control-plane/postgres/src/vector_index.rs`, add near `identity_column_for`:

```rust
/// Resolve the ontology type name backing `(table.schema, table.name)`.
pub async fn type_name_for(pool: &PgPool, table: &TableRef) -> Result<String> {
    sqlx::query_scalar!(
        "select name from ontology.object_type where table_schema = $1 and table_name = $2",
        table.schema,
        table.name,
    )
    .fetch_optional(pool)
    .await
    .map_err(backend)?
    .ok_or_else(|| ControlPlaneError::NotFound(format!("type for {}.{}", table.schema, table.name)))
}
```

Change the `build_vector_index` signature: replace params `column: &str, metric: Metric, index_spec: IndexSpec,` with `index_name: &str,`, and remove the `#[allow(clippy::too_many_arguments)]` attribute (now 5 args). At the top of the body, after capturing `s`/`at`, resolve the declaration BEFORE the data reads:

```rust
    // Resolve the named declaration (authoritative source of column/metric/spec).
    let type_name = type_name_for(pool, table).await?;
    let def = crate::ontology::vector_index_def_row(pool, &type_name, index_name)
        .await?
        .ok_or_else(|| {
            ControlPlaneError::NotFound(format!(
                "no vector index definition `{index_name}` on type `{type_name}`"
            ))
        })?;
    let column: &str = &def.property;
    let metric = def.metric;
    let index_spec = def.spec;
```

(`vector_index_def_row` is the Task A free helper in `crate::ontology`. The rest of the body — identity resolution, cold/hot reads, `match index_spec` build, Puffin write — is **UNCHANGED**, since `column`/`metric`/`index_spec` now exist as locals.) Finally, replace Task B's temporary `index_name: "default".to_string(),` in the `insert_vector_index` call with `index_name: index_name.to_string(),`.

- [ ] **Step 5: Shrink the proto + client.** In `src/services/engine-wire/proto/engine_control.proto`, replace the message:

```proto
message BuildVectorIndexRequest  { string schema = 1; string name = 2; string index_name = 3; }
```

In `src/services/engine-wire/src/client.rs`, replace `build_vector_index`:

```rust
    /// Build (or rebuild) the named vector index declared for `(schema, name)`.
    /// Kind/metric/params are resolved engine-side from the ontology declaration.
    pub async fn build_vector_index(
        &self,
        schema: String,
        name: String,
        index_name: String,
    ) -> Result<(i64, String, i64)> {
        let resp = self
            .inner
            .clone()
            .build_vector_index(pb::BuildVectorIndexRequest { schema, name, index_name })
            .await
            .map_err(be)?
            .into_inner();
        Ok((resp.covered_snapshot, resp.puffin_path, resp.row_count))
    }
```

- [ ] **Step 6: Rewrite the engine RPC handler.** In `src/services/engine/src/service.rs`, replace the `build_vector_index` handler body (remove `IndexSpec`/`Metric` use here):

```rust
    async fn build_vector_index(
        &self,
        req: Request<pb::BuildVectorIndexRequest>,
    ) -> std::result::Result<Response<pb::BuildVectorIndexResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef { schema: r.schema, name: r.name };
        let built = control_plane_postgres::vector_index::build_vector_index(
            &self.catalog,
            &self.pool,
            &table,
            &r.index_name,
            RunId(uuid::Uuid::new_v4()),
        )
        .await
        .map_err(status)?;
        Ok(Response::new(pb::BuildVectorIndexResponse {
            covered_snapshot: built.covered_snapshot,
            puffin_path: built.puffin_path,
            row_count: built.row_count,
        }))
    }
```

(Delete any now-unused `IndexSpec`/`Metric` imports in `service.rs`.)

- [ ] **Step 7: Shrink the worker handler.** In `src/services/worker/src/handler.rs`, replace the destructure + call in `handle_build_vector_index`:

```rust
    let BuildVectorIndexJob { schema, name, index_name } =
        serde_json::from_value(job.payload).map_err(|e| JobFailure {
            error: format!("bad build_vector_index payload: {e}"),
            policy: RetryPolicy::Abandon,
        })?;
    client
        .build_vector_index(schema, name, index_name)
        .await
        .map_err(|e| JobFailure {
            error: e.to_string(),
            policy: RetryPolicy::Retry { delay: tuning.backoff(job.attempts) },
        })?;
    Ok(())
```

- [ ] **Step 8: Flip the read path.** In `src/services/engine-wire/src/flight.rs`, replace `VectorSearchTicket`'s `column` field with `index_name`:

```rust
pub struct VectorSearchTicket {
    pub schema: String,
    pub name: String,
    pub index_name: String,
    pub query: Vec<f32>,
    pub k: u32,
}
```

In `src/services/engine-serving/src/vector_search.rs`, change `column: &str` → `index_name: &str`; replace the Task B `lookup_vector_index(pool, table_id, "default", q)` with `lookup_vector_index(pool, table_id, index_name, q)`; bind `let column: &str = &row.column;` right after the `row` is resolved (the hot path uses it); update the `NoIndex` message to name the index:

```rust
        .ok_or_else(|| {
            EngineServingError::NoIndex(format!(
                "no vector index `{}` on {}.{} at snapshot {}",
                index_name, table.schema, table.name, q
            ))
        })?;
    let column: &str = &row.column;
```

In `src/services/engine/src/flight.rs`, `do_get_vector_search` passes `&vs.index_name` instead of `&vs.column`.

- [ ] **Step 9: Update ALL test callers (declare → build → search by real name).** Using the Step 1 list, update each. The template for any build/search fixture: `define_type` the type (table = the landed table, with a `vector(N)` property + `identity`), `define_vector_index("<name>", …)`, then `build_vector_index(&catalog, &pool, &table, "<name>", RunId(…))` and (for read tests) `vector_search(&catalog, &pool, &table, "<name>", &q, k)`. Specifically:
  - `src/control-plane/postgres/tests/vector_index_hnsw.rs`, `vector_index_mirror.rs`, `vector_index_build.rs`, `vector_index_ivf.rs` — replace the old 8-arg `build_vector_index` + `"default"` lookups with declare-then-build-by-name; lookups use that name.
  - `src/services/engine-serving/tests/vector_search.rs` — `seed_and_build_*` helpers declare a named index, build by name; searches pass the name. Keep the cold∪hot freshness assertions.
  - `src/services/worker/tests/build_vector_index.rs` — `make_build_vector_index_job*` helpers build `BuildVectorIndexJob { schema, name, index_name }`; the e2e declares the index then drives the job; assert the mirror row's `index_name`/`index_kind`.
  - `src/services/engine/tests/vector_search_flight.rs` — the old primitive call → declare-then-build-by-name; `VectorSearchTicket { …, index_name, … }` (no `column`).
  - `src/services/engine-wire/tests/vector_search_ticket.rs` (if present) — construct the ticket with `index_name`; assert it round-trips.

- [ ] **Step 10: Add the negative-path acceptance (unknown index name fails the build).** In `src/services/worker/tests/build_vector_index.rs` (or the engine-serving build fixture), add a test that enqueues/drives a `build_vector_index` for an `index_name` with NO declaration and asserts the build errors (the job fails / the primitive returns `NotFound`). This is the spec's "an unknown `index_name` fails the build" acceptance.

- [ ] **Step 11: Regenerate `.sqlx`, full build, prove `"default"` is gone, run tests.**
```
tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; tail -5 /tmp/sqlx.log
buck2 build //src/... > /tmp/b.log 2>&1; tail -5 /tmp/b.log
grep -rn '"default"' src/control-plane/postgres/src/vector_index.rs src/services/engine-serving/src/vector_search.rs && echo "LEFTOVER PLACEHOLDER" || echo "clean"
buck2 test //src/control-plane/postgres:vector-index-hnsw //src/control-plane/postgres:vector-index-mirror //src/services/engine-serving:vector-search //src/services/worker:build-vector-index //src/services/engine:vector-search-flight //src/control-plane/core:vector-index-job > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expect: green build, "clean" (no leftover placeholder), green tests. (Use the real BUCK target names — confirm with `grep -n 'name = ' src/services/worker/BUCK src/services/engine/BUCK`.)

- [ ] **Step 12: Commit.**
```bash
git add -A
git commit -m "feat(vector): authoritative build + name-keyed read by index_name"
```

---

## Task D: Headline acceptance — multiple indexes per property

**Files:**
- Create: `src/control-plane/postgres/tests/vector_index_multi.rs` + BUCK target `vector-index-multi`

**Interfaces:**
- Consumes: `define_type`/`define_vector_index` (postgres `Ontology`), `build_vector_index(…, index_name, …)` (Task C), `lookup_vector_index(…, index_name, …)` (Task B), `read_vector_index` + `VectorIndex::search` (existing).

- [ ] **Step 1: Write the acceptance test.** Create `src/control-plane/postgres/tests/vector_index_multi.rs`. Boot `PgFixture`; land a `document(id long, embedding vector(8))` table (reuse the `columns()`/`ipc_body()`/`land(...)` pattern from `tests/vector_index_hnsw.rs`); `define_type` Document bound to that table (`embedding: vector(8)`, `identity: Some("id")`); declare **two** indexes on `embedding`:

```rust
    cp.define_vector_index(VectorIndexDef {
        name: "by_sim".into(), type_name: TypeName("Document".into()),
        property: "embedding".into(), metric: Metric::Cosine,
        spec: IndexSpec::Hnsw { m: Some(16), ef_construction: Some(200) },
    }).await.unwrap();
    cp.define_vector_index(VectorIndexDef {
        name: "by_cluster".into(), type_name: TypeName("Document".into()),
        property: "embedding".into(), metric: Metric::L2,
        spec: IndexSpec::IvfFlat { nlist: Some(4) },
    }).await.unwrap();
```

Build BOTH by name:

```rust
    build_vector_index(&catalog, &pool, &table, "by_sim", RunId(uuid::Uuid::new_v4())).await.unwrap();
    build_vector_index(&catalog, &pool, &table, "by_cluster", RunId(uuid::Uuid::new_v4())).await.unwrap();
```

- [ ] **Step 2: Assert two distinct mirror rows + correct kinds/metrics.** Resolve `table_id` (the live-mirror helper), then:

```rust
    let sim = lookup_vector_index(&pool, table_id, "by_sim", i64::MAX).await.unwrap().unwrap();
    assert_eq!(sim.index_kind, "hnsw");
    assert_eq!(sim.metric, "cosine");
    let clus = lookup_vector_index(&pool, table_id, "by_cluster", i64::MAX).await.unwrap().unwrap();
    assert_eq!(clus.index_kind, "ivf_flat");
    assert_eq!(clus.metric, "l2");
    assert_ne!(sim.puffin_path, clus.puffin_path, "each named index has its own blob");
```

- [ ] **Step 3: Assert each decodes + searches independently.** For each row, `read_vector_index(&file_io, &row.puffin_path)` then `.search(&query, 1)` returns the expected nearest id (seed the data so a known id is nearest). Mirror the decode+search assertion in `tests/vector_index_hnsw.rs`.

- [ ] **Step 4: Add the BUCK target** `vector-index-multi` in `src/control-plane/postgres/BUCK` (mirror `vector-index-hnsw`).

- [ ] **Step 5: Run it.**
```
buck2 test //src/control-plane/postgres:vector-index-multi > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expect green: two indexes, two blobs, two independent searches.

- [ ] **Step 6: Commit.**
```bash
git add src/control-plane/postgres/tests/vector_index_multi.rs src/control-plane/postgres/BUCK
git commit -m "test(vector): two named indexes on one property build and search independently"
```

---

## Final verification (after all tasks)

- [ ] Full sweep: `buck2 build //src/... > /tmp/b.log 2>&1; tail -3 /tmp/b.log` then `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|TESTS FAILED" /tmp/t.log`. Both green (re-run any flaky hermetic-postgres fixture in isolation to confirm).
- [ ] `tools/clippy-all.sh > /tmp/c.log 2>&1; tail -3 /tmp/c.log` — clean.
- [ ] No leftover `"default"` index_name placeholders; `BuildVectorIndexJob`/the proto carry no kind/params; `build_vector_index` takes `index_name`, not `column`/`metric`/`index_spec`; `vector_search`/`VectorSearchTicket` take `index_name`, not `column`.
- [ ] `loom-docs-update`: close `road-ontology-vector-index-def` (`- [ ]`→`- [x]`, `status:done`, add `pr:#N`); record any genuinely-deferred follow-on in FUTURE.
