# Actions — Part 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add named ontology **Actions** (insert-only): a governed `POST /actions/{name}` that inserts a new typed object via a low-latency **DuckLake inline write** behind a swappable `ActionEngine` trait, enforcing `Action::Write`.

**Architecture:** A new ontology `ActionDef` (name → target type + typed params) stored in Postgres; a write-capable `ActionEngine` trait whose `EmbeddedDuckDbWriter` impl ATTACHes DuckLake with inlining enabled and runs a parameterized INSERT; a query-api handler that resolves the action, enforces a `Write` ACL, parses typed params, inserts, and emits best-effort type-named lineage. The created object reads back through the existing read path.

**Tech Stack:** Rust 2024, buck2, axum, DuckDB-over-DuckLake (`duckdb` crate), Postgres control plane (sqlx compile-time SQL), `loom_fixture_test` (Postgres+DuckDB). Tests are `rust_test`/`loom_fixture_test` targets — never inline `#[cfg(test)]`.

**Spec:** `docs/superpowers/specs/2026-06-15-actions-part1-design.md`

**Two refinements over the spec (impl-level):**
1. Action parameters are stored in a **normalized `ontology.action_param` table** (mirroring `ontology.property`), not JSONB — matches the existing adapter pattern and needs no serde on core types.
2. `ActionEngine::insert_row` returns `Result<(), ActionEngineError>`; the handler reads the new snapshot id via `cp.catalog().current_snapshot(&table)` for best-effort lineage (reuses existing code; keeps the writer backend-focused / Iceberg-swappable). The spike (Task 1) confirms `current_snapshot` reflects an inline write.

---

## File Structure

| File | Responsibility | Action |
|------|----------------|--------|
| `src/services/query-api/tests/inline_write_spike.rs` | Decision-E spike: inline INSERT writes no Parquet, reads reconcile, snapshot obtainable | Create |
| `src/control-plane/core/src/ontology.rs` | `ActionName`/`ParamDef`/`ActionDef` + `define_action`/`get_action` on `Ontology` | Modify |
| `src/control-plane/core/src/lib.rs` | Export the new ontology symbols | Modify |
| `src/control-plane/postgres/migrations/00NN_actions.sql` | `ontology.action` + `ontology.action_param` tables | Create |
| `src/control-plane/postgres/src/ontology.rs` | `define_action`/`get_action` adapter impl | Modify |
| `src/control-plane/postgres/.sqlx/` | regenerated sqlx cache | Modify (generated) |
| `src/control-plane/memory/src/ontology.rs` | `define_action`/`get_action` fake impl | Modify |
| `src/control-plane/testkit/src/lib.rs` | `action_contract` + fold into `ontology_contract` | Modify |
| `src/services/query-api/src/serving.rs` | `ActionEngine` trait + `EmbeddedDuckDbWriter` | Modify |
| `src/services/query-api/src/params.rs` | typed JSON params → `Vec<(column, SqlValue)>` | Create |
| `src/services/query-api/src/action.rs` | `run_action` handler logic + `ActionDeps` + `ActionError` | Create |
| `src/services/query-api/src/http.rs` | `POST /actions/:name` route + `AppState.action_engine` | Modify |
| `src/services/query-api/src/lib.rs` | module wiring | Modify |
| `src/services/query-api/src/main.rs` | build `EmbeddedDuckDbWriter`, wire into `AppState` | Modify |
| `src/services/query-api/tests/action_e2e.rs` | e2e: define → grant → POST → read-back + lineage + inline; + Write-403 | Create |
| `src/services/query-api/BUCK` | new test targets | Modify |
| `docs/FUTURE.md`, roadmap | follow-ups + delivered marker | Modify |

---

## Task 1: Inline-write spike (Decision E — de-risk first)

Prove the DuckLake inline-write mechanism BEFORE building anything on it: a single-row INSERT with inlining enabled writes no Parquet file, the row reads back through the read-side ATTACH config, and the new snapshot is visible via the catalog.

**Files:**
- Create: `src/services/query-api/tests/inline_write_spike.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Add the BUCK target**

Append to `src/services/query-api/BUCK`:

```python
loom_fixture_test(
    name = "inline-write-spike",
    crate = "inline_write_spike",
    srcs = ["tests/inline_write_spike.rs"],
    crate_root = "tests/inline_write_spike.rs",
    duckdb = True,
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:duckdb",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Write the spike test**

Create `src/services/query-api/tests/inline_write_spike.rs`. It uses a RAW `duckdb` connection for the inline write (Task 4 turns this into `EmbeddedDuckDbWriter`). The ATTACH preamble mirrors `EmbeddedDuckDb::attach` but with `DATA_INLINING_ROW_LIMIT 100` (inlining ON):

```rust
//! Decision-E spike: prove DuckLake inline writes. A single-row INSERT with
//! DATA_INLINING_ROW_LIMIT > 0 must (1) write NO Parquet data file, (2) be readable
//! through the read-side ATTACH (DATA_INLINING_ROW_LIMIT 0), and (3) produce a new
//! catalog snapshot visible via Catalog::current_snapshot. Gates the rest of Actions.

use control_plane_core::{Catalog, ControlPlane, TableRef};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};

fn preamble(ext_dir: &str, pg_conn: &str, data_path: &std::path::Path, inline_limit: u32) -> String {
    format!(
        "SET extension_directory='{ext_dir}';\nLOAD ducklake;\nLOAD postgres_scanner;\n\
         ATTACH 'ducklake:postgres:{pg_conn}' AS lake \
         (DATA_PATH '{}/', DATA_INLINING_ROW_LIMIT {inline_limit});\nUSE lake;",
        data_path.display(),
    )
}

fn parquet_count(dir: &std::path::Path) -> usize {
    fn walk(dir: &std::path::Path, n: &mut usize) {
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, n);
                } else if p.extension().is_some_and(|x| x == "parquet") {
                    *n += 1;
                }
            }
        }
    }
    let mut n = 0;
    walk(dir, &mut n);
    n
}

#[tokio::test(flavor = "multi_thread")]
async fn inline_insert_writes_no_parquet_and_reads_back() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;

    // Create an empty table (0 batches => no Parquet written by seed).
    writer
        .seed(
            "main",
            "widget",
            &[
                ("id".into(), "BIGINT".into(), false),
                ("name".into(), "VARCHAR".into(), true),
            ],
            &[],
        )
        .await;
    let data_path = writer.data_path().to_path_buf();
    assert_eq!(parquet_count(&data_path), 0, "table created, no Parquet yet");

    let ext_dir = std::env::var("DUCKDB_EXTENSION_DIR").expect("DUCKDB_EXTENSION_DIR");
    let pg_conn = format!("dbname={db} host={} user=postgres", fx.socket_path().display());

    // INLINE write: ATTACH with inlining ON, INSERT one row.
    let attach_w = preamble(&ext_dir, &pg_conn, &data_path, 100);
    tokio::task::spawn_blocking(move || {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&attach_w).unwrap();
        conn.execute("INSERT INTO main.widget (id, name) VALUES (?, ?)", duckdb::params![1_i64, "a"])
            .unwrap();
    })
    .await
    .unwrap();

    // (1) No Parquet file was written — the row is inline.
    assert_eq!(parquet_count(&data_path), 0, "inline insert wrote no Parquet file");

    // (2) Read back through the READ-side config (inlining disabled) — must see the row.
    let attach_r = preamble(&ext_dir, &pg_conn, &data_path, 0);
    let count = tokio::task::spawn_blocking(move || {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&attach_r).unwrap();
        conn.query_row("SELECT count(*) FROM main.widget", [], |r| r.get::<_, i64>(0))
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(count, 1, "read-side ATTACH reconciles the inlined row");

    // (3) The new snapshot is visible via the catalog (the handler reads this for lineage).
    let snap = cp
        .catalog()
        .current_snapshot(&TableRef { schema: "main".into(), name: "widget".into() })
        .await
        .expect("current_snapshot after inline insert");
    assert!(snap.id.0 > 0, "inline insert advanced the catalog snapshot");
}
```

- [ ] **Step 3: Run the spike**

Run: `buck2 test //src/services/query-api:inline-write-spike > /tmp/spike.log 2>&1; grep -E "Tests finished|FAIL|panicked|assertion" /tmp/spike.log`
Expected: `Tests finished: Pass 1. Fail 0.`

**If it fails, STOP and report — this is the design's load-bearing assumption.** Common adjustments (still within Decision E): if the read-side (`DATA_INLINING_ROW_LIMIT 0`) does NOT see inlined rows, the read engine's ATTACH must change (note it for Task 6/7); if `current_snapshot` doesn't reflect the write, the writer must surface the snapshot id itself (revisit Task 4's trait return). Report which adjustment is needed rather than weakening the assertions.

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api/tests/inline_write_spike.rs src/services/query-api/BUCK
git commit -m "test(query-api): spike DuckLake inline write (no Parquet, reads reconcile, snapshot visible)"
```

---

## Task 2: Ontology `ActionDef` — core types, trait methods, postgres + memory impls

Add the `ActionDef` ontology concept and its two storage adapters. The trait-method addition forces both adapters to implement, so they land together.

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs`, `src/control-plane/core/src/lib.rs`
- Create: `src/control-plane/postgres/migrations/00NN_actions.sql`
- Modify: `src/control-plane/postgres/src/ontology.rs`, `src/control-plane/memory/src/ontology.rs`
- Modify (generated): `src/control-plane/postgres/.sqlx/`

- [ ] **Step 1: Add core types + trait methods**

In `src/control-plane/core/src/ontology.rs`, after the `LinkDef` struct (before the `Ontology` trait), add:

```rust
/// A named ontology action (e.g. "createCustomer").
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ActionName(pub String);

/// A typed input to an action. `ty` is the ontology's logical vocabulary (like `PropertyDef.ty`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParamDef {
    pub name: String,
    pub ty: String,
    pub required: bool,
}

/// A named ontology operation. Part-1 semantics: insert one new instance of `target`,
/// taking a value for each parameter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionDef {
    pub name: ActionName,
    pub target: TypeName,
    /// Ordered.
    pub parameters: Vec<ParamDef>,
}
```

Add two methods to the `Ontology` trait (after `resolve`):

```rust
    /// Create or replace a named action and its ordered parameter list. Upsert.
    async fn define_action(&self, action: ActionDef) -> Result<()>;
    /// Fetch an action by name. `NotFound` if absent.
    async fn get_action(&self, name: &ActionName) -> Result<ActionDef>;
```

In `src/control-plane/core/src/lib.rs`, change the ontology re-export to add the new symbols:

```rust
pub use ontology::{
    ActionDef, ActionName, Cardinality, LinkBacking, LinkDef, ObjectType, Ontology, ParamDef,
    PropertyDef, TypeName,
};
```

- [ ] **Step 2: Add the postgres migration**

List `src/control-plane/postgres/migrations/` and pick the next sequential number (the ontology one is `0002_ontology.sql`; use the next unused `00NN`). Create `src/control-plane/postgres/migrations/00NN_actions.sql`:

```sql
create table ontology.action (
    name        text primary key,
    target_type text not null references ontology.object_type (name) on delete cascade
);

create table ontology.action_param (
    action_name text    not null references ontology.action (name) on delete cascade,
    ordinal     int     not null,
    name        text    not null,
    ty          text    not null,
    required    boolean not null,
    primary key (action_name, ordinal)
);
```

- [ ] **Step 3: Implement in the postgres adapter**

In `src/control-plane/postgres/src/ontology.rs`, add to the `impl Ontology for PgControlPlane` block (mirror the verbatim style of `define_type`/`get_type` already in this file — same `sqlx::query!`, same `backend` error map, same delete-then-reinsert for the ordered child rows):

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_action(&self, action: ActionDef) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(backend)?;
        sqlx::query!(
            "insert into ontology.action (name, target_type) values ($1, $2) \
             on conflict (name) do update set target_type = excluded.target_type",
            action.name.0,
            action.target.0,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sqlx::query!(
            "delete from ontology.action_param where action_name = $1",
            action.name.0,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        for (i, p) in action.parameters.iter().enumerate() {
            sqlx::query!(
                "insert into ontology.action_param (action_name, ordinal, name, ty, required) \
                 values ($1, $2, $3, $4, $5)",
                action.name.0,
                i as i32,
                p.name,
                p.ty,
                p.required,
            )
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn get_action(&self, name: &ActionName) -> Result<ActionDef> {
        let row = sqlx::query!(
            "select target_type from ontology.action where name = $1",
            name.0,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))?;
        let params = sqlx::query!(
            "select name, ty, required from ontology.action_param \
             where action_name = $1 order by ordinal",
            name.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(ActionDef {
            name: name.clone(),
            target: TypeName(row.target_type),
            parameters: params
                .into_iter()
                .map(|r| ParamDef {
                    name: r.name,
                    ty: r.ty,
                    required: r.required,
                })
                .collect(),
        })
    }
```

Add `ActionDef`, `ActionName`, `ParamDef` to this file's `use control_plane_core::{...}` import line.

- [ ] **Step 4: Regenerate the sqlx cache**

The new `query!` macros must be added to the committed `.sqlx` cache (a build + the `sqlx-cache-check` test require it):

Run: `tools/sqlx-prepare.sh`
Then verify: `git status src/control-plane/postgres/.sqlx | head` shows new/changed `query-*.json` files.

- [ ] **Step 5: Implement in the memory fake**

In `src/control-plane/memory/src/ontology.rs`: add `actions: HashMap<String, ActionDef>` to `OntologyState` (the `#[derive(Default)]` struct), and add the two methods to the `impl Ontology` block (mirror `define_type`/`get_type`):

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_action(&self, action: ActionDef) -> Result<()> {
        self.ontology
            .lock()
            .unwrap()
            .actions
            .insert(action.name.0.clone(), action);
        Ok(())
    }

    async fn get_action(&self, name: &ActionName) -> Result<ActionDef> {
        self.ontology
            .lock()
            .unwrap()
            .actions
            .get(&name.0)
            .cloned()
            .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))
    }
```

Add `ActionDef`, `ActionName` to this file's `use control_plane_core::{...}` import line (`ParamDef` only if referenced).

- [ ] **Step 6: Build + lint + commit**

Run: `buck2 build //src/control-plane/... 2>&1 | tail -10` — expect clean (both adapters compile against the new trait).
Run: `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' '//src/control-plane/memory:memory[clippy.txt]' '//src/control-plane/core:core[clippy.txt]' 2>&1 | tail -5` — expect empty.

```bash
git add src/control-plane/core/src/ontology.rs src/control-plane/core/src/lib.rs \
  src/control-plane/postgres/migrations/ src/control-plane/postgres/src/ontology.rs \
  src/control-plane/postgres/.sqlx/ src/control-plane/memory/src/ontology.rs
git commit -m "feat(control-plane): ontology ActionDef — define_action/get_action (pg + memory)"
```

---

## Task 3: Testkit `action_contract`

Prove the new adapter methods round-trip identically on both backends, via the shared contract harness.

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs`

- [ ] **Step 1: Add the contract assertions**

In `src/control-plane/testkit/src/lib.rs`, inside the existing `ontology_contract<O: Ontology>(o: &O)` function (the harness both adapters already run), append (reuse its existing `tn`/`tref` helpers; define a local `an` helper for `ActionName`):

```rust
    // --- Actions ---
    // The target type must exist (FK in the pg adapter).
    o.define_type(ObjectType {
        name: tn("Widget"),
        table: tref("main", "widget"),
        properties: vec![
            PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
            PropertyDef { name: "name".into(), ty: "String".into(), required: false },
        ],
    })
    .await
    .expect("define Widget");

    let create_widget = ActionDef {
        name: ActionName("createWidget".into()),
        target: tn("Widget"),
        parameters: vec![
            ParamDef { name: "id".into(), ty: "Long".into(), required: true },
            ParamDef { name: "name".into(), ty: "String".into(), required: false },
        ],
    };
    o.define_action(create_widget.clone()).await.expect("define action");
    assert_eq!(
        o.get_action(&ActionName("createWidget".into())).await.unwrap(),
        create_widget,
        "action round-trips"
    );
    assert_eq!(
        o.get_action(&ActionName("createWidget".into()))
            .await
            .unwrap()
            .parameters
            .iter()
            .map(|p| p.name.clone())
            .collect::<Vec<_>>(),
        vec!["id".to_string(), "name".to_string()],
        "parameter order preserved"
    );
    // Upsert replaces the parameter list.
    o.define_action(ActionDef {
        name: ActionName("createWidget".into()),
        target: tn("Widget"),
        parameters: vec![ParamDef { name: "id".into(), ty: "Long".into(), required: true }],
    })
    .await
    .expect("redefine action");
    assert_eq!(
        o.get_action(&ActionName("createWidget".into())).await.unwrap().parameters.len(),
        1,
        "redefine replaces parameters"
    );
    // Unknown action -> NotFound.
    assert!(matches!(
        o.get_action(&ActionName("nope".into())).await,
        Err(ControlPlaneError::NotFound(_))
    ));
```

Add `ActionDef`, `ActionName`, `ParamDef` to the testkit's `use control_plane_core::{...}` imports, and `ControlPlaneError` if not already present. Add a local helper near the existing `tn`:

```rust
    fn an(s: &str) -> ActionName { ActionName(s.into()) }
```

(If `tn`/`tref` are closures/fns already in scope, mirror their definition style; if `an` is unused after using `ActionName(...)` inline, omit it.)

- [ ] **Step 2: Run the contract against both adapters**

Run: `buck2 test //src/control-plane/memory:ontology //src/control-plane/postgres:ontology > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: `Tests finished: Pass 2. Fail 0.` (the memory and postgres ontology contract tests, now including actions).

- [ ] **Step 3: Commit**

```bash
git add src/control-plane/testkit/src/lib.rs
git commit -m "test(testkit): action define/get round-trip contract (both adapters)"
```

---

## Task 4: `ActionEngine` trait + `EmbeddedDuckDbWriter`

The write seam: a trait + a DuckLake-inline impl, using the mechanism the spike proved.

**Files:**
- Modify: `src/services/query-api/src/serving.rs`
- Modify: `src/services/query-api/BUCK` (a fixture test)
- Create: `src/services/query-api/tests/action_engine.rs`

- [ ] **Step 1: Add the trait + impl to `serving.rs`**

In `src/services/query-api/src/serving.rs`, add (the `to_duck` converter and `SqlValue` already exist in this file; reuse them):

```rust
/// A write-capable serving engine — the seam a future Iceberg backend swaps in.
#[async_trait]
pub trait ActionEngine: Send + Sync {
    /// Insert one row of `values` into `table` (columns positionally aligned), INLINE —
    /// low-latency, no Parquet data file. The new catalog snapshot is read separately by
    /// the caller via `Catalog::current_snapshot`.
    async fn insert_row(
        &self,
        table: &control_plane_core::TableRef,
        columns: &[String],
        values: &[SqlValue],
    ) -> Result<(), ServingError>;
}

/// DuckLake inline writer: same ATTACH as `EmbeddedDuckDb` but with inlining ENABLED.
pub struct EmbeddedDuckDbWriter {
    attach_sql: String,
}

impl EmbeddedDuckDbWriter {
    /// Inlining threshold: rows per write below this land inline (no Parquet). Single-row
    /// action writes are always well under it.
    const INLINE_ROW_LIMIT: u32 = 1000;

    pub async fn attach(
        pg_conn: &str,
        data_path: &std::path::Path,
    ) -> Result<Self, ServingError> {
        let ext_dir = std::env::var("DUCKDB_EXTENSION_DIR")
            .map_err(|_| ServingError::Engine("DUCKDB_EXTENSION_DIR unset".into()))?;
        let attach_sql = format!(
            "SET extension_directory='{}';\nLOAD ducklake;\nLOAD postgres_scanner;\n\
             ATTACH 'ducklake:postgres:{}' AS lake \
             (DATA_PATH '{}/', DATA_INLINING_ROW_LIMIT {});\nUSE lake;",
            ext_dir,
            pg_conn,
            data_path.display(),
            Self::INLINE_ROW_LIMIT,
        );
        Ok(Self { attach_sql })
    }
}

#[async_trait]
impl ActionEngine for EmbeddedDuckDbWriter {
    async fn insert_row(
        &self,
        table: &control_plane_core::TableRef,
        columns: &[String],
        values: &[SqlValue],
    ) -> Result<(), ServingError> {
        // Identifiers come from the ontology (validated table/columns), not user input;
        // values bind as positional params. Quote identifiers to preserve case.
        let cols = columns
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = std::iter::repeat("?").take(values.len()).collect::<Vec<_>>().join(", ");
        let sql = format!(
            "INSERT INTO \"{}\".\"{}\" ({}) VALUES ({})",
            table.schema.replace('"', "\"\""),
            table.name.replace('"', "\"\""),
            cols,
            placeholders,
        );
        let attach = self.attach_sql.clone();
        let params = values.to_vec();
        tokio::task::spawn_blocking(move || -> Result<(), ServingError> {
            let conn = duckdb::Connection::open_in_memory()
                .map_err(|e| ServingError::Engine(e.to_string()))?;
            conn.execute_batch(&attach).map_err(|e| ServingError::Engine(e.to_string()))?;
            let bound: Vec<duckdb::types::Value> = params.iter().map(to_duck).collect();
            let pref: Vec<&dyn duckdb::ToSql> =
                bound.iter().map(|v| v as &dyn duckdb::ToSql).collect();
            conn.execute(&sql, pref.as_slice())
                .map_err(|e| ServingError::Engine(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| ServingError::Engine(format!("join: {e}")))?
    }
}
```

If `serving.rs` doesn't already `use control_plane_core::TableRef`, reference it fully-qualified as written above (it's used inline). Confirm `to_duck` is in scope (it's a module fn in this file).

- [ ] **Step 2: Export from `lib.rs`**

In `src/services/query-api/src/lib.rs`, ensure the serving re-export includes the new types (mirror the existing `pub use serving::{...}` or `pub mod serving;` — if the module is `pub`, no change needed; if items are re-exported, add `ActionEngine, EmbeddedDuckDbWriter`).

- [ ] **Step 3: Write a fixture test for the writer**

Create `src/services/query-api/tests/action_engine.rs`:

```rust
//! EmbeddedDuckDbWriter inserts a row inline; it reads back through the read engine.

use control_plane_core::TableRef;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use query_api::serving::{ActionEngine, EmbeddedDuckDb, EmbeddedDuckDbWriter, ServingEngine, SqlValue};

#[tokio::test(flavor = "multi_thread")]
async fn writer_inserts_a_row_read_back_by_the_reader() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let writer_fx = DuckLakeWriter::new(fx.socket_path(), &db);
    writer_fx.bootstrap().await;
    writer_fx
        .seed(
            "main",
            "widget",
            &[
                ("id".into(), "BIGINT".into(), false),
                ("name".into(), "VARCHAR".into(), true),
            ],
            &[],
        )
        .await;

    let pg_conn = format!("dbname={db} host={} user=postgres", fx.socket_path().display());
    let data_path = writer_fx.data_path();

    let engine = EmbeddedDuckDbWriter::attach(&pg_conn, data_path).await.unwrap();
    engine
        .insert_row(
            &TableRef { schema: "main".into(), name: "widget".into() },
            &["id".to_string(), "name".to_string()],
            &[SqlValue::Int(7), SqlValue::Text("hi".into())],
        )
        .await
        .unwrap();

    let reader = EmbeddedDuckDb::attach(&pg_conn, data_path).await.unwrap();
    let rows = reader
        .fetch_rows("SELECT id, name FROM main.widget", &[])
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 1, "the inline-written row reads back");
    assert_eq!(rows.rows[0][0], SqlValue::Int(7));
    assert_eq!(rows.rows[0][1], SqlValue::Text("hi".into()));
}
```

Append the BUCK target:

```python
loom_fixture_test(
    name = "action-engine",
    crate = "action_engine",
    srcs = ["tests/action_engine.rs"],
    crate_root = "tests/action_engine.rs",
    duckdb = True,
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 4: Run + lint + commit**

Run: `buck2 test //src/services/query-api:action-engine > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log` → `Pass 1. Fail 0.`
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` → empty.

```bash
git add src/services/query-api/src/serving.rs src/services/query-api/src/lib.rs \
  src/services/query-api/tests/action_engine.rs src/services/query-api/BUCK
git commit -m "feat(query-api): ActionEngine trait + EmbeddedDuckDbWriter (DuckLake inline insert)"
```

---

## Task 5: Typed parameter parsing

Parse a JSON action body into ordered `(column, SqlValue)` pairs, validated against the action's `ParamDef`s using loom's logical-type vocabulary — the inverse of the read-side `render.rs`.

**Files:**
- Create: `src/services/query-api/src/params.rs`
- Modify: `src/services/query-api/src/lib.rs`
- Create: `src/services/query-api/tests/params.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Create `params.rs`**

```rust
//! Parse a typed JSON action body into ordered (column, SqlValue) pairs, validated
//! against the action's parameters. The inverse of `render.rs`: Long arrives as a JSON
//! string, Double as a number, Boolean as a bool, String as a string, Date/Timestamp as
//! ISO strings. Pure logic, no I/O.

use control_plane_core::{JsonRepr, ParamDef, json_repr_of};
use serde_json::Value;

use crate::serving::SqlValue;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ParamError {
    #[error("missing required parameter: {0}")]
    Missing(String),
    #[error("unknown parameter: {0}")]
    Unknown(String),
    #[error("parameter {0}: {1}")]
    BadValue(String, String),
}

/// Validate `body` against `params`; return values in `params` order. Required params must
/// be present and non-null; extra keys are rejected; each value is parsed per its logical type.
pub fn parse_params(
    params: &[ParamDef],
    body: &serde_json::Map<String, Value>,
) -> Result<Vec<(String, SqlValue)>, ParamError> {
    // Reject unknown keys up front.
    for k in body.keys() {
        if !params.iter().any(|p| &p.name == k) {
            return Err(ParamError::Unknown(k.clone()));
        }
    }
    let mut out = Vec::with_capacity(params.len());
    for p in params {
        match body.get(&p.name) {
            None | Some(Value::Null) => {
                if p.required {
                    return Err(ParamError::Missing(p.name.clone()));
                }
                out.push((p.name.clone(), SqlValue::Null));
            }
            Some(v) => out.push((p.name.clone(), parse_value(&p.name, &p.ty, v)?)),
        }
    }
    Ok(out)
}

fn parse_value(name: &str, logical_ty: &str, v: &Value) -> Result<SqlValue, ParamError> {
    let bad = |m: &str| ParamError::BadValue(name.to_string(), m.to_string());
    let repr = json_repr_of(logical_ty).map_err(|_| bad("unknown logical type"))?;
    match repr {
        // Integer/Double both arrive as JSON numbers.
        JsonRepr::Number => {
            let n = v.as_f64().ok_or_else(|| bad("expected a number"))?;
            // Integer logical types still come through Number; keep them exact if whole.
            if n.fract() == 0.0 && n.abs() < 9.007e15 {
                Ok(SqlValue::Int(n as i64))
            } else {
                Ok(SqlValue::Double(n))
            }
        }
        // Long arrives as a JSON string (int64 precision).
        JsonRepr::NumericString => {
            let s = v.as_str().ok_or_else(|| bad("expected a numeric string"))?;
            s.parse::<i64>().map(SqlValue::Int).map_err(|_| bad("not an int64"))
        }
        JsonRepr::Bool => v.as_bool().map(SqlValue::Bool).ok_or_else(|| bad("expected a bool")),
        JsonRepr::PlainString => {
            v.as_str().map(|s| SqlValue::Text(s.to_string())).ok_or_else(|| bad("expected a string"))
        }
        JsonRepr::IsoDate => {
            let s = v.as_str().ok_or_else(|| bad("expected an ISO date string"))?;
            let fmt = time::macros::format_description!("[year]-[month]-[day]");
            time::Date::parse(s, &fmt).map(SqlValue::Date).map_err(|_| bad("invalid ISO date"))
        }
        JsonRepr::IsoTimestamp => {
            let s = v.as_str().ok_or_else(|| bad("expected an ISO timestamp string"))?;
            let fmt = time::macros::format_description!(
                "[year]-[month]-[day]T[hour]:[minute]:[second]"
            );
            time::PrimitiveDateTime::parse(s, &fmt)
                .map(SqlValue::Timestamp)
                .map_err(|_| bad("invalid ISO timestamp"))
        }
    }
}
```

(Note: `Integer` and `Double` logical types both map to `JsonRepr::Number`; `parse_value` returns `Int` for whole numbers and `Double` otherwise — DuckDB coerces to the column type on insert. If the build flags `time::macros` as unavailable, the `time` third-party dep already powers `render.rs`'s date formatting; mirror exactly how `render.rs` parses/formats dates instead.)

- [ ] **Step 2: Wire into `lib.rs`**

In `src/services/query-api/src/lib.rs`, add `pub mod params;` alongside the other module declarations.

- [ ] **Step 3: Write the tests**

Create `src/services/query-api/tests/params.rs`:

```rust
use control_plane_core::ParamDef;
use query_api::params::{ParamError, parse_params};
use query_api::serving::SqlValue;
use serde_json::json;

fn p(name: &str, ty: &str, required: bool) -> ParamDef {
    ParamDef { name: name.into(), ty: ty.into(), required }
}
fn body(v: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    v.as_object().unwrap().clone()
}

#[test]
fn parses_typed_params_in_order() {
    let params = vec![p("id", "Long", true), p("score", "Double", false), p("name", "String", false)];
    let got = parse_params(&params, &body(json!({ "id": "10", "score": 1.5, "name": "x" }))).unwrap();
    assert_eq!(
        got,
        vec![
            ("id".into(), SqlValue::Int(10)),
            ("score".into(), SqlValue::Double(1.5)),
            ("name".into(), SqlValue::Text("x".into())),
        ]
    );
}

#[test]
fn missing_required_is_an_error() {
    let params = vec![p("id", "Long", true)];
    assert_eq!(parse_params(&params, &body(json!({}))), Err(ParamError::Missing("id".into())));
}

#[test]
fn unknown_param_is_an_error() {
    let params = vec![p("id", "Long", true)];
    assert_eq!(
        parse_params(&params, &body(json!({ "id": "1", "extra": 2 }))),
        Err(ParamError::Unknown("extra".into()))
    );
}

#[test]
fn mistyped_value_is_an_error() {
    let params = vec![p("id", "Long", true)];
    assert!(matches!(
        parse_params(&params, &body(json!({ "id": 10 }))), // Long must be a string
        Err(ParamError::BadValue(_, _))
    ));
}

#[test]
fn optional_absent_becomes_null() {
    let params = vec![p("id", "Long", true), p("name", "String", false)];
    let got = parse_params(&params, &body(json!({ "id": "1" }))).unwrap();
    assert_eq!(got[1], ("name".into(), SqlValue::Null));
}
```

Append the BUCK target (pure `rust_test`):

```python
rust_test(
    name = "params",
    crate = "params",
    srcs = ["tests/params.rs"],
    crate_root = "tests/params.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//third-party:serde_json",
    ],
)
```

- [ ] **Step 4: Run + lint + commit**

Run: `buck2 test //src/services/query-api:params > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log` → `Pass 5. Fail 0.`
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` → empty.

```bash
git add src/services/query-api/src/params.rs src/services/query-api/src/lib.rs \
  src/services/query-api/tests/params.rs src/services/query-api/BUCK
git commit -m "feat(query-api): typed action parameter parsing"
```

---

## Task 6: `run_action` handler logic

The action orchestration: resolve → Write-ACL → parse params → conform → inline insert → best-effort lineage → return the created object. Pure of HTTP (testable directly); the e2e (Task 8) exercises it end to end.

**Files:**
- Create: `src/services/query-api/src/action.rs`
- Modify: `src/services/query-api/src/lib.rs`

- [ ] **Step 1: Create `action.rs`**

```rust
//! Action handler logic: invoke a named ontology action to insert one new typed object.
//! Governed by Action::Write; executes the inline write via the ActionEngine; emits
//! best-effort type-named lineage (a documented dangling slice — a failure to emit does
//! NOT fail the action).

use control_plane_core::{
    Action, ActionName, ControlPlane, ControlPlaneError, DatasetRef, Decision, EventType,
    LineageEvent, PolicyTarget, RunId, SubjectId,
};
use serde_json::Value;
use uuid::Uuid;

use crate::handler::ObjectRows;
use crate::params::{ParamError, parse_params};
use crate::serving::{ActionEngine, SqlValue};

pub struct ActionDeps<'a> {
    pub cp: &'a dyn ControlPlane,
    pub action_engine: &'a dyn ActionEngine,
}

#[derive(Debug, thiserror::Error)]
pub enum ActionError {
    #[error("unknown action: {0}")]
    UnknownAction(String),
    #[error("unknown object type: {0}")]
    UnknownType(String),
    #[error("forbidden")]
    Forbidden,
    #[error("bad parameters: {0}")]
    BadParams(#[from] ParamError),
    #[error(transparent)]
    ControlPlane(#[from] ControlPlaneError),
    #[error(transparent)]
    Serving(#[from] crate::serving::ServingError),
}

/// Run one action: insert a new instance of the action's target type from `body`.
/// Returns the created object as a single-row `ObjectRows` (rendered by the caller).
pub async fn run_action(
    action_name: &str,
    body: &serde_json::Map<String, Value>,
    subject: &SubjectId,
    deps: &ActionDeps<'_>,
) -> Result<ObjectRows, ActionError> {
    // 1. Resolve the action.
    let action = deps
        .cp
        .ontology()
        .get_action(&ActionName(action_name.to_string()))
        .await
        .map_err(|e| match e {
            ControlPlaneError::NotFound(_) => ActionError::UnknownAction(action_name.to_string()),
            other => ActionError::ControlPlane(other),
        })?;

    // 2. Resolve the target type (for its table + property logical types).
    let target = deps
        .cp
        .ontology()
        .get_type(&action.target)
        .await
        .map_err(|e| match e {
            ControlPlaneError::NotFound(_) => ActionError::UnknownType(action.target.0.clone()),
            other => ActionError::ControlPlane(other),
        })?;

    // 3. Govern: deny-by-default Write on the target type. First live use of Action::Write.
    let policy_target = PolicyTarget::Type(action.target.clone());
    if deps.cp.acl().check(subject, Action::Write, &policy_target).await? == Decision::Deny {
        return Err(ActionError::Forbidden);
    }

    // 4. Parse + validate the typed params (ordered by the action's parameter list).
    let pairs = parse_params(&action.parameters, body)?;
    let columns: Vec<String> = pairs.iter().map(|(c, _)| c.clone()).collect();
    let values: Vec<SqlValue> = pairs.iter().map(|(_, v)| v.clone()).collect();

    // 5. Inline insert via the engine.
    deps.action_engine.insert_row(&target.table, &columns, &values).await?;

    // 6. Best-effort, type-named lineage. A failure here is logged, NOT fatal (the
    //    documented dangling slice): the snapshot stands even if lineage didn't land.
    let snapshot_id = deps.cp.catalog().current_snapshot(&target.table).await.ok().map(|s| s.id.0);
    let event = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(&action.target)],
        payload: serde_json::json!({ "action": action_name, "snapshot_id": snapshot_id }),
    };
    if let Err(e) = deps.cp.lineage().emit(event).await {
        tracing::warn!(action = action_name, error = %e, "action lineage emit failed (dangling)");
    }

    // 7. Return the created object: the validated columns + values, with logical types
    //    from the target type's properties (in column order), for typed JSON rendering.
    let logical_types = columns
        .iter()
        .map(|c| {
            target
                .properties
                .iter()
                .find(|p| &p.name == c)
                .map(|p| p.ty.clone())
                .unwrap_or_default()
        })
        .collect();
    Ok(ObjectRows { columns, logical_types, rows: vec![values] })
}
```

(`deps.cp` is a `&dyn ControlPlane` trait object, so `cp.acl()`/`cp.catalog()`/`cp.lineage()`/`cp.ontology()` return trait objects and their methods resolve WITHOUT importing `Acl`/`Catalog`/`Lineage`/`Ontology` — do not import those traits here; only the data types above are needed. If clippy reports any import unused after the final code, trim it.)

- [ ] **Step 2: Wire into `lib.rs`**

Add `pub mod action;` to `src/services/query-api/src/lib.rs`.

- [ ] **Step 3: Build + lint**

Run: `buck2 build //src/services/query-api:query-api 2>&1 | tail -10` → clean.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` → empty (fix any unused-import warnings by trimming the imports).

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api/src/action.rs src/services/query-api/src/lib.rs
git commit -m "feat(query-api): run_action — governed inline insert + best-effort lineage"
```

---

## Task 7: HTTP `POST /actions/:name` route + binary wiring

Expose `run_action` over HTTP and wire the writer into the service.

**Files:**
- Modify: `src/services/query-api/src/http.rs`, `src/services/query-api/src/main.rs`

- [ ] **Step 1: Add the route + handler + state field in `http.rs`**

Add `action_engine` to `AppState`:

```rust
#[derive(Clone)]
pub struct AppState {
    pub cp: Arc<dyn ControlPlane>,
    pub serving: Arc<dyn ServingEngine>,
    pub action_engine: Arc<dyn ActionEngine>,
}
```

Add the route in `router`:

```rust
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/objects/:type_name", get(get_object))
        .route("/objects/:from_type/links/:link_name", get(get_linked))
        .route("/actions/:action_name", post(post_action))
        .with_state(state)
}
```

Add the handler (mirror `get_object`'s subject parsing + error→HTTP mapping; accept a JSON body):

```rust
async fn post_action(
    State(st): State<AppState>,
    Path(action_name): Path<String>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let subject = headers
        .get("X-Loom-Subject")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
    let obj = match body.as_object() {
        Some(m) => m.clone(),
        None => return (StatusCode::BAD_REQUEST, "body must be a JSON object").into_response(),
    };
    let deps = crate::action::ActionDeps {
        cp: st.cp.as_ref(),
        action_engine: st.action_engine.as_ref(),
    };
    match crate::action::run_action(&action_name, &obj, &SubjectId(subject), &deps).await {
        Ok(rows) => {
            let body = crate::render::objects_to_json(&rows);
            // objects_to_json yields {"objects":[{...}]}; return the single created object.
            let one = body
                .get("objects")
                .and_then(|a| a.as_array())
                .and_then(|a| a.first())
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            (StatusCode::CREATED, Json(one)).into_response()
        }
        Err(crate::action::ActionError::UnknownAction(a)) => (StatusCode::NOT_FOUND, a).into_response(),
        Err(crate::action::ActionError::UnknownType(t)) => (StatusCode::NOT_FOUND, t).into_response(),
        Err(crate::action::ActionError::Forbidden) => StatusCode::FORBIDDEN.into_response(),
        Err(crate::action::ActionError::BadParams(e)) => {
            (StatusCode::BAD_REQUEST, e.to_string()).into_response()
        }
        // Opaque body for backend/serving faults (no internal detail leaked).
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}
```

Update `http.rs` imports: add `post` to the `axum::routing` import (`use axum::routing::{get, post};`), `Json` to the axum extractors, `SubjectId` to the `control_plane_core` imports, and `ActionEngine` to the `crate::serving` imports. (Match the existing import grouping in the file.)

- [ ] **Step 2: Wire the writer in `main.rs`**

In `src/services/query-api/src/main.rs`, build an `EmbeddedDuckDbWriter` alongside the reader and put it in `AppState`:

```rust
use query_api::serving::{EmbeddedDuckDb, EmbeddedDuckDbWriter};
// ...
    let serving = Arc::new(EmbeddedDuckDb::attach(&cfg.db.ducklake_libpq(), &cfg.data_path).await?);
    let action_engine =
        Arc::new(EmbeddedDuckDbWriter::attach(&cfg.db.ducklake_libpq(), &cfg.data_path).await?);
    let app = router(AppState { cp, serving, action_engine });
```

- [ ] **Step 3: Build + lint + commit**

Run: `buck2 build //src/services/query-api:query-api //src/services/query-api:query-api-bin 2>&1 | tail -10` → clean.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` → empty.

```bash
git add src/services/query-api/src/http.rs src/services/query-api/src/main.rs
git commit -m "feat(query-api): POST /actions/{name} route + writer wiring"
```

---

## Task 8: Action e2e (acceptance) + Write-ACL test

The load-bearing proof: a defined action, invoked by a granted subject, inserts a typed object that reads back through the existing read path, with lineage and inline storage; and an ungranted subject is forbidden.

**Files:**
- Create: `src/services/query-api/tests/action_e2e.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Write the e2e**

Create `src/services/query-api/tests/action_e2e.rs`. It drives `run_action` directly (the HTTP layer is a thin wrapper already exercised by `http_smoke`), and reads back via `read_object`. Use the `bind_read_e2e.rs` ACL setup pattern and the spike's fixture pattern:

```rust
//! Actions e2e: define a type + a named insert action, grant Write, invoke the action,
//! and read the new object back through the governed read path. Also: an ungranted
//! subject is forbidden. Real Postgres + DuckDB.

use std::sync::Arc;

use control_plane_core::{
    Acl, Action, ActionDef, ActionName, ControlPlane, Effect, ObjectType, Ontology, ParamDef,
    PolicyTarget, PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use query_api::action::{ActionDeps, ActionError, run_action};
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::{EmbeddedDuckDb, EmbeddedDuckDbWriter};
use serde_json::json;

fn parquet_count(dir: &std::path::Path) -> usize {
    fn walk(dir: &std::path::Path, n: &mut usize) {
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() { walk(&p, n); }
                else if p.extension().is_some_and(|x| x == "parquet") { *n += 1; }
            }
        }
    }
    let mut n = 0; walk(dir, &mut n); n
}

#[tokio::test(flavor = "multi_thread")]
async fn action_inserts_a_typed_object_that_reads_back() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer_fx = DuckLakeWriter::new(fx.socket_path(), &db);
    writer_fx.bootstrap().await;
    writer_fx
        .seed(
            "main",
            "widget",
            &[("id".into(), "BIGINT".into(), false), ("name".into(), "VARCHAR".into(), true)],
            &[],
        )
        .await;
    let data_path = writer_fx.data_path().to_path_buf();
    let pg_conn = format!("dbname={db} host={} user=postgres", fx.socket_path().display());

    // Define the Widget type + a createWidget insert action.
    let widget = TypeName("Widget".into());
    cp.ontology()
        .define_type(ObjectType {
            name: widget.clone(),
            table: TableRef { schema: "main".into(), name: "widget".into() },
            properties: vec![
                PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
                PropertyDef { name: "name".into(), ty: "String".into(), required: false },
            ],
        })
        .await
        .unwrap();
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("createWidget".into()),
            target: widget.clone(),
            parameters: vec![
                ParamDef { name: "id".into(), ty: "Long".into(), required: true },
                ParamDef { name: "name".into(), ty: "String".into(), required: false },
            ],
        })
        .await
        .unwrap();

    // Grant Write on Widget to a subject.
    let subj = SubjectId("writer".into());
    let role = RoleId("writers".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(&role, Action::Write, PolicyTarget::Type(widget.clone()), Effect::Allow)
        .await
        .unwrap();
    // The read path needs a Read grant too (to verify the round-trip).
    cp.grant(&role, Action::Read, PolicyTarget::Type(widget.clone()), Effect::Allow)
        .await
        .unwrap();

    let engine = EmbeddedDuckDbWriter::attach(&pg_conn, &data_path).await.unwrap();
    let deps = ActionDeps { cp: &cp, action_engine: &engine };
    let body = json!({ "id": "42", "name": "gadget" });
    let created = run_action("createWidget", body.as_object().unwrap(), &subj, &deps)
        .await
        .expect("action runs");
    // The created object is returned with typed values.
    let created_json = objects_to_json(&created);
    assert_eq!(
        created_json["objects"][0],
        json!({ "id": "42", "name": "gadget" }),
        "created object echoed as typed JSON (Long id as string)"
    );

    // It inlined — no Parquet data file was written.
    assert_eq!(parquet_count(&data_path), 0, "action write inlined, no Parquet");

    // It reads back through the governed read path.
    let reader = EmbeddedDuckDb::attach(&pg_conn, &data_path).await.unwrap();
    let qdeps = QueryDeps { ontology: cp.ontology(), acl: cp.acl(), serving: &reader };
    let rows = read_object(
        &ObjectQuery { type_name: "Widget".into(), eq_filters: vec![] },
        &Subject(subj.clone()),
        &qdeps,
    )
    .await
    .unwrap();
    let read_json = objects_to_json(&rows);
    assert_eq!(read_json["objects"][0], json!({ "id": "42", "name": "gadget" }), "round-trips");

    // Lineage: run_action emits a best-effort, type-named LineageEvent for the write
    // (inputs=[], outputs=[Widget]). It is intentionally NOT asserted here — the event has
    // no inputs and run_action doesn't surface its run_id, so upstream/downstream/events_for
    // can't locate it from the test. Emission is exercised by run_action's code path;
    // strict, queryable action lineage is a follow-on (the dangling-slice note, Task 9).
}

#[tokio::test(flavor = "multi_thread")]
async fn ungranted_subject_is_forbidden() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer_fx = DuckLakeWriter::new(fx.socket_path(), &db);
    writer_fx.bootstrap().await;
    writer_fx
        .seed("main", "widget", &[("id".into(), "BIGINT".into(), false)], &[])
        .await;
    let pg_conn = format!("dbname={db} host={} user=postgres", fx.socket_path().display());

    let widget = TypeName("Widget".into());
    cp.ontology()
        .define_type(ObjectType {
            name: widget.clone(),
            table: TableRef { schema: "main".into(), name: "widget".into() },
            properties: vec![PropertyDef { name: "id".into(), ty: "Long".into(), required: true }],
        })
        .await
        .unwrap();
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("createWidget".into()),
            target: widget.clone(),
            parameters: vec![ParamDef { name: "id".into(), ty: "Long".into(), required: true }],
        })
        .await
        .unwrap();

    let engine = EmbeddedDuckDbWriter::attach(&pg_conn, writer_fx.data_path()).await.unwrap();
    let deps = ActionDeps { cp: &cp, action_engine: &engine };
    // No grant for this subject -> Forbidden, and nothing written.
    let subj = SubjectId("nobody".into());
    let err = run_action("createWidget", json!({ "id": "1" }).as_object().unwrap(), &subj, &deps)
        .await
        .unwrap_err();
    assert!(matches!(err, ActionError::Forbidden));
    // The table snapshot did not advance past bootstrap+seed (no rows).
    let count = writer_fx.query_scalar("SELECT count(*) FROM main.widget").await;
    assert_eq!(count, "0", "forbidden action wrote nothing");
}
```

- [ ] **Step 2: Add the BUCK target**

```python
loom_fixture_test(
    name = "action-e2e",
    crate = "action_e2e",
    srcs = ["tests/action_e2e.rs"],
    crate_root = "tests/action_e2e.rs",
    duckdb = True,
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:serde_json",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run + fix + commit**

Run: `buck2 test //src/services/query-api:action-e2e > /tmp/t8.log 2>&1; grep -E "Tests finished|FAIL|panicked|assertion" /tmp/t8.log`
Expected: `Tests finished: Pass 2. Fail 0.`

If the read-back is empty, revisit the Task-1 spike's finding about read-side inlining config. Do not weaken the round-trip or inline assertions.

```bash
git add src/services/query-api/tests/action_e2e.rs src/services/query-api/BUCK
git commit -m "test(query-api): action e2e — governed inline insert reads back; Write-403"
```

---

## Task 9: Docs — roadmap + FUTURE

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, `docs/FUTURE.md`

- [ ] **Step 1: Roadmap delivered marker**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, find the **Actions** bullet under Step 3 (currently described as future). Mark **Actions part-1** delivered, matching the format of the adjacent delivered entries (read them first): named `ActionDef` insert actions via `POST /actions/{name}`, first live `Action::Write` enforcement, DuckLake inline write behind the `ActionEngine` trait (Iceberg-swappable), best-effort type-named lineage, governed read-back. Reference `docs/superpowers/specs/2026-06-15-actions-part1-design.md`. Update the "Where we are" summary's verb set to include **write**.

- [ ] **Step 2: FUTURE.md follow-ups**

In `docs/FUTURE.md` (match its bullet style), add the Actions follow-ups:
- **Action lineage atomicity (dangling slice).** Action writes emit lineage best-effort on a separate connection after the DuckDB inline write; a crash in the gap can leave a snapshot without its lineage event. Close via a loom-owned DuckLake write or a compaction/reconciliation pass.
- **Update/delete actions** — gated on the deferred row-supersession/compaction work; part-1 is insert-only.
- **Fine-grained write governance** — part-1 enforces coarse `Write`-on-type only; row-filter / deny-write-column policy on `Write` is unbuilt.
- **Iceberg `ActionEngine` impl** — the trait's reason for being.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md docs/FUTURE.md
git commit -m "docs: actions part 1 delivered; record action follow-ups"
```

---

## Final Verification

- [ ] `buck2 build //src/... 2>&1 | tail -20` — clean.
- [ ] `buck2 test //src/... > /tmp/sweep.log 2>&1; grep -E "Tests finished|FAIL" /tmp/sweep.log` — `Fail 0`, including `inline-write-spike`, `action-engine`, `params`, `action-e2e`, and the ontology contracts.
- [ ] `tools/clippy-all.sh 2>&1 | tail -5` — clean.
- [ ] `git status` — no unexpected drift; the only `.sqlx` change is the new action queries (Task 2). No `Cargo.lock`/`third-party/BUCK` change (this slice adds no third-party deps).
- [ ] **Run buck2 commands serially** — never launch a second buck2 invocation (or a commit whose hooks run buck2) while a `buck2 test //src/...` sweep is running; the shared daemon serializes and a concurrent pair can stall remote-execution into a retry loop.

Then proceed to **superpowers:finishing-a-development-branch**.
