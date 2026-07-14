//! The catalog concern: a read-only view over the active table-format catalog. The
//! table-format adapter (Iceberg) populates it; loom reads it. Snapshots are
//! catalog-global and identified by a monotonic id; tables/files/columns are
//! versioned by `begin`/`end` snapshot ranges (MVCC), so reads are "this table
//! *at* that snapshot".

use async_trait::async_trait;
use time::OffsetDateTime;

use crate::error::{ControlPlaneError, Result};
use crate::page::{Page, PageReq};

/// A catalog-global snapshot/version id (monotonic). Portable across table formats
/// (Iceberg, Delta all key versions by i64).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapshotId(pub i64);

/// A `schema.table` reference within the table-format catalog.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TableRef {
    pub schema: String,
    pub name: String,
}

/// A point-in-time version of the catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub id: SnapshotId,
    pub time: OffsetDateTime,
    pub schema_version: i64,
}

/// A Parquet file backing a table at a snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRef {
    pub path: String,
    pub record_count: i64,
    pub file_size_bytes: i64,
}

/// The subset of `files` smaller than `threshold_bytes` — the compaction candidate set.
/// Pure — no I/O — so it is unit-testable without a catalog.
pub fn small_files(files: &[FileRef], threshold_bytes: i64) -> Vec<&FileRef> {
    files
        .iter()
        .filter(|f| f.file_size_bytes < threshold_bytes)
        .collect()
}

/// One column of a table's schema at a snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    pub order: i64,
    pub name: String,
    /// The column's loom LOGICAL type name (the adapter maps from its physical
    /// catalog type on read). Typing is an ontology concern.
    pub ty: String,
    pub nullable: bool,
}

/// A table's column schema at a snapshot, in column order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchema {
    pub columns: Vec<ColumnDef>,
}

/// A virtual dataset: a named row/column subset of one physical base table.
/// Shares the `(schema, name)` namespace with physical tables so it binds and
/// grants exactly like one (`ObjectType.table`, `PolicyTarget::Table`).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ViewDef {
    /// The view's own ref — must not collide with any physical table or view.
    pub view: TableRef,
    /// The physical base. v1 forbids view-over-view: this must be a table.
    pub base: TableRef,
    /// Row subset. `property` names a BASE column directly (Table-target
    /// `RowFilter` semantics). `None` = all rows.
    pub predicate: Option<crate::acl::RowFilter>,
    /// Column subset, in base-schema order significance. `None` = all columns.
    pub columns: Option<Vec<String>>,
}

/// Pure shape validation of a view definition against its base schema:
/// predicate columns and projection columns must exist in the base; the
/// projection must be non-empty and duplicate-free; the view must not name
/// itself as base. Existence/collision checks are the adapters' job (they
/// need store access); this is the shared schema-shape gate.
///
/// # Errors
///
/// Returns a human-readable reason on the first failure: the view naming
/// itself as base, a predicate referencing an unknown or caller-only-op
/// column (surfaced via [`crate::acl::validate_row_filter`]), an empty
/// projection, an unknown projection column, or a duplicate projection
/// column.
pub fn validate_view_shape(
    v: &ViewDef,
    base_schema: &TableSchema,
) -> std::result::Result<(), String> {
    if v.view == v.base {
        return Err(format!(
            "view {}.{} cannot use itself as base",
            v.view.schema, v.view.name
        ));
    }
    let base_cols: std::collections::HashSet<String> =
        base_schema.columns.iter().map(|c| c.name.clone()).collect();
    if let Some(f) = &v.predicate {
        crate::acl::validate_row_filter(f, Some(&base_cols))?;
    }
    if let Some(cols) = &v.columns {
        if cols.is_empty() {
            return Err("projection must name at least one column".into());
        }
        let mut seen = std::collections::HashSet::new();
        for c in cols {
            if !base_cols.contains(c) {
                return Err(format!("projection column `{c}` not in base schema"));
            }
            if !seen.insert(c) {
                return Err(format!("duplicate projection column `{c}`"));
            }
        }
    }
    Ok(())
}

#[async_trait]
pub trait Catalog {
    /// The latest snapshot at which `table` is live. `NotFound` if the table does
    /// not exist at the catalog's current snapshot.
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot>;
    /// The latest snapshot at or before `ts` at which `table` is live, or `None`
    /// if the table has no live snapshot at/before that instant (created later, or
    /// never existed). Time-travel resolution for a wall-clock `as_of` read.
    async fn snapshot_as_of(
        &self,
        table: &TableRef,
        ts: OffsetDateTime,
    ) -> Result<Option<Snapshot>>;
    /// All snapshots at which `table` is live, oldest first. `NotFound` if the
    /// table never existed. The `page` request is accepted but not yet enforced;
    /// results are a single full page.
    async fn snapshots(&self, table: &TableRef, page: PageReq) -> Result<Page<Snapshot>>;
    /// The global snapshot `id`, if it exists in the catalog's history AND `table`
    /// is live at it. `None` for an id never allocated (including any id above the
    /// newest snapshot), or one at which the table is not live (pre-creation /
    /// post-drop). The exact-history gate both time-travel read surfaces validate
    /// `?as_of_snapshot=` against. Same per-id semantics as the `snapshots` listing.
    async fn snapshot(&self, table: &TableRef, id: SnapshotId) -> Result<Option<Snapshot>>;
    /// The GC retention horizon at `cutoff`: the youngest snapshot wholly aged out
    /// (max snapshot id with `snapshot_time < cutoff`), or `None` when no snapshot
    /// has aged out. The same derivation `iceberg_gc` reclaims under; a read at a
    /// snapshot `>=` this horizon is guaranteed complete.
    async fn snapshot_horizon(&self, cutoff: OffsetDateTime) -> Result<Option<SnapshotId>>;
    /// The Parquet files live for `table` at snapshot `at`. `NotFound` if the
    /// table is not live at `at`. The `page` request is accepted but not yet enforced;
    /// results are a single full page.
    async fn files(&self, table: &TableRef, at: SnapshotId, page: PageReq)
    -> Result<Page<FileRef>>;
    /// `table`'s column schema at snapshot `at`, in column order. `NotFound` if
    /// the table is not live at `at`.
    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema>;
    /// Every table currently live in the mirror (no end-cap), `(schema, name)`-ordered.
    /// The `page` request is accepted but not yet enforced; results are a single
    /// full page.
    async fn list_tables(&self, page: PageReq) -> Result<Page<TableRef>>;

    /// Create a view. Create-only: an existing view (or a name colliding with
    /// a physical table) is `Conflict`. The base must exist, be physical (no
    /// view-over-view), and satisfy [`validate_view_shape`].
    async fn define_view(&self, view: ViewDef) -> Result<()> {
        let _ = view;
        Err(ControlPlaneError::Validation(
            "views are not supported by this catalog".into(),
        ))
    }

    /// Drop a view by its ref. `NotFound` if it does not exist.
    async fn drop_view(&self, view: &TableRef) -> Result<()> {
        Err(ControlPlaneError::NotFound(format!(
            "{}.{}",
            view.schema, view.name
        )))
    }

    /// Resolve a ref to its view definition, `None` if it is not a view.
    async fn get_view(&self, view: &TableRef) -> Result<Option<ViewDef>> {
        let _ = view;
        Ok(None)
    }

    /// All views, `(schema, name)`-ordered, single full page (parity with
    /// `list_tables`).
    async fn list_views(&self, page: PageReq) -> Result<Page<ViewDef>> {
        let _ = page;
        Ok(Page::from_full(Vec::new()))
    }
}
