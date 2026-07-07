//! loom-owned commit surface of the vendored SQL catalog: the [`CommitExtras`]
//! side-effect aggregate, the in-tx mirror projection (`write_mirror`), the
//! pointer-CAS commit (`do_update_table`), and physical object deletion
//! (`delete_file`). Kept out of `catalog.rs` so the vendored file stays close
//! to upstream for re-vendoring diffs.

use iceberg::table::Table;
use iceberg::{Catalog, Error, ErrorKind, MetadataLocation, Result, TableCommit, TableIdent};
use sqlx::{Postgres, Transaction};

use control_plane_core::{LineageEvent, SnapshotId, TableRef};

use crate::iceberg_mirror::{ProjectedColumn, ProjectedFile};
use crate::lineage::pg_emit;

use super::catalog::{
    CATALOG_FIELD_CATALOG_NAME, CATALOG_FIELD_METADATA_LOCATION_PROP,
    CATALOG_FIELD_PREVIOUS_METADATA_LOCATION_PROP, CATALOG_FIELD_RECORD_TYPE,
    CATALOG_FIELD_TABLE_NAME, CATALOG_FIELD_TABLE_NAMESPACE, CATALOG_FIELD_TABLE_RECORD_TYPE,
    CATALOG_TABLE_NAME, SqlCatalog,
};
use super::error::from_sqlx_error;

/// Side-effects to run inside the one `do_update_table` commit tx, alongside the
/// pointer-CAS + mirror projection. Both are optional and independent.
#[derive(Default, Clone)]
pub struct CommitExtras<'a> {
    /// Emit this lineage event in the commit tx (landing / flush provenance).
    pub lineage: Option<&'a LineageEvent>,
    /// Retire these inline rows at the commit's snapshot (flush compaction).
    pub end_cap: Option<InlineEndCap<'a>>,
    /// Overwrite/replace mode: end-cap every currently-live data file for the table
    /// at the commit's snapshot (before projecting the new files), so the new set is
    /// the sole live set while prior files stay reachable by time travel. This is the
    /// overwrite/replace commit primitive (`Tx::replace_files`). `false` (the `Default`) is append.
    pub overwrite: bool,
    /// Enqueue these jobs atomically with the snapshot commit (deduped — each job is
    /// inserted only if no `state='available'` job with the same `(kind, payload)`
    /// already exists). Empty slice (`&[]`, the `Default`) means no jobs.
    pub jobs: &'a [control_plane_core::NewJob],
    /// Fire data-triggered transforms for these committed tables inside the
    /// commit tx (slice 3). NEW-DATA commits only: data-preserving rewrites
    /// (the inline flush) leave this empty so already-fired data cannot
    /// re-fire on its own flush. Empty slice (the `Default`) fires nothing.
    pub data_trigger_tables: &'a [TableRef],
}

/// Mark inline rows `loom_row_id = ANY(row_ids)` of `iceberg_mirror.inline_<table_id>`
/// as ended at the commit's snapshot.
#[derive(Clone)]
pub struct InlineEndCap<'a> {
    /// The `iceberg_mirror` table id (from `inline_<table_id>`).
    pub table_id: i64,
    /// The `loom_row_id` values to retire.
    pub row_ids: &'a [i64],
}

/// Apply the non-projection commit side-effects — inline end-cap, lineage
/// emit, job enqueue — in the caller's commit transaction at snapshot `at`, in
/// that (load-bearing) order. Shared by [`SqlCatalog::do_update_table`] (the
/// CAS commit) and `iceberg_landing::land_additive` (the mirror-only additive
/// commit) so the extras semantics cannot drift between them.
///
/// `extras.overwrite` is NOT applied here: overwrite ordering (end-cap the live
/// files BEFORE projecting the new ones) belongs to the projection step
/// (`write_mirror` / `register_files`), which runs before this.
pub(crate) async fn apply_commit_extras(
    conn: &mut sqlx::PgConnection,
    at: SnapshotId,
    extras: &CommitExtras<'_>,
) -> control_plane_core::Result<()> {
    if let Some(cap) = &extras.end_cap {
        // Retire the flushed inline rows at the same snapshot the new data
        // becomes live, so reads never double-serve or drop them.
        crate::iceberg_inline::end_cap_inline_rows_by_id(conn, cap.table_id, cap.row_ids, at)
            .await?;
    }
    if let Some(ev) = extras.lineage {
        pg_emit(&mut *conn, ev).await?;
    }
    for job in extras.jobs {
        crate::queue::pg_insert_if_absent(&mut *conn, job).await?;
    }
    crate::transforms::pg_fire_data_triggers(
        &mut *conn,
        extras.data_trigger_tables,
        extras.lineage.map(|ev| ev.run_id.0),
    )
    .await?;
    Ok(())
}

impl SqlCatalog {
    /// Physically delete an object-store file by its absolute URL (e.g. a `file://`
    /// or `s3://` Parquet path). Idempotent: a missing object is not an error —
    /// idempotency is provided by the backend (`LocalFsStorage`/S3 both no-op on
    /// absence), not enforced here. This is the only object-store *delete*
    /// capability on the catalog — used by GC to reclaim the Parquet of end-capped
    /// data files; read/write paths are untouched.
    pub async fn delete_file(&self, path: &str) -> control_plane_core::Result<()> {
        self.fileio.delete(path).await.map_err(crate::backend)
    }

    /// Write the mirror rows for an already-committed table state, in the caller's
    /// tx, from **precomputed** inputs only. Takes no `&Table` / `FileIO`, so it is
    /// type-level incapable of reading object storage inside the transaction — the
    /// property `iss-iceberg-tx-objectstore` requires (enforced by the signature,
    /// not by convention). The object-store read (`added_files_of`) and schema read
    /// (`columns_of`) are done by the caller before `begin()`.
    ///
    /// Returns the mirror snapshot it allocated so callers can use it for further
    /// in-tx work (e.g. end-capping inline rows at the same snapshot).
    async fn write_mirror(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        ident: &TableIdent,
        staged_snap: Option<i64>,
        columns: &[ProjectedColumn],
        files: &[ProjectedFile],
        overwrite: bool,
    ) -> control_plane_core::Result<SnapshotId> {
        use crate::iceberg_mirror::{
            end_cap_live_data_files, ensure_table, next_snapshot, project_files,
            reconcile_and_project, stamp_schema_version,
        };

        let ns = ident.namespace().join(".");
        let name = ident.name();

        let conn = &mut **tx;
        let at = next_snapshot(conn, staged_snap).await?;
        let tid = ensure_table(conn, &ns, name, at).await?;
        // Overwrite/replace: end-cap the pre-existing live files at `at` BEFORE
        // projecting the new ones. The new `project_files` rows are written after this
        // (also `end_snapshot is null`, also `table_id = tid`), so they stay live; only
        // the prior files get `end_snapshot = at`. Ordering is load-bearing — end-capping
        // after `project_files` would wrongly retire the just-projected files too.
        if overwrite {
            end_cap_live_data_files(conn, tid, at).await?;
            crate::iceberg_inline::end_cap_live_inline_rows(conn, tid, at).await?;
        }
        reconcile_and_project(conn, tid, at, columns).await?;
        project_files(conn, tid, at, files).await?;
        stamp_schema_version(conn, tid, at).await?;
        Ok(at)
    }

    /// The real commit: pointer CAS + mirror projection (+ optional lineage and/or
    /// inline end-cap), all in one Postgres transaction. `update_table` calls this
    /// with `CommitExtras::default()`; the loom landing path passes a lineage event
    /// so it commits or rolls back together with the snapshot it describes; the
    /// flush path additionally passes an end-cap to retire inline rows at the
    /// same snapshot the new Parquet file becomes live.
    ///
    /// Thin owning wrapper around [`Self::do_update_table_in_tx`]: begins the tx,
    /// delegates, and commits on success. On a lost CAS (or any other error from
    /// the delegate) it rolls back its own tx before propagating the error —
    /// preserving the delegate's contract that it never touches `tx` on the
    /// conflict path (the delegate's caller — here — owns rollback).
    pub(crate) async fn do_update_table(
        &self,
        commit: TableCommit,
        extras: CommitExtras<'_>,
    ) -> Result<Table> {
        let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;

        match self.do_update_table_in_tx(&mut tx, commit, extras).await {
            Ok(table) => {
                tx.commit().await.map_err(from_sqlx_error)?;
                Ok(table)
            }
            Err(e) => {
                drop(tx.rollback().await);
                Err(e)
            }
        }
    }

    /// [`Self::do_update_table`]'s body, parameterized over a caller-provided
    /// transaction: the staged-metadata object-store write, the
    /// `added_files_of`/`columns_of` reads, the CAS `UPDATE`, `write_mirror`, and
    /// `apply_commit_extras` all run on `tx`. Does NOT `begin()` or `commit()` it —
    /// that is the caller's responsibility (see [`Self::do_update_table`] for the
    /// owning wrapper). Exists so a later commit path can run further work (e.g.
    /// offset allocation) on the same tx that commits the snapshot.
    ///
    /// On a lost CAS (`rows_affected() == 0`) returns a retryable
    /// `CatalogCommitConflicts` error WITHOUT touching `tx` — the caller decides
    /// whether to roll back.
    pub(crate) async fn do_update_table_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        commit: TableCommit,
        extras: CommitExtras<'_>,
    ) -> Result<Table> {
        let table_ident = commit.identifier().clone();
        let current_table = self.load_table(&table_ident).await?;
        let current_metadata_location = current_table.metadata_location_result()?.to_string();

        let staged_table = commit.apply(current_table)?;
        let staged_metadata_location = staged_table.metadata_location_result()?;
        // iceberg main's `TableMetadata::write_to` takes a typed `&MetadataLocation`
        // (was `&str`); parse the location string commit.apply already computed. The
        // string itself is still used below as the CAS pointer value.
        let staged_ml: MetadataLocation = staged_metadata_location.parse()?;

        staged_table
            .metadata()
            .write_to(staged_table.file_io(), &staged_ml)
            .await?;

        // Object-store reads happen here, BEFORE the CAS UPDATE: load the new
        // snapshot's manifests + Parquet footers and snapshot the staged schema. The
        // manifests are immutable and already persisted (fast_append wrote them;
        // write_to wrote the staged metadata above), so reading them here is
        // identical to reading them anywhere else in the tx — no read-after-write
        // hazard, and the CAS still guards the pointer.
        let mirror_files = crate::iceberg_mirror::added_files_of(&staged_table)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        let mirror_columns = crate::iceberg_mirror::columns_of(&staged_table)
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        let staged_snap = staged_table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id());

        let update_result = self
            .execute(
                &format!(
                    "UPDATE {CATALOG_TABLE_NAME}
                     SET {CATALOG_FIELD_METADATA_LOCATION_PROP} = ?, {CATALOG_FIELD_PREVIOUS_METADATA_LOCATION_PROP} = ?
                     WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                      AND (
                        {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}'
                        OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                      )
                      AND {CATALOG_FIELD_METADATA_LOCATION_PROP} = ?"
                ),
                vec![
                    Some(staged_metadata_location),
                    Some(current_metadata_location.as_str()),
                    Some(&self.name),
                    Some(table_ident.name()),
                    Some(&table_ident.namespace().join(".")),
                    Some(current_metadata_location.as_str()),
                ],
                Some(&mut *tx),
            )
            .await?;

        if update_result.rows_affected() == 0 {
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                format!("Commit conflicted for table: {table_ident}"),
            )
            .with_retryable(true));
        }

        let at = self
            .write_mirror(
                &mut *tx,
                &table_ident,
                staged_snap,
                &mirror_columns,
                &mirror_files,
                extras.overwrite,
            )
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        apply_commit_extras(tx, at, &extras)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        Ok(staged_table)
    }
}
