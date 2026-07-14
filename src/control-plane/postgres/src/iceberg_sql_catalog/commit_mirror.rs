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
use crate::mv_floor::EndCapIntent;

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
    /// Retire these inline rows at the commit's snapshot (flush compaction, or a
    /// targeted overwrite that consumed a known inline row set). In overwrite mode
    /// (`overwrite: true`), a `Some` here REPLACES the blanket live-inline-row cap
    /// `write_mirror` would otherwise apply: the caller consumed exactly this row
    /// set (the consolidation fold) and every other live inline row must SURVIVE —
    /// a delta committed mid-consolidation keeps shadowing the new base. `None`
    /// (the `Default`) in overwrite mode falls back to the blanket cap.
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
    /// Reuse this ALREADY-ALLOCATED loom snapshot for the mirror projection instead
    /// of allocating a fresh one. Used by the atomic direct-write stream path, which
    /// must allocate the snapshot (and the mirror table row it keys offset allocation
    /// by) on the shared tx BEFORE the Parquet write, then have this commit project
    /// columns/files at that same snapshot — so the whole write is ONE snapshot, not
    /// a spurious empty seed plus the commit's. `None` (the `Default`) allocates a
    /// fresh snapshot, exactly as before this field existed (every other caller).
    pub reuse_snapshot: Option<SnapshotId>,
    /// WHY this commit end-caps — both its `overwrite` file/inline caps (applied by
    /// `write_mirror`) and its targeted `end_cap` (applied by [`apply_commit_extras`]).
    /// Checked against the MV read-position floor (`crate::mv_floor::guard_end_cap`).
    /// Defaults to `Removing` — the fail-safe, so a new commit path that starts
    /// end-capping without thinking gets the guard. **Flush overrides it to
    /// `Reframing`**: it end-caps inline rows and re-projects those SAME rows into live
    /// Parquet at the SAME `(loom_bucket, loom_offset)`, so no MV can miss one.
    pub intent: EndCapIntent<'a>,
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
///
/// `table` is the identity the targeted inline end-cap is guarded against
/// (`extras.intent` vs the MV read-position floor); both callers already hold it.
pub(crate) async fn apply_commit_extras(
    conn: &mut sqlx::PgConnection,
    table: &TableRef,
    at: SnapshotId,
    extras: &CommitExtras<'_>,
) -> control_plane_core::Result<()> {
    if let Some(cap) = &extras.end_cap {
        // Retire the flushed inline rows at the same snapshot the new data
        // becomes live, so reads never double-serve or drop them.
        crate::iceberg_inline::end_cap_inline_rows_by_id(
            conn,
            table,
            cap.table_id,
            cap.row_ids,
            at,
            &extras.intent,
        )
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

/// The object-store-derived inputs to a commit's PG-only tail
/// ([`SqlCatalog::commit_mirror_in_tx`]): the staged table + its pointer
/// locations, the staged Iceberg snapshot id, and the mirror files/columns read
/// from the just-written staged metadata. Produced by [`SqlCatalog::stage_commit`]
/// — the object-store STAGING half of a commit — and consumed by the PG-only tail,
/// so the two halves cannot drift between the common short-tx path and the stream
/// long-tx path.
struct StagedCommit {
    staged_table: Table,
    current_metadata_location: String,
    staged_metadata_location: String,
    staged_snap: Option<i64>,
    mirror_files: Vec<ProjectedFile>,
    mirror_columns: Vec<ProjectedColumn>,
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
    /// (`columns_of`) are done by the caller during STAGING — before `begin()` on the
    /// common short-tx path ([`Self::do_update_table`]), or in-tx on the accepted
    /// long-tx stream direct-write path ([`Self::do_update_table_in_tx`]).
    ///
    /// Returns the mirror snapshot it allocated so callers can use it for further
    /// in-tx work (e.g. end-capping inline rows at the same snapshot).
    #[expect(
        clippy::too_many_arguments,
        reason = "one cohesive commit-projection call: the tx + table identity + \
                  staged snapshot + precomputed columns/files + overwrite mode + \
                  blanket-inline-cap decision + the end-cap intent those two caps are \
                  guarded against, plus the optional reuse-snapshot for the atomic \
                  stream direct-write path"
    )]
    async fn write_mirror(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        ident: &TableIdent,
        staged_snap: Option<i64>,
        columns: &[ProjectedColumn],
        files: &[ProjectedFile],
        overwrite: bool,
        blanket_inline_cap: bool,
        intent: &EndCapIntent<'_>,
        reuse_snapshot: Option<SnapshotId>,
    ) -> control_plane_core::Result<SnapshotId> {
        use crate::iceberg_mirror::{
            end_cap_live_data_files, ensure_table, next_snapshot, project_files,
            reconcile_and_project, stamp_schema_version,
        };

        let ns = ident.namespace().join(".");
        let name = ident.name();
        // The identity the end-caps below (and the compaction trigger at the tail) are
        // keyed by — built once from the ident this commit is for.
        let table = TableRef {
            schema: ns.clone(),
            name: name.to_owned(),
        };

        let conn = &mut **tx;
        // The atomic direct-write stream path pre-allocated the snapshot (and the
        // mirror table row keyed by offset allocation) on this same tx before the
        // Parquet write; reuse it so the write is ONE snapshot. Correlate it with
        // the staged Iceberg snapshot id (set NULL at pre-allocation) for parity with
        // the fresh-allocation path. Every other caller passes `None` → fresh alloc.
        let at = match reuse_snapshot {
            Some(at) => {
                crate::iceberg_mirror::set_iceberg_snapshot_id(conn, at, staged_snap).await?;
                at
            }
            None => next_snapshot(conn, staged_snap).await?,
        };
        let tid = ensure_table(conn, &ns, name, at).await?;
        // Overwrite/replace: end-cap the pre-existing live files at `at` BEFORE
        // projecting the new ones. The new `project_files` rows are written after this
        // (also `end_snapshot is null`, also `table_id = tid`), so they stay live; only
        // the prior files get `end_snapshot = at`. Ordering is load-bearing — end-capping
        // after `project_files` would wrongly retire the just-projected files too.
        //
        // Both caps are guarded by `intent` (the MV read-position floor): a `Removing`
        // overwrite of offsets a micro-batch MV has not read is REFUSED, rolling the
        // caller's commit tx back.
        if overwrite {
            end_cap_live_data_files(conn, &table, tid, at, intent).await?;
            // A targeted InlineEndCap riding this same commit supersedes the
            // blanket cap: the caller consumed a known inline row set (the
            // consolidation fold) and everything else must SURVIVE — a delta
            // committed mid-consolidation keeps shadowing the new base.
            if blanket_inline_cap {
                crate::iceberg_inline::end_cap_live_inline_rows(conn, &table, tid, at, intent)
                    .await?;
            }
        }
        reconcile_and_project(conn, tid, at, columns).await?;
        project_files(conn, tid, at, files).await?;
        stamp_schema_version(conn, tid, at).await?;
        // Event-driven compaction auto-trigger: evaluated last, once the new
        // files are already projected, so the live small-file count reflects
        // this commit. Covers every CAS commit — ingest multi-file land, flush,
        // COW overwrite, and the stream direct write. `None` (default) is a
        // no-op, preserving today's behavior byte-identically.
        if let Some(cfg) = &self.compact_trigger {
            crate::iceberg_compact::maybe_enqueue_compact(conn, &table, cfg).await?;
        }
        Ok(at)
    }

    /// The real commit: pointer CAS + mirror projection (+ optional lineage and/or
    /// inline end-cap), all in one Postgres transaction. `update_table` calls this
    /// with `CommitExtras::default()`; the loom landing path passes a lineage event
    /// so it commits or rolls back together with the snapshot it describes; the
    /// flush path additionally passes an end-cap to retire inline rows at the
    /// same snapshot the new Parquet file becomes live.
    ///
    /// The COMMON commit path (flush / landing / overwrite / inline-flush — every
    /// caller except the stream direct-write path): it does ALL the object-store
    /// STAGING ([`Self::stage_commit`] — staged-metadata `write_to`, `added_files_of`
    /// manifest+footer read, `columns_of`) BEFORE `begin()`, then opens the tx and
    /// runs only the PG-only tail ([`Self::commit_mirror_in_tx`]) on it. So the
    /// commit tx holds ONLY fast local Postgres work — no `await` on object storage
    /// between `begin()` and `commit()` — restoring the `iss-iceberg-tx-objectstore`
    /// invariant on the hot path across S3/MinIO latency. On a lost CAS (or any other
    /// error from the tail) it rolls back its own tx before propagating.
    pub(crate) async fn do_update_table(
        &self,
        commit: TableCommit,
        extras: CommitExtras<'_>,
    ) -> Result<Table> {
        // STAGING first — all object-store I/O happens here, BEFORE begin(), so the
        // short tx opened below never awaits object storage.
        let staged = self.stage_commit(commit).await?;

        let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;
        match self
            .commit_mirror_in_tx(
                &mut tx,
                staged.staged_table,
                &staged.current_metadata_location,
                &staged.staged_metadata_location,
                staged.staged_snap,
                &staged.mirror_files,
                &staged.mirror_columns,
                &extras,
            )
            .await
        {
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

    /// The STAGING half of a commit: the object-store side-effects and reads that
    /// prepare a [`StagedCommit`] for the PG-only tail. Loads the current table,
    /// applies the commit, writes the staged metadata, then reads the new snapshot's
    /// manifests + Parquet footers (`added_files_of`) and staged schema
    /// (`columns_of`). This is ALL the object-store I/O a commit performs — no
    /// storage access happens in [`Self::commit_mirror_in_tx`].
    ///
    /// The manifests are immutable and already persisted (`fast_append` wrote them;
    /// `write_to` wrote the staged metadata above), so reading them here — whether
    /// before `begin()` (common path) or in-tx (stream path) — is a read of durable
    /// state with no read-after-write hazard; the CAS in the tail still guards the
    /// pointer.
    async fn stage_commit(&self, commit: TableCommit) -> Result<StagedCommit> {
        let table_ident = commit.identifier().clone();
        let current_table = self.load_table(&table_ident).await?;
        let current_metadata_location = current_table.metadata_location_result()?.to_string();

        let staged_table = commit.apply(current_table)?;
        let staged_metadata_location = staged_table.metadata_location_result()?.to_string();
        // iceberg main's `TableMetadata::write_to` takes a typed `&MetadataLocation`
        // (was `&str`); parse the location string commit.apply already computed. The
        // string itself is still used below as the CAS pointer value.
        let staged_ml: MetadataLocation = staged_metadata_location.parse()?;

        staged_table
            .metadata()
            .write_to(staged_table.file_io(), &staged_ml)
            .await?;

        let mirror_files = crate::iceberg_mirror::added_files_of(&staged_table)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        let mirror_columns = crate::iceberg_mirror::columns_of(&staged_table)
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        let staged_snap = staged_table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id());

        Ok(StagedCommit {
            staged_table,
            current_metadata_location,
            staged_metadata_location,
            staged_snap,
            mirror_files,
            mirror_columns,
        })
    }

    /// The LONG-TX commit variant: same commit as [`Self::do_update_table`] but with
    /// BOTH the object-store STAGING ([`Self::stage_commit`]) AND the PG-only tail
    /// ([`Self::commit_mirror_in_tx`]) run on a caller-provided ALREADY-OPEN
    /// transaction `tx`. Does NOT `begin()` or `commit()` it — the caller owns that.
    ///
    /// Used ONLY by the stream direct-write path (`iceberg_landing::land_parquet` via
    /// the `TxCommitCatalog` decorator), whose Parquet write is itself in-tx: it must
    /// commit the snapshot on the SAME tx it already staged offset allocation on, so
    /// the whole write (offsets + Parquet + snapshot) is atomic. Holding the tx across
    /// the object-store Parquet write is the deliberate, accepted long-tx tradeoff for
    /// the bulk stream path (the full-atomic-parity decision) — the common path
    /// ([`Self::do_update_table`]) stages before `begin()` and is NOT affected.
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
        // Stream long-tx path: staging runs IN the caller's tx (accepted tradeoff).
        let staged = self.stage_commit(commit).await?;
        self.commit_mirror_in_tx(
            tx,
            staged.staged_table,
            &staged.current_metadata_location,
            &staged.staged_metadata_location,
            staged.staged_snap,
            &staged.mirror_files,
            &staged.mirror_columns,
            &extras,
        )
        .await
    }

    /// The PG-only TAIL shared by both commit owners: the pointer-CAS `UPDATE`, the
    /// lost-CAS conflict check, the mirror projection (`write_mirror`), and
    /// `apply_commit_extras` — all on the caller's `tx`, from **precomputed** staging
    /// inputs. Performs ZERO object-store I/O (every storage read/write was done in
    /// [`Self::stage_commit`]), so the tx it runs on holds only fast local Postgres
    /// work — the `iss-iceberg-tx-objectstore` invariant.
    ///
    /// On a lost CAS (`rows_affected() == 0`) returns a retryable
    /// `CatalogCommitConflicts` error WITHOUT touching `tx` — the caller decides
    /// whether to roll back.
    #[expect(
        clippy::too_many_arguments,
        reason = "one cohesive PG-only commit tail: the tx + the staged table and its two \
                  pointer locations + the precomputed staged snapshot/files/columns + the \
                  commit extras; every arg is a precomputed staging input so the body can \
                  touch no object storage"
    )]
    async fn commit_mirror_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        staged_table: Table,
        current_metadata_location: &str,
        staged_metadata_location: &str,
        staged_snap: Option<i64>,
        mirror_files: &[ProjectedFile],
        mirror_columns: &[ProjectedColumn],
        extras: &CommitExtras<'_>,
    ) -> Result<Table> {
        let table_ident = staged_table.identifier().clone();

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
                    Some(current_metadata_location),
                    Some(&self.name),
                    Some(table_ident.name()),
                    Some(&table_ident.namespace().join(".")),
                    Some(current_metadata_location),
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

        // A targeted end-cap riding an overwrite commit supersedes the blanket
        // live-inline-row cap `write_mirror` would otherwise apply (see
        // `CommitExtras::end_cap`); non-overwrite commits never blanket-cap
        // regardless, so this only matters when `extras.overwrite` is set.
        let blanket_inline_cap = extras.overwrite && extras.end_cap.is_none();
        let at = self
            .write_mirror(
                &mut *tx,
                &table_ident,
                staged_snap,
                mirror_columns,
                mirror_files,
                extras.overwrite,
                blanket_inline_cap,
                &extras.intent,
                extras.reuse_snapshot,
            )
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        let table = TableRef {
            schema: table_ident.namespace().join("."),
            name: table_ident.name().to_owned(),
        };
        apply_commit_extras(tx, &table, at, extras)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        Ok(staged_table)
    }
}
