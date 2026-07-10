//! The engine-side governed-write executor (relocated from query-api). It receives
//! a pre-authorized write: the caller (query-api) has already enforced ACL, built
//! the typed Arrow batch, and IPC-encoded it. This executor lands or overwrites it,
//! committing the row(s) and lineage atomically via `iceberg_landing`. It is
//! governance-free — exactly mirroring the read path.

use std::sync::Arc;

use control_plane_core::{ColumnSpec, ControlPlaneError, LineageEvent, SnapshotId, TableRef};
use control_plane_postgres::iceberg_inline;
use control_plane_postgres::iceberg_landing;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use sqlx::PgPool;

use crate::serving::EngineServingError;

/// One decoded target of a multi-target atomic write ([`IcebergActionWriter::write_steps`]):
/// a table, its column schema, the IPC-encoded batch to write, and whether the batch
/// appends to or overwrites the table's live set. Built server-side from the wire
/// `WriteStepsRequest` (one per `StepWrite` proto message).
pub struct StepWrite {
    pub table: TableRef,
    pub columns: Vec<ColumnSpec>,
    pub ipc: Vec<u8>,
    pub overwrite: bool,
}

/// The relocated `ActionEngine` executor. Holds the same dependencies the old
/// query-api writer held: an Iceberg `SqlCatalog`, a `PgPool`, and the inline/flush
/// byte routing knobs.
pub struct IcebergActionWriter {
    catalog: Arc<SqlCatalog>,
    pool: PgPool,
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
    /// Accumulated CDC delta-row count at/above which `write_delta` enqueues a
    /// `stream_consolidate` job (mirrors `flush_byte_threshold`'s role for the
    /// byte-triggered flush). Defaults to `i64::MAX` — i.e. never trigger — so
    /// every existing 4-arg `new()` caller keeps today's behaviour; production
    /// wiring opts in via [`Self::with_consolidate_delta_threshold`].
    consolidate_delta_threshold: i64,
}

impl IcebergActionWriter {
    #[must_use]
    pub fn new(
        catalog: Arc<SqlCatalog>,
        pool: PgPool,
        inline_byte_limit: usize,
        flush_byte_threshold: i64,
    ) -> Self {
        Self {
            catalog,
            pool,
            inline_byte_limit,
            flush_byte_threshold,
            consolidate_delta_threshold: i64::MAX,
        }
    }

    /// Override the CDC-delta consolidate-enqueue threshold sourced from config
    /// (e.g. `EngineTuning::consolidate_delta_threshold`, mirroring
    /// `RoutingTuning::consolidate_delta_threshold` on the ingest side).
    #[must_use]
    pub fn with_consolidate_delta_threshold(mut self, consolidate_delta_threshold: i64) -> Self {
        self.consolidate_delta_threshold = consolidate_delta_threshold;
        self
    }

    /// Governed typed-insert: land one IPC-encoded row + its lineage atomically.
    /// A typed insert always carries >= 1 row, so an empty/malformed `ipc` body
    /// surfaces as a decode error here — there is no meaningful empty-insert case
    /// (unlike `overwrite_table`, where empty is a valid truncate-all signal).
    pub async fn write_object(
        &self,
        table: &TableRef,
        columns: &[ColumnSpec],
        ipc: &[u8],
        event: LineageEvent,
        jobs: &[control_plane_core::NewJob],
    ) -> Result<SnapshotId, EngineServingError> {
        let (schema, batches) = datafusion_io::decode_ipc(ipc)
            .map_err(|e| EngineServingError::Engine(e.to_string()))?;
        // Call `land_cdc` directly (not the 9-arg `land`) so the action's resolved
        // downstream `jobs` ride the write's commit tx — atomic commit-or-neither.
        iceberg_landing::land_cdc(
            &self.pool,
            &self.catalog,
            table,
            columns,
            schema,
            batches,
            iceberg_landing::InlineLimits {
                inline_byte_limit: self.inline_byte_limit,
                flush_byte_threshold: self.flush_byte_threshold,
            },
            event,
            None,
            None,
            jobs,
        )
        .await
        .map_err(|e| EngineServingError::Engine(e.to_string()))
    }

    /// Stage N per-target writes + ONE lineage event in ONE transaction (one
    /// snapshot), so every target and the lineage land or roll back together — the
    /// atomic multi-object write seam for multi-step actions. Each [`StepWrite`]
    /// carries its own IPC-encoded batch, columns, and mode (append vs overwrite);
    /// the batches are decoded here (pure) and handed to
    /// [`iceberg_landing::write_steps`], which writes each target's Parquet before
    /// opening the single commit transaction. A `None` snapshot (nothing staged) maps
    /// to an error, exactly as the transform path maps `NoSnapshot`.
    pub async fn write_steps(
        &self,
        writes: &[StepWrite],
        event: LineageEvent,
        jobs: &[control_plane_core::NewJob],
    ) -> Result<SnapshotId, EngineServingError> {
        let mut steps = Vec::with_capacity(writes.len());
        for w in writes {
            // An empty `ipc` is a pure end-cap Overwrite (a multi-step Delete/Update that emptied
            // the table): stage zero batches, exactly as `overwrite_table` treats an empty body as
            // a truncate. `iceberg_landing::write_steps` end-caps that target at the shared snapshot.
            let batches = if w.ipc.is_empty() {
                Vec::new()
            } else {
                datafusion_io::decode_ipc(&w.ipc)
                    .map_err(|e| EngineServingError::Engine(e.to_string()))?
                    .1
            };
            steps.push(iceberg_landing::StepLand {
                table: w.table.clone(),
                columns: w.columns.clone(),
                batches,
                overwrite: w.overwrite,
            });
        }
        iceberg_landing::write_steps(&self.pool, &self.catalog, steps, event, jobs)
            .await
            .map_err(|e| EngineServingError::Engine(e.to_string()))
    }

    /// Copy-on-write overwrite (UPDATE/DELETE): replace the table's entire live
    /// contents with the decoded batch(es), committing `event` atomically. An empty
    /// `ipc` truncates the table (delete-all).
    pub async fn overwrite_table(
        &self,
        table: &TableRef,
        columns: &[ColumnSpec],
        ipc: &[u8],
        event: LineageEvent,
        jobs: &[control_plane_core::NewJob],
    ) -> Result<SnapshotId, EngineServingError> {
        let batches = if ipc.is_empty() {
            Vec::new()
        } else {
            datafusion_io::decode_ipc(ipc)
                .map_err(|e| EngineServingError::Engine(e.to_string()))?
                .1
        };
        iceberg_landing::overwrite_parquet_snapshot(
            &self.pool,
            &self.catalog,
            table,
            columns,
            batches,
            Some(&event),
            jobs,
        )
        .await
        .map_err(|e| EngineServingError::Engine(e.to_string()))
    }

    /// The current inline version of one identity — `id_ipc` is a one-row Arrow IPC
    /// stream holding just the id column, decoded here and handed to the postgres
    /// inline layer, which extracts the id cell and reads its live max version.
    pub async fn current_inline_version(
        &self,
        table: &TableRef,
        columns: &[ColumnSpec],
        id_column: &str,
        id_ipc: &[u8],
    ) -> Result<i64, EngineServingError> {
        let batch = datafusion_io::decode_ipc(id_ipc)
            .map_err(|e| EngineServingError::Engine(e.to_string()))?
            .1
            .into_iter()
            .next()
            .ok_or_else(|| EngineServingError::Engine("empty id batch".into()))?;
        iceberg_inline::current_inline_version(&self.pool, table, columns, id_column, &batch)
            .await
            .map_err(|e| EngineServingError::Engine(e.to_string()))
    }

    /// Write one O(change) inline delta row (a row-version or a tombstone) for a
    /// single identity, guarded by the postgres layer's per-identity CAS against
    /// `expected_version`. Returns the new snapshot id, or
    /// `EngineServingError::Conflict` if the CAS lost a race.
    #[allow(
        clippy::too_many_arguments,
        reason = "mirrors iceberg_inline::write_inline_delta's public contract — table + id-batch + version-vs-tombstone + optional before-image + lineage + CAS witness + downstream jobs; a params struct would only obscure the call site"
    )]
    pub async fn write_delta(
        &self,
        table: &TableRef,
        columns: &[ColumnSpec],
        id_column: &str,
        tombstone: bool,
        ipc: &[u8],
        before_ipc: &[u8],
        before_columns_json: &str,
        event: LineageEvent,
        expected_version: i64,
        jobs: &[control_plane_core::NewJob],
    ) -> Result<SnapshotId, EngineServingError> {
        let batch = datafusion_io::decode_ipc(ipc)
            .map_err(|e| EngineServingError::Engine(e.to_string()))?
            .1
            .into_iter()
            .next()
            .ok_or_else(|| EngineServingError::Engine("empty delta batch".into()))?;
        // Optional before-image (prior row). Empty IPC ⇒ absent (non-CDC / insert
        // paths). When present it carries its OWN ColumnSpecs, positionally aligned
        // with `before_batch`; postgres accepts but ignores the pair in this slice.
        let before = if before_ipc.is_empty() {
            None
        } else {
            let before_columns: Vec<ColumnSpec> = serde_json::from_str(before_columns_json)
                .map_err(|e| EngineServingError::Engine(format!("bad before_columns_json: {e}")))?;
            let before_batch = datafusion_io::decode_ipc(before_ipc)
                .map_err(|e| EngineServingError::Engine(e.to_string()))?
                .1
                .into_iter()
                .next()
                .ok_or_else(|| EngineServingError::Engine("empty before batch".into()))?;
            Some((before_columns, before_batch))
        };
        iceberg_inline::write_inline_delta(
            &self.pool,
            table,
            columns,
            id_column,
            tombstone,
            &batch,
            before
                .as_ref()
                .map(|(cols, batch)| (cols.as_slice(), batch)),
            event,
            expected_version,
            Some(self.consolidate_delta_threshold),
            jobs,
        )
        .await
        .map_err(|e| match e {
            ControlPlaneError::Conflict(m) => EngineServingError::Conflict(m),
            other => EngineServingError::Engine(other.to_string()),
        })
    }
}
