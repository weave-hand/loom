//! The third GC source: an orphaned-object sweep. `gc_table` reclaims only
//! MIRROR-REFERENCED dead bytes; nothing else ever asks "what object does no
//! mirror row reference?". Orphans accrue from write-then-commit crashes (every
//! landing path writes Parquet pre-tx) and commit-then-delete degradations (a
//! failed post-commit object delete is logged and left behind — the
//! "already-deferred orphaned-Parquet class" `iceberg_gc` names).
//!
//! `sweep_orphans` LISTs the warehouse, diffs the **pattern-scoped** data objects
//! (`*.parquet` + `*.puffin` only — Iceberg metadata/manifests are excluded by
//! scope, never diffed) against every mirror-referenced path, and deletes the
//! unreferenced remainder older than a write-race grace window.
//!
//! ## Safety (why this cannot delete a live file)
//! 1. **Pattern scoping** — metadata/manifest `.json`/`.avro` are structurally
//!    unreachable (never listed as candidates).
//! 2. **Reference over-approximation** — the reference set is EVERY `data_file.path`
//!    (any `end_snapshot`, live and historical-in-window) + every
//!    `vector_index.puffin_path`, across all tables incl. dropped-but-unreclaimed
//!    incarnations, so a still-retained file is always referenced.
//! 3. **LIST-before-read ordering** — a commit racing the sweep lands its
//!    reference row before the diff reads it (LIST happens first; the later
//!    reference read sees the new row).
//! 4. **Grace window** — an uncommitted in-flight file is younger than the grace.
//!
//! ## No transaction around object I/O
//! Reference reads are plain `SELECT`s; deletes go straight to the object store
//! outside any Postgres tx (the same no-store-I/O-in-a-tx rule `gc_table` follows).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use control_plane_core::{ControlPlaneError, Result};
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use sqlx::PgPool;
use time::OffsetDateTime;

use crate::backend;

/// Counts of what a `sweep_orphans` run reclaimed, for observability and tests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepSummary {
    pub objects_deleted: u64,
    pub bytes_deleted: u64,
    /// Unreferenced candidates younger than the grace window, held this run (they
    /// reclaim on a later sweep once aged past grace).
    pub candidates_skipped_grace: u64,
}

/// The sweep's data-pattern scope: `*.parquet` and `*.puffin` only. Everything
/// else under the warehouse — Iceberg metadata JSON, manifest lists, manifest
/// `.avro`, version hints, unknown files — is excluded by scope and can never be
/// flagged as an orphan.
fn in_scope(key: &str) -> bool {
    key.ends_with(".parquet") || key.ends_with(".puffin")
}

/// Normalize an absolute mirror path (`s3://bucket/key` or `file:///abs/key`) to
/// the store-relative key that `ObjectMeta.location` yields: strip the store's
/// `root_url` prefix, then any leading `/`.
///
/// A reference path that does NOT start with `root_url` cannot be normalized to a
/// comparable key, so it silently fails to protect a matching object — a
/// mass-delete precursor. Every verified caller's `root_url` prefixes every stored
/// path, so this should never fire; if it does, log loudly rather than silently
/// dropping the reference's protection.
fn to_store_key(root_url: &str, abs: &str) -> String {
    match abs.strip_prefix(root_url) {
        Some(rel) => rel.trim_start_matches('/').to_string(),
        None => {
            tracing::warn!(
                path = %abs,
                root_url = %root_url,
                "orphan-sweep: reference path does not start with the warehouse root_url; \
                 it cannot normalize to a store key and may fail to protect a matching object \
                 (possible warehouse-root/path mismatch)"
            );
            abs.trim_start_matches('/').to_string()
        }
    }
}

/// Object-store error → opaque backend error (transport/IO faults have no more
/// specific class here).
fn store_err<E: std::fmt::Display>(e: E) -> ControlPlaneError {
    ControlPlaneError::Backend(e.to_string().into())
}

/// Sweep orphaned data objects from the warehouse `store` (rooted at `root_url`).
///
/// LIST-then-read ordering + the `grace` window make concurrent writers/GC safe
/// (see module docs). Never opens a Postgres transaction around the object
/// deletes. Idempotent: a missing object deletes as success, and a failed delete
/// is retried by the next scheduled run.
pub async fn sweep_orphans(
    store: &Arc<dyn ObjectStore>,
    root_url: &str,
    pool: &PgPool,
    grace: Duration,
) -> Result<SweepSummary> {
    // 1. LIST the warehouse; keep in-scope data objects with their key/age/size.
    let mut listed: Vec<(object_store::path::Path, i64, u64)> = Vec::new();
    let mut stream = store.list(None);
    while let Some(item) = stream.next().await {
        let meta = item.map_err(store_err)?;
        if in_scope(meta.location.as_ref()) {
            // `ObjectMeta.size` is already `u64` in object_store 0.13 — no cast
            // (an `as u64` here trips `clippy::unnecessary_cast`, which is enforced).
            listed.push((
                meta.location.clone(),
                meta.last_modified.timestamp_millis(),
                meta.size,
            ));
        }
    }

    // 2. Read the reference set: ALL data_file paths + ALL puffin paths (no
    //    end_snapshot filter — historical-in-window and dropped-in-window rows
    //    included). Normalize both to store-relative keys before comparison.
    let mut referenced: HashSet<String> = HashSet::new();
    for p in sqlx::query_scalar!("select path from iceberg_mirror.data_file")
        .fetch_all(pool)
        .await
        .map_err(backend)?
    {
        referenced.insert(to_store_key(root_url, &p));
    }
    for p in sqlx::query_scalar!("select puffin_path from iceberg_mirror.vector_index")
        .fetch_all(pool)
        .await
        .map_err(backend)?
    {
        referenced.insert(to_store_key(root_url, &p));
    }

    // 3+4. Diff (listed − referenced) + grace filter, then 5. delete survivors
    //      outside any tx. Grace is applied at ms precision so a just-written file
    //      is deterministically past a zero grace.
    #[expect(
        clippy::integer_division,
        reason = "ms-precision truncation is intentional; sub-millisecond precision is not needed for grace-window comparison"
    )]
    let now_ms: i64 = (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
    let cutoff_ms = now_ms - grace.as_millis() as i64;
    let mut summary = SweepSummary::default();
    for (loc, modified_ms, size) in listed {
        let key = loc.as_ref();
        if referenced.contains(key) {
            continue; // referenced — never a candidate
        }
        if modified_ms >= cutoff_ms {
            summary.candidates_skipped_grace += 1;
            continue; // orphan, but younger than grace — hold
        }
        match store.delete(&loc).await {
            Ok(()) => {
                summary.objects_deleted += 1;
                summary.bytes_deleted += size;
                tracing::info!(
                    path = %key,
                    size,
                    age_ms = now_ms - modified_ms,
                    "orphan-sweep: deleted unreferenced object"
                );
            }
            // A concurrent sweep/GC already removed it — idempotent success.
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => tracing::warn!(
                error = %e,
                path = %key,
                "orphan-sweep: failed to delete; leaving for next run"
            ),
        }
    }
    tracing::info!(
        objects_deleted = summary.objects_deleted,
        bytes_deleted = summary.bytes_deleted,
        candidates_skipped_grace = summary.candidates_skipped_grace,
        "orphan-sweep: complete"
    );
    Ok(summary)
}
