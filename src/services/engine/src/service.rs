//! `EngineControlService` — the tonic server that implements `EngineControl`.
//! Delegates queue operations to a `PgControlPlane` and flush_table to
//! `iceberg_flush::flush_table`.

use std::sync::Arc;

use control_plane_core::{
    Catalog, ControlPlane, Queue, RetryPolicy, RunId, TableControlPlane, TableRef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_gc::gc_table;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use engine_wire::convert;
use engine_wire::pb;
use service_runtime::ServingStore;
use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::flight::serving_status;

/// Server-side ceiling on one changelog-feed page. query-api already clamps to
/// `FEED_BATCH_LIMIT` (256) before calling, but this is a public engine RPC — an
/// unbounded `limit` would mean an unbounded `page_json` in a unary message.
const MAX_FEED_LIMIT: usize = 1024;

/// Server-side ceiling on `await_changelog`'s long-poll timeout, mirroring
/// `MAX_FEED_LIMIT`'s reasoning: query-api's own caller always passes a bounded
/// `FEED_POLL_INTERVAL` (1s) plus its own +2s client deadline slack, but this is a
/// public engine RPC — an unclamped `timeout_ms` (e.g. a caller-supplied
/// `u64::MAX`) would pin an engine task + a Postgres `LISTEN` connection
/// indefinitely.
const MAX_AWAIT_MS: u64 = 30_000;

/// A single-event page (never zero) is the byte budget's floor: a page bounded by
/// bytes must still make forward progress on a poison-pill row, or a caught-up
/// subscriber that hits one oversized event stalls forever re-requesting the same
/// unservable page — the exact bug `MAX_FEED_PAGE_BYTES` truncation exists to fix.
/// Comfortably under the engine-wire channel's `MAX_RPC_MESSAGE_BYTES` decode
/// ceiling so a truncated `page_json` (plus its `ChangelogFeedResponse` framing)
/// never itself risks tripping the channel limit.
const MAX_FEED_PAGE_BYTES: usize = 8 * 1024 * 1024;

fn status(e: control_plane_core::ControlPlaneError) -> Status {
    use control_plane_core::ControlPlaneError::*;
    match e {
        NotFound(m) => Status::not_found(m.to_string()),
        Conflict(m) => Status::aborted(m.to_string()),
        Validation(m) => Status::invalid_argument(m),
        other => Status::internal(other.to_string()),
    }
}

fn parse_id(s: &str) -> std::result::Result<control_plane_core::JobId, Status> {
    Ok(control_plane_core::JobId(s.parse().map_err(|e| {
        Status::invalid_argument(format!("bad job id: {e}"))
    })?))
}

fn parse_run_id(s: &str) -> std::result::Result<uuid::Uuid, Status> {
    uuid::Uuid::parse_str(s).map_err(|e| Status::invalid_argument(format!("bad run_id: {e}")))
}

/// `pb::MvAdvance` -> `control_plane_core::WatermarkAdvance`, shared by
/// `commit_micro_batch`'s empty-output-with-advances branch and its normal
/// landing path below.
fn convert_advances(advances: &[pb::MvAdvance]) -> Vec<control_plane_core::WatermarkAdvance> {
    advances
        .iter()
        .map(|a| control_plane_core::WatermarkAdvance {
            bucket: a.bucket,
            from: a.from,
            to: a.to,
        })
        .collect()
}

fn de_arg<T: serde::de::DeserializeOwned>(
    json: &str,
    what: &str,
) -> std::result::Result<T, Status> {
    serde_json::from_str(json)
        .map_err(|e| Status::invalid_argument(format!("bad {what}_json: {e}")))
}

fn se_out<T: serde::Serialize>(v: &T) -> std::result::Result<String, Status> {
    serde_json::to_string(v).map_err(|e| Status::internal(format!("encode failed: {e}")))
}

/// The exact byte length `page`'s `page_json` wire field would carry.
fn page_json_len(page: &control_plane_core::ChangeFeedPage) -> std::result::Result<usize, Status> {
    serde_json::to_string(page)
        .map(|s| s.len())
        .map_err(|e| Status::internal(format!("page encode: {e}")))
}

/// Build a candidate page from the first `k` of `events` (in order) plus `next`
/// recomputed from exactly those `k` events, seeded at `positions` — the same
/// seed-and-fold `decode_page` (`feed.rs`) uses, so a `k == events.len()` prefix
/// reproduces the untruncated page's `next` exactly. No slice indexing
/// (`clippy::indexing_slicing` is enforced): walks via `Iterator::take`.
fn prefix_page(
    events: &[control_plane_core::ChangeEvent],
    k: usize,
    positions: &std::collections::BTreeMap<i32, i64>,
) -> control_plane_core::ChangeFeedPage {
    let kept: Vec<control_plane_core::ChangeEvent> = events.iter().take(k).cloned().collect();
    let mut next = positions.clone();
    for ev in &kept {
        next.insert(ev.bucket, ev.offset + 1);
    }
    control_plane_core::ChangeFeedPage { events: kept, next }
}

/// If `page`'s serialized `page_json` would exceed [`MAX_FEED_PAGE_BYTES`], keep
/// only the largest PREFIX of `page.events` (in emission order) that fits, and
/// recompute `page.next` from the retained events only — seeded at `positions`
/// (the caller's PRE-scan positions, not the untruncated page's `next`), so a
/// bucket touched by NO retained event keeps its caller-supplied resume point
/// rather than skipping ahead past events that were dropped. A short page is
/// completely correct here: the feed is resumable by cursor, so the consumer
/// just gets fewer events this round and resumes from the recomputed `next`.
///
/// CRITICAL: this NEVER truncates to zero events. If even the FIRST event alone
/// exceeds the budget, that one event is returned anyway — an empty page would
/// make a caught-up subscriber long-poll, reconnect, and re-request the
/// identical unservable page forever. That deterministic stall is exactly the
/// poison-pill bug this truncation exists to fix, so a wide-row page degrades to
/// "slow" (one oversized event per round-trip), never to "stuck".
fn truncate_feed_page_to_byte_budget(
    page: &mut control_plane_core::ChangeFeedPage,
    positions: &std::collections::BTreeMap<i32, i64>,
) -> std::result::Result<(), Status> {
    if page.events.is_empty() || page_json_len(page)? <= MAX_FEED_PAGE_BYTES {
        return Ok(());
    }
    // Binary search the largest 1..=n prefix that fits. A prefix's serialized size
    // is monotonically non-decreasing in its length (strictly appending events to
    // a JSON array), so binary search over the prefix length is valid.
    let n = page.events.len();
    let mut lo = 1usize;
    let mut hi = n;
    let mut best = 1usize;
    while lo <= hi {
        #[expect(
            clippy::integer_division,
            reason = "integer midpoint is exactly what a prefix-length binary search wants; \
                      a float would need re-truncating back to usize anyway"
        )]
        let mid = lo + (hi - lo) / 2;
        let candidate = prefix_page(&page.events, mid, positions);
        if page_json_len(&candidate)? <= MAX_FEED_PAGE_BYTES {
            best = mid;
            if mid == n {
                break;
            }
            lo = mid + 1;
        } else if mid == 1 {
            // Even a single event exceeds the budget: return it anyway (see the
            // CRITICAL note above) rather than shrinking `best` below 1.
            break;
        } else {
            hi = mid - 1;
        }
    }
    *page = prefix_page(&page.events, best, positions);
    Ok(())
}

/// The engine's gRPC service implementation.
pub struct EngineControlService {
    pub cp: PgControlPlane,
    pub catalog: Arc<SqlCatalog>,
    pub pool: PgPool,
    /// Retention window for `gc_table` (from `LOOM_GC_RETENTION_SECS`).
    pub retention: std::time::Duration,
    /// Governed-write executor (relocated from query-api).
    pub writer: engine_serving::IcebergActionWriter,
    /// Inline-row bytes above which a flush-to-Parquet job is enqueued
    /// (`EngineTuning::flush_byte_threshold`) — threaded into `commit_micro_batch`'s
    /// `inline_append_mv` call, exactly as the writer's own inline-writing paths use it.
    pub flush_byte_threshold: i64,
    /// The warehouse object store + root URL, for the orphan sweep's LIST/delete.
    pub write_store: service_runtime::WriteStore,
    /// Grace window for the orphan sweep (from `LOOM_ORPHAN_SWEEP_GRACE_SECS`).
    pub orphan_sweep_grace: std::time::Duration,
    /// `Some(ServingStore { bucket, store })` for an S3 warehouse; `None` => local FS.
    /// The changelog feed's object store (the catalog is built inline from `pool`, as
    /// `list_files` already does at `:190`).
    pub serving_store: Option<ServingStore>,
}

#[tonic::async_trait]
impl pb::engine_control_server::EngineControl for EngineControlService {
    async fn dequeue(
        &self,
        req: Request<pb::DequeueRequest>,
    ) -> std::result::Result<Response<pb::DequeueResponse>, Status> {
        let r = req.into_inner();
        let job = self.cp.dequeue(&r.kinds, &r.worker).await.map_err(status)?;
        Ok(Response::new(pb::DequeueResponse {
            job: job.as_ref().map(convert::job_to_pb),
        }))
    }

    async fn complete(
        &self,
        req: Request<pb::CompleteRequest>,
    ) -> std::result::Result<Response<pb::CompleteResponse>, Status> {
        let id = parse_id(&req.into_inner().id)?;
        self.cp.complete(id).await.map_err(status)?;
        Ok(Response::new(pb::CompleteResponse {}))
    }

    async fn fail(
        &self,
        req: Request<pb::FailRequest>,
    ) -> std::result::Result<Response<pb::FailResponse>, Status> {
        let r = req.into_inner();
        let id = parse_id(&r.id)?;
        let policy: RetryPolicy = convert::retry_policy_from_pb(
            r.policy
                .ok_or_else(|| Status::invalid_argument("missing policy"))?,
        )
        .map_err(|e| Status::invalid_argument(e.0))?;
        self.cp.fail(id, &r.error, policy).await.map_err(status)?;
        Ok(Response::new(pb::FailResponse {}))
    }

    async fn heartbeat(
        &self,
        req: Request<pb::HeartbeatRequest>,
    ) -> std::result::Result<Response<pb::HeartbeatResponse>, Status> {
        let id = parse_id(&req.into_inner().id)?;
        self.cp.heartbeat(id).await.map_err(status)?;
        Ok(Response::new(pb::HeartbeatResponse {}))
    }

    async fn await_jobs(
        &self,
        req: Request<pb::AwaitJobsRequest>,
    ) -> std::result::Result<Response<pb::AwaitJobsResponse>, Status> {
        let r = req.into_inner();
        self.cp
            .await_jobs(&r.kinds, std::time::Duration::from_millis(r.timeout_ms))
            .await
            .map_err(status)?;
        Ok(Response::new(pb::AwaitJobsResponse {}))
    }

    async fn changelog_latest(
        &self,
        req: Request<pb::ChangelogLatestRequest>,
    ) -> std::result::Result<Response<pb::ChangelogLatestResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        let latest = control_plane_postgres::stream::changelog_positions_latest(&self.pool, &table)
            .await
            .map_err(status)?;
        // `None` = not a declared CDC table: a legitimate answer, not an error.
        Ok(Response::new(pb::ChangelogLatestResponse {
            present: latest.is_some(),
            positions: latest.unwrap_or_default().into_iter().collect(),
        }))
    }

    async fn changelog_feed(
        &self,
        req: Request<pb::ChangelogFeedRequest>,
    ) -> std::result::Result<Response<pb::ChangelogFeedResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        let wire: engine_wire::client::WirePolicy = serde_json::from_str(&r.policy_json)
            .map_err(|e| Status::invalid_argument(format!("bad policy_json: {e}")))?;
        // Governance is enforced HERE, engine-side, before the ordered read. The wire
        // carried the resolved policy; the subject never crossed it.
        let policy = engine_serving::TablePolicy {
            row_filters: wire.row_filters,
            denied: wire.denied.into_iter().collect(),
            masked: wire.masked.into_iter().collect(),
        };
        let positions: std::collections::BTreeMap<i32, i64> = r.positions.into_iter().collect();
        let limit = usize::try_from(r.limit)
            .unwrap_or(MAX_FEED_LIMIT)
            .min(MAX_FEED_LIMIT);

        // Fast path: a cheap indexed Postgres probe BEFORE touching object storage.
        // `GovernedTableProvider::scan` deliberately does not push the resume
        // predicate down to the Parquet scan (see `governed.rs`), so an idle 1 Hz
        // subscriber that is already caught up would otherwise re-read the WHOLE
        // changelog file tier every poll to deliver zero events. If the caller is
        // caught up in EVERY bucket, answer empty here and skip the scan entirely.
        //
        // Invariant: `positions[b]` is the next offset the caller wants to CONSUME;
        // `latest[b]` (the `changelog_positions_latest` high-water) is one PAST the
        // last committed offset for bucket `b`. Nothing is available in bucket `b`
        // iff `positions[b] >= latest[b]`. A bucket present in `latest` but ABSENT
        // from `positions` means the caller has never advanced past it — treated as
        // position 0, i.e. always behind — so a caller behind in ANY bucket falls
        // through to the real scan below. This must never suppress a real event: it
        // only short-circuits when every bucket is provably exhausted.
        if let Some(latest) =
            control_plane_postgres::stream::changelog_positions_latest(&self.pool, &table)
                .await
                .map_err(status)?
            && latest
                .iter()
                .all(|(b, hi)| positions.get(b).copied().unwrap_or(0) >= *hi)
        {
            let page_json = se_out(&control_plane_core::ChangeFeedPage {
                events: vec![],
                next: positions,
            })?;
            return Ok(Response::new(pb::ChangelogFeedResponse { page_json }));
        }

        let ice = control_plane_postgres::iceberg_catalog::IcebergCatalog::new(self.pool.clone());
        let mut page = engine_serving::changelog_feed_scan(
            &ice,
            &table,
            self.serving_store.as_ref(),
            &positions,
            limit,
            &policy,
        )
        .await
        .map_err(serving_status)?;
        truncate_feed_page_to_byte_budget(&mut page, &positions)?;
        let page_json = serde_json::to_string(&page)
            .map_err(|e| Status::internal(format!("page encode: {e}")))?;
        Ok(Response::new(pb::ChangelogFeedResponse { page_json }))
    }

    async fn await_changelog(
        &self,
        req: Request<pb::AwaitChangelogRequest>,
    ) -> std::result::Result<Response<pb::AwaitChangelogResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        // Clamp BEFORE building the Duration — an unclamped caller-supplied
        // `timeout_ms` (e.g. `u64::MAX`) must not reach `Duration::from_millis`.
        let timeout_ms = r.timeout_ms.min(MAX_AWAIT_MS);
        control_plane_postgres::stream::await_changelog(
            &self.pool,
            &table,
            std::time::Duration::from_millis(timeout_ms),
        )
        .await
        .map_err(status)?;
        Ok(Response::new(pb::AwaitChangelogResponse {}))
    }

    async fn flush_table(
        &self,
        req: Request<pb::FlushTableRequest>,
    ) -> std::result::Result<Response<pb::FlushTableResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        let snap = flush_table(
            &self.catalog,
            &self.pool,
            &table,
            RunId(uuid::Uuid::new_v4()),
        )
        .await
        .map_err(status)?;
        Ok(Response::new(pb::FlushTableResponse {
            snapshot_id: snap.map(|s| s.0),
        }))
    }

    async fn gc_table(
        &self,
        req: Request<pb::GcTableRequest>,
    ) -> std::result::Result<Response<pb::GcTableResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        let summary = gc_table(&self.catalog, &self.pool, &table, self.retention)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::GcTableResponse {
            data_file_rows: summary.data_file_rows,
            inline_rows: summary.inline_rows,
            objects_deleted: summary.objects_deleted,
            held_by_mv_floor: summary.held_by_mv_floor,
        }))
    }

    async fn sweep_orphans(
        &self,
        _req: Request<pb::SweepOrphansRequest>,
    ) -> std::result::Result<Response<pb::SweepOrphansResponse>, Status> {
        let summary = control_plane_postgres::orphan_sweep::sweep_orphans(
            &self.write_store.store,
            &self.write_store.root_url,
            &self.pool,
            self.orphan_sweep_grace,
        )
        .await
        .map_err(status)?;
        Ok(Response::new(pb::SweepOrphansResponse {
            objects_deleted: summary.objects_deleted,
            bytes_deleted: summary.bytes_deleted,
            candidates_skipped_grace: summary.candidates_skipped_grace,
        }))
    }

    async fn list_files(
        &self,
        req: Request<pb::ListFilesRequest>,
    ) -> std::result::Result<Response<pb::ListFilesResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        let ice = control_plane_postgres::iceberg_catalog::IcebergCatalog::new(self.pool.clone());
        let (files, columns_json) = match ice.current_snapshot(&table).await {
            Ok(snap) => {
                let files = ice
                    .files_with_stats(&table, snap.id)
                    .await
                    .map_err(status)?;
                let schema = ice.schema(&table, snap.id).await.map_err(status)?;
                let columns: Vec<control_plane_core::ColumnSpec> = schema
                    .columns
                    .into_iter()
                    .map(|c| control_plane_core::ColumnSpec {
                        name: c.name,
                        ty: c.ty,
                        nullable: c.nullable,
                    })
                    .collect();
                (files, Some(se_out(&columns)?))
            }
            // Absent columns_json <=> the table does not exist; a live zero-file
            // table keeps its declared schema (the table-exists discriminator).
            Err(control_plane_core::ControlPlaneError::NotFound(_)) => (Vec::new(), None),
            Err(e) => return Err(status(e)),
        };
        Ok(Response::new(pb::ListFilesResponse {
            files: files
                .into_iter()
                .map(|f| pb::FileMeta {
                    path: f.path,
                    record_count: f.record_count,
                    file_size_bytes: f.file_size_bytes,
                })
                .collect(),
            columns_json,
        }))
    }

    async fn build_vector_index(
        &self,
        req: Request<pb::BuildVectorIndexRequest>,
    ) -> std::result::Result<Response<pb::BuildVectorIndexResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
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

    async fn compact_table(
        &self,
        req: Request<pb::CompactTableRequest>,
    ) -> std::result::Result<Response<pb::CompactTableResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        let write: Vec<control_plane_core::DataFile> = r
            .write_json
            .iter()
            .map(|s| {
                serde_json::from_str(s)
                    .map_err(|e| Status::invalid_argument(format!("bad write DataFile json: {e}")))
            })
            .collect::<std::result::Result<_, _>>()?;
        let snap = control_plane_postgres::iceberg_compact::compact_table(
            &self.pool, &table, &r.expire, &write,
        )
        .await
        .map_err(status)?;
        Ok(Response::new(pb::CompactTableResponse {
            snapshot_id: snap.map(|s| s.0),
        }))
    }

    async fn consolidate_stream(
        &self,
        req: Request<pb::ConsolidateStreamRequest>,
    ) -> std::result::Result<Response<pb::ConsolidateStreamResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        let snapshot_id =
            engine_serving::consolidate_table(&self.cp, &self.catalog, &self.pool, &table)
                .await
                .map_err(serving_status)?;
        Ok(Response::new(pb::ConsolidateStreamResponse { snapshot_id }))
    }

    async fn commit_transform(
        &self,
        req: Request<pb::CommitTransformRequest>,
    ) -> std::result::Result<Response<pb::CommitTransformResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        let columns: Vec<control_plane_core::ColumnSpec> = serde_json::from_str(&r.columns_json)
            .map_err(|e| Status::invalid_argument(format!("bad columns_json: {e}")))?;
        let write: Vec<control_plane_core::DataFile> = r
            .write_json
            .iter()
            .map(|s| {
                serde_json::from_str(s)
                    .map_err(|e| Status::invalid_argument(format!("bad write DataFile json: {e}")))
            })
            .collect::<std::result::Result<_, _>>()?;
        let wire: engine_wire::convert::LineageWire = serde_json::from_str(&r.lineage_json)
            .map_err(|e| Status::invalid_argument(format!("bad lineage_json: {e}")))?;
        let lineage = control_plane_core::LineageEvent::try_from(wire)
            .map_err(|e| Status::invalid_argument(format!("bad lineage: {e}")))?;
        // The same tx sequence the transform binary ran locally (run.rs step 6):
        // create_table (idempotent) + append/replace + emit lineage, one commit.
        let icp = control_plane_postgres::iceberg_control_plane::IcebergControlPlane::new(
            self.cp.clone(),
            self.catalog.clone(),
        );
        let mut tx = icp.begin_table().await.map_err(status)?;
        tx.create_table(&table, &columns).await.map_err(status)?;
        if r.replace {
            tx.replace_files(&table, &write).await.map_err(status)?;
        } else {
            tx.append_files(&table, &write).await.map_err(status)?;
        }
        tx.emit(lineage).await.map_err(status)?;
        if let Some(rid) = r.run_id.as_deref() {
            let rid = parse_run_id(rid)?;
            tx.mark_run_succeeded(rid).await.map_err(status)?;
        }
        let snap = tx.commit().await.map_err(status)?;
        Ok(Response::new(pb::CommitTransformResponse {
            snapshot_id: snap.map(|s| s.0),
        }))
    }

    /// The atomicity heart of the stream-continuous slice: inline-land a
    /// micro-batch result declared a log stream table, CAS-advance the source's
    /// per-bucket watermark, and mark the driving run succeeded — one Postgres
    /// transaction (`inline_append_mv`). Empty output (`ipc` empty) lands and
    /// declares nothing: with advances present (a filtering micro-batch) it still
    /// CAS-advances the watermark + marks the run in one tx; with no advances it
    /// just closes `run_id`.
    async fn commit_micro_batch(
        &self,
        req: Request<pb::CommitMicroBatchRequest>,
    ) -> std::result::Result<Response<pb::CommitMicroBatchResponse>, Status> {
        let r = req.into_inner();
        let run_id = r.run_id.as_deref().map(parse_run_id).transpose()?;

        if r.ipc.is_empty() {
            // No output rows to land. Two sub-cases, both land NOTHING and
            // declare NO output table:
            //  - advances present  => a filtering micro-batch consumed a delta
            //    but produced nothing; advance the watermark + mark the run
            //    ATOMICALLY (else the consumed delta reprocesses forever).
            //  - advances empty     => an empty source delta / spurious wakeup;
            //    just mark the run.
            let advances = convert_advances(&r.advances);
            if advances.is_empty() {
                if let Some(rid) = run_id {
                    self.cp
                        .transforms()
                        .finish_run(
                            rid,
                            control_plane_core::RunOutcome::Succeeded { snapshot_id: 0 },
                        )
                        .await
                        .map_err(status)?;
                }
            } else {
                // Resolve the source tid for the watermark key (absent =>
                // invalid_argument, same as the landing path below).
                let mut conn = self
                    .pool
                    .acquire()
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                let source_table_id = control_plane_postgres::iceberg_mirror::live_table_id(
                    &mut conn,
                    &r.source_schema,
                    &r.source_name,
                )
                .await
                .map_err(status)?
                .ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "commit_micro_batch: unknown source table {}.{}",
                        r.source_schema, r.source_name
                    ))
                })?;
                drop(conn);
                control_plane_postgres::iceberg_inline::advance_mv_watermark_only(
                    &self.pool,
                    &control_plane_postgres::iceberg_inline::MvCommit {
                        mv: r.mv,
                        source_table_id,
                        advances,
                        run_id,
                    },
                )
                .await
                .map_err(status)?;
            }
            return Ok(Response::new(pb::CommitMicroBatchResponse {
                snapshot_id: None,
            }));
        }

        let columns: Vec<control_plane_core::ColumnSpec> = serde_json::from_str(&r.columns_json)
            .map_err(|e| Status::invalid_argument(format!("bad columns_json: {e}")))?;
        let wire: engine_wire::convert::LineageWire = serde_json::from_str(&r.lineage_json)
            .map_err(|e| Status::invalid_argument(format!("bad lineage_json: {e}")))?;
        let lineage = control_plane_core::LineageEvent::try_from(wire)
            .map_err(|e| Status::invalid_argument(format!("bad lineage: {e}")))?;

        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let source_table_id = control_plane_postgres::iceberg_mirror::live_table_id(
            &mut conn,
            &r.source_schema,
            &r.source_name,
        )
        .await
        .map_err(status)?
        .ok_or_else(|| {
            Status::invalid_argument(format!(
                "commit_micro_batch: unknown source table {}.{}",
                r.source_schema, r.source_name
            ))
        })?;
        drop(conn);

        let (ipc_schema, ipc_batches) = datafusion_io::decode_ipc(&r.ipc)
            .map_err(|e| Status::invalid_argument(format!("bad ipc: {e}")))?;
        let batch = arrow_select::concat::concat_batches(&ipc_schema, &ipc_batches)
            .map_err(|e| Status::invalid_argument(format!("bad ipc: {e}")))?;

        let advances = convert_advances(&r.advances);

        let out_table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        let mv_commit = control_plane_postgres::iceberg_inline::MvCommit {
            mv: r.mv,
            source_table_id,
            advances,
            run_id,
        };

        let snap = control_plane_postgres::iceberg_inline::inline_append_mv(
            &self.pool,
            &out_table,
            &columns,
            &batch,
            lineage,
            Some(self.flush_byte_threshold),
            r.buckets,
            &mv_commit,
        )
        .await
        .map_err(status)?;

        Ok(Response::new(pb::CommitMicroBatchResponse {
            snapshot_id: Some(snap.0),
        }))
    }

    async fn mark_run_running(
        &self,
        req: Request<pb::MarkRunRunningRequest>,
    ) -> std::result::Result<Response<pb::MarkRunRunningResponse>, Status> {
        let rid = parse_run_id(&req.into_inner().run_id)?;
        self.cp
            .transforms()
            .mark_run_running(rid)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::MarkRunRunningResponse {}))
    }

    async fn finish_run_failed(
        &self,
        req: Request<pb::FinishRunFailedRequest>,
    ) -> std::result::Result<Response<pb::FinishRunFailedResponse>, Status> {
        let r = req.into_inner();
        let rid = parse_run_id(&r.run_id)?;
        let outcome = if r.terminal {
            control_plane_core::RunOutcome::Failed { error: r.error }
        } else {
            control_plane_core::RunOutcome::RetryQueued { error: r.error }
        };
        self.cp
            .transforms()
            .finish_run(rid, outcome)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::FinishRunFailedResponse {}))
    }

    async fn write_object(
        &self,
        req: Request<pb::WriteObjectRequest>,
    ) -> std::result::Result<Response<pb::WriteObjectResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        let columns: Vec<control_plane_core::ColumnSpec> = serde_json::from_str(&r.columns_json)
            .map_err(|e| Status::invalid_argument(format!("bad columns_json: {e}")))?;
        let wire: engine_wire::convert::LineageWire = serde_json::from_str(&r.lineage_json)
            .map_err(|e| Status::invalid_argument(format!("bad lineage_json: {e}")))?;
        let event: control_plane_core::LineageEvent = wire
            .try_into()
            .map_err(|e: String| Status::invalid_argument(format!("bad lineage: {e}")))?;
        let jobs: Vec<control_plane_core::NewJob> = if r.jobs_json.is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&r.jobs_json)
                .map_err(|e| Status::invalid_argument(format!("bad jobs_json: {e}")))?
        };
        let snap = self
            .writer
            .write_object(&table, &columns, &r.ipc, event, &jobs)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::WriteObjectResponse {
            snapshot_id: snap.0,
        }))
    }

    async fn write_steps(
        &self,
        req: Request<pb::WriteStepsRequest>,
    ) -> std::result::Result<Response<pb::WriteStepsResponse>, Status> {
        let r = req.into_inner();
        let mut writes = Vec::with_capacity(r.steps.len());
        for s in r.steps {
            let columns: Vec<control_plane_core::ColumnSpec> =
                serde_json::from_str(&s.columns_json)
                    .map_err(|e| Status::invalid_argument(format!("bad columns_json: {e}")))?;
            writes.push(engine_serving::StepWrite {
                table: TableRef {
                    schema: s.schema,
                    name: s.name,
                },
                columns,
                ipc: s.ipc,
                overwrite: s.overwrite,
            });
        }
        let wire: engine_wire::convert::LineageWire = serde_json::from_str(&r.lineage_json)
            .map_err(|e| Status::invalid_argument(format!("bad lineage_json: {e}")))?;
        let event: control_plane_core::LineageEvent = wire
            .try_into()
            .map_err(|e: String| Status::invalid_argument(format!("bad lineage: {e}")))?;
        let jobs: Vec<control_plane_core::NewJob> = if r.jobs_json.is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&r.jobs_json)
                .map_err(|e| Status::invalid_argument(format!("bad jobs_json: {e}")))?
        };
        let snap = self
            .writer
            .write_steps(&writes, event, &jobs)
            .await
            .map_err(|e| match e {
                engine_serving::EngineServingError::Validation(m) => Status::invalid_argument(m),
                other => Status::internal(other.to_string()),
            })?;
        Ok(Response::new(pb::WriteStepsResponse {
            snapshot_id: snap.0,
        }))
    }

    async fn overwrite_table(
        &self,
        req: Request<pb::OverwriteTableRequest>,
    ) -> std::result::Result<Response<pb::OverwriteTableResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        let columns: Vec<control_plane_core::ColumnSpec> = serde_json::from_str(&r.columns_json)
            .map_err(|e| Status::invalid_argument(format!("bad columns_json: {e}")))?;
        let wire: engine_wire::convert::LineageWire = serde_json::from_str(&r.lineage_json)
            .map_err(|e| Status::invalid_argument(format!("bad lineage_json: {e}")))?;
        let event: control_plane_core::LineageEvent = wire
            .try_into()
            .map_err(|e: String| Status::invalid_argument(format!("bad lineage: {e}")))?;
        let jobs: Vec<control_plane_core::NewJob> = if r.jobs_json.is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&r.jobs_json)
                .map_err(|e| Status::invalid_argument(format!("bad jobs_json: {e}")))?
        };
        let snap = self
            .writer
            .overwrite_table(&table, &columns, &r.ipc, event, &jobs)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::OverwriteTableResponse {
            snapshot_id: snap.0,
        }))
    }

    async fn current_inline_version(
        &self,
        req: Request<pb::CurrentInlineVersionRequest>,
    ) -> std::result::Result<Response<pb::CurrentInlineVersionResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        let columns: Vec<control_plane_core::ColumnSpec> = serde_json::from_str(&r.columns_json)
            .map_err(|e| Status::invalid_argument(format!("bad columns_json: {e}")))?;
        let version = self
            .writer
            .current_inline_version(&table, &columns, &r.id_column, &r.id_ipc)
            .await
            .map_err(serving_status)?;
        Ok(Response::new(pb::CurrentInlineVersionResponse { version }))
    }

    async fn write_delta(
        &self,
        req: Request<pb::WriteDeltaRequest>,
    ) -> std::result::Result<Response<pb::WriteDeltaResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        let columns: Vec<control_plane_core::ColumnSpec> = serde_json::from_str(&r.columns_json)
            .map_err(|e| Status::invalid_argument(format!("bad columns_json: {e}")))?;
        let wire: engine_wire::convert::LineageWire = serde_json::from_str(&r.lineage_json)
            .map_err(|e| Status::invalid_argument(format!("bad lineage_json: {e}")))?;
        let event: control_plane_core::LineageEvent = wire
            .try_into()
            .map_err(|e: String| Status::invalid_argument(format!("bad lineage: {e}")))?;
        let jobs: Vec<control_plane_core::NewJob> = if r.jobs_json.is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&r.jobs_json)
                .map_err(|e| Status::invalid_argument(format!("bad jobs_json: {e}")))?
        };
        let snap = self
            .writer
            .write_delta(
                &table,
                &columns,
                &r.id_column,
                r.tombstone,
                &r.ipc,
                &r.before_ipc,
                &r.before_columns_json,
                event,
                r.expected_version,
                &jobs,
            )
            .await
            .map_err(serving_status)?;
        Ok(Response::new(pb::WriteDeltaResponse {
            snapshot_id: snap.0,
        }))
    }

    // ---- Governance-read handlers ----

    async fn check(
        &self,
        req: Request<pb::CheckRequest>,
    ) -> std::result::Result<Response<pb::CheckResponse>, Status> {
        let r = req.into_inner();
        let subject: control_plane_core::SubjectId = de_arg(&r.subject_json, "subject")?;
        let action: control_plane_core::Action = de_arg(&r.action_json, "action")?;
        let target: control_plane_core::PolicyTarget = de_arg(&r.target_json, "target")?;
        let decision = self
            .cp
            .acl()
            .check(&subject, action, &target)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::CheckResponse {
            decision_json: se_out(&decision)?,
        }))
    }

    async fn policies_for(
        &self,
        req: Request<pb::PoliciesForRequest>,
    ) -> std::result::Result<Response<pb::PoliciesForResponse>, Status> {
        let r = req.into_inner();
        let subject: control_plane_core::SubjectId = de_arg(&r.subject_json, "subject")?;
        let action: control_plane_core::Action = de_arg(&r.action_json, "action")?;
        let target: control_plane_core::PolicyTarget = de_arg(&r.target_json, "target")?;
        let page: control_plane_core::PageReq = de_arg(&r.page_json, "page")?;
        let policies = self
            .cp
            .acl()
            .policies_for(&subject, action, &target, page)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::PoliciesForResponse {
            page_json: se_out(&policies)?,
        }))
    }

    async fn get_type(
        &self,
        req: Request<pb::GetTypeRequest>,
    ) -> std::result::Result<Response<pb::GetTypeResponse>, Status> {
        let name = control_plane_core::TypeName(req.into_inner().type_name);
        let ty = self.cp.ontology().get_type(&name).await.map_err(status)?;
        Ok(Response::new(pb::GetTypeResponse {
            object_type_json: se_out(&ty)?,
        }))
    }

    async fn resolve(
        &self,
        req: Request<pb::ResolveRequest>,
    ) -> std::result::Result<Response<pb::ResolveResponse>, Status> {
        let name = control_plane_core::TypeName(req.into_inner().type_name);
        let table = self.cp.ontology().resolve(&name).await.map_err(status)?;
        Ok(Response::new(pb::ResolveResponse {
            table_ref_json: se_out(&table)?,
        }))
    }

    async fn links(
        &self,
        req: Request<pb::LinksRequest>,
    ) -> std::result::Result<Response<pb::LinksResponse>, Status> {
        let r = req.into_inner();
        let name = control_plane_core::TypeName(r.type_name);
        let page: control_plane_core::PageReq = de_arg(&r.page_json, "page")?;
        let links = self
            .cp
            .ontology()
            .links(&name, page)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::LinksResponse {
            page_json: se_out(&links)?,
        }))
    }

    async fn links_to(
        &self,
        req: Request<pb::LinksToRequest>,
    ) -> std::result::Result<Response<pb::LinksToResponse>, Status> {
        let r = req.into_inner();
        let name = control_plane_core::TypeName(r.type_name);
        let page: control_plane_core::PageReq = de_arg(&r.page_json, "page")?;
        let links = self
            .cp
            .ontology()
            .links_to(&name, page)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::LinksToResponse {
            page_json: se_out(&links)?,
        }))
    }

    async fn list_types(
        &self,
        req: Request<pb::ListTypesRequest>,
    ) -> std::result::Result<Response<pb::ListTypesResponse>, Status> {
        let page: control_plane_core::PageReq = de_arg(&req.into_inner().page_json, "page")?;
        let types = self.cp.ontology().list_types(page).await.map_err(status)?;
        Ok(Response::new(pb::ListTypesResponse {
            page_json: se_out(&types)?,
        }))
    }

    async fn list_actions(
        &self,
        req: Request<pb::ListActionsRequest>,
    ) -> std::result::Result<Response<pb::ListActionsResponse>, Status> {
        let page: control_plane_core::PageReq = de_arg(&req.into_inner().page_json, "page")?;
        let actions = self
            .cp
            .ontology()
            .list_actions(page)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::ListActionsResponse {
            page_json: se_out(&actions)?,
        }))
    }

    async fn get_action(
        &self,
        req: Request<pb::GetActionRequest>,
    ) -> std::result::Result<Response<pb::GetActionResponse>, Status> {
        let name = control_plane_core::ActionName(req.into_inner().action_name);
        let action = self.cp.ontology().get_action(&name).await.map_err(status)?;
        Ok(Response::new(pb::GetActionResponse {
            action_def_json: se_out(&action)?,
        }))
    }

    async fn vector_indexes_for(
        &self,
        req: Request<pb::VectorIndexesForRequest>,
    ) -> std::result::Result<Response<pb::VectorIndexesForResponse>, Status> {
        let name = control_plane_core::TypeName(req.into_inner().type_name);
        let indexes = self
            .cp
            .ontology()
            .vector_indexes_for(&name)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::VectorIndexesForResponse {
            indexes_json: se_out(&indexes)?,
        }))
    }

    async fn get_vector_index(
        &self,
        req: Request<pb::GetVectorIndexRequest>,
    ) -> std::result::Result<Response<pb::GetVectorIndexResponse>, Status> {
        let r = req.into_inner();
        let name = control_plane_core::TypeName(r.type_name);
        let index = self
            .cp
            .ontology()
            .get_vector_index(&name, &r.name)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::GetVectorIndexResponse {
            index_json: se_out(&index)?,
        }))
    }
}
