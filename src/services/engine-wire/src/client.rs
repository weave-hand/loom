//! `GrpcQueueClient` — a `Queue` impl that tunnels queue operations (and
//! `flush_table`) over the `EngineControl` tonic service via a unix-domain socket.

use control_plane_core::{
    Action, ActionDef, ActionName, ControlPlaneError, Decision, Job, JobId, JobSchedule,
    JobScheduleStatus, LinkDef, NewJob, ObjectType, Page, PageReq, Policy, PolicyTarget, Queue,
    Result, RetryPolicy, ScheduleFired, SubjectId, TableRef, TypeName, VectorIndexDef,
};
use tonic::transport::Channel;

use crate::convert;
use crate::pb;
use crate::pb::engine_control_client::EngineControlClient;

/// Flatten ANY error to an opaque `ControlPlaneError::Backend` string.
/// WARNING: class-erasing — after road-engine-wire-dedup this is legitimate
/// only for transport faults and planes with no error classes; class-carrying
/// planes must use [`cp_status`] (governance) / `sql_status` (Flight SQL).
pub(crate) fn be<E: std::fmt::Display>(e: E) -> ControlPlaneError {
    ControlPlaneError::Backend(e.to_string().into())
}

/// Map a tonic [`tonic::Status`] from an `EngineControl` governance RPC back to a
/// [`ControlPlaneError`], inverting the engine-side `status` mapping so the error
/// *kind* (notably `NotFound`) survives the wire round-trip.
#[must_use]
pub fn cp_status(s: tonic::Status) -> ControlPlaneError {
    use tonic::Code;
    match s.code() {
        Code::NotFound => ControlPlaneError::NotFound(s.message().to_string()),
        Code::Aborted => ControlPlaneError::Conflict(s.message().to_string()),
        Code::InvalidArgument => ControlPlaneError::Validation(s.message().to_string()),
        other => ControlPlaneError::Backend(
            format!("engine governance RPC failed ({other:?}): {}", s.message()).into(),
        ),
    }
}

/// Map a tonic [`tonic::Status`] from a data-plane write RPC (`write_delta`) back
/// to a [`ControlPlaneError`], preserving `Aborted` as `Conflict` (rather than
/// collapsing it into an opaque [`be`] backend string) so a CAS loss on the
/// inline-delta path surfaces as a class the caller can retry on, not a generic
/// failure. Mirrors [`cp_status`]'s `Aborted` arm.
#[must_use]
pub fn write_status(s: tonic::Status) -> ControlPlaneError {
    match s.code() {
        tonic::Code::Aborted => ControlPlaneError::Conflict(s.message().to_string()),
        _ => be(s),
    }
}

/// Map a tonic [`tonic::Status`] from the engine's Flight **SQL** plane back to a
/// [`ControlPlaneError`], inverting the engine-side `serving_status` mapping so the
/// planning-error class survives the wire: `InvalidArgument` (the engine classifies
/// only `ctx.sql()` planning faults this way on the SQL plane) -> `Validation`;
/// everything else stays an opaque `Backend`. Mirrors [`cp_status`].
#[must_use]
pub fn sql_status(s: tonic::Status) -> ControlPlaneError {
    match s.code() {
        tonic::Code::InvalidArgument => ControlPlaneError::Validation(s.message().to_string()),
        _ => be(s),
    }
}

/// [`GrpcQueueClient::changelog_latest`]'s status-preserving error: distinguishes an
/// engine that does not (yet) implement `ChangelogLatest` — a rolling-deploy skew,
/// where query-api is ahead of the engine — from a genuine backend fault, so the
/// caller can answer 501 rather than 500. `changelog_latest` deliberately does NOT
/// return a plain [`ControlPlaneError`] (whose `Backend` variant would erase the gRPC
/// status class, as [`be`] does for every other RPC on this client) because this is
/// the one changelog RPC query-api's `http.rs` needs to tell "unsupported" apart from
/// "broken" on. Mirrors `engine-wire::flight::vector_search`'s `VectorSearchError`,
/// which preserves `tonic::Code` the same way for a Flight-plane RPC.
#[derive(Debug, thiserror::Error)]
pub enum ChangelogLatestError {
    /// The engine returned `Unimplemented` — it does not carry this RPC yet.
    #[error("changelog feed is not implemented by this engine")]
    Unimplemented,
    /// Any other transport/backend fault, opaque like every other [`be`]-mapped RPC.
    #[error(transparent)]
    Backend(ControlPlaneError),
}

/// Ceiling on a single `EngineControl` gRPC message, both directions. tonic's
/// built-in default is 4 MiB to *decode* but UNBOUNDED to *encode* — so without an
/// explicit ceiling here the engine will happily encode a `page_json` the client
/// then fails to decode. The changelog feed's `page_json` (a serde `ChangeFeedPage`)
/// is the only payload on this channel whose width is caller-influenced (a wide-row
/// CDC type) rather than bounded by loom's own framing, so this must exceed the
/// engine's `MAX_FEED_PAGE_BYTES` truncation budget with headroom to spare, while
/// still capping the channel so a bug elsewhere can't grow a message unboundedly.
const MAX_RPC_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Decode a serde-JSON governance payload, mapping decode failure to `Serialization`.
fn de<T: serde::de::DeserializeOwned>(json: &str) -> Result<T> {
    serde_json::from_str(json).map_err(|e| ControlPlaneError::Serialization(e.to_string()))
}

/// Encode a governance argument to serde-JSON, mapping failure to `Serialization`.
fn se<T: serde::Serialize>(v: &T) -> Result<String> {
    serde_json::to_string(v).map_err(|e| ControlPlaneError::Serialization(e.to_string()))
}

/// Expand a governance-read RPC method. Every governance getter shares one body
/// shape — build the pb request, call the RPC, map `Status` via [`cp_status`]
/// (class-preserving), JSON-decode the named response field via `de` — so the
/// macro takes the method name + args, the RPC and response-field names, and the
/// request expression, and generates exactly that body. Data-plane RPCs
/// (flush/gc/write/…) stay hand-written: different error mapping (`be`) and
/// typed non-JSON responses.
///
/// Matcher shape note: the argument list is captured as `$($args:tt)*` and the
/// request as one trailing `$req:expr` because the naive per-field fragments
/// (`$ty:ty` before `)`, `$v:expr` before `}`) violate `macro_rules!`
/// follow-set rules.
macro_rules! gov_rpc {
    (
        $(#[$meta:meta])*
        fn $name:ident($($args:tt)*) -> $ret:ty;
        rpc $rpc:ident, out $out:ident, req $req:expr
    ) => {
        $(#[$meta])*
        pub async fn $name(&self, $($args)*) -> Result<$ret> {
            let resp = self
                .inner
                .clone()
                .$rpc($req)
                .await
                .map_err(cp_status)?
                .into_inner();
            de(&resp.$out)
        }
    };
}

/// A table's live file set plus its declared schema. `columns` is `None` iff the
/// table does not exist (the wire's `columns_json` was absent) — the discriminator
/// that lets a live zero-file table register as an empty relation.
#[derive(Debug, Clone, PartialEq)]
pub struct TableFiles {
    pub files: Vec<control_plane_core::FileRef>,
    pub columns: Option<Vec<control_plane_core::ColumnSpec>>,
}

/// The caller-resolved change-feed policy as it crosses the wire: row filters plus the
/// denied/masked column names. Mirrors query-api's `ChangeFeedPolicy` (which engine-wire
/// must not depend on); the engine converts it to `engine_serving::TablePolicy` and
/// enforces it there. The wire carries the RESOLVED policy, never the subject.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct WirePolicy {
    pub row_filters: Vec<control_plane_core::RowFilter>,
    pub denied: Vec<String>,
    pub masked: Vec<String>,
}

/// A cloneable gRPC client for the engine's `EngineControl` service.
/// Connects over a unix-domain socket; implements [`Queue`] by delegating to the
/// remote server. `enqueue` is intentionally unsupported — workers don't enqueue.
#[derive(Clone)]
pub struct GrpcQueueClient {
    inner: EngineControlClient<Channel>,
}

impl GrpcQueueClient {
    /// Connect to the engine's `EngineControl` service at the given UDS path.
    pub async fn connect(socket: impl Into<String>) -> Result<Self> {
        let channel = crate::uds_channel(socket.into()).await?;
        Ok(Self {
            inner: EngineControlClient::new(channel)
                .max_decoding_message_size(MAX_RPC_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_RPC_MESSAGE_BYTES),
        })
    }

    /// Flush a table by schema + name; returns the new snapshot id (or `None` if
    /// there were no live inline rows to flush).
    pub async fn flush_table(&self, schema: String, name: String) -> Result<Option<i64>> {
        let resp = self
            .inner
            .clone()
            .flush_table(pb::FlushTableRequest { schema, name })
            .await
            .map_err(be)?
            .into_inner();
        Ok(resp.snapshot_id)
    }

    /// The per-bucket high-water offsets of a declared CDC table's changelog, or `None`
    /// when it is not a declared CDC table (the "is this subscribable" probe — absence
    /// is not an error).
    pub async fn changelog_latest(
        &self,
        schema: String,
        name: String,
    ) -> std::result::Result<Option<std::collections::BTreeMap<i32, i64>>, ChangelogLatestError>
    {
        let resp = self
            .inner
            .clone()
            .changelog_latest(pb::ChangelogLatestRequest { schema, name })
            .await
            .map_err(|s: tonic::Status| match s.code() {
                tonic::Code::Unimplemented => ChangelogLatestError::Unimplemented,
                _ => ChangelogLatestError::Backend(be(s)),
            })?
            .into_inner();
        if resp.present {
            Ok(Some(resp.positions.into_iter().collect()))
        } else {
            Ok(None)
        }
    }

    /// One bounded, governed, ordered page of a CDC table's changelog feed from
    /// per-bucket `positions`. `policy` is the caller-RESOLVED governance policy; the
    /// engine enforces it before the ordered read.
    pub async fn changelog_feed(
        &self,
        schema: String,
        name: String,
        positions: &std::collections::BTreeMap<i32, i64>,
        limit: u64,
        policy: &WirePolicy,
    ) -> Result<control_plane_core::ChangeFeedPage> {
        let resp = self
            .inner
            .clone()
            .changelog_feed(pb::ChangelogFeedRequest {
                schema,
                name,
                positions: positions.iter().map(|(b, o)| (*b, *o)).collect(),
                limit,
                policy_json: se(policy)?,
            })
            .await
            .map_err(be)?
            .into_inner();
        de(&resp.page_json)
    }

    /// Block until a CDC write commits against the table, or `timeout` elapses. Ok on
    /// timeout (never an error).
    pub async fn await_changelog(
        &self,
        schema: String,
        name: String,
        timeout: std::time::Duration,
    ) -> Result<()> {
        let mut req = tonic::Request::new(pb::AwaitChangelogRequest {
            schema,
            name,
            timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        });
        // The client deadline must EXCEED the server's long-poll timeout, or we get a
        // spurious DeadlineExceeded before the server returns normally. Same +2s as
        // `await_jobs` (`:688-698`).
        req.set_timeout(timeout + std::time::Duration::from_secs(2));
        self.inner.clone().await_changelog(req).await.map_err(be)?;
        Ok(())
    }

    /// GC a table by schema + name; returns the reclaim counts
    /// `(data_file_rows, inline_rows, objects_deleted)`.
    pub async fn gc_table(&self, schema: String, name: String) -> Result<(u64, u64, u64)> {
        let resp = self
            .inner
            .clone()
            .gc_table(pb::GcTableRequest { schema, name })
            .await
            .map_err(be)?
            .into_inner();
        Ok((resp.data_file_rows, resp.inline_rows, resp.objects_deleted))
    }

    /// Sweep orphaned warehouse objects (no mirror row references them and older
    /// than the engine's grace window). Returns
    /// `(objects_deleted, bytes_deleted, candidates_skipped_grace)`.
    pub async fn sweep_orphans(&self) -> Result<(u64, u64, u64)> {
        let resp = self
            .inner
            .clone()
            .sweep_orphans(pb::SweepOrphansRequest {})
            .await
            .map_err(be)?
            .into_inner();
        Ok((
            resp.objects_deleted,
            resp.bytes_deleted,
            resp.candidates_skipped_grace,
        ))
    }

    /// List a table's live files (path + counts) plus its declared schema, for
    /// worker-side small-file selection and transform input registration.
    pub async fn list_files(&self, schema: String, name: String) -> Result<TableFiles> {
        let resp = self
            .inner
            .clone()
            .list_files(pb::ListFilesRequest { schema, name })
            .await
            .map_err(be)?
            .into_inner();
        let columns = resp
            .columns_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(be)?;
        Ok(TableFiles {
            files: resp
                .files
                .into_iter()
                .map(|f| control_plane_core::FileRef {
                    path: f.path,
                    record_count: f.record_count,
                    file_size_bytes: f.file_size_bytes,
                })
                .collect(),
            columns,
        })
    }

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
            .build_vector_index(pb::BuildVectorIndexRequest {
                schema,
                name,
                index_name,
            })
            .await
            .map_err(be)?
            .into_inner();
        Ok((resp.covered_snapshot, resp.puffin_path, resp.row_count))
    }

    /// Governed typed-insert over the wire. Returns the new snapshot id.
    pub async fn write_object(
        &self,
        schema: String,
        name: String,
        ipc: Vec<u8>,
        columns_json: String,
        lineage_json: String,
        jobs_json: &str,
    ) -> Result<i64> {
        let resp = self
            .inner
            .clone()
            .write_object(pb::WriteObjectRequest {
                schema,
                name,
                ipc,
                columns_json,
                lineage_json,
                jobs_json: jobs_json.to_string(),
            })
            .await
            .map_err(be)?
            .into_inner();
        Ok(resp.snapshot_id)
    }

    /// Stage N per-target writes + one lineage event in one transaction (one
    /// snapshot) over the wire. Returns the new snapshot id. Each `steps` entry is a
    /// `(schema, name, ipc, columns_json, overwrite)` tuple for one target; the single
    /// `lineage_json` carries every target in its outputs.
    pub async fn write_steps(
        &self,
        steps: Vec<pb::StepWrite>,
        lineage_json: String,
        jobs_json: &str,
    ) -> Result<i64> {
        let resp = self
            .inner
            .clone()
            .write_steps(pb::WriteStepsRequest {
                steps,
                lineage_json,
                jobs_json: jobs_json.to_string(),
            })
            .await
            .map_err(cp_status)?
            .into_inner();
        Ok(resp.snapshot_id)
    }

    /// Copy-on-write overwrite (UPDATE/DELETE) over the wire. Returns the new
    /// snapshot id. Empty `ipc` truncates the table.
    pub async fn overwrite_table(
        &self,
        schema: String,
        name: String,
        ipc: Vec<u8>,
        columns_json: String,
        lineage_json: String,
        jobs_json: &str,
    ) -> Result<i64> {
        let resp = self
            .inner
            .clone()
            .overwrite_table(pb::OverwriteTableRequest {
                schema,
                name,
                ipc,
                columns_json,
                lineage_json,
                jobs_json: jobs_json.to_string(),
            })
            .await
            .map_err(be)?
            .into_inner();
        Ok(resp.snapshot_id)
    }

    /// The current inline version of one identity (`0` if no live inline row
    /// exists). `id_ipc` is a one-row Arrow IPC stream holding just the id column;
    /// `columns_json` (a `Vec<ColumnSpec>`) describes it.
    pub async fn current_inline_version(
        &self,
        schema: String,
        name: String,
        id_column: String,
        id_ipc: Vec<u8>,
        columns_json: String,
    ) -> Result<i64> {
        let resp = self
            .inner
            .clone()
            .current_inline_version(pb::CurrentInlineVersionRequest {
                schema,
                name,
                id_column,
                id_ipc,
                columns_json,
            })
            .await
            .map_err(be)?
            .into_inner();
        Ok(resp.version)
    }

    /// Write one O(change) inline delta row (a row-version or a tombstone) over
    /// the wire, guarded engine-side by a per-identity CAS against
    /// `expected_version`. Returns the new snapshot id. A CAS loss surfaces as
    /// [`ControlPlaneError::Conflict`] (via [`write_status`]), not a generic
    /// backend error, so callers can retry.
    #[allow(
        clippy::too_many_arguments,
        reason = "mirrors the WriteDeltaRequest wire shape one-for-one; a params struct would only obscure the call site"
    )]
    pub async fn write_delta(
        &self,
        schema: String,
        name: String,
        id_column: String,
        tombstone: bool,
        ipc: Vec<u8>,
        columns_json: String,
        lineage_json: String,
        expected_version: i64,
        before_ipc: Vec<u8>,
        before_columns_json: String,
        jobs_json: &str,
    ) -> Result<i64> {
        let resp = self
            .inner
            .clone()
            .write_delta(pb::WriteDeltaRequest {
                schema,
                name,
                id_column,
                tombstone,
                ipc,
                columns_json,
                lineage_json,
                expected_version,
                before_ipc,
                before_columns_json,
                jobs_json: jobs_json.to_string(),
            })
            .await
            .map_err(write_status)?
            .into_inner();
        Ok(resp.snapshot_id)
    }

    /// Commit a compaction swap: expire `expire` (absolute paths) + register `write`
    /// (already written). Returns the new snapshot id (or `None` if the table was
    /// never written). Each `DataFile` is sent as a JSON string in `write_json`.
    pub async fn compact_table(
        &self,
        schema: String,
        name: String,
        expire: Vec<String>,
        write: &[control_plane_core::DataFile],
    ) -> Result<Option<i64>> {
        let write_json = write
            .iter()
            .map(|f| serde_json::to_string(f).map_err(be))
            .collect::<Result<Vec<_>>>()?;
        let resp = self
            .inner
            .clone()
            .compact_table(pb::CompactTableRequest {
                schema,
                name,
                expire,
                write_json,
            })
            .await
            .map_err(be)?
            .into_inner();
        Ok(resp.snapshot_id)
    }

    /// Fold a `kind='cdc'` table's base by LastRow-per-identity (the engine-side
    /// `consolidate_stream` op) and clear its inline-shadow flag. Returns the new
    /// base snapshot id, or `0` if the table is not a declared CDC table (a no-op).
    pub async fn consolidate_stream(&self, schema: String, name: String) -> Result<i64> {
        let resp = self
            .inner
            .clone()
            .consolidate_stream(pb::ConsolidateStreamRequest { schema, name })
            .await
            .map_err(be)?
            .into_inner();
        Ok(resp.snapshot_id)
    }

    /// Commit a transform's output: create the table (idempotent), register the
    /// already-written `write` files (append, or replace the live set when
    /// `replace`), and emit `lineage` — one atomic engine-side transaction.
    /// Returns the new snapshot id (`None` if the commit produced no snapshot).
    /// Each `DataFile` is sent as a JSON string in `write_json` (mirrors
    /// `compact_table`); the lineage event rides as a `LineageWire` JSON.
    pub async fn commit_transform(
        &self,
        schema: String,
        name: String,
        columns: &[control_plane_core::ColumnSpec],
        write: &[control_plane_core::DataFile],
        lineage: &control_plane_core::LineageEvent,
        replace: bool,
        run_id: Option<uuid::Uuid>,
    ) -> Result<Option<i64>> {
        let columns_json = serde_json::to_string(columns).map_err(be)?;
        let write_json = write
            .iter()
            .map(|f| serde_json::to_string(f).map_err(be))
            .collect::<Result<Vec<_>>>()?;
        let lineage_json =
            serde_json::to_string(&convert::LineageWire::from(lineage)).map_err(be)?;
        let resp = self
            .inner
            .clone()
            .commit_transform(pb::CommitTransformRequest {
                schema,
                name,
                columns_json,
                write_json,
                lineage_json,
                replace,
                run_id: run_id.map(|u| u.to_string()),
            })
            .await
            .map_err(cp_status)?
            .into_inner();
        Ok(resp.snapshot_id)
    }

    /// Commit one micro-batch of a standing query's output: inline-append `ipc`
    /// to `schema.name` (declared/confirmed a `buckets`-bucket log stream table),
    /// CAS-advance `mv`'s per-bucket watermarks against
    /// `source_schema.source_name`, and mark `run_id` succeeded — one atomic
    /// engine-side transaction. Returns the new snapshot id (`None` if the
    /// micro-batch was empty — no rows, no advances). A stale `advances` entry
    /// (or a declare conflict) surfaces as [`ControlPlaneError::Conflict`] (via
    /// [`cp_status`], preserving `Aborted` — mirrors `commit_transform`), rolling
    /// back the whole commit. `advances` are already-built [`pb::MvAdvance`]s
    /// (mirrors `write_steps`'s `Vec<pb::StepWrite>`).
    #[allow(
        clippy::too_many_arguments,
        reason = "mirrors the CommitMicroBatchRequest wire shape one-for-one; a params struct would only obscure the call site"
    )]
    pub async fn commit_micro_batch(
        &self,
        mv: String,
        source_schema: String,
        source_name: String,
        schema: String,
        name: String,
        buckets: i32,
        columns: &[control_plane_core::ColumnSpec],
        ipc: Vec<u8>,
        lineage: &control_plane_core::LineageEvent,
        advances: Vec<pb::MvAdvance>,
        run_id: Option<uuid::Uuid>,
    ) -> Result<Option<i64>> {
        let columns_json = serde_json::to_string(columns).map_err(be)?;
        let lineage_json =
            serde_json::to_string(&convert::LineageWire::from(lineage)).map_err(be)?;
        let resp = self
            .inner
            .clone()
            .commit_micro_batch(pb::CommitMicroBatchRequest {
                mv,
                source_schema,
                source_name,
                schema,
                name,
                buckets,
                ipc,
                columns_json,
                lineage_json,
                advances,
                run_id: run_id.map(|u| u.to_string()),
            })
            .await
            .map_err(cp_status)?
            .into_inner();
        Ok(resp.snapshot_id)
    }

    /// Mark a transform run Running (dequeued by a worker).
    pub async fn mark_run_running(&self, run_id: uuid::Uuid) -> Result<()> {
        self.inner
            .clone()
            .mark_run_running(pb::MarkRunRunningRequest {
                run_id: run_id.to_string(),
            })
            .await
            .map_err(cp_status)?;
        Ok(())
    }

    /// Report a run failure: `terminal` abandons (Failed); otherwise the run
    /// goes back to Queued with the error retained.
    pub async fn finish_run_failed(
        &self,
        run_id: uuid::Uuid,
        error: &str,
        terminal: bool,
    ) -> Result<()> {
        self.inner
            .clone()
            .finish_run_failed(pb::FinishRunFailedRequest {
                run_id: run_id.to_string(),
                error: error.to_string(),
                terminal,
            })
            .await
            .map_err(cp_status)?;
        Ok(())
    }

    gov_rpc! {
        /// Governance: check whether `subject` may perform `action` on `target`.
        fn gov_check(subject: &SubjectId, action: Action, target: &PolicyTarget) -> Decision;
        rpc check, out decision_json, req pb::CheckRequest {
            subject_json: se(subject)?,
            action_json: se(&action)?,
            target_json: se(target)?,
        }
    }

    gov_rpc! {
        /// Governance: list policies granting `subject` `action` on `target`.
        fn gov_policies_for(subject: &SubjectId, action: Action, target: &PolicyTarget, page: &PageReq) -> Page<Policy>;
        rpc policies_for, out page_json, req pb::PoliciesForRequest {
            subject_json: se(subject)?,
            action_json: se(&action)?,
            target_json: se(target)?,
            page_json: se(page)?,
        }
    }

    gov_rpc! {
        /// Governance: fetch the ontology definition of a named object type.
        fn gov_get_type(name: &TypeName) -> ObjectType;
        rpc get_type, out object_type_json, req pb::GetTypeRequest {
            type_name: name.0.clone(),
        }
    }

    gov_rpc! {
        /// Governance: resolve a type name to its underlying catalog `TableRef`.
        fn gov_resolve(name: &TypeName) -> TableRef;
        rpc resolve, out table_ref_json, req pb::ResolveRequest {
            type_name: name.0.clone(),
        }
    }

    gov_rpc! {
        /// Governance: list links declared on a named object type.
        fn gov_links(name: &TypeName, page: &PageReq) -> Page<LinkDef>;
        rpc links, out page_json, req pb::LinksRequest {
            type_name: name.0.clone(),
            page_json: se(page)?,
        }
    }

    gov_rpc! {
        /// Governance: list links that target a named object type.
        fn gov_links_to(name: &TypeName, page: &PageReq) -> Page<LinkDef>;
        rpc links_to, out page_json, req pb::LinksToRequest {
            type_name: name.0.clone(),
            page_json: se(page)?,
        }
    }

    gov_rpc! {
        /// Governance: list all defined object types.
        fn gov_list_types(page: &PageReq) -> Page<ObjectType>;
        rpc list_types, out page_json, req pb::ListTypesRequest {
            page_json: se(page)?,
        }
    }

    gov_rpc! {
        /// Governance: list all defined actions.
        fn gov_list_actions(page: &PageReq) -> Page<ActionDef>;
        rpc list_actions, out page_json, req pb::ListActionsRequest {
            page_json: se(page)?,
        }
    }

    gov_rpc! {
        /// Governance: fetch the definition of a named action.
        fn gov_get_action(name: &ActionName) -> ActionDef;
        rpc get_action, out action_def_json, req pb::GetActionRequest {
            action_name: name.0.clone(),
        }
    }

    gov_rpc! {
        /// Governance: list all vector index definitions for a named object type.
        fn gov_vector_indexes_for(type_name: &TypeName) -> Vec<VectorIndexDef>;
        rpc vector_indexes_for, out indexes_json, req pb::VectorIndexesForRequest {
            type_name: type_name.0.clone(),
        }
    }

    gov_rpc! {
        /// Governance: fetch a specific named vector index definition for a type.
        fn gov_get_vector_index(type_name: &TypeName, name: &str) -> Option<VectorIndexDef>;
        rpc get_vector_index, out index_json, req pb::GetVectorIndexRequest {
            type_name: type_name.0.clone(),
            name: name.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl Queue for GrpcQueueClient {
    async fn enqueue(&self, _job: NewJob) -> Result<JobId> {
        Err(ControlPlaneError::Backend(
            "enqueue is not available over the engine-wire".into(),
        ))
    }

    async fn dequeue(&self, kinds: &[String], worker: &str) -> Result<Option<Job>> {
        let resp = self
            .inner
            .clone()
            .dequeue(pb::DequeueRequest {
                kinds: kinds.to_vec(),
                worker: worker.into(),
            })
            .await
            .map_err(be)?
            .into_inner();
        resp.job
            .map(convert::job_from_pb)
            .transpose()
            .map_err(|e| be(e.0))
    }

    async fn complete(&self, id: JobId) -> Result<()> {
        self.inner
            .clone()
            .complete(pb::CompleteRequest {
                id: id.0.to_string(),
            })
            .await
            .map_err(be)?;
        Ok(())
    }

    async fn fail(&self, id: JobId, error: &str, policy: RetryPolicy) -> Result<()> {
        self.inner
            .clone()
            .fail(pb::FailRequest {
                id: id.0.to_string(),
                error: error.into(),
                policy: Some(convert::retry_policy_to_pb(&policy)),
            })
            .await
            .map_err(be)?;
        Ok(())
    }

    async fn heartbeat(&self, id: JobId) -> Result<()> {
        self.inner
            .clone()
            .heartbeat(pb::HeartbeatRequest {
                id: id.0.to_string(),
            })
            .await
            .map_err(be)?;
        Ok(())
    }

    async fn await_jobs(&self, kinds: &[String], timeout: std::time::Duration) -> Result<()> {
        let mut req = tonic::Request::new(pb::AwaitJobsRequest {
            kinds: kinds.to_vec(),
            timeout_ms: timeout.as_millis() as u64,
        });
        // Client deadline must exceed the server's long-poll timeout so we don't get
        // a spurious DeadlineExceeded before the server returns normally.
        req.set_timeout(timeout + std::time::Duration::from_secs(2));
        self.inner.clone().await_jobs(req).await.map_err(be)?;
        Ok(())
    }

    // Compiling stubs: job schedules are not (yet) exposed over the engine-wire
    // RPC surface. Mirrors the `enqueue` stub's error form above.
    async fn define_job_schedule(&self, _s: JobSchedule) -> Result<()> {
        Err(ControlPlaneError::Backend(
            "job schedules are not available over the engine-wire".into(),
        ))
    }

    async fn list_job_schedules(&self) -> Result<Vec<JobScheduleStatus>> {
        Err(ControlPlaneError::Backend(
            "job schedules are not available over the engine-wire".into(),
        ))
    }

    async fn delete_job_schedule(&self, _name: &str) -> Result<()> {
        Err(ControlPlaneError::Backend(
            "job schedules are not available over the engine-wire".into(),
        ))
    }

    async fn fire_due_job_schedules(
        &self,
        _now: time::OffsetDateTime,
        _limit: u32,
    ) -> Result<Vec<ScheduleFired>> {
        Err(ControlPlaneError::Backend(
            "job schedules are not available over the engine-wire".into(),
        ))
    }
}
