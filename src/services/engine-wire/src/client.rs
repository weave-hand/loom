//! `GrpcQueueClient` — a `Queue` impl that tunnels queue operations (and
//! `flush_table`) over the `EngineControl` tonic service via a unix-domain socket.

use control_plane_core::{
    Action, ActionDef, ActionName, ControlPlaneError, Decision, Job, JobId, LinkDef, NewJob,
    ObjectType, Page, PageReq, Policy, PolicyTarget, Queue, Result, RetryPolicy, SubjectId,
    TableRef, TypeName, VectorIndexDef,
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
            inner: EngineControlClient::new(channel),
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

    /// List a table's live files (path + counts) for worker-side small-file selection.
    pub async fn list_files(
        &self,
        schema: String,
        name: String,
    ) -> Result<Vec<control_plane_core::FileRef>> {
        let resp = self
            .inner
            .clone()
            .list_files(pb::ListFilesRequest { schema, name })
            .await
            .map_err(be)?
            .into_inner();
        Ok(resp
            .files
            .into_iter()
            .map(|f| control_plane_core::FileRef {
                path: f.path,
                record_count: f.record_count,
                file_size_bytes: f.file_size_bytes,
            })
            .collect())
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
            })
            .await
            .map_err(be)?
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
}
