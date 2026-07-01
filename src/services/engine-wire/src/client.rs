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

/// Decode a serde-JSON governance payload, mapping decode failure to `Serialization`.
fn de<T: serde::de::DeserializeOwned>(json: &str) -> Result<T> {
    serde_json::from_str(json).map_err(|e| ControlPlaneError::Serialization(e.to_string()))
}

/// Encode a governance argument to serde-JSON, mapping failure to `Serialization`.
fn se<T: serde::Serialize>(v: &T) -> Result<String> {
    serde_json::to_string(v).map_err(|e| ControlPlaneError::Serialization(e.to_string()))
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

    /// Governance: check whether `subject` may perform `action` on `target`.
    pub async fn gov_check(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
    ) -> Result<Decision> {
        let resp = self
            .inner
            .clone()
            .check(pb::CheckRequest {
                subject_json: se(subject)?,
                action_json: se(&action)?,
                target_json: se(target)?,
            })
            .await
            .map_err(cp_status)?
            .into_inner();
        de(&resp.decision_json)
    }

    /// Governance: list policies granting `subject` `action` on `target`.
    pub async fn gov_policies_for(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
        page: &PageReq,
    ) -> Result<Page<Policy>> {
        let resp = self
            .inner
            .clone()
            .policies_for(pb::PoliciesForRequest {
                subject_json: se(subject)?,
                action_json: se(&action)?,
                target_json: se(target)?,
                page_json: se(page)?,
            })
            .await
            .map_err(cp_status)?
            .into_inner();
        de(&resp.page_json)
    }

    /// Governance: fetch the ontology definition of a named object type.
    pub async fn gov_get_type(&self, name: &TypeName) -> Result<ObjectType> {
        let resp = self
            .inner
            .clone()
            .get_type(pb::GetTypeRequest {
                type_name: name.0.clone(),
            })
            .await
            .map_err(cp_status)?
            .into_inner();
        de(&resp.object_type_json)
    }

    /// Governance: resolve a type name to its underlying catalog `TableRef`.
    pub async fn gov_resolve(&self, name: &TypeName) -> Result<TableRef> {
        let resp = self
            .inner
            .clone()
            .resolve(pb::ResolveRequest {
                type_name: name.0.clone(),
            })
            .await
            .map_err(cp_status)?
            .into_inner();
        de(&resp.table_ref_json)
    }

    /// Governance: list links declared on a named object type.
    pub async fn gov_links(&self, name: &TypeName, page: &PageReq) -> Result<Page<LinkDef>> {
        let resp = self
            .inner
            .clone()
            .links(pb::LinksRequest {
                type_name: name.0.clone(),
                page_json: se(page)?,
            })
            .await
            .map_err(cp_status)?
            .into_inner();
        de(&resp.page_json)
    }

    /// Governance: list links that target a named object type.
    pub async fn gov_links_to(&self, name: &TypeName, page: &PageReq) -> Result<Page<LinkDef>> {
        let resp = self
            .inner
            .clone()
            .links_to(pb::LinksToRequest {
                type_name: name.0.clone(),
                page_json: se(page)?,
            })
            .await
            .map_err(cp_status)?
            .into_inner();
        de(&resp.page_json)
    }

    /// Governance: list all defined object types.
    pub async fn gov_list_types(&self, page: &PageReq) -> Result<Page<ObjectType>> {
        let resp = self
            .inner
            .clone()
            .list_types(pb::ListTypesRequest {
                page_json: se(page)?,
            })
            .await
            .map_err(cp_status)?
            .into_inner();
        de(&resp.page_json)
    }

    /// Governance: fetch the definition of a named action.
    pub async fn gov_get_action(&self, name: &ActionName) -> Result<ActionDef> {
        let resp = self
            .inner
            .clone()
            .get_action(pb::GetActionRequest {
                action_name: name.0.clone(),
            })
            .await
            .map_err(cp_status)?
            .into_inner();
        de(&resp.action_def_json)
    }

    /// Governance: list all vector index definitions for a named object type.
    pub async fn gov_vector_indexes_for(
        &self,
        type_name: &TypeName,
    ) -> Result<Vec<VectorIndexDef>> {
        let resp = self
            .inner
            .clone()
            .vector_indexes_for(pb::VectorIndexesForRequest {
                type_name: type_name.0.clone(),
            })
            .await
            .map_err(cp_status)?
            .into_inner();
        de(&resp.indexes_json)
    }

    /// Governance: fetch a specific named vector index definition for a type.
    pub async fn gov_get_vector_index(
        &self,
        type_name: &TypeName,
        name: &str,
    ) -> Result<Option<VectorIndexDef>> {
        let resp = self
            .inner
            .clone()
            .get_vector_index(pb::GetVectorIndexRequest {
                type_name: type_name.0.clone(),
                name: name.to_string(),
            })
            .await
            .map_err(cp_status)?
            .into_inner();
        de(&resp.index_json)
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
