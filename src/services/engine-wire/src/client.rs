//! `GrpcQueueClient` — a `Queue` impl that tunnels queue operations (and
//! `flush_table`) over the `EngineControl` tonic service via a unix-domain socket.

use control_plane_core::{ControlPlaneError, Job, JobId, NewJob, Queue, Result, RetryPolicy};
use tonic::transport::Channel;

use crate::convert;
use crate::pb;
use crate::pb::engine_control_client::EngineControlClient;

pub(crate) fn be<E: std::fmt::Display>(e: E) -> ControlPlaneError {
    ControlPlaneError::Backend(e.to_string().into())
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
