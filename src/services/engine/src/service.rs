//! `EngineControlService` — the tonic server that implements `EngineControl`.
//! Delegates queue operations to a `PgControlPlane` and flush_table to
//! `iceberg_flush::flush_table`.

use control_plane_core::{Queue, RetryPolicy, RunId, TableRef};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_gc::gc_table;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use engine_wire::convert;
use engine_wire::pb;
use sqlx::PgPool;
use tonic::{Request, Response, Status};

fn status(e: control_plane_core::ControlPlaneError) -> Status {
    match e {
        control_plane_core::ControlPlaneError::NotFound(m) => Status::not_found(m.to_string()),
        other => Status::internal(other.to_string()),
    }
}

fn parse_id(s: &str) -> std::result::Result<control_plane_core::JobId, Status> {
    Ok(control_plane_core::JobId(
        s.parse()
            .map_err(|_| Status::invalid_argument("bad job id"))?,
    ))
}

/// The engine's gRPC service implementation.
pub struct EngineControlService {
    pub cp: PgControlPlane,
    pub catalog: SqlCatalog,
    pub pool: PgPool,
    /// Retention window for `gc_table` (from `LOOM_GC_RETENTION_SECS`).
    pub retention: std::time::Duration,
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
        }))
    }
}
