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
use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::flight::serving_status;

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

/// The engine's gRPC service implementation.
pub struct EngineControlService {
    pub cp: PgControlPlane,
    pub catalog: Arc<SqlCatalog>,
    pub pool: PgPool,
    /// Retention window for `gc_table` (from `LOOM_GC_RETENTION_SECS`).
    pub retention: std::time::Duration,
    /// Governed-write executor (relocated from query-api).
    pub writer: engine_serving::IcebergActionWriter,
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
            engine_serving::consolidate_stream(&self.cp, &self.catalog, &self.pool, &table)
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
