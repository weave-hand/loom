//! The `EngineQuery` tonic service: runs compiled, param-inlined read SQL through
//! the engine-serving DataFusion tier and returns Arrow IPC bytes. The engine crate
//! itself never touches arrow/datafusion types — only the opaque IPC `Vec<u8>`.

use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use engine_wire::pb::engine_query_server::EngineQuery;
use engine_wire::pb::{ExecuteQueryRequest, ExecuteQueryResponse};
use tonic::{Request, Response, Status};

pub struct EngineQueryService {
    pub catalog: IcebergCatalog,
}

#[tonic::async_trait]
impl EngineQuery for EngineQueryService {
    async fn execute_query(
        &self,
        request: Request<ExecuteQueryRequest>,
    ) -> Result<Response<ExecuteQueryResponse>, Status> {
        let sql = request.into_inner().sql;
        let ipc = engine_serving::execute_query_to_ipc(&self.catalog, &sql)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ExecuteQueryResponse { ipc }))
    }
}
