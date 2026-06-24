//! `EngineServingClient` — a `ServingEngine` that runs reads on the engine service
//! over the engine wire: inline params, send compiled SQL, decode the Arrow-58 IPC
//! result into `Rows`. Replaces the in-process DataFusion engine.
//!
//! Also exports `InProcessServingEngine` for tests that need a `dyn ServingEngine`
//! backed by `engine_serving::execute_query` without a gRPC hop.

use arrow::ipc::reader::StreamReader;
use async_trait::async_trait;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use engine_wire::client::EngineQueryClient;

use crate::serving::{Rows, ServingError, SqlValue, inline_params};
use crate::serving_datafusion::batches_to_rows;
use crate::sql::SqlDialect;

pub struct EngineServingClient {
    client: EngineQueryClient,
}

impl EngineServingClient {
    /// Connect to the engine's `EngineQuery` service at `socket` (a UDS path).
    pub async fn connect(socket: impl Into<String>) -> Result<Self, ServingError> {
        let client = EngineQueryClient::connect(socket)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl crate::serving::ServingEngine for EngineServingClient {
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError> {
        // Same param inlining the in-process DataFusion engine used; the engine has
        // no positional bind slot.
        let inlined = inline_params(sql, params);
        let ipc = self
            .client
            .execute_query(inlined)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        // Empty result → empty Rows (no column names). This matches the OLD
        // DataFusion path (which also returned empty Rows for a zero-row result);
        // it differs from EmbeddedDuckDb (which preserves columns for empty
        // results), but is behavior-preserving for the Iceberg backend — not a
        // regression introduced here.
        if ipc.is_empty() {
            return Ok(Rows::default());
        }
        let reader = StreamReader::try_new(std::io::Cursor::new(ipc), None)
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        Ok(batches_to_rows(batches))
    }
    fn dialect(&self) -> &'static dyn SqlDialect {
        &crate::sql::DataFusionDialect
    }
}

/// In-process `ServingEngine` backed by `engine_serving::execute_query`. Used by
/// tests that need a `dyn ServingEngine` over an `IcebergCatalog` without a gRPC hop.
pub struct InProcessServingEngine {
    catalog: IcebergCatalog,
}

impl InProcessServingEngine {
    pub fn new(catalog: IcebergCatalog) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl crate::serving::ServingEngine for InProcessServingEngine {
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError> {
        let inlined = inline_params(sql, params);
        let batches = engine_serving::execute_query(&self.catalog, &inlined)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        Ok(batches_to_rows(batches))
    }
    fn dialect(&self) -> &'static dyn SqlDialect {
        &crate::sql::DataFusionDialect
    }
}
