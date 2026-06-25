//! `EngineServingClient` — a `ServingEngine` that runs reads on the engine service
//! over **internal Flight SQL**: inline params, send the compiled SQL as a
//! `CommandStatementQuery`, consume the streamed Arrow-58 batches, flatten to `Rows`.

use async_trait::async_trait;
use engine_wire::flight::FlightSqlClient;

use crate::serving::{Rows, ServingError, SqlValue, inline_params};
use crate::serving_datafusion::batches_to_rows;
use crate::sql::SqlDialect;

pub struct EngineServingClient {
    client: FlightSqlClient,
}

impl EngineServingClient {
    /// Connect to the engine's Arrow Flight service at `socket` (a UDS path).
    pub async fn connect(socket: impl Into<String>) -> Result<Self, ServingError> {
        let client = FlightSqlClient::connect(socket)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl crate::serving::ServingEngine for EngineServingClient {
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError> {
        // Same param inlining the unary path used; the engine has no positional bind slot.
        let inlined = inline_params(sql, params);
        let batches = self
            .client
            .execute(inlined)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        // Empty result → empty Rows (no column names) — behaviour-preserving for the
        // Iceberg backend, matching the previous unary/DataFusion path. `batches_to_rows`
        // already returns `Rows::default()` for an empty batch list.
        Ok(batches_to_rows(batches))
    }
    fn dialect(&self) -> &'static dyn SqlDialect {
        &crate::sql::DataFusionDialect
    }
}
