//! `EngineServingClient` — a `ServingEngine` that runs reads on the engine service
//! over **internal Flight SQL**: inline params, send the compiled SQL as a
//! `CommandStatementQuery`, consume the streamed Arrow-58 batches, flatten to `Rows`.
//! Also implements `vector_search` via a `FlightTableClient` kNN ticket.

use async_trait::async_trait;
use engine_wire::flight::{FlightSqlClient, FlightTableClient};

use crate::serving::{ChangeFeedPolicy, Rows, ServingError, SqlValue, inline_params};
use crate::serving_datafusion::batches_to_rows;
use crate::sql::SqlDialect;

pub struct EngineServingClient {
    sql: FlightSqlClient,
    table: FlightTableClient,
    /// The changelog feed rides the control plane (three unary RPCs), not Flight: a page
    /// is bounded (<= FEED_BATCH_LIMIT events) and `ndjson_feed_stream` re-serializes it
    /// to JSON anyway.
    control: engine_wire::client::GrpcQueueClient,
}

impl EngineServingClient {
    /// Connect to the engine's Flight + control services at `socket` (a UDS path).
    pub async fn connect(socket: impl Into<String>) -> Result<Self, ServingError> {
        let socket = socket.into();
        let sql = FlightSqlClient::connect(socket.clone())
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        let table = FlightTableClient::connect(socket.clone())
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        let control = engine_wire::client::GrpcQueueClient::connect(socket)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        Ok(Self {
            sql,
            table,
            control,
        })
    }
}

#[async_trait]
impl crate::serving::ServingEngine for EngineServingClient {
    async fn fetch_rows(
        &self,
        sql: &str,
        params: &[SqlValue],
        at: Option<control_plane_core::SnapshotId>,
    ) -> Result<Rows, ServingError> {
        // Same param inlining the unary path used; the engine has no positional bind slot.
        let inlined = inline_params(sql, params);
        let map_err = |e| match e {
            control_plane_core::ControlPlaneError::Validation(m) => ServingError::Plan(m),
            other => ServingError::Engine(other.to_string()),
        };
        // Empty result → empty Rows (no column names) — behaviour-preserving for the
        // Iceberg backend, matching the previous unary/DataFusion path. `batches_to_rows`
        // already returns `Rows::default()` for an empty batch list.
        let batches = match at {
            None => self.sql.execute(inlined).await.map_err(map_err)?,
            Some(id) => self
                .sql
                .execute_as_of(inlined, id.0)
                .await
                .map_err(map_err)?,
        };
        Ok(batches_to_rows(batches))
    }

    async fn vector_search(
        &self,
        table: &control_plane_core::TableRef,
        index_name: &str,
        query: &[f32],
        k: usize,
        nprobe: Option<u32>,
        ef_search: Option<u32>,
    ) -> Result<Rows, ServingError> {
        use engine_wire::flight::{VectorSearchError, VectorSearchTicket};
        let k_u32 = u32::try_from(k)
            .map_err(|_k_err| ServingError::Engine("k exceeds u32 range".to_string()))?;
        let ticket = VectorSearchTicket {
            schema: table.schema.clone(),
            name: table.name.clone(),
            index_name: index_name.to_string(),
            query: query.to_vec(),
            k: k_u32,
            nprobe,
            ef_search,
        };
        let batches = self
            .table
            .vector_search(ticket)
            .await
            .map_err(|e| match e {
                VectorSearchError::NoIndex(m) => ServingError::NoIndex(m),
                VectorSearchError::DimMismatch(m) => ServingError::DimMismatch(m),
                VectorSearchError::Engine(m) => ServingError::Engine(m),
            })?;
        Ok(batches_to_rows(batches))
    }

    fn dialect(&self) -> &'static dyn SqlDialect {
        &crate::sql::DataFusionDialect
    }

    async fn changelog_latest(
        &self,
        table: &control_plane_core::TableRef,
    ) -> Result<Option<std::collections::BTreeMap<i32, i64>>, ServingError> {
        self.control
            .changelog_latest(table.schema.clone(), table.name.clone())
            .await
            .map_err(|e| match e {
                // A rolling deploy where query-api is ahead of the engine: preserve it
                // as the semantically-correct 501 (`http.rs`'s `Unsupported` arm),
                // rather than letting it fall through to the 500 every other engine
                // fault gets.
                engine_wire::client::ChangelogLatestError::Unimplemented => {
                    ServingError::Unsupported("changelog feed".into())
                }
                engine_wire::client::ChangelogLatestError::Backend(e) => {
                    ServingError::Engine(e.to_string())
                }
            })
    }

    async fn changelog_feed(
        &self,
        table: &control_plane_core::TableRef,
        positions: &std::collections::BTreeMap<i32, i64>,
        limit: usize,
        policy: &ChangeFeedPolicy,
    ) -> Result<control_plane_core::ChangeFeedPage, ServingError> {
        let wire = engine_wire::client::WirePolicy {
            row_filters: policy.row_filters.clone(),
            denied: policy.denied.clone(),
            masked: policy.masked.clone(),
        };
        // No `as` cast: usize -> u64 must not silently truncate (clippy restriction).
        let limit = u64::try_from(limit).unwrap_or(u64::MAX);
        self.control
            .changelog_feed(
                table.schema.clone(),
                table.name.clone(),
                positions,
                limit,
                &wire,
            )
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))
    }

    async fn await_changelog(
        &self,
        table: &control_plane_core::TableRef,
        timeout: std::time::Duration,
    ) -> Result<(), ServingError> {
        self.control
            .await_changelog(table.schema.clone(), table.name.clone(), timeout)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))
    }
}
