//! Arrow Flight wire types shared between the engine and its clients.
//!
//! This module holds [`FlightTicket`] (the ticket payload naming the data files
//! a Flight `do_get` will stream) and [`FlightTableClient`] (the zero-pool
//! consumer that dials the engine's UDS and reconstructs `RecordBatch`es).

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use arrow_flight::{FlightDescriptor, Ticket};
use control_plane_core::Result;
use futures::{Stream, TryStreamExt};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use tonic::transport::Channel;

/// What a Flight `Ticket` names: an explicit set of a table's data files to
/// stream. `files` are the data-file path strings exactly as stored in the
/// iceberg mirror (passed verbatim to the engine's `FileIO::new_input`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlightTicket {
    pub schema: String,
    pub name: String,
    pub files: Vec<String>,
}

impl FlightTicket {
    /// JSON-encode for the `Ticket.ticket` bytes.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("FlightTicket is always serializable")
    }

    /// Decode from `Ticket.ticket` bytes.
    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// A loom-native Flight `do_get` ticket requesting an engine-side k-NN search.
/// JSON-encoded; `deny_unknown_fields` guarantees it never aliases a `FlightTicket`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorSearchTicket {
    pub schema: String,
    pub name: String,
    pub column: String,
    pub query: Vec<f32>,
    pub k: u32,
}

impl VectorSearchTicket {
    /// JSON-encode for the `Ticket.ticket` bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("VectorSearchTicket is always serializable")
    }

    /// Decode from `Ticket.ticket` bytes.
    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// Zero-pool client for the engine's Arrow Flight data plane. Holds no
/// Postgres connection: it streams a file set's rows from the engine over
/// the engine's Unix-domain socket.
#[derive(Clone)]
pub struct FlightTableClient {
    inner: FlightServiceClient<Channel>,
}

impl FlightTableClient {
    /// Connect to the engine's Arrow Flight service over the given UDS path.
    pub async fn connect(socket: impl Into<String>) -> Result<Self> {
        let channel = crate::uds_channel(socket.into()).await?;
        Ok(Self {
            inner: FlightServiceClient::new(channel),
        })
    }

    /// Send a [`FlightTicket`] via `do_get` and collect all returned
    /// [`RecordBatch`]es. The engine streams schema-first Arrow IPC; this
    /// method reconstructs the batches and returns them as a `Vec`.
    pub async fn fetch(&self, ticket: FlightTicket) -> Result<Vec<RecordBatch>> {
        let resp = self
            .inner
            .clone()
            .do_get(Ticket {
                ticket: ticket.encode().into(),
            })
            .await
            .map_err(crate::client::be)?;
        // Map inbound tonic::Status errors to FlightError::Tonic via From impl,
        // then decode the schema-first FlightData stream into RecordBatches.
        let stream = FlightRecordBatchStream::new_from_flight_data(
            resp.into_inner()
                .map_err(arrow_flight::error::FlightError::from),
        );
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(crate::client::be)?;
        Ok(batches)
    }

    /// Send a [`VectorSearchTicket`] via `do_get` and collect all returned
    /// [`RecordBatch`]es (k-NN result rows streamed from the engine).
    pub async fn vector_search(&self, ticket: VectorSearchTicket) -> Result<Vec<RecordBatch>> {
        let resp = self
            .inner
            .clone()
            .do_get(Ticket {
                ticket: ticket.encode().into(),
            })
            .await
            .map_err(crate::client::be)?;
        let stream = FlightRecordBatchStream::new_from_flight_data(
            resp.into_inner()
                .map_err(arrow_flight::error::FlightError::from),
        );
        stream.try_collect().await.map_err(crate::client::be)
    }
}

/// Zero-pool client for the engine's internal **Flight SQL** read plane. Performs
/// the `CommandStatementQuery` dance — `get_flight_info` returns a ticket carrying
/// the SQL, `do_get` streams the result `RecordBatch`es — over the engine's UDS.
/// Holds no Postgres connection. Replaces the unary `EngineQueryClient`.
#[derive(Clone)]
pub struct FlightSqlClient {
    inner: FlightServiceClient<Channel>,
}

impl FlightSqlClient {
    /// Connect to the engine's Arrow Flight service over the given UDS path.
    pub async fn connect(socket: impl Into<String>) -> Result<Self> {
        let channel = crate::uds_channel(socket.into()).await?;
        Ok(Self {
            inner: FlightServiceClient::new(channel),
        })
    }

    /// Execute already-compiled, param-inlined `sql` and collect the streamed result.
    /// Each `RecordBatch` arrives as its own Flight message, so a wide/large result
    /// never serialises into a single oversized gRPC message (the unary path's cap).
    pub async fn execute(&self, sql: String) -> Result<Vec<RecordBatch>> {
        let cmd = CommandStatementQuery {
            query: sql,
            transaction_id: None,
        };
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());

        let info = self
            .inner
            .clone()
            .get_flight_info(descriptor)
            .await
            .map_err(crate::client::be)?
            .into_inner();
        let ticket = info
            .endpoint
            .into_iter()
            .next()
            .and_then(|e| e.ticket)
            .ok_or_else(|| crate::client::be("flight info carried no ticket"))?;

        let resp = self
            .inner
            .clone()
            .do_get(ticket)
            .await
            .map_err(crate::client::be)?;
        let stream = FlightRecordBatchStream::new_from_flight_data(
            resp.into_inner()
                .map_err(arrow_flight::error::FlightError::from),
        );
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(crate::client::be)?;
        Ok(batches)
    }

    /// Like [`execute`](Self::execute) but returns the decoded `do_get` result as a
    /// **stream** of `RecordBatch`es instead of buffering them into a `Vec`. The caller
    /// (query-api's governed Flight export) re-encodes this stream straight out, so the
    /// engine→consumer path stays back-pressured and a large export never materialises
    /// in query-api's memory. Performs the same `CommandStatementQuery` get_flight_info →
    /// do_get dance as `execute`.
    pub async fn execute_stream(
        &self,
        sql: String,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> {
        let cmd = CommandStatementQuery {
            query: sql,
            transaction_id: None,
        };
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());

        let info = self
            .inner
            .clone()
            .get_flight_info(descriptor)
            .await
            .map_err(crate::client::be)?
            .into_inner();
        let ticket = info
            .endpoint
            .into_iter()
            .next()
            .and_then(|e| e.ticket)
            .ok_or_else(|| crate::client::be("flight info carried no ticket"))?;

        let resp = self
            .inner
            .clone()
            .do_get(ticket)
            .await
            .map_err(crate::client::be)?;
        // Decode the schema-first FlightData stream into RecordBatches, mapping the
        // stream's FlightError items to control-plane errors (same `be` mapping the
        // buffered path uses). The stream owns the (cloned) response, so it is 'static.
        let stream = FlightRecordBatchStream::new_from_flight_data(
            resp.into_inner()
                .map_err(arrow_flight::error::FlightError::from),
        )
        .map_err(crate::client::be);
        Ok(Box::pin(stream))
    }
}
