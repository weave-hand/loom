//! Arrow Flight wire types shared between the engine and its clients.
//!
//! This module holds [`FlightTicket`] (the ticket payload naming the data files
//! a Flight `do_get` will stream) and [`FlightTableClient`] (the zero-pool
//! consumer that dials the engine's UDS and reconstructs `RecordBatch`es).

use arrow_array::RecordBatch;
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use control_plane_core::Result;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tonic::transport::Channel;

/// What a Flight `Ticket` names: an explicit set of a table's data files to
/// stream. `files` are the data-file path strings exactly as stored in the
/// iceberg mirror (passed verbatim to the engine's `FileIO::new_input`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}
