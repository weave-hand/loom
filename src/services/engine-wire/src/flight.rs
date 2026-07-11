//! Arrow Flight wire types shared between the engine and its clients.
//!
//! This module holds [`FlightTicket`] (the ticket payload naming the data files
//! a Flight `do_get` will stream) and [`FlightTableClient`] (the zero-pool
//! consumer that dials the engine's UDS and reconstructs `RecordBatch`es).

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{Any, CommandStatementQuery, ProstMessageExt, TicketStatementQuery};
use arrow_flight::{FlightData, FlightDescriptor, Ticket};
use arrow_schema::{ArrowError, SchemaRef};
use control_plane_core::{GovernedCatalog, Result};
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
    pub index_name: String,
    pub query: Vec<f32>,
    pub k: u32,
    /// IVF-Flat probe count for this query (ignored by other kinds). Defaults to `None`.
    #[serde(default)]
    pub nprobe: Option<u32>,
    /// HNSW candidate width for this query (ignored by other kinds). Defaults to `None`.
    #[serde(default)]
    pub ef_search: Option<u32>,
}

/// Outcome of a kNN `do_get` that preserves the engine's gRPC status *code*,
/// which `client::be` would otherwise flatten to a string. Lets query-api map a
/// missing index to 404 and a query-dim mismatch to 400.
#[derive(Debug, thiserror::Error)]
pub enum VectorSearchError {
    #[error("no vector index: {0}")]
    NoIndex(String),
    #[error("dimension mismatch: {0}")]
    DimMismatch(String),
    #[error("engine: {0}")]
    Engine(String),
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

/// A loom-native Flight `do_get` ticket carrying arbitrary client SQL plus the caller's
/// fully-resolved governed catalog. JSON-encoded; `deny_unknown_fields` keeps it disjoint
/// from `FlightTicket`/`VectorSearchTicket`. Dispatched by the engine to
/// `engine_serving::execute_governed_sql_stream`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernedStatementQuery {
    pub sql: String,
    pub catalog: GovernedCatalog,
}

impl GovernedStatementQuery {
    /// JSON-encode for the `Ticket.ticket` bytes.
    #[must_use]
    #[expect(
        clippy::expect_used,
        reason = "serde_json of an owned serializable type is infallible; matches VectorSearchTicket::encode"
    )]
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("GovernedStatementQuery is always serializable")
    }

    /// Decode from `Ticket.ticket` bytes.
    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// A loom-native `do_get` ticket requesting a standing query's framed source
/// delta: every `(loom_bucket, loom_offset)`-ordered event of `schema.name` at
/// or beyond `mv`'s committed watermark. INTERNAL data plane: the response
/// carries the `loom_*` framing columns (the worker derives its watermark CAS
/// bounds from them, then strips them before running user SQL). The required
/// `mv` field keeps it disjoint (`deny_unknown_fields`) from every other shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MvDeltaTicket {
    pub mv: String,
    pub schema: String,
    pub name: String,
}

impl MvDeltaTicket {
    /// JSON-encode for the `Ticket.ticket` bytes.
    #[must_use]
    #[expect(
        clippy::expect_used,
        reason = "serde_json of an owned serializable type is infallible; matches VectorSearchTicket::encode"
    )]
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("MvDeltaTicket is always serializable")
    }

    /// Decode from `Ticket.ticket` bytes.
    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// A loom-native `do_get` ticket requesting an enrich table's current state,
/// optionally filtered to a JSON key set: `(schema, name)` names the enrich
/// side of a stream-join, `key` names the equality column (when the join is
/// keyed), and `keys` — when non-empty — restricts the fetch to rows whose
/// `key` column value is one of the given JSON values. The required
/// `enrich_schema`/`enrich_name` fields keep it disjoint (`deny_unknown_fields`)
/// from every other JSON ticket shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MvEnrichTicket {
    pub enrich_schema: String,
    pub enrich_name: String,
    pub key: Option<String>,
    #[serde(default)]
    pub keys: Vec<serde_json::Value>,
}

impl MvEnrichTicket {
    /// JSON-encode for the `Ticket.ticket` bytes.
    #[must_use]
    #[expect(
        clippy::expect_used,
        reason = "serde_json of an owned serializable type is infallible; matches MvDeltaTicket::encode"
    )]
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("MvEnrichTicket is always serializable")
    }

    /// Decode from `Ticket.ticket` bytes.
    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// A loom-native as-of read ticket: run `sql` with every referenced table read at
/// snapshot `as_of_snapshot` instead of its current snapshot. Bypasses the standard
/// `CommandStatementQuery` (loom's external Flight SQL interop surface) so that
/// surface stays a bare query string. `deny_unknown_fields` keeps it disjoint from
/// the other JSON ticket shapes for the `EngineTicket` decode.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AsOfStatementQuery {
    pub sql: String,
    pub as_of_snapshot: i64,
}

impl AsOfStatementQuery {
    #[must_use]
    #[expect(
        clippy::expect_used,
        reason = "serde_json of an owned serializable type is infallible; matches GovernedStatementQuery::encode"
    )]
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("AsOfStatementQuery is always serializable")
    }

    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// A decoded engine `do_get` ticket — one variant per serving plane. The decode
/// ORDER is load-bearing and lives here, next to the ticket types whose
/// `deny_unknown_fields` disjointness it depends on: the protobuf Flight SQL
/// ticket is tried first (a legacy JSON ticket always starts with `{`, an invalid
/// protobuf `Any`, so the file path is never misrouted), then the six JSON
/// shapes fall through in order, the file ticket terminal.
#[derive(Debug, Clone, PartialEq)]
pub enum EngineTicket {
    /// Flight SQL read plane: the SQL carried in a `TicketStatementQuery` handle.
    Sql(String),
    /// Governed-SQL plane: arbitrary client SQL + a caller-resolved governed catalog.
    GovernedSql(GovernedStatementQuery),
    /// Loom-native as-of SQL plane: client SQL + a resolved snapshot id.
    AsOfSql(AsOfStatementQuery),
    /// Standing-query framed source delta plane (internal data plane).
    MvDelta(MvDeltaTicket),
    /// Enrich-table current-state read plane (internal data plane): a stream
    /// join's enrich side, optionally filtered to a JSON key set.
    MvEnrich(MvEnrichTicket),
    /// k-NN vector-search plane.
    VectorSearch(VectorSearchTicket),
    /// File-ticket data plane: an explicit live-file set to stream.
    Files(FlightTicket),
}

/// Why a `do_get` ticket failed to decode. Each variant's `Display` is the exact
/// wire message the engine emitted before this enum existed (pinned by
/// `engine/tests/ticket_errors.rs`); the `From<TicketError> for tonic::Status`
/// below fixes the code split.
#[derive(Debug, thiserror::Error)]
pub enum TicketError {
    /// A protobuf `Any` matched `TicketStatementQuery` but failed to unpack.
    /// No fall-through: a matched Flight SQL ticket fails in place.
    #[error("bad flight-sql ticket: {0}")]
    FlightSqlUnpack(#[source] ArrowError),
    /// `Any::unpack` returned `None` after `is::<TicketStatementQuery>()` matched —
    /// an arrow-flight invariant violation, the server's fault, never the client's.
    #[error("flight-sql ticket unpack returned None")]
    FlightSqlEmpty,
    /// The statement handle of a matched Flight SQL ticket is not UTF-8.
    #[error("non-utf8 sql: {0}")]
    NonUtf8Sql(#[source] std::string::FromUtf8Error),
    /// The terminal failure of the fall-through chain: a ticket that is neither
    /// Flight SQL, governed, nor kNN must be a file ticket.
    #[error("bad flight ticket: {0}")]
    BadFileTicket(#[source] serde_json::Error),
}

impl From<TicketError> for tonic::Status {
    fn from(e: TicketError) -> Self {
        match &e {
            // Server-side invariant violation, not a client fault.
            TicketError::FlightSqlEmpty => tonic::Status::internal(e.to_string()),
            TicketError::FlightSqlUnpack(_)
            | TicketError::NonUtf8Sql(_)
            | TicketError::BadFileTicket(_) => tonic::Status::invalid_argument(e.to_string()),
        }
    }
}

impl EngineTicket {
    /// Decode a `Ticket.ticket` payload into its serving plane.
    ///
    /// Flight SQL read path first: a `TicketStatementQuery` (Any-wrapped) carrying
    /// the SQL. Try the protobuf decode first; a legacy JSON ticket always starts
    /// with `{` (an invalid protobuf `Any`), so this never misroutes the file path.
    /// (The decode-then-`is::<>()` ordering is load-bearing.) The JSON planes then
    /// fall through in order — `deny_unknown_fields` on all six JSON shapes makes
    /// each stage unambiguous — with the file ticket terminal.
    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, TicketError> {
        if let Ok(any) = Any::decode(bytes)
            && any.is::<TicketStatementQuery>()
        {
            let tsq = any
                .unpack::<TicketStatementQuery>()
                .map_err(TicketError::FlightSqlUnpack)?
                .ok_or(TicketError::FlightSqlEmpty)?;
            let sql = String::from_utf8(tsq.statement_handle.to_vec())
                .map_err(TicketError::NonUtf8Sql)?;
            return Ok(Self::Sql(sql));
        }
        // loom-native governed SQL ticket (JSON): disjoint fields
        // (deny_unknown_fields) from the other JSON tickets.
        if let Ok(gq) = GovernedStatementQuery::decode(bytes) {
            return Ok(Self::GovernedSql(gq));
        }
        // loom-native as-of SQL ticket (JSON): disjoint fields
        // (deny_unknown_fields) from the other JSON tickets — requires
        // `as_of_snapshot`, which `GovernedStatementQuery` (requires `catalog`)
        // and the others do not carry.
        if let Ok(q) = AsOfStatementQuery::decode(bytes) {
            return Ok(EngineTicket::AsOfSql(q));
        }
        // loom-native mv-delta ticket (JSON): disjoint fields (deny_unknown_fields)
        // from the other JSON tickets — the required `mv` field is unique to this
        // shape, so it can never alias `GovernedStatementQuery`/`AsOfStatementQuery`/
        // `VectorSearchTicket`/`FlightTicket`. Tried after `AsOfSql` (both are
        // two/three-field shapes so order between them is arbitrary) and before
        // `VectorSearch` (also arbitrary — kept ticket-type declaration order).
        if let Ok(mv) = MvDeltaTicket::decode(bytes) {
            return Ok(EngineTicket::MvDelta(mv));
        }
        // loom-native mv-enrich ticket (JSON): disjoint fields (deny_unknown_fields)
        // from the other JSON tickets — the required `enrich_schema`/`enrich_name`
        // fields are unique to this shape, so it can never alias
        // `GovernedStatementQuery`/`AsOfStatementQuery`/`MvDeltaTicket`/
        // `VectorSearchTicket`/`FlightTicket`. Tried after `MvDelta` (both are
        // internal data-plane shapes) and before `VectorSearch`/`Files`.
        if let Ok(me) = MvEnrichTicket::decode(bytes) {
            return Ok(EngineTicket::MvEnrich(me));
        }
        // loom-native k-NN ticket (JSON). Disjoint fields from FlightTicket
        // (deny_unknown_fields on both) make this unambiguous.
        if let Ok(vs) = VectorSearchTicket::decode(bytes) {
            return Ok(Self::VectorSearch(vs));
        }
        // File-ticket data plane: a JSON `FlightTicket` naming data files.
        FlightTicket::decode(bytes)
            .map(Self::Files)
            .map_err(TicketError::BadFileTicket)
    }
}

/// Decode a `do_get` response's schema-first `FlightData` stream into
/// `RecordBatch`es, mapping inbound `tonic::Status` items to
/// `FlightError::Tonic` (via `From`). The shared decode step of every
/// `do_get` consumer in this module; error mapping onto the caller's domain
/// stays at each call site.
fn decode_batches(resp: tonic::Response<tonic::Streaming<FlightData>>) -> FlightRecordBatchStream {
    FlightRecordBatchStream::new_from_flight_data(
        resp.into_inner()
            .map_err(arrow_flight::error::FlightError::from),
    )
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

    /// Send a raw `Ticket.ticket` payload via `do_get` and collect all returned
    /// [`RecordBatch`]es. The engine streams schema-first Arrow IPC; this method
    /// reconstructs the batches and returns them as a `Vec`. Shared ticket→batches
    /// plumbing for every JSON-ticket `do_get` consumer on this client (`fetch`,
    /// `fetch_mv_delta`); `vector_search` stays separate — it needs the gRPC status
    /// code preserved, not flattened by [`crate::client::be`].
    async fn do_get_batches(&self, ticket: Vec<u8>) -> Result<Vec<RecordBatch>> {
        let resp = self
            .inner
            .clone()
            .do_get(Ticket {
                ticket: ticket.into(),
            })
            .await
            .map_err(crate::client::be)?;
        decode_batches(resp)
            .try_collect()
            .await
            .map_err(crate::client::be)
    }

    /// Like [`do_get_batches`](Self::do_get_batches) but ALSO returns the wire
    /// schema. Collects the `FlightRecordBatchStream` while keeping it alive
    /// (`&mut stream` borrow) so its schema — decoded from the leading Schema
    /// message the engine always sends — can be read afterwards. Used by
    /// `fetch_mv_enrich`, whose response can legitimately be zero batches (a
    /// live-but-empty enrich table): the schema is what lets the worker register
    /// an empty enrich table instead of abandoning the run.
    async fn do_get_batches_with_schema(
        &self,
        ticket: Vec<u8>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let resp = self
            .inner
            .clone()
            .do_get(Ticket {
                ticket: ticket.into(),
            })
            .await
            .map_err(crate::client::be)?;
        let mut stream = decode_batches(resp);
        let mut batches = Vec::new();
        while let Some(b) = stream.try_next().await.map_err(crate::client::be)? {
            batches.push(b);
        }
        let schema = stream
            .schema()
            .cloned()
            .ok_or_else(|| crate::client::be("mv enrich: no schema on wire"))?;
        Ok((schema, batches))
    }

    /// Send a [`FlightTicket`] via `do_get` and collect all returned
    /// [`RecordBatch`]es. The engine streams schema-first Arrow IPC; this
    /// method reconstructs the batches and returns them as a `Vec`.
    pub async fn fetch(&self, ticket: FlightTicket) -> Result<Vec<RecordBatch>> {
        self.do_get_batches(ticket.encode()).await
    }

    /// Fetch a standing query's framed source delta (see [`MvDeltaTicket`]).
    pub async fn fetch_mv_delta(
        &self,
        mv: String,
        schema: String,
        name: String,
    ) -> Result<Vec<RecordBatch>> {
        let ticket = MvDeltaTicket { mv, schema, name };
        self.do_get_batches(ticket.encode()).await
    }

    /// Fetch an enrich table's current state, optionally restricted to a key
    /// set (see [`MvEnrichTicket`]), returning the wire schema alongside the
    /// batches. The schema is surfaced (not derived from the first batch)
    /// because a live-but-empty enrich table yields zero batches yet a schema
    /// the engine always sends — the worker registers an empty enrich table
    /// from it rather than abandoning the join run.
    pub async fn fetch_mv_enrich(
        &self,
        ticket: MvEnrichTicket,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        self.do_get_batches_with_schema(ticket.encode()).await
    }

    /// Send a [`VectorSearchTicket`] via `do_get` and collect the kNN result rows.
    /// The engine's gRPC status code is preserved: `NotFound` → [`VectorSearchError::NoIndex`],
    /// `InvalidArgument` → [`VectorSearchError::DimMismatch`], anything else → `Engine`.
    pub async fn vector_search(
        &self,
        ticket: VectorSearchTicket,
    ) -> std::result::Result<Vec<RecordBatch>, VectorSearchError> {
        let resp = self
            .inner
            .clone()
            .do_get(Ticket {
                ticket: ticket.encode().into(),
            })
            .await
            .map_err(|s: tonic::Status| match s.code() {
                tonic::Code::NotFound => VectorSearchError::NoIndex(s.message().to_string()),
                tonic::Code::InvalidArgument => {
                    VectorSearchError::DimMismatch(s.message().to_string())
                }
                _ => VectorSearchError::Engine(s.message().to_string()),
            })?;
        decode_batches(resp)
            .try_collect()
            .await
            .map_err(|e| VectorSearchError::Engine(e.to_string()))
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
    /// Buffering wrapper over [`execute_stream`](Self::execute_stream) — the stream's
    /// items are already mapped to control-plane errors, so collecting preserves the
    /// error class/messages of the old hand-rolled body.
    pub async fn execute(&self, sql: String) -> Result<Vec<RecordBatch>> {
        self.execute_stream(sql).await?.try_collect().await
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
            .map_err(crate::client::sql_status)?
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
            .map_err(crate::client::sql_status)?;
        // Decode the schema-first FlightData stream into RecordBatches, mapping the
        // stream's FlightError items to control-plane errors (same `be` mapping the
        // buffered path uses). The stream owns the (cloned) response, so it is 'static.
        Ok(Box::pin(decode_batches(resp).map_err(crate::client::be)))
    }

    /// Execute arbitrary client `sql` under a caller-resolved governed catalog,
    /// returning the decoded result stream. Single-hop `do_get` of a
    /// `GovernedStatementQuery` ticket (the standard `CommandStatementQuery`
    /// cannot carry the catalog). The stream never materialises in the caller —
    /// query-api's external SQL wire relays it straight out.
    pub async fn execute_governed_stream(
        &self,
        sql: String,
        catalog: GovernedCatalog,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> {
        let ticket = GovernedStatementQuery { sql, catalog };
        let resp = self
            .inner
            .clone()
            .do_get(Ticket {
                ticket: ticket.encode().into(),
            })
            .await
            .map_err(crate::client::sql_status)?;
        Ok(Box::pin(decode_batches(resp).map_err(crate::client::be)))
    }

    /// Execute `sql` with every referenced table read at `as_of_snapshot`, buffering
    /// the streamed result. Single-hop `do_get` of an `AsOfStatementQuery` ticket
    /// (the standard `CommandStatementQuery` cannot carry the snapshot id).
    pub async fn execute_as_of(
        &self,
        sql: String,
        as_of_snapshot: i64,
    ) -> Result<Vec<RecordBatch>> {
        let ticket = AsOfStatementQuery {
            sql,
            as_of_snapshot,
        };
        let resp = self
            .inner
            .clone()
            .do_get(Ticket {
                ticket: ticket.encode().into(),
            })
            .await
            .map_err(crate::client::sql_status)?;
        decode_batches(resp)
            .map_err(crate::client::be)
            .try_collect()
            .await
    }
}
