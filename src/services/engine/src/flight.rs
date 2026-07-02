//! Arrow Flight data plane: stream a table's data-file rows out as Arrow
//! batches. The engine owns Postgres + object store, so it is the data source;
//! the worker is a pure compute client over the wire.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::{Any, CommandStatementQuery, ProstMessageExt, TicketStatementQuery};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use control_plane_core::{Catalog, TableRef};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use control_plane_postgres::read_files_as_batches;
use engine_wire::flight::EngineTicket;
use futures::TryStreamExt; // for `.map_err` on the FlightDataEncoder stream
use prost::Message;
use sqlx::PgPool;
use tonic::{Request, Response, Status, Streaming};

/// The one total `EngineServingError` -> gRPC `Status` mapping for the engine's
/// Flight data plane. Class-preserving: `Plan` (bad SQL — the client's fault) ->
/// `invalid_argument`; `NoIndex` -> `not_found`; `DimMismatch` ->
/// `invalid_argument`; `Engine` (execution/backend) -> `internal`. The
/// NoIndex/DimMismatch arms carry the INNER message only (no enum prefix),
/// preserving the wire messages the clients' inverse mappings decode.
pub fn serving_status(e: engine_serving::EngineServingError) -> Status {
    use engine_serving::EngineServingError as E;
    match e {
        E::NoIndex(m) => Status::not_found(m),
        E::DimMismatch(m) => Status::invalid_argument(m),
        e @ E::Plan(_) => Status::invalid_argument(e.to_string()),
        e @ E::Engine(_) => Status::internal(e.to_string()),
    }
}

pub struct FlightDataService {
    /// File-ticket data plane (worker/compaction): a real Iceberg `SqlCatalog`.
    pub catalog: SqlCatalog,
    pub pool: PgPool,
    /// Flight SQL read plane: the live-table catalog the governed reads run against.
    pub serving_catalog: IcebergCatalog,
    /// `Some((bucket, store))` for an S3 warehouse; `None` => local filesystem.
    pub serving_store: Option<(String, Arc<dyn object_store::ObjectStore>)>,
}

impl FlightDataService {
    /// Flight-encode a `RecordBatch` stream (schema message first, then batches)
    /// and box it as the `do_get` response. Encoder/stream errors map to
    /// `Status::internal`, matching the old unary handler's mapping so query-api's
    /// HTTP error codes are unchanged. The shared tail of all four serving planes.
    fn encode_response(
        batches: impl futures::Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static,
    ) -> Response<<Self as FlightService>::DoGetStream> {
        let out = FlightDataEncoderBuilder::new()
            .build(batches)
            .map_err(|e| Status::internal(e.to_string()));
        Response::new(Box::pin(out))
    }

    /// Run `sql` through the streaming serving tier and Flight-encode the result
    /// `RecordBatch` stream (schema message first, then batches). Serving errors map
    /// via [`serving_status`]: planning faults to `invalid_argument`, execution
    /// faults to `internal` — so query-api's HTTP error codes track the class.
    async fn do_get_sql(
        &self,
        sql: String,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let stream = engine_serving::execute_query_stream(
            &self.serving_catalog,
            &sql,
            self.serving_store.as_ref(),
        )
        .await
        .map_err(serving_status)?;
        Ok(Self::encode_response(stream.map_err(|e| {
            FlightError::from_external_error(Box::new(e))
        })))
    }

    /// Run a governed SQL statement (client SQL + caller-resolved governed catalog) through
    /// the governed serving path and Flight-encode the result stream. Mirrors `do_get_sql`
    /// (planning faults map to `invalid_argument`, execution faults to `internal`).
    async fn do_get_governed_sql(
        &self,
        q: engine_wire::flight::GovernedStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let stream = engine_serving::execute_governed_sql_stream(
            &self.serving_catalog,
            &q.sql,
            &q.catalog,
            self.serving_store.as_ref(),
        )
        .await
        .map_err(serving_status)?;
        Ok(Self::encode_response(stream.map_err(|e| {
            FlightError::from_external_error(Box::new(e))
        })))
    }

    /// Run a k-NN vector search and Flight-encode the single resulting
    /// `RecordBatch`. `EngineServingError::NoIndex` maps to `not_found` so the
    /// caller can distinguish a missing index from an internal error.
    async fn do_get_vector_search(
        &self,
        vs: engine_wire::flight::VectorSearchTicket,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let table = TableRef {
            schema: vs.schema,
            name: vs.name,
        };
        let batch = engine_serving::vector_search(
            &self.catalog,
            &self.pool,
            &table,
            &vs.index_name,
            &vs.query,
            vs.k as usize,
            vs.nprobe,
            vs.ef_search,
        )
        .await
        .map_err(serving_status)?;
        Ok(Self::encode_response(futures::stream::iter(
            std::iter::once(Ok::<_, FlightError>(batch)),
        )))
    }

    /// File-ticket data plane: stream an explicit live-file set. Every
    /// ticket-named path must belong to the table's live snapshot (see the
    /// defense-in-depth comment inline).
    async fn do_get_files(
        &self,
        req: engine_wire::flight::FlightTicket,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let table = TableRef {
            schema: req.schema,
            name: req.name,
        };

        // Defense-in-depth: every ticket-named path must belong to the table's
        // live snapshot. The mirror stores absolute `file://` paths, so an
        // unchecked ticket could otherwise name another table's file (or any path
        // FileIO can resolve). Cross-check against the live file set before reading
        // any bytes. An unknown table is itself a bad ticket (no-leak: we never
        // reveal existence beyond "rejected").
        //
        // The snapshot may advance between this check and the read below; a path
        // live now but GC'd by read-time degrades to a benign read error, never a
        // cross-table leak — acceptable for the live-snapshot-only contract.
        let snap = match self.serving_catalog.current_snapshot(&table).await {
            Ok(s) => s,
            // An unknown table is itself a bad ticket; reject without revealing more.
            Err(control_plane_core::ControlPlaneError::NotFound(_)) => {
                return Err(Status::invalid_argument(
                    "flight ticket names an unknown table",
                ));
            }
            // A real backend failure is internal, not the client's fault.
            Err(e) => return Err(Status::internal(e.to_string())),
        };
        let live: HashSet<String> = self
            .serving_catalog
            .files_with_stats(&table, snap.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .map(|f| f.path)
            .collect();
        if !all_in_live_set(&live, &req.files) {
            // Do not echo the offending path — that would confirm what paths exist.
            return Err(Status::invalid_argument(
                "flight ticket names a file outside the table's live snapshot",
            ));
        }

        // The schema is discarded here on purpose: FlightDataEncoderBuilder
        // derives it from the batches below, so we don't pass it explicitly.
        let (_schema, batches) = read_files_as_batches(&self.catalog, &table, &req.files)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // FlightDataEncoderBuilder emits a schema message first, then the data —
        // satisfying the stream's schema-fidelity contract.
        Ok(Self::encode_response(futures::stream::iter(
            batches.into_iter().map(Ok::<_, FlightError>),
        )))
    }
}

#[tonic::async_trait]
impl FlightService for FlightDataService {
    type HandshakeStream =
        Pin<Box<dyn futures::Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
    type ListFlightsStream =
        Pin<Box<dyn futures::Stream<Item = Result<FlightInfo, Status>> + Send>>;
    type DoGetStream = Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoPutStream = Pin<Box<dyn futures::Stream<Item = Result<PutResult, Status>> + Send>>;
    type DoExchangeStream = Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoActionStream =
        Pin<Box<dyn futures::Stream<Item = Result<arrow_flight::Result, Status>> + Send>>;
    type ListActionsStream =
        Pin<Box<dyn futures::Stream<Item = Result<ActionType, Status>> + Send>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        // Decode order + fall-through invariants live with the ticket types:
        // engine_wire::flight::EngineTicket (and its unit pins).
        match EngineTicket::decode(&ticket.ticket)? {
            EngineTicket::Sql(sql) => self.do_get_sql(sql).await,
            EngineTicket::GovernedSql(q) => self.do_get_governed_sql(q).await,
            EngineTicket::VectorSearch(vs) => self.do_get_vector_search(vs).await,
            EngineTicket::Files(ft) => self.do_get_files(ft).await,
        }
    }

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }
    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let any = Any::decode(&descriptor.cmd[..])
            .map_err(|e| Status::invalid_argument(format!("bad flight-sql command: {e}")))?;
        let cmd = any
            .unpack::<CommandStatementQuery>()
            .map_err(|e| Status::invalid_argument(format!("bad CommandStatementQuery: {e}")))?
            .ok_or_else(|| {
                Status::unimplemented("only CommandStatementQuery is supported on this plane")
            })?;
        // The ticket carries the SQL string in a TicketStatementQuery handle; do_get
        // decodes it and runs the stream. No schema is attached to the FlightInfo —
        // the client reads the schema from the do_get stream's first message.
        let ticket = TicketStatementQuery {
            statement_handle: cmd.query.into_bytes().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: ticket.as_any().encode_to_vec().into(),
        });
        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);
        Ok(Response::new(info))
    }
    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }
    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }
    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }
    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

/// Pure membership check for the Flight file-ticket guard: `true` iff every
/// `requested` path is present in `live` (the table's live-snapshot file set).
/// No I/O — the catalog round-trip that builds `live` stays in `do_get`. An empty
/// `requested` is vacuously `true`.
///
/// Both `live` and `requested` are mirror path strings in the **same encoding**
/// (absolute, as stored by the iceberg mirror), so an exact-string `HashSet`
/// compare is correct — no path normalization is needed.
#[must_use = "the membership result must be mapped to a Status"]
pub fn all_in_live_set(live: &HashSet<String>, requested: &[String]) -> bool {
    requested.iter().all(|p| live.contains(p))
}
