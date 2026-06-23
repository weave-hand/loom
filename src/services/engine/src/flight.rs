//! Arrow Flight data plane: stream a table's data-file rows out as Arrow
//! batches. The engine owns Postgres + object store, so it is the data source;
//! the worker is a pure compute client over the wire.

use std::pin::Pin;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use control_plane_core::TableRef;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use control_plane_postgres::read_files_as_batches;
use futures::TryStreamExt;
use sqlx::PgPool;
use tonic::{Request, Response, Status, Streaming};

pub struct FlightDataService {
    pub catalog: SqlCatalog,
    pub pool: PgPool,
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
        let ticket = FlightTicketReq::decode(request.into_inner())?;
        let table = TableRef {
            schema: ticket.schema,
            name: ticket.name,
        };
        let (_schema, batches) = read_files_as_batches(&self.catalog, &table, &ticket.files)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // FlightDataEncoderBuilder emits a schema message first, then the data —
        // satisfying the stream's schema-fidelity contract.
        let input = futures::stream::iter(batches.into_iter().map(Ok));
        let stream = FlightDataEncoderBuilder::new()
            .build(input)
            .map_err(|e| Status::internal(e.to_string()));
        Ok(Response::new(Box::pin(stream)))
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
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
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

/// Decode a `FlightTicket` from the request, mapping a bad ticket to
/// `invalid_argument`.
struct FlightTicketReq {
    schema: String,
    name: String,
    files: Vec<String>,
}
impl FlightTicketReq {
    fn decode(t: Ticket) -> Result<Self, Status> {
        let ft = engine_wire::flight::FlightTicket::decode(&t.ticket)
            .map_err(|e| Status::invalid_argument(format!("bad flight ticket: {e}")))?;
        Ok(Self {
            schema: ft.schema,
            name: ft.name,
            files: ft.files,
        })
    }
}
