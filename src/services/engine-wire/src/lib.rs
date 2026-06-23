//! loom engine-wire: the shared tonic contract between the engine (server) and
//! clients (the worker). Generated code is included from the codegen genrule via
//! the ENGINE_PB env-location (mirrors the postgres crate's SQLX_OFFLINE_DIR).

use control_plane_core::Result;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

/// Generated protobuf + tonic client/server stubs.
pub mod pb {
    include!(concat!(env!("ENGINE_PB"), "/loom.engine.v1.rs"));
}

pub mod client;
pub mod convert;
pub mod flight;

/// Dial the engine's Unix-domain socket and return a tonic [`Channel`].
/// The URI is ignored by the connector; the connector dials the UDS path.
pub(crate) async fn uds_channel(socket: String) -> Result<Channel> {
    Endpoint::try_from("http://[::]:50051")
        .map_err(client::be)?
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket = socket.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(socket).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .map_err(client::be)
}
