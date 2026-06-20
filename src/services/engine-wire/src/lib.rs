//! loom engine-wire: the shared tonic contract between the engine (server) and
//! clients (the worker). Generated code is included from the codegen genrule via
//! the ENGINE_PB env-location (mirrors the postgres crate's SQLX_OFFLINE_DIR).

/// Generated protobuf + tonic client/server stubs.
pub mod pb {
    include!(concat!(env!("ENGINE_PB"), "/loom.engine.v1.rs"));
}

pub mod convert;
