//! loom query-api: the governed read service. A Rust HTTP chokepoint that resolves
//! ontology types, applies ACL policy by generating SQL, and runs it on the
//! loom-native DataFusion serving engine over the Iceberg mirror (reads stream over
//! the engine wire via `EngineServingClient`).
//! See docs/superpowers/specs/2026-06-10-query-governed-object-read-slice-design.md.

pub mod action;
pub mod chain_filter;
pub mod config;
pub mod engine_action_client;
pub mod engine_client;
pub mod filter;
pub mod flight_export;
pub mod handler;
pub mod http;
pub mod lineage_read;
pub mod openapi;
pub mod openapi_gen;
pub mod params;
pub mod path_parse;
pub mod render;
pub mod serve;
pub mod serving;
pub mod serving_datafusion;
pub mod sql;
pub mod web_static;
pub mod wire_control_plane;
pub mod write_filter;

pub use openapi::{build_openapi, live_openapi};
pub use serve::serve;
