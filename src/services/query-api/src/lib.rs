//! loom query-api: the governed read service. A Rust HTTP chokepoint that resolves
//! ontology types, applies ACL policy by generating SQL, and runs it on a DuckDB
//! serving engine. See docs/superpowers/specs/2026-06-10-query-governed-object-read-slice-design.md.
//!
//! Serving-engine decision (Task 1 spike, GO): embedded duckdb-rs `1.10503.1`
//! bundles DuckDB 1.5.3 — the exact version loom's vendored `ducklake` extension
//! is ABI-locked to — and the spike (tests/spike_duckdb.rs) proves it loads the
//! vendored 1.5.3 ducklake + postgres_scanner extensions and ATTACHes/reads
//! loom's DuckLake-on-Postgres catalog. The serving engine is embedded duckdb-rs.

pub mod action;
pub mod chain_filter;
pub mod engine_client;
pub mod filter;
pub mod handler;
pub mod http;
pub mod params;
pub mod path_parse;
pub mod render;
pub mod serving;
pub mod serving_datafusion;
pub mod sql;
pub mod write_filter;
