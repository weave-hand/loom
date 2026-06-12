//! loom ingest: the landing-edge materializer. Turns Arrow batches into a
//! registered DuckLake snapshot + lineage via the part-1 snapshot-commit
//! primitive. See docs/superpowers/specs/2026-06-11-ingest-materializer-primitive-design.md.

pub mod gate;
pub mod infer;
pub mod write;
