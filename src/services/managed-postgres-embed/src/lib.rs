//! Embeds the PostgreSQL distribution into the binary and self-extracts it at
//! runtime, so an embedded loom boot needs no PG binaries pre-staged. Gated in
//! its own crate so the 11.7 MB never reaches lean service binaries.

/// The PG distribution `.tar.gz`, baked in at compile time. `LOOM_PG_TARBALL` is
/// the buck `$(location :postgres-tarball)` (a plain absolute path).
static PG_TARBALL: &[u8] = include_bytes!(env!("LOOM_PG_TARBALL"));

/// The pinned PostgreSQL version (e.g. "17.9.0"), from `PG_VERSION` in
/// `//src/control-plane/postgres:BUCK`. Used as the extraction cache key.
pub const PG_VERSION: &str = env!("LOOM_PG_VERSION");

/// Length of the embedded distribution in bytes (forces the `include_bytes!` to
/// materialize; used by the embed-present test).
#[must_use]
pub fn pg_tarball_len() -> usize {
    PG_TARBALL.len()
}
