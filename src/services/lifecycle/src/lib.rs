//! Process lifecycle shared by every loom binary: tracing initialisation and
//! graceful shutdown (signal source + bounded drain).
//!
//! Deliberately tiny and dependency-light so the Postgres-free `worker-bin` can
//! depend on it directly; `service_runtime` re-exports the whole surface for the
//! binaries that already depend on it.

mod shutdown;

pub use shutdown::shutdown_signal;

/// Install a `tracing-subscriber` for the process. Uses `RUST_LOG` env (default
/// `info`). Idempotent — a second call from a test harness or re-entrant path does
/// not panic.
pub fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    drop(fmt().with_env_filter(filter).try_init());
}
