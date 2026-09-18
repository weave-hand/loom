//! Optional [LiquidCache](https://github.com/datafusion-contrib/liquid-cache) under the
//! serving path.
//!
//! LiquidCache is a pushdown cache for DataFusion: it transcodes scanned Parquet into a
//! cache-specific columnar format, keeps query-relevant columns resident in memory, and
//! spills the rest to local SSD with direct I/O. It hooks in as a **`PhysicalOptimizerRule`**
//! (`LocalModeOptimizer`) that rewrites each `DataSourceExec` over a `ParquetSource` into
//! one over a `LiquidParquetSource`. That matters here because
//! [`crate::serving::IcebergMirrorTableProvider::scan`] emits exactly that node shape, so
//! the cache applies to loom's hand-written provider with no provider changes at all, and
//! composes with (rather than replacing) the mirror-stat file pruning that runs first.
//!
//! # Why this is opt-in
//!
//! Two unresolved questions keep this off unless an operator asks for it:
//!
//! 1. **Resource budgeting.** [`crate::governed::build_session`] deliberately pairs a
//!    `GreedyMemoryPool` with `DiskManagerMode::Disabled` so a statement's memory budget
//!    actually binds and cannot be traded for an unbounded-`/tmp` disk DoS. LiquidCache
//!    introduces its own memory budget *and* its own on-disk cache, both outside
//!    DataFusion's `MemoryPool` and outside the disabled `DiskManager`. Until that
//!    reasoning is redone, the **governed** path (arbitrary client SQL under ACL) is
//!    deliberately NOT cached — only the server-built serving path is.
//! 2. **ACL interaction.** `LiquidCacheLocalBuilder` forces
//!    `execution.parquet.pushdown_filters = true`, which makes predicates — including
//!    row filters from [`crate::governed::GovernedTableProvider`] — candidates for
//!    evaluation *inside* the cache layer, over transcoded data keyed per file and shared
//!    across subjects. That is very likely sound, but "very likely" is not the standard
//!    for the governance boundary, and it is the second reason the governed path is
//!    excluded here.
//!
//! See `docs/spikes/2026-09-18-liquid-cache-evaluation.md` for the full evaluation.

use std::path::PathBuf;

use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::session_state::SessionState;
use liquid_cache_datafusion_local::LiquidCacheLocalBuilder;
use tokio::sync::OnceCell;

use crate::serving::EngineServingError;

/// Typed configuration for the serving-path cache. Parsed from the environment by the
/// `engine` binary (`EngineTuning`), never read from the environment here — engine-serving
/// stays env-free, matching how [`crate::sql_limits::GovernedSqlLimits`] is threaded in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiquidCacheConfig {
    /// Directory backing the cache's on-disk tier. LiquidCache mounts a `t4` store at
    /// `<cache_dir>/liquid_cache.t4` and uses direct I/O (`O_DIRECT`) against it, so this
    /// should be node-local scratch, never a network filesystem.
    pub cache_dir: PathBuf,
    /// Ceiling on the cache's in-memory tier, in bytes. This is the cache's OWN budget and
    /// is accounted separately from any DataFusion `MemoryPool` — see the module note.
    pub max_memory_bytes: usize,
}

/// The process-lifetime cache. A `SessionState` (not a `SessionContext`) so each query can
/// still get a fresh context for its own table registrations while sharing one cache and
/// one `LocalModeOptimizer`: `SessionState` is `Clone`, and the clone carries the `Arc`'d
/// rule, so the cache itself is shared rather than rebuilt.
///
/// Without this, the cache would be pointless: [`crate::serving::execute_query_stream`]
/// builds a `SessionContext` per query, so a per-context cache would never survive to a
/// second query and would never hit.
static SHARED: OnceCell<SessionState> = OnceCell::const_new();

/// Build the shared cache-backed `SessionState` once, for the life of the process.
///
/// Idempotent and safe to race: the first caller wins and every later caller observes that
/// same state, so the cache is never built twice. Calling this is what switches the serving
/// path from uncached to cached; if it is never called, [`new_session`] returns a plain
/// context and nothing about serving changes.
///
/// # Errors
/// Returns [`EngineServingError::Engine`] if the cache directory cannot be mounted (e.g. the
/// path is not writable, or the filesystem does not support the direct-I/O the `t4` store
/// opens with).
pub async fn configure(cfg: &LiquidCacheConfig) -> Result<(), EngineServingError> {
    SHARED
        .get_or_try_init(|| async {
            // `build` overrides several session options itself — `pushdown_filters = true`,
            // `schema_force_view_types = false`, `skip_metadata`/`skip_arrow_metadata`, and
            // the batch size — so the config handed in is a starting point, not the last
            // word. The returned `LiquidCacheParquetRef` is the cache handle; the optimizer
            // rule already holds an `Arc` to it, so dropping our copy does not drop the
            // cache, and loom has no admin surface to report on it yet.
            let (ctx, _cache) = LiquidCacheLocalBuilder::new()
                .with_cache_dir(cfg.cache_dir.clone())
                .with_max_memory_bytes(cfg.max_memory_bytes)
                .build(SessionConfig::new())
                .await
                .map_err(|e| {
                    EngineServingError::Engine(format!(
                        "liquid cache: failed to mount cache at {}: {e}",
                        cfg.cache_dir.display()
                    ))
                })?;
            Ok(ctx.state())
        })
        .await
        .map(|_| ())
}

/// A `SessionContext` for one query.
///
/// Returns a context sharing the process-wide cache when [`configure`] has run, and a plain
/// `SessionContext::new()` otherwise — so the uncached path is byte-identical to what
/// serving did before this module existed.
#[must_use]
pub fn new_session() -> SessionContext {
    match SHARED.get() {
        Some(state) => SessionContext::new_with_state(state.clone()),
        None => SessionContext::new(),
    }
}

/// Whether the serving path is currently cache-backed. Exposed for the engine binary's
/// startup log, so an operator can tell from the logs whether the cache actually mounted
/// rather than inferring it from the absence of an error.
#[must_use]
pub fn is_enabled() -> bool {
    SHARED.get().is_some()
}
