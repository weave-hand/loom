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
//! # What is cached, precisely
//!
//! Only the *arbitrary-SQL* governed plane — [`crate::governed::execute_governed_sql_stream`],
//! which query-api uses for its SQL console and `EXPLAIN` validation — stays uncached. It
//! builds its own session ([`crate::governed::build_session`]) and never comes through here.
//!
//! **Every other read does come through here, ACL-bearing reads included.** query-api's
//! `read_object` resolves the subject's policy, compiles the row filter and column
//! projection into SQL *text*, and ships it as a plain `CommandStatementQuery`; the engine
//! runs that through [`crate::serving::execute_query_stream`], i.e. through the context
//! this module hands out. So "governed" and "cached" are not opposites, and any reasoning
//! here has to hold for ACL-filtered SQL. `//src/services/query-api:liquid-cache-acl-e2e`
//! is the standing assertion that it does: two subjects with disjoint row filters read the
//! same table over one warm cache and each keeps seeing only its own rows.
//!
//! The reason that is sound rather than lucky: LiquidCache keys cache entries by *file and
//! column*, and caches scanned data, not query results. Two subjects sharing an entry share
//! no more than they already share by reading the same Parquet file; each query's predicate
//! — including the one compiled from its ACL policy — is still evaluated per query, and the
//! forced `execution.parquet.pushdown_filters = true` changes where that evaluation happens,
//! not whether it happens.
//!
//! # Why this is opt-in
//!
//! Two things keep it off unless an operator asks for it:
//!
//! 1. **Resource budgeting.** LiquidCache carries its own memory budget *and* its own
//!    on-disk cache, both outside DataFusion's `MemoryPool` and outside the `DiskManager`
//!    that [`crate::governed::build_session`] deliberately disables so a statement's memory
//!    budget cannot be traded for an unbounded-`/tmp` disk DoS. The serving path has no
//!    such budget to breach, but an operator turning this on is adding an accounting
//!    channel nothing else in loom bounds.
//! 2. **`io_uring` is a hard prerequisite.** The cache's `t4` store mounts with the
//!    `io-uring` feature on by default, so [`configure`] fails with `ENOSYS` wherever the
//!    syscall is unavailable — notably under the container seccomp profiles common in
//!    Kubernetes, which is loom's own deploy target. Being opt-in is what keeps that a
//!    startup choice rather than a surprise.
//!
//! See `docs/spikes/2026-09-18-liquid-cache-evaluation.md` for the full evaluation.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::catalog::MemoryCatalogProviderList;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
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

/// The process-lifetime cache, held as a **template** `SessionState` that [`new_session`]
/// derives each query's state from. Not handed out directly, and not cloned as-is — see
/// [`new_session`] for why the catalog registry must not be shared.
///
/// Something process-lifetime is required for the cache to do anything at all:
/// [`crate::serving::execute_query_stream`] builds a context per query, so a per-context
/// cache would never survive to a second query and would never hit.
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
///
/// # The catalog registry is deliberately NOT shared
///
/// `SessionState` is `Clone`, but its clone is shallow: `catalog_list` is an `Arc`, so a
/// bare `state.clone()` hands every query the *same* table registry. That is wrong here in
/// two escalating ways, and the first one hides the second:
///
/// 1. [`crate::serving::execute_query_stream`] registers the statement's tables by name
///    into the context it is given. With a shared registry the second query to mention a
///    table dies on `The table <name> already exists` — so the cached path breaks on the
///    second query, which is to say on every real workload.
/// 2. Worse, had registration been overwrite-rather-than-error, a shared registry would
///    mean one query's registration could serve another's scan. Registration binds a
///    snapshot (`execute_query_stream`'s `at`) and a concrete file list, and the SQL that
///    arrives here already has the caller's ACL row filter and column projection compiled
///    into it by query-api — so a stale binding is a correctness and a governance fault,
///    not just a stale read.
///
/// So each query gets a fresh [`MemoryCatalogProviderList`] while inheriting everything the
/// cache actually needs from the template: the `SessionConfig` (`build` sets
/// `pushdown_filters` and friends), the physical-optimizer rules — including the
/// `LocalModeOptimizer` whose `Arc` *is* the shared cache — and the registered UDFs.
/// `create_default_catalog_and_schema` is re-asserted because
/// `SessionStateBuilder::new_from_existing` clears it when the state it copies already has
/// a default catalog, which would otherwise leave the fresh registry with nothing to
/// register into.
///
/// Asserted end-to-end by `//src/services/query-api:liquid-cache-acl-e2e`.
#[must_use]
pub fn new_session() -> SessionContext {
    match SHARED.get() {
        Some(state) => {
            let config = state
                .config()
                .clone()
                .with_create_default_catalog_and_schema(true);
            let fresh = SessionStateBuilder::new_from_existing(state.clone())
                .with_config(config)
                .with_catalog_list(Arc::new(MemoryCatalogProviderList::new()))
                .build();
            SessionContext::new_with_state(fresh)
        }
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
