//! The cross-concern transaction seam. `ControlPlane::begin` opens a unit of work;
//! operations issued on the returned `Tx` commit together or roll back together.
//!
//! NOTE: `probe_put`/`probe_get` are TEMPORARY scaffolding. They exist only so the
//! contract suite can verify commit-visibility / rollback / isolation before any
//! real concern provides Tx operations. They are removed once Phase 1 (queue) puts
//! real operations on `Tx`.

use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait ControlPlane: Send + Sync {
    /// Open a unit of work. Issue operations on the returned `Tx`, then
    /// `commit` or `rollback`. (Concern accessors — `queue()`, `catalog()`, … —
    /// are added in their respective phases.)
    async fn begin(&self) -> Result<Box<dyn Tx + Send>>;
}

#[async_trait]
pub trait Tx: Send {
    /// Commit all staged operations.
    async fn commit(self: Box<Self>) -> Result<()>;
    /// Discard all staged operations.
    async fn rollback(self: Box<Self>) -> Result<()>;

    /// TEMPORARY (see module docs): stage a key/value within this unit of work.
    async fn probe_put(&mut self, key: &str, val: i64) -> Result<()>;
    /// TEMPORARY (see module docs): read a key — own staged writes first, then
    /// committed state. Uncommitted writes from *other* transactions are not visible.
    async fn probe_get(&mut self, key: &str) -> Result<Option<i64>>;
}
