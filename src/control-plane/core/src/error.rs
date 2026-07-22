//! The unified control-plane error type. Adapters map their native errors into
//! these variants; contract tests assert on variants, never on messages.

/// Result alias used throughout the control plane.
pub type Result<T> = std::result::Result<T, ControlPlaneError>;

/// `#[non_exhaustive]` so future variants (e.g. a cross-concern validation failure,
/// when reference validation is taken up — see the GitHub issue tracker) are additive rather
/// than a breaking change for downstream `match`es.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ControlPlaneError {
    #[error("not found: {0}")]
    NotFound(String),
    /// A write lost an optimistic-concurrency / uniqueness race. No producer yet —
    /// today's writes are idempotent upserts; reserved for future non-idempotent
    /// writes (e.g. optimistic snapshot commit).
    #[error("conflict: {0}")]
    Conflict(String),
    /// The caller is not authorized. Reserved for the Step-3 service auth layer; the
    /// control plane itself never authenticates a caller (ACL `check` returns a
    /// `Decision`, not an error).
    #[error("unauthorized")]
    Unauthorized,
    #[error("serialization: {0}")]
    Serialization(String),
    /// A request or stored value failed validation (e.g. a malformed or
    /// invalid-property RowFilter at `set_policy`). Distinct from `NotFound`
    /// (missing entity) and `Conflict` (uniqueness/concurrency).
    #[error("validation error: {0}")]
    Validation(String),
    #[error(transparent)]
    Backend(#[from] Box<dyn std::error::Error + Send + Sync>),
}
