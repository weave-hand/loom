//! The unified control-plane error type. Adapters map their native errors into
//! these variants; contract tests assert on variants, never on messages.

/// Result alias used throughout the control plane.
pub type Result<T> = std::result::Result<T, ControlPlaneError>;

#[derive(Debug, thiserror::Error)]
pub enum ControlPlaneError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("serialization: {0}")]
    Serialization(String),
    #[error(transparent)]
    Backend(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn _assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn variants_display_and_are_send_sync() {
        _assert_send_sync::<ControlPlaneError>();
        assert_eq!(
            ControlPlaneError::NotFound("job 7".into()).to_string(),
            "not found: job 7"
        );
        assert_eq!(ControlPlaneError::Unauthorized.to_string(), "unauthorized");
    }
}
