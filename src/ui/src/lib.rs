//! Platform-neutral logic shared by the wasm `app` binary and its native tests.
//! Pure Rust (no web-sys), so it compiles for the host and is `rust_test`-able.

use std::fmt;

/// Join a runtime API base with a request path. An empty base yields a relative
/// (same-origin) URL; a non-empty base is used as a prefix with at most one `/`.
#[must_use]
pub fn url(base: &str, path: &str) -> String {
    if base.is_empty() {
        return path.to_string();
    }
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

/// Why a login attempt failed, mapped to user-facing text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// The server rejected the credentials (HTTP 401).
    BadCredentials,
    /// The server responded with an unexpected status.
    Server(u16),
    /// The request never completed (transport / decode failure).
    Network,
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadCredentials => write!(f, "incorrect username or password"),
            Self::Server(code) => write!(f, "server error ({code})"),
            Self::Network => write!(f, "could not reach the server"),
        }
    }
}

/// Map a non-success HTTP status from `/auth/login` to an [`AuthError`].
#[must_use]
pub fn status_to_error(status: u16) -> AuthError {
    if status == 401 {
        AuthError::BadCredentials
    } else {
        AuthError::Server(status)
    }
}
