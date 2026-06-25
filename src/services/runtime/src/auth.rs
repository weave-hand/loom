//! The authentication seam: a `Subject` extractor, the `require_auth` middleware
//! that resolves a bearer session token to a verified `SubjectId`, and the
//! `protect` combinator both binaries apply to their routers. This is the first
//! and only producer of `ControlPlaneError::Unauthorized` (→ HTTP 401).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::extract::{FromRequestParts, Request, State};
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use control_plane_core::{Auth, ControlPlaneError, SubjectId};
use time::OffsetDateTime;

use crate::token_sha256;

/// A verified request principal, injected into request extensions by
/// [`require_auth`] and handed to handlers by the [`Subject`] extractor.
#[derive(Clone, Debug)]
pub struct Subject(pub SubjectId);

/// Shared state for the auth routes + middleware.
#[derive(Clone)]
pub struct AuthState {
    pub auth: Arc<dyn Auth + Send + Sync>,
    pub session_ttl: Duration,
}

/// Map a control-plane error to an HTTP status. Centralised so the reserved
/// `Unauthorized` variant has exactly one producer path.
pub fn status_for(e: &ControlPlaneError) -> StatusCode {
    match e {
        ControlPlaneError::Unauthorized => StatusCode::UNAUTHORIZED,
        ControlPlaneError::NotFound(_) => StatusCode::NOT_FOUND,
        ControlPlaneError::Conflict(_) => StatusCode::CONFLICT,
        ControlPlaneError::Validation(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn unauthorized() -> Response {
    let e = ControlPlaneError::Unauthorized;
    (status_for(&e), e.to_string()).into_response()
}

/// Extract the bearer token from the `Authorization` header, if present.
fn bearer_token(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.to_string())
}

#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for Subject {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Subject>()
            .cloned()
            .ok_or_else(unauthorized)
    }
}

/// Resolve the bearer session token to a verified `Subject`, inject it into
/// request extensions, and run the handler. Absent/invalid/expired → 401.
pub async fn require_auth(State(st): State<AuthState>, mut req: Request, next: Next) -> Response {
    let Some(token) = bearer_token(req.headers()) else {
        return unauthorized();
    };
    let hash = token_sha256(&token);
    match st
        .auth
        .resolve_session(&hash, OffsetDateTime::now_utc())
        .await
    {
        Ok(Some(sid)) => {
            req.extensions_mut().insert(Subject(sid));
            next.run(req).await
        }
        Ok(None) => unauthorized(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// Apply the authn gate to every route in `router`.
pub fn protect(router: Router, auth: AuthState) -> Router {
    router.route_layer(axum::middleware::from_fn_with_state(auth, require_auth))
}
