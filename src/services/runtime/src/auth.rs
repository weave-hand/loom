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
use control_plane_core::{Auth, ControlPlaneError, NewUser, SubjectId};
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

/// Resolve a bearer token hash to a subject: try a login session first (interactive
/// traffic dominates), then a service token. Both are constant-time hash lookups, so
/// order is performance-only. This is the single sequencing point so there stays
/// exactly one path that produces `Unauthorized`.
async fn resolve_bearer(
    auth: &(dyn Auth + Send + Sync),
    hash: &[u8; 32],
    now: OffsetDateTime,
) -> Result<Option<SubjectId>, ControlPlaneError> {
    if let Some(sid) = auth.resolve_session(hash, now).await? {
        return Ok(Some(sid));
    }
    auth.resolve_service_token(hash, now).await
}

/// Resolve the bearer session token to a verified `Subject`, inject it into
/// request extensions, and run the handler. Absent/invalid/expired → 401.
pub async fn require_auth(State(st): State<AuthState>, mut req: Request, next: Next) -> Response {
    let Some(token) = bearer_token(req.headers()) else {
        return unauthorized();
    };
    let hash = token_sha256(&token);
    match resolve_bearer(st.auth.as_ref(), &hash, OffsetDateTime::now_utc()).await {
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

// ---------------------------------------------------------------------------
// Login / logout route handlers
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct LoginReq {
    username: String,
    password: String,
}

#[derive(serde::Serialize)]
struct LoginResp {
    token: String,
}

/// `POST /auth/login` — public. Verify the password, mint a session token,
/// return it once. A uniform 401 on unknown user OR bad password (and a
/// dummy hash on unknown user keeps login timing uniform).
async fn login(State(st): State<AuthState>, axum::Json(req): axum::Json<LoginReq>) -> Response {
    match st.auth.find_password_credential(&req.username).await {
        Ok(Some(cred)) => {
            if crate::verify_password(&req.password, &cred.password_phc) {
                let token = crate::generate_session_token();
                let hash = token_sha256(&token);
                #[expect(
                    clippy::expect_used,
                    reason = "session_ttl comes from Duration::from_secs(u64) which always fits in time::Duration (<292 years)"
                )]
                let expires = OffsetDateTime::now_utc()
                    + time::Duration::try_from(st.session_ttl)
                        .expect("session_ttl fits in time::Duration");
                match st
                    .auth
                    .create_session(&cred.subject_id, &hash, expires)
                    .await
                {
                    Ok(()) => (StatusCode::OK, axum::Json(LoginResp { token })).into_response(),
                    Err(e) => status_for(&e).into_response(),
                }
            } else {
                unauthorized()
            }
        }
        // Unknown user: burn comparable time with a dummy hash, then the same 401.
        Ok(None) => {
            drop(crate::hash_password(&req.password));
            unauthorized()
        }
        Err(e) => status_for(&e).into_response(),
    }
}

/// `POST /auth/logout` — authenticated. Revoke the presented session
/// (idempotent). The `Subject` extractor enforces the caller is verified;
/// the token to revoke is re-read from the `Authorization` header.
async fn logout(
    _subject: Subject,
    State(st): State<AuthState>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Some(token) = bearer_token(&headers) {
        let hash = token_sha256(&token);
        if let Err(e) = st.auth.revoke_session(&hash).await {
            return status_for(&e).into_response();
        }
    }
    StatusCode::OK.into_response()
}

/// Public auth routes (login). Mount un-gated.
pub fn login_routes(auth: AuthState) -> Router {
    Router::new()
        .route("/auth/login", axum::routing::post(login))
        .with_state(auth)
}

/// Authenticated session routes (logout), behind the authn gate.
pub fn session_routes(auth: AuthState) -> Router {
    protect(
        Router::new()
            .route("/auth/logout", axum::routing::post(logout))
            .with_state(auth.clone()),
        auth,
    )
}

// ---------------------------------------------------------------------------
// Bootstrap-admin seeding
// ---------------------------------------------------------------------------

/// Failure seeding the bootstrap admin.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error(transparent)]
    Hash(#[from] crate::AuthError),
    #[error(transparent)]
    Store(#[from] ControlPlaneError),
}

/// Read the session TTL from `LOOM_SESSION_TTL_SECS` (default 24h).
pub fn session_ttl_from_env() -> Duration {
    std::env::var("LOOM_SESSION_TTL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(86_400))
}

/// Seed `username`/`password` as the first user iff the store has no users yet.
/// The admin's subject id equals its username (role assignment stays an operator
/// ACL task). A non-empty store is a no-op (so a restart never re-seeds).
pub async fn bootstrap_admin<A: Auth>(
    auth: &A,
    username: &str,
    password: &str,
) -> Result<(), BootstrapError> {
    if auth.has_any_user().await? {
        return Ok(());
    }
    let password_phc = crate::hash_password(password)?;
    auth.create_user(&NewUser {
        subject_id: SubjectId(username.to_string()),
        username: username.to_string(),
        password_phc,
    })
    .await?;
    Ok(())
}
