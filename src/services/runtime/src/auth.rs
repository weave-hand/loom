//! The authentication seam: a `Subject` extractor, the `require_auth` middleware
//! that resolves a bearer session token to a verified `SubjectId`, and the
//! `protect` combinator both binaries apply to their routers. This is the first
//! and only producer of `ControlPlaneError::Unauthorized` (→ HTTP 401).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::extract::{FromRequestParts, Path, Request, State};
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use control_plane_core::{
    ADMIN_ROLE, Auth, ControlPlane, ControlPlaneError, NewServiceAccount, PageReq, RoleId,
    SubjectId,
};
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

pub(crate) fn unauthorized() -> Response {
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
// Service-account management (admin-gated)
// ---------------------------------------------------------------------------

/// Shared state for the admin-gated service-account routes. Carries the authn store,
/// the control plane used to check the reserved `admin` role, and the mandatory-TTL
/// cap.
#[derive(Clone)]
struct ServiceAccountState {
    auth: Arc<dyn Auth + Send + Sync>,
    cp: Arc<dyn ControlPlane>,
    max_ttl: Duration,
}

/// 403 unless the verified subject holds the reserved `admin` role. Matches the
/// `/admin/*` gate (`crate::admin::require_admin`); a lookup error fails closed.
async fn ensure_admin(subject: &Subject, st: &ServiceAccountState) -> Result<(), Response> {
    match st
        .cp
        .acl()
        .has_role(&subject.0, &RoleId(ADMIN_ROLE.to_string()))
        .await
    {
        Ok(true) => Ok(()),
        _ => Err((
            StatusCode::FORBIDDEN,
            "service-account management is admin-only",
        )
            .into_response()),
    }
}

#[derive(serde::Deserialize)]
struct CreateAccountReq {
    name: String,
}

#[derive(serde::Serialize)]
struct AccountResp {
    subject_id: String,
    name: String,
}

#[derive(serde::Deserialize)]
struct MintTokenReq {
    label: String,
    /// Requested lifetime in seconds. `expires_at = now + ttl`, capped at max_ttl.
    ttl_secs: u64,
}

#[derive(serde::Serialize)]
struct MintTokenResp {
    /// The plaintext token — returned exactly once, never persisted or re-derivable.
    token: String,
    /// Hex SHA-256 of the token: its stable id, usable in the revoke URL.
    token_id: String,
    label: String,
    /// Expiry as a Unix timestamp (seconds).
    expires_at: i64,
}

#[derive(serde::Serialize)]
struct TokenMetaResp {
    token_id: String,
    label: String,
    created_at: i64,
    expires_at: i64,
    revoked_at: Option<i64>,
}

/// `POST /auth/service-accounts` — admin. Create a service account.
async fn create_account(
    subject: Subject,
    State(st): State<ServiceAccountState>,
    axum::Json(req): axum::Json<CreateAccountReq>,
) -> Response {
    if let Err(r) = ensure_admin(&subject, &st).await {
        return r;
    }
    // subject_id == name keeps machine identities operator-legible, mirroring how the
    // bootstrap admin's subject_id equals its username.
    let account = NewServiceAccount {
        subject_id: SubjectId(req.name.clone()),
        name: req.name.clone(),
    };
    match st.auth.create_service_account(&account).await {
        Ok(()) => (
            StatusCode::OK,
            axum::Json(AccountResp {
                subject_id: req.name.clone(),
                name: req.name,
            }),
        )
            .into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// `GET /auth/service-accounts` — admin. List service accounts.
async fn list_accounts(subject: Subject, State(st): State<ServiceAccountState>) -> Response {
    if let Err(r) = ensure_admin(&subject, &st).await {
        return r;
    }
    match st.auth.list_service_accounts(PageReq::unbounded()).await {
        Ok(page) => {
            let accounts: Vec<AccountResp> = page
                .items
                .into_iter()
                .map(|a| AccountResp {
                    subject_id: a.subject_id.0,
                    name: a.name,
                })
                .collect();
            (
                StatusCode::OK,
                axum::Json(serde_json::json!({ "accounts": accounts })),
            )
                .into_response()
        }
        Err(e) => status_for(&e).into_response(),
    }
}

/// `POST /auth/service-accounts/{id}/tokens` — admin. Mint a token (plaintext once).
async fn mint_token(
    subject: Subject,
    State(st): State<ServiceAccountState>,
    Path(id): Path<String>,
    axum::Json(req): axum::Json<MintTokenReq>,
) -> Response {
    if let Err(r) = ensure_admin(&subject, &st).await {
        return r;
    }
    // Mandatory-TTL cap: reject an over-cap (or zero) request — no immortal tokens.
    let ttl = Duration::from_secs(req.ttl_secs);
    if req.ttl_secs == 0 || ttl > st.max_ttl {
        return (
            StatusCode::BAD_REQUEST,
            format!(
                "ttl_secs must be in 1..={} (LOOM_SERVICE_TOKEN_MAX_TTL)",
                st.max_ttl.as_secs()
            ),
        )
            .into_response();
    }
    let Ok(ttl_time) = time::Duration::try_from(ttl) else {
        return (StatusCode::BAD_REQUEST, "ttl_secs too large").into_response();
    };
    let expires = OffsetDateTime::now_utc() + ttl_time;
    let token = crate::generate_session_token();
    let hash = token_sha256(&token);
    match st
        .auth
        .create_service_token(&SubjectId(id), &hash, &req.label, expires)
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            axum::Json(MintTokenResp {
                token,
                token_id: hex::encode(hash),
                label: req.label,
                expires_at: expires.unix_timestamp(),
            }),
        )
            .into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// `GET /auth/service-accounts/{id}/tokens` — admin. List a token's metadata.
async fn list_tokens(
    subject: Subject,
    State(st): State<ServiceAccountState>,
    Path(id): Path<String>,
) -> Response {
    if let Err(r) = ensure_admin(&subject, &st).await {
        return r;
    }
    match st
        .auth
        .list_service_tokens(&SubjectId(id), PageReq::unbounded())
        .await
    {
        Ok(page) => {
            let tokens: Vec<TokenMetaResp> = page
                .items
                .into_iter()
                .map(|t| TokenMetaResp {
                    token_id: hex::encode(t.token_sha256),
                    label: t.label,
                    created_at: t.created_at.unix_timestamp(),
                    expires_at: t.expires_at.unix_timestamp(),
                    revoked_at: t.revoked_at.map(|r| r.unix_timestamp()),
                })
                .collect();
            (
                StatusCode::OK,
                axum::Json(serde_json::json!({ "tokens": tokens })),
            )
                .into_response()
        }
        Err(e) => status_for(&e).into_response(),
    }
}

/// `DELETE /auth/service-accounts/{id}/tokens/{token_id}` — admin. Revoke a token,
/// addressed by its hex SHA-256 id (from the mint/list responses). Idempotent.
async fn revoke_token(
    subject: Subject,
    State(st): State<ServiceAccountState>,
    Path((_id, token_id)): Path<(String, String)>,
) -> Response {
    if let Err(r) = ensure_admin(&subject, &st).await {
        return r;
    }
    let Ok(bytes) = hex::decode(&token_id) else {
        return (StatusCode::BAD_REQUEST, "token_id is not valid hex").into_response();
    };
    let Ok(hash): Result<[u8; 32], _> = bytes.try_into() else {
        return (
            StatusCode::BAD_REQUEST,
            "token_id must be a 32-byte sha-256",
        )
            .into_response();
    };
    match st.auth.revoke_service_token(&hash).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// Admin-gated service-account management routes, behind the authn gate. `auth`
/// supplies authn; `cp` supplies the ACL check gating management on the reserved
/// `admin` role; `max_ttl` caps minted-token lifetime.
pub fn service_account_routes(
    auth: AuthState,
    cp: Arc<dyn ControlPlane>,
    max_ttl: Duration,
) -> Router {
    let mgmt = ServiceAccountState {
        auth: auth.auth.clone(),
        cp,
        max_ttl,
    };
    protect(
        Router::new()
            .route(
                "/auth/service-accounts",
                axum::routing::post(create_account).get(list_accounts),
            )
            .route(
                "/auth/service-accounts/:id/tokens",
                axum::routing::post(mint_token).get(list_tokens),
            )
            .route(
                "/auth/service-accounts/:id/tokens/:token_id",
                axum::routing::delete(revoke_token),
            )
            .with_state(mgmt),
        auth,
    )
}

/// Read the service-token TTL cap from `LOOM_SERVICE_TOKEN_MAX_TTL` (seconds, default
/// 90 days). Mint requests over this are rejected (400).
pub fn service_token_max_ttl_from_env() -> Duration {
    std::env::var("LOOM_SERVICE_TOKEN_MAX_TTL")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(90 * 24 * 3600))
}

/// Read the session TTL from `LOOM_SESSION_TTL_SECS` (default 24h).
pub fn session_ttl_from_env() -> Duration {
    std::env::var("LOOM_SESSION_TTL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(86_400))
}
