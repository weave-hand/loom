//! The authentication seam: a `Subject` extractor, the `require_auth` middleware
//! that resolves a bearer session token to a verified `SubjectId`, and the
//! `protect` combinator both binaries apply to their routers. This is the first
//! and only producer of `ControlPlaneError::Unauthorized` (→ HTTP 401).

use std::collections::HashMap;
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
    ADMIN_ROLE, Auth, ControlPlane, ControlPlaneError, LockoutPolicy, NewServiceAccount, PageReq,
    Redacted, RoleId, SubjectId,
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
    pub lockout: LockoutPolicy,
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

/// Map a control-plane error to an HTTP response, carrying the message for
/// client-error variants. `Validation`/`NotFound`/`Conflict`/`Unauthorized`
/// deliver `{"error": <message>}` — these are admin-facing and hold no secrets,
/// and leaving them bodyless forced callers to guess the cause (#364, e.g. a
/// grant on an unknown type produced a bare `400`). Server errors stay bodyless:
/// their detail is logged server-side, never leaked on the wire (cf. #140).
pub fn error_response(e: &ControlPlaneError) -> Response {
    let status = status_for(e);
    if status.is_server_error() {
        status.into_response()
    } else {
        (
            status,
            axum::Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response()
    }
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
///
/// Public: this is the single bearer-resolution seam shared by the HTTP
/// `require_auth` middleware AND the Flight surfaces (the governed export's
/// `flight_auth::authenticate`, and the external SQL wire) — both accept a
/// session token or a service token via the same fallback order.
pub async fn resolve_bearer(
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

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct LoginReq {
    username: String,
    password: String,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct LoginResp {
    token: String,
}

/// Log in with a username and password; returns a session token once.
///
/// Public. A locked account, unknown user, and wrong password all return the same
/// generic 401 with uniform timing (no enumeration); each failure is recorded and a
/// success clears the counter.
#[utoipa::path(
    post, path = "/auth/login",
    request_body = LoginReq,
    responses(
        (status = 200, description = "Session token, returned exactly once", body = LoginResp),
        (status = 401, description = "Unknown user, bad password, or locked account (uniform)"),
    ),
    tag = "auth",
)]
async fn login(State(st): State<AuthState>, axum::Json(req): axum::Json<LoginReq>) -> Response {
    let now = OffsetDateTime::now_utc();
    let cred = match st.auth.find_password_credential(&req.username).await {
        Ok(Some(c)) => c,
        // Unknown or disabled user: burn comparable time, then the same 401.
        Ok(None) => {
            drop(crate::hash_password(&req.password));
            return unauthorized();
        }
        Err(e) => return status_for(&e).into_response(),
    };

    // Locked: reject with the same generic 401 (burn a dummy hash so a locked
    // account is indistinguishable from a bad password by response and by timing).
    if cred.locked_until.is_some_and(|lu| lu > now) {
        drop(crate::hash_password(&req.password));
        return unauthorized();
    }

    if crate::verify_password(&req.password, cred.password_phc.expose()) {
        if let Err(e) = st.auth.reset_failed_logins(&req.username).await {
            return status_for(&e).into_response();
        }
        let token = crate::generate_session_token();
        let hash = token_sha256(&token);
        #[expect(
            clippy::expect_used,
            reason = "session_ttl comes from Duration::from_secs(u64) which always fits in time::Duration (<292 years)"
        )]
        let expires = now
            + time::Duration::try_from(st.session_ttl).expect("session_ttl fits in time::Duration");
        match st
            .auth
            .create_session(&cred.subject_id, &hash, expires)
            .await
        {
            Ok(()) => (StatusCode::OK, axum::Json(LoginResp { token })).into_response(),
            Err(e) => status_for(&e).into_response(),
        }
    } else {
        if let Err(e) = st
            .auth
            .record_failed_login(&req.username, now, st.lockout)
            .await
        {
            return status_for(&e).into_response();
        }
        unauthorized()
    }
}

/// Revoke the presented session token (idempotent).
#[utoipa::path(
    post, path = "/auth/logout",
    responses((status = 200, description = "Presented session revoked (idempotent)")),
    security(("bearer_auth" = [])),
    tag = "auth",
)]
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

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct ChangePasswordReq {
    current: String,
    new: String,
}

/// Rotate the caller's password and revoke their other sessions.
///
/// Verifies the current password (wrong → 403, nothing changed); the presented session
/// is preserved. Password-strength policy is out of scope.
#[utoipa::path(
    post, path = "/auth/password",
    request_body = ChangePasswordReq,
    responses(
        (status = 200, description = "Password rotated; the caller's other sessions revoked"),
        (status = 403, description = "Current password is incorrect (nothing changed)"),
    ),
    security(("bearer_auth" = [])),
    tag = "auth",
)]
async fn change_password(
    subject: Subject,
    State(st): State<AuthState>,
    headers: axum::http::HeaderMap,
    axum::Json(req): axum::Json<ChangePasswordReq>,
) -> Response {
    let phc = match st.auth.password_phc_for_subject(&subject.0).await {
        Ok(Some(p)) => p,
        // An authenticated subject with no credential should not happen; fail closed.
        Ok(None) => return unauthorized(),
        Err(e) => return status_for(&e).into_response(),
    };
    if !crate::verify_password(&req.current, phc.expose()) {
        return (StatusCode::FORBIDDEN, "current password is incorrect").into_response();
    }
    let Ok(new_phc) = crate::hash_password(&req.new) else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "password hashing failed").into_response();
    };
    if let Err(e) = st
        .auth
        .update_password(&subject.0, &Redacted::new(new_phc))
        .await
    {
        return status_for(&e).into_response();
    }
    // Keep the current session, revoke the rest.
    let keep = bearer_token(&headers).map(|t| token_sha256(&t));
    if let Err(e) = st
        .auth
        .revoke_subject_sessions(&subject.0, keep.as_ref())
        .await
    {
        return status_for(&e).into_response();
    }
    StatusCode::OK.into_response()
}

/// Public auth routes (login). Mount un-gated.
pub fn login_routes(auth: AuthState) -> Router {
    Router::new()
        .route("/auth/login", axum::routing::post(login))
        .with_state(auth)
}

/// Authenticated session routes (logout, self-service password change), behind the
/// authn gate.
pub fn session_routes(auth: AuthState) -> Router {
    protect(
        Router::new()
            .route("/auth/logout", axum::routing::post(logout))
            .route("/auth/password", axum::routing::post(change_password))
            .with_state(auth.clone()),
        auth,
    )
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(login, logout, change_password),
    components(schemas(LoginReq, LoginResp, ChangePasswordReq))
)]
struct AuthApiDoc;

/// OpenAPI fragment for the `login_routes` + `session_routes` surface. Mergeable
/// into a service's document via `utoipa::openapi::OpenApi::merge`; the bearer
/// security scheme the ops reference is registered by the serve seam
/// (`register_bearer_scheme`), not here.
#[must_use]
pub fn auth_openapi() -> utoipa::openapi::OpenApi {
    <AuthApiDoc as utoipa::OpenApi>::openapi()
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

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct CreateAccountReq {
    name: String,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct AccountResp {
    subject_id: String,
    name: String,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct MintTokenReq {
    label: String,
    /// Requested lifetime in seconds. `expires_at = now + ttl`, capped at max_ttl.
    ttl_secs: u64,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
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

/// Create a service account. Admin only.
#[utoipa::path(
    post, path = "/auth/service-accounts",
    request_body = CreateAccountReq,
    responses(
        (status = 200, description = "Service account created", body = AccountResp),
        (status = 403, description = "Caller does not hold the reserved admin role"),
    ),
    security(("bearer_auth" = [])),
    tag = "service-accounts",
)]
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

/// List all service accounts. Admin only.
#[utoipa::path(
    get, path = "/auth/service-accounts",
    responses(
        (status = 200, description = "All service accounts, as `{\"accounts\": [...]}`"),
        (status = 403, description = "Caller does not hold the reserved admin role"),
    ),
    security(("bearer_auth" = [])),
    tag = "service-accounts",
)]
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

/// Mint a bearer token for a service account (plaintext returned once). Admin only.
#[utoipa::path(
    post, path = "/auth/service-accounts/{id}/tokens",
    params(("id" = String, Path, description = "Service-account subject id")),
    request_body = MintTokenReq,
    responses(
        (status = 200, description = "Token minted; plaintext returned exactly once", body = MintTokenResp),
        (status = 400, description = "ttl_secs is zero or exceeds the configured cap"),
        (status = 403, description = "Caller does not hold the reserved admin role"),
    ),
    security(("bearer_auth" = [])),
    tag = "service-accounts",
)]
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

/// List a service account's token metadata (never plaintext). Admin only.
#[utoipa::path(
    get, path = "/auth/service-accounts/{id}/tokens",
    params(("id" = String, Path, description = "Service-account subject id")),
    responses(
        (status = 200, description = "Token metadata (never plaintext tokens), as `{\"tokens\": [...]}`"),
        (status = 403, description = "Caller does not hold the reserved admin role"),
    ),
    security(("bearer_auth" = [])),
    tag = "service-accounts",
)]
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

/// Revoke a service-account token by its hex SHA-256 id (idempotent). Admin only.
#[utoipa::path(
    delete, path = "/auth/service-accounts/{id}/tokens/{token_id}",
    params(
        ("id" = String, Path, description = "Service-account subject id"),
        ("token_id" = String, Path, description = "Hex SHA-256 token id from the mint/list responses"),
    ),
    responses(
        (status = 200, description = "Token revoked (idempotent)"),
        (status = 400, description = "token_id is not a hex-encoded 32-byte SHA-256"),
        (status = 403, description = "Caller does not hold the reserved admin role"),
    ),
    security(("bearer_auth" = [])),
    tag = "service-accounts",
)]
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

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(create_account, list_accounts, mint_token, list_tokens, revoke_token),
    components(schemas(CreateAccountReq, AccountResp, MintTokenReq, MintTokenResp))
)]
struct ServiceAccountApiDoc;

/// OpenAPI fragment for the `service_account_routes` surface. Mergeable into a
/// service's document via `utoipa::openapi::OpenApi::merge`; the bearer security
/// scheme the ops reference is registered by the serve seam
/// (`register_bearer_scheme`), not here.
#[must_use]
pub fn service_account_openapi() -> utoipa::openapi::OpenApi {
    <ServiceAccountApiDoc as utoipa::OpenApi>::openapi()
}

/// Fail-loud read of the service-token TTL cap from the env snapshot
/// (`LOOM_SERVICE_TOKEN_MAX_TTL`, seconds, default 90 days). Mint requests over
/// the cap are rejected (400). A present-but-malformed value is a startup error
/// naming the key — never a silent fallback (iss-config-silent-fallbacks).
pub fn service_token_max_ttl(
    vars: &HashMap<String, String>,
) -> Result<Duration, loom_config::ConfigError> {
    Ok(Duration::from_secs(loom_config::parse_var(
        vars,
        "LOOM_SERVICE_TOKEN_MAX_TTL",
        90 * 24 * 3600_u64,
    )?))
}

/// Fail-loud read of the session TTL from the env snapshot
/// (`LOOM_SESSION_TTL_SECS`, default 24h). Same fallback semantics as
/// [`service_token_max_ttl`].
pub fn session_ttl(vars: &HashMap<String, String>) -> Result<Duration, loom_config::ConfigError> {
    Ok(Duration::from_secs(loom_config::parse_var(
        vars,
        "LOOM_SESSION_TTL_SECS",
        86_400_u64,
    )?))
}

/// Fail-loud read of the failed-login lockout policy from the env snapshot:
/// `LOOM_LOGIN_LOCKOUT_THRESHOLD` (count, default 5), `LOOM_LOGIN_LOCKOUT_WINDOW`
/// and `LOOM_LOGIN_LOCKOUT_DURATION` (seconds, default 900 = 15 min each). Same
/// fallback semantics as [`session_ttl`] — a present-but-malformed value is a
/// startup error naming the key, never a silent fallback.
pub fn login_lockout(
    vars: &HashMap<String, String>,
) -> Result<LockoutPolicy, loom_config::ConfigError> {
    let threshold = loom_config::parse_var(vars, "LOOM_LOGIN_LOCKOUT_THRESHOLD", 5_u32)?;
    let window_secs = loom_config::parse_var(vars, "LOOM_LOGIN_LOCKOUT_WINDOW", 900_u64)?;
    let duration_secs = loom_config::parse_var(vars, "LOOM_LOGIN_LOCKOUT_DURATION", 900_u64)?;
    Ok(LockoutPolicy {
        threshold,
        window: time::Duration::seconds(i64::try_from(window_secs).unwrap_or(i64::MAX)),
        lockout_duration: time::Duration::seconds(i64::try_from(duration_secs).unwrap_or(i64::MAX)),
    })
}
