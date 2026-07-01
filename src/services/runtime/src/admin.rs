//! Admin-gated user provisioning: the `require_admin` gate (config-named
//! bootstrap admin) plus shared `/admin/users` routes (create / list / disable /
//! enable). Mounted by query-api, the governance surface. The gate is the single
//! admin notion this slice introduces — a verified `Subject` whose id equals the
//! configured bootstrap-admin username; anything else on `/admin/*` is 403.

use std::sync::Arc;

use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use control_plane_core::{
    Acl, Auth, ControlPlaneError, NewUser, PageReq, RoleId, SubjectId, UserSummary,
};

use crate::auth::{AuthState, Subject, protect, status_for, unauthorized};
use crate::hash_password;

/// Shared state for the admin routes + gate.
#[derive(Clone)]
pub struct AdminState {
    pub auth: Arc<dyn Auth + Send + Sync>,
    pub acl: Arc<dyn Acl + Send + Sync>,
    /// The configured bootstrap-admin username (`LOOM_BOOTSTRAP_ADMIN_USERNAME`).
    pub admin_username: String,
}

fn forbidden() -> Response {
    (StatusCode::FORBIDDEN, "forbidden").into_response()
}

/// Gate `/admin/*`: allow only a verified subject whose id equals the configured
/// bootstrap admin. Layered AFTER `require_auth` (which injects [`Subject`]); a
/// missing `Subject` (unauthenticated) is 401, a non-admin is 403.
pub async fn require_admin(State(st): State<AdminState>, req: Request, next: Next) -> Response {
    match req.extensions().get::<Subject>() {
        Some(Subject(sid)) if sid.0 == st.admin_username => next.run(req).await,
        Some(_) => forbidden(),
        None => unauthorized(),
    }
}

#[derive(serde::Deserialize)]
struct CreateUserReq {
    username: String,
    password: String,
    #[serde(default)]
    roles: Vec<String>,
}

#[derive(serde::Serialize)]
struct CreateUserResp {
    username: String,
    subject_id: String,
    /// False iff the user already existed (idempotent create-or-complete-grants).
    created: bool,
    assigned_roles: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// `POST /admin/users` — create (or complete-grants for) a user with a starting
/// password + roles. Sequenced autocommit: `create_user` → `define_subject` →
/// `assign_role`*. A pre-existing username is not a hard conflict (retry-safe;
/// note a retry does NOT reset the stored password — `create_user` is the only
/// non-idempotent step and is skipped once the user exists); an unknown role is a
/// 400 that reports what already completed.
async fn create_user(State(st): State<AdminState>, Json(req): Json<CreateUserReq>) -> Response {
    let Ok(phc) = hash_password(&req.password) else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "password hashing failed").into_response();
    };
    let subject = SubjectId(req.username.clone());
    let created = match st
        .auth
        .create_user(&NewUser {
            subject_id: subject.clone(),
            username: req.username.clone(),
            password_phc: phc,
        })
        .await
    {
        Ok(()) => true,
        Err(ControlPlaneError::Conflict(_)) => false,
        Err(e) => return status_for(&e).into_response(),
    };
    if let Err(e) = st.acl.define_subject(&subject).await {
        return status_for(&e).into_response();
    }
    let mut assigned = Vec::new();
    for r in &req.roles {
        match st.acl.assign_role(&subject, &RoleId(r.clone())).await {
            Ok(()) => assigned.push(r.clone()),
            Err(ControlPlaneError::NotFound(_)) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(CreateUserResp {
                        username: req.username.clone(),
                        subject_id: subject.0.clone(),
                        created,
                        assigned_roles: assigned,
                        error: Some(format!("role does not exist: {r}")),
                    }),
                )
                    .into_response();
            }
            Err(e) => return status_for(&e).into_response(),
        }
    }
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    (
        status,
        Json(CreateUserResp {
            username: req.username.clone(),
            subject_id: subject.0.clone(),
            created,
            assigned_roles: assigned,
            error: None,
        }),
    )
        .into_response()
}

#[derive(serde::Serialize)]
struct UserView {
    username: String,
    subject_id: String,
    disabled: bool,
    created_at: String,
}

#[derive(serde::Serialize)]
struct ListUsersResp {
    users: Vec<UserView>,
}

fn to_view(u: UserSummary) -> UserView {
    let created_at = u
        .created_at
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    UserView {
        username: u.username,
        subject_id: u.subject_id.0,
        disabled: u.disabled,
        created_at,
    }
}

/// `GET /admin/users` — list all users (identity + activation + created-at), never
/// the verifier.
async fn list_users(State(st): State<AdminState>) -> Response {
    match st.auth.list_users(PageReq::unbounded()).await {
        Ok(page) => {
            let users = page.into_iter().map(to_view).collect();
            (StatusCode::OK, Json(ListUsersResp { users })).into_response()
        }
        Err(e) => status_for(&e).into_response(),
    }
}

/// `POST /admin/users/:username/disable` — deactivate (revokes sessions).
async fn disable_user(State(st): State<AdminState>, Path(username): Path<String>) -> Response {
    match st.auth.set_user_disabled(&username, true).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// `POST /admin/users/:username/enable` — reactivate (does not resurrect sessions).
async fn enable_user(State(st): State<AdminState>, Path(username): Path<String>) -> Response {
    match st.auth.set_user_disabled(&username, false).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// Admin routes, behind `require_auth` (401) then [`require_admin`] (403).
pub fn admin_routes(admin: AdminState, auth: AuthState) -> Router {
    let inner = Router::new()
        .route("/admin/users", post(create_user).get(list_users))
        .route("/admin/users/:username/disable", post(disable_user))
        .route("/admin/users/:username/enable", post(enable_user))
        .with_state(admin.clone())
        .route_layer(axum::middleware::from_fn_with_state(admin, require_admin));
    protect(inner, auth)
}
