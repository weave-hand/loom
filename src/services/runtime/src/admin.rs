//! Admin-gated user provisioning: the `require_admin` gate (reserved `admin`
//! role) plus shared `/admin/users` routes (create / list / disable / enable).
//! Mounted by query-api, the governance surface. The gate is the single admin
//! notion this slice introduces — a verified `Subject` holding the reserved
//! `admin` role (`control_plane_core::ADMIN_ROLE`, the role `loom create-admin`
//! assigns); anything else on `/admin/*` is 403.

use std::sync::Arc;

use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use control_plane_core::{
    ADMIN_ROLE, Auth, ControlPlane, ControlPlaneError, NewUser, PageReq, RoleId, SubjectId,
    UserSummary,
};

use crate::auth::{AuthState, Subject, protect, status_for, unauthorized};
use crate::hash_password;

/// Shared state for the admin routes + gate.
#[derive(Clone)]
pub struct AdminState {
    pub auth: Arc<dyn Auth + Send + Sync>,
    /// The direct (postgres-backed) control plane. Supplies `acl()` for the gate
    /// and role/grant writes, and `ontology()` for `define_type`.
    pub cp: Arc<dyn ControlPlane>,
}

fn forbidden() -> Response {
    (StatusCode::FORBIDDEN, "forbidden").into_response()
}

/// Gate `/admin/*`: allow only a verified subject holding the reserved `admin`
/// role. Layered AFTER `require_auth` (which injects [`Subject`]); missing
/// `Subject` → 401, a non-admin → 403, a lookup error → 403 (fail closed).
pub async fn require_admin(State(st): State<AdminState>, req: Request, next: Next) -> Response {
    let Some(Subject(sid)) = req.extensions().get::<Subject>().cloned() else {
        return unauthorized();
    };
    match st
        .cp
        .acl()
        .has_role(&sid, &RoleId(ADMIN_ROLE.to_string()))
        .await
    {
        Ok(true) => next.run(req).await,
        Ok(false) => forbidden(),
        Err(_) => forbidden(),
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
    if let Err(e) = st.cp.acl().define_subject(&subject).await {
        return status_for(&e).into_response();
    }
    let mut assigned = Vec::new();
    for r in &req.roles {
        match st.cp.acl().assign_role(&subject, &RoleId(r.clone())).await {
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
