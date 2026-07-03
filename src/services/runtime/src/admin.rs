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
    ADMIN_ROLE, Action, Auth, ControlPlane, ControlPlaneError, Effect, NewUser, ObjectType,
    PageReq, PolicyTarget, PropertyDef, RoleId, SubjectId, TableRef, TypeName, UserSummary,
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

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct CreateUserReq {
    username: String,
    password: String,
    #[serde(default)]
    roles: Vec<String>,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
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
#[utoipa::path(
    post, path = "/admin/users",
    request_body = CreateUserReq,
    responses(
        (status = 201, description = "User created with all roles assigned", body = CreateUserResp),
        (status = 200, description = "User already existed; grants completed (password NOT reset)", body = CreateUserResp),
        (status = 400, description = "Unknown role; reports what already completed", body = CreateUserResp),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
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

#[derive(serde::Serialize, utoipa::ToSchema)]
struct UserView {
    username: String,
    subject_id: String,
    disabled: bool,
    created_at: String,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
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
#[utoipa::path(
    get, path = "/admin/users",
    responses(
        (status = 200, description = "All users (identity + activation + created-at)", body = ListUsersResp),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
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
#[utoipa::path(
    post, path = "/admin/users/{username}/disable",
    params(("username" = String, Path, description = "Username to deactivate")),
    responses((status = 200, description = "User deactivated; sessions revoked")),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn disable_user(State(st): State<AdminState>, Path(username): Path<String>) -> Response {
    match st.auth.set_user_disabled(&username, true).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// `POST /admin/users/:username/enable` — reactivate (does not resurrect sessions).
#[utoipa::path(
    post, path = "/admin/users/{username}/enable",
    params(("username" = String, Path, description = "Username to reactivate")),
    responses((status = 200, description = "User reactivated (sessions are not resurrected)")),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn enable_user(State(st): State<AdminState>, Path(username): Path<String>) -> Response {
    match st.auth.set_user_disabled(&username, false).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct ResetPasswordReq {
    new: String,
}

/// `POST /admin/users/:username/password` — admin. Set a new password for any user
/// with NO current-verify (operator-driven recovery), and revoke ALL that user's
/// sessions (force re-login). `NotFound` (404) if the username is unknown. The
/// subject id equals the username, mirroring `create_user` on this surface.
#[utoipa::path(
    post, path = "/admin/users/{username}/password",
    params(("username" = String, Path, description = "Username whose password to reset")),
    request_body = ResetPasswordReq,
    responses(
        (status = 200, description = "Password reset; ALL the user's sessions revoked"),
        (status = 404, description = "Unknown username"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn reset_password(
    State(st): State<AdminState>,
    Path(username): Path<String>,
    Json(req): Json<ResetPasswordReq>,
) -> Response {
    let Ok(new_phc) = hash_password(&req.new) else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "password hashing failed").into_response();
    };
    let subject = SubjectId(username);
    if let Err(e) = st.auth.update_password(&subject, &new_phc).await {
        return status_for(&e).into_response();
    }
    if let Err(e) = st.auth.revoke_subject_sessions(&subject, None).await {
        return status_for(&e).into_response();
    }
    StatusCode::OK.into_response()
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct CreateRoleReq {
    role: String,
}

/// `POST /admin/roles` — declare a new role (governance target for grants).
#[utoipa::path(
    post, path = "/admin/roles",
    request_body = CreateRoleReq,
    responses((status = 201, description = "Role declared")),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn create_role(State(st): State<AdminState>, Json(req): Json<CreateRoleReq>) -> Response {
    match st.cp.acl().define_role(&RoleId(req.role.clone())).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(serde_json::json!({ "role": req.role })),
        )
            .into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// `GET /admin/roles` — list all declared role ids.
#[utoipa::path(
    get, path = "/admin/roles",
    responses((status = 200, description = "All declared role ids, as `{\"roles\": [...]}`")),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn list_roles(State(st): State<AdminState>) -> Response {
    match st.cp.acl().list_roles().await {
        Ok(roles) => Json(serde_json::json!({
            "roles": roles.into_iter().map(|r| r.0).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct GrantReq {
    action: String,
    r#type: String,
}

/// `POST /admin/roles/:role/grants` — grant a role coarse `Read`/`Write` Allow
/// on a type. An unknown grant-target type surfaces as 400 (the control plane
/// returns `Validation`, mapped by `status_for`).
#[utoipa::path(
    post, path = "/admin/roles/{role}/grants",
    params(("role" = String, Path, description = "Role receiving the grant")),
    request_body = GrantReq,
    responses(
        (status = 201, description = "Granted"),
        (status = 400, description = "action is not read|write, or unknown grant-target type"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn grant(
    State(st): State<AdminState>,
    Path(role): Path<String>,
    Json(req): Json<GrantReq>,
) -> Response {
    let action = match req.action.as_str() {
        "read" => Action::Read,
        "write" => Action::Write,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "action must be read|write" })),
            )
                .into_response();
        }
    };
    let target = PolicyTarget::Type(TypeName(req.r#type.clone()));
    match st
        .cp
        .acl()
        .grant(&RoleId(role), action, target, Effect::Allow)
        .await
    {
        Ok(()) => (StatusCode::CREATED, "granted").into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct PropReq {
    name: String,
    ty: String,
    #[serde(default)]
    required: bool,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct TableReq {
    schema: String,
    name: String,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct DefineModelReq {
    name: String,
    table: TableReq,
    identity: Option<String>,
    properties: Vec<PropReq>,
}

/// `POST /admin/models` — define a model (ontology type) over an existing table.
#[utoipa::path(
    post, path = "/admin/models",
    request_body = DefineModelReq,
    responses((status = 201, description = "Model (ontology type) defined")),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn define_model(State(st): State<AdminState>, Json(req): Json<DefineModelReq>) -> Response {
    let otype = ObjectType {
        name: TypeName(req.name.clone()),
        table: TableRef {
            schema: req.table.schema,
            name: req.table.name,
        },
        properties: req
            .properties
            .into_iter()
            .map(|p| PropertyDef {
                name: p.name,
                ty: p.ty,
                required: p.required,
                constraints: control_plane_core::PropertyConstraints::default(),
            })
            .collect(),
        derived: vec![],
        identity: req.identity,
    };
    match st.cp.ontology().define_type(otype).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(serde_json::json!({ "name": req.name })),
        )
            .into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// Admin routes, behind `require_auth` (401) then [`require_admin`] (403).
pub fn admin_routes(admin: AdminState, auth: AuthState) -> Router {
    let inner = Router::new()
        .route("/admin/users", post(create_user).get(list_users))
        .route("/admin/users/:username/disable", post(disable_user))
        .route("/admin/users/:username/enable", post(enable_user))
        .route("/admin/users/:username/password", post(reset_password))
        .route("/admin/models", post(define_model))
        .route("/admin/roles", post(create_role).get(list_roles))
        .route("/admin/roles/:role/grants", post(grant))
        .with_state(admin.clone())
        .route_layer(axum::middleware::from_fn_with_state(admin, require_admin));
    protect(inner, auth)
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        create_user,
        list_users,
        disable_user,
        enable_user,
        reset_password,
        create_role,
        list_roles,
        grant,
        define_model
    ),
    components(schemas(
        CreateUserReq,
        CreateUserResp,
        UserView,
        ListUsersResp,
        ResetPasswordReq,
        CreateRoleReq,
        GrantReq,
        DefineModelReq,
        TableReq,
        PropReq
    ))
)]
struct AdminApiDoc;

/// OpenAPI fragment for the `admin_routes` surface. Mergeable into a service's
/// document via `utoipa::openapi::OpenApi::merge`; the bearer security scheme
/// the ops reference is registered by the serve seam (`register_bearer_scheme`),
/// not here.
#[must_use]
pub fn admin_openapi() -> utoipa::openapi::OpenApi {
    <AdminApiDoc as utoipa::OpenApi>::openapi()
}
