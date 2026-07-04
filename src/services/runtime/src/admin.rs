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
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use control_plane_core::{
    ADMIN_ROLE, Action, ActionDef, ActionName, Auth, ControlPlane, ControlPlaneError, Effect,
    LinkDef, NewUser, ObjectType, PageReq, PolicyTarget, PropertyDef, RoleId, RunState, RunTrigger,
    SubjectId, TableRef, TransformBody, TransformDef, TransformName, TransformRun, TypeName,
    UserSummary,
};
use time::format_description::well_known::Rfc3339;

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

/// Create a user (or complete role grants for an existing one) with a starting password.
///
/// Retry-safe: a pre-existing username is not a conflict, and a retry does NOT reset the
/// stored password. An unknown role is a 400 that reports what already completed.
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

/// List all users (identity, activation, created-at; never the password verifier).
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

/// Deactivate a user and revoke their sessions.
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

/// Reactivate a user (does not resurrect their revoked sessions).
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

/// Set a new password for any user and revoke all their sessions.
///
/// Operator-driven recovery — no current-password check. Forces the user to re-log in;
/// 404 if the username is unknown.
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

/// Declare a new role (a governance target for grants).
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

/// List all declared role ids.
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

/// Grant a role coarse Read/Write access on a type.
///
/// An unknown grant-target type is a 400.
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

/// Define a model (ontology type) over an existing table.
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

/// Define an ontology link between two existing types.
///
/// The body is a `LinkDef` in its serde shape (documented on the request body below).
#[utoipa::path(
    post, path = "/admin/links",
    request_body(
        content = serde_json::Value,
        description = "A `LinkDef` in its serde shape: `{\"name\", \"from\", \"to\", \
            \"cardinality\": \"One\"|\"Many\", \"backing\": {\"ForeignKey\": {\"from_column\", \
            \"to_column\"}} | {\"JoinTable\": {\"table\": {\"schema\", \"name\"}, \"from_key\", \
            \"from_column\", \"to_column\", \"to_key\"}}}`",
    ),
    responses(
        (status = 201, description = "Link defined"),
        (status = 400, description = "Body does not decode as a LinkDef, or validation failed"),
        (status = 404, description = "Unknown endpoint type (`from` or `to`)"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn define_link_route(
    State(st): State<AdminState>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let link: LinkDef = match serde_json::from_value(body) {
        Ok(l) => l,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("invalid LinkDef: {e}")).into_response();
        }
    };
    match st.cp.ontology().define_link(link).await {
        Ok(()) => (StatusCode::CREATED, "defined").into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// Remove a link definition (idempotent; physical columns/join tables untouched).
#[utoipa::path(
    delete, path = "/admin/links/{from}/{name}",
    params(
        ("from" = String, Path, description = "The link's `from` type"),
        ("name" = String, Path, description = "Link name"),
    ),
    responses((status = 200, description = "Link definition removed (idempotent)")),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn delete_link_route(
    State(st): State<AdminState>,
    Path((from, name)): Path<(String, String)>,
) -> Response {
    match st
        .cp
        .ontology()
        .delete_link(&TypeName(from.clone()), &name)
        .await
    {
        Ok(()) => {
            Json(serde_json::json!({ "deleted": { "from": from, "name": name } })).into_response()
        }
        Err(e) => status_for(&e).into_response(),
    }
}

/// Define an ontology action.
///
/// The body is an `ActionDef` in its serde shape (documented on the request body below).
#[utoipa::path(
    post, path = "/admin/actions",
    request_body(
        content = serde_json::Value,
        description = "An `ActionDef` in its serde shape: flat single-step \
            `{\"name\", \"target\", \"kind\"?, \"parameters\"?, \"assignments\"?}` or stepped \
            `{\"name\", \"steps\": [{\"target\", \"kind\", \"parameters\", \"assignments\", \
            \"bind\"?}]}`",
    ),
    responses(
        (status = 201, description = "Action defined"),
        (status = 400, description = "Body does not decode as an ActionDef, or validation \
            failed (e.g. unknown target type)"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn define_action_route(
    State(st): State<AdminState>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let action: ActionDef = match serde_json::from_value(body) {
        Ok(a) => a,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("invalid ActionDef: {e}")).into_response();
        }
    };
    match st.cp.ontology().define_action(action).await {
        Ok(()) => (StatusCode::CREATED, "defined").into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// Remove an action definition (idempotent).
#[utoipa::path(
    delete, path = "/admin/actions/{name}",
    params(("name" = String, Path, description = "Action name")),
    responses((status = 200, description = "Action definition removed (idempotent)")),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn delete_action_route(State(st): State<AdminState>, Path(name): Path<String>) -> Response {
    match st
        .cp
        .ontology()
        .delete_action(&ActionName(name.clone()))
        .await
    {
        Ok(()) => Json(serde_json::json!({ "deleted": { "name": name } })).into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// Delete a role and everything hanging off it (idempotent).
///
/// Removes the role's memberships, grants, policies, and inheritance edges.
#[utoipa::path(
    delete, path = "/admin/roles/{role}",
    params(("role" = String, Path, description = "Role id to delete")),
    responses((status = 200, description = "Role and its grants/memberships removed (idempotent)")),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn delete_role_route(State(st): State<AdminState>, Path(role): Path<String>) -> Response {
    match st.cp.acl().delete_role(&RoleId(role.clone())).await {
        Ok(()) => Json(serde_json::json!({ "deleted": { "role": role } })).into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// One grant row as rendered on the admin read surface: wire tokens for
/// action/effect plus the `PolicyTarget` in its serde shape.
#[derive(serde::Serialize, utoipa::ToSchema)]
struct GrantView {
    action: String,
    /// The `PolicyTarget` serde shape, e.g. `{"Type": "Widget"}`.
    target: serde_json::Value,
    effect: String,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct RoleGrantsResp {
    grants: Vec<GrantView>,
}

/// List a role's coarse grants.
#[utoipa::path(
    get, path = "/admin/roles/{role}/grants",
    params(("role" = String, Path, description = "Role whose grants to list")),
    responses(
        (status = 200, description = "The role's grants", body = RoleGrantsResp),
        (status = 404, description = "Unknown role"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn list_role_grants(State(st): State<AdminState>, Path(role): Path<String>) -> Response {
    match st
        .cp
        .acl()
        .list_grants(&RoleId(role), PageReq::unbounded())
        .await
    {
        Ok(page) => {
            let grants = page
                .items
                .into_iter()
                .map(|g| GrantView {
                    action: g.action.as_str().to_string(),
                    target: serde_json::to_value(&g.target).unwrap_or_default(),
                    effect: g.effect.as_str().to_string(),
                })
                .collect();
            Json(RoleGrantsResp { grants }).into_response()
        }
        Err(e) => status_for(&e).into_response(),
    }
}

/// Revoke a coarse grant (idempotent).
///
/// The body is the same `GrantReq` the grant POST takes; revoking an absent grant is a no-op.
#[utoipa::path(
    delete, path = "/admin/roles/{role}/grants",
    params(("role" = String, Path, description = "Role whose grant to revoke")),
    request_body = GrantReq,
    responses(
        (status = 200, description = "Revoked (idempotent)"),
        (status = 400, description = "action is not read|write"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn revoke_grant(
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
    match st.cp.acl().revoke(&RoleId(role), action, &target).await {
        Ok(()) => (StatusCode::OK, "revoked").into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// List the roles assigned to a user.
#[utoipa::path(
    get, path = "/admin/users/{username}/roles",
    params(("username" = String, Path, description = "Username whose roles to list")),
    responses(
        (status = 200, description = "The user's role ids, as `{\"roles\": [...]}`"),
        (status = 404, description = "Unknown user"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn user_roles(State(st): State<AdminState>, Path(username): Path<String>) -> Response {
    match st
        .cp
        .acl()
        .roles_of(&SubjectId(username), PageReq::unbounded())
        .await
    {
        Ok(page) => Json(serde_json::json!({
            "roles": page.items.into_iter().map(|r| r.0).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// Assign a role to a user (idempotent).
#[utoipa::path(
    put, path = "/admin/users/{username}/roles/{role}",
    params(
        ("username" = String, Path, description = "Username receiving the role"),
        ("role" = String, Path, description = "Role id to assign"),
    ),
    responses(
        (status = 200, description = "Role assigned (idempotent)"),
        (status = 404, description = "Unknown user or role"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn assign_user_role(
    State(st): State<AdminState>,
    Path((username, role)): Path<(String, String)>,
) -> Response {
    match st
        .cp
        .acl()
        .assign_role(&SubjectId(username), &RoleId(role))
        .await
    {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// Unassign a role from a user (idempotent; 404 only if the user is unknown).
#[utoipa::path(
    delete, path = "/admin/users/{username}/roles/{role}",
    params(
        ("username" = String, Path, description = "Username losing the role"),
        ("role" = String, Path, description = "Role id to unassign"),
    ),
    responses(
        (status = 200, description = "Role unassigned (idempotent)"),
        (status = 404, description = "Unknown user"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn unassign_user_role(
    State(st): State<AdminState>,
    Path((username, role)): Path<(String, String)>,
) -> Response {
    let subject = SubjectId(username);
    // Existence gate: the trait's unassign_role is unconditional Ok(()).
    if let Err(e) = st.cp.acl().roles_of(&subject, PageReq::unbounded()).await {
        return status_for(&e).into_response();
    }
    match st.cp.acl().unassign_role(&subject, &RoleId(role)).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// A transform definition, echoed in its serde shape.
#[derive(serde::Serialize, utoipa::ToSchema)]
struct TransformDefView {
    name: String,
    /// The `TransformBody` serde shape (`{"kind": "physical"|"typed", ...}`).
    #[schema(value_type = Object)]
    body: serde_json::Value,
    schedule: Option<String>,
    on_input_commit: bool,
    /// Next scheduled fire (RFC3339, UTC) — present only when scheduled.
    #[serde(skip_serializing_if = "Option::is_none")]
    next_run_at: Option<String>,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct ListTransformsResp {
    transforms: Vec<TransformDefView>,
}

/// One transform run.
#[derive(serde::Serialize, utoipa::ToSchema)]
struct TransformRunView {
    run_id: String,
    /// Absent for ad-hoc runs.
    transform: Option<String>,
    trigger: String,
    state: String,
    #[schema(value_type = Object)]
    body: serde_json::Value,
    queued_at: String,
    started_at: Option<String>,
    finished_at: Option<String>,
    snapshot_id: Option<i64>,
    error: Option<String>,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct ListRunsResp {
    runs: Vec<TransformRunView>,
}

/// Acknowledgement of an accepted (asynchronous) run submission.
#[derive(serde::Serialize, utoipa::ToSchema)]
struct RunSubmittedResp {
    run_id: String,
}

fn rfc3339(t: time::OffsetDateTime) -> String {
    t.format(&Rfc3339).unwrap_or_default()
}

fn def_view(d: &TransformDef) -> TransformDefView {
    TransformDefView {
        name: d.name.0.clone(),
        body: serde_json::to_value(&d.body).unwrap_or(serde_json::Value::Null),
        schedule: d.schedule.clone(),
        on_input_commit: d.on_input_commit,
        next_run_at: None,
    }
}

fn run_view(r: &TransformRun) -> TransformRunView {
    TransformRunView {
        run_id: r.run_id.to_string(),
        transform: r.transform.as_ref().map(|t| t.0.clone()),
        trigger: r.trigger.as_str().to_string(),
        state: r.state.as_str().to_string(),
        body: serde_json::to_value(&r.body).unwrap_or(serde_json::Value::Null),
        queued_at: rfc3339(r.queued_at),
        started_at: r.started_at.map(rfc3339),
        finished_at: r.finished_at.map(rfc3339),
        snapshot_id: r.snapshot_id,
        error: r.error.clone(),
    }
}

/// Shared submit path for run-now and ad-hoc runs: builds a fresh `Queued`
/// [`TransformRun`] + its queue job and submits them atomically.
async fn submit_new_run(
    st: &AdminState,
    transform: Option<TransformName>,
    trigger: RunTrigger,
    body: TransformBody,
) -> Response {
    let run_id = uuid::Uuid::new_v4();
    let run = TransformRun {
        run_id,
        transform,
        trigger,
        state: RunState::Queued,
        body: body.clone(),
        queued_at: time::OffsetDateTime::now_utc(),
        started_at: None,
        finished_at: None,
        snapshot_id: None,
        error: None,
    };
    match st
        .cp
        .transforms()
        .submit_run(run, body.to_job(run_id))
        .await
    {
        Ok(_) => (
            StatusCode::ACCEPTED,
            Json(RunSubmittedResp {
                run_id: run_id.to_string(),
            }),
        )
            .into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// `POST /admin/transforms` — define (or redefine) a named transform.
/// `TransformDef` carries no `ToSchema`, so the body is documented as its
/// serde shape and deserialized inside the handler (the `post_action`
/// open-body pattern).
#[utoipa::path(
    post, path = "/admin/transforms",
    request_body(
        content = serde_json::Value,
        description = "A `TransformDef` in its serde shape: `{\"name\", \"body\": \
            {\"kind\": \"physical\"|\"typed\", \"inputs\", \"output\", \"sql\", \"output_mode\"?}, \
            \"schedule\"?, \"on_input_commit\"?}`. `schedule`, if present, is a live 5-field \
            UTC cron expression (e.g. `\"0 3 * * *\"`) validated at define time.",
    ),
    responses(
        (status = 201, description = "Transform defined"),
        (status = 400, description = "Body does not decode as a TransformDef, or validation \
            failed (e.g. an invalid cron `schedule` expression, on_input_commit not yet \
            supported, or a typed body referencing unknown ontology types)"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn define_transform_route(
    State(st): State<AdminState>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let def: TransformDef = match serde_json::from_value(body) {
        Ok(d) => d,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid TransformDef: {e}"),
            )
                .into_response();
        }
    };
    match st.cp.transforms().define_transform(def).await {
        Ok(()) => (StatusCode::CREATED, "defined").into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// `GET /admin/transforms` — list all transform definitions.
#[utoipa::path(
    get, path = "/admin/transforms",
    responses((status = 200, description = "All transform definitions", body = ListTransformsResp)),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn list_transforms_route(State(st): State<AdminState>) -> Response {
    match st.cp.transforms().list_transforms(PageReq::default()).await {
        Ok(page) => {
            let transforms = page.items.iter().map(def_view).collect();
            (StatusCode::OK, Json(ListTransformsResp { transforms })).into_response()
        }
        Err(e) => status_for(&e).into_response(),
    }
}

/// `GET /admin/transforms/:name` — fetch one transform definition.
#[utoipa::path(
    get, path = "/admin/transforms/{name}",
    params(("name" = String, Path, description = "Transform name")),
    responses(
        (status = 200, description = "The transform definition (with `next_run_at` when \
            scheduled)", body = TransformDefView),
        (status = 404, description = "Unknown transform"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn get_transform_route(State(st): State<AdminState>, Path(name): Path<String>) -> Response {
    let name = TransformName(name);
    let def = match st.cp.transforms().get_transform(&name).await {
        Ok(d) => d,
        Err(e) => return status_for(&e).into_response(),
    };
    let nra = match st.cp.transforms().next_run_at(&name).await {
        Ok(n) => n,
        Err(e) => return status_for(&e).into_response(),
    };
    let mut view = def_view(&def);
    view.next_run_at = nra.map(rfc3339);
    (StatusCode::OK, Json(view)).into_response()
}

/// `DELETE /admin/transforms/:name` — remove a transform definition
/// (idempotent). Runs keep their frozen body and name; history survives
/// deletion.
#[utoipa::path(
    delete, path = "/admin/transforms/{name}",
    params(("name" = String, Path, description = "Transform name")),
    responses((status = 200, description = "Transform definition removed (idempotent)")),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn delete_transform_route(
    State(st): State<AdminState>,
    Path(name): Path<String>,
) -> Response {
    match st
        .cp
        .transforms()
        .delete_transform(&TransformName(name.clone()))
        .await
    {
        Ok(()) => Json(serde_json::json!({ "deleted": name })).into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// `POST /admin/transforms/:name/run` — run a defined transform now
/// (`RunTrigger::Manual`), submitting its frozen body as a fresh run.
#[utoipa::path(
    post, path = "/admin/transforms/{name}/run",
    params(("name" = String, Path, description = "Transform name")),
    responses(
        (status = 202, description = "Run accepted and queued", body = RunSubmittedResp),
        (status = 404, description = "Unknown transform"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn run_transform_route(State(st): State<AdminState>, Path(name): Path<String>) -> Response {
    let name = TransformName(name);
    let def = match st.cp.transforms().get_transform(&name).await {
        Ok(d) => d,
        Err(e) => return status_for(&e).into_response(),
    };
    submit_new_run(&st, Some(name), RunTrigger::Manual, def.body).await
}

/// `POST /admin/transforms/run` — run an ad-hoc `TransformBody` (no saved
/// definition), submitted with `RunTrigger::AdHoc`. `TransformBody` carries
/// no `ToSchema`, so the body is documented as its serde shape and
/// deserialized inside the handler.
#[utoipa::path(
    post, path = "/admin/transforms/run",
    request_body(
        content = serde_json::Value,
        description = "A `TransformBody` in its serde shape: `{\"kind\": \"physical\"|\"typed\", \
            \"inputs\", \"output\", \"sql\", \"output_mode\"?}`",
    ),
    responses(
        (status = 202, description = "Run accepted and queued", body = RunSubmittedResp),
        (status = 400, description = "Body does not decode as a TransformBody"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn run_adhoc_route(
    State(st): State<AdminState>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let body: TransformBody = match serde_json::from_value(body) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid TransformBody: {e}"),
            )
                .into_response();
        }
    };
    submit_new_run(&st, None, RunTrigger::AdHoc, body).await
}

/// `GET /admin/transforms/:name/runs` — a transform's run history, newest
/// first. 404 for an unknown transform name (distinguishes "no runs" from
/// "no such transform").
#[utoipa::path(
    get, path = "/admin/transforms/{name}/runs",
    params(("name" = String, Path, description = "Transform name")),
    responses(
        (status = 200, description = "The transform's runs, newest first", body = ListRunsResp),
        (status = 404, description = "Unknown transform"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn list_transform_runs(State(st): State<AdminState>, Path(name): Path<String>) -> Response {
    let name = TransformName(name);
    if let Err(e) = st.cp.transforms().get_transform(&name).await {
        return status_for(&e).into_response();
    }
    match st
        .cp
        .transforms()
        .list_runs(Some(&name), PageReq::default())
        .await
    {
        Ok(page) => {
            let runs = page.items.iter().map(run_view).collect();
            (StatusCode::OK, Json(ListRunsResp { runs })).into_response()
        }
        Err(e) => status_for(&e).into_response(),
    }
}

/// `GET /admin/runs/:run_id` — fetch one run by id.
#[utoipa::path(
    get, path = "/admin/runs/{run_id}",
    params(("run_id" = String, Path, description = "Run id (UUID)")),
    responses(
        (status = 200, description = "The run", body = TransformRunView),
        (status = 400, description = "run_id is not a valid UUID"),
        (status = 404, description = "Unknown run"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn get_run_route(State(st): State<AdminState>, Path(run_id): Path<String>) -> Response {
    let rid = match uuid::Uuid::parse_str(&run_id) {
        Ok(u) => u,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("invalid run id: {e}")).into_response();
        }
    };
    match st.cp.transforms().get_run(rid).await {
        Ok(run) => (StatusCode::OK, Json(run_view(&run))).into_response(),
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
        .route(
            "/admin/roles/:role/grants",
            post(grant).get(list_role_grants).delete(revoke_grant),
        )
        .route("/admin/roles/:role", delete(delete_role_route))
        .route("/admin/links", post(define_link_route))
        .route("/admin/links/:from/:name", delete(delete_link_route))
        .route("/admin/actions", post(define_action_route))
        .route("/admin/actions/:name", delete(delete_action_route))
        .route("/admin/users/:username/roles", get(user_roles))
        .route(
            "/admin/users/:username/roles/:role",
            put(assign_user_role).delete(unassign_user_role),
        )
        .route(
            "/admin/transforms",
            post(define_transform_route).get(list_transforms_route),
        )
        .route("/admin/transforms/run", post(run_adhoc_route))
        .route(
            "/admin/transforms/:name",
            get(get_transform_route).delete(delete_transform_route),
        )
        .route("/admin/transforms/:name/run", post(run_transform_route))
        .route("/admin/transforms/:name/runs", get(list_transform_runs))
        .route("/admin/runs/:run_id", get(get_run_route))
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
        define_model,
        define_link_route,
        delete_link_route,
        define_action_route,
        delete_action_route,
        delete_role_route,
        list_role_grants,
        revoke_grant,
        user_roles,
        assign_user_role,
        unassign_user_role,
        define_transform_route,
        list_transforms_route,
        get_transform_route,
        delete_transform_route,
        run_transform_route,
        run_adhoc_route,
        list_transform_runs,
        get_run_route
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
        PropReq,
        GrantView,
        RoleGrantsResp,
        TransformDefView,
        ListTransformsResp,
        TransformRunView,
        ListRunsResp,
        RunSubmittedResp
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
