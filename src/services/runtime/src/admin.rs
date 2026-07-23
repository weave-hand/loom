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
    ADMIN_ROLE, Action, ActionDef, ActionName, Aggregation, Auth, COMPACT_JOB_KIND, ControlPlane,
    ControlPlaneError, DerivedPropertyDef, Effect, GC_JOB_KIND, IndexSpec, JobSchedule,
    JobScheduleStatus, LengthConstraint, LinkDef, Metric, NewUser, ObjectType, PageReq, Policy,
    PolicyTarget, PropertyConstraints, PropertyDef, RangeConstraint, RoleId, RowFilter, RunState,
    RunTrigger, SubjectId, TableRef, TransformBody, TransformDef, TransformName, TransformRun,
    TypeName, UserSummary, VectorIndexDef, ViewDef,
};
use time::format_description::well_known::Rfc3339;

use crate::auth::{AuthState, Subject, error_response, protect, unauthorized};
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
        Err(e) => return error_response(&e),
    };
    if let Err(e) = st.cp.acl().define_subject(&subject).await {
        return error_response(&e);
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
            Err(e) => return error_response(&e),
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
        Err(e) => error_response(&e),
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
        Err(e) => error_response(&e),
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
        Err(e) => error_response(&e),
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
        return error_response(&e);
    }
    if let Err(e) = st.auth.revoke_subject_sessions(&subject, None).await {
        return error_response(&e);
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
        Err(e) => error_response(&e),
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
        Err(e) => error_response(&e),
    }
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct GrantReq {
    action: String,
    /// Exactly one of `type`/`table` must be set.
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    table: Option<TableReq>,
}

/// Parse the wire action token shared by grant/policy bodies.
#[expect(
    clippy::result_large_err,
    reason = "the Err path returns straight through to the handler as the HTTP response body"
)]
fn parse_action(s: &str) -> std::result::Result<Action, Response> {
    match s {
        "read" => Ok(Action::Read),
        "write" => Ok(Action::Write),
        _ => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "action must be read|write" })),
        )
            .into_response()),
    }
}

/// Resolve the exclusive `type`/`table` target pair shared by grant/policy
/// bodies. Exactly one must be set.
#[expect(
    clippy::result_large_err,
    reason = "the Err path returns straight through to the handler as the HTTP response body"
)]
fn parse_target(
    ty: Option<String>,
    table: Option<TableReq>,
) -> std::result::Result<PolicyTarget, Response> {
    match (ty, table) {
        (Some(t), None) => Ok(PolicyTarget::Type(TypeName(t))),
        (None, Some(t)) => Ok(PolicyTarget::Table(TableRef {
            schema: t.schema,
            name: t.name,
        })),
        _ => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "exactly one of type or table" })),
        )
            .into_response()),
    }
}

/// Grant a role coarse Read/Write access on a type or table.
///
/// An unknown grant-target type is a 400.
#[utoipa::path(
    post, path = "/admin/roles/{role}/grants",
    params(("role" = String, Path, description = "Role receiving the grant")),
    request_body(content = GrantReq, description = "Exactly one of `type`/`table` must be set"),
    responses(
        (status = 201, description = "Granted"),
        (status = 400, description = "action is not read|write, exactly-one-of-target violated, or unknown grant-target type"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn grant(
    State(st): State<AdminState>,
    Path(role): Path<String>,
    Json(req): Json<GrantReq>,
) -> Response {
    let action = match parse_action(&req.action) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let target = match parse_target(req.r#type, req.table) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    match st
        .cp
        .acl()
        .grant(&RoleId(role), action, target, Effect::Allow)
        .await
    {
        Ok(()) => (StatusCode::CREATED, "granted").into_response(),
        Err(e) => error_response(&e),
    }
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct RangeReq {
    #[serde(default)]
    min: Option<f64>,
    #[serde(default)]
    max: Option<f64>,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct LengthReq {
    #[serde(default)]
    min: Option<u32>,
    #[serde(default)]
    max: Option<u32>,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct ConstraintsReq {
    /// Numeric bound; valid only on numeric property types.
    #[serde(default)]
    range: Option<RangeReq>,
    /// String length bound; valid only on string property types.
    #[serde(default)]
    length: Option<LengthReq>,
    /// Regex the value must match; valid only on string property types.
    #[serde(default)]
    pattern: Option<String>,
    /// Closed value vocabulary; valid only on string property types.
    #[serde(default)]
    one_of: Option<Vec<String>>,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct AggReq {
    /// "count" | "sum" | "avg" | "min" | "max".
    kind: String,
    /// Target-type column to aggregate; required for every kind except "count".
    #[serde(default)]
    column: Option<String>,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct DerivedReq {
    name: String,
    /// Result type, e.g. "Int".
    ty: String,
    /// The link the aggregation traverses.
    link: String,
    agg: AggReq,
    /// Optional human-readable prose describing this entity. Pure annotation.
    #[serde(default)]
    description: Option<String>,
}

fn to_constraints(c: Option<ConstraintsReq>) -> PropertyConstraints {
    let Some(c) = c else {
        return PropertyConstraints::default();
    };
    PropertyConstraints {
        range: c.range.map(|r| RangeConstraint {
            min: r.min,
            max: r.max,
        }),
        length: c.length.map(|l| LengthConstraint {
            min: l.min,
            max: l.max,
        }),
        pattern: c.pattern,
        one_of: c.one_of,
    }
}

fn parse_agg(agg: AggReq) -> std::result::Result<Aggregation, String> {
    fn need(
        column: Option<String>,
        kind: &str,
        f: fn(String) -> Aggregation,
    ) -> std::result::Result<Aggregation, String> {
        column
            .map(f)
            .ok_or_else(|| format!("agg kind {kind} requires a column"))
    }
    match agg.kind.as_str() {
        "count" => match agg.column {
            None => Ok(Aggregation::Count),
            Some(_) => Err("agg kind count takes no column".to_string()),
        },
        "sum" => need(agg.column, "sum", Aggregation::Sum),
        "avg" => need(agg.column, "avg", Aggregation::Avg),
        "min" => need(agg.column, "min", Aggregation::Min),
        "max" => need(agg.column, "max", Aggregation::Max),
        other => Err(format!(
            "unknown agg kind `{other}` (want count|sum|avg|min|max)"
        )),
    }
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct PropReq {
    name: String,
    ty: String,
    #[serde(default)]
    required: bool,
    /// Per-value constraints enforced by the land and action gates (422 on violation).
    #[serde(default)]
    constraints: Option<ConstraintsReq>,
    /// Optional human-readable prose describing this entity. Pure annotation.
    #[serde(default)]
    description: Option<String>,
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
    /// Aggregate-over-link derived properties. Link *existence* is not validated
    /// here (matches `define_type`; see iss-delete-link-derived-dangle) — but when
    /// the link and its target type DO resolve, `define_type` best-effort checks a
    /// *declared* target property's type for applicability (Sum/Avg numeric, Min/Max
    /// ordered), a `Validation` error surfaced here as 400. A catalog-only column
    /// (one the target type doesn't declare as a property) is left to the ingest
    /// `bind` seam's catalog-aware check.
    #[serde(default)]
    derived: Vec<DerivedReq>,
    /// Optional human-readable prose describing this entity. Pure annotation.
    #[serde(default)]
    description: Option<String>,
}

/// Define a model (ontology type) over an existing table.
#[utoipa::path(
    post, path = "/admin/models",
    request_body = DefineModelReq,
    responses(
        (status = 201, description = "Model (ontology type) defined"),
        (status = 400, description = "invalid agg kind/column pairing, or constraint invalid \
            for the property type"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn define_model(State(st): State<AdminState>, Json(req): Json<DefineModelReq>) -> Response {
    let mut derived = Vec::with_capacity(req.derived.len());
    for d in req.derived {
        match parse_agg(d.agg) {
            Ok(agg) => derived.push(DerivedPropertyDef {
                name: d.name,
                ty: d.ty,
                link: d.link,
                agg,
                description: d.description,
            }),
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": e })),
                )
                    .into_response();
            }
        }
    }
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
                constraints: to_constraints(p.constraints),
                description: p.description,
            })
            .collect(),
        derived,
        identity: req.identity,
        version: None,
        description: req.description,
    };
    match st.cp.ontology().define_type(otype).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(serde_json::json!({ "name": req.name })),
        )
            .into_response(),
        Err(e) => error_response(&e),
    }
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct VectorIndexSpecReq {
    /// "flat" | "ivf_flat" | "hnsw".
    kind: String,
    /// ivf_flat only.
    #[serde(default)]
    nlist: Option<u32>,
    /// hnsw only.
    #[serde(default)]
    m: Option<u32>,
    /// hnsw only.
    #[serde(default)]
    ef_construction: Option<u32>,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct VectorIndexReq {
    name: String,
    /// The vector-typed property the index covers.
    property: String,
    /// "cosine" (default) | "l2".
    #[serde(default)]
    metric: Option<String>,
    /// Defaults to `{"kind": "flat"}`.
    #[serde(default)]
    spec: Option<VectorIndexSpecReq>,
    /// Optional human-readable prose describing this entity. Pure annotation.
    #[serde(default)]
    description: Option<String>,
}

/// Admin-wire spec parsing. Deliberately NOT `IndexSpec::from_label` — that
/// helper silently drops tuning fields that don't belong to the kind; the
/// admin surface rejects them so an operator's typo cannot vanish.
fn parse_index_spec(spec: Option<VectorIndexSpecReq>) -> std::result::Result<IndexSpec, String> {
    let Some(s) = spec else {
        return Ok(IndexSpec::Flat);
    };
    match s.kind.as_str() {
        "flat" => {
            if s.nlist.is_some() || s.m.is_some() || s.ef_construction.is_some() {
                return Err("flat takes no tuning fields".to_string());
            }
            Ok(IndexSpec::Flat)
        }
        "ivf_flat" => {
            if s.m.is_some() || s.ef_construction.is_some() {
                return Err("ivf_flat takes only nlist".to_string());
            }
            Ok(IndexSpec::IvfFlat { nlist: s.nlist })
        }
        "hnsw" => {
            if s.nlist.is_some() {
                return Err("hnsw takes only m and ef_construction".to_string());
            }
            Ok(IndexSpec::Hnsw {
                m: s.m,
                ef_construction: s.ef_construction,
            })
        }
        other => Err(format!(
            "unknown index kind `{other}` (want flat|ivf_flat|hnsw)"
        )),
    }
}

/// Declare (or replace, by `(type, name)`) a vector index over a vector-typed property.
#[utoipa::path(
    post, path = "/admin/models/{type}/vector-indexes",
    params(("type" = String, Path, description = "Ontology type the index belongs to")),
    request_body = VectorIndexReq,
    responses(
        (status = 201, description = "Vector index declared (upsert by type/name)"),
        (status = 400, description = "Unknown metric or index kind, tuning field on the \
            wrong kind, or the property is not vector-typed"),
        (status = 404, description = "Unknown type"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn define_vector_index_route(
    State(st): State<AdminState>,
    Path(ty): Path<String>,
    Json(req): Json<VectorIndexReq>,
) -> Response {
    let metric = match req.metric.as_deref() {
        None => Metric::default(),
        Some(s) => match s.parse::<Metric>() {
            Ok(m) => m,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "metric must be cosine|l2" })),
                )
                    .into_response();
            }
        },
    };
    let spec = match parse_index_spec(req.spec) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };
    let def = VectorIndexDef {
        name: req.name.clone(),
        type_name: TypeName(ty),
        property: req.property,
        metric,
        spec,
        description: req.description,
    };
    match st.cp.ontology().define_vector_index(def).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(serde_json::json!({ "name": req.name })),
        )
            .into_response(),
        Err(e) => error_response(&e),
    }
}

/// One vector index as rendered on the admin read surface (request vocabulary).
#[derive(serde::Serialize, utoipa::ToSchema)]
struct VectorIndexView {
    name: String,
    property: String,
    metric: String,
    /// `{"kind": ...}` plus the kind's tuning fields when set.
    spec: serde_json::Value,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct VectorIndexesResp {
    indexes: Vec<VectorIndexView>,
}

/// List a type's declared vector indexes.
#[utoipa::path(
    get, path = "/admin/models/{type}/vector-indexes",
    params(("type" = String, Path, description = "Ontology type whose indexes to list")),
    responses((status = 200, description = "The type's vector indexes", body = VectorIndexesResp)),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn list_vector_indexes_route(
    State(st): State<AdminState>,
    Path(ty): Path<String>,
) -> Response {
    match st.cp.ontology().vector_indexes_for(&TypeName(ty)).await {
        Ok(defs) => {
            let indexes = defs
                .into_iter()
                .map(|d| {
                    let (kind, nlist, m, ef) = d.spec.as_cols();
                    let mut spec = serde_json::Map::new();
                    spec.insert("kind".into(), kind.into());
                    if let Some(v) = nlist {
                        spec.insert("nlist".into(), v.into());
                    }
                    if let Some(v) = m {
                        spec.insert("m".into(), v.into());
                    }
                    if let Some(v) = ef {
                        spec.insert("ef_construction".into(), v.into());
                    }
                    VectorIndexView {
                        name: d.name,
                        property: d.property,
                        metric: d.metric.as_str().to_string(),
                        spec: serde_json::Value::Object(spec),
                    }
                })
                .collect();
            Json(VectorIndexesResp { indexes }).into_response()
        }
        Err(e) => error_response(&e),
    }
}

/// Typed OpenAPI request-body mirrors for the two `/admin/*` routes whose handlers take
/// a raw `Json<serde_json::Value>` and deserialize into `control_plane_core::LinkDef` /
/// `ActionDef` internally (issue #365 — utoipa otherwise documents an empty `{}` body).
/// These twins exist ONLY to shape the generated schema; they are never constructed (the
/// handlers still parse the raw JSON), so their fields are deliberately unread. They MUST
/// track the core types' serde shapes.
#[allow(
    dead_code,
    reason = "OpenAPI schema mirrors for #365 — fields shape the generated docs only; the handlers deserialize raw serde_json::Value, so nothing reads these"
)]
mod openapi_bodies {
    use super::TableReq;

    /// Mirror of `control_plane_core::LinkDef`.
    #[derive(serde::Deserialize, utoipa::ToSchema)]
    pub(super) struct DefineLinkReq {
        /// Link name (unique per `from` type).
        pub name: String,
        /// The link's source type.
        pub from: String,
        /// The link's destination type.
        pub to: String,
        pub cardinality: CardinalityReq,
        pub backing: LinkBackingReq,
        #[serde(default)]
        pub description: Option<String>,
    }

    /// Mirror of `Cardinality` — externally-tagged unit variants (`"One"` / `"Many"`).
    #[derive(serde::Deserialize, utoipa::ToSchema)]
    pub(super) enum CardinalityReq {
        One,
        Many,
    }

    /// Mirror of `LinkBacking` — externally-tagged struct variants.
    #[derive(serde::Deserialize, utoipa::ToSchema)]
    pub(super) enum LinkBackingReq {
        /// Direct equijoin `from.from_column = to.to_column`.
        ForeignKey {
            from_column: String,
            to_column: String,
        },
        /// Many-to-many through a mapping table.
        JoinTable {
            table: TableReq,
            from_key: String,
            from_column: String,
            to_column: String,
            to_key: String,
        },
    }

    /// Mirror of `control_plane_core::ActionDef`, which serializes through an untagged
    /// `Flat`-or-`Stepped` bridge: a single-step action uses the flat legacy shape, a
    /// multi-step action the explicit `steps` array.
    #[derive(serde::Deserialize, utoipa::ToSchema)]
    #[serde(untagged)]
    pub(super) enum DefineActionReq {
        /// Flat single-step action (the common shape).
        Flat {
            name: String,
            target: String,
            #[serde(default)]
            kind: ActionKindReq,
            #[serde(default)]
            parameters: Vec<ParamReq>,
            #[serde(default)]
            assignments: Vec<AssignmentReq>,
            #[serde(default)]
            downstream: Vec<JobTemplateReq>,
            #[serde(default)]
            description: Option<String>,
        },
        /// Multi-step action: an ordered list of single-target steps committed atomically.
        Stepped {
            name: String,
            steps: Vec<ActionStepReq>,
            #[serde(default)]
            downstream: Vec<JobTemplateReq>,
            #[serde(default)]
            description: Option<String>,
        },
    }

    /// Mirror of `ActionKind` — `#[serde(rename_all = "lowercase")]`, default `insert`.
    #[derive(serde::Deserialize, utoipa::ToSchema, Default)]
    #[serde(rename_all = "lowercase")]
    pub(super) enum ActionKindReq {
        #[default]
        Insert,
        Update,
        Delete,
    }

    /// Mirror of `ActionStep`.
    #[derive(serde::Deserialize, utoipa::ToSchema)]
    pub(super) struct ActionStepReq {
        /// The step's target type.
        pub target: String,
        pub kind: ActionKindReq,
        #[serde(default)]
        pub parameters: Vec<ParamReq>,
        #[serde(default)]
        pub assignments: Vec<AssignmentReq>,
        /// Names this step's row for cross-step `@bind.prop` references.
        #[serde(default)]
        pub bind: Option<String>,
    }

    /// Mirror of `ParamDef`.
    #[derive(serde::Deserialize, utoipa::ToSchema)]
    pub(super) struct ParamReq {
        pub name: String,
        /// The parameter's logical type (the ontology vocabulary).
        pub ty: String,
        pub required: bool,
        /// The property this parameter writes; `None` ⇒ the property named `name`.
        #[serde(default)]
        pub binds: Option<String>,
        #[serde(default)]
        pub description: Option<String>,
    }

    /// Mirror of `Assignment`.
    #[derive(serde::Deserialize, utoipa::ToSchema)]
    pub(super) struct AssignmentReq {
        pub property: String,
        pub source: AssignmentSourceReq,
    }

    /// Mirror of `AssignmentSource` — externally-tagged.
    #[derive(serde::Deserialize, utoipa::ToSchema)]
    pub(super) enum AssignmentSourceReq {
        /// A fixed constant (any JSON scalar), coerced to the property's logical type.
        Const(serde_json::Value),
        /// A bounded expression over the action's params / earlier-resolved properties.
        Expr(String),
        /// A reference to an earlier step's resolved property (`@bind.prop`).
        StepRef { bind: String, prop: String },
    }

    /// Mirror of `JobTemplate` (a downstream job enqueued atomically with the write).
    #[derive(serde::Deserialize, utoipa::ToSchema)]
    pub(super) struct JobTemplateReq {
        pub kind: String,
        #[serde(default)]
        pub payload: serde_json::Value,
    }
}

use openapi_bodies::{
    ActionKindReq, ActionStepReq, AssignmentReq, AssignmentSourceReq, CardinalityReq,
    DefineActionReq, DefineLinkReq, JobTemplateReq, LinkBackingReq, ParamReq,
};

/// Define an ontology link between two existing types.
///
/// The body is a `LinkDef` in its serde shape (mirrored by [`DefineLinkReq`] for the docs).
#[utoipa::path(
    post, path = "/admin/links",
    request_body = DefineLinkReq,
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
        Err(e) => error_response(&e),
    }
}

/// Remove a link definition (idempotent; physical columns/join tables untouched).
#[utoipa::path(
    delete, path = "/admin/links/{from}/{name}",
    params(
        ("from" = String, Path, description = "The link's `from` type"),
        ("name" = String, Path, description = "Link name"),
    ),
    responses(
        (status = 200, description = "Link definition removed (idempotent)"),
        (status = 409, description = "Link is referenced by a derived property; delete blocked"),
    ),
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
        Err(control_plane_core::ControlPlaneError::Conflict(msg)) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": msg })),
        )
            .into_response(),
        Err(e) => error_response(&e),
    }
}

/// Define an ontology action.
///
/// The body is an `ActionDef` in its serde shape (mirrored by [`DefineActionReq`] for the
/// docs): a flat single-step action, or a stepped action with an explicit `steps` array.
#[utoipa::path(
    post, path = "/admin/actions",
    request_body = DefineActionReq,
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
        Err(e) => error_response(&e),
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
        Err(e) => error_response(&e),
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
        Err(e) => error_response(&e),
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
        Err(e) => error_response(&e),
    }
}

/// Revoke a coarse grant (idempotent).
///
/// The body is the same `GrantReq` the grant POST takes; revoking an absent grant is a no-op.
#[utoipa::path(
    delete, path = "/admin/roles/{role}/grants",
    params(("role" = String, Path, description = "Role whose grant to revoke")),
    request_body(content = GrantReq, description = "Exactly one of `type`/`table` must be set"),
    responses(
        (status = 200, description = "Revoked (idempotent)"),
        (status = 400, description = "action is not read|write, or exactly-one-of-target violated"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn revoke_grant(
    State(st): State<AdminState>,
    Path(role): Path<String>,
    Json(req): Json<GrantReq>,
) -> Response {
    let action = match parse_action(&req.action) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let target = match parse_target(req.r#type, req.table) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    match st.cp.acl().revoke(&RoleId(role), action, &target).await {
        Ok(()) => (StatusCode::OK, "revoked").into_response(),
        Err(e) => error_response(&e),
    }
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct PolicyReq {
    action: String,
    /// Exactly one of `type`/`table` must be set.
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    table: Option<TableReq>,
    /// A `RowFilter` in its serde shape, e.g.
    /// `{"Compare":{"property":"region","op":"Eq","value":{"Text":"emea"}}}`,
    /// `{"And":[...]}`, `{"Not":{...}}`. Absent/null means no row filter.
    #[serde(default)]
    row_filter: Option<serde_json::Value>,
    #[serde(default)]
    deny_columns: Vec<String>,
    #[serde(default)]
    mask_columns: Vec<String>,
}

/// Create or replace the fine-grained policy for `(role, action, target)`.
#[utoipa::path(
    post, path = "/admin/roles/{role}/policies",
    params(("role" = String, Path, description = "Role the policy binds")),
    request_body = PolicyReq,
    responses(
        (status = 201, description = "Policy set (upsert by role/action/target)"),
        (status = 400, description = "action is not read|write, exactly-one-of-target violated, \
            row_filter does not decode as a RowFilter, or validation failed (unknown type, \
            unknown row-filter property, caller-predicate-only operator)"),
        (status = 404, description = "Unknown role"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn set_policy_route(
    State(st): State<AdminState>,
    Path(role): Path<String>,
    Json(req): Json<PolicyReq>,
) -> Response {
    let action = match parse_action(&req.action) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let target = match parse_target(req.r#type, req.table) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let row_filter: Option<RowFilter> = match req.row_filter {
        Some(v) => match serde_json::from_value(v) {
            Ok(f) => Some(f),
            Err(e) => {
                return (StatusCode::BAD_REQUEST, format!("invalid RowFilter: {e}"))
                    .into_response();
            }
        },
        None => None,
    };
    let policy = Policy {
        target,
        row_filter,
        deny_columns: req.deny_columns,
        mask_columns: req.mask_columns,
    };
    match st.cp.acl().set_policy(&RoleId(role), action, policy).await {
        Ok(()) => (StatusCode::CREATED, "defined").into_response(),
        Err(e) => error_response(&e),
    }
}

/// One policy row as rendered on the admin read surface.
#[derive(serde::Serialize, utoipa::ToSchema)]
struct PolicyView {
    action: String,
    /// The `PolicyTarget` serde shape, e.g. `{"Type": "Widget"}`.
    target: serde_json::Value,
    /// The stored `RowFilter` serde shape, or `null`.
    row_filter: serde_json::Value,
    deny_columns: Vec<String>,
    mask_columns: Vec<String>,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct RolePoliciesResp {
    policies: Vec<PolicyView>,
}

/// List a role's fine-grained policies.
#[utoipa::path(
    get, path = "/admin/roles/{role}/policies",
    params(("role" = String, Path, description = "Role whose policies to list")),
    responses(
        (status = 200, description = "The role's policies", body = RolePoliciesResp),
        (status = 404, description = "Unknown role"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn list_role_policies(State(st): State<AdminState>, Path(role): Path<String>) -> Response {
    match st
        .cp
        .acl()
        .list_policies(&RoleId(role), PageReq::unbounded())
        .await
    {
        Ok(page) => {
            let policies = page
                .items
                .into_iter()
                .map(|rp| PolicyView {
                    action: rp.action.as_str().to_string(),
                    target: serde_json::to_value(&rp.policy.target).unwrap_or_default(),
                    row_filter: rp
                        .policy
                        .row_filter
                        .as_ref()
                        .map(|f| serde_json::to_value(f).unwrap_or_default())
                        .unwrap_or(serde_json::Value::Null),
                    deny_columns: rp.policy.deny_columns,
                    mask_columns: rp.policy.mask_columns,
                })
                .collect();
            Json(RolePoliciesResp { policies }).into_response()
        }
        Err(e) => error_response(&e),
    }
}

/// Remove the policy for `(role, action, target)` (idempotent).
///
/// The body is a `PolicyReq`; only `action` and the target pair are read.
#[utoipa::path(
    delete, path = "/admin/roles/{role}/policies",
    params(("role" = String, Path, description = "Role whose policy to clear")),
    request_body = PolicyReq,
    responses(
        (status = 200, description = "Cleared (idempotent)"),
        (status = 400, description = "action is not read|write, or exactly-one-of-target violated"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn clear_policy_route(
    State(st): State<AdminState>,
    Path(role): Path<String>,
    Json(req): Json<PolicyReq>,
) -> Response {
    let action = match parse_action(&req.action) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let target = match parse_target(req.r#type, req.table) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    match st
        .cp
        .acl()
        .clear_policy(&RoleId(role), action, &target)
        .await
    {
        Ok(()) => (StatusCode::OK, "cleared").into_response(),
        Err(e) => error_response(&e),
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
        Err(e) => error_response(&e),
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
        Err(e) => error_response(&e),
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
        return error_response(&e);
    }
    match st.cp.acl().unassign_role(&subject, &RoleId(role)).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => error_response(&e),
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
        Err(e) => error_response(&e),
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
            failed (e.g. an invalid cron `schedule` expression, an `on_input_commit` def that \
            would close a data-trigger cycle, or a typed body referencing unknown ontology \
            types)"),
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
    // Capture the physical output table (if any) before the def is moved into define.
    let grant_table = def.body.physical_output_grant_table().cloned();
    if let Err(e) = st.cp.transforms().define_transform(def).await {
        return error_response(&e);
    }
    // A physical output is a fresh untyped table with no grant; grant the reserved
    // admin role Read so its catalog metadata + lineage node are visible regardless
    // of the defining client. Idempotent (no-op upsert on redefine). A grant failure
    // surfaces (the define is committed and idempotent, so a re-POST recovers — the
    // known cross-concern-atomicity gap, fut-auth-acl-provisioning-tx).
    if let Some(output) = grant_table
        && let Err(e) = st
            .cp
            .acl()
            .grant(
                &RoleId(ADMIN_ROLE.to_string()),
                Action::Read,
                PolicyTarget::Table(output),
                Effect::Allow,
            )
            .await
    {
        return error_response(&e);
    }
    (StatusCode::CREATED, "defined").into_response()
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
        Err(e) => error_response(&e),
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
        Err(e) => return error_response(&e),
    };
    let nra = match st.cp.transforms().next_run_at(&name).await {
        Ok(n) => n,
        Err(e) => return error_response(&e),
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
        Err(e) => error_response(&e),
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
        Err(e) => return error_response(&e),
    };
    submit_new_run(&st, Some(name), RunTrigger::Manual, def.body).await
}

/// `POST /admin/transforms/run` — run an ad-hoc `TransformBody` (no saved
/// definition), submitted with `RunTrigger::AdHoc`. `TransformBody` carries
/// no `ToSchema`, so the body is documented as its serde shape and
/// deserialized inside the handler. `microbatch`/`microbatch_join` bodies are
/// rejected with 400: a micro-batch MV's watermark key is named by a live
/// `define_transform` def, so an ad-hoc run would create a defless watermark.
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
    if matches!(
        body,
        TransformBody::MicroBatch { .. } | TransformBody::MicroBatchJoin { .. }
    ) {
        return (
            StatusCode::BAD_REQUEST,
            "ad-hoc micro-batch runs are not supported: a micro-batch MV must be registered with \
             define_transform (its watermark key must be named by a live def)"
                .to_string(),
        )
            .into_response();
    }
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
        return error_response(&e);
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
        Err(e) => error_response(&e),
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
        Err(e) => error_response(&e),
    }
}

/// A `gc_table`/`compact_table` job payload's table reference, the shape
/// shared by [`control_plane_core::GcJob`] and [`control_plane_core::CompactJob`]
/// — used only to pull `(schema, name)` out of the wire payload for the
/// define-time catalog-existence check, not to re-validate the payload (that
/// is [`control_plane_core::validate_job_schedule`]'s job, reached via
/// `define_job_schedule`).
#[derive(serde::Deserialize)]
struct ScheduleTablePayload {
    schema: String,
    name: String,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct JobScheduleReq {
    name: String,
    /// `"gc_table"` | `"compact_table"` (the only schedulable job kinds).
    kind: String,
    /// The kind's typed job body, e.g. `{"schema": "main", "name": "orders"}`.
    payload: serde_json::Value,
    /// A 5-field UTC cron expression, e.g. `"0 3 * * *"`.
    cron: String,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct JobScheduleView {
    name: String,
    kind: String,
    payload: serde_json::Value,
    cron: String,
    /// RFC3339, UTC.
    next_run_at: String,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct ListSchedulesResp {
    schedules: Vec<JobScheduleView>,
}

fn schedule_view(s: &JobScheduleStatus) -> JobScheduleView {
    JobScheduleView {
        name: s.schedule.name.clone(),
        kind: s.schedule.kind.clone(),
        payload: s.schedule.payload.clone(),
        cron: s.schedule.cron.clone(),
        next_run_at: rfc3339(s.next_run_at),
    }
}

/// Define-time catalog-existence check for the two schedulable job kinds: the
/// table named by the payload's `{schema, name}` must already be live in the
/// catalog, or a long-lived schedule for a typo'd table would fail forever.
/// `Some(response)` short-circuits the caller with a 400 naming the missing
/// table; `None` means either the check passed or the kind/payload isn't this
/// check's business (an unschedulable kind, or a payload that doesn't decode —
/// both are left to `define_job_schedule`'s own `validate_job_schedule`).
async fn schedule_table_check(
    st: &AdminState,
    kind: &str,
    payload: &serde_json::Value,
) -> Option<Response> {
    if kind != GC_JOB_KIND && kind != COMPACT_JOB_KIND {
        return None;
    }
    let Ok(t) = serde_json::from_value::<ScheduleTablePayload>(payload.clone()) else {
        return None;
    };
    let table = TableRef {
        schema: t.schema,
        name: t.name,
    };
    match st.cp.catalog().current_snapshot(&table).await {
        Ok(_) => None,
        Err(ControlPlaneError::NotFound(_)) => Some(
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("unknown table: {}.{}", table.schema, table.name)
                })),
            )
                .into_response(),
        ),
        Err(e) => Some(error_response(&e)),
    }
}

/// Define (or redefine) a named cron schedule for a recurring maintenance job.
///
/// For `gc_table`/`compact_table` (the only schedulable kinds) the payload's
/// `{schema, name}` table must already exist in the catalog at define time —
/// stricter than the manual `/tables/{schema}/{table}/compact`-style enqueue
/// endpoints, since a long-lived schedule for a typo'd table would fail
/// forever. A redefine (same `name`) resets `next_run_at` from now.
#[utoipa::path(
    post, path = "/admin/schedules",
    request_body = JobScheduleReq,
    responses(
        (status = 201, description = "Schedule defined (redefine resets next_run_at)"),
        (status = 400, description = "Invalid cron expression, an unschedulable kind, a \
            payload that fails to decode as the kind's job body, or (gc_table/compact_table \
            only) the payload names a table that does not exist in the catalog"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn define_schedule_route(
    State(st): State<AdminState>,
    Json(req): Json<JobScheduleReq>,
) -> Response {
    if let Some(resp) = schedule_table_check(&st, &req.kind, &req.payload).await {
        return resp;
    }
    let schedule = JobSchedule {
        name: req.name.clone(),
        kind: req.kind,
        payload: req.payload,
        cron: req.cron,
    };
    match st.cp.queue().define_job_schedule(schedule).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(serde_json::json!({ "name": req.name })),
        )
            .into_response(),
        Err(e) => error_response(&e),
    }
}

/// List all defined job schedules with their derived `next_run_at`.
#[utoipa::path(
    get, path = "/admin/schedules",
    responses((status = 200, description = "All defined schedules", body = ListSchedulesResp)),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn list_schedules_route(State(st): State<AdminState>) -> Response {
    match st.cp.queue().list_job_schedules().await {
        Ok(list) => {
            let schedules = list.iter().map(schedule_view).collect();
            (StatusCode::OK, Json(ListSchedulesResp { schedules })).into_response()
        }
        Err(e) => error_response(&e),
    }
}

/// Remove a named job schedule.
#[utoipa::path(
    delete, path = "/admin/schedules/{name}",
    params(("name" = String, Path, description = "Schedule name")),
    responses(
        (status = 204, description = "Schedule deleted"),
        (status = 404, description = "Unknown schedule name"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn delete_schedule_route(State(st): State<AdminState>, Path(name): Path<String>) -> Response {
    match st.cp.queue().delete_job_schedule(&name).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => error_response(&e),
    }
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct DefineViewReq {
    view: TableReq,
    base: TableReq,
    /// A `RowFilter` in its serde shape, e.g.
    /// `{"Compare":{"property":"region","op":"Eq","value":{"Text":"EU"}}}`,
    /// `{"And":[...]}`, `{"Not":{...}}`. Absent/null means no row filter (all
    /// base rows are in the view).
    #[serde(default)]
    predicate: Option<serde_json::Value>,
    /// Column subset (base-schema-order significant). Absent/null means all
    /// base columns are projected.
    #[serde(default)]
    columns: Option<Vec<String>>,
}

/// `POST /admin/views` — define (create-only) a virtual dataset: a named
/// row/column subset of one physical base table. Mirrors the
/// `define_transform_route` open-body-decode-then-call pattern; unlike
/// transforms, the request shape is fully typed (`RowFilter` alone lacks
/// `ToSchema`, so its field stays `serde_json::Value` and is decoded inside
/// the handler, matching `PolicyReq::row_filter`).
#[utoipa::path(
    post, path = "/admin/views",
    request_body = DefineViewReq,
    responses(
        (status = 201, description = "View defined"),
        (status = 400, description = "predicate does not decode as a RowFilter, or \
            validate_view_shape fails (predicate/projection names an unknown base column, \
            empty/duplicate projection, or view naming itself as base)"),
        (status = 404, description = "base table does not exist"),
        (status = 409, description = "view name already exists, or collides with a physical \
            table"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn define_view_route(
    State(st): State<AdminState>,
    Json(req): Json<DefineViewReq>,
) -> Response {
    let predicate: Option<RowFilter> = match req.predicate {
        Some(v) => match serde_json::from_value(v) {
            Ok(f) => Some(f),
            Err(e) => {
                return (StatusCode::BAD_REQUEST, format!("invalid RowFilter: {e}"))
                    .into_response();
            }
        },
        None => None,
    };
    let view = ViewDef {
        view: TableRef {
            schema: req.view.schema,
            name: req.view.name,
        },
        base: TableRef {
            schema: req.base.schema,
            name: req.base.name,
        },
        predicate,
        columns: req.columns,
    };
    match st.cp.catalog().define_view(view).await {
        Ok(()) => (StatusCode::CREATED, "defined").into_response(),
        Err(e) => error_response(&e),
    }
}

/// `DELETE /admin/views/:schema/:name` — drop a view by its ref.
#[utoipa::path(
    delete, path = "/admin/views/{schema}/{name}",
    params(
        ("schema" = String, Path, description = "View schema"),
        ("name" = String, Path, description = "View name"),
    ),
    responses(
        (status = 200, description = "View dropped"),
        (status = 404, description = "Unknown view"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn drop_view_route(
    State(st): State<AdminState>,
    Path((schema, name)): Path<(String, String)>,
) -> Response {
    let view = TableRef { schema, name };
    match st.cp.catalog().drop_view(&view).await {
        Ok(()) => Json(serde_json::json!({
            "dropped": format!("{}.{}", view.schema, view.name)
        }))
        .into_response(),
        Err(e) => error_response(&e),
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
        .route(
            "/admin/models/:type/vector-indexes",
            post(define_vector_index_route).get(list_vector_indexes_route),
        )
        .route("/admin/roles", post(create_role).get(list_roles))
        .route(
            "/admin/roles/:role/grants",
            post(grant).get(list_role_grants).delete(revoke_grant),
        )
        .route(
            "/admin/roles/:role/policies",
            post(set_policy_route)
                .get(list_role_policies)
                .delete(clear_policy_route),
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
        .route(
            "/admin/schedules",
            post(define_schedule_route).get(list_schedules_route),
        )
        .route("/admin/schedules/:name", delete(delete_schedule_route))
        .route("/admin/views", post(define_view_route))
        .route("/admin/views/:schema/:name", delete(drop_view_route))
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
        define_vector_index_route,
        list_vector_indexes_route,
        define_link_route,
        delete_link_route,
        define_action_route,
        delete_action_route,
        delete_role_route,
        list_role_grants,
        revoke_grant,
        set_policy_route,
        list_role_policies,
        clear_policy_route,
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
        get_run_route,
        define_schedule_route,
        list_schedules_route,
        delete_schedule_route,
        define_view_route,
        drop_view_route
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
        ConstraintsReq,
        RangeReq,
        LengthReq,
        AggReq,
        DerivedReq,
        VectorIndexReq,
        VectorIndexSpecReq,
        VectorIndexView,
        VectorIndexesResp,
        GrantView,
        RoleGrantsResp,
        PolicyReq,
        PolicyView,
        RolePoliciesResp,
        TransformDefView,
        ListTransformsResp,
        TransformRunView,
        ListRunsResp,
        RunSubmittedResp,
        JobScheduleReq,
        JobScheduleView,
        ListSchedulesResp,
        DefineViewReq,
        DefineLinkReq,
        CardinalityReq,
        LinkBackingReq,
        DefineActionReq,
        ActionKindReq,
        ActionStepReq,
        ParamReq,
        AssignmentReq,
        AssignmentSourceReq,
        JobTemplateReq
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
