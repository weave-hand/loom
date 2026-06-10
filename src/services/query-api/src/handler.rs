//! The socket-free governed-read core. resolve type -> table + property columns;
//! load ACL policy -> row filter + denied columns; project allowed columns; compile
//! SQL with bound params; execute on the serving engine.

use control_plane_core::{
    Acl, ControlPlaneError, Ontology, PageReq, PolicyTarget, RowFilter, SubjectId, TypeName,
};

use crate::serving::{Rows, ServingEngine, SqlValue};
use crate::sql::compile_select;

const DEFAULT_LIMIT: u32 = 1000;

/// The authenticated caller (authn is a later spec; carried from a request header).
pub struct Subject(pub SubjectId);

/// A read request: an ontology type plus optional equality filters on allowed columns.
pub struct ObjectQuery {
    pub type_name: String,
    pub eq_filters: Vec<(String, SqlValue)>,
}

/// Borrowed dependencies for one read.
pub struct QueryDeps<'a> {
    pub ontology: &'a (dyn Ontology + Sync),
    pub acl: &'a (dyn Acl + Sync),
    pub serving: &'a dyn ServingEngine,
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("unknown object type: {0}")]
    UnknownType(String),
    #[error("forbidden")]
    Forbidden,
    #[error("filter column not permitted: {0}")]
    BadFilter(String),
    #[error(transparent)]
    ControlPlane(#[from] control_plane_core::ControlPlaneError),
    #[error(transparent)]
    Serving(#[from] crate::serving::ServingError),
}

pub async fn read_object(
    q: &ObjectQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<Rows, QueryError> {
    let type_name = TypeName(q.type_name.clone());

    // resolve: type -> ObjectType (table + ordered properties). A genuine miss is a
    // client 404 (UnknownType); a backend fault must propagate as itself (-> 500),
    // not masquerade as an unknown type.
    let object_type = deps
        .ontology
        .get_type(&type_name)
        .await
        .map_err(|e| match e {
            ControlPlaneError::NotFound(_) => QueryError::UnknownType(q.type_name.clone()),
            other => QueryError::ControlPlane(other),
        })?;
    let target = PolicyTarget::Type(type_name.clone());

    // policy: gather row filters + denied columns across the subject's matching policies.
    // Minimal ACL for this slice: restrictions are cumulative — row filters are ANDed
    // (via compile_select) and denied columns unioned. No deny-override / allow-widening
    // across policies; that refinement is the deferred full-ACL spec.
    let policies = deps
        .acl
        .policies_for(&subject.0, &target, PageReq::unbounded())
        .await?;
    let mut row_filters: Vec<RowFilter> = Vec::new();
    let mut denied: std::collections::HashSet<String> = std::collections::HashSet::new();
    for p in policies.items {
        if let Some(f) = p.row_filter {
            row_filters.push(f);
        }
        denied.extend(p.deny_columns);
    }

    // projection: type properties minus denied columns, preserving property order.
    let allowed: Vec<String> = object_type
        .properties
        .iter()
        .map(|p| p.name.clone())
        .filter(|n| !denied.contains(n))
        .collect();
    if allowed.is_empty() {
        return Err(QueryError::Forbidden);
    }

    // request equality filters must target an allowed (visible) column.
    for (col, _) in &q.eq_filters {
        if !allowed.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
    }

    let (sql, params) = compile_select(
        &object_type.table,
        &allowed,
        &row_filters,
        &q.eq_filters,
        DEFAULT_LIMIT,
    );
    Ok(deps.serving.fetch_rows(&sql, &params).await?)
}
