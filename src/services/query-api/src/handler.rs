//! The socket-free governed-read core. resolve type -> table + property columns;
//! load ACL policy -> row filter + denied columns; project allowed columns; compile
//! SQL with bound params; execute on the serving engine.

use control_plane_core::{
    Acl, Action, ControlPlaneError, Decision, Ontology, PageReq, PolicyTarget, PropertyDef,
    RowFilter, SubjectId, TypeName,
};

use crate::serving::{ServingEngine, SqlValue};
use crate::sql::{compile_select, compile_traversal};

/// A governed read result: rows plus, for each projected column, the ontology
/// property's logical type — the input the wire renderer needs to type each value.
/// `columns`, `logical_types`, and every row's cells are positionally aligned.
#[derive(Debug)]
pub struct ObjectRows {
    pub columns: Vec<String>,
    pub logical_types: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
}

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
    pub ontology: &'a (dyn Ontology + Send + Sync),
    pub acl: &'a (dyn Acl + Send + Sync),
    pub serving: &'a dyn ServingEngine,
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("unknown object type: {0}")]
    UnknownType(String),
    #[error("unknown link: {0}")]
    UnknownLink(String),
    #[error("forbidden")]
    Forbidden,
    #[error("filter column not permitted: {0}")]
    BadFilter(String),
    #[error(transparent)]
    ControlPlane(#[from] control_plane_core::ControlPlaneError),
    #[error(transparent)]
    Serving(#[from] crate::serving::ServingError),
    #[error(transparent)]
    Malformed(#[from] crate::sql::CompileError),
}

/// Load the subject's cumulative policy for `target`: row filters (ANDed by the
/// caller via the SQL compiler), unioned denied + masked columns. Minimal ACL.
async fn load_policy(
    acl: &(dyn Acl + Send + Sync),
    subject: &SubjectId,
    target: &PolicyTarget,
) -> Result<
    (
        Vec<RowFilter>,
        std::collections::HashSet<String>,
        std::collections::HashSet<String>,
    ),
    QueryError,
> {
    let policies = acl
        .policies_for(subject, target, PageReq::unbounded())
        .await?;
    let mut row_filters = Vec::new();
    let mut denied = std::collections::HashSet::new();
    let mut masked = std::collections::HashSet::new();
    for p in policies.items {
        if let Some(f) = p.row_filter {
            row_filters.push(f);
        }
        denied.extend(p.deny_columns);
        masked.extend(p.mask_columns);
    }
    Ok((row_filters, denied, masked))
}

/// An object type's properties (in order) minus denied columns.
fn project_allowed(
    properties: &[PropertyDef],
    denied: &std::collections::HashSet<String>,
) -> Vec<String> {
    properties
        .iter()
        .map(|p| p.name.clone())
        .filter(|n| !denied.contains(n))
        .collect()
}

pub async fn read_object(
    q: &ObjectQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let target = PolicyTarget::Type(type_name.clone());

    // Coarse gate, deny-by-default: the subject must hold a Read grant on this type.
    // No grant — including an unknown/anonymous subject — is Forbidden, returned BEFORE
    // we reveal whether the type exists. Fine-grained row/column policy below only
    // narrows what an already-permitted subject sees.
    if deps.acl.check(&subject.0, Action::Read, &target).await? == Decision::Deny {
        return Err(QueryError::Forbidden);
    }

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

    let (row_filters, denied, masked) = load_policy(deps.acl, &subject.0, &target).await?;

    // projection: type properties minus denied columns, preserving property order.
    let allowed: Vec<String> = project_allowed(&object_type.properties, &denied);
    if allowed.is_empty() {
        return Err(QueryError::Forbidden);
    }
    // masked columns to actually apply: those still visible (deny wins over mask).
    let mask_cols: Vec<String> = allowed
        .iter()
        .filter(|c| masked.contains(*c))
        .cloned()
        .collect();

    // request equality filters must target a visible, non-masked column.
    for (col, _) in &q.eq_filters {
        if !allowed.contains(col) || masked.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
    }

    let (sql, params) = compile_select(
        &object_type.table,
        &allowed,
        &mask_cols,
        &row_filters,
        &q.eq_filters,
        DEFAULT_LIMIT,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    // Logical type per projected column, in `allowed` order — which is the SELECT
    // order compile_select emits, hence the order of `served.rows`' cells. A column
    // with no matching property (cannot happen post-projection) maps to "" -> the
    // renderer's natural fallback.
    let logical_types: Vec<String> = allowed
        .iter()
        .map(|name| {
            object_type
                .properties
                .iter()
                .find(|p| &p.name == name)
                .map(|p| p.ty.clone())
                .unwrap_or_default()
        })
        .collect();
    // compile_select SELECTs `allowed` verbatim, in order, so the serving engine must
    // echo those exact column names — that is the contract that lets us zip
    // `logical_types`/`columns` onto each row's cells by position. Guard it in debug so
    // any future SQL-rewrite that reorders columns is caught by the test suite rather
    // than silently mis-typing the output.
    debug_assert_eq!(
        served.columns, allowed,
        "serving engine returned columns out of the projected order"
    );
    Ok(ObjectRows {
        columns: allowed,
        logical_types,
        rows: served.rows,
    })
}

/// A governed traversal: from source objects matching `source_filters`, follow
/// `link`, return the linked target objects.
pub struct LinkQuery {
    pub from_type: String,
    pub link: String,
    pub source_filters: Vec<(String, SqlValue)>,
}

pub async fn read_linked_objects(
    q: &LinkQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let from_name = TypeName(q.from_type.clone());
    let from_target = PolicyTarget::Type(from_name.clone());

    // Read on the source (deny-by-default, before existence is revealed).
    if deps
        .acl
        .check(&subject.0, Action::Read, &from_target)
        .await?
        == Decision::Deny
    {
        return Err(QueryError::Forbidden);
    }
    let from_type = deps
        .ontology
        .get_type(&from_name)
        .await
        .map_err(|e| match e {
            ControlPlaneError::NotFound(_) => QueryError::UnknownType(q.from_type.clone()),
            other => QueryError::ControlPlane(other),
        })?;

    // Resolve the link by name among the source's links.
    let links = deps
        .ontology
        .links(&from_name, PageReq::unbounded())
        .await
        .map_err(|e| match e {
            ControlPlaneError::NotFound(_) => QueryError::UnknownType(q.from_type.clone()),
            other => QueryError::ControlPlane(other),
        })?;
    let link = links
        .items
        .into_iter()
        .find(|l| l.name == q.link)
        .ok_or_else(|| QueryError::UnknownLink(q.link.clone()))?;
    let to_name = link.to.clone();
    let to_target = PolicyTarget::Type(to_name.clone());

    // Read on the target.
    if deps.acl.check(&subject.0, Action::Read, &to_target).await? == Decision::Deny {
        return Err(QueryError::Forbidden);
    }
    // A link pointing at a missing type is an internal inconsistency, not a client 404.
    let to_type = deps.ontology.get_type(&to_name).await?;

    let (from_row_filters, from_denied, from_masked) =
        load_policy(deps.acl, &subject.0, &from_target).await?;
    let (to_row_filters, to_denied, to_masked) =
        load_policy(deps.acl, &subject.0, &to_target).await?;

    // Source filter columns must be visible (allowed, non-masked) on the source.
    let from_allowed = project_allowed(&from_type.properties, &from_denied);
    for (col, _) in &q.source_filters {
        if !from_allowed.contains(col) || from_masked.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
    }

    // Target projection.
    let to_allowed = project_allowed(&to_type.properties, &to_denied);
    if to_allowed.is_empty() {
        return Err(QueryError::Forbidden);
    }
    let to_mask_cols: Vec<String> = to_allowed
        .iter()
        .filter(|c| to_masked.contains(*c))
        .cloned()
        .collect();

    let (sql, params) = compile_traversal(
        &from_type.table,
        &to_type.table,
        &link.backing,
        &to_allowed,
        &to_mask_cols,
        &from_row_filters,
        &to_row_filters,
        &q.source_filters,
        DEFAULT_LIMIT,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    let logical_types: Vec<String> = to_allowed
        .iter()
        .map(|name| {
            to_type
                .properties
                .iter()
                .find(|p| &p.name == name)
                .map(|p| p.ty.clone())
                .unwrap_or_default()
        })
        .collect();
    debug_assert_eq!(
        served.columns, to_allowed,
        "serving engine returned columns out of the projected order"
    );
    Ok(ObjectRows {
        columns: to_allowed,
        logical_types,
        rows: served.rows,
    })
}
