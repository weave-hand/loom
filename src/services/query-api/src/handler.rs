//! The socket-free governed-read core. resolve type -> table + property columns;
//! load ACL policy -> row filter + denied columns; project allowed columns; compile
//! SQL with bound params; execute on the serving engine.

use control_plane_core::{
    Acl, Action, ControlPlaneError, Decision, ObjectType, Ontology, PageReq, PolicyTarget,
    PropertyDef, RowFilter, SubjectId, TypeName,
};

use crate::serving::{ServingEngine, SqlValue};
use crate::sql::{compile_chain_with, compile_select_with};

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
    pub eq_filters: Vec<(String, String)>,
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
    /// The traversal chain is malformed (empty path, or depth over the cap).
    #[error("malformed traversal chain: {0}")]
    BadChain(String),
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
        .policies_for(subject, Action::Read, target, PageReq::unbounded())
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

/// The target column a derived aggregate reads, if any (COUNT reads none).
fn agg_column(a: &control_plane_core::Aggregation) -> Option<&str> {
    use control_plane_core::Aggregation::*;
    match a {
        Count => None,
        Sum(c) | Avg(c) | Min(c) | Max(c) => Some(c.as_str()),
    }
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

    // Visibility first (denied/masked column -> 400, no type info leak), then parse the
    // raw value into a typed predicate (operator + coerced operands) for the column.
    let mut predicates: Vec<crate::filter::CallerPredicate> =
        Vec::with_capacity(q.eq_filters.len());
    for (col, raw) in &q.eq_filters {
        if !allowed.contains(col) || masked.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
        let ty = object_type
            .properties
            .iter()
            .find(|p| &p.name == col)
            .map(|p| p.ty.as_str())
            .unwrap_or("");
        let p = crate::filter::coerce_predicate(col, ty, raw)
            .map_err(|_| QueryError::BadFilter(col.clone()))?;
        predicates.push(p);
    }

    // Derived properties (aggregate-over-link), governed both-ends. Resolved + appended
    // after the physical projection, in declaration order; omitted (like a denied column)
    // when the subject can't read the linked type, the link/target is missing, or the
    // aggregated column is denied on the target — never an error, just absent.
    let mut derived_names: Vec<String> = Vec::new();
    let mut derived_types: Vec<String> = Vec::new();
    let mut derived_selects: Vec<crate::sql::DerivedSelect> = Vec::new();
    if !object_type.derived.is_empty() {
        let links = deps
            .ontology
            .links(&type_name, PageReq::unbounded())
            .await?;
        for d in &object_type.derived {
            if denied.contains(&d.name) {
                continue;
            }
            if masked.contains(&d.name) {
                derived_names.push(d.name.clone());
                derived_types.push(d.ty.clone());
                derived_selects.push(crate::sql::DerivedSelect::Masked(d.name.clone()));
                continue;
            }
            let Some(link) = links.items.iter().find(|l| l.name == d.link) else {
                continue; // missing link -> omit (no define-time validation in part-1)
            };
            let target_pt = PolicyTarget::Type(link.to.clone());
            // Both-ends: the subject must be permitted to read the linked type.
            if deps.acl.check(&subject.0, Action::Read, &target_pt).await? == Decision::Deny {
                continue;
            }
            let target_type = match deps.ontology.get_type(&link.to).await {
                Ok(t) => t,
                Err(ControlPlaneError::NotFound(_)) => continue, // target type gone -> omit
                Err(other) => return Err(QueryError::ControlPlane(other)),
            };
            let (t_filters, t_denied, _t_masked) =
                load_policy(deps.acl, &subject.0, &target_pt).await?;
            // Don't leak a target column the subject may not see, via an aggregate over it.
            if let Some(col) = agg_column(&d.agg)
                && t_denied.contains(col)
            {
                continue;
            }
            derived_names.push(d.name.clone());
            derived_types.push(d.ty.clone());
            derived_selects.push(crate::sql::DerivedSelect::Aggregate(Box::new(
                crate::sql::DerivedAggregate {
                    name: d.name.clone(),
                    agg: d.agg.clone(),
                    backing: link.backing.clone(),
                    target_table: target_type.table.clone(),
                    target_filters: t_filters,
                },
            )));
        }
    }

    let (sql, params) = compile_select_with(
        deps.serving.dialect(),
        &object_type.table,
        &allowed,
        &mask_cols,
        &row_filters,
        &predicates,
        &derived_selects,
        DEFAULT_LIMIT,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    // Output columns = physical `allowed` (in order) ++ surviving derived (in order).
    let mut columns = allowed.clone();
    columns.extend(derived_names.iter().cloned());
    // Logical type per projected column, in output order — which is the SELECT order
    // compile_select emits, hence the order of `served.rows`' cells. Physical columns map
    // from the type's properties (a column with no matching property — cannot happen
    // post-projection — maps to "" -> the renderer's natural fallback); derived columns
    // carry their declared `ty`.
    let mut logical_types: Vec<String> = allowed
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
    logical_types.extend(derived_types.iter().cloned());
    // compile_select SELECTs `allowed` then the surviving derived columns, in order, so
    // the serving engine must echo those exact column names — that is the contract that
    // lets us zip `logical_types`/`columns` onto each row's cells by position. Guard it in
    // debug so any future SQL-rewrite that reorders columns is caught by the test suite
    // rather than silently mis-typing the output.
    debug_assert_eq!(
        served.columns, columns,
        "serving engine returned columns out of the projected order"
    );
    Ok(ObjectRows {
        columns,
        logical_types,
        rows: served.rows,
    })
}

/// Direction a link hop is followed. `Forward` follows the link as defined
/// (`from -> to`); `Inverse` follows it backwards (`to -> from`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Direction {
    #[default]
    Forward,
    Inverse,
}

/// One hop in a traversal path: a link name and the direction to follow it. The `From`
/// conversions yield a Forward hop, so a bare link name (`"orders".into()`) keeps every
/// existing forward call site unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hop {
    pub link: String,
    pub direction: Direction,
}

impl From<&str> for Hop {
    fn from(link: &str) -> Self {
        Hop {
            link: link.to_string(),
            direction: Direction::Forward,
        }
    }
}

impl From<String> for Hop {
    fn from(link: String) -> Self {
        Hop {
            link,
            direction: Direction::Forward,
        }
    }
}

/// A governed single-hop traversal (the `N=1` chain): from source objects, follow
/// `link`, return the linked targets. `filters` are positioned (0 = source, 1 = target).
pub struct LinkQuery {
    pub from_type: String,
    pub link: String,
    pub filters: Vec<ChainFilter>,
}

pub async fn read_linked_objects(
    q: &LinkQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    read_linked_chain(
        &ChainQuery {
            from_type: q.from_type.clone(),
            path: vec![q.link.clone().into()],
            filters: q.filters.clone(),
        },
        subject,
        deps,
    )
    .await
}

/// Maximum chain depth (number of hops). A request beyond this is rejected before
/// any catalog/ACL work — bounds the join count.
const MAX_CHAIN_DEPTH: usize = 4;

/// Per-position governance metadata for a resolved chain, aligned with the `ChainType`
/// vector passed to `compile_chain` (index 0 = source, index k = final target).
struct HopMeta {
    otype: ObjectType,
    denied: std::collections::HashSet<String>,
    masked: std::collections::HashSet<String>,
}

/// A caller equality filter addressed at a chain position. `position` 0 is the source;
/// `position` k is the final target. Built by the HTTP resolver from a `<linkname>.col`
/// (or bare = source) query key; coerced + visibility-checked against the type at that
/// position in `read_linked_chain`.
#[derive(Debug, Clone)]
pub struct ChainFilter {
    pub position: usize,
    pub column: String,
    pub raw: String,
}

/// A governed multi-hop traversal: from source objects matching the position-0 filters,
/// follow `path` (an ordered list of directed hops), return the deduped final-target
/// objects. Every type in the chain is governed (Read + row-filters) and caller-filterable.
pub struct ChainQuery {
    pub from_type: String,
    pub path: Vec<Hop>,
    pub filters: Vec<ChainFilter>,
}

pub async fn read_linked_chain(
    q: &ChainQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    if q.path.is_empty() || q.path.len() > MAX_CHAIN_DEPTH {
        return Err(QueryError::BadChain(format!(
            "path length {} (allowed 1..={MAX_CHAIN_DEPTH})",
            q.path.len()
        )));
    }

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
    let (s_filters, s_denied, s_masked) = load_policy(deps.acl, &subject.0, &from_target).await?;

    // Per-position governance metadata, aligned with `ctypes` (index 0 = source).
    let mut metas: Vec<HopMeta> = vec![HopMeta {
        otype: from_type.clone(),
        denied: s_denied,
        masked: s_masked,
    }];
    let mut ctypes: Vec<crate::sql::ChainType> = vec![crate::sql::ChainType {
        table: from_type.table.clone(),
        row_filters: s_filters,
        predicates: vec![],
    }];
    let mut hops: Vec<control_plane_core::LinkBacking> = Vec::with_capacity(q.path.len());

    let mut current_name = from_name.clone();
    for hop in &q.path {
        let links = deps
            .ontology
            .links(&current_name, PageReq::unbounded())
            .await
            .map_err(|e| match e {
                ControlPlaneError::NotFound(_) => QueryError::UnknownType(current_name.0.clone()),
                other => QueryError::ControlPlane(other),
            })?;
        let link = links
            .items
            .into_iter()
            .find(|l| l.name == hop.link)
            .ok_or_else(|| QueryError::UnknownLink(hop.link.clone()))?;
        let to_name = link.to.clone();
        let to_target = PolicyTarget::Type(to_name.clone());
        // Read on every hop type (the leak-free guarantee).
        if deps.acl.check(&subject.0, Action::Read, &to_target).await? == Decision::Deny {
            return Err(QueryError::Forbidden);
        }
        // A link pointing at a missing type is an internal inconsistency, not a 404.
        let to_type = deps.ontology.get_type(&to_name).await?;
        let (t_filters, t_denied, t_masked) = load_policy(deps.acl, &subject.0, &to_target).await?;
        hops.push(link.backing.clone());
        ctypes.push(crate::sql::ChainType {
            table: to_type.table.clone(),
            row_filters: t_filters,
            predicates: vec![],
        });
        metas.push(HopMeta {
            otype: to_type,
            denied: t_denied,
            masked: t_masked,
        });
        current_name = to_name;
    }

    // Caller filters, governed per position: visibility first (denied/masked or unknown
    // column -> 400, no type-info leak), then parse the raw value into a typed predicate
    // (operator + coerced operands) bound at the position's alias `t_i`.
    for f in &q.filters {
        if f.position >= ctypes.len() {
            return Err(QueryError::BadFilter(f.column.clone()));
        }
        let meta = &metas[f.position];
        let allowed = project_allowed(&meta.otype.properties, &meta.denied);
        if !allowed.contains(&f.column) || meta.masked.contains(&f.column) {
            return Err(QueryError::BadFilter(f.column.clone()));
        }
        let ty = meta
            .otype
            .properties
            .iter()
            .find(|p| p.name == f.column)
            .map(|p| p.ty.as_str())
            .unwrap_or("");
        let p = crate::filter::coerce_predicate(&f.column, ty, &f.raw)
            .map_err(|_| QueryError::BadFilter(f.column.clone()))?;
        ctypes[f.position].predicates.push(p);
    }

    // Final-target projection, from the last position (path is non-empty => >= 2 metas).
    let target = metas.last().expect("non-empty path yields a final target");
    let to_allowed = project_allowed(&target.otype.properties, &target.denied);
    if to_allowed.is_empty() {
        return Err(QueryError::Forbidden);
    }
    let to_mask_cols: Vec<String> = to_allowed
        .iter()
        .filter(|c| target.masked.contains(*c))
        .cloned()
        .collect();

    let (sql, params) = compile_chain_with(
        deps.serving.dialect(),
        &ctypes,
        &hops,
        &to_allowed,
        &to_mask_cols,
        DEFAULT_LIMIT,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    let logical_types: Vec<String> = to_allowed
        .iter()
        .map(|name| {
            target
                .otype
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
