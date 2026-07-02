//! The governed-read spine: the one copy of the governance prologue every query-api
//! entry point runs — coarse Read gate (deny-by-default, BEFORE existence is revealed),
//! type resolution with a 404-vs-403 knob, and the folded row/column policy — plus the
//! projection, seed-predicate, and hop-resolution helpers the read paths share.

use std::collections::HashSet;

use control_plane_core::{
    Acl, Action, ControlPlaneError, Decision, ObjectType, Ontology, PageReq, PolicyTarget,
    PropertyDef, RowFilter, SubjectId, TypeName,
};

use crate::handler::QueryError;

/// The resolved, policy-folded governance context for one `(subject, type)` pair: the
/// object type plus the subject's cumulative Read policy (row filters ANDed by the SQL
/// compiler, unioned denied + masked column sets). Produced by [`resolve_governed`] —
/// the single copy of the gate → `get_type` → policy prologue.
#[derive(Debug, Clone)]
pub struct GovernedType {
    pub otype: ObjectType,
    pub row_filters: Vec<RowFilter>,
    pub denied: HashSet<String>,
    pub masked: HashSet<String>,
}

impl GovernedType {
    /// The type's properties (in declaration order) minus denied columns — the visible
    /// physical projection.
    pub fn allowed(&self) -> Vec<String> {
        project_allowed(&self.otype.properties, &self.denied)
    }

    /// True when the declared identity column is denied or masked, so its values must
    /// not be revealed. Identity-less types are never governed here.
    pub fn identity_governed(&self) -> bool {
        identity_is_governed(&self.otype, &self.denied, &self.masked)
    }
}

/// How [`resolve_governed`] maps a granted-but-nonexistent type.
#[derive(Debug, Clone, Copy)]
pub enum OnMissing {
    /// A genuine client miss: `QueryError::UnknownType` (404). The read endpoints.
    NotFound,
    /// No-leak: `QueryError::Forbidden` (403) — the response would itself reveal
    /// existence (vector search returns identity values).
    Forbidden,
    /// An internal inconsistency, not a client fault: propagate the
    /// `ControlPlaneError::NotFound` as `QueryError::ControlPlane` (500). Used for
    /// hop-landed types — a link pointing at a missing type is corrupt ontology state.
    Internal,
}

/// The single governance prologue: coarse Read gate (deny-by-default, returned BEFORE
/// we reveal whether the type exists), type resolution (`on_missing` maps a genuine
/// miss), and the subject's folded row/column policy. Every governed read starts here
/// so the deny-before-existence-leak invariant lives in exactly one place.
pub async fn resolve_governed(
    ontology: &(dyn Ontology + Send + Sync),
    acl: &(dyn Acl + Send + Sync),
    subject: &SubjectId,
    name: &TypeName,
    on_missing: OnMissing,
) -> Result<GovernedType, QueryError> {
    let target = PolicyTarget::Type(name.clone());
    if acl.check(subject, Action::Read, &target).await? == Decision::Deny {
        return Err(QueryError::Forbidden);
    }
    let otype = match ontology.get_type(name).await {
        Ok(t) => t,
        Err(ControlPlaneError::NotFound(what)) => {
            return Err(match on_missing {
                OnMissing::NotFound => QueryError::UnknownType(name.0.clone()),
                OnMissing::Forbidden => QueryError::Forbidden,
                OnMissing::Internal => QueryError::ControlPlane(ControlPlaneError::NotFound(what)),
            });
        }
        Err(other) => return Err(QueryError::ControlPlane(other)),
    };
    let (row_filters, denied, masked) = load_policy(acl, subject, &target).await?;
    Ok(GovernedType {
        otype,
        row_filters,
        denied,
        masked,
    })
}

/// The declared logical type of `otype`'s property `name`, if any — the explicit
/// lookup replacing the per-site `properties.iter().find(…).map(…).unwrap_or("")`
/// sentinel chains.
pub fn prop_ty<'a>(otype: &'a ObjectType, name: &str) -> Option<&'a str> {
    otype
        .properties
        .iter()
        .find(|p| p.name == name)
        .map(|p| p.ty.as_str())
}

/// Load the subject's cumulative policy for `target`: row filters (ANDed by the
/// caller via the SQL compiler), unioned denied + masked columns. Minimal ACL.
pub(crate) async fn load_policy(
    acl: &(dyn Acl + Send + Sync),
    subject: &SubjectId,
    target: &PolicyTarget,
) -> Result<(Vec<RowFilter>, HashSet<String>, HashSet<String>), QueryError> {
    let policies = acl
        .policies_for(subject, Action::Read, target, PageReq::unbounded())
        .await?;
    let mut row_filters = Vec::new();
    let mut denied = HashSet::new();
    let mut masked = HashSet::new();
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
pub(crate) fn project_allowed(properties: &[PropertyDef], denied: &HashSet<String>) -> Vec<String> {
    properties
        .iter()
        .map(|p| p.name.clone())
        .filter(|n| !denied.contains(n))
        .collect()
}

/// True when the type's declared identity column is denied or masked by policy,
/// so its values must not be revealed. Identity-less types are never governed here.
pub fn identity_is_governed(
    otype: &ObjectType,
    denied: &HashSet<String>,
    masked: &HashSet<String>,
) -> bool {
    match &otype.identity {
        Some(id) => denied.contains(id) || masked.contains(id),
        None => false,
    }
}

/// Lower an object-set input (`ids`) to an `In` predicate on `otype`'s declared identity
/// column, governed like any caller filter. `None` when `ids` is empty. Errors: no
/// declared identity (`NoIdentity`); the identity column is denied or masked, so it is not
/// a permitted filter column (`BadFilter`); or a value does not coerce (`BadFilter`).
pub fn identity_in_predicate(
    otype: &ObjectType,
    denied: &HashSet<String>,
    masked: &HashSet<String>,
    ids: &[String],
) -> Result<Option<crate::filter::CallerPredicate>, QueryError> {
    if ids.is_empty() {
        return Ok(None);
    }
    let identity = otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(otype.name.0.clone()))?;
    // The identity column must be a permitted filter column: not denied, not masked.
    // Re-expressed via the shared `identity_is_governed` helper so the /search guard
    // and this filter-lowering check cannot drift. (`project_allowed` computes the same
    // deny membership via `!denied.contains`; `identity_is_governed` checks
    // `denied.contains(id) || masked.contains(id)` directly — equivalent.)
    if identity_is_governed(otype, denied, masked) {
        return Err(QueryError::BadFilter(identity));
    }
    let ty = prop_ty(otype, &identity).unwrap_or("");
    let mut values = Vec::with_capacity(ids.len());
    for raw in ids {
        values.push(crate::filter::coerce_filter(&identity, ty, raw)?);
    }
    Ok(Some(crate::filter::CallerPredicate {
        column: identity,
        op: control_plane_core::CompareOp::In,
        values,
    }))
}

/// Coerce one raw caller filter `raw` on `col` into a typed predicate, applying the same
/// visibility gate a plain filter gets: a denied (not in `allowed`) or masked column is a
/// `BadFilter` (400, no type-info leak), never a silent pass. Shared by plain `eq_filters`
/// and every `_or` member so an OR-group can never widen what a column-denial forbids.
pub(crate) fn coerce_visible_predicate(
    col: &str,
    raw: &str,
    object_type: &ObjectType,
    allowed: &[String],
    masked: &HashSet<String>,
) -> Result<crate::filter::CallerPredicate, QueryError> {
    if !allowed.iter().any(|c| c.as_str() == col) || masked.contains(col) {
        return Err(QueryError::BadFilter(col.to_string()));
    }
    let ty = prop_ty(object_type, col).unwrap_or("");
    Ok(crate::filter::coerce_predicate(col, ty, raw)?)
}
