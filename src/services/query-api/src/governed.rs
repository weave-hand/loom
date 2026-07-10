//! The governed-read spine: the one copy of the governance prologue every query-api
//! entry point runs — coarse Read gate (deny-by-default, BEFORE existence is revealed),
//! type resolution with a 404-vs-403 knob, and the folded row/column policy — plus the
//! projection, seed-predicate, and hop-resolution helpers the read paths share.

use std::collections::HashSet;

use control_plane_core::{
    Acl, Action, ControlPlaneError, Decision, GovernedCatalog, GovernedTable, ObjectType, Ontology,
    PageReq, PolicyTarget, PropertyDef, RowFilter, SubjectId, TypeName,
};

use crate::handler::{Direction, Hop, QueryError};

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

/// Resolve the subject's per-request governed catalog: one `GovernedTable` per
/// ontology type the subject holds a coarse Read grant on (deny-by-default — an
/// ungranted type is OMITTED, and the engine's closed-world registration makes an
/// omitted table unresolvable). Fail-closed: any ACL/ontology error aborts the
/// whole resolution — never an empty-policy fallback entry. If two allowed types
/// bind the same table the first (by list order) wins — each type's policy is
/// independently reachable over the HTTP read path already, so this leaks nothing
/// beyond existing capability; a warn fires for audit.
pub async fn resolve_governed_catalog(
    ontology: &(dyn Ontology + Send + Sync),
    acl: &(dyn Acl + Send + Sync),
    subject: &SubjectId,
) -> Result<GovernedCatalog, QueryError> {
    let types = ontology.list_types(PageReq::unbounded()).await?;
    let mut tables: Vec<GovernedTable> = Vec::new();
    for ty in types.items {
        let target = PolicyTarget::Type(ty.name.clone());
        if acl.check(subject, Action::Read, &target).await? == Decision::Deny {
            continue;
        }
        if tables.iter().any(|gt| gt.table == ty.table) {
            tracing::warn!(
                table = ?ty.table,
                ty = %ty.name.0,
                "duplicate table binding in governed catalog; first entry wins"
            );
            continue;
        }
        let (row_filters, denied, masked) = load_policy(acl, subject, &target).await?;
        tables.push(GovernedTable {
            table: ty.table,
            row_filters,
            denied: denied.into_iter().collect(),
            masked: masked.into_iter().collect(),
        });
    }
    Ok(GovernedCatalog { tables })
}

/// The governed output-column set of one read, in SELECT order: the visible physical
/// columns (type properties minus denied, in property order), extended with any derived
/// columns via [`Projection::push`]. Owns the positional logical-type zip, the masked
/// subset (columns SELECTed as the mask marker, so streamed back as Utf8 — load-bearing
/// for the Flight export schema), and the served-rows conversion with its column-order
/// contract check.
#[derive(Debug)]
pub struct Projection {
    pub columns: Vec<String>,
    pub logical_types: Vec<String>,
    pub masked: Vec<String>,
}

impl Projection {
    /// The visible physical projection of `g`. Fail-closed: a subject with no visible
    /// columns gets `Forbidden`, never an empty SELECT.
    pub fn visible(g: &GovernedType) -> Result<Self, QueryError> {
        let columns = project_allowed(&g.otype.properties, &g.denied);
        if columns.is_empty() {
            return Err(QueryError::Forbidden);
        }
        let logical_types = columns
            .iter()
            .map(|name| {
                prop_ty(&g.otype, name)
                    .map(str::to_string)
                    .unwrap_or_default()
            })
            .collect();
        let masked = columns
            .iter()
            .filter(|c| g.masked.contains(*c))
            .cloned()
            .collect();
        Ok(Self {
            columns,
            logical_types,
            masked,
        })
    }

    /// The ungoverned projection of explicit `columns` against `otype`'s declared
    /// properties: logical types via [`prop_ty`] in the caller's column order (an
    /// unknown column zips to `""`, the pre-existing sentinel), no masked columns.
    /// The ACTION write path's response epilogue — the affected row echoes columns
    /// the caller already cleared the Write gate for; masking is a read-render
    /// concept and does not apply to a write echo.
    pub fn of_columns(otype: &ObjectType, columns: Vec<String>) -> Self {
        let logical_types = columns
            .iter()
            .map(|name| prop_ty(otype, name).map(str::to_string).unwrap_or_default())
            .collect();
        Self {
            columns,
            logical_types,
            masked: Vec::new(),
        }
    }

    /// Append one derived output column (name + declared logical type); `masked` marks
    /// it as mask-marker-SELECTed for the output mask set.
    pub fn push(&mut self, name: String, ty: String, masked: bool) {
        if masked {
            self.masked.push(name.clone());
        }
        self.columns.push(name);
        self.logical_types.push(ty);
    }

    /// Zip rows the caller already holds into an `ObjectRows` — the write path's
    /// conversion, where there is no serving-engine echo to cross-check.
    /// [`Self::into_object_rows`] delegates here after its column-order contract
    /// check.
    pub fn object_rows(
        self,
        rows: Vec<Vec<crate::serving::SqlValue>>,
    ) -> crate::handler::ObjectRows {
        crate::handler::ObjectRows {
            columns: self.columns,
            logical_types: self.logical_types,
            rows,
        }
    }

    /// Zip served rows into an `ObjectRows`. The serving engine must echo the projected
    /// columns in SELECT order — the contract that lets the renderer zip
    /// `logical_types`/`columns` onto each row's cells by position.
    pub fn into_object_rows(self, served: crate::serving::Rows) -> crate::handler::ObjectRows {
        debug_assert_eq!(
            served.columns, self.columns,
            "serving engine returned columns out of the projected order"
        );
        self.object_rows(served.rows)
    }
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

/// Resolve one directed hop from `current`: `Forward` follows an outbound link by name
/// (`links`); `Inverse` follows an inbound link backwards (`links_to`) with an ambiguity
/// check (inbound names need not be unique — links are keyed `(name, from)`) and the
/// backing column roles swapped so the symmetric join compiler reads it in reverse.
/// A missing link is `UnknownLink`; a `links()`/`links_to()` NotFound on `current` is a
/// defensive `UnknownType` (unreachable when the caller just resolved `current`).
pub async fn resolve_hop(
    ontology: &(dyn Ontology + Send + Sync),
    current: &TypeName,
    hop: &Hop,
) -> Result<(TypeName, control_plane_core::LinkBacking), QueryError> {
    match hop.direction {
        Direction::Forward => {
            let links = ontology
                .links(current, PageReq::unbounded())
                .await
                .map_err(|e| match e {
                    ControlPlaneError::NotFound(_) => QueryError::UnknownType(current.0.clone()),
                    other => QueryError::ControlPlane(other),
                })?;
            let link = links
                .items
                .into_iter()
                .find(|l| l.name == hop.link)
                .ok_or_else(|| QueryError::UnknownLink(hop.link.clone()))?;
            Ok((link.to, link.backing))
        }
        Direction::Inverse => {
            let links = ontology
                .links_to(current, PageReq::unbounded())
                .await
                .map_err(|e| match e {
                    ControlPlaneError::NotFound(_) => QueryError::UnknownType(current.0.clone()),
                    other => QueryError::ControlPlane(other),
                })?;
            let mut matches = links.items.into_iter().filter(|l| l.name == hop.link);
            let link = matches
                .next()
                .ok_or_else(|| QueryError::UnknownLink(hop.link.clone()))?;
            if matches.next().is_some() {
                return Err(QueryError::AmbiguousLink(hop.link.clone()));
            }
            // Inverse: land on the origin, backing reversed so the symmetric join
            // reaches `current` back to `link.from`.
            Ok((link.from, link.backing.reversed()))
        }
    }
}

/// The shared caller-filter + object-set seed loop: each `(column, raw)` filter is
/// visibility-gated against `allowed`/masked (denied, masked, or unknown column ->
/// `BadFilter`, no type-info leak) and coerced to a typed predicate; then `ids` lowers
/// to an `In` predicate on the declared identity. Order: filters (as given), then ids.
pub fn seed_predicates(
    g: &GovernedType,
    allowed: &[String],
    filters: &[(String, String)],
    ids: &[String],
) -> Result<Vec<crate::filter::CallerPredicate>, QueryError> {
    let mut predicates = Vec::with_capacity(filters.len() + 1);
    for (col, raw) in filters {
        predicates.push(coerce_visible_predicate(
            col, raw, &g.otype, allowed, &g.masked,
        )?);
    }
    if let Some(p) = identity_in_predicate(&g.otype, &g.denied, &g.masked, ids)? {
        predicates.push(p);
    }
    Ok(predicates)
}
