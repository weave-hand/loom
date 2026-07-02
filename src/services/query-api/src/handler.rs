//! The socket-free governed-read core. resolve type -> table + property columns;
//! load ACL policy -> row filter + denied columns; project allowed columns; compile
//! SQL with bound params; execute on the serving engine.

use control_plane_core::{
    Acl, Action, ControlPlaneError, Decision, ObjectType, Ontology, PageReq, PolicyTarget,
    PropertyDef, RowFilter, SubjectId, TypeName,
};
pub use service_runtime::Subject;

use crate::serving::{Rows, ServingEngine, SqlValue};
use crate::sql::SqlDialect;
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

/// One node of a shortest-path tree: its identity value, BFS depth (0 = root), predecessor
/// identity value (`SqlValue::Null` for a root), and the governed object cells aligned to
/// `ObjectTree.columns`.
#[derive(Debug)]
pub struct TreeNode {
    pub id: SqlValue,
    pub depth: i64,
    pub parent: SqlValue,
    pub cells: Vec<SqlValue>,
}

/// A governed shortest-path-tree read result: the object projection columns + their logical
/// types (positionally aligned to each node's `cells`), the identity property's logical type
/// (renders each node's `id` and `parent`), and the nodes ordered by `(depth, id)`.
#[derive(Debug)]
pub struct ObjectTree {
    pub columns: Vec<String>,
    pub logical_types: Vec<String>,
    pub identity_type: String,
    pub nodes: Vec<TreeNode>,
}

/// The governed, compiled-but-not-yet-executed form of an object read: the SQL string +
/// positional params, plus the projected output columns and their logical types (in SELECT
/// order), and which of those output columns were **masked**. Shared by `read_object`
/// (executes → `ObjectRows`) and the Flight export path (streams the engine result + builds
/// the Arrow schema from `columns`/`logical_types`/`masked_columns`).
///
/// `masked_columns` is load-bearing for the export schema: a masked column is SELECTed as the
/// constant `'***'` (`sql.rs` `MASK_MARKER`), so the engine streams it back as **Utf8**, not as
/// its declared logical type. The export schema builder must therefore advertise masked columns
/// as `Utf8` — otherwise `get_flight_info`'s schema (e.g. `Float64`/`List<Float32>`) disagrees
/// with the `do_get` data schema and a strict Flight client errors.
#[derive(Debug)]
pub struct GovernedRead {
    pub sql: String,
    pub params: Vec<SqlValue>,
    pub columns: Vec<String>,
    pub logical_types: Vec<String>,
    pub masked_columns: Vec<String>,
}

/// A read request: an ontology type plus optional filters on allowed columns.
pub struct ObjectQuery {
    pub type_name: String,
    /// Caller filter predicates as `(column, raw)` pairs — parsed by
    /// `filter::coerce_predicate` (eq/ne/lt/le/gt/ge/in/nin/isnull/isnotnull/between/
    /// contains/startswith/endswith). Repeated keys AND together.
    pub filters: Vec<(String, String)>,
    /// Object-set input: scope the read to these identity values (an `In` predicate on
    /// the declared identity). Empty = no scoping.
    pub ids: Vec<String>,
}

/// Borrowed dependencies for one read.
pub struct QueryDeps<'a> {
    pub ontology: &'a (dyn Ontology + Send + Sync),
    pub acl: &'a (dyn Acl + Send + Sync),
    pub serving: &'a dyn ServingEngine,
    pub default_limit: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("unknown object type: {0}")]
    UnknownType(String),
    #[error("unknown link: {0}")]
    UnknownLink(String),
    /// More than one inbound link shares the requested name (links are keyed by
    /// `(name, from)`, so `(name, to)` need not be unique). A governed read cannot pick
    /// one deterministically.
    #[error("ambiguous inbound link: {0}")]
    AmbiguousLink(String),
    #[error("forbidden")]
    Forbidden,
    #[error("filter column not permitted: {0}")]
    BadFilter(String),
    /// A caller filter value did not coerce to its column's declared logical type. Carries
    /// the underlying `FilterError` (whose message already names the column) so the parse
    /// detail survives to the HTTP layer instead of being discarded. Distinct from
    /// `BadFilter`, which is a governance denial (column not permitted), not a parse fault.
    #[error(transparent)]
    BadFilterValue(#[from] crate::filter::FilterError),
    /// The traversal chain is malformed (empty path, or depth over the cap).
    #[error("malformed traversal chain: {0}")]
    BadChain(String),
    /// Association was requested but a projected end (source or final target) has no
    /// declared identity, so its objects cannot be named in a pair.
    #[error("type has no declared identity: {0}")]
    NoIdentity(String),
    /// `/graph` was given a path that does not form a cycle (following it does not return to
    /// the queried type), so it cannot be repeated. Use relational `/links` for fixed paths.
    #[error("path is not a cycle on the queried type: {0}")]
    NotCyclicPath(String),
    /// `/graph` part-B: the `*`-suffixed recursive core is malformed — the core link is not a
    /// self-link on the queried type, or the relational tail is empty. (Structural faults — a
    /// misplaced/duplicated `*` — are rejected by the HTTP layer before the handler.)
    #[error("malformed graph path: {0}")]
    BadGraphPath(String),
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

/// True when the type's declared identity column is denied or masked by policy,
/// so its values must not be revealed. Identity-less types are never governed here.
pub fn identity_is_governed(
    otype: &ObjectType,
    denied: &std::collections::HashSet<String>,
    masked: &std::collections::HashSet<String>,
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
    denied: &std::collections::HashSet<String>,
    masked: &std::collections::HashSet<String>,
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
    let ty = otype
        .properties
        .iter()
        .find(|p| p.name == identity)
        .map(|p| p.ty.as_str())
        .unwrap_or("");
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

/// The target column a derived aggregate reads, if any (COUNT reads none).
fn agg_column(a: &control_plane_core::Aggregation) -> Option<&str> {
    use control_plane_core::Aggregation::*;
    match a {
        Count => None,
        Sum(c) | Avg(c) | Min(c) | Max(c) => Some(c.as_str()),
    }
}

/// Govern + compile an object read without executing it. Resolves the type, applies the
/// deny-by-default Read gate, loads row/column policy, projects allowed columns, resolves
/// governed derived (aggregate-over-link) columns, and compiles the SELECT (with `limit`).
/// Returns the SQL + params + projected `columns`/`logical_types` (SELECT order) + which
/// output columns were masked. Shared by the HTTP read path and the Flight export path so
/// governance lives in exactly one place.
pub async fn compile_object_read(
    q: &ObjectQuery,
    subject: &Subject,
    ontology: &(dyn Ontology + Send + Sync),
    acl: &(dyn Acl + Send + Sync),
    dialect: &dyn SqlDialect,
    limit: u32,
) -> Result<GovernedRead, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let target = PolicyTarget::Type(type_name.clone());

    // Coarse gate, deny-by-default: the subject must hold a Read grant on this type.
    // No grant — including an unknown/anonymous subject — is Forbidden, returned BEFORE
    // we reveal whether the type exists. Fine-grained row/column policy below only
    // narrows what an already-permitted subject sees.
    if acl.check(&subject.0, Action::Read, &target).await? == Decision::Deny {
        return Err(QueryError::Forbidden);
    }

    // resolve: type -> ObjectType (table + ordered properties). A genuine miss is a
    // client 404 (UnknownType); a backend fault must propagate as itself (-> 500),
    // not masquerade as an unknown type.
    let object_type = ontology.get_type(&type_name).await.map_err(|e| match e {
        ControlPlaneError::NotFound(_) => QueryError::UnknownType(q.type_name.clone()),
        other => QueryError::ControlPlane(other),
    })?;

    let (row_filters, denied, masked) = load_policy(acl, &subject.0, &target).await?;

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
    let mut predicates: Vec<crate::filter::CallerPredicate> = Vec::with_capacity(q.filters.len());
    for (col, raw) in &q.filters {
        if !allowed.contains(col) || masked.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
        let ty = object_type
            .properties
            .iter()
            .find(|p| &p.name == col)
            .map(|p| p.ty.as_str())
            .unwrap_or("");
        let p = crate::filter::coerce_predicate(col, ty, raw)?;
        predicates.push(p);
    }

    // Object-set input: scope to the given identities (an In predicate on the identity).
    if let Some(p) = identity_in_predicate(&object_type, &denied, &masked, &q.ids)? {
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
        let links = ontology.links(&type_name, PageReq::unbounded()).await?;
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
            if acl.check(&subject.0, Action::Read, &target_pt).await? == Decision::Deny {
                continue;
            }
            let target_type = match ontology.get_type(&link.to).await {
                Ok(t) => t,
                Err(ControlPlaneError::NotFound(_)) => continue, // target type gone -> omit
                Err(other) => return Err(QueryError::ControlPlane(other)),
            };
            let (t_filters, t_denied, _t_masked) = load_policy(acl, &subject.0, &target_pt).await?;
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
        dialect,
        &object_type.table,
        &allowed,
        &mask_cols,
        &row_filters,
        &predicates,
        &derived_selects,
        limit,
    )?;
    // Output columns = physical `allowed` (in order) ++ surviving derived (in order).
    let mut columns = allowed.clone();
    columns.extend(derived_names.iter().cloned());
    // Logical type per projected column, in output order — which is the SELECT order
    // compile_select emits, hence the order of a served row's cells. Physical columns map
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
    // Which projected output columns were masked (SELECTed as the constant mask marker, so
    // streamed back as Utf8). Load-bearing for the export Arrow schema; see `GovernedRead`.
    let masked_columns: Vec<String> = columns
        .iter()
        .filter(|c| masked.contains(*c))
        .cloned()
        .collect();
    Ok(GovernedRead {
        sql,
        params,
        columns,
        logical_types,
        masked_columns,
    })
}

pub async fn read_object(
    q: &ObjectQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let g = compile_object_read(
        q,
        subject,
        deps.ontology,
        deps.acl,
        deps.serving.dialect(),
        deps.default_limit,
    )
    .await?;
    let served = deps.serving.fetch_rows(&g.sql, &g.params).await?;
    // The serving engine must echo the projected columns in SELECT order — the contract
    // that lets the renderer zip logical_types/columns onto each row's cells by position.
    debug_assert_eq!(
        served.columns, g.columns,
        "serving engine returned columns out of the projected order"
    );
    Ok(ObjectRows {
        columns: g.columns,
        logical_types: g.logical_types,
        rows: served.rows,
    })
}

/// One ranked kNN hit: the identity value and its distance.
pub struct VectorHit {
    pub id: SqlValue,
    pub distance: f64,
}

/// A governed kNN request: the type + named index, the probe vector, the fan-out `k`,
/// and the optional index-tuning knobs threaded to the engine.
pub struct VectorSearchQuery {
    pub type_name: String,
    pub index_name: String,
    pub query: Vec<f32>,
    pub k: usize,
    pub nprobe: Option<u32>,
    pub ef_search: Option<u32>,
}

/// Decode a 2-column engine result (`id`, `_distance`) into ordered hits, preserving the
/// engine's ascending-distance order. A row whose distance cell is neither `Double` nor
/// the defensive `Int` fallback is dropped (never a panic).
fn rows_to_hits(rows: &Rows) -> Vec<VectorHit> {
    rows.rows
        .iter()
        .filter_map(|r| {
            let id = r.first()?.clone();
            let distance = match r.get(1) {
                Some(SqlValue::Double(f)) => *f,
                // Defensive fallback: the expected arrow mapping is Float32 -> Double, so an
                // `Int` distance is rare and small in magnitude.
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "distance fallback, magnitude small"
                )]
                Some(SqlValue::Int(i)) => *i as f64,
                _ => return None,
            };
            Some(VectorHit { id, distance })
        })
        .collect()
}

/// Canonical string key for set membership across engine hits and post-filter rows.
fn sqlvalue_to_id_string(v: &SqlValue) -> String {
    match v {
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Text(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

/// Governed kNN: coarse Read gate → engine kNN → row-filter post-filter.
/// Unknown type and a missing Read grant both return `Forbidden` (no existence leak).
pub async fn vector_search(
    q: &VectorSearchQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<Vec<VectorHit>, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let target = PolicyTarget::Type(type_name.clone());

    // Coarse gate, deny-by-default, BEFORE revealing whether the type exists.
    if deps.acl.check(&subject.0, Action::Read, &target).await? == Decision::Deny {
        return Err(QueryError::Forbidden);
    }
    // Resolve the type; a granted-but-nonexistent type is still no-leak Forbidden.
    let otype = match deps.ontology.get_type(&type_name).await {
        Ok(t) => t,
        Err(ControlPlaneError::NotFound(_)) => return Err(QueryError::Forbidden),
        Err(e) => return Err(QueryError::ControlPlane(e)),
    };

    // Engine kNN over the named index (ServingError::NoIndex/DimMismatch propagate as Serving).
    let rows = deps
        .serving
        .vector_search(
            &otype.table,
            &q.index_name,
            &q.query,
            q.k,
            q.nprobe,
            q.ef_search,
        )
        .await?;
    let mut hits = rows_to_hits(&rows);
    if hits.is_empty() {
        return Ok(hits);
    }

    // Row-filter post-filter. Empty filters (unrestricted) → return engine hits unchanged.
    let (row_filters, denied, masked) = load_policy(deps.acl, &subject.0, &target).await?;
    // Fail closed: the search response *is* a list of identity values, so a policy that
    // denies or masks the identity column must not be silently disregarded. Guard runs
    // BEFORE the empty-filter early return so both policy shapes (no row filter, and with
    // a row filter) refuse identically with a deliberate 403 instead of leaking ids
    // (empty-filter path) or an incidental BadFilter/500 (row-filter path).
    if identity_is_governed(&otype, &denied, &masked) {
        return Err(QueryError::Forbidden);
    }
    if row_filters.is_empty() {
        return Ok(hits);
    }
    let identity = otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(otype.name.0.clone()))?;
    let candidate_strs: Vec<String> = hits.iter().map(|h| sqlvalue_to_id_string(&h.id)).collect();
    let Some(pred) = identity_in_predicate(&otype, &denied, &masked, &candidate_strs)? else {
        return Ok(hits); // no candidates to scope (empty handled above; defensive)
    };
    let limit = u32::try_from(candidate_strs.len()).unwrap_or(u32::MAX);
    let (sql, params) = compile_select_with(
        deps.serving.dialect(),
        &otype.table,
        std::slice::from_ref(&identity),
        &[],
        &row_filters,
        std::slice::from_ref(&pred),
        &[],
        limit,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    let surviving: std::collections::HashSet<String> = served
        .rows
        .iter()
        .filter_map(|r| r.first())
        .map(sqlvalue_to_id_string)
        .collect();
    // Keep only surviving ids, preserving the engine's distance order; may return < k.
    hits.retain(|h| surviving.contains(&sqlvalue_to_id_string(&h.id)));
    Ok(hits)
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
            ids: vec![],
        },
        subject,
        deps,
    )
    .await
}

/// Maximum chain depth (number of hops). A request beyond this is rejected before
/// any catalog/ACL work — bounds the join count. Deliberately `const`, not config: a
/// safety guardrail an operator must not be able to lift per-deployment. See
/// road-config-seam-unification.
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
    /// Object-set input: scope the SOURCE to these identity values. Empty = no scoping.
    pub ids: Vec<String>,
}

/// A bounded recursive reachability read over a path-cycle. `filters`/`ids` scope the SEED
/// set (the starting objects); the recursion repeats `path` (a cyclic link pattern that
/// returns to `type_name`) up to `depth` times. A 1-element `path` is the single-self-link
/// case.
pub struct GraphQuery {
    pub type_name: String,
    pub path: Vec<Hop>,
    pub depth: u32,
    pub filters: Vec<(String, String)>,
    pub ids: Vec<String>,
}

/// A bounded recursive reachability read over a UNION of self-links. `filters`/`ids` scope the
/// SEED set (the starting objects); the recursion follows ANY ONE of `links` (each a self-link
/// on `type_name`) up to `depth` times.
pub struct GraphUnionQuery {
    pub type_name: String,
    pub links: Vec<String>,
    pub depth: u32,
    pub filters: Vec<(String, String)>,
    pub ids: Vec<String>,
}

/// Resolve + govern a chain: depth check, source Read gate, per-hop type resolution
/// (forward/inverse) with Read-on-every-reached-type, per-position row-filters, and
/// caller-filter coercion/visibility. Returns the per-position metadata, the compiler
/// `ChainType`s (row-filters + caller predicates), and the hop backings. Shared by the
/// object-projection read and the association read so governance lives in one place.
async fn resolve_chain(
    q: &ChainQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<
    (
        Vec<HopMeta>,
        Vec<crate::sql::ChainType>,
        Vec<control_plane_core::LinkBacking>,
    ),
    QueryError,
> {
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
        let (next_name, backing) = match hop.direction {
            Direction::Forward => {
                let links = deps
                    .ontology
                    .links(&current_name, PageReq::unbounded())
                    .await
                    .map_err(|e| match e {
                        ControlPlaneError::NotFound(_) => {
                            QueryError::UnknownType(current_name.0.clone())
                        }
                        other => QueryError::ControlPlane(other),
                    })?;
                let link = links
                    .items
                    .into_iter()
                    .find(|l| l.name == hop.link)
                    .ok_or_else(|| QueryError::UnknownLink(hop.link.clone()))?;
                (link.to.clone(), link.backing.clone())
            }
            Direction::Inverse => {
                let links = deps
                    .ontology
                    .links_to(&current_name, PageReq::unbounded())
                    .await
                    .map_err(|e| match e {
                        ControlPlaneError::NotFound(_) => {
                            QueryError::UnknownType(current_name.0.clone())
                        }
                        other => QueryError::ControlPlane(other),
                    })?;
                let mut matches = links.items.into_iter().filter(|l| l.name == hop.link);
                let link = matches
                    .next()
                    .ok_or_else(|| QueryError::UnknownLink(hop.link.clone()))?;
                if matches.next().is_some() {
                    return Err(QueryError::AmbiguousLink(hop.link.clone()));
                }
                // Inverse: follow the link to its origin, with the backing column roles
                // swapped so the symmetric chain compiler joins `current` back to `from`.
                (link.from.clone(), link.backing.reversed())
            }
        };
        let next_target = PolicyTarget::Type(next_name.clone());
        // Read on every reached type (the leak-free guarantee), forward or inverse.
        if deps
            .acl
            .check(&subject.0, Action::Read, &next_target)
            .await?
            == Decision::Deny
        {
            return Err(QueryError::Forbidden);
        }
        // A link pointing at a missing type is an internal inconsistency, not a 404.
        let next_type = deps.ontology.get_type(&next_name).await?;
        let (t_filters, t_denied, t_masked) =
            load_policy(deps.acl, &subject.0, &next_target).await?;
        hops.push(backing);
        ctypes.push(crate::sql::ChainType {
            table: next_type.table.clone(),
            row_filters: t_filters,
            predicates: vec![],
        });
        metas.push(HopMeta {
            otype: next_type,
            denied: t_denied,
            masked: t_masked,
        });
        current_name = next_name;
    }

    // Caller filters, governed per position: visibility first (denied/masked or unknown
    // column -> 400, no type-info leak), then parse the raw value into a typed predicate
    // (operator + coerced operands) bound at the position's alias `t_i`.
    for f in &q.filters {
        if f.position >= ctypes.len() {
            return Err(QueryError::BadFilter(f.column.clone()));
        }
        let meta = metas
            .get(f.position)
            .ok_or_else(|| QueryError::BadFilter(f.column.clone()))?;
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
        let p = crate::filter::coerce_predicate(&f.column, ty, &f.raw)?;
        ctypes
            .get_mut(f.position)
            .ok_or_else(|| QueryError::BadFilter(f.column.clone()))?
            .predicates
            .push(p);
    }

    // Object-set input: scope the SOURCE (position 0) to the given identities.
    let source = metas
        .first()
        .ok_or_else(|| QueryError::BadChain("empty chain".to_string()))?;
    if let Some(p) = identity_in_predicate(&source.otype, &source.denied, &source.masked, &q.ids)? {
        ctypes
            .first_mut()
            .ok_or_else(|| QueryError::BadChain("empty chain".to_string()))?
            .predicates
            .push(p);
    }

    Ok((metas, ctypes, hops))
}

pub async fn read_linked_chain(
    q: &ChainQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let (metas, ctypes, hops) = resolve_chain(q, subject, deps).await?;
    // Final-target projection, from the last position (path is non-empty => >= 2 metas).
    let target = metas
        .last()
        .ok_or_else(|| QueryError::BadChain("empty chain".to_string()))?;
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
        deps.default_limit,
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

/// A governed source→target association result: deduped identity pairs plus the logical
/// type of each end's identity (for typed rendering).
#[derive(Debug)]
pub struct Associations {
    pub from_id_type: String,
    pub to_id_type: String,
    pub pairs: Vec<(SqlValue, SqlValue)>,
}

/// A governed traversal returning source↔final-target identity pairs (the edge list)
/// instead of the projected target objects. Resolves + governs the chain identically to
/// `read_linked_chain`, then requires a declared, caller-visible identity on the source
/// and final-target types and projects the two id columns as a DISTINCT pair.
pub async fn read_associations(
    q: &ChainQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<Associations, QueryError> {
    let (metas, ctypes, hops) = resolve_chain(q, subject, deps).await?;
    let source = metas
        .first()
        .ok_or_else(|| QueryError::BadChain("empty chain".to_string()))?;
    let target = metas
        .last()
        .ok_or_else(|| QueryError::BadChain("empty chain".to_string()))?;

    // Both projected ends must declare an identity.
    let source_id = source
        .otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(source.otype.name.0.clone()))?;
    let target_id = target
        .otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(target.otype.name.0.clone()))?;

    // …and the identity column must be visible (not denied, not masked) on each end —
    // you cannot associate objects you cannot identify.
    let s_allowed = project_allowed(&source.otype.properties, &source.denied);
    if !s_allowed.contains(&source_id) || source.masked.contains(&source_id) {
        return Err(QueryError::Forbidden);
    }
    let t_allowed = project_allowed(&target.otype.properties, &target.denied);
    if !t_allowed.contains(&target_id) || target.masked.contains(&target_id) {
        return Err(QueryError::Forbidden);
    }

    let from_id_type = source
        .otype
        .properties
        .iter()
        .find(|p| p.name == source_id)
        .map(|p| p.ty.clone())
        .unwrap_or_default();
    let to_id_type = target
        .otype
        .properties
        .iter()
        .find(|p| p.name == target_id)
        .map(|p| p.ty.clone())
        .unwrap_or_default();

    let (sql, params) = crate::sql::compile_chain_pairs(
        deps.serving.dialect(),
        &ctypes,
        &hops,
        &source_id,
        &target_id,
        deps.default_limit,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    let pairs: Vec<(SqlValue, SqlValue)> = served
        .rows
        .into_iter()
        .map(|mut row| {
            // compile_chain_pairs SELECTs exactly [source_id, target_id] in that order.
            let to = row.pop().unwrap_or(SqlValue::Null);
            let from = row.pop().unwrap_or(SqlValue::Null);
            (from, to)
        })
        .collect();
    Ok(Associations {
        from_id_type,
        to_id_type,
        pairs,
    })
}

/// Serve a bounded recursive reachability read over a path-cycle: from the seed set, repeat
/// `path` (a cyclic link pattern returning to the queried type) up to `depth` times, return
/// the deduped reachable objects. Governed: Read on the queried type AND every intermediate
/// type in the cycle, row-filters at the seed/every recursive expansion/projection, declared
/// identity (dedup key; visibility not required since it is never projected unless it is
/// itself a visible column).
pub async fn read_graph_reach(
    q: &GraphQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let r = resolve_graph(q, subject, deps).await?;

    let (sql, params) = crate::sql::compile_graph_reach(
        deps.serving.dialect(),
        &r.object_type.table,
        &r.identity,
        &r.steps,
        &r.seed_predicates,
        &r.row_filters,
        &r.allowed,
        &r.mask_cols,
        q.depth,
        deps.default_limit,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    let logical_types: Vec<String> = r
        .allowed
        .iter()
        .map(|name| {
            r.object_type
                .properties
                .iter()
                .find(|p| &p.name == name)
                .map(|p| p.ty.clone())
                .unwrap_or_default()
        })
        .collect();
    debug_assert_eq!(
        served.columns, r.allowed,
        "serving engine returned columns out of the projected order"
    );
    Ok(ObjectRows {
        columns: r.allowed,
        logical_types,
        rows: served.rows,
    })
}

/// Serve a bounded shortest-path-**tree** read over a path-cycle (a 1-element path is the
/// single-self-link case): from the seed set, repeat `path` up to `depth` times, and for every
/// reachable node return its shortest-path parent pointer + BFS depth, rooted at the seed set.
/// Governance is `read_graph_reach`'s exactly (Read on the queried + every intermediate type;
/// row-filters at seed/expansion/projection so no denied intermediate can be a parent), plus
/// one precondition: because the tree PROJECTS identity as `id`/`parent`, a denied or masked
/// identity cannot be served without leaking it -> `Forbidden` (undeclared identity is already
/// `NoIdentity` from `resolve_graph`). The compiler emits no LIMIT (a LIMIT could drop a parent
/// while keeping its child); the depth cap bounds the *path length*, so — unlike the
/// `default_limit`-capped reach read — the full reachable set within the cap is returned
/// unpaginated, which can be wider than the equivalent reach query.
pub async fn read_graph_tree(
    q: &GraphQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectTree, QueryError> {
    let r = resolve_graph(q, subject, deps).await?;

    // The tree projects identity (id + parent). A denied/masked identity would leak -> Forbidden.
    if identity_is_governed(&r.object_type, &r.denied, &r.masked) {
        return Err(QueryError::Forbidden);
    }

    let (sql, params) = crate::sql::compile_graph_tree(
        deps.serving.dialect(),
        &r.object_type.table,
        &r.identity,
        &r.steps,
        &r.seed_predicates,
        &r.row_filters,
        &r.allowed,
        &r.mask_cols,
        q.depth,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    // Contract guard (mirrors the sibling reads): the tree projects the visible columns then
    // the three trailing tree columns, in this exact order — the positional split below relies
    // on it. A compiler/engine column-order regression trips this in tests rather than silently
    // producing garbled nodes.
    debug_assert_eq!(
        served.columns,
        {
            let mut expected = r.allowed.clone();
            expected.extend(
                [
                    crate::sql::TREE_DEPTH_COL,
                    crate::sql::TREE_PARENT_COL,
                    crate::sql::TREE_NODE_ID_COL,
                ]
                .into_iter()
                .map(String::from),
            );
            expected
        },
        "serving engine returned tree columns out of the projected order"
    );

    let logical_types: Vec<String> = r
        .allowed
        .iter()
        .map(|name| {
            r.object_type
                .properties
                .iter()
                .find(|p| &p.name == name)
                .map(|p| p.ty.clone())
                .unwrap_or_default()
        })
        .collect();
    let identity_type = r
        .object_type
        .properties
        .iter()
        .find(|p| p.name == r.identity)
        .map(|p| p.ty.clone())
        .unwrap_or_default();

    // Each served row is [object cells..., __depth, __parent, __id] (compile_graph_tree order).
    // Pop the three trailing columns off the end; the remainder are the object cells.
    let ncols = r.allowed.len();
    let nodes: Vec<TreeNode> = served
        .rows
        .into_iter()
        .map(|mut row| {
            let node_id = row.pop().unwrap_or(SqlValue::Null); // __id
            let parent = row.pop().unwrap_or(SqlValue::Null); // __parent
            let depth = match row.pop() {
                Some(SqlValue::Int(d)) => d,
                _ => 0, // a serving-engine surprise must not panic a permitted read
            };
            row.truncate(ncols); // defensive: keep exactly the object cells
            TreeNode {
                id: node_id,
                depth,
                parent,
                cells: row,
            }
        })
        .collect();

    Ok(ObjectTree {
        columns: r.allowed,
        logical_types,
        identity_type,
        nodes,
    })
}

/// Everything the two graph reads (reachable-set and shortest-path-tree) need after resolving
/// the queried type, ACL policy, and the path-cycle: the resolved object type + declared
/// identity, the compiler `GraphStep`s (per-intermediate governance already folded in), the
/// start row-filters, the visible/masked projection, the raw denied/masked column sets (so a
/// caller can additionally require identity visibility), and the coerced seed predicates.
struct GraphResolved {
    object_type: ObjectType,
    identity: String,
    steps: Vec<crate::sql::GraphStep>,
    row_filters: Vec<RowFilter>,
    allowed: Vec<String>,
    mask_cols: Vec<String>,
    denied: std::collections::HashSet<String>,
    masked: std::collections::HashSet<String>,
    seed_predicates: Vec<crate::filter::CallerPredicate>,
}

/// Resolve + govern a graph read: Read-gate the queried type, resolve the path-cycle (Read on
/// every intermediate type, its row-filters folded into the step), require a declared identity
/// (the recursion's dedup key), project the visible columns, and coerce/visibility-check the
/// seed predicates + `?_ids=`. Shared verbatim by `read_graph_reach` (reachable set) and
/// `read_graph_tree` (shortest-path tree) so governance lives in one place.
async fn resolve_graph(
    q: &GraphQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<GraphResolved, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let target = PolicyTarget::Type(type_name.clone());

    // Read gate (deny-by-default, before existence is revealed).
    if deps.acl.check(&subject.0, Action::Read, &target).await? == Decision::Deny {
        return Err(QueryError::Forbidden);
    }
    let object_type = deps
        .ontology
        .get_type(&type_name)
        .await
        .map_err(|e| match e {
            ControlPlaneError::NotFound(_) => QueryError::UnknownType(q.type_name.clone()),
            other => QueryError::ControlPlane(other),
        })?;
    let (row_filters, denied, masked) = load_policy(deps.acl, &subject.0, &target).await?;

    // Declared identity is the recursion's dedup key.
    let identity = object_type
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(q.type_name.clone()))?;

    // Resolve the path-cycle: walk l1..lK forward from the queried type. Each landed type is
    // Read-gated and its row-filters loaded (intermediate governance). After the last link the
    // type must be the queried type again (a cycle) — else it cannot be repeated.
    if q.path.is_empty() {
        return Err(QueryError::NotCyclicPath(String::new()));
    }
    let mut steps: Vec<crate::sql::GraphStep> = Vec::with_capacity(q.path.len());
    let mut current = type_name.clone();
    let last = q.path.len() - 1;
    for (i, hop) in q.path.iter().enumerate() {
        let (landed, backing) = match hop.direction {
            Direction::Forward => {
                let links = deps.ontology.links(&current, PageReq::unbounded()).await?;
                let link = links
                    .items
                    .into_iter()
                    .find(|l| l.name == hop.link)
                    .ok_or_else(|| QueryError::UnknownLink(hop.link.clone()))?;
                (link.to.clone(), link.backing.clone())
            }
            Direction::Inverse => {
                let links = deps
                    .ontology
                    .links_to(&current, PageReq::unbounded())
                    .await?;
                let mut matches = links.items.into_iter().filter(|l| l.name == hop.link);
                let link = matches
                    .next()
                    .ok_or_else(|| QueryError::UnknownLink(hop.link.clone()))?;
                if matches.next().is_some() {
                    return Err(QueryError::AmbiguousLink(hop.link.clone()));
                }
                // Inverse: land on the origin, backing reversed so the symmetric join
                // reaches `current` back to `link.from`.
                (link.from.clone(), link.backing.reversed())
            }
        };
        let landed_target = PolicyTarget::Type(landed.clone());
        // Read on every reached type (intermediate + final), forward or inverse.
        if deps
            .acl
            .check(&subject.0, Action::Read, &landed_target)
            .await?
            == Decision::Deny
        {
            return Err(QueryError::Forbidden);
        }
        let landed_type = deps.ontology.get_type(&landed).await?;
        let (landed_filters, _ld, _lm) = load_policy(deps.acl, &subject.0, &landed_target).await?;
        // Intermediates carry their own row-filters; the FINAL landing is the start type, whose
        // filters are rendered at `nxt` by the compiler -> pass empty here (no double-render).
        let next_filters = if i == last {
            Vec::new()
        } else {
            landed_filters
        };
        steps.push(crate::sql::GraphStep {
            backing,
            next_table: landed_type.table.clone(),
            next_filters,
        });
        current = landed;
    }
    if current != type_name {
        return Err(QueryError::NotCyclicPath(hop_path_string(&q.path)));
    }

    // Projection: visible columns minus denied; masked applied. Empty -> Forbidden.
    let allowed = project_allowed(&object_type.properties, &denied);
    if allowed.is_empty() {
        return Err(QueryError::Forbidden);
    }
    let mask_cols: Vec<String> = allowed
        .iter()
        .filter(|c| masked.contains(*c))
        .cloned()
        .collect();

    // Seed predicates: source filters (visibility-checked + coerced) then the ?_ids= set.
    let mut seed_predicates: Vec<crate::filter::CallerPredicate> = Vec::new();
    for (col, raw) in &q.filters {
        if !allowed.contains(col) || masked.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
        let ty = object_type
            .properties
            .iter()
            .find(|p| &p.name == col)
            .map(|p| p.ty.as_str())
            .unwrap_or("");
        seed_predicates.push(crate::filter::coerce_predicate(col, ty, raw)?);
    }
    if let Some(p) = identity_in_predicate(&object_type, &denied, &masked, &q.ids)? {
        seed_predicates.push(p);
    }

    Ok(GraphResolved {
        object_type,
        identity,
        steps,
        row_filters,
        allowed,
        mask_cols,
        denied,
        masked,
        seed_predicates,
    })
}

/// Re-serialize a resolved path for error messages, re-emitting the `~` sigil for inverse
/// hops so the rendered path round-trips the request (`memberOf,~memberOf`).
fn hop_path_string(path: &[Hop]) -> String {
    path.iter()
        .map(|h| match h.direction {
            Direction::Forward => h.link.clone(),
            Direction::Inverse => format!("~{}", h.link),
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Serve a bounded recursive reachability read over a UNION of self-links: from the seed set,
/// repeatedly follow ANY ONE of `links` (each a self-link on the queried type) up to `depth`
/// times, return the deduped reachable objects. Governed: Read on the queried type and the
/// queried type's row-filters at the seed/every recursive expansion/projection; declared identity
/// (dedup key). Every named link must be a self-link (its `to` is the queried type) -> else
/// NotCyclicPath; an unknown link -> UnknownLink. Because every link lands on the already-gated
/// queried type, there are no intermediate types and no per-link Read gate.
pub async fn read_graph_reach_union(
    q: &GraphUnionQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let target = PolicyTarget::Type(type_name.clone());

    // Read gate (deny-by-default, before existence is revealed).
    if deps.acl.check(&subject.0, Action::Read, &target).await? == Decision::Deny {
        return Err(QueryError::Forbidden);
    }
    let object_type = deps
        .ontology
        .get_type(&type_name)
        .await
        .map_err(|e| match e {
            ControlPlaneError::NotFound(_) => QueryError::UnknownType(q.type_name.clone()),
            other => QueryError::ControlPlane(other),
        })?;
    let (row_filters, denied, masked) = load_policy(deps.acl, &subject.0, &target).await?;

    // Declared identity is the recursion's dedup key.
    let identity = object_type
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(q.type_name.clone()))?;

    // Resolve the self-link set: every named link must be an outbound link of the queried type
    // whose `to` is the queried type (a self-link). Dedup by name (first-seen order; a link
    // listed twice yields one arm). No per-link Read gate — every link lands on the queried type,
    // already gated above.
    if q.links.is_empty() {
        return Err(QueryError::NotCyclicPath(String::new()));
    }
    let links = deps
        .ontology
        .links(&type_name, PageReq::unbounded())
        .await?;
    let mut backings: Vec<control_plane_core::LinkBacking> = Vec::with_capacity(q.links.len());
    let mut seen: std::collections::HashSet<&String> = std::collections::HashSet::new();
    for link_name in &q.links {
        if !seen.insert(link_name) {
            continue; // duplicate -> one arm
        }
        let link = links
            .items
            .iter()
            .find(|l| &l.name == link_name)
            .ok_or_else(|| QueryError::UnknownLink(link_name.clone()))?;
        if link.to != type_name {
            return Err(QueryError::NotCyclicPath(link_name.clone()));
        }
        backings.push(link.backing.clone());
    }

    // Projection: visible columns minus denied; masked applied. Empty -> Forbidden.
    let allowed = project_allowed(&object_type.properties, &denied);
    if allowed.is_empty() {
        return Err(QueryError::Forbidden);
    }
    let mask_cols: Vec<String> = allowed
        .iter()
        .filter(|c| masked.contains(*c))
        .cloned()
        .collect();

    // Seed predicates: source filters (visibility-checked + coerced) then the ?_ids= set.
    let mut seed_predicates: Vec<crate::filter::CallerPredicate> = Vec::new();
    for (col, raw) in &q.filters {
        if !allowed.contains(col) || masked.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
        let ty = object_type
            .properties
            .iter()
            .find(|p| &p.name == col)
            .map(|p| p.ty.as_str())
            .unwrap_or("");
        seed_predicates.push(crate::filter::coerce_predicate(col, ty, raw)?);
    }
    if let Some(p) = identity_in_predicate(&object_type, &denied, &masked, &q.ids)? {
        seed_predicates.push(p);
    }

    let (sql, params) = crate::sql::compile_graph_reach_union(
        deps.serving.dialect(),
        &object_type.table,
        &identity,
        &backings,
        &seed_predicates,
        &row_filters,
        &allowed,
        &mask_cols,
        q.depth,
        deps.default_limit,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
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

/// A bounded recursive-core + relational-tail reachability read. `core_link` is a `*`-suffixed
/// self-link on `type_name` followed transitively up to `depth` times (the recursive core);
/// `tail_links` is a forward chain of ordinary links continuing from the depth>=1 reachable set,
/// landing on a (possibly different) final type that is projected. `filters`/`ids` scope the SEED
/// set (the recursion start), as in part-1/2/3.
pub struct GraphTailQuery {
    pub type_name: String,
    pub core_link: String,
    pub tail_links: Vec<String>,
    pub depth: u32,
    pub filters: Vec<(String, String)>,
    pub ids: Vec<String>,
}

/// Serve a recursive-core + relational-tail read: from the seed set, follow `core_link` (a
/// self-link) 1..`depth` times to a reachable set, then chain `tail_links` forward off that set
/// and project the final type. Governed: Read on the queried type (whose row-filters govern the
/// recursive core, applied at the seed and every recursive expansion) AND every tail-reached type
/// (row-filters at each), declared identity on the queried type (the recursion's dedup key + the
/// join key from the tail back to the reachable set). The core link must be a self-link and the
/// tail non-empty, else `BadGraphPath`; an unknown core/tail link -> `UnknownLink`.
pub async fn read_graph_reach_with_tail(
    q: &GraphTailQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let target = PolicyTarget::Type(type_name.clone());

    // Read gate on the queried (core) type (deny-by-default, before existence is revealed).
    if deps.acl.check(&subject.0, Action::Read, &target).await? == Decision::Deny {
        return Err(QueryError::Forbidden);
    }
    let object_type = deps
        .ontology
        .get_type(&type_name)
        .await
        .map_err(|e| match e {
            ControlPlaneError::NotFound(_) => QueryError::UnknownType(q.type_name.clone()),
            other => QueryError::ControlPlane(other),
        })?;
    let (core_filters, denied, masked) = load_policy(deps.acl, &subject.0, &target).await?;

    // Declared identity: the recursion's dedup key and the join key from the tail back to reach.
    let identity = object_type
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(q.type_name.clone()))?;

    // The relational tail must be non-empty (a bare recursive core is `/graph/:link`).
    if q.tail_links.is_empty() {
        return Err(QueryError::BadGraphPath(format!(
            "recursive core `{}*` requires a relational tail; use /graph/:link for bare reachability",
            q.core_link
        )));
    }

    // Resolve the recursive core link: an outbound link of the queried type whose `to` is the
    // queried type itself (a self-link).
    let links = deps
        .ontology
        .links(&type_name, PageReq::unbounded())
        .await?;
    let core = links
        .items
        .iter()
        .find(|l| l.name == q.core_link)
        .ok_or_else(|| QueryError::UnknownLink(q.core_link.clone()))?;
    if core.to != type_name {
        return Err(QueryError::BadGraphPath(format!(
            "recursive core `{}*` must land back on `{}`",
            q.core_link, q.type_name
        )));
    }
    let core_backing = core.backing.clone();

    // Resolve the forward tail. Position 0 is the queried type with EMPTY row-filters — its
    // governance lives in the recursive CTE; the tail constrains it by reach-membership. Each
    // tail-landed type is Read-gated and its row-filters loaded; the final landing is projected.
    let mut tail_types: Vec<crate::sql::ChainType> = vec![crate::sql::ChainType {
        table: object_type.table.clone(),
        row_filters: vec![],
        predicates: vec![],
    }];
    let mut tail_hops: Vec<control_plane_core::LinkBacking> =
        Vec::with_capacity(q.tail_links.len());
    let mut current = type_name.clone();
    let mut final_type = object_type.clone();
    let mut final_denied = denied.clone();
    let mut final_masked = masked.clone();
    for link_name in &q.tail_links {
        let outbound = deps.ontology.links(&current, PageReq::unbounded()).await?;
        let link = outbound
            .items
            .into_iter()
            .find(|l| &l.name == link_name)
            .ok_or_else(|| QueryError::UnknownLink(link_name.clone()))?;
        let landed = link.to.clone();
        let landed_target = PolicyTarget::Type(landed.clone());
        // Read on every reached type (the leak-free guarantee).
        if deps
            .acl
            .check(&subject.0, Action::Read, &landed_target)
            .await?
            == Decision::Deny
        {
            return Err(QueryError::Forbidden);
        }
        let landed_type = deps.ontology.get_type(&landed).await?;
        let (l_filters, l_denied, l_masked) =
            load_policy(deps.acl, &subject.0, &landed_target).await?;
        tail_hops.push(link.backing.clone());
        tail_types.push(crate::sql::ChainType {
            table: landed_type.table.clone(),
            row_filters: l_filters,
            predicates: vec![],
        });
        final_type = landed_type;
        final_denied = l_denied;
        final_masked = l_masked;
        current = landed;
    }

    // Projection: the FINAL tail type's visible columns (masked -> marker). Empty -> Forbidden.
    let allowed = project_allowed(&final_type.properties, &final_denied);
    if allowed.is_empty() {
        return Err(QueryError::Forbidden);
    }
    let mask_cols: Vec<String> = allowed
        .iter()
        .filter(|c| final_masked.contains(*c))
        .cloned()
        .collect();

    // Seed predicates scope the recursion start (alias `s` in the CTE): source filters
    // (visibility-checked + coerced against the queried type) then the ?_ids= set.
    let source_allowed = project_allowed(&object_type.properties, &denied);
    let mut seed_predicates: Vec<crate::filter::CallerPredicate> = Vec::new();
    for (col, raw) in &q.filters {
        if !source_allowed.contains(col) || masked.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
        let ty = object_type
            .properties
            .iter()
            .find(|p| &p.name == col)
            .map(|p| p.ty.as_str())
            .unwrap_or("");
        seed_predicates.push(crate::filter::coerce_predicate(col, ty, raw)?);
    }
    if let Some(p) = identity_in_predicate(&object_type, &denied, &masked, &q.ids)? {
        seed_predicates.push(p);
    }

    let (sql, params) = crate::sql::compile_graph_reach_tail(
        deps.serving.dialect(),
        &object_type.table,
        &identity,
        &core_backing,
        &seed_predicates,
        &core_filters,
        &tail_types,
        &tail_hops,
        &allowed,
        &mask_cols,
        q.depth,
        deps.default_limit,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    let logical_types: Vec<String> = allowed
        .iter()
        .map(|name| {
            final_type
                .properties
                .iter()
                .find(|p| &p.name == name)
                .map(|p| p.ty.clone())
                .unwrap_or_default()
        })
        .collect();
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
