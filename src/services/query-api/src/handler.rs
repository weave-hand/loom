//! The socket-free governed-read core. resolve type -> table + property columns;
//! load ACL policy -> row filter + denied columns; project allowed columns; compile
//! SQL with bound params; execute on the serving engine.

use control_plane_core::{
    Acl, Action, ControlPlaneError, Decision, Ontology, PageReq, PolicyTarget, TypeName,
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

impl GovernedRead {
    /// Zip served rows into an `ObjectRows` using this read's projected columns and
    /// logical types, asserting the engine echoed the SELECT column order.
    pub fn into_object_rows(self, served: crate::serving::Rows) -> ObjectRows {
        debug_assert_eq!(
            served.columns, self.columns,
            "serving engine returned columns out of the projected order"
        );
        ObjectRows {
            columns: self.columns,
            logical_types: self.logical_types,
            rows: served.rows,
        }
    }
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
    /// OR-group inputs: the raw value of each `_or` query param (a comma-separated list of
    /// `column:value` member predicates). Each entry becomes one parenthesized disjunction
    /// ANDed into the WHERE. Empty = no OR-groups. Parsed in `compile_object_read`.
    pub or_raw: Vec<String>,
    /// Optional time-travel selector; `None` = read the live snapshot.
    pub as_of: Option<AsOfSelector>,
}

/// A parsed-but-unresolved time-travel selector from `?as_of_snapshot=` / `?as_of=`.
/// Resolved to a concrete `SnapshotId` (against the target table) inside the read path.
#[derive(Debug, Clone, PartialEq)]
pub enum AsOfSelector {
    /// An exact mirror snapshot id (`?as_of_snapshot=`).
    Snapshot(i64),
    /// A wall-clock instant (`?as_of=`, RFC3339) -> the latest snapshot at/before it.
    Time(time::OffsetDateTime),
}

/// Borrowed dependencies for one read.
pub struct QueryDeps<'a> {
    pub ontology: &'a (dyn Ontology + Send + Sync),
    pub acl: &'a (dyn Acl + Send + Sync),
    pub serving: &'a dyn ServingEngine,
    pub catalog: &'a (dyn control_plane_core::Catalog + Send + Sync),
    pub default_limit: u32,
    /// GC retention window (`LOOM_GC_RETENTION_SECS`), threaded from `AppState`.
    pub gc_retention: std::time::Duration,
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
    /// Cursor pagination cannot be honored for this request: the type has no declared
    /// identity, the identity column is denied/masked, `_ids` and pagination were both
    /// given, or the `cursor` value does not coerce to the identity's logical type. Fail
    /// closed — never emit a cursor over a masked/denied identity column.
    #[error("bad pagination: {0}")]
    BadPagination(String),
    /// A server-side fault detected while processing engine-derived (non-caller)
    /// values — e.g. a mirror-returned identity cell that fails `coerce_filter`
    /// against its declared logical type, or a served vector type with no declared
    /// identity. Never caller-forgeable: renders as an opaque 500, logged
    /// server-side with the wrapped error's full detail.
    #[error("internal fault: {context}: {source}")]
    Internal {
        context: &'static str,
        source: Box<QueryError>,
    },
    #[error(transparent)]
    ControlPlane(#[from] control_plane_core::ControlPlaneError),
    #[error(transparent)]
    Serving(#[from] crate::serving::ServingError),
    #[error(transparent)]
    Malformed(#[from] crate::sql::CompileError),
    /// A time-travel selector (`?as_of`/`?as_of_snapshot`) resolved to no live snapshot:
    /// an id the table is not (or not yet) live at, or a timestamp before the table's
    /// first snapshot. Distinct from `ControlPlane(NotFound)` (which renders 500) so this
    /// caller-forgeable case renders 404.
    #[error("no snapshot at or before the requested point: {0}")]
    AsOfNotFound(String),
}

impl QueryError {
    /// Reclassify a fault born from engine-derived (non-caller) input as an internal
    /// fault, boxing the original as the `source` so its detail survives to the log.
    #[must_use]
    pub fn into_internal(self, context: &'static str) -> QueryError {
        QueryError::Internal {
            context,
            source: Box::new(self),
        }
    }
}

pub use crate::governed::{
    GovernedType, OnMissing, Projection, identity_in_predicate, identity_is_governed, prop_ty,
    resolve_governed, resolve_hop, seed_predicates,
};
use crate::governed::{coerce_visible_predicate, load_policy};

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
///
/// `order_by` and `extra_predicate` are the pagination hooks used by `read_object_page`:
/// `order_by` is the identity column to `ORDER BY … ASC` (the callers of the plain,
/// un-paginated read pass `None`), and `extra_predicate` is the caller's already-coerced
/// keyset predicate (`identity > cursor`), trusted as-is since `read_object_page` only
/// builds it after its own fail-closed identity-visibility guards pass. Threading both
/// through here — rather than have `read_object_page` re-run projection/derived-resolution
/// itself — is what gives the paginated read the same derived (aggregate-over-link) columns
/// as the plain read.
///
/// A thin wrapper over [`compile_object_read_with`]: resolves the governance prologue once
/// via [`resolve_governed`] and hands the result over. Kept as its own function (exact
/// signature preserved) so the Flight export path and the plain, un-paginated HTTP read can
/// call it without also having to resolve governance themselves.
#[allow(
    clippy::too_many_arguments,
    reason = "governed-read compile function requires all builder parameters"
)]
pub async fn compile_object_read(
    q: &ObjectQuery,
    subject: &Subject,
    ontology: &(dyn Ontology + Send + Sync),
    acl: &(dyn Acl + Send + Sync),
    dialect: &dyn SqlDialect,
    limit: u32,
    order_by: Option<&str>,
    extra_predicate: Option<crate::filter::CallerPredicate>,
) -> Result<GovernedRead, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let g = resolve_governed(ontology, acl, &subject.0, &type_name, OnMissing::NotFound).await?;
    compile_object_read_with(
        &g,
        q,
        subject,
        ontology,
        acl,
        dialect,
        limit,
        order_by,
        extra_predicate,
    )
    .await
}

/// The compile stage of a governed object read, over an already-resolved
/// [`GovernedType`] — so a caller that needed the governance context for its own
/// guards (`read_object_page`) resolves it exactly once. `ontology`/`acl` are still
/// needed to govern derived (aggregate-over-link) columns both-ends.
#[allow(
    clippy::too_many_arguments,
    reason = "governed-read compile function requires all builder parameters"
)]
pub async fn compile_object_read_with(
    g: &GovernedType,
    q: &ObjectQuery,
    subject: &Subject,
    ontology: &(dyn Ontology + Send + Sync),
    acl: &(dyn Acl + Send + Sync),
    dialect: &dyn SqlDialect,
    limit: u32,
    order_by: Option<&str>,
    extra_predicate: Option<crate::filter::CallerPredicate>,
) -> Result<GovernedRead, QueryError> {
    let type_name = TypeName(q.type_name.clone());

    // projection: type properties minus denied, preserving property order; fail-closed.
    let mut proj = Projection::visible(g)?;

    // Caller filters + object-set ids, visibility-gated and coerced; then pagination's
    // pre-coerced keyset predicate (trusted — read_object_page built it after its own
    // fail-closed identity guards passed).
    let mut predicates = seed_predicates(g, &proj.columns, &q.filters, &q.ids)?;
    if let Some(p) = extra_predicate {
        predicates.push(p);
    }

    // OR-groups: each `_or` param is its own parenthesized disjunction. Members reuse the
    // plain-predicate visibility + coercion, so a denied/masked column inside a group fails
    // the request exactly as a denied plain filter does — an OR-group adds a combinator, not
    // a governance bypass. Groups are ANDed above; row-filters/`_ids` are never disjoined.
    let mut or_groups: Vec<Vec<crate::filter::CallerPredicate>> =
        Vec::with_capacity(q.or_raw.len());
    for raw in &q.or_raw {
        let members = crate::filter::split_or_members(raw)?;
        let mut group = Vec::with_capacity(members.len());
        for member in &members {
            let (col, val) = crate::filter::split_member(member)?;
            group.push(coerce_visible_predicate(
                col,
                val,
                &g.otype,
                &proj.columns,
                &g.masked,
            )?);
        }
        or_groups.push(group);
    }

    // Derived properties (aggregate-over-link), governed both-ends. Resolved + appended
    // after the physical projection, in declaration order; omitted (like a denied column)
    // when the subject can't read the linked type, the link/target is missing, or the
    // aggregated column is denied on the target — never an error, just absent.
    let mut derived_names: Vec<String> = Vec::new();
    let mut derived_types: Vec<String> = Vec::new();
    let mut derived_selects: Vec<crate::sql::DerivedSelect> = Vec::new();
    if !g.otype.derived.is_empty() {
        let links = ontology.links(&type_name, PageReq::unbounded()).await?;
        for d in &g.otype.derived {
            if g.denied.contains(&d.name) {
                continue;
            }
            if g.masked.contains(&d.name) {
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

    // Compile over the PHYSICAL projection (derived columns ride in derived_selects).
    let (sql, params) = compile_select_with(
        dialect,
        &g.otype.table,
        &proj.columns,
        &proj.masked,
        &crate::sql::SelectInputs {
            row_filters: &g.row_filters,
            predicates: &predicates,
            or_groups: &or_groups,
            derived: &derived_selects,
        },
        order_by,
        limit,
    )?;
    // Output columns = physical (in order) ++ surviving derived (in order).
    for (name, ty) in derived_names.into_iter().zip(derived_types) {
        let is_masked = g.masked.contains(&name);
        proj.push(name, ty, is_masked);
    }
    Ok(GovernedRead {
        sql,
        params,
        columns: proj.columns,
        logical_types: proj.logical_types,
        masked_columns: proj.masked,
    })
}

/// Resolve a time-travel selector to a concrete snapshot id for `table`, or `None`
/// when no selector was given (live read). A selector that resolves to no
/// history-backed snapshot — an as-of snapshot id absent from the table's
/// snapshot history (below, above, or never live), or a timestamp before the
/// table's first snapshot — is `AsOfNotFound` (renders 404); a genuine backend
/// fault from the catalog stays `ControlPlane` (renders 500).
async fn resolve_read_snapshot(
    deps: &QueryDeps<'_>,
    table: &control_plane_core::TableRef,
    sel: Option<&AsOfSelector>,
) -> Result<Option<control_plane_core::SnapshotId>, QueryError> {
    let Some(sel) = sel else { return Ok(None) };
    let id = match sel {
        AsOfSelector::Snapshot(id) => {
            let sid = control_plane_core::SnapshotId(*id);
            // Exact-history gate: the id must exist in the catalog's snapshot
            // history AND the table must be live at it. Unlike the previous
            // `schema()` liveness-range check this is bounded above — an id
            // past the newest snapshot 404s instead of reading live data.
            match deps.catalog.snapshot(table, sid).await {
                Ok(Some(s)) => s.id,
                Ok(None) => {
                    return Err(QueryError::AsOfNotFound(format!(
                        "{}.{} has no snapshot {}",
                        table.schema, table.name, id
                    )));
                }
                Err(e) => return Err(QueryError::ControlPlane(e)), // backend fault -> 500
            }
        }
        AsOfSelector::Time(ts) => match deps.catalog.snapshot_as_of(table, *ts).await {
            Ok(Some(s)) => s.id,
            Ok(None) => {
                return Err(QueryError::AsOfNotFound(format!(
                    "{}.{} has no snapshot at or before {ts}",
                    table.schema, table.name
                )));
            }
            Err(e) => return Err(QueryError::ControlPlane(e)), // real backend fault -> 500
        },
    };
    Ok(Some(id))
}

pub async fn read_object(
    q: &ObjectQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let g = resolve_governed(
        deps.ontology,
        deps.acl,
        &subject.0,
        &type_name,
        OnMissing::NotFound,
    )
    .await?;
    let at = resolve_read_snapshot(deps, &g.otype.table, q.as_of.as_ref()).await?;
    let gr = compile_object_read_with(
        &g,
        q,
        subject,
        deps.ontology,
        deps.acl,
        deps.serving.dialect(),
        deps.default_limit,
        None,
        None,
    )
    .await?;
    let served = deps.serving.fetch_rows(&gr.sql, &gr.params, at).await?;
    Ok(gr.into_object_rows(served))
}

/// Cap on the caller-requested page size (`?limit=`), independent of `deps.default_limit`
/// (the un-paginated path's cap). A request above this is clamped, never rejected.
pub const MAX_PAGE: u32 = 200;

/// Encode an identity cell as an opaque keyset cursor — the same total, round-trippable
/// scalar rendering `sqlvalue_to_id_string` uses (an integer id -> its digits, a string id
/// -> itself). A `SqlValue::Null` identity cannot occur for a primary key.
fn encode_id_cursor(v: &SqlValue) -> control_plane_core::Cursor {
    control_plane_core::Cursor(sqlvalue_to_id_string(v))
}

/// A governed, cursor-paginated object read: orders by the type's declared identity
/// (ascending), applies `after` as a strict `>` keyset predicate on it, fetches `limit + 1`
/// rows, and returns the (possibly truncated) page plus the cursor to fetch the next page
/// (`None` on the last page). Fails closed with `BadPagination` when: the type has no
/// declared identity; the identity column is denied or masked (never emit a cursor over an
/// ungoverned-visibility identity); the identity's logical type is not one the cursor
/// round-trips losslessly (only Integer/Long/String are — see `sqlvalue_to_id_string`);
/// `_ids` is also present (mutually exclusive with pagination); or `after` does not coerce
/// to the identity's logical type.
pub async fn read_object_page(
    q: &ObjectQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
    limit: u32,
    after: Option<control_plane_core::Cursor>,
) -> Result<(ObjectRows, Option<control_plane_core::Cursor>), QueryError> {
    if !q.ids.is_empty() {
        return Err(QueryError::BadPagination(
            "_ids and pagination are mutually exclusive".to_string(),
        ));
    }

    let type_name = TypeName(q.type_name.clone());
    let g = resolve_governed(
        deps.ontology,
        deps.acl,
        &subject.0,
        &type_name,
        OnMissing::NotFound,
    )
    .await?;
    let at = resolve_read_snapshot(deps, &g.otype.table, q.as_of.as_ref()).await?;

    let identity =
        g.otype.identity.clone().ok_or_else(|| {
            QueryError::BadPagination("type has no declared identity".to_string())
        })?;
    if g.identity_governed() {
        return Err(QueryError::BadPagination(
            "identity column not readable".to_string(),
        ));
    }

    let id_ty = prop_ty(&g.otype, &identity).unwrap_or("");

    // Fail closed: only reject identity logical types the cursor round-trips losslessly.
    // `sqlvalue_to_id_string` only encodes `SqlValue::Int` (digits) and `SqlValue::Text`
    // (verbatim) without loss; every other `SqlValue` kind falls back to `{:?}` Debug
    // formatting, which `coerce_filter` cannot decode back on the next page's `cursor=`.
    // `BaseType::Integer`/`Long` always coerce to `SqlValue::Int` (see `coerce_filter`'s
    // `JsonRepr::Number`/`NumericString` arms — Integer identities are always
    // integer-valued, and Long always parses as i64), and `BaseType::String` always
    // coerces to `SqlValue::Text`. Reject everything else (Double, Boolean, Date,
    // Timestamp, Vector, or an unrecognized type name) up front rather than emitting a
    // cursor that stalls pagination on page 2.
    let cursor_round_trips = matches!(
        control_plane_core::resolve_logical(id_ty),
        Some(
            control_plane_core::BaseType::Integer
                | control_plane_core::BaseType::Long
                | control_plane_core::BaseType::String
        )
    );
    if !cursor_round_trips {
        return Err(QueryError::BadPagination(
            "pagination unsupported for this identity type".to_string(),
        ));
    }

    // Keyset predicate: identity > cursor. A cursor that doesn't coerce to the identity's
    // logical type is a malformed cursor, not a backend fault -> BadPagination (400). Built
    // here (after every fail-closed identity guard above has passed) and handed to
    // `compile_object_read` as a trusted, already-coerced extra predicate — the same
    // governed-projection path (including derived columns) the plain read uses.
    let extra_predicate = match &after {
        Some(cursor) => {
            let bound = crate::filter::coerce_filter(&identity, id_ty, &cursor.0)
                .map_err(|e| QueryError::BadPagination(format!("invalid cursor: {e}")))?;
            Some(crate::filter::CallerPredicate {
                column: identity.clone(),
                op: control_plane_core::CompareOp::Gt,
                values: vec![bound],
            })
        }
        None => None,
    };

    let fetch_limit = limit.saturating_add(1);
    let gr = compile_object_read_with(
        &g,
        q,
        subject,
        deps.ontology,
        deps.acl,
        deps.serving.dialect(),
        fetch_limit,
        Some(&identity),
        extra_predicate,
    )
    .await?;
    let served = deps.serving.fetch_rows(&gr.sql, &gr.params, at).await?;
    debug_assert_eq!(
        served.columns, gr.columns,
        "serving engine returned columns out of the projected order"
    );

    let id_idx = gr
        .columns
        .iter()
        .position(|c| c == &identity)
        .ok_or_else(|| QueryError::BadPagination("identity column not projected".to_string()))?;

    let page = control_plane_core::Page::from_keyset(served.rows, Some(limit), |row| {
        let cell = row.get(id_idx).unwrap_or(&SqlValue::Null);
        encode_id_cursor(cell)
    });

    Ok((
        ObjectRows {
            columns: gr.columns,
            logical_types: gr.logical_types,
            rows: page.items,
        },
        page.next,
    ))
}

/// One ranked kNN hit: the identity value and its distance.
#[derive(Debug)]
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
    // No-leak: unknown type and missing Read grant are both Forbidden.
    let g = resolve_governed(
        deps.ontology,
        deps.acl,
        &subject.0,
        &type_name,
        OnMissing::Forbidden,
    )
    .await?;

    // Engine kNN over the named index (ServingError::NoIndex/DimMismatch propagate as Serving).
    let rows = deps
        .serving
        .vector_search(
            &g.otype.table,
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

    // Row-filter post-filter. Fail closed: the search response *is* a list of identity
    // values, so a policy that denies or masks the identity column must not be silently
    // disregarded. Guard runs BEFORE the empty-filter early return so both policy shapes
    // refuse identically with a deliberate 403.
    if g.identity_governed() {
        return Err(QueryError::Forbidden);
    }
    // Interim cold-hit suppression: the survivor post-filter must run for EVERY
    // identity-bearing type — not only when a row filter happens to exist — because a
    // COW inline-shadow UPDATE/DELETE leaves a stale/tombstoned vector in the cold
    // Puffin index that `merge_topk` does not suppress (#iss-search-cold-superseded-hits).
    // Identity-less types cannot be inline-shadowed (mutation requires a declared
    // identity), so they accrue no stale cold entries and keep the raw additive hits.
    if g.otype.identity.is_none() {
        return Ok(hits);
    }
    let identity = g.otype.identity.clone().ok_or_else(|| {
        // unreachable after the is_none() carve-out above; defensive. A served vector
        // type with no declared identity is server ontology/config state, not
        // caller-forgeable here — classify as internal, not a 400.
        QueryError::NoIdentity(g.otype.name.0.clone())
            .into_internal("vector-search post-filter: served vector type has no declared identity")
    })?;
    let candidate_strs: Vec<String> = hits.iter().map(|h| sqlvalue_to_id_string(&h.id)).collect();
    // The candidate ids are the ENGINE's own hit identities, not caller input; a
    // coercion failure here is server-data-integrity drift → internal, not a caller 400.
    // (The `identity_governed()` guard above already returned Forbidden for the BadFilter arm,
    // so only coercion errors can flow from here.)
    let Some(pred) = identity_in_predicate(&g.otype, &g.denied, &g.masked, &candidate_strs)
        .map_err(|e| {
            e.into_internal("vector-search post-filter: engine hit identity failed coercion")
        })?
    else {
        return Ok(hits); // no candidates to scope (empty handled above; defensive)
    };
    let limit = u32::try_from(candidate_strs.len()).unwrap_or(u32::MAX);
    let (sql, params) = compile_select_with(
        deps.serving.dialect(),
        &g.otype.table,
        std::slice::from_ref(&identity),
        &[],
        &crate::sql::SelectInputs {
            row_filters: &g.row_filters,
            predicates: std::slice::from_ref(&pred),
            ..crate::sql::SelectInputs::default()
        },
        None,
        limit,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params, None).await?;
    let surviving: std::collections::HashSet<String> = served
        .rows
        .iter()
        .filter_map(|r| r.first())
        .map(sqlvalue_to_id_string)
        .collect();
    // Keep only surviving identities, ONE hit per identity (the nearest, since `hits`
    // is distance-ordered): collapses an UPDATE's (stale-cold, fresh-hot) duplicate
    // pair to a single hit. A tombstoned identity survives in neither the merged view
    // nor `surviving`, so it is dropped. May return < k.
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    hits.retain(|h| {
        let id = sqlvalue_to_id_string(&h.id);
        surviving.contains(&id) && seen.insert(id)
    });
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
/// caller-filter coercion/visibility. Returns the per-position governance context, the
/// compiler `ChainType`s (row-filters + caller predicates), and the hop backings. Shared
/// by the object-projection read and the association read so governance lives in one
/// place.
async fn resolve_chain(
    q: &ChainQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<
    (
        Vec<GovernedType>,
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
    // Read on the source (deny-by-default, before existence is revealed).
    let source = resolve_governed(
        deps.ontology,
        deps.acl,
        &subject.0,
        &from_name,
        OnMissing::NotFound,
    )
    .await?;

    let mut ctypes: Vec<crate::sql::ChainType> = vec![crate::sql::ChainType {
        table: source.otype.table.clone(),
        row_filters: source.row_filters.clone(),
        predicates: vec![],
    }];
    let mut metas: Vec<GovernedType> = vec![source];
    let mut hops: Vec<control_plane_core::LinkBacking> = Vec::with_capacity(q.path.len());

    let mut current_name = from_name;
    for hop in &q.path {
        let (next_name, backing) = resolve_hop(deps.ontology, &current_name, hop).await?;
        // Read on every reached type (the leak-free guarantee), forward or inverse. A
        // link pointing at a missing type is an internal inconsistency, not a 404.
        let next = resolve_governed(
            deps.ontology,
            deps.acl,
            &subject.0,
            &next_name,
            OnMissing::Internal,
        )
        .await?;
        hops.push(backing);
        ctypes.push(crate::sql::ChainType {
            table: next.otype.table.clone(),
            row_filters: next.row_filters.clone(),
            predicates: vec![],
        });
        metas.push(next);
        current_name = next_name;
    }

    // Caller filters, governed per position: visibility first (denied/masked or unknown
    // column -> 400, no type-info leak), then parse the raw value into a typed predicate
    // bound at the position's alias `t_i`. The per-position visible projection is
    // computed ONCE, not per filter.
    let allowed_per_position: Vec<Vec<String>> = metas.iter().map(GovernedType::allowed).collect();
    for f in &q.filters {
        let (Some(meta), Some(allowed)) =
            (metas.get(f.position), allowed_per_position.get(f.position))
        else {
            return Err(QueryError::BadFilter(f.column.clone()));
        };
        let p = coerce_visible_predicate(&f.column, &f.raw, &meta.otype, allowed, &meta.masked)?;
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
    let proj = Projection::visible(target)?;

    let (sql, params) = compile_chain_with(
        deps.serving.dialect(),
        &ctypes,
        &hops,
        &proj.columns,
        &proj.masked,
        target.otype.identity.as_deref(),
        deps.default_limit,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params, None).await?;
    Ok(proj.into_object_rows(served))
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
    let s_allowed = source.allowed();
    if !s_allowed.contains(&source_id) || source.masked.contains(&source_id) {
        return Err(QueryError::Forbidden);
    }
    let t_allowed = target.allowed();
    if !t_allowed.contains(&target_id) || target.masked.contains(&target_id) {
        return Err(QueryError::Forbidden);
    }

    let from_id_type = prop_ty(&source.otype, &source_id)
        .map(str::to_string)
        .unwrap_or_default();
    let to_id_type = prop_ty(&target.otype, &target_id)
        .map(str::to_string)
        .unwrap_or_default();

    let (sql, params) = crate::sql::compile_chain_pairs(
        deps.serving.dialect(),
        &ctypes,
        &hops,
        &source_id,
        &target_id,
        deps.default_limit,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params, None).await?;
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
    read_graph_reach_spec(GraphReadSpec::PathCycle(q), subject, deps).await
}

/// Path-cycle compile stage: resolve + govern the cycle, compile the reach SQL.
async fn compile_reach_cycle(
    q: &GraphQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<(Projection, String, Vec<SqlValue>), QueryError> {
    let r = resolve_graph(q, subject, deps).await?;
    let (sql, params) = crate::sql::compile_graph_reach(
        deps.serving.dialect(),
        &crate::sql::ReachSpec {
            table: &r.g.otype.table,
            identity: &r.identity,
            seed_predicates: &r.seed_predicates,
            row_filters: &r.g.row_filters,
            allowed_cols: &r.proj.columns,
            mask_cols: &r.proj.masked,
            depth: q.depth,
        },
        &r.steps,
        deps.default_limit,
    )?;
    Ok((r.proj, sql, params))
}

/// One /graph reachability read, whichever recursion structure the route selected.
/// Borrowed: the HTTP layer builds the query struct and hands a reference in.
pub enum GraphReadSpec<'q> {
    /// `?path=l1,..,lk` (or the single `/graph/:link`): a path-cycle repeated to depth.
    PathCycle(&'q GraphQuery),
    /// `?links=l1,..`: a union of self-links.
    UnionSelfLinks(&'q GraphUnionQuery),
    /// `?path=l0*,l1,..`: a recursive core + relational tail.
    CoreTail(&'q GraphTailQuery),
}

/// The one /graph reachability spine: run the variant's compile stage, execute on
/// the serving engine, zip the projection onto the served rows. Governance lives in
/// the compile stages (each starts at `governed_identity` -> `resolve_governed`).
pub async fn read_graph_reach_spec(
    spec: GraphReadSpec<'_>,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let (proj, sql, params) = match spec {
        GraphReadSpec::PathCycle(q) => compile_reach_cycle(q, subject, deps).await?,
        GraphReadSpec::UnionSelfLinks(q) => compile_reach_union(q, subject, deps).await?,
        GraphReadSpec::CoreTail(q) => compile_reach_tail(q, subject, deps).await?,
    };
    let served = deps.serving.fetch_rows(&sql, &params, None).await?;
    Ok(proj.into_object_rows(served))
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
    if r.g.identity_governed() {
        return Err(QueryError::Forbidden);
    }

    let (sql, params) = crate::sql::compile_graph_tree(
        deps.serving.dialect(),
        &crate::sql::ReachSpec {
            table: &r.g.otype.table,
            identity: &r.identity,
            seed_predicates: &r.seed_predicates,
            row_filters: &r.g.row_filters,
            allowed_cols: &r.proj.columns,
            mask_cols: &r.proj.masked,
            depth: q.depth,
        },
        &r.steps,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params, None).await?;
    // Contract guard (mirrors the sibling reads): the tree projects the visible columns then
    // the three trailing tree columns, in this exact order — the positional split below relies
    // on it. A compiler/engine column-order regression trips this in tests rather than silently
    // producing garbled nodes.
    debug_assert_eq!(
        served.columns,
        {
            let mut expected = r.proj.columns.clone();
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

    let identity_type = prop_ty(&r.g.otype, &r.identity)
        .map(str::to_string)
        .unwrap_or_default();

    // Each served row is [object cells..., __depth, __parent, __id] (compile_graph_tree order).
    // Pop the three trailing columns off the end; the remainder are the object cells.
    let ncols = r.proj.columns.len();
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

    let Projection {
        columns,
        logical_types,
        ..
    } = r.proj;
    Ok(ObjectTree {
        columns,
        logical_types,
        identity_type,
        nodes,
    })
}

/// The shared prologue of every /graph read variant: Read-gate + resolve the queried
/// type (`resolve_governed`, deny-before-existence-leak) and require its declared
/// identity — the recursion's dedup key. Previously copied verbatim into
/// `resolve_graph`, the union read, and the tail read.
async fn governed_identity(
    type_name: &str,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<(GovernedType, String), QueryError> {
    let name = TypeName(type_name.to_string());
    let g = resolve_governed(
        deps.ontology,
        deps.acl,
        &subject.0,
        &name,
        OnMissing::NotFound,
    )
    .await?;
    let identity = g
        .otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(type_name.to_string()))?;
    Ok((g, identity))
}

/// Everything the two graph reads (reachable-set and shortest-path-tree) need after
/// resolving the queried type, ACL policy, and the path-cycle: the governed type (whose
/// row-filters govern the recursion start), the declared identity (the recursion's
/// dedup key), the compiler `GraphStep`s (per-intermediate governance folded in), the
/// visible/masked projection, and the coerced seed predicates.
struct GraphResolved {
    g: GovernedType,
    identity: String,
    steps: Vec<crate::sql::GraphStep>,
    proj: Projection,
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
    let (g, identity) = governed_identity(&q.type_name, subject, deps).await?;
    let type_name = TypeName(q.type_name.clone());

    // Resolve the path-cycle: walk l1..lK from the queried type. Each landed type is
    // Read-gated and its row-filters loaded (intermediate governance). After the last
    // link the type must be the queried type again (a cycle) — else it cannot repeat.
    if q.path.is_empty() {
        return Err(QueryError::NotCyclicPath(String::new()));
    }
    let mut steps: Vec<crate::sql::GraphStep> = Vec::with_capacity(q.path.len());
    let mut current = type_name.clone();
    let last = q.path.len() - 1;
    for (i, hop) in q.path.iter().enumerate() {
        let (landed, backing) = resolve_hop(deps.ontology, &current, hop).await?;
        // Read on every reached type (intermediate + final), forward or inverse. A link
        // pointing at a missing type is an internal inconsistency, not a 404.
        let landed_g = resolve_governed(
            deps.ontology,
            deps.acl,
            &subject.0,
            &landed,
            OnMissing::Internal,
        )
        .await?;
        // Intermediates carry their own row-filters; the FINAL landing is the start
        // type, whose filters are rendered at `nxt` by the compiler -> pass empty here
        // (no double-render).
        steps.push(crate::sql::GraphStep {
            backing,
            next_table: landed_g.otype.table.clone(),
            next_filters: if i == last {
                Vec::new()
            } else {
                landed_g.row_filters
            },
        });
        current = landed;
    }
    if current != type_name {
        return Err(QueryError::NotCyclicPath(hop_path_string(&q.path)));
    }

    // Projection: visible columns minus denied; masked applied. Empty -> Forbidden.
    let proj = Projection::visible(&g)?;

    // Seed predicates: source filters (visibility-checked + coerced) then the ?_ids= set.
    let seed = seed_predicates(&g, &proj.columns, &q.filters, &q.ids)?;

    Ok(GraphResolved {
        g,
        identity,
        steps,
        proj,
        seed_predicates: seed,
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
    read_graph_reach_spec(GraphReadSpec::UnionSelfLinks(q), subject, deps).await
}

/// Union-of-self-links compile stage: resolve + govern the self-link set, compile the
/// union reach SQL.
async fn compile_reach_union(
    q: &GraphUnionQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<(Projection, String, Vec<SqlValue>), QueryError> {
    let (g, identity) = governed_identity(&q.type_name, subject, deps).await?;
    let type_name = TypeName(q.type_name.clone());

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

    let proj = Projection::visible(&g)?;
    let seeds = seed_predicates(&g, &proj.columns, &q.filters, &q.ids)?;

    let (sql, params) = crate::sql::compile_graph_reach_union(
        deps.serving.dialect(),
        &crate::sql::ReachSpec {
            table: &g.otype.table,
            identity: &identity,
            seed_predicates: &seeds,
            row_filters: &g.row_filters,
            allowed_cols: &proj.columns,
            mask_cols: &proj.masked,
            depth: q.depth,
        },
        &backings,
        deps.default_limit,
    )?;
    Ok((proj, sql, params))
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
    read_graph_reach_spec(GraphReadSpec::CoreTail(q), subject, deps).await
}

/// Recursive-core + relational-tail compile stage: resolve + govern the core self-link
/// and every tail-reached type, compile the core+tail reach SQL.
async fn compile_reach_tail(
    q: &GraphTailQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<(Projection, String, Vec<SqlValue>), QueryError> {
    let (g, identity) = governed_identity(&q.type_name, subject, deps).await?;
    let type_name = TypeName(q.type_name.clone());

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

    // Resolve the forward tail. Position 0 is the queried type with EMPTY row-filters —
    // its governance lives in the recursive CTE; the tail constrains it by
    // reach-membership. Each tail-landed type is Read-gated and its row-filters loaded;
    // the final landing is projected.
    let mut tail_types: Vec<crate::sql::ChainType> = vec![crate::sql::ChainType {
        table: g.otype.table.clone(),
        row_filters: vec![],
        predicates: vec![],
    }];
    let mut tail_hops: Vec<control_plane_core::LinkBacking> =
        Vec::with_capacity(q.tail_links.len());
    let mut current = type_name.clone();
    let mut final_g = g.clone();
    for link_name in &q.tail_links {
        let (landed, backing) =
            resolve_hop(deps.ontology, &current, &Hop::from(link_name.as_str())).await?;
        let landed_g = resolve_governed(
            deps.ontology,
            deps.acl,
            &subject.0,
            &landed,
            OnMissing::Internal,
        )
        .await?;
        tail_hops.push(backing);
        tail_types.push(crate::sql::ChainType {
            table: landed_g.otype.table.clone(),
            row_filters: landed_g.row_filters.clone(),
            predicates: vec![],
        });
        final_g = landed_g;
        current = landed;
    }

    // Projection: the FINAL tail type's visible columns (masked -> marker). Empty -> Forbidden.
    let proj = Projection::visible(&final_g)?;

    // Seed predicates scope the recursion start (alias `s` in the CTE), governed by the
    // QUERIED type's projection.
    let source_allowed = g.allowed();
    let seeds = seed_predicates(&g, &source_allowed, &q.filters, &q.ids)?;

    let (sql, params) = crate::sql::compile_graph_reach_tail(
        deps.serving.dialect(),
        &crate::sql::ReachSpec {
            table: &g.otype.table,
            identity: &identity,
            seed_predicates: &seeds,
            row_filters: &g.row_filters,
            allowed_cols: &proj.columns,
            mask_cols: &proj.masked,
            depth: q.depth,
        },
        &core_backing,
        &tail_types,
        &tail_hops,
        final_g.otype.identity.as_deref(),
        deps.default_limit,
    )?;
    Ok((proj, sql, params))
}
