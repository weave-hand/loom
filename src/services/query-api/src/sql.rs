//! Compile an ACL RowFilter tree + a column projection + request equality filters
//! into a single read-only SELECT. Identifiers (table, columns) come ONLY from
//! trusted ontology/ACL metadata and are double-quoted; every caller VALUE is a
//! bound `?` parameter (never interpolated) — this is the injection boundary.

use std::fmt::Write as _;

use control_plane_core::{
    Aggregation, CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef, validate_row_filter,
};

use crate::filter::CallerPredicate;
use crate::serving::SqlValue;

/// The dialect-variant tokens of the read-SQL the compiler emits. The compiler is
/// otherwise dialect-neutral (ANSI joins/predicates); only these knobs differ across
/// the serving engines loom might target. Extend the trait only when a real dialect
/// needs a token that is currently hardcoded (e.g. an aggregate spelling).
pub trait SqlDialect: Send + Sync {
    /// Quote a (trusted, ontology/ACL-derived) identifier.
    fn quote_ident(&self, id: &str) -> String;
    /// The placeholder for the `one_based`-th bound parameter. The DataFusion engine
    /// ignores the index (`?`); a positional dialect would render `$1`, `$2`, …​.
    fn placeholder(&self, one_based: usize) -> String;
    /// The trailing row-limit clause (no leading space added by the dialect).
    fn limit_clause(&self, limit: u32) -> String;
}

/// The DataFusion serving dialect — loom's sole serving dialect.
pub struct DataFusionDialect;

impl SqlDialect for DataFusionDialect {
    fn quote_ident(&self, id: &str) -> String {
        // Escape any embedded double-quote per SQL identifier rules (`"` -> `""`)
        // rather than panicking. Identifiers come from trusted ontology/ACL
        // metadata, but an unvalidated backing column containing a `"` must still
        // render as a valid quoted identifier — not abort the request thread.
        // See iss-quote-ident-panic and [[fut-define-link-validation]].
        format!("\"{}\"", id.replace('"', "\"\""))
    }
    fn placeholder(&self, _one_based: usize) -> String {
        "?".to_string()
    }
    fn limit_clause(&self, limit: u32) -> String {
        format!("LIMIT {limit}")
    }
}

/// The value substituted for a masked column. A compile-time constant (never caller
/// data), so inlining it as a SQL literal is not an injection vector.
const MASK_MARKER: &str = "***";

/// Output column aliases for the shortest-path-tree read: the settled BFS depth, the
/// predecessor (parent) identity value (NULL for roots), and the node's own identity value.
/// The handler splits these three trailing columns off each served row.
pub const TREE_DEPTH_COL: &str = "__depth";
pub const TREE_PARENT_COL: &str = "__parent";
pub const TREE_NODE_ID_COL: &str = "__id";

/// A row filter that violated the CompareOp<->ScalarValue invariant (e.g. malformed
/// persisted policy data). Surfaced by the query API as an opaque 500, never a panic.
#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("malformed row filter: {0}")]
    MalformedFilter(String),
}

/// A column reference, optionally table-qualified. `alias` empty -> unqualified.
fn col_ref(dialect: &dyn SqlDialect, alias: &str, id: &str) -> String {
    if alias.is_empty() {
        dialect.quote_ident(id)
    } else {
        format!("{alias}.{}", dialect.quote_ident(id))
    }
}

#[expect(
    clippy::unreachable,
    reason = "validate_row_filter enforces CompareOp<->ScalarValue invariant"
)]
fn scalar(v: &ScalarValue, out: &mut Vec<SqlValue>) {
    match v {
        ScalarValue::Text(s) => out.push(SqlValue::Text(s.clone())),
        ScalarValue::Int(i) => out.push(SqlValue::Int(*i)),
        ScalarValue::Bool(b) => out.push(SqlValue::Bool(*b)),
        ScalarValue::List(_) => unreachable!("validated by validate_row_filter"),
    }
}

#[expect(
    clippy::unreachable,
    reason = "validate_row_filter enforces CompareOp<->ScalarValue invariant"
)]
fn op_sql(op: CompareOp) -> &'static str {
    match op {
        CompareOp::Eq => "=",
        CompareOp::Ne => "<>",
        CompareOp::Lt => "<",
        CompareOp::Le => "<=",
        CompareOp::Gt => ">",
        CompareOp::Ge => ">=",
        _ => unreachable!("validated by validate_row_filter"),
    }
}

/// PRECONDITION: `f` has passed `control_plane_core::validate_row_filter` (the sole
/// caller, `compile_select`, enforces this up front). The `unreachable!` arms below —
/// and those in `scalar`/`op_sql` — rely on that CompareOp<->ScalarValue invariant; a
/// caller that skips validation could turn them into a panic.
#[expect(
    clippy::unreachable,
    reason = "validate_row_filter enforces CompareOp<->ScalarValue invariant"
)]
fn filter_sql(
    dialect: &dyn SqlDialect,
    f: &RowFilter,
    alias: &str,
    params: &mut Vec<SqlValue>,
) -> String {
    match f {
        RowFilter::Compare {
            property,
            op,
            value,
        } => match op {
            CompareOp::In | CompareOp::NotIn => {
                let items = match value {
                    ScalarValue::List(xs) => xs,
                    _ => unreachable!("validated by validate_row_filter"),
                };
                let mut placeholders = Vec::with_capacity(items.len());
                for it in items {
                    scalar(it, params);
                    placeholders.push(dialect.placeholder(params.len()));
                }
                let kw = if matches!(op, CompareOp::In) {
                    "IN"
                } else {
                    "NOT IN"
                };
                format!(
                    "({} {} ({}))",
                    col_ref(dialect, alias, property),
                    kw,
                    placeholders.join(", ")
                )
            }
            CompareOp::IsNull => format!("({} IS NULL)", col_ref(dialect, alias, property)),
            CompareOp::IsNotNull => {
                format!("({} IS NOT NULL)", col_ref(dialect, alias, property))
            }
            _ => {
                scalar(value, params);
                format!(
                    "({} {} {})",
                    col_ref(dialect, alias, property),
                    op_sql(*op),
                    dialect.placeholder(params.len())
                )
            }
        },
        RowFilter::And(xs) => format!(
            "({})",
            xs.iter()
                .map(|x| filter_sql(dialect, x, alias, params))
                .collect::<Vec<_>>()
                .join(" AND ")
        ),
        RowFilter::Or(xs) => format!(
            "({})",
            xs.iter()
                .map(|x| filter_sql(dialect, x, alias, params))
                .collect::<Vec<_>>()
                .join(" OR ")
        ),
        RowFilter::Not(x) => format!("(NOT {})", filter_sql(dialect, x, alias, params)),
    }
}

/// An aggregate-over-link derived property to compile into a correlated subquery in the
/// SELECT. Owned (the handler clones the resolved link/target/filters into it). The
/// outer object table is aliased `o`; the subquery's target table is aliased `sub`.
pub struct DerivedAggregate {
    pub name: String,
    pub agg: Aggregation,
    pub backing: LinkBacking,
    pub target_table: TableRef,
    /// The linked type's ACL row-filters (both-ends governance), applied inside the
    /// subquery (aliased `sub`).
    pub target_filters: Vec<RowFilter>,
}

/// A derived-property SELECT expression: either a computed aggregate, or a masked marker
/// (the property is visible-but-masked — emit the marker, never the aggregate).
pub enum DerivedSelect {
    Masked(String),
    Aggregate(Box<DerivedAggregate>),
}

/// Build the correlated-subquery SQL for one derived aggregate, pushing its target-filter
/// params (in order) onto `params`. The outer object table is aliased `o`.
///
/// PRECONDITION: each `d.target_filters` entry has passed `validate_row_filter`
/// (`compile_select` enforces this up front) so the `filter_sql` invariant arms cannot
/// panic.
fn derived_aggregate_sql(
    dialect: &dyn SqlDialect,
    d: &DerivedAggregate,
    params: &mut Vec<SqlValue>,
) -> String {
    let target = format!(
        "{}.{}",
        dialect.quote_ident(&d.target_table.schema),
        dialect.quote_ident(&d.target_table.name)
    );
    let aggfn = match &d.agg {
        Aggregation::Count => "COUNT(*)".to_string(),
        Aggregation::Sum(c) => format!("COALESCE(SUM(sub.{}), 0)", dialect.quote_ident(c)),
        Aggregation::Avg(c) => format!("AVG(sub.{})", dialect.quote_ident(c)),
        Aggregation::Min(c) => format!("MIN(sub.{})", dialect.quote_ident(c)),
        Aggregation::Max(c) => format!("MAX(sub.{})", dialect.quote_ident(c)),
    };
    let (from_join, correlation) = match &d.backing {
        LinkBacking::ForeignKey {
            from_column,
            to_column,
        } => (
            format!("{target} sub"),
            format!(
                "sub.{} = o.{}",
                dialect.quote_ident(to_column),
                dialect.quote_ident(from_column)
            ),
        ),
        LinkBacking::JoinTable {
            table,
            from_key,
            from_column,
            to_column,
            to_key,
        } => {
            let jt = format!(
                "{}.{}",
                dialect.quote_ident(&table.schema),
                dialect.quote_ident(&table.name)
            );
            (
                format!(
                    "{target} sub JOIN {jt} j ON j.{} = sub.{}",
                    dialect.quote_ident(to_column),
                    dialect.quote_ident(to_key)
                ),
                format!(
                    "j.{} = o.{}",
                    dialect.quote_ident(from_column),
                    dialect.quote_ident(from_key)
                ),
            )
        }
    };
    let mut conjuncts = vec![correlation];
    for f in &d.target_filters {
        conjuncts.push(filter_sql(dialect, f, "sub", params));
    }
    format!(
        "(SELECT {aggfn} FROM {from_join} WHERE {}) AS {}",
        conjuncts.join(" AND "),
        dialect.quote_ident(&d.name)
    )
}

/// Render one caller predicate at `alias` (empty = unqualified), pushing its operand
/// params in conjunct order. Scalar ops use `op_sql`; set ops expand to N placeholders;
/// null ops emit no param. The column is a trusted ontology identifier (quoted), every
/// operand a bound placeholder. A violated operand-arity invariant (enforced upstream by
/// `filter::coerce_predicate`) is a `MalformedFilter` error — fail closed on this
/// ACL/caller-predicate injection-boundary path, never a panic or a placeholder bound to
/// a stale param.
fn caller_predicate_sql(
    dialect: &dyn SqlDialect,
    p: &CallerPredicate,
    alias: &str,
    params: &mut Vec<SqlValue>,
) -> Result<String, CompileError> {
    use control_plane_core::CompareOp::*;
    let col = col_ref(dialect, alias, &p.column);
    Ok(match p.op {
        In | NotIn => {
            let kw = if matches!(p.op, In) { "IN" } else { "NOT IN" };
            let mut placeholders = Vec::with_capacity(p.values.len());
            for v in &p.values {
                params.push(v.clone());
                placeholders.push(dialect.placeholder(params.len()));
            }
            format!("({col} {kw} ({}))", placeholders.join(", "))
        }
        IsNull => format!("({col} IS NULL)"),
        IsNotNull => format!("({col} IS NOT NULL)"),
        Between => {
            let [lo_v, hi_v] = p.values.as_slice() else {
                return Err(CompileError::MalformedFilter(
                    "between predicate must have two operands".to_string(),
                ));
            };
            params.push(lo_v.clone());
            let lo = dialect.placeholder(params.len());
            params.push(hi_v.clone());
            let hi = dialect.placeholder(params.len());
            format!("({col} BETWEEN {lo} AND {hi})")
        }
        Contains | StartsWith | EndsWith => {
            let [v] = p.values.as_slice() else {
                return Err(CompileError::MalformedFilter(
                    "text-pattern predicate must have one operand".to_string(),
                ));
            };
            params.push(v.clone());
            format!(
                "({col} ILIKE {} ESCAPE '\\')",
                dialect.placeholder(params.len())
            )
        }
        _ => {
            let [v] = p.values.as_slice() else {
                return Err(CompileError::MalformedFilter(
                    "scalar predicate must have one operand".to_string(),
                ));
            };
            params.push(v.clone());
            format!(
                "({col} {} {})",
                op_sql(p.op),
                dialect.placeholder(params.len())
            )
        }
    })
}

/// Masked physical-column projection exprs: a column in `mask_cols` emits the constant
/// `MASK_MARKER` aliased to the column name; otherwise it is referenced as
/// `{alias}{quoted}` — `alias` is `""` for an unaliased table, or e.g. `"p."` when
/// projecting a named result alias. Pushes no params.
fn masked_col_exprs(
    dialect: &dyn SqlDialect,
    allowed_cols: &[String],
    mask_cols: &[String],
    alias: &str,
) -> Vec<String> {
    allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                format!("'{MASK_MARKER}' AS {}", dialect.quote_ident(c))
            } else {
                format!("{alias}{}", dialect.quote_ident(c))
            }
        })
        .collect()
}

/// Validate the ACL filters a SELECT will build SQL from — outer `row_filters` and each
/// derived aggregate's `target_filters` — up front, so the building arms cannot hit a
/// `CompareOp`<->`ScalarValue` mismatch.
fn validate_select_filters(
    row_filters: &[RowFilter],
    derived: &[DerivedSelect],
) -> Result<(), CompileError> {
    for f in row_filters {
        validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
    }
    for d in derived {
        if let DerivedSelect::Aggregate(a) = d {
            for f in &a.target_filters {
                validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
            }
        }
    }
    Ok(())
}

/// SELECT-list exprs: physical (masked) columns followed by the `derived` columns.
/// Aggregate subquery params are pushed onto `params` here — i.e. BEFORE the WHERE
/// params — matching their left-to-right position in the SELECT clause.
fn select_col_exprs(
    dialect: &dyn SqlDialect,
    allowed_cols: &[String],
    mask_cols: &[String],
    derived: &[DerivedSelect],
    params: &mut Vec<SqlValue>,
) -> Vec<String> {
    let mut col_exprs = masked_col_exprs(dialect, allowed_cols, mask_cols, "");
    for d in derived {
        match d {
            DerivedSelect::Masked(name) => {
                col_exprs.push(format!("'{MASK_MARKER}' AS {}", dialect.quote_ident(name)))
            }
            DerivedSelect::Aggregate(a) => {
                col_exprs.push(derived_aggregate_sql(dialect, a, params))
            }
        }
    }
    col_exprs
}

/// WHERE conjuncts for a flat SELECT: `row_filters`, then caller `predicates`, then each
/// `or_groups` disjunction — all ANDed at the unaliased table (`""`). Params are pushed in
/// that order. An OR-group renders `(m1 OR m2 …)`, each member through `caller_predicate_sql`
/// (so members carry their own bound params); governance conjuncts stay ANDed above them.
fn select_where_conjuncts(
    dialect: &dyn SqlDialect,
    row_filters: &[RowFilter],
    predicates: &[CallerPredicate],
    or_groups: &[Vec<CallerPredicate>],
    params: &mut Vec<SqlValue>,
) -> Result<Vec<String>, CompileError> {
    let mut conjuncts: Vec<String> = Vec::new();
    for f in row_filters {
        conjuncts.push(filter_sql(dialect, f, "", params));
    }
    for p in predicates {
        conjuncts.push(caller_predicate_sql(dialect, p, "", params)?);
    }
    for group in or_groups {
        // Skip an empty group so a `pub` caller passing `vec![vec![]]` can never emit the
        // invalid `()` fragment; the handler already guarantees >=2 members per group via
        // `filter::split_or_members`, so this is defense-in-depth for the public API.
        if group.is_empty() {
            continue;
        }
        let members: Vec<String> = group
            .iter()
            .map(|m| caller_predicate_sql(dialect, m, "", params))
            .collect::<Result<_, _>>()?;
        conjuncts.push(format!("({})", members.join(" OR ")));
    }
    Ok(conjuncts)
}

/// `allowed_cols` must be non-empty (caller enforces). `row_filters` and `predicates`
/// are ANDed together as conjuncts. `derived` aggregate subqueries (if any) are appended
/// to the SELECT list; their params precede the WHERE params. The outer table is aliased
/// `o` only when at least one aggregate is present (so the no-derived output is unchanged).
#[allow(
    clippy::too_many_arguments,
    reason = "SQL compile functions require all builder parameters"
)]
pub fn compile_select_with(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    allowed_cols: &[String],
    mask_cols: &[String],
    row_filters: &[RowFilter],
    predicates: &[CallerPredicate],
    or_groups: &[Vec<CallerPredicate>],
    derived: &[DerivedSelect],
    order_by: Option<&str>,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    validate_select_filters(row_filters, derived)?;

    let mut params = Vec::new();
    let cols = select_col_exprs(dialect, allowed_cols, mask_cols, derived, &mut params).join(", ");

    let from = format!(
        "{}.{}",
        dialect.quote_ident(&table.schema),
        dialect.quote_ident(&table.name)
    );
    // The outer table needs an alias only when a correlated aggregate subquery references it.
    let has_aggregate = derived
        .iter()
        .any(|d| matches!(d, DerivedSelect::Aggregate(_)));
    let from_clause = if has_aggregate {
        format!("{from} o")
    } else {
        from
    };

    let conjuncts =
        select_where_conjuncts(dialect, row_filters, predicates, or_groups, &mut params)?;

    let mut sql = format!("SELECT {cols} FROM {from_clause}");
    if !conjuncts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conjuncts.join(" AND "));
    }
    if let Some(col) = order_by {
        let _write = write!(sql, " ORDER BY {} ASC", dialect.quote_ident(col));
    }
    let _write = write!(sql, " {}", dialect.limit_clause(limit));
    Ok((sql, params))
}

/// Compile a governed SELECT for loom's default (`DataFusionDialect`) dialect.
/// Convenience wrapper for tests; production read paths use [`compile_select_with`]
/// with the serving engine's dialect so the engine selects it.
#[allow(
    clippy::too_many_arguments,
    reason = "SQL compile functions require all builder parameters"
)]
pub fn compile_select(
    table: &TableRef,
    allowed_cols: &[String],
    mask_cols: &[String],
    row_filters: &[RowFilter],
    predicates: &[CallerPredicate],
    or_groups: &[Vec<CallerPredicate>],
    derived: &[DerivedSelect],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    compile_select_with(
        &DataFusionDialect,
        table,
        allowed_cols,
        mask_cols,
        row_filters,
        predicates,
        or_groups,
        derived,
        None,
        limit,
    )
}

/// One type in a traversal chain: its physical table, the ACL row-filters that govern
/// it, and the caller equality filters bound at this position. Every position's filters
/// are ANDed at its alias `t_i` — the chain is governed and caller-filterable at every
/// type, not just its endpoints.
pub struct ChainType {
    pub table: TableRef,
    pub row_filters: Vec<RowFilter>,
    /// Caller filter predicates for this position, bound at alias `t_i`. Position 0's
    /// predicates are the source filters (no special-case in the compiler).
    pub predicates: Vec<CallerPredicate>,
}

/// Compile a governed multi-hop traversal with an explicit SQL dialect. `types` is the
/// chain `[t_0 .. t_k]` (`t_0` = source, `t_k` = final target); `hops[i]` is the link
/// backing connecting `types[i]` (from) to `types[i+1]` (to). Only the final target is
/// projected (`allowed_cols`, `mask_cols` rendered as the marker). Each type's
/// `predicates` bind at its alias `t_i` (position 0 = source). Every type's row-filters
/// are ANDed into the WHERE.
///
/// Precondition: `types.len() == hops.len() + 1` and `hops` is non-empty (`k >= 1`).
/// As in `compile_select_with`, row filters are validated up front so the `filter_sql`
/// invariant arms cannot panic.
///
/// The compiler is **direction-agnostic**: an inverse hop is expressed entirely by the
/// caller passing that hop's `LinkBacking::reversed()` in `hops` and the link's origin
/// type in `types` — the symmetric `from_alias.from_column = to_alias.to_column` join is
/// unchanged.
/// Build the shared FROM clause (final target, then JOIN each predecessor down to the
/// source) and the per-position WHERE conjuncts (caller predicates then ACL row-filters,
/// bound at alias `t_i`) for a chain. The projection differs per caller. Row filters are
/// validated up front so `filter_sql` cannot panic.
fn chain_from_where(
    dialect: &dyn SqlDialect,
    types: &[ChainType],
    hops: &[LinkBacking],
) -> Result<(String, Vec<String>, Vec<SqlValue>), CompileError> {
    debug_assert_eq!(types.len(), hops.len() + 1, "chain types must be hops + 1");
    for t in types {
        for f in &t.row_filters {
            validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
        }
    }
    let k = hops.len();
    let alias = |i: usize| format!("t_{i}");
    let tbl = |t: &TableRef| {
        format!(
            "{}.{}",
            dialect.quote_ident(&t.schema),
            dialect.quote_ident(&t.name)
        )
    };

    let types_k = types.get(k).ok_or_else(|| {
        CompileError::MalformedFilter("chain types index out of range".to_string())
    })?;
    let mut from = format!("{} {}", tbl(&types_k.table), alias(k));
    for i in (1..=k).rev() {
        let to_alias = alias(i);
        let from_alias = alias(i - 1);
        let types_prev = types.get(i - 1).ok_or_else(|| {
            CompileError::MalformedFilter("chain types index out of range".to_string())
        })?;
        let from_tbl = tbl(&types_prev.table);
        let hop = hops.get(i - 1).ok_or_else(|| {
            CompileError::MalformedFilter("chain hops index out of range".to_string())
        })?;
        match hop {
            LinkBacking::ForeignKey {
                from_column,
                to_column,
            } => {
                let _write = write!(
                    from,
                    " JOIN {from_tbl} {from_alias} ON {from_alias}.{} = {to_alias}.{}",
                    dialect.quote_ident(from_column),
                    dialect.quote_ident(to_column),
                );
            }
            LinkBacking::JoinTable {
                table,
                from_key,
                from_column,
                to_column,
                to_key,
            } => {
                let jt = tbl(table);
                let j = format!("j{i}");
                let _write = write!(
                    from,
                    " JOIN {jt} {j} ON {j}.{} = {to_alias}.{} JOIN {from_tbl} {from_alias} ON {from_alias}.{} = {j}.{}",
                    dialect.quote_ident(to_column),
                    dialect.quote_ident(to_key),
                    dialect.quote_ident(from_key),
                    dialect.quote_ident(from_column),
                );
            }
        }
    }

    let mut params = Vec::new();
    let mut conjuncts: Vec<String> = Vec::new();
    for (i, t) in types.iter().enumerate() {
        let a = alias(i);
        for p in &t.predicates {
            conjuncts.push(caller_predicate_sql(dialect, p, &a, &mut params)?);
        }
        for f in &t.row_filters {
            conjuncts.push(filter_sql(dialect, f, &a, &mut params));
        }
    }
    Ok((from, conjuncts, params))
}

pub fn compile_chain_with(
    dialect: &dyn SqlDialect,
    types: &[ChainType],
    hops: &[LinkBacking],
    allowed_cols: &[String],
    mask_cols: &[String],
    identity: Option<&str>,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    let k = hops.len();
    let final_alias = format!("t_{k}");
    let col_exprs = masked_col_exprs(dialect, allowed_cols, mask_cols, &format!("{final_alias}."));
    let (from, conjuncts, params) = chain_from_where(dialect, types, hops)?;
    let where_sql = if conjuncts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conjuncts.join(" AND "))
    };
    let limit_clause = dialect.limit_clause(limit);
    let sql = match identity {
        // Identity-keyed dedup: partition on the RAW identity (below the masking layer) so
        // distinct objects are never collapsed when the identity column is masked/denied. The
        // raw identity appears only in PARTITION BY inside the subquery; the outer select
        // projects the masked/visible columns verbatim, so it never leaks.
        Some(id) => {
            let inner_cols = col_exprs.join(", ");
            let partition = format!("{final_alias}.{}", dialect.quote_ident(id));
            let outer_cols = allowed_cols
                .iter()
                .map(|c| dialect.quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "SELECT {outer_cols} FROM (\
                   SELECT {inner_cols}, \
                   ROW_NUMBER() OVER (PARTITION BY {partition}) AS _loom_rn \
                   FROM {from}{where_sql}\
                 ) _dedup WHERE _loom_rn = 1 {limit_clause}"
            )
        }
        // No declared identity: back-compatible DISTINCT over the visible projection.
        None => {
            let cols = col_exprs.join(", ");
            format!("SELECT DISTINCT {cols} FROM {from}{where_sql} {limit_clause}")
        }
    };
    Ok((sql, params))
}

/// Compile a governed chain that projects exactly the source (`t_0`) and final-target
/// (`t_k`) identity columns as a DISTINCT pair, through the same governed joins/filters as
/// [`compile_chain_with`]. `source_id`/`target_id` are the identity property names (=
/// physical columns) of the source and final-target types.
pub fn compile_chain_pairs(
    dialect: &dyn SqlDialect,
    types: &[ChainType],
    hops: &[LinkBacking],
    source_id: &str,
    target_id: &str,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    let k = hops.len();
    let cols = format!(
        "t_0.{}, t_{k}.{}",
        dialect.quote_ident(source_id),
        dialect.quote_ident(target_id),
    );
    let (from, conjuncts, params) = chain_from_where(dialect, types, hops)?;
    let mut sql = format!("SELECT DISTINCT {cols} FROM {from}");
    if !conjuncts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conjuncts.join(" AND "));
    }
    let _write = write!(sql, " {}", dialect.limit_clause(limit));
    Ok((sql, params))
}

/// One step of a graph path-cycle: a link's backing, the table of the type it lands on, and
/// that landed type's ACL row-filters (intermediate governance). For the FINAL step the
/// landed type is the start type; the handler passes its `next_filters` empty, since the
/// start type's `row_filters` govern the final node `nxt`.
pub struct GraphStep {
    pub backing: LinkBacking,
    pub next_table: TableRef,
    pub next_filters: Vec<RowFilter>,
}

/// Build a single link-hop JOIN fragment landing on `to_tbl`: `from_alias` -> `to_alias`.
/// The FK form is one JOIN (`from_alias.from_column = to_alias.to_column`); the join-table
/// form is two JOINs through `jt_alias`. The leading space is included so callers can
/// `push_str` it onto an accumulating FROM/JOIN string. Shared by the path-cycle and union
/// recursive-CTE compilers so the self-hop join shape lives in exactly one place.
fn link_join(
    dialect: &dyn SqlDialect,
    backing: &LinkBacking,
    from_alias: &str,
    to_alias: &str,
    to_tbl: &str,
    jt_alias: &str,
) -> String {
    let q = |id: &str| dialect.quote_ident(id);
    match backing {
        LinkBacking::ForeignKey {
            from_column,
            to_column,
        } => format!(
            " JOIN {to_tbl} {to_alias} ON {from_alias}.{} = {to_alias}.{}",
            q(from_column),
            q(to_column),
        ),
        LinkBacking::JoinTable {
            table,
            from_key,
            from_column,
            to_column,
            to_key,
        } => {
            let jtbl = format!("{}.{}", q(&table.schema), q(&table.name));
            format!(
                " JOIN {jtbl} {jt_alias} ON {from_alias}.{} = {jt_alias}.{} JOIN {to_tbl} {to_alias} ON {jt_alias}.{} = {to_alias}.{}",
                q(from_key),
                q(from_column),
                q(to_column),
                q(to_key),
            )
        }
    }
}

/// Validate the ACL filters a reachability query will build SQL from: the start
/// `row_filters` and each path step's `next_filters`.
fn validate_reach_filters(
    row_filters: &[RowFilter],
    path: &[GraphStep],
) -> Result<(), CompileError> {
    for f in row_filters {
        validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
    }
    for step in path {
        for f in &step.next_filters {
            validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
        }
    }
    Ok(())
}

/// The recursive step's landing alias for path index `i`: the final landing (`i+1 == k`)
/// is `nxt`; intermediate landings are `g1..g{k-1}`.
fn reach_to_alias(i: usize, k: usize) -> String {
    if i + 1 == k {
        "nxt".to_string()
    } else {
        format!("g{}", i + 1)
    }
}

/// Anchor (seed) WHERE at alias `s`: caller seed predicates, then the start ACL
/// row-filters. Empty when there are neither (no `WHERE` emitted).
fn reach_seed_where(
    dialect: &dyn SqlDialect,
    seed_predicates: &[CallerPredicate],
    row_filters: &[RowFilter],
    params: &mut Vec<SqlValue>,
) -> Result<String, CompileError> {
    let mut seed_conj: Vec<String> = Vec::new();
    for p in seed_predicates {
        seed_conj.push(caller_predicate_sql(dialect, p, "s", params)?);
    }
    for f in row_filters {
        seed_conj.push(filter_sql(dialect, f, "s", params));
    }
    Ok(if seed_conj.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", seed_conj.join(" AND "))
    })
}

/// Recursive-step JOINs: join `cur` through every path link to `nxt`. Intermediate landings
/// are aliased `g1..g{k-1}`; the final landing is `nxt`. A join-table step adds a per-step
/// alias — `j` for a single-step path (byte-identical to the prior single-link form), else
/// `j{i+1}`. Pushes no params.
fn reach_joins(dialect: &dyn SqlDialect, path: &[GraphStep]) -> String {
    let q = |id: &str| dialect.quote_ident(id);
    let k = path.len();
    let from_alias = |i: usize| {
        if i == 0 {
            "cur".to_string()
        } else {
            format!("g{i}")
        }
    };
    let mut joins = String::new();
    for (i, step) in path.iter().enumerate() {
        let fa = from_alias(i);
        let ta = reach_to_alias(i, k);
        let to_tbl = format!(
            "{}.{}",
            q(&step.next_table.schema),
            q(&step.next_table.name)
        );
        let jt_alias = if k == 1 {
            "j".to_string()
        } else {
            format!("j{}", i + 1)
        };
        joins.push_str(&link_join(
            dialect,
            &step.backing,
            &fa,
            &ta,
            &to_tbl,
            &jt_alias,
        ));
    }
    joins
}

/// Recursive WHERE: the depth bound, then each intermediate's filters at its alias (path
/// order), then the start `row_filters` at the final node `nxt`.
fn reach_recursive_where(
    dialect: &dyn SqlDialect,
    path: &[GraphStep],
    row_filters: &[RowFilter],
    depth: u32,
    params: &mut Vec<SqlValue>,
) -> String {
    let k = path.len();
    let mut rec_conj: Vec<String> = vec![format!("r.depth < {depth}")];
    for (i, step) in path.iter().enumerate() {
        let ta = reach_to_alias(i, k);
        for f in &step.next_filters {
            rec_conj.push(filter_sql(dialect, f, &ta, params));
        }
    }
    for f in row_filters {
        rec_conj.push(filter_sql(dialect, f, "nxt", params));
    }
    rec_conj.join(" AND ")
}

/// Projection WHERE at alias `p`: reachable in >= 1 hop, then the start `row_filters`.
/// `id` is the already-quoted identity column.
fn reach_projection_where(
    dialect: &dyn SqlDialect,
    id: &str,
    row_filters: &[RowFilter],
    params: &mut Vec<SqlValue>,
) -> String {
    let mut proj_conj: Vec<String> =
        vec![format!("p.{id} IN (SELECT id FROM reach WHERE depth >= 1)")];
    for f in row_filters {
        proj_conj.push(filter_sql(dialect, f, "p", params));
    }
    proj_conj.join(" AND ")
}

/// The shared parameter core of the reach-family compilers ([`compile_graph_reach`],
/// [`compile_graph_tree`], [`compile_graph_reach_union`], [`compile_graph_reach_tail`],
/// and the private `recursive_reach_cte`): the queried table + its declared identity
/// (the recursion's dedup key), the caller seed predicates + start-type ACL row-filters,
/// the visible/masked projection, and the inlined depth bound. What varies per compiler —
/// the recursion structure (path steps / union backings / core+tail) and the limit —
/// stays a positional parameter. For the core+tail compiler, `row_filters` are the CORE
/// (queried-type) filters and `allowed_cols`/`mask_cols` are the FINAL tail-landed type's
/// projection — see [`compile_graph_reach_tail`].
pub struct ReachSpec<'a> {
    pub table: &'a TableRef,
    pub identity: &'a str,
    pub seed_predicates: &'a [CallerPredicate],
    pub row_filters: &'a [RowFilter],
    pub allowed_cols: &'a [String],
    pub mask_cols: &'a [String],
    pub depth: u32,
}

/// Compile a depth-bounded recursive reachability query over a PATH-CYCLE: the deduped set of
/// `table` rows reachable from the seed set by repeating `path` (which starts and ends at
/// `table`) up to `depth` times. Each recursive step joins `cur` through the whole path to
/// `nxt` (both `table`), governing each intermediate landing with its `next_filters` and the
/// final node `nxt` with the start `row_filters`. A 1-step path is the single-self-link case
/// (byte-identical SQL). Termination by the inlined `depth` bound; the recursive `UNION` dedups
/// the reachable id set, and the `p` projection is keyed by `p.id IN (SELECT id FROM reach)` so it
/// is already object-unique (no projection `DISTINCT` — that would collapse masked identities).
/// Every caller value is a bound param.
pub fn compile_graph_reach(
    dialect: &dyn SqlDialect,
    spec: &ReachSpec<'_>,
    path: &[GraphStep],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    let &ReachSpec {
        table,
        identity,
        seed_predicates,
        row_filters,
        allowed_cols,
        mask_cols,
        depth,
    } = spec;
    validate_reach_filters(row_filters, path)?;

    let q = |id: &str| dialect.quote_ident(id);
    let tbl = format!("{}.{}", q(&table.schema), q(&table.name));
    let id = q(identity);
    let mut params: Vec<SqlValue> = Vec::new();

    let seed_where = reach_seed_where(dialect, seed_predicates, row_filters, &mut params)?;
    let joins = reach_joins(dialect, path);
    let rec_where = reach_recursive_where(dialect, path, row_filters, depth, &mut params);

    // Projection of `p`: visible columns (masked -> marker), reachable in >= 1 hop, governed.
    let cols = masked_col_exprs(dialect, allowed_cols, mask_cols, "p.").join(", ");
    let proj_where = reach_projection_where(dialect, &id, row_filters, &mut params);

    let limit_clause = dialect.limit_clause(limit);

    let sql = format!(
        "WITH RECURSIVE reach(id, depth) AS (\
           SELECT s.{id} AS id, 0 AS depth FROM {tbl} s{seed_where} \
           UNION \
           SELECT nxt.{id} AS id, r.depth + 1 AS depth FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}\
         ) \
         SELECT {cols} FROM {tbl} p WHERE {proj_where} {limit_clause}"
    );
    Ok((sql, params))
}

/// Compile a depth-bounded shortest-path-**tree** query over a path-cycle (a 1-step path is the
/// single-self-link case). A variant of [`compile_graph_reach`]: the recursive CTE carries the
/// predecessor node id (`reach(id, depth, pred)`) so every reach-edge records which node it was
/// reached from; a non-recursive `settled` CTE keeps exactly one row per node — minimum depth,
/// then minimum predecessor identity (`ROW_NUMBER() OVER (PARTITION BY id ORDER BY depth ASC,
/// pred ASC NULLS FIRST)` + `WHERE rn = 1`) — the shortest-path parent, deterministically. The
/// outer SELECT joins settled ids back to the table and projects the visible object columns plus
/// `__depth`/`__parent`/`__id`. Unlike reachability it **includes depth-0 roots** (a tree needs
/// its roots) and emits **no LIMIT** (the depth cap bounds the tree; a LIMIT could orphan a
/// child). Governance is threaded exactly as reachability: seed/every recursive hop/projection
/// row-filters. Param order matches [`compile_graph_reach`] (seed predicates, seed filters,
/// recursive filters, projection filters) minus the limit. The anchor's `pred` is
/// `NULLIF(s.<id>, s.<id>)` — a NULL typed as the identity column, union-compatible with the
/// recursive term's `r.id` without the compiler knowing the SQL type.
pub fn compile_graph_tree(
    dialect: &dyn SqlDialect,
    spec: &ReachSpec<'_>,
    path: &[GraphStep],
) -> Result<(String, Vec<SqlValue>), CompileError> {
    let &ReachSpec {
        table,
        identity,
        seed_predicates,
        row_filters,
        allowed_cols,
        mask_cols,
        depth,
    } = spec;
    validate_reach_filters(row_filters, path)?;

    let q = |id: &str| dialect.quote_ident(id);
    let tbl = format!("{}.{}", q(&table.schema), q(&table.name));
    let id = q(identity);
    let mut params: Vec<SqlValue> = Vec::new();

    let seed_where = reach_seed_where(dialect, seed_predicates, row_filters, &mut params)?;
    let joins = reach_joins(dialect, path);
    let rec_where = reach_recursive_where(dialect, path, row_filters, depth, &mut params);

    // Projection of `p`: visible columns (masked -> marker), governed by the start row-filters.
    let cols = masked_col_exprs(dialect, allowed_cols, mask_cols, "p.").join(", ");
    let mut proj_conj: Vec<String> = Vec::new();
    for f in row_filters {
        proj_conj.push(filter_sql(dialect, f, "p", &mut params));
    }
    let proj_and = if proj_conj.is_empty() {
        String::new()
    } else {
        format!(" AND {}", proj_conj.join(" AND "))
    };

    let depth_col = TREE_DEPTH_COL;
    let parent_col = TREE_PARENT_COL;
    let node_id_col = TREE_NODE_ID_COL;

    let sql = format!(
        "WITH RECURSIVE reach(id, depth, pred) AS (\
           SELECT s.{id} AS id, 0 AS depth, NULLIF(s.{id}, s.{id}) AS pred FROM {tbl} s{seed_where} \
           UNION \
           SELECT nxt.{id} AS id, r.depth + 1 AS depth, r.id AS pred FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}\
         ), \
         settled AS (\
           SELECT id, depth, pred, ROW_NUMBER() OVER (PARTITION BY id ORDER BY depth ASC, pred ASC NULLS FIRST) AS rn FROM reach\
         ) \
         SELECT {cols}, t.depth AS {depth_col}, t.pred AS {parent_col}, p.{id} AS {node_id_col} \
         FROM settled t JOIN {tbl} p ON p.{id} = t.id WHERE t.rn = 1{proj_and} \
         ORDER BY t.depth ASC, p.{id} ASC"
    );
    Ok((sql, params))
}

/// Compile a depth-bounded recursive reachability query over a UNION of self-links: the deduped
/// set of `table` rows reachable from the seed set by repeatedly following ANY ONE of `backings`
/// (each a self-link on `table`) up to `depth` times.
///
/// The recursive CTE has a single recursive self-reference to avoid a "Circular reference to CTE"
/// planner error that occurs when multiple arms each reference the CTE name. Instead, all edge
/// arms are collapsed into a non-recursive `(from_id, to_id)` subquery joined in one step:
///
/// ```sql
/// WITH RECURSIVE reach(id, depth) AS (
///   SELECT s.id AS id, 0 AS depth FROM tbl s WHERE {seed}
///   UNION
///   SELECT e.to_id AS id, r.depth + 1 AS depth
///   FROM reach r
///   JOIN (arm0 UNION ALL arm1 ...) e ON r.id = e.from_id
///   JOIN tbl nxt ON e.to_id = nxt.id
///   WHERE r.depth < {depth} AND {row_filters_at_nxt}
/// )
/// SELECT {cols} FROM tbl p WHERE p.id IN (SELECT id FROM reach WHERE depth >= 1)
///   AND {row_filters_at_p} LIMIT {limit}
/// ```
///
/// Param order: seed predicates, seed row-filters (`s`), recursive row-filters (`nxt`, ONE set
/// shared across all arms), projection row-filters (`p`). `backings` is non-empty (enforced by
/// caller). Every caller value is a bound param.
pub fn compile_graph_reach_union(
    dialect: &dyn SqlDialect,
    spec: &ReachSpec<'_>,
    backings: &[LinkBacking],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    let &ReachSpec {
        table,
        identity,
        seed_predicates,
        row_filters,
        allowed_cols,
        mask_cols,
        depth,
    } = spec;
    validate_reach_filters(row_filters, &[])?;
    let q = |id: &str| dialect.quote_ident(id);
    let tbl = format!("{}.{}", q(&table.schema), q(&table.name));
    let id = q(identity);
    let mut params: Vec<SqlValue> = Vec::new();

    let seed_where = reach_seed_where(dialect, seed_predicates, row_filters, &mut params)?;

    // Edge subquery: each backing contributes one non-recursive arm that emits (from_id, to_id)
    // pairs. Arms are joined with UNION ALL (duplicates acceptable here; the outer CTE dedupes).
    // The join-table alias `j{i}` is per-arm so multiple join-table links never collide.
    let edge_arms: Vec<String> = backings
        .iter()
        .enumerate()
        .map(|(i, backing)| {
            let jt_alias = format!("j{i}");
            let joins = link_join(dialect, backing, "cur", "nxt", &tbl, &jt_alias);
            format!("SELECT cur.{id} AS from_id, nxt.{id} AS to_id FROM {tbl} cur{joins}")
        })
        .collect();
    let edges_sql = edge_arms.join(" UNION ALL ");

    // Recursive step: single join of `reach r` to the edge subquery, then to `nxt` for filter.
    // Row-filters are applied at `nxt` (the landing node). This single `reach` reference avoids
    // a "Circular reference to CTE" planner error that arises from multiple arms each referencing
    // the CTE name. The empty path renders exactly the depth bound + row-filters at `nxt`.
    let rec_where = reach_recursive_where(dialect, &[], row_filters, depth, &mut params);
    let recursive = format!(
        "SELECT e.to_id AS id, r.depth + 1 AS depth FROM reach r JOIN ({edges_sql}) e ON r.id = e.from_id JOIN {tbl} nxt ON e.to_id = nxt.{id} WHERE {rec_where}"
    );

    // Projection of `p`: visible columns (masked -> marker), reachable in >= 1 hop, governed.
    let cols = masked_col_exprs(dialect, allowed_cols, mask_cols, "p.").join(", ");
    let proj_where = reach_projection_where(dialect, &id, row_filters, &mut params);

    let limit_clause = dialect.limit_clause(limit);

    let sql = format!(
        "WITH RECURSIVE reach(id, depth) AS (\
           SELECT s.{id} AS id, 0 AS depth FROM {tbl} s{seed_where} \
           UNION \
           {recursive}\
         ) \
         SELECT {cols} FROM {tbl} p WHERE {proj_where} {limit_clause}"
    );
    Ok((sql, params))
}

/// Build the single-self-link recursive reachability CTE (`WITH RECURSIVE reach(id, depth) AS
/// (…)`) used by the recursive-core + relational-tail compiler. The emitted CTE is the
/// degenerate 1-step case of [`compile_graph_reach`]'s path-cycle CTE: a seed anchor governed by
/// `seed_predicates` + `row_filters` at alias `s`, a distinct `UNION` (not `UNION ALL`) to
/// deduplicate across iterations, and a single recursive step following `backing` (the self-link,
/// via the shared [`link_join`]) with `row_filters` applied at the landing node `nxt` under the
/// inlined `r.depth < depth` bound. The `UNION`'s deduplication ensures recursion terminates and
/// stays cycle-safe on cyclic self-links. Seed then recursive params are appended to `params` in
/// that order. This is a focused helper, NOT a refactor of `compile_graph_reach` (whose CTE
/// generalizes over a multi-link path); the shared surface is the self-hop join, which already
/// lives in `link_join`.
fn recursive_reach_cte(
    dialect: &dyn SqlDialect,
    spec: &ReachSpec<'_>,
    backing: &LinkBacking,
    params: &mut Vec<SqlValue>,
) -> Result<String, CompileError> {
    let &ReachSpec {
        table,
        identity,
        seed_predicates,
        row_filters,
        depth,
        ..
    } = spec;
    validate_reach_filters(row_filters, &[])?;
    let q = |id: &str| dialect.quote_ident(id);
    let tbl = format!("{}.{}", q(&table.schema), q(&table.name));
    let id = q(identity);

    let seed_where = reach_seed_where(dialect, seed_predicates, row_filters, params)?;

    let joins = link_join(dialect, backing, "cur", "nxt", &tbl, "j");

    let rec_where = reach_recursive_where(dialect, &[], row_filters, depth, params);

    Ok(format!(
        "WITH RECURSIVE reach(id, depth) AS (\
           SELECT s.{id} AS id, 0 AS depth FROM {tbl} s{seed_where} \
           UNION \
           SELECT nxt.{id} AS id, r.depth + 1 AS depth FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}\
         )"
    ))
}

/// Compile a depth-bounded recursive-core + relational-tail reachability read: from the seed set,
/// follow `core_backing` (a self-link on `table`) 1..`depth` times to a reachable set, then chain
/// `tail_hops` forward off that set (`tail_types[0]` = `table`, `tail_types[k]` = the projected
/// final type) and project the final type's columns, deduped per object. When `final_identity` is
/// `Some`, the dedup partitions on the final type's RAW identity via a windowed `ROW_NUMBER()`
/// (below the masking layer, so a masked/denied identity cannot collapse distinct objects); when
/// `None`, it falls back to `SELECT DISTINCT` over the visible projection. The recursive core is
/// the [`recursive_reach_cte`]; the tail is the shared [`chain_from_where`]; the two are glued by
/// `t_0.{identity} IN (SELECT id FROM reach WHERE depth >= 1)` (the depth>=1 reachable set,
/// excluding the seed unless a cycle re-reaches it). `tail_types[0]` MUST carry empty row-filters:
/// the queried type's governance lives in the CTE (`core_row_filters`), so re-applying at `t_0`
/// would only duplicate params. Param order: seed predicates, seed `core_row_filters` (s),
/// recursive `core_row_filters` (nxt), then the tail's per-position params. Precondition:
/// `tail_types.len() == tail_hops.len() + 1` and `tail_hops` non-empty. Every caller value is a
/// bound param.
pub fn compile_graph_reach_tail(
    dialect: &dyn SqlDialect,
    spec: &ReachSpec<'_>,
    core_backing: &LinkBacking,
    tail_types: &[ChainType],
    tail_hops: &[LinkBacking],
    final_identity: Option<&str>,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    let &ReachSpec {
        identity,
        allowed_cols,
        mask_cols,
        ..
    } = spec;
    debug_assert_eq!(
        tail_types.len(),
        tail_hops.len() + 1,
        "tail types must be hops + 1"
    );
    debug_assert!(!tail_hops.is_empty(), "part-B tail must have >= 1 hop");
    debug_assert!(
        tail_types
            .first()
            .is_some_and(|t| t.row_filters.is_empty() && t.predicates.is_empty()),
        "tail_types[0] must carry empty row-filters and predicates: governance lives in the CTE"
    );
    let q = |id: &str| dialect.quote_ident(id);
    let id = q(identity);
    let mut params: Vec<SqlValue> = Vec::new();

    // Recursive core CTE first (the CTE is textually first, so its `?` placeholders bind first).
    let cte = recursive_reach_cte(dialect, spec, core_backing, &mut params)?;

    // Relational tail: t_0 = `table` (the reachable set), chained forward to the final type t_k.
    let (from, conjuncts, tail_params) = chain_from_where(dialect, tail_types, tail_hops)?;
    params.extend(tail_params);

    let k = tail_hops.len();
    let final_alias = format!("t_{k}");
    let col_exprs = masked_col_exprs(dialect, allowed_cols, mask_cols, &format!("{final_alias}."));

    // Glue: t_0 (the queried type at the tail's source) is constrained to the depth>=1 reach set.
    let mut where_conj = vec![format!(
        "t_0.{id} IN (SELECT id FROM reach WHERE depth >= 1)"
    )];
    where_conj.extend(conjuncts);
    let where_sql = where_conj.join(" AND ");

    let limit_clause = dialect.limit_clause(limit);

    let sql = match final_identity {
        // Identity-keyed dedup on the RAW final-target identity, below the masking layer (same
        // shape as `compile_chain_with`). NB: `fid` shadows nothing risky — the outer `id`
        // (core self-link PK) is already baked into `where_sql` above; `fid` is used only for the
        // final-target PARTITION BY.
        Some(fid) => {
            let inner_cols = col_exprs.join(", ");
            let partition = format!("{final_alias}.{}", q(fid));
            let outer_cols = allowed_cols
                .iter()
                .map(|c| q(c))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "{cte} SELECT {outer_cols} FROM (\
                   SELECT {inner_cols}, \
                   ROW_NUMBER() OVER (PARTITION BY {partition}) AS _loom_rn \
                   FROM {from} WHERE {where_sql}\
                 ) _dedup WHERE _loom_rn = 1 {limit_clause}"
            )
        }
        // No declared final identity: back-compatible DISTINCT over the visible projection.
        None => {
            let cols = col_exprs.join(", ");
            format!("{cte} SELECT DISTINCT {cols} FROM {from} WHERE {where_sql} {limit_clause}")
        }
    };
    Ok((sql, params))
}

/// Compile a governed multi-hop traversal for loom's default (`DataFusionDialect`)
/// dialect. Convenience wrapper for tests; production paths use [`compile_chain_with`]
/// with the serving engine's dialect.
pub fn compile_chain(
    types: &[ChainType],
    hops: &[LinkBacking],
    allowed_cols: &[String],
    mask_cols: &[String],
    identity: Option<&str>,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    compile_chain_with(
        &DataFusionDialect,
        types,
        hops,
        allowed_cols,
        mask_cols,
        identity,
        limit,
    )
}
