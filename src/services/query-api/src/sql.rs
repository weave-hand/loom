//! Compile an ACL RowFilter tree + a column projection + request equality filters
//! into a single read-only SELECT. Identifiers (table, columns) come ONLY from
//! trusted ontology/ACL metadata and are double-quoted; every caller VALUE is a
//! bound `?` parameter (never interpolated) — this is the injection boundary.

use control_plane_core::{
    Aggregation, CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef, validate_row_filter,
};

use crate::filter::CallerPredicate;
use crate::serving::SqlValue;

/// The dialect-variant tokens of the read-SQL the compiler emits. The compiler is
/// otherwise dialect-neutral (ANSI joins/predicates); only these three knobs differ
/// across the serving engines loom might target. Extend the trait only when a real
/// dialect needs a token that is currently hardcoded (e.g. an aggregate spelling).
pub trait SqlDialect: Send + Sync {
    /// Quote a (trusted, ontology/ACL-derived) identifier.
    fn quote_ident(&self, id: &str) -> String;
    /// The placeholder for the `one_based`-th bound parameter. DuckDB ignores the
    /// index (`?`); a positional dialect would render `$1`, `$2`, …​.
    fn placeholder(&self, one_based: usize) -> String;
    /// The trailing row-limit clause (no leading space added by the dialect).
    fn limit_clause(&self, limit: u32) -> String;
}

/// The DuckDB dialect — loom's only serving dialect today.
pub struct DuckDbDialect;

impl SqlDialect for DuckDbDialect {
    fn quote_ident(&self, id: &str) -> String {
        assert!(
            !id.contains('"'),
            "identifier must not contain a double quote: {id}"
        );
        format!("\"{id}\"")
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

fn scalar(v: &ScalarValue, out: &mut Vec<SqlValue>) {
    match v {
        ScalarValue::Text(s) => out.push(SqlValue::Text(s.clone())),
        ScalarValue::Int(i) => out.push(SqlValue::Int(*i)),
        ScalarValue::Bool(b) => out.push(SqlValue::Bool(*b)),
        ScalarValue::List(_) => unreachable!("validated by validate_row_filter"),
    }
}

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
/// operand a bound placeholder.
fn caller_predicate_sql(
    dialect: &dyn SqlDialect,
    p: &CallerPredicate,
    alias: &str,
    params: &mut Vec<SqlValue>,
) -> String {
    use control_plane_core::CompareOp::*;
    let col = col_ref(dialect, alias, &p.column);
    match p.op {
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
        _ => {
            debug_assert_eq!(p.values.len(), 1, "scalar predicate must have one operand");
            params.push(p.values[0].clone());
            format!(
                "({col} {} {})",
                op_sql(p.op),
                dialect.placeholder(params.len())
            )
        }
    }
}

/// `allowed_cols` must be non-empty (caller enforces). `row_filters` and `predicates`
/// are ANDed together as conjuncts. `derived` aggregate subqueries (if any) are appended
/// to the SELECT list; their params precede the WHERE params. The outer table is aliased
/// `o` only when at least one aggregate is present (so the no-derived output is unchanged).
#[allow(clippy::too_many_arguments)]
pub fn compile_select_with(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    allowed_cols: &[String],
    mask_cols: &[String],
    row_filters: &[RowFilter],
    predicates: &[CallerPredicate],
    derived: &[DerivedSelect],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    // Validate each ACL filter's shape up front — outer row filters and each derived
    // aggregate's target filters — so the SQL-building arms below cannot hit a
    // CompareOp<->ScalarValue mismatch.
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

    let mut params = Vec::new();
    // Physical columns (no params).
    let mut col_exprs: Vec<String> = allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                // Masked: emit the constant marker, never the column's value.
                format!("'{MASK_MARKER}' AS {}", dialect.quote_ident(c))
            } else {
                dialect.quote_ident(c)
            }
        })
        .collect();
    // Derived columns. Aggregate subquery params are pushed here — i.e. BEFORE the WHERE
    // params below — matching their left-to-right position in the SELECT clause.
    let has_aggregate = derived
        .iter()
        .any(|d| matches!(d, DerivedSelect::Aggregate(_)));
    for d in derived {
        match d {
            DerivedSelect::Masked(name) => {
                col_exprs.push(format!("'{MASK_MARKER}' AS {}", dialect.quote_ident(name)))
            }
            DerivedSelect::Aggregate(a) => {
                col_exprs.push(derived_aggregate_sql(dialect, a, &mut params))
            }
        }
    }
    let cols = col_exprs.join(", ");

    let from = format!(
        "{}.{}",
        dialect.quote_ident(&table.schema),
        dialect.quote_ident(&table.name)
    );
    // The outer table needs an alias only when a correlated subquery references it.
    let from_clause = if has_aggregate {
        format!("{from} o")
    } else {
        from
    };

    let mut conjuncts: Vec<String> = Vec::new();
    for f in row_filters {
        conjuncts.push(filter_sql(dialect, f, "", &mut params));
    }
    for p in predicates {
        conjuncts.push(caller_predicate_sql(dialect, p, "", &mut params));
    }

    let mut sql = format!("SELECT {cols} FROM {from_clause}");
    if !conjuncts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conjuncts.join(" AND "));
    }
    sql.push_str(&format!(" {}", dialect.limit_clause(limit)));
    Ok((sql, params))
}

/// Compile a governed SELECT for loom's default (DuckDB) dialect. Convenience wrapper
/// for statically-DuckDB callers (e.g. tests); production read paths must use
/// [`compile_select_with`] with the serving engine's dialect so the engine selects it.
#[allow(clippy::too_many_arguments)]
pub fn compile_select(
    table: &TableRef,
    allowed_cols: &[String],
    mask_cols: &[String],
    row_filters: &[RowFilter],
    predicates: &[CallerPredicate],
    derived: &[DerivedSelect],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    compile_select_with(
        &DuckDbDialect,
        table,
        allowed_cols,
        mask_cols,
        row_filters,
        predicates,
        derived,
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

    let mut from = format!("{} {}", tbl(&types[k].table), alias(k));
    for i in (1..=k).rev() {
        let to_alias = alias(i);
        let from_alias = alias(i - 1);
        let from_tbl = tbl(&types[i - 1].table);
        match &hops[i - 1] {
            LinkBacking::ForeignKey {
                from_column,
                to_column,
            } => {
                from.push_str(&format!(
                    " JOIN {from_tbl} {from_alias} ON {from_alias}.{} = {to_alias}.{}",
                    dialect.quote_ident(from_column),
                    dialect.quote_ident(to_column),
                ));
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
                from.push_str(&format!(
                    " JOIN {jt} {j} ON {j}.{} = {to_alias}.{} JOIN {from_tbl} {from_alias} ON {from_alias}.{} = {j}.{}",
                    dialect.quote_ident(to_column),
                    dialect.quote_ident(to_key),
                    dialect.quote_ident(from_key),
                    dialect.quote_ident(from_column),
                ));
            }
        }
    }

    let mut params = Vec::new();
    let mut conjuncts: Vec<String> = Vec::new();
    for (i, t) in types.iter().enumerate() {
        let a = alias(i);
        for p in &t.predicates {
            conjuncts.push(caller_predicate_sql(dialect, p, &a, &mut params));
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
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    let k = hops.len();
    let final_alias = format!("t_{k}");
    let cols = allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                format!("'{MASK_MARKER}' AS {}", dialect.quote_ident(c))
            } else {
                format!("{final_alias}.{}", dialect.quote_ident(c))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let (from, conjuncts, params) = chain_from_where(dialect, types, hops)?;
    let mut sql = format!("SELECT DISTINCT {cols} FROM {from}");
    if !conjuncts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conjuncts.join(" AND "));
    }
    sql.push_str(&format!(" {}", dialect.limit_clause(limit)));
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
    sql.push_str(&format!(" {}", dialect.limit_clause(limit)));
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

/// Compile a depth-bounded recursive reachability query over a PATH-CYCLE: the deduped set of
/// `table` rows reachable from the seed set by repeating `path` (which starts and ends at
/// `table`) up to `depth` times. Each recursive step joins `cur` through the whole path to
/// `nxt` (both `table`), governing each intermediate landing with its `next_filters` and the
/// final node `nxt` with the start `row_filters`. A 1-step path is the single-self-link case
/// (byte-identical SQL). Termination by the inlined `depth` bound; `DISTINCT` dedups. Every
/// caller value is a bound param.
#[allow(clippy::too_many_arguments)]
pub fn compile_graph_reach(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    identity: &str,
    path: &[GraphStep],
    seed_predicates: &[CallerPredicate],
    row_filters: &[RowFilter],
    allowed_cols: &[String],
    mask_cols: &[String],
    depth: u32,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    for f in row_filters {
        validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
    }
    for step in path {
        for f in &step.next_filters {
            validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
        }
    }
    let q = |id: &str| dialect.quote_ident(id);
    let tbl = format!("{}.{}", q(&table.schema), q(&table.name));
    let id = q(identity);
    let mut params: Vec<SqlValue> = Vec::new();

    // Anchor (seed) WHERE at alias `s`: caller seed predicates, then the start ACL row-filters.
    let mut seed_conj: Vec<String> = Vec::new();
    for p in seed_predicates {
        seed_conj.push(caller_predicate_sql(dialect, p, "s", &mut params));
    }
    for f in row_filters {
        seed_conj.push(filter_sql(dialect, f, "s", &mut params));
    }
    let seed_where = if seed_conj.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", seed_conj.join(" AND "))
    };

    // Recursive step: join `cur` through every path link to `nxt`. Intermediate landings are
    // aliased g1..g{K-1}; the final landing is `nxt`. A join-table step adds a per-step `j{n}`.
    let k = path.len();
    let from_alias = |i: usize| {
        if i == 0 {
            "cur".to_string()
        } else {
            format!("g{i}")
        }
    };
    let to_alias = |i: usize| {
        if i + 1 == k {
            "nxt".to_string()
        } else {
            format!("g{}", i + 1)
        }
    };
    let mut joins = String::new();
    for (i, step) in path.iter().enumerate() {
        let fa = from_alias(i);
        let ta = to_alias(i);
        let to_tbl = format!(
            "{}.{}",
            q(&step.next_table.schema),
            q(&step.next_table.name)
        );
        // For a single-step path use join-table alias `j` (byte-identical to the prior
        // single-link form); multi-step paths index the alias `j{i+1}` to avoid collisions.
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

    // Recursive WHERE: depth bound, each intermediate's filters at its alias (path order),
    // then the start row_filters at the final node `nxt`.
    let mut rec_conj: Vec<String> = vec![format!("r.depth < {depth}")];
    for (i, step) in path.iter().enumerate() {
        let ta = to_alias(i);
        for f in &step.next_filters {
            rec_conj.push(filter_sql(dialect, f, &ta, &mut params));
        }
    }
    for f in row_filters {
        rec_conj.push(filter_sql(dialect, f, "nxt", &mut params));
    }
    let rec_where = rec_conj.join(" AND ");

    // Projection of `p`: visible columns (masked -> marker), reachable in >= 1 hop, governed.
    let cols = allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                format!("'{MASK_MARKER}' AS {}", q(c))
            } else {
                format!("p.{}", q(c))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut proj_conj: Vec<String> =
        vec![format!("p.{id} IN (SELECT id FROM reach WHERE depth >= 1)")];
    for f in row_filters {
        proj_conj.push(filter_sql(dialect, f, "p", &mut params));
    }
    let proj_where = proj_conj.join(" AND ");

    let sql = format!(
        "WITH RECURSIVE reach(id, depth) AS (\
           SELECT s.{id}, 0 FROM {tbl} s{seed_where} \
           UNION \
           SELECT nxt.{id}, r.depth + 1 FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}\
         ) \
         SELECT DISTINCT {cols} FROM {tbl} p WHERE {proj_where} {}",
        dialect.limit_clause(limit)
    );
    Ok((sql, params))
}

/// Compile a depth-bounded recursive reachability query over a UNION of self-links: the deduped
/// set of `table` rows reachable from the seed set by repeatedly following ANY ONE of `backings`
/// (each a self-link on `table`) up to `depth` times.
///
/// The recursive CTE has a single recursive self-reference to avoid DuckDB's "Circular reference
/// to CTE" error that occurs when multiple arms each reference the CTE name. Instead, all edge
/// arms are collapsed into a non-recursive `(from_id, to_id)` subquery joined in one step:
///
/// ```sql
/// WITH RECURSIVE reach(id, depth) AS (
///   SELECT s.id, 0 FROM tbl s WHERE {seed}
///   UNION
///   SELECT e.to_id, r.depth + 1
///   FROM reach r
///   JOIN (arm0 UNION ALL arm1 ...) e ON r.id = e.from_id
///   JOIN tbl nxt ON e.to_id = nxt.id
///   WHERE r.depth < {depth} AND {row_filters_at_nxt}
/// )
/// SELECT DISTINCT {cols} FROM tbl p WHERE p.id IN (SELECT id FROM reach WHERE depth >= 1)
///   AND {row_filters_at_p} LIMIT {limit}
/// ```
///
/// Param order: seed predicates, seed row-filters (`s`), recursive row-filters (`nxt`, ONE set
/// shared across all arms), projection row-filters (`p`). `backings` is non-empty (enforced by
/// caller). Every caller value is a bound param.
#[allow(clippy::too_many_arguments)]
pub fn compile_graph_reach_union(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    identity: &str,
    backings: &[LinkBacking],
    seed_predicates: &[CallerPredicate],
    row_filters: &[RowFilter],
    allowed_cols: &[String],
    mask_cols: &[String],
    depth: u32,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    for f in row_filters {
        validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
    }
    let q = |id: &str| dialect.quote_ident(id);
    let tbl = format!("{}.{}", q(&table.schema), q(&table.name));
    let id = q(identity);
    let mut params: Vec<SqlValue> = Vec::new();

    // Anchor (seed) WHERE at alias `s`: caller seed predicates, then the start ACL row-filters.
    let mut seed_conj: Vec<String> = Vec::new();
    for p in seed_predicates {
        seed_conj.push(caller_predicate_sql(dialect, p, "s", &mut params));
    }
    for f in row_filters {
        seed_conj.push(filter_sql(dialect, f, "s", &mut params));
    }
    let seed_where = if seed_conj.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", seed_conj.join(" AND "))
    };

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
    // DuckDB's "Circular reference to CTE" error that arises from multiple arms each referencing
    // the CTE name.
    let mut rec_conj: Vec<String> = vec![format!("r.depth < {depth}")];
    for f in row_filters {
        rec_conj.push(filter_sql(dialect, f, "nxt", &mut params));
    }
    let rec_where = rec_conj.join(" AND ");
    let recursive = format!(
        "SELECT e.to_id, r.depth + 1 FROM reach r JOIN ({edges_sql}) e ON r.id = e.from_id JOIN {tbl} nxt ON e.to_id = nxt.{id} WHERE {rec_where}"
    );

    // Projection of `p`: visible columns (masked -> marker), reachable in >= 1 hop, governed.
    let cols = allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                format!("'{MASK_MARKER}' AS {}", q(c))
            } else {
                format!("p.{}", q(c))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut proj_conj: Vec<String> =
        vec![format!("p.{id} IN (SELECT id FROM reach WHERE depth >= 1)")];
    for f in row_filters {
        proj_conj.push(filter_sql(dialect, f, "p", &mut params));
    }
    let proj_where = proj_conj.join(" AND ");

    let sql = format!(
        "WITH RECURSIVE reach(id, depth) AS (\
           SELECT s.{id}, 0 FROM {tbl} s{seed_where} \
           UNION \
           {recursive}\
         ) \
         SELECT DISTINCT {cols} FROM {tbl} p WHERE {proj_where} {}",
        dialect.limit_clause(limit)
    );
    Ok((sql, params))
}

/// Build the single-self-link recursive reachability CTE (`WITH RECURSIVE reach(id, depth) AS
/// (…)`) used by the recursive-core + relational-tail compiler. The emitted CTE is the
/// degenerate 1-step case of [`compile_graph_reach`]'s path-cycle CTE: a seed anchor governed by
/// `seed_predicates` + `row_filters` at alias `s`, a distinct `UNION`, and a single recursive
/// step following `backing` (the self-link, via the shared [`link_join`]) with `row_filters`
/// applied at the landing node `nxt` under the inlined `r.depth < depth` bound. Seed then
/// recursive params are appended to `params` in that order. This is a focused helper, NOT a
/// refactor of `compile_graph_reach` (whose CTE generalizes over a multi-link path); the shared
/// surface is the self-hop join, which already lives in `link_join`.
#[allow(clippy::too_many_arguments)]
fn recursive_reach_cte(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    identity: &str,
    backing: &LinkBacking,
    seed_predicates: &[CallerPredicate],
    row_filters: &[RowFilter],
    depth: u32,
    params: &mut Vec<SqlValue>,
) -> Result<String, CompileError> {
    for f in row_filters {
        validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
    }
    let q = |id: &str| dialect.quote_ident(id);
    let tbl = format!("{}.{}", q(&table.schema), q(&table.name));
    let id = q(identity);

    let mut seed_conj: Vec<String> = Vec::new();
    for p in seed_predicates {
        seed_conj.push(caller_predicate_sql(dialect, p, "s", params));
    }
    for f in row_filters {
        seed_conj.push(filter_sql(dialect, f, "s", params));
    }
    let seed_where = if seed_conj.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", seed_conj.join(" AND "))
    };

    let joins = link_join(dialect, backing, "cur", "nxt", &tbl, "j");

    let mut rec_conj: Vec<String> = vec![format!("r.depth < {depth}")];
    for f in row_filters {
        rec_conj.push(filter_sql(dialect, f, "nxt", params));
    }
    let rec_where = rec_conj.join(" AND ");

    Ok(format!(
        "WITH RECURSIVE reach(id, depth) AS (\
           SELECT s.{id}, 0 FROM {tbl} s{seed_where} \
           UNION \
           SELECT nxt.{id}, r.depth + 1 FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}\
         )"
    ))
}

/// Compile a depth-bounded recursive-core + relational-tail reachability read: from the seed set,
/// follow `core_backing` (a self-link on `table`) 1..`depth` times to a reachable set, then chain
/// `tail_hops` forward off that set (`tail_types[0]` = `table`, `tail_types[k]` = the projected
/// final type) and project the final type's columns DISTINCT. The recursive core is the
/// [`recursive_reach_cte`]; the tail is the shared [`chain_from_where`]; the two are glued by
/// `t_0.{identity} IN (SELECT id FROM reach WHERE depth >= 1)` (the depth>=1 reachable set,
/// excluding the seed unless a cycle re-reaches it). `tail_types[0]` MUST carry empty row-filters:
/// the queried type's governance lives in the CTE (`core_row_filters`), so re-applying at `t_0`
/// would only duplicate params. Param order: seed predicates, seed `core_row_filters` (s),
/// recursive `core_row_filters` (nxt), then the tail's per-position params. Precondition:
/// `tail_types.len() == tail_hops.len() + 1` and `tail_hops` non-empty. Every caller value is a
/// bound param.
#[allow(clippy::too_many_arguments)]
pub fn compile_graph_reach_tail(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    identity: &str,
    core_backing: &LinkBacking,
    seed_predicates: &[CallerPredicate],
    core_row_filters: &[RowFilter],
    tail_types: &[ChainType],
    tail_hops: &[LinkBacking],
    allowed_cols: &[String],
    mask_cols: &[String],
    depth: u32,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    debug_assert_eq!(
        tail_types.len(),
        tail_hops.len() + 1,
        "tail types must be hops + 1"
    );
    debug_assert!(!tail_hops.is_empty(), "part-B tail must have >= 1 hop");
    let q = |id: &str| dialect.quote_ident(id);
    let id = q(identity);
    let mut params: Vec<SqlValue> = Vec::new();

    // Recursive core CTE first (the CTE is textually first, so its `?` placeholders bind first).
    let cte = recursive_reach_cte(
        dialect,
        table,
        identity,
        core_backing,
        seed_predicates,
        core_row_filters,
        depth,
        &mut params,
    )?;

    // Relational tail: t_0 = `table` (the reachable set), chained forward to the final type t_k.
    let (from, conjuncts, tail_params) = chain_from_where(dialect, tail_types, tail_hops)?;
    params.extend(tail_params);

    let k = tail_hops.len();
    let final_alias = format!("t_{k}");
    let cols = allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                format!("'{MASK_MARKER}' AS {}", q(c))
            } else {
                format!("{final_alias}.{}", q(c))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    // Glue: t_0 (the queried type at the tail's source) is constrained to the depth>=1 reach set.
    let mut where_conj = vec![format!(
        "t_0.{id} IN (SELECT id FROM reach WHERE depth >= 1)"
    )];
    where_conj.extend(conjuncts);
    let where_sql = where_conj.join(" AND ");

    let sql = format!(
        "{cte} SELECT DISTINCT {cols} FROM {from} WHERE {where_sql} {}",
        dialect.limit_clause(limit)
    );
    Ok((sql, params))
}

/// Compile a governed multi-hop traversal for loom's default (DuckDB) dialect. Convenience
/// wrapper for statically-DuckDB callers (e.g. tests); production paths must use
/// [`compile_chain_with`] with the serving engine's dialect.
pub fn compile_chain(
    types: &[ChainType],
    hops: &[LinkBacking],
    allowed_cols: &[String],
    mask_cols: &[String],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    compile_chain_with(&DuckDbDialect, types, hops, allowed_cols, mask_cols, limit)
}
