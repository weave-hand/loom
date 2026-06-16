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
pub fn compile_chain_with(
    dialect: &dyn SqlDialect,
    types: &[ChainType],
    hops: &[LinkBacking],
    allowed_cols: &[String],
    mask_cols: &[String],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
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

    // Projection: final target `t_k` only.
    let final_alias = alias(k);
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

    // FROM final target, then JOIN each predecessor down to the source.
    let mut from = format!("{} {}", tbl(&types[k].table), final_alias);
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

    // WHERE: per position in chain order, this type's caller predicates then its ACL
    // row-filters, both bound at alias `t_i`. Params are pushed in conjunct-emission
    // order so positional `?` alignment holds. Source filters are just position 0's
    // predicates — no special case.
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

    let mut sql = format!("SELECT DISTINCT {cols} FROM {from}");
    if !conjuncts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conjuncts.join(" AND "));
    }
    sql.push_str(&format!(" {}", dialect.limit_clause(limit)));
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
