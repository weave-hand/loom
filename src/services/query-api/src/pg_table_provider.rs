//! A DataFusion `TableProvider` that serves a Postgres relation directly over
//! loom's `sqlx::PgPool`, pushing projection/filter/limit into a per-scan
//! `SELECT`. Built for Iceberg inline rows (`iceberg_mirror.inline_<tid>`), where
//! a fixed *base predicate* carries the per-query MVCC snapshot filter, but the
//! type is relation-agnostic and reusable for any "DataFusion scans Postgres"
//! need. See docs/superpowers/specs/2026-06-22-iceberg-inline-pg-tableprovider-design.md.

// Task 2 adds the TableProvider impl that uses all of the imports below;
// allow the interim unused-import/dead-code lint until that lands.
#![allow(unused_imports, dead_code)]

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::PostgreSqlDialect;
use sqlx::{AssertSqlSafe, PgPool, Row};

use crate::serving::ServingError;

/// Serves a Postgres relation as a DataFusion table. `scan` generates
/// `SELECT <proj> FROM <relation> WHERE <base_filter> [AND <pushed>] [LIMIT n]`,
/// runs it over `pool`, and decodes the result to one arrow-58 batch.
#[derive(Debug, Clone)]
pub struct PgTableProvider {
    pool: PgPool,
    /// The Postgres relation, e.g. `iceberg_mirror.inline_7`. Trusted (built from
    /// an internal table id); spliced into SQL via `AssertSqlSafe`.
    relation: String,
    /// The authoritative arrow-58 schema the provider presents (table schema).
    schema: SchemaRef,
    /// loom logical type per column, parallel to `schema.fields()` — drives the
    /// PG-row → arrow decode (the seven logical types in `arrow_field`).
    logical_types: Vec<String>,
    /// A fixed predicate always ANDed into the scan's WHERE (the MVCC snapshot
    /// filter for inline rows). `None` for an unfiltered scan.
    base_filter: Option<String>,
}

impl PgTableProvider {
    pub fn new(
        pool: PgPool,
        relation: String,
        schema: SchemaRef,
        logical_types: Vec<String>,
        base_filter: Option<String>,
    ) -> Self {
        Self {
            pool,
            relation,
            schema,
            logical_types,
            base_filter,
        }
    }
}

/// Quote a SQL identifier (double embedded `"`), matching `iceberg_inline.rs`.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Build the scan SQL. Pure (no pool) so it is unit-testable. Projection picks the
/// SELECT list (empty projection -> `SELECT 1` for COUNT(*)-style scans); the base
/// predicate and each unparseable-free pushed filter are ANDed into one WHERE; a
/// filter the unparser cannot render is skipped (the provider reports `Inexact`,
/// so DataFusion re-applies it — correctness over completeness).
pub fn build_scan_sql(
    relation: &str,
    schema: &Schema,
    base_filter: Option<&str>,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> String {
    let select_list = match projection {
        Some(idx) if idx.is_empty() => "1".to_string(),
        Some(idx) => idx
            .iter()
            .map(|&i| quote_ident(schema.field(i).name()))
            .collect::<Vec<_>>()
            .join(", "),
        None => schema
            .fields()
            .iter()
            .map(|f| quote_ident(f.name()))
            .collect::<Vec<_>>()
            .join(", "),
    };

    let unparser = Unparser::new(&PostgreSqlDialect {});
    let mut conds: Vec<String> = Vec::new();
    if let Some(bf) = base_filter {
        conds.push(bf.to_string());
    }
    for f in filters {
        if let Ok(sql) = unparser.expr_to_sql(f) {
            conds.push(format!("({sql})"));
        }
    }
    let where_clause = if conds.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conds.join(" AND "))
    };
    let limit_clause = limit.map(|n| format!(" LIMIT {n}")).unwrap_or_default();
    format!("SELECT {select_list} FROM {relation}{where_clause}{limit_clause}")
}
