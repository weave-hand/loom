//! A DataFusion `TableProvider` that serves a Postgres relation directly over
//! loom's `sqlx::PgPool`, pushing projection/filter/limit into a per-scan
//! `SELECT`. Built for Iceberg inline rows (`iceberg_mirror.inline_<tid>`), where
//! a fixed *base predicate* carries the per-query MVCC snapshot filter, but the
//! type is relation-agnostic and reusable for any "DataFusion scans Postgres"
//! need. See docs/superpowers/specs/2026-06-22-iceberg-inline-pg-tableprovider-design.md.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::PostgreSqlDialect;
use sqlx::{AssertSqlSafe, PgPool, Row};

use crate::serving::EngineServingError as ServingError;

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

/// Decode `rows` to one arrow-58 array per column, typed by `logical_types[i]`
/// (positional — the SELECT list order). Mirrors `iceberg_inline::column_array`
/// (the whole tree is arrow 58). sqlx decodes to plain Rust types, so only the
/// arrow side differs.
fn pg_rows_to_arrays(
    rows: &[sqlx::postgres::PgRow],
    logical_types: &[String],
) -> Result<Vec<ArrayRef>, ServingError> {
    use arrow::array::builder::{
        BooleanBuilder, Date32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
        TimestampMicrosecondBuilder,
    };

    let map_err = |e: sqlx::Error| ServingError::Engine(e.to_string());
    macro_rules! get {
        ($ty:ty, $i:expr) => {
            rows.iter()
                .map(|r| r.try_get::<Option<$ty>, _>($i))
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_err)?
        };
    }

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(logical_types.len());
    for (i, logical) in logical_types.iter().enumerate() {
        let array: ArrayRef = match logical.as_str() {
            "integer" => {
                let mut b = Int32Builder::new();
                for v in get!(i32, i) {
                    b.append_option(v);
                }
                Arc::new(b.finish())
            }
            "long" => {
                let mut b = Int64Builder::new();
                for v in get!(i64, i) {
                    b.append_option(v);
                }
                Arc::new(b.finish())
            }
            "double" => {
                let mut b = Float64Builder::new();
                for v in get!(f64, i) {
                    b.append_option(v);
                }
                Arc::new(b.finish())
            }
            "boolean" => {
                let mut b = BooleanBuilder::new();
                for v in get!(bool, i) {
                    b.append_option(v);
                }
                Arc::new(b.finish())
            }
            "string" => {
                let mut b = StringBuilder::new();
                for v in get!(String, i) {
                    b.append_option(v);
                }
                Arc::new(b.finish())
            }
            "date" => {
                let mut b = Date32Builder::new();
                let epoch = time::macros::date!(1970 - 01 - 01);
                for v in get!(time::Date, i) {
                    b.append_option(v.map(|d| (d - epoch).whole_days() as i32));
                }
                Arc::new(b.finish())
            }
            "timestamp" => {
                let mut b = TimestampMicrosecondBuilder::new();
                for v in get!(time::PrimitiveDateTime, i) {
                    b.append_option(v.map(|t| {
                        (t.assume_utc() - time::OffsetDateTime::UNIX_EPOCH)
                            .whole_microseconds()
                            .try_into()
                            .unwrap_or(i64::MAX)
                    }));
                }
                Arc::new(b.finish())
            }
            other => {
                return Err(ServingError::Engine(format!(
                    "inline PG provider: unsupported logical type {other:?}"
                )));
            }
        };
        arrays.push(array);
    }
    Ok(arrays)
}

impl PgTableProvider {
    /// Run `sql`, decode the projected columns (`proj_logicals`, in SELECT order)
    /// into one batch with `proj_schema`. For an empty SELECT list (`SELECT 1`),
    /// `proj_schema` is empty and the batch carries only the row count.
    async fn fetch_batch(
        &self,
        sql: String,
        proj_schema: SchemaRef,
        proj_logicals: &[String],
    ) -> Result<RecordBatch, ServingError> {
        let rows = sqlx::query(AssertSqlSafe(sql))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;

        if proj_schema.fields().is_empty() {
            // Empty projection (e.g. COUNT(*)): a 0-column batch with the row count.
            let opts = RecordBatchOptions::new().with_row_count(Some(rows.len()));
            return RecordBatch::try_new_with_options(proj_schema, vec![], &opts)
                .map_err(|e| ServingError::Engine(e.to_string()));
        }
        let arrays = pg_rows_to_arrays(&rows, proj_logicals)?;
        RecordBatch::try_new(proj_schema, arrays).map_err(|e| ServingError::Engine(e.to_string()))
    }
}

#[async_trait]
impl TableProvider for PgTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        // Inexact: we push what unparses into the SQL, but DataFusion must still
        // re-apply every predicate (an unparseable filter is silently skipped in
        // `build_scan_sql`, so the SQL is never *more* restrictive than asked).
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Empty projection is pushed as `SELECT 1` (handled in build_scan_sql); we
        // then materialize a 0-column batch and let MemTable surface the row count.
        let sql = build_scan_sql(
            &self.relation,
            self.schema.as_ref(),
            self.base_filter.as_deref(),
            projection,
            filters,
            limit,
        );

        // Projected schema + parallel logical types for the decode.
        let (proj_schema, proj_logicals): (SchemaRef, Vec<String>) = match projection {
            Some(idx) if idx.is_empty() => (Arc::new(Schema::empty()), Vec::new()),
            Some(idx) => {
                let s = self.schema.project(idx).map_err(|e| {
                    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                })?;
                let l = idx.iter().map(|&i| self.logical_types[i].clone()).collect();
                (Arc::new(s), l)
            }
            None => (self.schema.clone(), self.logical_types.clone()),
        };

        let batch = self
            .fetch_batch(sql, proj_schema.clone(), &proj_logicals)
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // The SQL already applied projection/filter/limit, so the MemTable scan
        // adds nothing on top.
        let mem = MemTable::try_new(proj_schema, vec![vec![batch]])?;
        mem.scan(state, None, &[], None).await
    }
}
