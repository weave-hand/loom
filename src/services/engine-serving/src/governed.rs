//! The governed external-SQL path: translates loom's [`control_plane_core::RowFilter`]
//! ACL policy into DataFusion `Expr`s so the engine can apply row-level governance
//! directly in the query plan, matching `query-api::sql::filter_sql`'s SQL semantics.
//! See docs/superpowers/specs/2026-06-24-engine-serving-execution-wire-design.md.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use control_plane_core::{
    CompareOp, GovernedCatalog, RowFilter, ScalarValue, TableRef, validate_row_filter,
};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::context::{ExecutionProps, SessionContext};
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType, not};
use datafusion::physical_expr::{
    PhysicalExpr, create_physical_expr,
    expressions::{Column, lit as phys_lit},
};
use datafusion::physical_plan::{
    ExecutionPlan, SendableRecordBatchStream,
    filter::FilterExec,
    limit::GlobalLimitExec,
    projection::{ProjectionExec, ProjectionExpr},
};
use datafusion::prelude::{Expr, col, lit};
use datafusion::scalar::ScalarValue as DfScalar;
use store_config::ServingStore;

use crate::serving::{EngineServingError, build_serving_provider, register_qualified, to_serving};

/// The redaction marker a masked column's every value is replaced with. Kept local
/// to engine-serving (query-api owns its own private copy) so the enforcing path has
/// no cross-crate dependency on the presentation layer.
const MASK_MARKER: &str = "***";

/// A leaf `ScalarValue` (never a `List`) → a DataFusion literal `Expr`.
fn scalar_lit(v: &ScalarValue) -> Result<Expr, EngineServingError> {
    let s = match v {
        ScalarValue::Text(s) => DfScalar::Utf8(Some(s.clone())),
        ScalarValue::Int(i) => DfScalar::Int64(Some(*i)),
        ScalarValue::Bool(b) => DfScalar::Boolean(Some(*b)),
        ScalarValue::List(_) => {
            return Err(EngineServingError::Engine(
                "row filter: unexpected list scalar in leaf position".into(),
            ));
        }
    };
    Ok(lit(s))
}

/// Translate a `RowFilter` tree into a DataFusion `Expr`, matching
/// `query-api::sql::filter_sql` semantics. Fails closed: an invariant violation
/// (validated by `validate_row_filter`) returns an error, never a panic.
pub fn row_filter_to_expr(f: &RowFilter) -> Result<Expr, EngineServingError> {
    validate_row_filter(f, None).map_err(EngineServingError::Engine)?;
    build_expr(f)
}

fn build_expr(f: &RowFilter) -> Result<Expr, EngineServingError> {
    match f {
        RowFilter::Compare {
            property,
            op,
            value,
        } => {
            let c = col(property);
            match op {
                CompareOp::Eq => Ok(c.eq(scalar_lit(value)?)),
                CompareOp::Ne => Ok(c.not_eq(scalar_lit(value)?)),
                CompareOp::Lt => Ok(c.lt(scalar_lit(value)?)),
                CompareOp::Le => Ok(c.lt_eq(scalar_lit(value)?)),
                CompareOp::Gt => Ok(c.gt(scalar_lit(value)?)),
                CompareOp::Ge => Ok(c.gt_eq(scalar_lit(value)?)),
                CompareOp::In | CompareOp::NotIn => {
                    let ScalarValue::List(items) = value else {
                        return Err(EngineServingError::Engine(
                            "row filter: In/NotIn requires a list value".into(),
                        ));
                    };
                    let list = items
                        .iter()
                        .map(scalar_lit)
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(c.in_list(list, matches!(op, CompareOp::NotIn)))
                }
                CompareOp::IsNull => Ok(c.is_null()),
                CompareOp::IsNotNull => Ok(c.is_not_null()),
                // Not dead code: `row_filter_to_expr` (the only caller that runs
                // `validate_row_filter` first) never reaches this arm, but
                // `GovernedTableProvider::scan` -> `row_filters_conjunction` calls
                // `build_expr` directly, skipping `validate_row_filter`. This arm is
                // that path's sole in-module fail-closed defense against these four
                // caller-predicate-only ops; it is unreachable in practice today only
                // because ACL-write-time validation (control-plane memory/postgres
                // `acl.rs`) already rejects such filters before they can persist.
                CompareOp::Between
                | CompareOp::Contains
                | CompareOp::StartsWith
                | CompareOp::EndsWith => Err(EngineServingError::Engine(format!(
                    "{op:?} is not supported in a row filter"
                ))),
            }
        }
        RowFilter::And(xs) => fold_bool(xs, true),
        RowFilter::Or(xs) => fold_bool(xs, false),
        RowFilter::Not(x) => Ok(not(build_expr(x)?)),
    }
}

/// AND-fold (`and_identity=true`) or OR-fold (`false`) a slice of sub-filters.
/// An empty slice yields the identity literal (`true` for AND, `false` for OR).
fn fold_bool(xs: &[RowFilter], and: bool) -> Result<Expr, EngineServingError> {
    let mut acc: Option<Expr> = None;
    for x in xs {
        let e = build_expr(x)?;
        acc = Some(match acc {
            None => e,
            Some(a) if and => a.and(e),
            Some(a) => a.or(e),
        });
    }
    Ok(acc.unwrap_or_else(|| lit(and)))
}

/// AND-fold a slice of top-level row filters into one optional predicate.
pub(crate) fn row_filters_conjunction(
    fs: &[RowFilter],
) -> Result<Option<Expr>, EngineServingError> {
    if fs.is_empty() {
        return Ok(None);
    }
    Ok(Some(fold_bool(fs, true)?))
}

/// The enforcing per-table policy the provider applies. `denied`/`masked` are sets
/// for O(1) column membership tests during `scan`/`schema`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TablePolicy {
    pub row_filters: Vec<RowFilter>,
    pub denied: HashSet<String>,
    pub masked: HashSet<String>,
}

/// The policy for `table` from `catalog`, or the empty (fully-visible) policy when
/// absent (per spec "absent ⇒ visible").
#[must_use]
pub fn policy_for(catalog: &GovernedCatalog, table: &TableRef) -> TablePolicy {
    match catalog.table_for(table) {
        Some(gt) => TablePolicy {
            row_filters: gt.row_filters.clone(),
            denied: gt.denied.iter().cloned().collect(),
            masked: gt.masked.iter().cloned().collect(),
        },
        None => TablePolicy::default(),
    }
}

/// An enforcing `TableProvider` decorator: applies a `TablePolicy` (row filters,
/// denied columns, masked columns) to an inner serving provider. No code path returns
/// a denied column, an unmasked masked value, or a filter-excluded row.
///
/// Enforcement is *by construction*, not by trusting the client SQL: `scan` always
/// scans the inner provider over its FULL schema and all rows (ignoring the client
/// projection/filter/limit for the inner read), applies the policy row filters over
/// that full schema (so a filter may reference a denied column), THEN drops denied
/// columns and replaces masked columns with the `'***'` literal. The client's SQL
/// only ever sees the governed output.
#[derive(Debug)]
pub struct GovernedTableProvider {
    inner: Arc<dyn TableProvider>,
    policy: TablePolicy,
    governed_schema: SchemaRef,
}

impl GovernedTableProvider {
    /// Precompute the governed schema (denied fields removed, masked fields re-typed to
    /// `Utf8`). The remaining fields keep their inner type and (for masked → Utf8)
    /// become nullable, matching the `'***'` literal's presentation.
    pub fn new(
        inner: Arc<dyn TableProvider>,
        policy: TablePolicy,
    ) -> Result<Self, EngineServingError> {
        let inner_schema = inner.schema();
        let fields: Vec<Field> = inner_schema
            .fields()
            .iter()
            .filter(|f| !policy.denied.contains(f.name()))
            .map(|f| {
                if policy.masked.contains(f.name()) {
                    Field::new(f.name(), DataType::Utf8, true)
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        let governed_schema = Arc::new(Schema::new(fields));
        Ok(Self {
            inner,
            policy,
            governed_schema,
        })
    }
}

#[async_trait]
impl TableProvider for GovernedTableProvider {
    fn schema(&self) -> SchemaRef {
        self.governed_schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // Inexact: governance re-applies its row filters regardless, so any client
        // predicate is only ever an optimization — DataFusion must still re-apply it.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // 1. Inner scan over the FULL schema, all rows — governance must see every
        //    column (a row filter may reference a denied one) and every row (before
        //    any client predicate). The client projection/filter/limit are NOT pushed
        //    into the inner scan; correctness over the pushdown optimization.
        let inner_schema = self.inner.schema();
        let mut plan = self.inner.scan(state, None, &[], None).await?;

        // 2. Row-filter FilterExec over the inner schema (may reference denied cols).
        //    Building the physical predicate can fail closed → surface as a plan error.
        if let Some(expr) = row_filters_conjunction(&self.policy.row_filters)
            .map_err(|e| DataFusionError::Plan(e.to_string()))?
        {
            let df_schema = DFSchema::try_from(inner_schema.clone())?;
            let phys = create_physical_expr(&expr, &df_schema, &ExecutionProps::new())?;
            plan = Arc::new(FilterExec::try_new(phys, plan)?);
        }

        // 3. Governed projection: denied columns dropped, masked columns replaced with
        //    the `'***'` literal, honoring the client projection (over GOVERNED field
        //    indices). Each output column resolves against the FILTERED inner plan by
        //    name (masked → literal, else the inner column at its by-name index).
        let governed = self.governed_schema.clone();
        let indices: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => (0..governed.fields().len()).collect(),
        };
        let mut proj: Vec<ProjectionExpr> = Vec::with_capacity(indices.len());
        for gi in indices {
            let field = governed.field(gi);
            let name = field.name().to_string();
            let expr: Arc<dyn PhysicalExpr> = if self.policy.masked.contains(&name) {
                phys_lit(DfScalar::Utf8(Some(MASK_MARKER.to_string())))
            } else {
                let inner_idx = inner_schema
                    .index_of(&name)
                    .map_err(|e| DataFusionError::Plan(e.to_string()))?;
                Arc::new(Column::new(&name, inner_idx))
            };
            proj.push(ProjectionExpr::new(expr, name));
        }
        plan = Arc::new(ProjectionExec::try_new(proj, plan)?);

        // 4. Client limit (the only client hint we honor, applied AFTER governance).
        if let Some(n) = limit {
            plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(n)));
        }
        Ok(plan)
    }
}

/// Run client `sql` over every live Iceberg table **listed in the governed catalog**,
/// wrapped in its `GovernedTableProvider`, returning the result stream. For each such
/// live table: build the inner serving provider (file ∪ inline), resolve its
/// `TablePolicy` from `governed`, wrap it in a `GovernedTableProvider`, register it
/// schema-qualified, then run the SQL. Governance is enforced by the providers, so the
/// client SQL is arbitrary — it can never observe a denied column, an unmasked value,
/// or a filter-excluded row.
pub async fn execute_governed_sql_stream(
    catalog: &IcebergCatalog,
    sql: &str,
    governed: &GovernedCatalog,
    serving_store: Option<&ServingStore>,
) -> Result<SendableRecordBatchStream, EngineServingError> {
    let ctx = SessionContext::new();
    for table in catalog.live_tables().await.map_err(to_serving)? {
        // Closed-world: a live table with NO GovernedTable entry is not
        // registered at all — it does not exist for this session. Deny-by-
        // default holds even if the edge under-lists (unbound datasets,
        // ungranted types). See 2026-07-09-external-sql-wire-design.md.
        if governed.table_for(&table).is_none() {
            continue;
        }
        // This loop always passes `at: None` (current snapshot), so
        // `build_serving_provider`'s `Ok(None)` (as-of-not-live) case is
        // unreachable here in practice — a live-but-empty table now yields
        // `Ok(Some(_))` (a zero-row provider). Kept as a harmless skip.
        let Some(inner) =
            build_serving_provider(&ctx, catalog, &table, serving_store, None).await?
        else {
            continue;
        };
        let policy = policy_for(governed, &table);
        let provider: Arc<dyn TableProvider> = Arc::new(GovernedTableProvider::new(inner, policy)?);
        register_qualified(&ctx, &table.schema, &table.name, provider)?;
    }
    let df = ctx.sql(sql).await.map_err(EngineServingError::Plan)?;
    df.execute_stream().await.map_err(to_serving)
}
