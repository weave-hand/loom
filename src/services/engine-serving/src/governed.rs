//! The governed external-SQL path: translates loom's [`control_plane_core::RowFilter`]
//! ACL policy into DataFusion `Expr`s so the engine can apply row-level governance
//! directly in the query plan, matching `query-api::sql::filter_sql`'s SQL semantics.
//! See git history: 2026-06-24-engine-serving-execution-wire-design.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use control_plane_core::{
    CompareOp, GovernedCatalog, RowFilter, ScalarValue, TableRef, validate_row_filter,
};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::context::{ExecutionProps, SQLOptions, SessionConfig, SessionContext};
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
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

use crate::serving::{
    EngineServingError, build_serving_provider, fold_view, register_qualified, to_serving,
};
use crate::sql_limits::{DeadlineStream, GovernedSqlLimits, governed_stream_error};

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
///
/// Both client surfaces (`POST /sql` and the external Flight SQL wire) reach this
/// execution path through `do_get_governed_sql`. `limits` applies a per-statement
/// memory pool and a wall-clock deadline covering the whole query lifetime —
/// registration, planning, and every batch. Engine-wide admission is enforced by
/// the Flight handler before this function is called. See #664 and #678.
pub async fn execute_governed_sql_stream(
    catalog: &IcebergCatalog,
    sql: &str,
    governed: &GovernedCatalog,
    serving_store: Option<&ServingStore>,
    limits: &GovernedSqlLimits,
) -> Result<SendableRecordBatchStream, EngineServingError> {
    // `checked_add`, not `+`: `LOOM_SQL_TIMEOUT_SECS` parses as a `u64`, so an absurd
    // value would overflow the `Instant` and panic inside the Flight handler on EVERY
    // governed query. An unrepresentable deadline degrades to "no deadline" — the same
    // behaviour as the documented `0` escape hatch — rather than taking the engine down.
    let deadline = limits.deadline.and_then(|d| Instant::now().checked_add(d));
    let planned = plan_governed_sql(catalog, sql, governed, serving_store, limits.memory_bytes);
    let stream = match deadline {
        None => planned.await?,
        Some(t) => tokio::time::timeout_at(t.into(), planned)
            .await
            .map_err(|_elapsed| {
                EngineServingError::ResourceExhausted(
                    "statement exceeded its wall-clock budget (LOOM_SQL_TIMEOUT_SECS) \
                     while planning"
                        .to_string(),
                )
            })??,
    };
    // The SAME absolute deadline bounds the stream the caller will drive: a wrapping
    // future could not bound work performed after this function returns.
    Ok(match deadline {
        None => stream,
        Some(t) => Box::pin(DeadlineStream::until(stream, t)),
    })
}

/// The registration + planning + execute half of [`execute_governed_sql_stream`],
/// split out so the wrapper above can wrap the WHOLE of it in one wall-clock budget.
/// `memory_bytes` bounds the statement's DataFusion memory pool.
async fn plan_governed_sql(
    catalog: &IcebergCatalog,
    sql: &str,
    governed: &GovernedCatalog,
    serving_store: Option<&ServingStore>,
    memory_bytes: Option<usize>,
) -> Result<SendableRecordBatchStream, EngineServingError> {
    let ctx = build_session(memory_bytes)?;
    register_governed_tables(&ctx, catalog, governed, serving_store).await?;
    register_governed_views(&ctx, catalog, governed, serving_store).await?;
    // Read-only guard (load-bearing): this path plans ARBITRARY client SQL, so it must
    // reject DDL, DML, and statements — `COPY … TO`, `INSERT`/`UPDATE`/`DELETE`,
    // `CREATE`, `SET`, … — none of which route through the governed `TableProvider`s and
    // would otherwise let any caller holding a Read grant write to the object store /
    // server filesystem (a bypass of the read-only, governed contract). `SELECT`/`WITH`
    // are `Query` nodes, ungated by these flags, so read queries are unaffected; a
    // rejected statement is an `EngineServingError::Plan` → the caller's own 400.
    let opts = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);
    let df = ctx
        .sql_with_options(sql, opts)
        .await
        .map_err(EngineServingError::Plan)?;
    // CLASSIFICATION IS LOAD-BEARING: `to_serving` is class-erasing and would bury a
    // pool breach raised here as `Engine`/500. Route through the shared classifier.
    df.execute_stream()
        .await
        .map_err(|e| governed_stream_error(&e))
}

/// Register every live base table **listed in the governed catalog** on `ctx`, each
/// wrapped in its own `GovernedTableProvider`.
///
/// Closed-world: a live table with NO `GovernedTable` entry is not registered at all —
/// it does not exist for this session, so an ungranted table is indistinguishable from
/// a nonexistent one and there is no existence leak through plan errors. Deny-by-default
/// therefore holds even if the edge under-lists (unbound datasets, ungranted types).
/// See 2026-07-09-external-sql-wire-design.md.
async fn register_governed_tables(
    ctx: &SessionContext,
    catalog: &IcebergCatalog,
    governed: &GovernedCatalog,
    serving_store: Option<&ServingStore>,
) -> Result<(), EngineServingError> {
    for table in catalog.live_tables().await.map_err(to_serving)? {
        if governed.table_for(&table).is_none() {
            continue;
        }
        // This loop always passes `at: None` (current snapshot), so
        // `build_serving_provider`'s `Ok(None)` (as-of-not-live) case is
        // unreachable here in practice — a live-but-empty table now yields
        // `Ok(Some(_))` (a zero-row provider). Kept as a harmless skip.
        let Some(inner) = build_serving_provider(ctx, catalog, &table, serving_store, None).await?
        else {
            continue;
        };
        let policy = policy_for(governed, &table);
        let provider: Arc<dyn TableProvider> = Arc::new(GovernedTableProvider::new(inner, policy)?);
        register_qualified(ctx, &table.schema, &table.name, provider)?;
    }
    Ok(())
}

/// Register every catalog view listed in the governed catalog on `ctx`.
///
/// A view is an ordinary governed relation, with one deliberate twist: it registers only
/// when the VIEW itself has a `GovernedTable` entry (closed-world, exactly as for a base
/// table) — a view grant is sufficient and never requires the base's own grant. The
/// view's inner base relation is therefore built PRIVATELY and UNGOVERNED
/// (`build_serving_provider`, never `ctx.table`): in this governed session a base is
/// registered only when it has its own entry, and then it is wrapped in the BASE's
/// policy — both wrong for the view. The folded view is wrapped in the VIEW's policy, so
/// no base policy contaminates the view scan.
async fn register_governed_views(
    ctx: &SessionContext,
    catalog: &IcebergCatalog,
    governed: &GovernedCatalog,
    serving_store: Option<&ServingStore>,
) -> Result<(), EngineServingError> {
    use control_plane_core::Catalog as _;
    for v in catalog
        .list_views(control_plane_core::PageReq::unbounded())
        .await
        .map_err(to_serving)?
        .items
    {
        if governed.table_for(&v.view).is_none() {
            continue; // closed-world: unlisted view is unresolvable
        }
        let policy = policy_for(governed, &v.view);
        let Some(inner) =
            build_serving_provider(ctx, catalog, &v.base, serving_store, None).await?
        else {
            continue; // dangling view: base not live here
        };
        let df = ctx.read_table(inner).map_err(to_serving)?;
        let provider = fold_view(df, &v)?.into_view();
        let governed_provider: Arc<dyn TableProvider> =
            Arc::new(GovernedTableProvider::new(provider, policy)?);
        register_qualified(ctx, &v.view.schema, &v.view.name, governed_provider)?;
    }
    Ok(())
}

/// A fresh `SessionContext` for one governed statement, optionally over a runtime
/// whose memory pool is capped at `memory_bytes`.
///
/// TWO settings, both required — neither implies the other:
///
/// 1. `GreedyMemoryPool`, NOT `FairSpillPool`. Memory-tracked operators (sort, hash
///    aggregate, hash join, sort-merge join) reserve from the pool and fail with
///    `DataFusionError::ResourcesExhausted` once it is exhausted.
/// 2. `DiskManagerMode::Disabled`. The pool choice does NOT disable spilling — the
///    disk manager does, and `RuntimeEnvBuilder` defaults it to the OS temp dir. Left
///    at the default, `SortExec` spills instead of failing and an oversized query
///    succeeds while writing unbounded files into `/tmp`: a disk DoS traded for the
///    memory DoS. Disabled, an attempted spill errors and the budget actually binds.
pub fn build_session(memory_bytes: Option<usize>) -> Result<SessionContext, EngineServingError> {
    let Some(bytes) = memory_bytes else {
        return Ok(SessionContext::new());
    };
    let rt = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(bytes)))
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build_arc()
        .map_err(to_serving)?;
    Ok(SessionContext::new_with_config_rt(SessionConfig::new(), rt))
}
