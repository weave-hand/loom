//! The slice-5 enrich read: a table's folded current state (the merge engine
//! applied for CDC tables; files ∪ inline for log/plain tables), optionally
//! filtered to a lookup-key set. A LOGICAL read — framing/reserved columns
//! are hidden by the serving provider. This is the point-lookup API the
//! deferred fut-stream-pk-index later re-backs with an index probe; v1 backs
//! it with the predicated merge-on-read scan (correctness never depends on
//! pruning). Internal, ungoverned data plane — parity with transform inputs.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, SchemaRef};
use control_plane_core::{Catalog, ColumnSpec, ControlPlaneError, TableRef};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Expr;
use datafusion::prelude::{col, lit};
use datafusion::scalar::ScalarValue;
use datafusion_io::logical_arrow_schema;
use store_config::ServingStore;

use crate::serving::{EngineServingError, build_serving_provider, to_serving};

/// The stable `"mv enrich: "` message prefix every deterministic refusal from
/// this module carries (unknown table, missing/non-coercible key, unsupported
/// column type). The message — not a status code — is what survives the
/// gRPC-status flattening the engine (Task 4) and worker (Task 5) key off.
fn refuse(msg: &str) -> EngineServingError {
    EngineServingError::Engine(format!("mv enrich: {msg}"))
}

/// `table`'s folded current state — the merge engine applied for a CDC/identity
/// table, or the plain files-∪-inline union otherwise — optionally narrowed to
/// `key`'s `(column, values)` lookup set via an `IN` predicate. `serving_store`
/// is the S3 store to register for `s3://` warehouse paths (as
/// [`build_serving_provider`] takes); `None` uses the local-filesystem default.
///
/// A table that has never been committed to the Iceberg mirror at all (no
/// declared/landed data, ever) is a deterministic error. A table that IS live
/// in the mirror but currently has zero files and zero live inline rows (the
/// "live-but-empty" transform-input posture) serves its declared logical
/// schema with zero rows — not an error.
pub async fn mv_enrich_scan(
    catalog: &IcebergCatalog,
    table: &TableRef,
    key: Option<(&str, &[serde_json::Value])>,
    serving_store: Option<&ServingStore>,
) -> Result<(SchemaRef, Vec<RecordBatch>), EngineServingError> {
    // Resolve the table's current snapshot FIRST: this is the only place that
    // still sees the TYPED `ControlPlaneError::NotFound` distinguishing a
    // genuinely unknown table from one that is live but has no data yet.
    // `build_serving_provider` re-resolves this snapshot internally, but by
    // the time an error crosses that boundary it has already been flattened
    // into an opaque `EngineServingError::Engine` string.
    let snap = match catalog.current_snapshot(table).await {
        Ok(s) => s,
        Err(ControlPlaneError::NotFound(_)) => {
            return Err(refuse(&format!(
                "unknown table {}.{}",
                table.schema, table.name
            )));
        }
        Err(e) => return Err(to_serving(e)),
    };

    let ctx = SessionContext::new();
    let Some(provider) = build_serving_provider(&ctx, catalog, table, serving_store, None).await?
    else {
        // Live-but-empty: no files, no live inline rows. Serve the declared
        // logical schema (the mirror's authoritative column list, already
        // known to be resolvable since `current_snapshot` above succeeded)
        // with zero rows — the transform-input posture.
        let table_schema = catalog.schema(table, snap.id).await.map_err(to_serving)?;
        let columns: Vec<ColumnSpec> = table_schema
            .columns
            .into_iter()
            .map(|c| ColumnSpec {
                name: c.name,
                ty: c.ty,
                nullable: c.nullable,
            })
            .collect();
        let schema = logical_arrow_schema(&columns)
            .map_err(|e| refuse(&format!("unsupported column type: {e}")))?;
        return Ok((schema, Vec::new()));
    };

    let df = ctx.read_table(provider).map_err(to_serving)?;
    let df = match key {
        None => df,
        Some((col_name, keys)) => {
            let data_type = df
                .schema()
                .field_with_unqualified_name(col_name)
                .map_err(|e| {
                    refuse(&format!(
                        "key column '{col_name}' not in {}.{}: {e}",
                        table.schema, table.name
                    ))
                })?
                .data_type()
                .clone();
            let literals = coerce_keys(&data_type, col_name, keys)?;
            df.filter(col(col_name).in_list(literals, false))
                .map_err(to_serving)?
        }
    };

    let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await.map_err(to_serving)?;
    Ok((schema, batches))
}

/// Coerce every JSON scalar in `keys` into a typed literal `Expr` matching
/// `data_type` (`Int32`/`Int64`/`Utf8` in v1). A JSON number that doesn't fit,
/// a non-scalar value, or any other column type is a loud, deterministic
/// `"mv enrich: ..."` error — never a silent empty result.
fn coerce_keys(
    data_type: &DataType,
    col_name: &str,
    keys: &[serde_json::Value],
) -> Result<Vec<Expr>, EngineServingError> {
    keys.iter()
        .map(|v| coerce_key(data_type, col_name, v))
        .collect()
}

fn coerce_key(
    data_type: &DataType,
    col_name: &str,
    v: &serde_json::Value,
) -> Result<Expr, EngineServingError> {
    let scalar = match data_type {
        DataType::Int32 => {
            let n = v.as_i64().ok_or_else(|| non_coercible(col_name, v))?;
            let n32 = i32::try_from(n)
                .ok()
                .ok_or_else(|| non_coercible(col_name, v))?;
            ScalarValue::Int32(Some(n32))
        }
        DataType::Int64 => {
            let n = v.as_i64().ok_or_else(|| non_coercible(col_name, v))?;
            ScalarValue::Int64(Some(n))
        }
        DataType::Utf8 => {
            let s = v.as_str().ok_or_else(|| non_coercible(col_name, v))?;
            ScalarValue::Utf8(Some(s.to_string()))
        }
        other => {
            return Err(refuse(&format!(
                "unsupported key column type for '{col_name}': {other:?}"
            )));
        }
    };
    Ok(lit(scalar))
}

/// A key value that cannot be coerced to the key column's Arrow type — a
/// non-scalar JSON value, or a number that doesn't fit the target width.
fn non_coercible(col_name: &str, v: &serde_json::Value) -> EngineServingError {
    refuse(&format!(
        "key value {v} is not coercible to column '{col_name}'"
    ))
}
