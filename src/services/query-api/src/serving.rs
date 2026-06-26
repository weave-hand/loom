//! The serving-engine seam: the `ServingEngine`/`ActionEngine` traits plus the
//! engine-neutral row/value types and the param-inlining helper. Concrete engines
//! live elsewhere — production reads go through `EngineServingClient` (Flight SQL
//! over the engine wire); tests use the in-process Iceberg/DataFusion engine.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use control_plane_core::{BaseType, ColumnSpec, resolve_logical};

use crate::sql::{DataFusionDialect, SqlDialect};

/// A backend-neutral scalar cell. Scalar-only by design (lists are expanded into
/// placeholders before binding — see sql::compile_select_with).
#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Text(String),
    Int(i64),
    Bool(bool),
    Double(f64),
    Date(time::Date),
    Timestamp(time::PrimitiveDateTime),
    Null,
}

/// ISO-8601 date `YYYY-MM-DD`. Shared by the JSON renderer and the Quack literal path.
pub(crate) fn iso_date(d: &time::Date) -> String {
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    d.format(&fmt).unwrap_or_else(|_| d.to_string())
}

/// ISO-8601 datetime `YYYY-MM-DDThh:mm:ss` (no subseconds, no offset — bare timestamp).
pub(crate) fn iso_timestamp(ts: &time::PrimitiveDateTime) -> String {
    let fmt = time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
    ts.format(&fmt).unwrap_or_else(|_| ts.to_string())
}

/// A result set: column names plus rows of cells (row-major, aligned to `columns`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Rows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ServingError {
    #[error("serving engine: {0}")]
    Engine(String),
}

/// Build a one-row Arrow `RecordBatch` + `Schema` + loom `ColumnSpec` list from an
/// aligned `(columns, values, logical_types)` triple. Each logical type resolves
/// (via `resolve_logical`) to a `BaseType` that fixes BOTH the Arrow `DataType` and
/// the loom logical `ColumnSpec.ty` (its canonical name). Every field is nullable —
/// an action write passes `SqlValue::Null` for any property it does not set. A length
/// mismatch, an empty input, an unknown logical type, or a value whose variant does
/// not match its column's base type is a `ServingError::Engine`.
pub fn build_object_batch(
    columns: &[String],
    values: &[SqlValue],
    logical_types: &[String],
) -> Result<(Arc<Schema>, RecordBatch, Vec<ColumnSpec>), ServingError> {
    if columns.is_empty() || columns.len() != values.len() || columns.len() != logical_types.len() {
        return Err(ServingError::Engine(format!(
            "build_object_batch: {} columns / {} values / {} types (need >= 1, equal counts)",
            columns.len(),
            values.len(),
            logical_types.len()
        )));
    }
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    let mut specs: Vec<ColumnSpec> = Vec::with_capacity(columns.len());
    for ((name, value), logical) in columns.iter().zip(values).zip(logical_types) {
        let base = resolve_logical(logical)
            .ok_or_else(|| ServingError::Engine(format!("unknown logical type `{logical}`")))?;
        let (dt, array) = one_cell(base, value, name)?;
        fields.push(Field::new(name, dt, true));
        arrays.push(array);
        specs.push(ColumnSpec {
            name: name.clone(),
            ty: base.canonical_name(),
            nullable: true,
        });
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| ServingError::Engine(e.to_string()))?;
    Ok((schema, batch, specs))
}

/// One single-row Arrow array for a cell of base type `base`. `SqlValue::Null`
/// yields a typed null; any non-null variant must match `base` (the canonical
/// scalar-to-Arrow mapping) or it is a `ServingError`.
#[expect(
    clippy::map_err_ignore,
    reason = "error-handling debt — see docs/error-handling-debt.md"
)]
fn one_cell(base: BaseType, v: &SqlValue, col: &str) -> Result<(DataType, ArrayRef), ServingError> {
    let mismatch = || {
        ServingError::Engine(format!(
            "value for column `{col}` does not match its {base:?} type"
        ))
    };
    Ok(match base {
        BaseType::Integer => {
            let cell: Option<i32> = match v {
                SqlValue::Null => None,
                SqlValue::Int(i) => Some((*i).try_into().map_err(|_| {
                    ServingError::Engine(format!("integer overflow for column `{col}`"))
                })?),
                _ => return Err(mismatch()),
            };
            (DataType::Int32, Arc::new(Int32Array::from(vec![cell])))
        }
        BaseType::Long => {
            let cell: Option<i64> = match v {
                SqlValue::Null => None,
                SqlValue::Int(i) => Some(*i),
                _ => return Err(mismatch()),
            };
            (DataType::Int64, Arc::new(Int64Array::from(vec![cell])))
        }
        BaseType::Double => {
            let cell: Option<f64> = match v {
                SqlValue::Null => None,
                SqlValue::Double(f) => Some(*f),
                _ => return Err(mismatch()),
            };
            (DataType::Float64, Arc::new(Float64Array::from(vec![cell])))
        }
        BaseType::Boolean => {
            let cell: Option<bool> = match v {
                SqlValue::Null => None,
                SqlValue::Bool(b) => Some(*b),
                _ => return Err(mismatch()),
            };
            (DataType::Boolean, Arc::new(BooleanArray::from(vec![cell])))
        }
        BaseType::String => {
            let cell: Option<String> = match v {
                SqlValue::Null => None,
                SqlValue::Text(s) => Some(s.clone()),
                _ => return Err(mismatch()),
            };
            (DataType::Utf8, Arc::new(StringArray::from(vec![cell])))
        }
        BaseType::Date => {
            let cell: Option<i32> = match v {
                SqlValue::Null => None,
                SqlValue::Date(d) => {
                    Some((*d - time::macros::date!(1970 - 01 - 01)).whole_days() as i32)
                }
                _ => return Err(mismatch()),
            };
            (DataType::Date32, Arc::new(Date32Array::from(vec![cell])))
        }
        BaseType::Timestamp => {
            let cell: Option<i64> = match v {
                SqlValue::Null => None,
                SqlValue::Timestamp(ts) => Some(
                    (ts.assume_utc() - time::OffsetDateTime::UNIX_EPOCH)
                        .whole_microseconds()
                        .try_into()
                        .unwrap_or(i64::MAX),
                ),
                _ => return Err(mismatch()),
            };
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                Arc::new(TimestampMicrosecondArray::from(vec![cell])),
            )
        }
        // Serving a vector cell needs a list-bearing `SqlValue` variant + a
        // `List<Float32>` array builder — wired in road-vector-column-type task 4.
        BaseType::Vector(_) => {
            return Err(ServingError::Engine(format!(
                "serving a vector column (`{col}`) is not yet implemented"
            )));
        }
    })
}

#[async_trait]
pub trait ServingEngine: Send + Sync {
    /// Execute read-only `sql`, binding `params` positionally (`?` placeholders).
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError>;

    /// The SQL dialect this engine speaks. Defaults to `DataFusionDialect` — loom's
    /// sole serving dialect. (Production reads go through `EngineServingClient`, which
    /// also overrides this with `DataFusionDialect`.)
    fn dialect(&self) -> &'static dyn SqlDialect {
        &DataFusionDialect
    }
}

/// A write-capable serving engine — the atomic action write-back seam. An impl
/// writes ONE row AND commits its lineage event in the same transaction, so the
/// snapshot and its lineage land or roll back together (no dangling slice). The
/// seam is Arrow-free (takes `logical_types`, not a `RecordBatch`); each impl
/// builds Arrow internally. Mirrors the Iceberg `inline_append(.., lineage) ->
/// SnapshotId` contract.
#[async_trait]
pub trait ActionEngine: Send + Sync {
    async fn write_object(
        &self,
        table: &control_plane_core::TableRef,
        columns: &[String],
        values: &[SqlValue],
        logical_types: &[String],
        event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError>;
}

/// Escape a string for embedding in a DuckDB single-quoted literal: double every
/// `'`. This is the complete escape for DuckDB standard string literals (no
/// backslash escapes by default).
fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

/// Render a typed scalar as a DuckDB SQL literal.
fn render_literal(v: &SqlValue) -> String {
    match v {
        SqlValue::Int(n) => n.to_string(),
        SqlValue::Double(f) => f.to_string(),
        SqlValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Text(s) => format!("'{}'", sql_escape(s)),
        SqlValue::Date(d) => format!("DATE '{}'", iso_date(d)),
        SqlValue::Timestamp(ts) => format!("TIMESTAMP '{}'", iso_timestamp(ts)),
    }
}

/// Substitute each `?` placeholder in `sql` with the next rendered param, copying
/// every other character verbatim. The Quack path uses this because `quack_query`
/// takes SQL as a string with no bind slot. Relies on the `compile_select`
/// contract that `?` appears ONLY as a bind placeholder (never a literal `?`
/// inside a string), so a single left-to-right pass over the ORIGINAL `sql` is
/// correct — it never re-scans substituted text (a rendered value may contain `?`).
pub fn inline_params(sql: &str, params: &[SqlValue]) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut it = params.iter();
    for ch in sql.chars() {
        if ch == '?' {
            match it.next() {
                Some(p) => out.push_str(&render_literal(p)),
                None => out.push('?'),
            }
        } else {
            out.push(ch);
        }
    }
    out
}
