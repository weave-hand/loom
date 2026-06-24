//! Shared serving utilities for the query-api Iceberg path: serialisation helpers
//! (`encode_ipc_stream`, `batches_to_rows`, `arrow_to_sqlvalue`), the Iceberg action
//! writer, and the backend selector. The DataFusion execution layer has moved to
//! `engine-serving`; this file retains only the pieces still owned by query-api.

use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use async_trait::async_trait;
use control_plane_postgres::iceberg_landing;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use sqlx::PgPool;

use crate::serving::{ActionEngine, Rows, ServingError, SqlValue, build_object_batch};

/// Any error -> opaque serving error. Kept here (not deleted) because
/// `encode_ipc_stream` still uses it — engine-serving has its own copy.
fn to_serving<E: std::fmt::Display>(e: E) -> ServingError {
    ServingError::Engine(e.to_string())
}

/// Encode a (one-row) `RecordBatch` to an Arrow IPC *stream* body — the bytes
/// `iceberg_landing::land` re-decodes in the postgres crate (which owns the Iceberg
/// writer chain). Any writer error maps to an opaque serving error.
pub fn encode_ipc_stream(batch: &RecordBatch) -> Result<Vec<u8>, ServingError> {
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(to_serving)?;
        w.write(batch).map_err(to_serving)?;
        w.finish().map_err(to_serving)?;
    }
    Ok(buf)
}

/// The `ActionEngine` for the Iceberg serving backend: a governed typed-insert is
/// built into the same one-row batch the DuckLake writer uses, encoded to Arrow IPC,
/// and forwarded to the atomic inline-write seam `iceberg_landing::land`. A single
/// action row inlines (mirror-only typed rows): one Postgres transaction committing
/// the row and its lineage together, drained to real Parquet later by the flush
/// vertical. Holds the same dependencies as ingest's `IcebergMaterializer`.
pub struct IcebergActionWriter {
    catalog: Arc<SqlCatalog>,
    pool: PgPool,
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
}

impl IcebergActionWriter {
    pub fn new(
        catalog: Arc<SqlCatalog>,
        pool: PgPool,
        inline_byte_limit: usize,
        flush_byte_threshold: i64,
    ) -> Self {
        Self {
            catalog,
            pool,
            inline_byte_limit,
            flush_byte_threshold,
        }
    }
}

#[async_trait]
impl ActionEngine for IcebergActionWriter {
    async fn write_object(
        &self,
        table: &control_plane_core::TableRef,
        columns: &[String],
        values: &[SqlValue],
        logical_types: &[String],
        event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        let (_schema, batch, specs) = build_object_batch(columns, values, logical_types)?;
        let ipc_body = encode_ipc_stream(&batch)?;
        iceberg_landing::land(
            &self.pool,
            &self.catalog,
            table,
            &specs,
            &ipc_body,
            self.inline_byte_limit,
            self.flush_byte_threshold,
            event,
        )
        .await
        .map_err(|e| ServingError::Engine(e.to_string()))
    }
}

/// Which table-format backend the query-api binary serves reads from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServingBackend {
    /// DuckLake via embedded DuckDB (default; today's behavior).
    DuckLake,
    /// File-backed Iceberg via the loom-native DataFusion engine.
    Iceberg,
}

/// Parse `LOOM_SERVING_BACKEND`. Unset -> DuckLake. Case-insensitive.
pub fn parse_serving_backend(v: Option<&str>) -> Result<ServingBackend, String> {
    match v.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("ducklake") => Ok(ServingBackend::DuckLake),
        Some("iceberg") => Ok(ServingBackend::Iceberg),
        Some(other) => Err(format!(
            "LOOM_SERVING_BACKEND must be 'ducklake' or 'iceberg', got {other:?}"
        )),
    }
}

/// Flatten DataFusion result batches into the engine-neutral `Rows`. Columns come
/// from the first batch's schema (DataFusion preserves projection order, satisfying
/// the handler's column-order contract); an empty result yields empty `Rows`.
pub fn batches_to_rows(batches: Vec<RecordBatch>) -> Rows {
    let Some(first) = batches.first() else {
        return Rows::default();
    };
    let columns: Vec<String> = first
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut rows = Vec::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            let mut cells = Vec::with_capacity(batch.num_columns());
            for c in 0..batch.num_columns() {
                cells.push(arrow_to_sqlvalue(batch.column(c), r));
            }
            rows.push(cells);
        }
    }
    Rows { columns, rows }
}

/// One Arrow cell -> `SqlValue`. Covers the scalar set loom serves; an unmapped
/// Arrow type falls back to a debug `Text` so a read never panics (mirrors the
/// DuckDB engine's `from_duck` fallback).
fn arrow_to_sqlvalue(array: &dyn Array, row: usize) -> SqlValue {
    if array.is_null(row) {
        return SqlValue::Null;
    }
    macro_rules! dc {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .expect("arrow downcast")
        };
    }
    match array.data_type() {
        DataType::Utf8 => SqlValue::Text(dc!(StringArray).value(row).to_string()),
        DataType::LargeUtf8 => SqlValue::Text(dc!(LargeStringArray).value(row).to_string()),
        DataType::Boolean => SqlValue::Bool(dc!(BooleanArray).value(row)),
        DataType::Int8 => SqlValue::Int(dc!(Int8Array).value(row) as i64),
        DataType::Int16 => SqlValue::Int(dc!(Int16Array).value(row) as i64),
        DataType::Int32 => SqlValue::Int(dc!(Int32Array).value(row) as i64),
        DataType::Int64 => SqlValue::Int(dc!(Int64Array).value(row)),
        DataType::Float32 => SqlValue::Double(dc!(Float32Array).value(row) as f64),
        DataType::Float64 => SqlValue::Double(dc!(Float64Array).value(row)),
        DataType::Date32 => {
            let days = dc!(Date32Array).value(row);
            SqlValue::Date(time::macros::date!(1970 - 01 - 01) + time::Duration::days(days as i64))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = dc!(TimestampMicrosecondArray).value(row);
            let odt = time::OffsetDateTime::from_unix_timestamp_nanos(micros as i128 * 1_000)
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
            SqlValue::Timestamp(time::PrimitiveDateTime::new(odt.date(), odt.time()))
        }
        // Defensive: a type loom doesn't serve as a first-class scalar. Render the
        // single cell (not the whole array) so the fallback is bounded and
        // row-correct; mirrors the DuckDB engine's per-value `from_duck` fallback.
        _ => match ArrayFormatter::try_new(array, &FormatOptions::default()) {
            Ok(fmt) => SqlValue::Text(fmt.value(row).to_string()),
            Err(_) => SqlValue::Null,
        },
    }
}
