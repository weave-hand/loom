//! Arrow→Rows helpers for the query-api read path: `batches_to_rows` and
//! `arrow_to_sqlvalue`. These are postgres-free utilities used by `engine_client.rs`
//! (the Flight SQL read client) and the read-path tests. The Iceberg action writer
//! that previously lived here has been relocated to `engine-serving`.

use arrow::array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::util::display::{ArrayFormatter, FormatOptions};

use crate::serving::{Rows, SqlValue};

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
/// Arrow type falls back to a debug `Text` so a read never panics.
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
        // row-correct.
        _ => match ArrayFormatter::try_new(array, &FormatOptions::default()) {
            Ok(fmt) => SqlValue::Text(fmt.value(row).to_string()),
            Err(_) => SqlValue::Null,
        },
    }
}
