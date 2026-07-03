//! Single authoritative Arrow-IPC stream decode. The three per-crate copies
//! (ingest HTTP, engine-serving action writer, postgres iceberg_landing) collapse
//! to this — callers on the umbrella `arrow` crate call it directly; the postgres
//! crate stops decoding entirely (it now takes pre-decoded batches).
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;

/// Decode an Arrow IPC stream into its schema and batches.
pub fn decode_ipc(
    body: &[u8],
) -> Result<(Arc<Schema>, Vec<RecordBatch>), arrow::error::ArrowError> {
    let reader = StreamReader::try_new(Cursor::new(body), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}
