use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use datafusion_io::decode_ipc;

/// Encode a small batch to an Arrow IPC stream via `StreamWriter`.
fn encode_ipc(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    buf
}

#[test]
fn round_trips_schema_and_rows() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
        ],
    )
    .unwrap();

    let bytes = encode_ipc(&batch);
    let (decoded_schema, batches) = decode_ipc(&bytes).unwrap();

    assert_eq!(decoded_schema.as_ref(), schema.as_ref());
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3);
    assert_eq!(batches[0].num_columns(), 2);
}

#[test]
fn garbage_bytes_return_err_not_panic() {
    let garbage = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    assert!(decode_ipc(&garbage).is_err());
}

#[test]
fn truncated_stream_returns_err_not_panic() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let bytes = encode_ipc(&batch);
    // Chop off the tail of the stream so the reader hits an incomplete message.
    #[expect(
        clippy::integer_division,
        reason = "test-only truncation midpoint; precision loss is immaterial"
    )]
    let half = bytes.len() / 2;
    let truncated = &bytes[..half];
    assert!(decode_ipc(truncated).is_err());
}
