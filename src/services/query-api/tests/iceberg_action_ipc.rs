//! IcebergActionWriter's batch->IPC shaping: build_object_batch + encode_ipc_stream
//! produce an Arrow IPC stream that round-trips back to the same one-row batch.
//! Pure logic (no DB).

use arrow::array::{Array, Int64Array, StringArray};
use arrow::ipc::reader::StreamReader;
use query_api::serving::{SqlValue, build_object_batch};
use query_api::serving_datafusion::encode_ipc_stream;

#[test]
fn batch_encodes_to_ipc_and_round_trips() {
    let (_schema, batch, specs) = build_object_batch(
        &["id".into(), "name".into()],
        &[SqlValue::Int(42), SqlValue::Text("gadget".into())],
        &["Long".into(), "String".into()],
    )
    .expect("batch");
    // ColumnSpec.ty is the loom logical canonical name (what land() consumes).
    assert_eq!(
        specs.iter().map(|s| s.ty.as_str()).collect::<Vec<_>>(),
        vec!["long", "string"]
    );

    let body = encode_ipc_stream(&batch).expect("encode");
    let mut reader = StreamReader::try_new(std::io::Cursor::new(body), None).expect("reader");
    let decoded = reader.next().expect("one batch").expect("ok");
    assert!(reader.next().is_none(), "single-batch stream");
    assert_eq!(decoded.num_rows(), 1);
    assert_eq!(decoded.num_columns(), 2);
    let id = decoded
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id.value(0), 42);
    let name = decoded
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name.value(0), "gadget");
}
