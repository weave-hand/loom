//! The `employees` demo fixture stays a valid Arrow IPC stream that decodes to
//! the schema `land_model` infers a clean object type from. Pure-logic guard —
//! it round-trips [`loom_test_seed::employees_ipc`] through the same
//! `StreamReader` shape `datafusion_io::decode_ipc` uses, with no fixture.

use std::io::Cursor;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use loom_test_seed::{employees_batch, employees_ipc};

#[test]
fn employees_ipc_roundtrips_to_eight_rows() {
    let bytes = employees_ipc();
    let reader = StreamReader::try_new(Cursor::new(bytes), None).expect("stream reader");
    let batches: Vec<RecordBatch> = reader.map(|b| b.expect("batch")).collect();

    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 8, "8 demo rows");

    // Schema matches the in-memory batch: the columns land_model infers the type from.
    let schema = employees_batch().schema();
    assert_eq!(batches[0].schema(), schema);
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["id", "name", "department", "salary", "active"]);
}
