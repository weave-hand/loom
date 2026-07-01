use control_plane_core::{
    ControlPlaneError, Cursor, DatasetRef, LINEAGE_MAX_DEPTH, check_depth, decode_dataset_cursor,
    decode_event_cursor, encode_dataset_cursor, encode_event_cursor,
};

fn ds(ns: &str, name: &str) -> DatasetRef {
    DatasetRef {
        namespace: ns.into(),
        name: name.into(),
    }
}

#[test]
fn dataset_cursor_round_trips() {
    let d = ds("warehouse", "main.orders");
    let c = encode_dataset_cursor(&d);
    assert_eq!(decode_dataset_cursor(&c).unwrap(), d);
}

#[test]
fn dataset_cursor_handles_special_chars() {
    // namespace/name may contain commas, quotes, brackets — the encoding must round-trip them.
    let d = ds("s3://b,x", r#"weird"]name"#);
    let c = encode_dataset_cursor(&d);
    assert_eq!(decode_dataset_cursor(&c).unwrap(), d);
}

#[test]
fn event_cursor_round_trips() {
    let c = encode_event_cursor(42);
    assert_eq!(decode_event_cursor(&c).unwrap(), 42);
}

#[test]
fn malformed_cursor_is_validation_error() {
    let bad = Cursor("not-a-valid-cursor".into());
    assert!(matches!(
        decode_dataset_cursor(&bad),
        Err(ControlPlaneError::Validation(_))
    ));
    assert!(matches!(
        decode_event_cursor(&bad),
        Err(ControlPlaneError::Validation(_))
    ));
}

#[test]
fn check_depth_bounds() {
    assert!(check_depth(1).is_ok());
    assert!(check_depth(LINEAGE_MAX_DEPTH).is_ok());
    assert!(matches!(
        check_depth(0),
        Err(ControlPlaneError::Validation(_))
    ));
    assert!(matches!(
        check_depth(LINEAGE_MAX_DEPTH + 1),
        Err(ControlPlaneError::Validation(_))
    ));
}
