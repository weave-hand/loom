//! ColumnSpec must round-trip through serde_json — it crosses the engine wire as a
//! JSON string in WriteObject/OverwriteTable requests.

use control_plane_core::ColumnSpec;

#[test]
fn column_spec_json_round_trip() {
    let specs = vec![
        ColumnSpec {
            name: "id".into(),
            ty: "Long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "name".into(),
            ty: "String".into(),
            nullable: true,
        },
    ];
    let json = serde_json::to_string(&specs).expect("serialize");
    let back: Vec<ColumnSpec> = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(specs, back);
}
