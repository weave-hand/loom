use control_plane_core::BaseType;
use control_plane_postgres::iceberg_type::logical_from_iceberg;

#[test]
fn logical_from_iceberg_maps_the_closed_vocabulary() {
    assert_eq!(logical_from_iceberg("int"), Some(BaseType::Integer));
    assert_eq!(logical_from_iceberg("long"), Some(BaseType::Long));
    assert_eq!(logical_from_iceberg("double"), Some(BaseType::Double));
    assert_eq!(logical_from_iceberg("boolean"), Some(BaseType::Boolean));
    assert_eq!(logical_from_iceberg("string"), Some(BaseType::String));
    assert_eq!(logical_from_iceberg("date"), Some(BaseType::Date));
    assert_eq!(logical_from_iceberg("timestamp"), Some(BaseType::Timestamp));
    assert_eq!(
        logical_from_iceberg("timestamptz"),
        Some(BaseType::Timestamp)
    );
}

#[test]
fn logical_from_iceberg_rejects_unknown() {
    assert_eq!(logical_from_iceberg("fixed[16]"), None);
    assert_eq!(logical_from_iceberg("decimal(9,2)"), None);
    assert_eq!(logical_from_iceberg(""), None);
}

/// Pin the Iceberg `PrimitiveType` Display strings that `logical_from_iceberg` keys on, so an
/// `iceberg` crate bump that changes the spelling can't silently break the type round-trip. The
/// seeder only emits `long`/`string` today, so without this the other branches are unguarded;
/// the slice-2 write path relies on the full set.
#[test]
fn primitive_type_display_matches_decode_keys() {
    use iceberg::spec::PrimitiveType;
    for (prim, expected) in [
        (PrimitiveType::Int, "int"),
        (PrimitiveType::Long, "long"),
        (PrimitiveType::Double, "double"),
        (PrimitiveType::Boolean, "boolean"),
        (PrimitiveType::String, "string"),
        (PrimitiveType::Date, "date"),
        (PrimitiveType::Timestamp, "timestamp"),
        (PrimitiveType::Timestamptz, "timestamptz"),
    ] {
        assert_eq!(
            prim.to_string(),
            expected,
            "Iceberg PrimitiveType Display drift"
        );
        assert!(
            logical_from_iceberg(&prim.to_string()).is_some(),
            "{expected} must decode to a loom logical type"
        );
    }
}
