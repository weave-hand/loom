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
