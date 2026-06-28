//! Forward logical-name -> Iceberg-type-name and logical-name -> Postgres-type maps
//! for the inline write path. `rust_test` (pure logic).

use control_plane_postgres::iceberg_type::{
    iceberg_physical_type, mirror_column_type, pg_type_for,
};

#[test]
fn iceberg_names_cover_the_scalar_set() {
    for (logical, iceberg) in [
        ("integer", "int"),
        ("long", "long"),
        ("double", "double"),
        ("boolean", "boolean"),
        ("string", "string"),
        ("date", "date"),
        ("timestamp", "timestamp"),
    ] {
        assert_eq!(iceberg_physical_type(logical), Some(iceberg), "{logical}");
    }
    assert_eq!(iceberg_physical_type("decimal"), None);
}

#[test]
fn pg_types_cover_the_scalar_set() {
    for (logical, pg) in [
        ("integer", "integer"),
        ("long", "bigint"),
        ("double", "double precision"),
        ("boolean", "boolean"),
        ("string", "text"),
        ("date", "date"),
        ("timestamp", "timestamp"),
    ] {
        assert_eq!(pg_type_for(logical), Some(pg), "{logical}");
    }
    assert_eq!(pg_type_for("decimal"), None);
}

#[test]
fn vector_maps_to_real_array_and_keeps_dimension() {
    // Inline Postgres storage type: a native f32 array, dimension-independent.
    assert_eq!(pg_type_for("vector(4)"), Some("real[]"));
    assert_eq!(pg_type_for("vector(1536)"), Some("real[]"));

    // The mirror `column_type` text preserves the parameterized form for BOTH
    // write paths (the dimension lives here and is decoded by logical_from_iceberg).
    assert_eq!(
        mirror_column_type("vector(4)").as_deref(),
        Some("vector(4)")
    );
    assert_eq!(
        mirror_column_type("vector(1536)").as_deref(),
        Some("vector(1536)")
    );
    assert_eq!(mirror_column_type("long").as_deref(), Some("long"));
    assert_eq!(
        mirror_column_type("timestamp").as_deref(),
        Some("timestamp")
    );
    assert_eq!(mirror_column_type("decimal"), None);
}
