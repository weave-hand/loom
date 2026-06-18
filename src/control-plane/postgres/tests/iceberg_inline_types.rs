//! Forward logical-name -> Iceberg-type-name and logical-name -> Postgres-type maps
//! for the inline write path. `rust_test` (pure logic).

use control_plane_postgres::iceberg_type::{iceberg_physical_type, pg_type_for};

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
