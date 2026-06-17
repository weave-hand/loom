//! Iceberg physical type names → loom logical types for the `iceberg_mirror` read path. The
//! closed vocabulary is authoritative: an Iceberg type loom has no logical name for maps to
//! `None` (the adapter surfaces it as an explicit error rather than leaking a raw Iceberg type
//! string into `core`). Mirror of `ducklake_type::logical_from_ducklake`.

use control_plane_core::BaseType;

/// Iceberg primitive type name → loom logical base type (read path, `schema()`).
/// `None` for a type loom has no logical name for (decimal, fixed, uuid, binary, …).
pub fn logical_from_iceberg(physical: &str) -> Option<BaseType> {
    match physical.trim().to_ascii_lowercase().as_str() {
        "int" => Some(BaseType::Integer),
        "long" => Some(BaseType::Long),
        "double" => Some(BaseType::Double),
        "boolean" => Some(BaseType::Boolean),
        "string" => Some(BaseType::String),
        "date" => Some(BaseType::Date),
        // Iceberg spells microsecond timestamps "timestamp"/"timestamptz"; loom maps both to
        // its single logical `timestamp`.
        "timestamp" | "timestamptz" => Some(BaseType::Timestamp),
        _ => None,
    }
}
