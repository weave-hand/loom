//! Iceberg physical type names → loom logical types for the `iceberg_mirror` read path. The
//! closed vocabulary is authoritative: an Iceberg type loom has no logical name for maps to
//! `None` (the adapter surfaces it as an explicit error rather than leaking a raw Iceberg type
//! string into `core`).

use control_plane_core::BaseType;

/// Iceberg primitive type name → loom logical base type (read path, `schema()`).
/// `None` for a type loom has no logical name for (decimal, fixed, uuid, binary, …).
pub fn logical_from_iceberg(physical: &str) -> Option<BaseType> {
    let lower = physical.trim().to_ascii_lowercase();
    match lower.as_str() {
        "int" => Some(BaseType::Integer),
        "long" => Some(BaseType::Long),
        "double" => Some(BaseType::Double),
        "boolean" => Some(BaseType::Boolean),
        "string" => Some(BaseType::String),
        "date" => Some(BaseType::Date),
        // Iceberg spells microsecond timestamps "timestamp"/"timestamptz"; loom maps both to
        // its single logical `timestamp`.
        "timestamp" | "timestamptz" => Some(BaseType::Timestamp),
        // A `list<float>` vector column is mirrored as `vector(N)` (N from the field doc,
        // see `iceberg_mirror::columns_of`); reuse the core codec to parse the dimension.
        v if v.starts_with("vector(") => control_plane_core::resolve_logical(v),
        _ => None,
    }
}

/// loom logical type name -> Iceberg primitive type name (write path, used to
/// project `iceberg_mirror.column` rows for an inline-only table). The inverse of
/// `logical_from_iceberg`. `None` for a name loom has no Iceberg mapping for.
pub fn iceberg_physical_type(logical: &str) -> Option<&'static str> {
    match logical.trim().to_ascii_lowercase().as_str() {
        "integer" => Some("int"),
        "long" => Some("long"),
        "double" => Some("double"),
        "boolean" => Some("boolean"),
        "string" => Some("string"),
        "date" => Some("date"),
        "timestamp" => Some("timestamp"),
        _ => None,
    }
}

/// loom logical type -> the `iceberg_mirror.column.column_type` string written by
/// BOTH the cold (Parquet) and inline write paths. A `vector(N)` column keeps its
/// parameterized form verbatim — the dimension lives in this text and is decoded
/// back by `logical_from_iceberg` — while every other type maps to its Iceberg
/// primitive name. `None` if loom has no Iceberg mapping for the type.
pub fn mirror_column_type(logical: &str) -> Option<String> {
    match control_plane_core::resolve_logical(logical) {
        Some(control_plane_core::BaseType::Vector(n)) => Some(format!("vector({n})")),
        _ => iceberg_physical_type(logical).map(str::to_string),
    }
}

/// loom logical type name -> Postgres column type for the per-table inline storage
/// (`iceberg_mirror.inline_<table_id>`). `None` for an unsupported name.
pub fn pg_type_for(logical: &str) -> Option<&'static str> {
    match logical.trim().to_ascii_lowercase().as_str() {
        "integer" => Some("integer"),
        "long" => Some("bigint"),
        "double" => Some("double precision"),
        "boolean" => Some("boolean"),
        "string" => Some("text"),
        "date" => Some("date"),
        "timestamp" => Some("timestamp"),
        // A vector(N) column stores as a Postgres real[] — native f32, exact and
        // compact. The dimension N is carried by the mirror column_type text, not
        // the Postgres type, so this arm is dimension-independent.
        v if v.starts_with("vector(") => Some("real[]"),
        _ => None,
    }
}
