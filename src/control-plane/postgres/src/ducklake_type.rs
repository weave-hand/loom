//! DuckLake physical-dialect mapping: the encoding that used to live in `core` and
//! in `datafusion-io::write`. Confined here so `core` stays format-neutral.

use control_plane_core::{BaseType, StatValue};

/// loom logical base type → DuckLake physical type string (single-valued; was
/// `core::BaseType::physical_affinity`). Used on the write path (`create_table`).
pub fn ducklake_physical_type(base: BaseType) -> &'static str {
    match base {
        BaseType::Integer => "int32",
        BaseType::Long => "int64",
        BaseType::Double => "float64",
        BaseType::Boolean => "boolean",
        BaseType::String => "varchar",
        BaseType::Date => "date",
        BaseType::Timestamp => "timestamp",
        // DuckLake vector storage (`FLOAT[N]`) is deferred (`fut-vector-ducklake`);
        // vectors are Iceberg-only, so the landing/bind path rejects a DuckLake vector
        // before it can reach here. Guard the unreachable mapping explicitly.
        BaseType::Vector(_) => panic!("DuckLake vector storage is deferred (fut-vector-ducklake)"),
    }
}

/// DuckLake physical type string → loom logical base type (read path, `schema()`).
/// `None` for a physical type loom has no logical name for (surfaced as an error,
/// never leaked back into core as a raw string).
pub fn logical_from_ducklake(physical: &str) -> Option<BaseType> {
    match physical.trim().to_ascii_lowercase().as_str() {
        "int32" => Some(BaseType::Integer),
        "int64" => Some(BaseType::Long),
        "float64" => Some(BaseType::Double),
        "boolean" => Some(BaseType::Boolean),
        "varchar" => Some(BaseType::String),
        "date" => Some(BaseType::Date),
        "timestamp" => Some(BaseType::Timestamp),
        _ => None,
    }
}

/// Encode a typed stat bound as DuckLake's VARCHAR stat string. Byte-identical to the
/// former `datafusion_io::write::Bound::to_stat_string` (the interop oracle is the test).
pub fn to_ducklake_stat_string(v: &StatValue) -> String {
    match v {
        StatValue::Bool(b) => b.to_string(),
        StatValue::I32(n) => n.to_string(),
        StatValue::I64(n) => n.to_string(),
        StatValue::F32(f) => f.to_string(),
        StatValue::F64(f) => f.to_string(),
        StatValue::Str(s) => s.clone(),
    }
}
