//! DuckLake physical-dialect mapping: the encoding that used to live in `core` and
//! in `datafusion-io::write`. Confined here so `core` stays format-neutral.

use control_plane_core::StatValue;

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
