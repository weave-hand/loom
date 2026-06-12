//! loom's logical-type vocabulary: the base scalar types, their DuckLake physical
//! affinities, and the semantic aliases. Used by dataset->model binding to check a
//! landed physical column satisfies a declared logical property type. Pure logic,
//! no I/O. The vocabulary is CLOSED: an unrecognized logical type is an error, never
//! a silent pass — that keeps the ontology authoritative.
//!
//! NOTE: this vocabulary is the natural anchor for a later query-path typed JSON
//! serialization (Date/Timestamp -> ISO-8601 strings, Long -> JSON string to keep
//! int64 precision past 2^53). That wire-encoding axis is intentionally NOT here.

/// A loom base scalar logical type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BaseType {
    Integer,
    Long,
    Double,
    Boolean,
    String,
    Date,
    Timestamp,
}

/// A logical type loom does not recognize (neither a base type nor a known alias).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownLogicalType(pub String);

impl BaseType {
    /// The DuckLake physical type strings (canonical lowercase) that satisfy this
    /// base type. Exact-match, no implicit widening (Integer is 32-bit, Long 64-bit).
    pub fn physical_affinity(self) -> &'static [&'static str] {
        match self {
            BaseType::Integer => &["int32"],
            BaseType::Long => &["int64"],
            BaseType::Double => &["double"],
            BaseType::Boolean => &["boolean"],
            BaseType::String => &["varchar"],
            BaseType::Date => &["date"],
            BaseType::Timestamp => &["timestamp"],
        }
    }
}

/// Resolve a logical type name (a base name or a known semantic alias,
/// case-insensitively) to its BaseType. `None` if loom does not recognize it.
pub fn resolve_logical(ty: &str) -> Option<BaseType> {
    match ty.trim().to_ascii_lowercase().as_str() {
        "integer" => Some(BaseType::Integer),
        "long" => Some(BaseType::Long),
        "double" => Some(BaseType::Double),
        "boolean" => Some(BaseType::Boolean),
        "string" => Some(BaseType::String),
        "date" => Some(BaseType::Date),
        "timestamp" => Some(BaseType::Timestamp),
        // semantic aliases -> base
        "emailaddress" => Some(BaseType::String),
        "url" => Some(BaseType::String),
        "phonenumber" => Some(BaseType::String),
        _ => None,
    }
}

/// Does a DuckLake physical type string satisfy a logical type? Both sides are
/// normalized (trim + lowercase) before comparison. `Err(UnknownLogicalType)` if
/// the logical type is neither a base nor a known alias.
pub fn satisfies(logical_ty: &str, physical_ty: &str) -> Result<bool, UnknownLogicalType> {
    let base = resolve_logical(logical_ty)
        .ok_or_else(|| UnknownLogicalType(logical_ty.trim().to_string()))?;
    let phys = physical_ty.trim().to_ascii_lowercase();
    Ok(base.physical_affinity().contains(&phys.as_str()))
}
