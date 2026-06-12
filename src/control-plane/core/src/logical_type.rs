//! loom's logical-type vocabulary: the base scalar types, their DuckLake physical
//! affinities, and the semantic aliases. Used by dataset->model binding to check a
//! landed physical column satisfies a declared logical property type. Pure logic,
//! no I/O. The vocabulary is CLOSED: an unrecognized logical type is an error, never
//! a silent pass — that keeps the ontology authoritative.
//!
//! NOTE: this vocabulary also classifies how each type renders on the JSON wire
//! (see `JsonRepr` / `json_repr_of`): Date/Timestamp -> ISO-8601 strings, Long ->
//! JSON string to keep int64 precision past 2^53. That is a classification only;
//! the actual serde_json construction lives at the query-api boundary where the
//! scalar values are, so core stays JSON-free.

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

/// How a logical type renders on the JSON wire. A classification only — the actual
/// `serde_json` construction happens where the scalar values live (query-api), so
/// `core` needs no JSON dependency.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsonRepr {
    /// Integer, Double -> JSON number.
    Number,
    /// Long -> JSON string (int64 exceeds JSON's 2^53 safe-integer range).
    NumericString,
    /// Boolean -> JSON bool.
    Bool,
    /// String (+ aliases) -> JSON string.
    PlainString,
    /// Date -> ISO-8601 date string (YYYY-MM-DD).
    IsoDate,
    /// Timestamp -> ISO-8601 datetime string (YYYY-MM-DDThh:mm:ss).
    IsoTimestamp,
}

impl BaseType {
    /// The DuckLake physical type strings (canonical lowercase) that satisfy this
    /// base type. Exact-match, no implicit widening (Integer is 32-bit, Long 64-bit).
    pub fn physical_affinity(self) -> &'static [&'static str] {
        match self {
            BaseType::Integer => &["int32"],
            BaseType::Long => &["int64"],
            BaseType::Double => &["float64"],
            BaseType::Boolean => &["boolean"],
            BaseType::String => &["varchar"],
            BaseType::Date => &["date"],
            BaseType::Timestamp => &["timestamp"],
        }
    }

    /// How a value of this base type renders on the JSON wire.
    pub fn json_repr(self) -> JsonRepr {
        match self {
            BaseType::Integer | BaseType::Double => JsonRepr::Number,
            BaseType::Long => JsonRepr::NumericString,
            BaseType::Boolean => JsonRepr::Bool,
            BaseType::String => JsonRepr::PlainString,
            BaseType::Date => JsonRepr::IsoDate,
            BaseType::Timestamp => JsonRepr::IsoTimestamp,
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

/// The JSON wire rendering for a logical type name (base or alias, case-insensitively).
/// `Err(UnknownLogicalType)` if loom does not recognize the type — callers fall back
/// to a best-effort natural rendering rather than failing a permitted read.
pub fn json_repr_of(logical_ty: &str) -> Result<JsonRepr, UnknownLogicalType> {
    resolve_logical(logical_ty)
        .map(BaseType::json_repr)
        .ok_or_else(|| UnknownLogicalType(logical_ty.trim().to_string()))
}
