//! loom's logical-type vocabulary: the base scalar types and their semantic aliases.
//! Used by dataset→model binding to check a landed column's logical type satisfies a
//! declared property's logical type. Pure logic, no I/O. The vocabulary is CLOSED: an
//! unrecognized logical type is an error, never a silent pass — that keeps the ontology
//! authoritative. Physical mappings: logical ↔ Iceberg type strings live in the
//! postgres adapter (`control_plane_postgres::iceberg_type`); logical → Arrow
//! `DataType` lives HERE (`BaseType::arrow_data_type`) as the single map every
//! conversion site consults (adapter inline reads, engine serving, flight export,
//! ingest inference).
//!
//! NOTE: this vocabulary also classifies how each type renders on the JSON wire
//! (see `JsonRepr` / `json_repr_of`): Date/Timestamp -> ISO-8601 strings, Long ->
//! JSON string to keep int64 precision past 2^53. That is a classification only;
//! the actual serde_json construction lives at the query-api boundary where the
//! scalar values are, so core stays JSON-free.

/// A loom base scalar logical type. `Vector(N)` is the one parameterized,
/// non-primitive member — a dense `FixedSizeList<f32, N>` embedding stored as Iceberg
/// `list<float>`; the `u32` is the dimension `N`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BaseType {
    Integer,
    Long,
    Double,
    Boolean,
    String,
    Date,
    Timestamp,
    /// A dense `f32` vector of fixed dimension `N`. Logical name `vector(N)`.
    Vector(u32),
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
    /// Vector -> JSON array of numbers (f32 elements).
    FloatArray,
}

impl BaseType {
    /// The canonical lowercase logical name (inverse of `resolve_logical`). A `String`
    /// because `Vector(N)` renders the dynamic `vector(N)` form; the primitives are
    /// fixed names.
    pub fn canonical_name(self) -> String {
        match self {
            BaseType::Integer => "integer".to_string(),
            BaseType::Long => "long".to_string(),
            BaseType::Double => "double".to_string(),
            BaseType::Boolean => "boolean".to_string(),
            BaseType::String => "string".to_string(),
            BaseType::Date => "date".to_string(),
            BaseType::Timestamp => "timestamp".to_string(),
            BaseType::Vector(n) => format!("vector({n})"),
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
            BaseType::Vector(_) => JsonRepr::FloatArray,
        }
    }

    /// THE authoritative loom-logical → Arrow (58) physical mapping. Every
    /// conversion site (postgres adapter reads, engine serving schemas, query-api
    /// flight export, datafusion-io inference) consults this — never a local copy
    /// (see iss-pg-provider-vector-drift for what a drifted copy cost). Canonical,
    /// non-`*View` variants, so decoded values match the file-Parquet side.
    pub fn arrow_data_type(self) -> arrow_schema::DataType {
        use arrow_schema::{DataType, TimeUnit};
        match self {
            BaseType::Integer => DataType::Int32,
            BaseType::Long => DataType::Int64,
            BaseType::Double => DataType::Float64,
            BaseType::Boolean => DataType::Boolean,
            BaseType::String => DataType::Utf8,
            BaseType::Date => DataType::Date32,
            BaseType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            BaseType::Vector(_) => DataType::List(std::sync::Arc::new(vector_list_field())),
        }
    }

    /// `Sum`/`Avg` apply only to numeric base types.
    #[must_use]
    pub fn is_numeric(self) -> bool {
        matches!(self, BaseType::Integer | BaseType::Long | BaseType::Double)
    }

    /// `Min`/`Max` apply to any totally-ordered base type — every base type except
    /// `Boolean`. (Preserves the pre-decomposition bind.rs semantics: `Vector`,
    /// `String`, `Date`, `Timestamp` are all treated as ordered here.)
    #[must_use]
    pub fn is_ordered(self) -> bool {
        !matches!(self, BaseType::Boolean)
    }
}

/// The Arrow list-child field of every loom `Vector(N)` column, in memory and on
/// the wire: `"item"`, non-null `Float32`. `"item"` is arrow-rs's own default
/// (`ListBuilder`, `Field::new_list_field`), so wire batches match without
/// relabeling. Iceberg's Parquet storage uses `"element"` (+ `PARQUET:field_id`);
/// that relabel happens ONLY at the storage write boundary
/// (`iceberg_landing::coerce_batch_to_ice`) — DataFusion's schema adapter casts
/// the name back on read.
pub fn vector_list_field() -> arrow_schema::Field {
    arrow_schema::Field::new("item", arrow_schema::DataType::Float32, false)
}

/// Parse the parameterized `vector(N)` logical form (case-insensitive) to its
/// dimension. `None` if `s` is not a well-formed `vector(<u32>)` (e.g. `vector()`,
/// `vector(x)`, a negative, or overflow).
fn parse_vector(s: &str) -> Option<u32> {
    let inner = s.strip_prefix("vector(")?.strip_suffix(')')?;
    let n: u32 = inner.trim().parse().ok()?;
    Some(n)
}

/// Resolve a logical type name (a base name or a known semantic alias,
/// case-insensitively) to its BaseType. `None` if loom does not recognize it.
pub fn resolve_logical(ty: &str) -> Option<BaseType> {
    let lower = ty.trim().to_ascii_lowercase();
    match lower.as_str() {
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
        // parameterized: vector(N)
        other if other.starts_with("vector(") => parse_vector(other).map(BaseType::Vector),
        _ => None,
    }
}

/// Does a column's loom LOGICAL type satisfy a declared logical property type? Both
/// names are resolved to `BaseType` and compared (no implicit widening: Integer is
/// 32-bit, Long 64-bit — distinct base types). `Err(UnknownLogicalType)` if the
/// PROPERTY's logical type is unrecognized (an authoring error); an unrecognized
/// column type simply does not satisfy (`Ok(false)`).
pub fn satisfies(property_ty: &str, column_ty: &str) -> Result<bool, UnknownLogicalType> {
    let want = resolve_logical(property_ty)
        .ok_or_else(|| UnknownLogicalType(property_ty.trim().to_string()))?;
    Ok(resolve_logical(column_ty) == Some(want))
}

/// The JSON wire rendering for a logical type name (base or alias, case-insensitively).
/// `Err(UnknownLogicalType)` if loom does not recognize the type — callers fall back
/// to a best-effort natural rendering rather than failing a permitted read.
pub fn json_repr_of(logical_ty: &str) -> Result<JsonRepr, UnknownLogicalType> {
    resolve_logical(logical_ty)
        .map(BaseType::json_repr)
        .ok_or_else(|| UnknownLogicalType(logical_ty.trim().to_string()))
}
