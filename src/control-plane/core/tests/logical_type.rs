use control_plane_core::{
    BaseType, JsonRepr, UnknownLogicalType, json_repr_of, resolve_logical, satisfies,
};

#[test]
fn resolves_base_names_case_insensitively() {
    assert_eq!(resolve_logical("Integer"), Some(BaseType::Integer));
    assert_eq!(resolve_logical("integer"), Some(BaseType::Integer));
    assert_eq!(resolve_logical("LONG"), Some(BaseType::Long));
    assert_eq!(resolve_logical("Timestamp"), Some(BaseType::Timestamp));
}

#[test]
fn resolves_semantic_aliases_to_base() {
    assert_eq!(resolve_logical("EmailAddress"), Some(BaseType::String));
    assert_eq!(resolve_logical("Url"), Some(BaseType::String));
    assert_eq!(resolve_logical("PhoneNumber"), Some(BaseType::String));
}

#[test]
fn unknown_logical_type_does_not_resolve() {
    assert_eq!(resolve_logical("Money"), None);
}

#[test]
fn satisfies_matches_physical_affinity() {
    assert_eq!(satisfies("Long", "int64"), Ok(true));
    assert_eq!(satisfies("Integer", "int32"), Ok(true));
    assert_eq!(satisfies("Double", "double"), Ok(true));
    assert_eq!(satisfies("Boolean", "boolean"), Ok(true));
    assert_eq!(satisfies("String", "varchar"), Ok(true));
    assert_eq!(satisfies("EmailAddress", "varchar"), Ok(true));
    assert_eq!(satisfies("Date", "date"), Ok(true));
    assert_eq!(satisfies("Timestamp", "timestamp"), Ok(true));
}

#[test]
fn satisfies_rejects_width_mismatch() {
    assert_eq!(satisfies("Integer", "int64"), Ok(false));
    assert_eq!(satisfies("Long", "int32"), Ok(false));
}

#[test]
fn satisfies_normalizes_physical_casing_and_whitespace() {
    assert_eq!(satisfies("String", "VARCHAR"), Ok(true));
    assert_eq!(satisfies("Long", " int64 "), Ok(true));
    assert_eq!(satisfies(" Long ", "int64"), Ok(true));
}

#[test]
fn satisfies_errors_on_unknown_logical_type() {
    assert_eq!(
        satisfies("Money", "double"),
        Err(UnknownLogicalType("Money".into()))
    );
}

#[test]
fn base_types_classify_to_json_repr() {
    assert_eq!(BaseType::Integer.json_repr(), JsonRepr::Number);
    assert_eq!(BaseType::Double.json_repr(), JsonRepr::Number);
    assert_eq!(BaseType::Long.json_repr(), JsonRepr::NumericString);
    assert_eq!(BaseType::Boolean.json_repr(), JsonRepr::Bool);
    assert_eq!(BaseType::String.json_repr(), JsonRepr::PlainString);
    assert_eq!(BaseType::Date.json_repr(), JsonRepr::IsoDate);
    assert_eq!(BaseType::Timestamp.json_repr(), JsonRepr::IsoTimestamp);
}

#[test]
fn json_repr_of_resolves_names_and_aliases() {
    assert_eq!(json_repr_of("Long"), Ok(JsonRepr::NumericString));
    assert_eq!(json_repr_of("integer"), Ok(JsonRepr::Number));
    assert_eq!(json_repr_of("EmailAddress"), Ok(JsonRepr::PlainString));
    assert_eq!(json_repr_of("  timestamp "), Ok(JsonRepr::IsoTimestamp));
}

#[test]
fn json_repr_of_errors_on_unknown_type() {
    assert_eq!(
        json_repr_of("Money"),
        Err(UnknownLogicalType("Money".into()))
    );
}
