use control_plane_core::{BaseType, UnknownLogicalType, resolve_logical, satisfies};

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
}

#[test]
fn satisfies_errors_on_unknown_logical_type() {
    assert_eq!(
        satisfies("Money", "double"),
        Err(UnknownLogicalType("Money".into()))
    );
}
