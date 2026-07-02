use control_plane_core::{
    BaseType, JsonRepr, UnknownLogicalType, json_repr_of, resolve_logical, satisfies,
    vector_list_field,
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
fn satisfies_matches_same_base_type() {
    assert_eq!(satisfies("Long", "long"), Ok(true));
    assert_eq!(satisfies("Integer", "integer"), Ok(true));
    assert_eq!(satisfies("Double", "double"), Ok(true));
    assert_eq!(satisfies("Boolean", "boolean"), Ok(true));
    assert_eq!(satisfies("String", "string"), Ok(true));
    assert_eq!(satisfies("EmailAddress", "string"), Ok(true));
    assert_eq!(satisfies("String", "Url"), Ok(true));
    assert_eq!(satisfies("Date", "date"), Ok(true));
    assert_eq!(satisfies("Timestamp", "timestamp"), Ok(true));
}

#[test]
fn satisfies_rejects_different_base_type() {
    assert_eq!(satisfies("Integer", "long"), Ok(false));
    assert_eq!(satisfies("Long", "integer"), Ok(false));
}

#[test]
fn satisfies_unknown_column_type_does_not_satisfy() {
    assert_eq!(satisfies("Long", "int64"), Ok(false));
}

#[test]
fn satisfies_normalizes_casing_and_whitespace() {
    assert_eq!(satisfies("String", "STRING"), Ok(true));
    assert_eq!(satisfies("Long", " long "), Ok(true));
    assert_eq!(satisfies(" Long ", "long"), Ok(true));
}

#[test]
fn satisfies_errors_on_unknown_property_type() {
    assert_eq!(
        satisfies("Money", "double"),
        Err(UnknownLogicalType("Money".into()))
    );
}

#[test]
fn canonical_name_round_trips_base_names() {
    for name in [
        "integer",
        "long",
        "double",
        "boolean",
        "string",
        "date",
        "timestamp",
    ] {
        let base = resolve_logical(name).unwrap();
        assert_eq!(base.canonical_name(), name);
    }
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

#[test]
fn arrow_data_type_maps_every_base_type() {
    use arrow_schema::{DataType, TimeUnit};
    assert_eq!(BaseType::Integer.arrow_data_type(), DataType::Int32);
    assert_eq!(BaseType::Long.arrow_data_type(), DataType::Int64);
    assert_eq!(BaseType::Double.arrow_data_type(), DataType::Float64);
    assert_eq!(BaseType::Boolean.arrow_data_type(), DataType::Boolean);
    assert_eq!(BaseType::String.arrow_data_type(), DataType::Utf8);
    assert_eq!(BaseType::Date.arrow_data_type(), DataType::Date32);
    assert_eq!(
        BaseType::Timestamp.arrow_data_type(),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
}

#[test]
fn vector_arrow_type_is_list_of_item_float32() {
    use arrow_schema::DataType;
    // The list child is named "item" (arrow-rs's default; loom's wire/in-memory
    // convention). Iceberg's Parquet storage relabels to "element" at the write
    // boundary only (coerce_batch_to_ice) — never in an in-memory schema.
    match BaseType::Vector(4).arrow_data_type() {
        DataType::List(f) => {
            assert_eq!(f.name(), "item");
            assert_eq!(f.data_type(), &DataType::Float32);
            assert!(!f.is_nullable());
        }
        other => panic!("expected List, got {other:?}"),
    }
    assert_eq!(vector_list_field().name(), "item");
}

#[test]
fn is_numeric_covers_only_integer_long_double() {
    use control_plane_core::BaseType::*;
    for b in [Integer, Long, Double] {
        assert!(b.is_numeric(), "{b:?} should be numeric");
    }
    for b in [Boolean, String, Date, Timestamp] {
        assert!(!b.is_numeric(), "{b:?} should not be numeric");
    }
    assert!(!control_plane_core::BaseType::Vector(3).is_numeric());
}

#[test]
fn is_ordered_is_everything_except_boolean() {
    use control_plane_core::BaseType::*;
    assert!(!Boolean.is_ordered());
    for b in [Integer, Long, Double, String, Date, Timestamp] {
        assert!(b.is_ordered(), "{b:?} should be ordered");
    }
    assert!(control_plane_core::BaseType::Vector(3).is_ordered());
}
