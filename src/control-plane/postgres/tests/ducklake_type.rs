use control_plane_core::{BaseType, StatValue};
use control_plane_postgres::ducklake_type::{
    ducklake_physical_type, logical_from_ducklake, to_ducklake_stat_string,
};

#[test]
fn physical_and_logical_round_trip() {
    for base in [
        BaseType::Integer,
        BaseType::Long,
        BaseType::Double,
        BaseType::Boolean,
        BaseType::String,
        BaseType::Date,
        BaseType::Timestamp,
    ] {
        let phys = ducklake_physical_type(base);
        assert_eq!(logical_from_ducklake(phys), Some(base));
    }
}

#[test]
fn unknown_physical_type_has_no_logical() {
    assert_eq!(logical_from_ducklake("decimal(10,2)"), None);
}

#[test]
fn stat_strings_match_ducklake_varchar_encoding() {
    assert_eq!(to_ducklake_stat_string(&StatValue::I64(42)), "42");
    assert_eq!(to_ducklake_stat_string(&StatValue::I32(-7)), "-7");
    assert_eq!(to_ducklake_stat_string(&StatValue::Bool(true)), "true");
    assert_eq!(to_ducklake_stat_string(&StatValue::F64(1.5)), "1.5");
    assert_eq!(to_ducklake_stat_string(&StatValue::F32(2.25)), "2.25");
    assert_eq!(
        to_ducklake_stat_string(&StatValue::Str("ann".into())),
        "ann"
    );
}
