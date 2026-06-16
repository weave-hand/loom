use control_plane_core::StatValue;
use control_plane_postgres::ducklake_type::to_ducklake_stat_string;

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
