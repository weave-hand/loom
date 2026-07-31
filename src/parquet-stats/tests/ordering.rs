//! Unit: the StatValue partial order used to fold per-row-group bounds into a
//! file-wide min/max. The mismatched-variant branch is unreachable through
//! `column_stats` (a Parquet column has one physical type), so it is covered here.

use std::cmp::Ordering;

use control_plane_core::snapshot::StatValue;
use parquet_stats::stat_partial_cmp;

#[test]
fn orders_same_typed_values() {
    assert_eq!(
        stat_partial_cmp(&StatValue::I64(1), &StatValue::I64(2)),
        Some(Ordering::Less)
    );
    assert_eq!(
        stat_partial_cmp(&StatValue::I32(5), &StatValue::I32(5)),
        Some(Ordering::Equal)
    );
    assert_eq!(
        stat_partial_cmp(&StatValue::Str("b".into()), &StatValue::Str("a".into())),
        Some(Ordering::Greater)
    );
    assert_eq!(
        stat_partial_cmp(&StatValue::Bool(false), &StatValue::Bool(true)),
        Some(Ordering::Less)
    );
    assert_eq!(
        stat_partial_cmp(&StatValue::F32(1.5), &StatValue::F32(2.5)),
        Some(Ordering::Less)
    );
    assert_eq!(
        stat_partial_cmp(&StatValue::F64(2.5), &StatValue::F64(1.5)),
        Some(Ordering::Greater)
    );
}

#[test]
fn mismatched_variants_are_incomparable() {
    assert_eq!(
        stat_partial_cmp(&StatValue::I32(1), &StatValue::I64(1)),
        None
    );
    assert_eq!(
        stat_partial_cmp(&StatValue::Str("1".into()), &StatValue::I64(1)),
        None
    );
    assert_eq!(
        stat_partial_cmp(&StatValue::Bool(true), &StatValue::F64(1.0)),
        None
    );
}

#[test]
fn nan_is_incomparable() {
    assert_eq!(
        stat_partial_cmp(&StatValue::F64(f64::NAN), &StatValue::F64(1.0)),
        None
    );
    assert_eq!(
        stat_partial_cmp(&StatValue::F32(1.0), &StatValue::F32(f32::NAN)),
        None
    );
}
