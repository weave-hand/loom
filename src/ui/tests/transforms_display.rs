use loom_ui_core::{
    BadgeTone, Status, TransformKind, clamp_drawer_width, kind_badge_label, run_state_status,
    run_state_tone, trigger_label,
};

#[test]
fn state_maps_to_tone() {
    assert_eq!(run_state_tone("succeeded"), BadgeTone::Success);
    assert_eq!(run_state_tone("failed"), BadgeTone::Danger);
    assert_eq!(run_state_tone("running"), BadgeTone::Info);
    assert_eq!(run_state_tone("queued"), BadgeTone::Neutral);
    assert_eq!(run_state_tone("weird"), BadgeTone::Neutral);
}

#[test]
fn state_maps_to_status() {
    assert_eq!(run_state_status("succeeded"), Status::Ok);
    assert_eq!(run_state_status("failed"), Status::Error);
    assert_eq!(run_state_status("running"), Status::Warn);
    assert_eq!(run_state_status("queued"), Status::Warn);
}

#[test]
fn trigger_labels() {
    assert_eq!(trigger_label("manual"), "Manual");
    assert_eq!(trigger_label("data-trigger"), "Data trigger");
    assert_eq!(trigger_label("ad-hoc"), "Ad-hoc");
    assert_eq!(trigger_label("nope"), "Unknown");
}

#[test]
fn kind_labels() {
    assert_eq!(kind_badge_label(TransformKind::Physical), "Physical");
    assert_eq!(kind_badge_label(TransformKind::Typed), "Typed");
}

#[test]
fn drawer_width_clamps_both_ends_and_negatives() {
    assert_eq!(clamp_drawer_width(600, 360, 900), 600);
    assert_eq!(clamp_drawer_width(100, 360, 900), 360);
    assert_eq!(clamp_drawer_width(1200, 360, 900), 900);
    assert_eq!(clamp_drawer_width(-50, 360, 900), 360);
}
