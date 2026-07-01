use loom_ui_core::{Align, BadgeTone, ButtonVariant, Status, format_count};

#[test]
fn format_count_scales_and_rounds() {
    assert_eq!(format_count(2_410_000), "2.41M");
    assert_eq!(format_count(18_200), "18.2K");
    assert_eq!(format_count(880_000), "880K");
    assert_eq!(format_count(9_700), "9.7K");
    assert_eq!(format_count(142_000), "142K");
    assert_eq!(format_count(999), "999");
    assert_eq!(format_count(0), "0");
    assert_eq!(format_count(1_000), "1K");
    assert_eq!(format_count(1_000_000), "1M");
    assert_eq!(format_count(999_999), "1M");
    assert_eq!(format_count(999_500), "1M"); // rounds up across the boundary
    assert_eq!(format_count(994_000), "994K"); // stays in K tier, no false promotion
}

#[test]
fn status_maps_to_token_var() {
    assert_eq!(Status::Ok.css_var(), "--loom-ok");
    assert_eq!(Status::Warn.css_var(), "--loom-warn");
    assert_eq!(Status::Error.css_var(), "--loom-danger");
}

#[test]
fn button_variant_modifier() {
    assert_eq!(ButtonVariant::Primary.modifier(), "primary");
    assert_eq!(ButtonVariant::Secondary.modifier(), "secondary");
    assert_eq!(ButtonVariant::Ghost.modifier(), "ghost");
}

#[test]
fn badge_tone_maps_to_token_var() {
    assert_eq!(BadgeTone::Neutral.css_var(), "--loom-text-mut");
    assert_eq!(BadgeTone::Info.css_var(), "--loom-accent");
    assert_eq!(BadgeTone::Pii.css_var(), "--loom-danger");
    assert_eq!(BadgeTone::Success.css_var(), "--loom-ok");
    assert_eq!(BadgeTone::Warning.css_var(), "--loom-warn");
    assert_eq!(BadgeTone::Danger.css_var(), "--loom-danger");
}

#[test]
fn align_maps_to_css_value() {
    assert_eq!(Align::Start.css_value(), "flex-start");
    assert_eq!(Align::End.css_value(), "flex-end");
}
