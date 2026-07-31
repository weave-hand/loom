//! `Redacted<T>` must never render its inner value. These assertions are the
//! whole contract: a regression here is a credential in the logs.
use control_plane_core::Redacted;

#[test]
fn debug_renders_the_redacted_marker_only() {
    let secret = Redacted::new("hunter2".to_owned());
    let rendered = format!("{secret:?}");
    assert_eq!(rendered, "<redacted>");
    assert!(
        !rendered.contains("hunter2"),
        "Debug leaked the secret: {rendered}"
    );
}

#[test]
fn alternate_debug_also_redacts() {
    // `{:#?}` is a distinct formatter code path and an easy miss.
    let secret = Redacted::new("hunter2".to_owned());
    let rendered = format!("{secret:#?}");
    assert_eq!(rendered, "<redacted>");
    assert!(
        !rendered.contains("hunter2"),
        "alternate Debug leaked the secret: {rendered}"
    );
}

#[test]
fn debug_reveals_neither_length_nor_type() {
    // A length is a real hint against a password, so short and long secrets must
    // render identically.
    let short = Redacted::new("a".to_owned());
    let long = Redacted::new("a".repeat(512));
    assert_eq!(format!("{short:?}"), format!("{long:?}"));
}

#[test]
fn expose_round_trips_the_value() {
    let secret = Redacted::new("hunter2".to_owned());
    assert_eq!(secret.expose(), "hunter2");
    assert_eq!(secret.into_inner(), "hunter2");
}

#[test]
fn from_wraps_and_equality_is_by_inner_value() {
    let a: Redacted<String> = "same".to_owned().into();
    let b = Redacted::new("same".to_owned());
    let c = Redacted::new("other".to_owned());
    assert_eq!(a, b);
    assert_ne!(a, c);
}

#[test]
fn clone_preserves_the_value() {
    let a = Redacted::new("v".to_owned());
    let b = a.clone();
    assert_eq!(b.expose(), "v");
}
