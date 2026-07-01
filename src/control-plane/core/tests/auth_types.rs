//! Compile/shape guard for the auth domain types. Behavior is covered by the
//! testkit `auth_contract`; this just pins the public type surface.
use control_plane_core::{NewUser, PasswordCredential, SubjectId, UserSummary};

#[test]
fn new_user_and_credential_construct() {
    let u = NewUser {
        subject_id: SubjectId("alice".into()),
        username: "alice".into(),
        password_phc: "$argon2id$v=19$m=19456,t=2,p=1$abc$def".into(),
    };
    assert_eq!(u.subject_id, SubjectId("alice".into()));
    assert_eq!(u.username, "alice");

    let c = PasswordCredential {
        subject_id: u.subject_id.clone(),
        password_phc: u.password_phc.clone(),
    };
    assert_eq!(c.subject_id, u.subject_id);
    assert_eq!(c.password_phc, u.password_phc);
}

#[test]
fn user_summary_fields_are_public() {
    let s = UserSummary {
        subject_id: SubjectId("u".into()),
        username: "u".into(),
        disabled: false,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    };
    assert_eq!(s.username, "u");
    assert!(!s.disabled);
    assert_eq!(s.subject_id, SubjectId("u".into()));
    assert_eq!(s.created_at, time::OffsetDateTime::UNIX_EPOCH);
}
