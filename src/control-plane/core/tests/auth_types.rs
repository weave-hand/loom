//! Compile/shape guard for the auth domain types. Behavior is covered by the
//! testkit `auth_contract`; this just pins the public type surface.
use control_plane_core::{NewUser, PasswordCredential, SubjectId};

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
fn service_account_types_construct() {
    use control_plane_core::{NewServiceAccount, ServiceAccount, ServiceToken};
    use time::OffsetDateTime;

    let na = NewServiceAccount {
        subject_id: SubjectId("svc".into()),
        name: "etl".into(),
    };
    assert_eq!(na.name, "etl");

    let now = OffsetDateTime::now_utc();
    let acct = ServiceAccount {
        subject_id: na.subject_id.clone(),
        name: na.name.clone(),
        created_at: now,
    };
    assert_eq!(acct.subject_id, na.subject_id);

    let tok = ServiceToken {
        token_sha256: [7u8; 32],
        subject_id: na.subject_id.clone(),
        label: "primary".into(),
        created_at: now,
        expires_at: now,
        revoked_at: None,
    };
    assert_eq!(tok.token_sha256, [7u8; 32]);
    assert!(tok.revoked_at.is_none());
}
