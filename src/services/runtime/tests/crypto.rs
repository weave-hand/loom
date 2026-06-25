use service_runtime::{generate_session_token, hash_password, token_sha256, verify_password};

#[test]
fn argon2_round_trip_and_reject() {
    let phc = hash_password("correct horse").unwrap();
    assert!(phc.starts_with("$argon2"));
    assert!(verify_password("correct horse", &phc));
    assert!(!verify_password("wrong", &phc));
    assert!(!verify_password("correct horse", "not-a-phc-string"));
}

#[test]
fn distinct_salts_per_hash() {
    // Same password hashed twice → different PHC (random salt), both verify.
    let a = hash_password("pw").unwrap();
    let b = hash_password("pw").unwrap();
    assert_ne!(a, b);
    assert!(verify_password("pw", &a));
    assert!(verify_password("pw", &b));
}

#[test]
fn token_is_high_entropy_and_hash_is_stable() {
    let t1 = generate_session_token();
    let t2 = generate_session_token();
    assert_eq!(t1.len(), 64); // 32 bytes hex
    assert_ne!(t1, t2);
    // hashing is deterministic for a given token, differs across tokens
    assert_eq!(token_sha256(&t1), token_sha256(&t1));
    assert_ne!(token_sha256(&t1), token_sha256(&t2));
}
