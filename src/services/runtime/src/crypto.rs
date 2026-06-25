//! Service-side cryptography for the auth slice. The control-plane `Auth` trait
//! is a pure store; all hashing/token minting lives here.
//!
//! - Passwords: Argon2id, stored as a PHC verifier string.
//! - Session tokens: a 256-bit CSPRNG secret returned to the caller once; only
//!   its SHA-256 is persisted. Tokens are high-entropy, so a fast hash is correct
//!   (unlike passwords, which need the slow KDF).

use argon2::Argon2;
use argon2::password_hash::rand_core::{OsRng, RngCore};
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use sha2::{Digest, Sha256};

/// A crypto failure (e.g. hashing). Verification never errors — it returns `false`.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("password hashing failed: {0}")]
    Hash(String),
}

/// Hash a plaintext password to an Argon2id PHC string (random salt).
pub fn hash_password(password: &str) -> Result<String, AuthError> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| AuthError::Hash(e.to_string()))
}

/// Verify a plaintext password against a stored PHC string. `false` on any
/// parse or verification failure (never panics, never errors).
pub fn verify_password(password: &str, phc: &str) -> bool {
    match PasswordHash::new(phc) {
        Ok(parsed) => Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok(),
        Err(_) => false,
    }
}

/// Mint a fresh 256-bit session token, hex-encoded (64 chars). Returned to the
/// caller once; store only `token_sha256(&token)`.
pub fn generate_session_token() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// SHA-256 of the raw token string — the value persisted as the session key.
pub fn token_sha256(token: &str) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(token.as_bytes());
    h.finalize().into()
}
