//! The `auth` concern: loom's credential and session store. Like `acl` (which
//! stores policy but never interprets a `RowFilter`), this trait PERSISTS
//! password verifiers and sessions but performs NO cryptography — hashing,
//! verification, and token minting live in the service layer. The `auth` schema
//! is loom-owned. A loom *user* is an ACL subject that has credentials; the two
//! concerns join by [`SubjectId`].

use async_trait::async_trait;
use time::OffsetDateTime;

use crate::acl::SubjectId;
use crate::error::Result;

/// A user to be created: an ACL subject, a unique username, and the Argon2 PHC
/// verifier (computed service-side — the trait never sees the plaintext).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewUser {
    pub subject_id: SubjectId,
    pub username: String,
    /// Argon2 PHC string, computed service-side.
    pub password_phc: String,
}

/// A username's stored password verifier, returned for login.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PasswordCredential {
    pub subject_id: SubjectId,
    pub password_phc: String,
}

#[async_trait]
pub trait Auth {
    /// Create a user bound to `user.subject_id`, storing the Argon2 PHC verifier.
    /// Ensures the ACL subject exists (so the user is immediately a valid ACL
    /// principal; role assignment stays an ACL operation). `Conflict` if the
    /// username is already taken.
    async fn create_user(&self, user: &NewUser) -> Result<()>;

    /// Look up a username's subject + stored password verifier for login.
    /// Unknown username → `Ok(None)` (the caller must not distinguish
    /// "no such user" from "bad password" in its response).
    async fn find_password_credential(
        &self,
        username: &str,
    ) -> Result<Option<PasswordCredential>>;

    /// Persist a session: the SHA-256 of the issued token plus its expiry.
    /// Idempotent on the token hash.
    async fn create_session(
        &self,
        subject: &SubjectId,
        token_sha256: &[u8; 32],
        expires_at: OffsetDateTime,
    ) -> Result<()>;

    /// Resolve a presented token hash to its subject, iff unexpired
    /// (`expires_at > now`). Unknown/expired → `Ok(None)`.
    async fn resolve_session(
        &self,
        token_sha256: &[u8; 32],
        now: OffsetDateTime,
    ) -> Result<Option<SubjectId>>;

    /// Revoke a session (logout). Idempotent (no-op if absent).
    async fn revoke_session(&self, token_sha256: &[u8; 32]) -> Result<()>;

    /// True iff at least one user exists. Drives bootstrap ("seed admin if the
    /// user table is empty").
    async fn has_any_user(&self) -> Result<bool>;
}
