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
use crate::page::{Page, PageReq};

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

/// A service account to be created: an ACL subject and a unique operator-facing
/// name. It has NO password credential (cannot password-login). Machine identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewServiceAccount {
    pub subject_id: SubjectId,
    pub name: String,
}

/// A service account's stored metadata, returned by `list_service_accounts`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceAccount {
    pub subject_id: SubjectId,
    pub name: String,
    pub created_at: OffsetDateTime,
}

/// A service token's stored metadata, returned by `list_service_tokens`. Carries
/// the token's SHA-256 (its stable id — the raw token is never stored or returned),
/// its label, and its lifecycle timestamps. `revoked_at.is_some()` ⇒ revoked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceToken {
    /// SHA-256 of the issued token; the row's primary key and its addressable id.
    pub token_sha256: [u8; 32],
    pub subject_id: SubjectId,
    pub label: String,
    pub created_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
    pub revoked_at: Option<OffsetDateTime>,
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
    async fn find_password_credential(&self, username: &str) -> Result<Option<PasswordCredential>>;

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

    /// Create a service account bound to `account.subject_id`. Ensures the ACL
    /// subject exists (so the account is immediately a valid ACL principal, exactly
    /// like `create_user`). No password. `Conflict` if the name is already taken OR
    /// if `subject_id` already belongs to a human `auth.user` — the machine- and
    /// human-identity namespaces must not overlap, so a token can never authenticate
    /// as an existing user's subject.
    async fn create_service_account(&self, account: &NewServiceAccount) -> Result<()>;

    /// Persist a service token: the SHA-256 of the issued token, its owning account,
    /// a label, and a mandatory expiry. `NotFound` if `subject` is not a service
    /// account. Multiple live tokens per account are allowed (rotation).
    async fn create_service_token(
        &self,
        subject: &SubjectId,
        token_sha256: &[u8; 32],
        label: &str,
        expires_at: OffsetDateTime,
    ) -> Result<()>;

    /// Resolve a presented token hash to its account's subject, iff it is neither
    /// revoked (`revoked_at IS NULL`) nor expired (`expires_at > now`). Otherwise
    /// `Ok(None)`.
    async fn resolve_service_token(
        &self,
        token_sha256: &[u8; 32],
        now: OffsetDateTime,
    ) -> Result<Option<SubjectId>>;

    /// Revoke a service token (idempotent; no-op if absent). A revoked token never
    /// resolves again. Revocation is a soft update (`revoked_at` is set, the row is
    /// kept) — unlike a session, which is hard-deleted on logout — so a revoked token
    /// stays visible in `list_service_tokens` for audit/rotation review.
    async fn revoke_service_token(&self, token_sha256: &[u8; 32]) -> Result<()>;

    /// List a service account's tokens (metadata only — never the raw token),
    /// including revoked/expired ones, stable order (oldest first).
    async fn list_service_tokens(
        &self,
        subject: &SubjectId,
        page: PageReq,
    ) -> Result<Page<ServiceToken>>;

    /// List all service accounts (metadata), stable order.
    async fn list_service_accounts(&self, page: PageReq) -> Result<Page<ServiceAccount>>;
}
