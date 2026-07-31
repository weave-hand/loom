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
use crate::secret::Redacted;

/// A user to be created: an ACL subject, a unique username, and the Argon2 PHC
/// verifier (computed service-side — the trait never sees the plaintext).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewUser {
    pub subject_id: SubjectId,
    pub username: String,
    /// Argon2 PHC string, computed service-side. `Redacted` so a `{:?}` of this
    /// struct can never print the verifier.
    pub password_phc: Redacted<String>,
}

/// Failed-login lockout parameters (service-layer config, passed to the store so
/// the increment-and-maybe-lock decision is a single operation). `threshold`
/// consecutive failures within `window` lock the account for `lockout_duration`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LockoutPolicy {
    pub threshold: u32,
    pub window: time::Duration,
    pub lockout_duration: time::Duration,
}

impl Default for LockoutPolicy {
    fn default() -> Self {
        LockoutPolicy {
            threshold: 5,
            window: time::Duration::minutes(15),
            lockout_duration: time::Duration::minutes(15),
        }
    }
}

/// A username's stored password verifier, returned for login.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PasswordCredential {
    pub subject_id: SubjectId,
    pub password_phc: Redacted<String>,
    /// When the account is locked (failed-login threshold reached), the instant the
    /// lock expires; `None` if not locked. The login path rejects while
    /// `locked_until > now`.
    pub locked_until: Option<OffsetDateTime>,
}

/// A user as surfaced by an admin listing: identity + activation state, never the
/// password verifier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserSummary {
    pub subject_id: SubjectId,
    pub username: String,
    /// True iff the user is deactivated (cannot log in; sessions revoked).
    pub disabled: bool,
    pub created_at: OffsetDateTime,
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
    /// Unknown username OR a **disabled** user → `Ok(None)` (the caller must not
    /// distinguish "no such user" from "bad password" from "disabled" in its
    /// response).
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
    /// (`expires_at > now`) AND the subject is not a disabled user. Unknown /
    /// expired / disabled → `Ok(None)`.
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

    /// `true` once the instance has been bootstrapped (an admin created + sealed).
    /// Bootstrap is a one-way state machine: there is deliberately no unseal method.
    async fn is_bootstrap_sealed(&self) -> Result<bool>;
    /// Mark the instance sealed. Insert-once: a second call returns `Conflict`.
    async fn seal_bootstrap(&self) -> Result<()>;

    /// List every user (identity + activation state + created-at), never the
    /// password verifier. `page` is accepted for signature stability; adapters
    /// return a single full page today.
    async fn list_users(&self, page: PageReq) -> Result<Page<UserSummary>>;

    /// Set or clear a user's disabled flag. On disable (`true`), the user's
    /// sessions are revoked immediately. `NotFound` if the username is unknown.
    /// Idempotent for a fixed target state.
    async fn set_user_disabled(&self, username: &str, disabled: bool) -> Result<()>;

    /// Replace `subject`'s stored password verifier (and bump `updated_at`). Shared by
    /// the self-service change and the admin reset — the verify-current decision is a
    /// service-layer concern, consistent with this trait's no-cryptography contract.
    /// `NotFound` if the subject has no password credential.
    async fn update_password(&self, subject: &SubjectId, new_phc: &Redacted<String>) -> Result<()>;

    /// The stored PHC for `subject`, or `None` if the subject has no credential. Lets
    /// the self-service change verify the current password when the caller is
    /// identified by their session subject rather than a username. `Redacted` for the
    /// same reason the struct fields are: a trait method that hands back a naked
    /// verifier is the same hazard one indirection away.
    async fn password_phc_for_subject(
        &self,
        subject: &SubjectId,
    ) -> Result<Option<Redacted<String>>>;

    /// Revoke `subject`'s sessions. `keep = Some(hash)` preserves that one session
    /// (self-service change keeps the caller logged in); `None` revokes all (admin
    /// reset). Idempotent.
    async fn revoke_subject_sessions(
        &self,
        subject: &SubjectId,
        keep: Option<&[u8; 32]>,
    ) -> Result<()>;

    /// Record a failed login for `username` under `policy`: increment the
    /// failed-attempt counter (resetting it to 1 first if the time since
    /// `last_failed_at` exceeded `policy.window`), stamp `last_failed_at = now`, and
    /// set `locked_until = now + policy.lockout_duration` once the counter reaches
    /// `policy.threshold`. No-op if the username is unknown — lockout is keyed by
    /// username and protects existing accounts only.
    async fn record_failed_login(
        &self,
        username: &str,
        now: OffsetDateTime,
        policy: LockoutPolicy,
    ) -> Result<()>;

    /// Clear `username`'s failed-attempt counter and lock, on a successful login.
    /// Idempotent; no-op if the username is unknown.
    async fn reset_failed_logins(&self, username: &str) -> Result<()>;

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
