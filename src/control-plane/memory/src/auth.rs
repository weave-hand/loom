use std::collections::HashMap;

use async_trait::async_trait;
use control_plane_core::{
    Auth, ControlPlaneError, LockoutPolicy, NewServiceAccount, NewUser, Page, PageReq,
    PasswordCredential, Redacted, Result, ServiceAccount, ServiceToken, SubjectId, UserSummary,
};
use time::OffsetDateTime;

use crate::MemoryControlPlane;

struct MemUser {
    subject_id: String,
    password_phc: Redacted<String>,
    disabled: bool,
    created_at: OffsetDateTime,
    failed_attempt_count: u32,
    last_failed_at: Option<OffsetDateTime>,
    locked_until: Option<OffsetDateTime>,
}

struct MemSession {
    subject_id: String,
    expires_at: OffsetDateTime,
}

struct MemServiceAccount {
    subject_id: String,
    name: String,
    created_at: OffsetDateTime,
}

struct MemServiceToken {
    subject_id: String,
    label: String,
    created_at: OffsetDateTime,
    expires_at: OffsetDateTime,
    revoked_at: Option<OffsetDateTime>,
}

#[derive(Default)]
pub(crate) struct AuthState {
    /// username -> user
    users: HashMap<String, MemUser>,
    /// token sha-256 -> session
    sessions: HashMap<[u8; 32], MemSession>,
    /// subject_id -> service account
    service_accounts: HashMap<String, MemServiceAccount>,
    /// token sha-256 -> service token
    service_tokens: HashMap<[u8; 32], MemServiceToken>,
    /// Set once by `seal_bootstrap`; the one-way bootstrap seal.
    bootstrap_sealed: bool,
}

#[async_trait]
impl Auth for MemoryControlPlane {
    #[tracing::instrument(skip(self, user), level = "debug")]
    async fn create_user(&self, user: &NewUser) -> Result<()> {
        let mut auth = self.auth.lock();
        if auth.users.contains_key(&user.username) {
            return Err(ControlPlaneError::Conflict(format!(
                "username {}",
                user.username
            )));
        }
        auth.users.insert(
            user.username.clone(),
            MemUser {
                subject_id: user.subject_id.0.clone(),
                password_phc: user.password_phc.clone(),
                disabled: false,
                created_at: OffsetDateTime::now_utc(),
                failed_attempt_count: 0,
                last_failed_at: None,
                locked_until: None,
            },
        );
        drop(auth);
        // Ensure the ACL subject exists (so the user is a valid ACL principal).
        self.acl.lock().subjects_insert(&user.subject_id.0);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn find_password_credential(&self, username: &str) -> Result<Option<PasswordCredential>> {
        let auth = self.auth.lock();
        Ok(auth
            .users
            .get(username)
            .filter(|u| !u.disabled)
            .map(|u| PasswordCredential {
                subject_id: SubjectId(u.subject_id.clone()),
                password_phc: u.password_phc.clone(),
                locked_until: u.locked_until,
            }))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn create_session(
        &self,
        subject: &SubjectId,
        token_sha256: &[u8; 32],
        expires_at: OffsetDateTime,
    ) -> Result<()> {
        self.auth.lock().sessions.insert(
            *token_sha256,
            MemSession {
                subject_id: subject.0.clone(),
                expires_at,
            },
        );
        Ok(())
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn resolve_session(
        &self,
        token_sha256: &[u8; 32],
        now: OffsetDateTime,
    ) -> Result<Option<SubjectId>> {
        let auth = self.auth.lock();
        let Some(s) = auth.sessions.get(token_sha256) else {
            return Ok(None);
        };
        if s.expires_at <= now {
            return Ok(None);
        }
        // Reject a disabled user's token (defense in depth: a session may have
        // been minted before disable, or in a race with it).
        let disabled = auth
            .users
            .values()
            .any(|u| u.subject_id == s.subject_id && u.disabled);
        Ok((!disabled).then(|| SubjectId(s.subject_id.clone())))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn revoke_session(&self, token_sha256: &[u8; 32]) -> Result<()> {
        self.auth.lock().sessions.remove(token_sha256);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn has_any_user(&self) -> Result<bool> {
        Ok(!self.auth.lock().users.is_empty())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn is_bootstrap_sealed(&self) -> Result<bool> {
        Ok(self.auth.lock().bootstrap_sealed)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn seal_bootstrap(&self) -> Result<()> {
        let mut auth = self.auth.lock();
        if auth.bootstrap_sealed {
            return Err(ControlPlaneError::Conflict(
                "bootstrap already sealed".into(),
            ));
        }
        auth.bootstrap_sealed = true;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_users(&self, _page: PageReq) -> Result<Page<UserSummary>> {
        let auth = self.auth.lock();
        let mut out: Vec<UserSummary> = auth
            .users
            .iter()
            .map(|(username, u)| UserSummary {
                subject_id: SubjectId(u.subject_id.clone()),
                username: username.clone(),
                disabled: u.disabled,
                created_at: u.created_at,
            })
            .collect();
        out.sort_by(|a, b| a.username.cmp(&b.username));
        Ok(Page::from_full(out))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn set_user_disabled(&self, username: &str, disabled: bool) -> Result<()> {
        let mut auth = self.auth.lock();
        let Some(u) = auth.users.get_mut(username) else {
            return Err(ControlPlaneError::NotFound(format!("user {username}")));
        };
        u.disabled = disabled;
        if disabled {
            let subject = u.subject_id.clone();
            auth.sessions.retain(|_, s| s.subject_id != subject);
        }
        Ok(())
    }

    // `new_phc` is the raw Argon2 verifier — never record it as a span field.
    #[tracing::instrument(skip(self, new_phc), level = "debug")]
    async fn update_password(&self, subject: &SubjectId, new_phc: &str) -> Result<()> {
        let mut auth = self.auth.lock();
        match auth.users.values_mut().find(|u| u.subject_id == subject.0) {
            Some(u) => {
                u.password_phc = Redacted::new(new_phc.to_owned());
                Ok(())
            }
            None => Err(ControlPlaneError::NotFound(format!(
                "credential for subject {}",
                subject.0
            ))),
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn password_phc_for_subject(
        &self,
        subject: &SubjectId,
    ) -> Result<Option<Redacted<String>>> {
        let auth = self.auth.lock();
        Ok(auth
            .users
            .values()
            .find(|u| u.subject_id == subject.0)
            .map(|u| u.password_phc.clone()))
    }

    #[tracing::instrument(skip(self, keep), level = "debug")]
    async fn revoke_subject_sessions(
        &self,
        subject: &SubjectId,
        keep: Option<&[u8; 32]>,
    ) -> Result<()> {
        let mut auth = self.auth.lock();
        match keep {
            Some(k) => auth
                .sessions
                .retain(|hash, s| s.subject_id != subject.0 || hash == k),
            None => auth.sessions.retain(|_, s| s.subject_id != subject.0),
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn record_failed_login(
        &self,
        username: &str,
        now: OffsetDateTime,
        policy: LockoutPolicy,
    ) -> Result<()> {
        let mut auth = self.auth.lock();
        if let Some(u) = auth.users.get_mut(username) {
            let within_window = u.last_failed_at.is_some_and(|t| now - t <= policy.window);
            u.failed_attempt_count = if within_window {
                u.failed_attempt_count + 1
            } else {
                1
            };
            u.last_failed_at = Some(now);
            if u.failed_attempt_count >= policy.threshold {
                u.locked_until = Some(now + policy.lockout_duration);
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn reset_failed_logins(&self, username: &str) -> Result<()> {
        let mut auth = self.auth.lock();
        if let Some(u) = auth.users.get_mut(username) {
            u.failed_attempt_count = 0;
            u.last_failed_at = None;
            u.locked_until = None;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, account), level = "debug")]
    async fn create_service_account(&self, account: &NewServiceAccount) -> Result<()> {
        let mut auth = self.auth.lock();
        if auth
            .service_accounts
            .values()
            .any(|a| a.name == account.name)
        {
            return Err(ControlPlaneError::Conflict(format!(
                "service account name {}",
                account.name
            )));
        }
        // Reject a subject_id that is already a human user: the machine- and
        // human-identity namespaces must not overlap (else a minted token could
        // authenticate as an existing user's subject).
        if auth
            .users
            .values()
            .any(|u| u.subject_id == account.subject_id.0)
        {
            return Err(ControlPlaneError::Conflict(format!(
                "subject {} already belongs to a user",
                account.subject_id.0
            )));
        }
        auth.service_accounts.insert(
            account.subject_id.0.clone(),
            MemServiceAccount {
                subject_id: account.subject_id.0.clone(),
                name: account.name.clone(),
                created_at: OffsetDateTime::now_utc(),
            },
        );
        drop(auth);
        // Ensure the ACL subject exists (so the account is a valid ACL principal).
        self.acl.lock().subjects_insert(&account.subject_id.0);
        Ok(())
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn create_service_token(
        &self,
        subject: &SubjectId,
        token_sha256: &[u8; 32],
        label: &str,
        expires_at: OffsetDateTime,
    ) -> Result<()> {
        let mut auth = self.auth.lock();
        if !auth.service_accounts.contains_key(&subject.0) {
            return Err(ControlPlaneError::NotFound(format!(
                "service account {}",
                subject.0
            )));
        }
        auth.service_tokens.insert(
            *token_sha256,
            MemServiceToken {
                subject_id: subject.0.clone(),
                label: label.to_string(),
                created_at: OffsetDateTime::now_utc(),
                expires_at,
                revoked_at: None,
            },
        );
        Ok(())
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn resolve_service_token(
        &self,
        token_sha256: &[u8; 32],
        now: OffsetDateTime,
    ) -> Result<Option<SubjectId>> {
        let auth = self.auth.lock();
        Ok(auth.service_tokens.get(token_sha256).and_then(|t| {
            (t.revoked_at.is_none() && t.expires_at > now).then(|| SubjectId(t.subject_id.clone()))
        }))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn revoke_service_token(&self, token_sha256: &[u8; 32]) -> Result<()> {
        if let Some(t) = self.auth.lock().service_tokens.get_mut(token_sha256)
            && t.revoked_at.is_none()
        {
            t.revoked_at = Some(OffsetDateTime::now_utc());
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_service_tokens(
        &self,
        subject: &SubjectId,
        _page: PageReq,
    ) -> Result<Page<ServiceToken>> {
        let auth = self.auth.lock();
        let mut items: Vec<ServiceToken> = auth
            .service_tokens
            .iter()
            .filter(|(_, t)| t.subject_id == subject.0)
            .map(|(hash, t)| ServiceToken {
                token_sha256: *hash,
                subject_id: SubjectId(t.subject_id.clone()),
                label: t.label.clone(),
                created_at: t.created_at,
                expires_at: t.expires_at,
                revoked_at: t.revoked_at,
            })
            .collect();
        items.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.token_sha256.cmp(&b.token_sha256))
        });
        Ok(Page::from_full(items))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_service_accounts(&self, _page: PageReq) -> Result<Page<ServiceAccount>> {
        let auth = self.auth.lock();
        let mut items: Vec<ServiceAccount> = auth
            .service_accounts
            .values()
            .map(|a| ServiceAccount {
                subject_id: SubjectId(a.subject_id.clone()),
                name: a.name.clone(),
                created_at: a.created_at,
            })
            .collect();
        items.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.subject_id.0.cmp(&b.subject_id.0))
        });
        Ok(Page::from_full(items))
    }
}
