use std::collections::HashMap;

use async_trait::async_trait;
use control_plane_core::{
    Auth, ControlPlaneError, NewUser, Page, PageReq, PasswordCredential, Result, SubjectId,
    UserSummary,
};
use time::OffsetDateTime;

use crate::MemoryControlPlane;

struct MemUser {
    subject_id: String,
    password_phc: String,
    disabled: bool,
    created_at: OffsetDateTime,
}

struct MemSession {
    subject_id: String,
    expires_at: OffsetDateTime,
}

#[derive(Default)]
pub(crate) struct AuthState {
    /// username -> user
    users: HashMap<String, MemUser>,
    /// token sha-256 -> session
    sessions: HashMap<[u8; 32], MemSession>,
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
}
