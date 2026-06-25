use std::collections::HashMap;

use async_trait::async_trait;
use control_plane_core::{Auth, ControlPlaneError, NewUser, PasswordCredential, Result, SubjectId};
use time::OffsetDateTime;

use crate::MemoryControlPlane;

struct MemUser {
    subject_id: String,
    password_phc: String,
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
        let mut auth = self.auth.lock().unwrap();
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
            },
        );
        drop(auth);
        // Ensure the ACL subject exists (so the user is a valid ACL principal).
        self.acl.lock().unwrap().subjects_insert(&user.subject_id.0);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn find_password_credential(&self, username: &str) -> Result<Option<PasswordCredential>> {
        let auth = self.auth.lock().unwrap();
        Ok(auth.users.get(username).map(|u| PasswordCredential {
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
        self.auth.lock().unwrap().sessions.insert(
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
        let auth = self.auth.lock().unwrap();
        Ok(auth
            .sessions
            .get(token_sha256)
            .and_then(|s| (s.expires_at > now).then(|| SubjectId(s.subject_id.clone()))))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn revoke_session(&self, token_sha256: &[u8; 32]) -> Result<()> {
        self.auth.lock().unwrap().sessions.remove(token_sha256);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn has_any_user(&self) -> Result<bool> {
        Ok(!self.auth.lock().unwrap().users.is_empty())
    }
}
