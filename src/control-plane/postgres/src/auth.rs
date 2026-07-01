use async_trait::async_trait;
use control_plane_core::{
    Auth, ControlPlaneError, NewServiceAccount, NewUser, Page, PageReq, PasswordCredential, Result,
    ServiceAccount, ServiceToken, SubjectId,
};
use time::OffsetDateTime;

use crate::{PgControlPlane, backend};

/// Map a unique-violation (SQLSTATE 23505) to `Conflict`, anything else to `Backend`.
fn conflict_or_backend(e: sqlx::Error, what: &str) -> ControlPlaneError {
    if let sqlx::Error::Database(db) = &e
        && db.code().as_deref() == Some("23505")
    {
        return ControlPlaneError::Conflict(what.to_string());
    }
    ControlPlaneError::Backend(Box::new(e))
}

/// Map a foreign-key violation (SQLSTATE 23503) to `NotFound`, anything else to `Backend`.
fn notfound_or_backend(e: sqlx::Error, what: &str) -> ControlPlaneError {
    if let sqlx::Error::Database(db) = &e
        && db.code().as_deref() == Some("23503")
    {
        return ControlPlaneError::NotFound(what.to_string());
    }
    ControlPlaneError::Backend(Box::new(e))
}

#[async_trait]
impl Auth for PgControlPlane {
    #[tracing::instrument(skip(self, user), level = "debug")]
    async fn create_user(&self, user: &NewUser) -> Result<()> {
        // One transaction: ensure the ACL subject, then the user + credential.
        // A duplicate username aborts on the auth.user insert (23505 -> Conflict).
        let mut tx = self.pool().begin().await.map_err(backend)?;
        sqlx::query!(
            "insert into acl.subject (id) values ($1) on conflict (id) do nothing",
            &user.subject_id.0,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sqlx::query!(
            "insert into auth.user (subject_id, username) values ($1, $2)",
            &user.subject_id.0,
            &user.username,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| conflict_or_backend(e, &format!("username {}", user.username)))?;
        sqlx::query!(
            "insert into auth.password_credential (subject_id, password_phc) values ($1, $2)",
            &user.subject_id.0,
            &user.password_phc,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn find_password_credential(&self, username: &str) -> Result<Option<PasswordCredential>> {
        let row = sqlx::query!(
            "select u.subject_id, pc.password_phc \
             from auth.user u \
             join auth.password_credential pc on pc.subject_id = u.subject_id \
             where u.username = $1",
            username,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?;
        Ok(row.map(|r| PasswordCredential {
            subject_id: SubjectId(r.subject_id),
            password_phc: r.password_phc,
        }))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn create_session(
        &self,
        subject: &SubjectId,
        token_sha256: &[u8; 32],
        expires_at: OffsetDateTime,
    ) -> Result<()> {
        sqlx::query!(
            "insert into auth.session (token_sha256, subject_id, expires_at) \
             values ($1, $2, $3) \
             on conflict (token_sha256) do update set \
                 subject_id = excluded.subject_id, expires_at = excluded.expires_at",
            &token_sha256[..],
            &subject.0,
            expires_at,
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn resolve_session(
        &self,
        token_sha256: &[u8; 32],
        now: OffsetDateTime,
    ) -> Result<Option<SubjectId>> {
        let row = sqlx::query_scalar!(
            "select subject_id from auth.session \
             where token_sha256 = $1 and expires_at > $2",
            &token_sha256[..],
            now,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?;
        Ok(row.map(SubjectId))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn revoke_session(&self, token_sha256: &[u8; 32]) -> Result<()> {
        sqlx::query!(
            "delete from auth.session where token_sha256 = $1",
            &token_sha256[..],
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn has_any_user(&self) -> Result<bool> {
        let exists = sqlx::query_scalar!("select exists (select 1 from auth.user)")
            .fetch_one(self.pool())
            .await
            .map_err(backend)?
            .unwrap_or(false);
        Ok(exists)
    }

    #[tracing::instrument(skip(self, account), level = "debug")]
    async fn create_service_account(&self, account: &NewServiceAccount) -> Result<()> {
        // One transaction: ensure the ACL subject, then the service account.
        // A duplicate name aborts on the service_account insert (23505 -> Conflict).
        let mut tx = self.pool().begin().await.map_err(backend)?;
        sqlx::query!(
            "insert into acl.subject (id) values ($1) on conflict (id) do nothing",
            &account.subject_id.0,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sqlx::query!(
            "insert into auth.service_account (subject_id, name) values ($1, $2)",
            &account.subject_id.0,
            &account.name,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| conflict_or_backend(e, &format!("service account name {}", account.name)))?;
        tx.commit().await.map_err(backend)?;
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
        sqlx::query!(
            "insert into auth.service_token (token_sha256, subject_id, label, expires_at) \
             values ($1, $2, $3, $4)",
            &token_sha256[..],
            &subject.0,
            label,
            expires_at,
        )
        .execute(self.pool())
        .await
        .map_err(|e| notfound_or_backend(e, &format!("service account {}", subject.0)))?;
        Ok(())
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn resolve_service_token(
        &self,
        token_sha256: &[u8; 32],
        now: OffsetDateTime,
    ) -> Result<Option<SubjectId>> {
        let row = sqlx::query_scalar!(
            "select subject_id from auth.service_token \
             where token_sha256 = $1 and revoked_at is null and expires_at > $2",
            &token_sha256[..],
            now,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?;
        Ok(row.map(SubjectId))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn revoke_service_token(&self, token_sha256: &[u8; 32]) -> Result<()> {
        sqlx::query!(
            "update auth.service_token set revoked_at = now() \
             where token_sha256 = $1 and revoked_at is null",
            &token_sha256[..],
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_service_tokens(
        &self,
        subject: &SubjectId,
        _page: PageReq,
    ) -> Result<Page<ServiceToken>> {
        let rows = sqlx::query!(
            "select token_sha256, subject_id, label, created_at, expires_at, revoked_at \
             from auth.service_token where subject_id = $1 \
             order by created_at, token_sha256",
            &subject.0,
        )
        .fetch_all(self.pool())
        .await
        .map_err(backend)?;
        let items = rows
            .into_iter()
            .map(|r| {
                // The bytea is always 32 bytes (every insert writes a [u8; 32]), but
                // convert fallibly — no panic path — to satisfy the panic-safety lints.
                // Carry the source error rather than discarding it (map_err_ignore).
                let hash: [u8; 32] = r
                    .token_sha256
                    .as_slice()
                    .try_into()
                    .map_err(|e| ControlPlaneError::Backend(Box::new(e)))?;
                Ok(ServiceToken {
                    token_sha256: hash,
                    subject_id: SubjectId(r.subject_id),
                    label: r.label,
                    created_at: r.created_at,
                    expires_at: r.expires_at,
                    revoked_at: r.revoked_at,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Page::from_full(items))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_service_accounts(&self, _page: PageReq) -> Result<Page<ServiceAccount>> {
        let rows = sqlx::query!(
            "select subject_id, name, created_at from auth.service_account \
             order by created_at, subject_id",
        )
        .fetch_all(self.pool())
        .await
        .map_err(backend)?;
        let items = rows
            .into_iter()
            .map(|r| ServiceAccount {
                subject_id: SubjectId(r.subject_id),
                name: r.name,
                created_at: r.created_at,
            })
            .collect();
        Ok(Page::from_full(items))
    }
}
