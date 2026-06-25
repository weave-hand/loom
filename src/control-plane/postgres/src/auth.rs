use async_trait::async_trait;
use control_plane_core::{Auth, ControlPlaneError, NewUser, PasswordCredential, Result, SubjectId};
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
}
